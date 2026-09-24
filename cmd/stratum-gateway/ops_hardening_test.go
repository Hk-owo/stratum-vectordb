package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// opsHardenedServer serves the console the way cmd/stratum-gateway does: through
// opsManager.handler(), i.e. with the request guard installed. testOpsServer
// mounts the bare mux, which is what the older tests want; the H2 tests have to
// go through the real entry point or they would not be testing it.
func opsHardenedServer(t *testing.T, nodeID int) (*httptest.Server, *opsManager) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "console.yaml")

	cfg := defaultOpsConfig(nodeID)
	cfg.BinDir = dir
	cfg.LogDir = filepath.Join(dir, "logs")
	cfg.ConfigDir = filepath.Join(dir, "configs")
	if err := saveOpsConfig(cfgPath, &cfg); err != nil {
		t.Fatal(err)
	}
	m, err := newOpsManager(cfgPath, nodeID)
	if err != nil {
		t.Fatalf("newOpsManager: %v", err)
	}
	srv := httptest.NewServer(m.handler())
	t.Cleanup(srv.Close)
	return srv, m
}

func doOpsWithOrigin(t *testing.T, url, method string, body any, origin, secFetchSite string) int {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if secFetchSite != "" {
		req.Header.Set("Sec-Fetch-Site", secFetchSite)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// H2 of docs/code-review-2026-09-24.md: /ops changes process state, and a page on
// another origin can POST to it without a preflight. `POST /ops/stop` with an
// empty body stops every service, so the cross-site case has to be refused.
func TestOpsGuardRefusesCrossSiteWrites(t *testing.T) {
	srv, _ := opsHardenedServer(t, 1)

	for name, tc := range map[string]struct {
		origin, secFetchSite string
		want                 int
	}{
		"cross-site Origin":          {origin: "http://evil.example", want: http.StatusForbidden},
		"Sec-Fetch-Site cross-site":  {secFetchSite: "cross-site", want: http.StatusForbidden},
		"same-origin Origin":         {origin: srv.URL, want: http.StatusOK},
		"no browser headers at all":  {want: http.StatusOK},
		"same-site (different port)": {origin: "http://127.0.0.1:1", want: http.StatusForbidden},
	} {
		t.Run(name, func(t *testing.T) {
			// /ops/stop with an empty body means "every service" and is idempotent,
			// so it is the harmless stand-in for the state change a cross-site page
			// would trigger.
			got := doOpsWithOrigin(t, srv.URL+"/ops/stop", http.MethodPost, nil, tc.origin, tc.secFetchSite)
			if got != tc.want {
				t.Fatalf("POST /ops/stop with Origin=%q Sec-Fetch-Site=%q: status = %d, want %d",
					tc.origin, tc.secFetchSite, got, tc.want)
			}
		})
	}
}

// Reads stay open: the guard is a CSRF check for state changes, and a cross-site
// GET cannot change anything.
func TestOpsGuardAllowsCrossSiteReads(t *testing.T) {
	srv, _ := opsHardenedServer(t, 1)

	got := doOpsWithOrigin(t, srv.URL+"/ops/health", http.MethodGet, nil, "http://evil.example", "cross-site")
	if got != http.StatusOK {
		t.Fatalf("GET /ops/health = %d, want 200", got)
	}
}

// The heart of H2: these fields decide WHAT RUNS, so they are not editable over
// HTTP at all — a console that can rewrite them is a remote execution endpoint
// (PUT bin_dir + POST start, or docker.script + POST docker/up).
func TestOpsPutConfigRefusesExecSemanticEdits(t *testing.T) {
	srv, m := opsHardenedServer(t, 1)

	m.mu.Lock()
	before := *m.cfg
	m.mu.Unlock()

	for name, patch := range map[string]map[string]any{
		"bin_dir":         {"bin_dir": "/tmp/somewhere-else"},
		"cluster":         {"cluster": []map[string]any{{"id": 9, "gateway_addr": "http://169.254.169.254:80"}}},
		"docker.script":   {"docker": map[string]any{"script": "/tmp/evil.sh"}},
		"station_secret":  {"services": map[string]any{"stratum": map[string]any{"station_secret": "attacker-key"}}},
		"docker.enabled+": {"docker": map[string]any{"enabled": true, "nodes": 9}},
	} {
		t.Run(name, func(t *testing.T) {
			status := doOpsWithOrigin(t, srv.URL+"/ops/config", http.MethodPut, patch, "", "")
			if name == "docker.enabled+" {
				// A plain parameter edit inside the docker section is allowed.
				if status != http.StatusOK {
					t.Fatalf("PUT /ops/config (%v) = %d, want 200", patch, status)
				}
				return
			}
			if status != http.StatusForbidden {
				t.Fatalf("PUT /ops/config (%v) = %d, want 403", patch, status)
			}
		})
	}

	m.mu.Lock()
	after := *m.cfg
	m.mu.Unlock()
	if after.BinDir != before.BinDir {
		t.Errorf("bin_dir changed despite the refusal: %q → %q", before.BinDir, after.BinDir)
	}
	if len(after.Cluster) != len(before.Cluster) {
		t.Errorf("cluster changed despite the refusal: %v → %v", before.Cluster, after.Cluster)
	}
	if after.Services.Stratum.StationSecret != before.Services.Stratum.StationSecret {
		t.Error("station_secret changed despite the refusal")
	}
	if after.Docker.Script != before.Docker.Script {
		t.Errorf("docker.script changed despite the refusal: %q → %q", before.Docker.Script, after.Docker.Script)
	}
}

// The remaining editable half: a binary NAME inside bin_dir is fine, a name that
// climbs out of it is the same remote execution as rewriting bin_dir itself.
func TestOpsPutConfigRefusesABinaryOutsideBinDir(t *testing.T) {
	srv, m := opsHardenedServer(t, 1)

	status := doOpsWithOrigin(t, srv.URL+"/ops/config", http.MethodPut,
		map[string]any{"services": map[string]any{"vecstore": map[string]any{"bin": "../../../tmp/evil"}}}, "", "")
	if status != http.StatusForbidden {
		t.Fatalf("PUT with an escaping bin path = %d, want 403", status)
	}

	m.mu.Lock()
	bin := m.cfg.Services.Vecstore.Bin
	m.mu.Unlock()
	if bin == "../../../tmp/evil" {
		t.Error("the escaping bin path was persisted")
	}
}

func TestOpsPutConfigRefusesAScriptOutsideScriptsDir(t *testing.T) {
	srv, _ := opsHardenedServer(t, 1)

	status := doOpsWithOrigin(t, srv.URL+"/ops/config", http.MethodPut,
		map[string]any{"docker": map[string]any{"script": "/tmp/evil.sh"}}, "", "")
	if status != http.StatusForbidden {
		t.Fatalf("PUT with a script outside scripts/ = %d, want 403", status)
	}
}

// script_two_tier is the same class of field — the two-tier topology runs it instead
// — so it gets the same check, not only the one-tier path.
func TestOpsPutConfigRefusesATwoTierScriptOutsideScriptsDir(t *testing.T) {
	srv, _ := opsHardenedServer(t, 1)

	status := doOpsWithOrigin(t, srv.URL+"/ops/config", http.MethodPut,
		map[string]any{"docker": map[string]any{"script_two_tier": "/tmp/evil.sh"}}, "", "")
	if status != http.StatusForbidden {
		t.Fatalf("PUT with a two-tier script outside scripts/ = %d, want 403", status)
	}
}

// The ordinary case still works: the console must remain usable, or operators
// turn the protections off.
func TestOpsPutConfigAcceptsAnOrdinaryParameterEdit(t *testing.T) {
	srv, m := opsHardenedServer(t, 1)

	status := doOpsWithOrigin(t, srv.URL+"/ops/config", http.MethodPut,
		map[string]any{"services": map[string]any{"stratum": map[string]any{"grpc_addr": "0.0.0.0:7999"}}}, "", "")
	if status != http.StatusOK {
		t.Fatalf("PUT of an ordinary parameter = %d, want 200", status)
	}

	m.mu.Lock()
	addr := m.cfg.Services.Stratum.GRPCAddr
	m.mu.Unlock()
	if addr != "0.0.0.0:7999" {
		t.Errorf("grpc_addr = %q, want it applied", addr)
	}
}
