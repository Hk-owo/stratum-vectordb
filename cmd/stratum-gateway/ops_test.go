package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testOpsServer builds an httptest server exposing /ops/* for one node.
func testOpsServer(t *testing.T, nodeID int, cluster []ClusterNode) (*httptest.Server, *opsManager) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "console.yaml")

	cfg := defaultOpsConfig(nodeID)
	cfg.BinDir = dir
	cfg.LogDir = filepath.Join(dir, "logs")
	cfg.ConfigDir = filepath.Join(dir, "configs")
	cfg.Cluster = cluster
	// Fake service binaries so start/stop lifecycle can be exercised.
	fakeServiceBin(t, dir, cfg.Services.Stratum.Bin)
	fakeServiceBin(t, dir, cfg.Services.Vecstore.Bin)
	fakeServiceBin(t, dir, cfg.Services.Embed.Bin)
	if err := saveOpsConfig(cfgPath, &cfg); err != nil {
		t.Fatal(err)
	}

	m, err := newOpsManager(cfgPath, nodeID)
	if err != nil {
		t.Fatalf("newOpsManager: %v", err)
	}
	srv := httptest.NewServer(m.opsMux)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { m.sup.StopAll(3 * time.Second) })
	return srv, m
}

func doOps(t *testing.T, url, method string, body any) (int, map[string]any) {
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
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestOpsHealth(t *testing.T) {
	srv, _ := testOpsServer(t, 1, nil)
	code, body := doOps(t, srv.URL+"/ops/health", "GET", nil)
	if code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("health: code=%d body=%v", code, body)
	}
}

func TestOpsStartStopStatus(t *testing.T) {
	srv, _ := testOpsServer(t, 1, nil)

	code, body := doOps(t, srv.URL+"/ops/status", "GET", nil)
	if code != http.StatusOK {
		t.Fatalf("status: code=%d", code)
	}
	svcs := body["services"].([]any)
	if len(svcs) != 3 {
		t.Fatalf("expected 3 services, got %d", len(svcs))
	}
	if svcs[0].(map[string]any)["running"] != false {
		t.Errorf("services should start stopped: %v", svcs)
	}

	// Start all.
	code, body = doOps(t, srv.URL+"/ops/start", "POST", map[string]any{"services": []string{}})
	if code != http.StatusOK {
		t.Fatalf("start: code=%d body=%v", code, body)
	}
	_, body = doOps(t, srv.URL+"/ops/status", "GET", nil)
	for _, s := range body["services"].([]any) {
		if !s.(map[string]any)["running"].(bool) {
			t.Errorf("service should be running after start: %v", s)
		}
	}

	// Stop just vecstore.
	code, body = doOps(t, srv.URL+"/ops/stop", "POST", map[string]any{"services": []string{"vecstore"}})
	if code != http.StatusOK {
		t.Fatalf("stop: code=%d body=%v", code, body)
	}
	_, body = doOps(t, srv.URL+"/ops/status", "GET", nil)
	for _, s := range body["services"].([]any) {
		svc := s.(map[string]any)
		if svc["service"] == "vecstore" && svc["running"] == true {
			t.Errorf("vecstore should be stopped: %v", svc)
		}
	}

	// Unknown service → 400.
	code, _ = doOps(t, srv.URL+"/ops/start", "POST", map[string]any{"services": []string{"nope"}})
	if code != http.StatusBadRequest {
		t.Errorf("unknown service should 400, got %d", code)
	}
}

func TestOpsConfigGetPut(t *testing.T) {
	srv, m := testOpsServer(t, 1, nil)

	code, body := doOps(t, srv.URL+"/ops/config", "GET", nil)
	if code != http.StatusOK {
		t.Fatalf("get config: code=%d", code)
	}
	svc := body["services"].(map[string]any)["stratum"].(map[string]any)
	oldAddr := svc["grpc_addr"].(string)

	// PUT a partial update: new grpc addr + vecstore rocksdb path.
	patch := map[string]any{
		"node_id": 1,
		"services": map[string]any{
			"stratum":  map[string]any{"grpc_addr": "0.0.0.0:7999"},
			"vecstore": map[string]any{"rocksdb_path": "/tmp/rocks"},
		},
	}
	code, body = doOps(t, srv.URL+"/ops/config", "PUT", patch)
	if code != http.StatusOK {
		t.Fatalf("put config: code=%d body=%v", code, body)
	}

	_, body = doOps(t, srv.URL+"/ops/config", "GET", nil)
	svc = body["services"].(map[string]any)["stratum"].(map[string]any)
	if svc["grpc_addr"].(string) != "0.0.0.0:7999" {
		t.Errorf("grpc_addr = %v", svc["grpc_addr"])
	}
	vs := body["services"].(map[string]any)["vecstore"].(map[string]any)
	if vs["rocksdb_path"].(string) != "/tmp/rocks" {
		t.Errorf("rocksdb_path = %v", vs["rocksdb_path"])
	}
	// Persisted on disk.
	if _, err := os.Stat(m.cfgPath); err != nil {
		t.Errorf("config not persisted: %v", err)
	}
	// Unchanged fields preserved.
	if oldAddr == "0.0.0.0:7999" {
		t.Fatal("bad test setup")
	}
	if svc["raft_addr"] == nil {
		t.Errorf("raft_addr lost after merge")
	}
}

func TestOpsLogs(t *testing.T) {
	srv, m := testOpsServer(t, 1, nil)
	logPath := filepath.Join(m.cfg.LogDir, "stratum.log")
	if err := os.MkdirAll(m.cfg.LogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	content := "line1\nline2\nline3\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	code, body := doOps(t, srv.URL+"/ops/logs/stratum?lines=2", "GET", nil)
	if code != http.StatusOK {
		t.Fatalf("logs: code=%d", code)
	}
	lines := body["lines"].([]any)
	if len(lines) != 2 || lines[0] != "line2" || lines[1] != "line3" {
		t.Errorf("tail lines = %v, want [line2 line3]", lines)
	}
	if body["truncated"] != true {
		t.Errorf("truncated should be true, got %v", body["truncated"])
	}

	// Missing log file → 200 with empty lines.
	code, body = doOps(t, srv.URL+"/ops/logs/embed?lines=10", "GET", nil)
	if code != http.StatusOK {
		t.Fatalf("missing log: code=%d", code)
	}
	if lines := body["lines"].([]any); len(lines) != 0 {
		t.Errorf("missing log should yield empty lines, got %v", lines)
	}

	// Unknown service → 400.
	code, _ = doOps(t, srv.URL+"/ops/logs/whatever", "GET", nil)
	if code != http.StatusBadRequest {
		t.Errorf("unknown service logs should 400, got %d", code)
	}
}

// TestOpsNodesLocal verifies the local node appears online with services.
func TestOpsNodesLocal(t *testing.T) {
	srv, _ := testOpsServer(t, 1, nil)
	code, body := doOps(t, srv.URL+"/ops/nodes", "GET", nil)
	if code != http.StatusOK {
		t.Fatalf("nodes: code=%d", code)
	}
	if body["local_node_id"].(float64) != 1 {
		t.Errorf("local_node_id = %v", body["local_node_id"])
	}
	nodes := body["nodes"].([]any)
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	n := nodes[0].(map[string]any)
	if n["online"] != true || n["local"] != true {
		t.Errorf("node should be local+online: %v", n)
	}
	if n["services"] == nil {
		t.Errorf("local node should carry services")
	}
}

// TestOpsCrossNodeForwarding wires two gateways (node 1 and node 2) and
// drives node 2 through node 1's console endpoints.
func TestOpsCrossNodeForwarding(t *testing.T) {
	srv2, m2 := testOpsServer(t, 2, nil)
	srv1, _ := testOpsServer(t, 1, []ClusterNode{
		{ID: 1, GatewayAddr: "http://127.0.0.1:1"}, // placeholder, replaced below? no: local node1 needs a real addr only for probes of itself
		{ID: 2, GatewayAddr: srv2.URL},
	})

	// /ops/nodes on node 1: node1 local+online, node2 online via probe.
	code, body := doOps(t, srv1.URL+"/ops/nodes", "GET", nil)
	if code != http.StatusOK {
		t.Fatalf("nodes: code=%d", code)
	}
	nodes := body["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	byID := map[float64]map[string]any{}
	for _, n := range nodes {
		m := n.(map[string]any)
		byID[m["id"].(float64)] = m
	}
	if !byID[1]["online"].(bool) || !byID[1]["local"].(bool) {
		t.Errorf("node1 should be local+online: %v", byID[1])
	}
	if !byID[2]["online"].(bool) || byID[2]["local"].(bool) {
		t.Errorf("node2 should be online+remote: %v", byID[2])
	}

	// Forward /ops/nodes/2/status.
	code, body = doOps(t, srv1.URL+"/ops/nodes/2/status", "GET", nil)
	if code != http.StatusOK {
		t.Fatalf("forward status: code=%d body=%v", code, body)
	}
	if body["node_id"].(float64) != 2 {
		t.Errorf("forwarded status node_id = %v", body["node_id"])
	}

	// Forward start of a single service on node 2.
	code, body = doOps(t, srv1.URL+"/ops/nodes/2/start", "POST", map[string]any{"services": []string{"embed"}})
	if code != http.StatusOK {
		t.Fatalf("forward start: code=%d body=%v", code, body)
	}
	st := m2.sup.Status()
	if !findService(st, ServiceEmbed).Running {
		t.Errorf("embed should be running on node 2 after forwarded start")
	}

	// Forwarded logs read from node 2's log dir.
	logPath := filepath.Join(m2.cfg.LogDir, "embed.log")
	if err := os.MkdirAll(m2.cfg.LogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("hello from node2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, body = doOps(t, srv1.URL+"/ops/nodes/2/logs/embed", "GET", nil)
	if code != http.StatusOK {
		t.Fatalf("forward logs: code=%d", code)
	}
	lines := body["lines"].([]any)
	if len(lines) != 1 || lines[0] != "hello from node2" {
		t.Errorf("forwarded logs = %v", lines)
	}

	// Forwarded config edit on node 2 persists there.
	code, body = doOps(t, srv1.URL+"/ops/nodes/2/config", "PUT", map[string]any{
		"services": map[string]any{"stratum": map[string]any{"grpc_addr": "0.0.0.0:7002"}},
	})
	if code != http.StatusOK {
		t.Fatalf("forward put config: code=%d body=%v", code, body)
	}
	_, body = doOps(t, srv2.URL+"/ops/config", "GET", nil) // read directly on node 2
	svc := body["services"].(map[string]any)["stratum"].(map[string]any)
	if svc["grpc_addr"].(string) != "0.0.0.0:7002" {
		t.Errorf("forwarded config not applied on node2: %v", svc["grpc_addr"])
	}

	// Unknown node → 404.
	code, _ = doOps(t, srv1.URL+"/ops/nodes/99/status", "GET", nil)
	if code != http.StatusNotFound {
		t.Errorf("unknown node should 404, got %d", code)
	}
}

// TestOpsLocalDispatchThroughNodePrefix verifies /ops/nodes/{local}/...
// is served locally (same code path the frontend always uses).
func TestOpsLocalDispatchThroughNodePrefix(t *testing.T) {
	srv, _ := testOpsServer(t, 1, nil)
	code, body := doOps(t, srv.URL+"/ops/nodes/1/status", "GET", nil)
	if code != http.StatusOK {
		t.Fatalf("local dispatch: code=%d body=%v", code, body)
	}
	if body["node_id"].(float64) != 1 {
		t.Errorf("local dispatch node_id = %v", body["node_id"])
	}
}
