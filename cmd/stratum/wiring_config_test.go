package main

// wiring_config_test.go — the config keys added when the "implemented but never
// wired" audit closed its findings: control_plane.failure_budget,
// index_manager.candidate_n and node.metrics_addr.
//
// Each of them is a knob whose failure mode is silence. A mistyped or
// unparsed key does not error — it just leaves the behaviour at the default, and
// the default is exactly what the feature was added to make changeable
// (the failure budget was hardcoded, the candidate budget could only be edited
// in C++, and the metrics endpoint did not exist at all).

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadConfig_ControlPlaneFailureBudget(t *testing.T) {
	path := writeConfigFile(t, "control_plane:\n  failure_budget: 2\n")
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.ControlPlaneFailureBudget != 2 {
		t.Errorf("ControlPlaneFailureBudget = %d, want 2", cfg.ControlPlaneFailureBudget)
	}
}

// TestLoadConfig_ControlPlaneFailureBudgetUnsetKeepsThePlaneDefault is the safety
// half: an absent key must not become a zero budget, which the control plane
// would read as "use DefaultFailureBudget" — the same behaviour, but only by
// accident. Pinning it here keeps the intent visible.
func TestLoadConfig_ControlPlaneFailureBudgetUnsetKeepsThePlaneDefault(t *testing.T) {
	path := writeConfigFile(t, "node:\n  node_id: 7\n")
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.ControlPlaneFailureBudget != 0 {
		t.Errorf("ControlPlaneFailureBudget = %d, want 0 (unset → plane default)",
			cfg.ControlPlaneFailureBudget)
	}
}

func TestLoadConfig_CandidateNAndMetricsAddr(t *testing.T) {
	path := writeConfigFile(t, "node:\n  metrics_addr: \"127.0.0.1:9100\"\n"+
		"index_manager:\n  candidate_n: 128\n")
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.IndexCandidateN != 128 {
		t.Errorf("IndexCandidateN = %d, want 128", cfg.IndexCandidateN)
	}
	if cfg.MetricsAddr != "127.0.0.1:9100" {
		t.Errorf("MetricsAddr = %q, want 127.0.0.1:9100", cfg.MetricsAddr)
	}
}

// TestStartMetricsServer_EmptyAddrIsDisabled pins the default: metrics are
// opt-in, because the endpoint reports node state without authenticating anyone.
func TestStartMetricsServer_EmptyAddrIsDisabled(t *testing.T) {
	srv, err := startMetricsServer("", zap.NewNop(), nodeMetricsSource{})
	if err != nil {
		t.Fatalf("startMetricsServer(\"\"): %v", err)
	}
	if srv != nil {
		t.Errorf("startMetricsServer(\"\") = %v, want nil (disabled)", srv)
	}
}

// TestMetricsEndpoint_ReportsTheStandardCollectorsAndTheNodeGauges is the
// end-to-end half: the endpoint has to actually serve, and it has to carry both
// the collectors every Prometheus deployment expects and the node-specific
// series that are the reason to scrape THIS process.
func TestMetricsEndpoint_ReportsTheStandardCollectorsAndTheNodeGauges(t *testing.T) {
	src := nodeMetricsSource{
		LoadedIndexes:   func() int { return 3 },
		ChunkStoreBytes: func(context.Context) (uint64, error) { return 4242, nil },
	}
	srv, err := startMetricsServer("127.0.0.1:0", zap.NewNop(), src)
	if err != nil {
		t.Fatalf("startMetricsServer: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	resp, err := http.Get("http://" + srv.Addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200\n%s", resp.StatusCode, body)
	}

	text := string(body)
	for _, want := range []string{
		"go_goroutines",            // the standard Go collector
		"stratum_loaded_indexes 3", // the node's own gauge, with its value
		"stratum_chunk_store_bytes 4242",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("/metrics does not contain %q", want)
		}
	}
}

// TestMetricsEndpoint_OmitsWhatTheNodeCannotAnswer: a control-role node has no
// index manager and no chunk store. Reporting 0 for "indexes in memory" would be
// an invented fact, so the series is left out instead.
func TestMetricsEndpoint_OmitsWhatTheNodeCannotAnswer(t *testing.T) {
	srv, err := startMetricsServer("127.0.0.1:0", zap.NewNop(), nodeMetricsSource{})
	if err != nil {
		t.Fatalf("startMetricsServer: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	resp, err := http.Get("http://" + srv.Addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	text := string(body)
	if strings.Contains(text, "stratum_loaded_indexes") {
		t.Error("stratum_loaded_indexes is present without an index manager")
	}
	if strings.Contains(text, "stratum_chunk_store_bytes") {
		t.Error("stratum_chunk_store_bytes is present without a chunk store")
	}
	if !strings.Contains(text, "go_goroutines") {
		t.Error("the standard collectors must still be served")
	}
}
