package main

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestLoadOpsConfig_Defaults verifies a missing config file yields the
// single-node defaults.
func TestLoadOpsConfig_Defaults(t *testing.T) {
	cfg, err := loadOpsConfig(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("loadOpsConfig: %v", err)
	}
	if cfg.NodeID != 0 {
		t.Errorf("NodeID = %d, want 0 (seeded later by the gateway)", cfg.NodeID)
	}
	if cfg.BinDir == "" || cfg.LogDir == "" || cfg.ConfigDir == "" {
		t.Errorf("path defaults not applied: %+v", cfg)
	}
	s := cfg.Services.Stratum
	if s.GRPCAddr != "0.0.0.0:7000" || s.RaftAddr != "0.0.0.0:8000" {
		t.Errorf("stratum defaults wrong: %+v", s)
	}
	if len(cfg.Services.Stratum.Peers) != 1 {
		t.Errorf("expected 1 default peer, got %+v", s.Peers)
	}
}

// TestLoadOpsConfig_Overlays verifies YAML values overlay defaults while
// unset fields keep them.
func TestLoadOpsConfig_Overlays(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console.yaml")
	content := `node_id: 2
services:
  stratum:
    grpc_addr: "0.0.0.0:7002"
    data_dir: "/data/node2"
  vecstore:
    grpc_addr: "127.0.0.1:7102"
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadOpsConfig(path)
	if err != nil {
		t.Fatalf("loadOpsConfig: %v", err)
	}
	s := cfg.Services.Stratum
	if s.GRPCAddr != "0.0.0.0:7002" || s.DataDir != "/data/node2" {
		t.Errorf("overlay failed: %+v", s)
	}
	if s.RaftAddr != "0.0.0.0:8000" {
		t.Errorf("unset field should keep default, got %q", s.RaftAddr)
	}
	if cfg.Services.Vecstore.GRPCAddr != "127.0.0.1:7102" {
		t.Errorf("vecstore overlay failed: %+v", cfg.Services.Vecstore)
	}
	if cfg.Services.Embed.ServiceAddr != "http://localhost:8080" {
		t.Errorf("embed default lost: %q", cfg.Services.Embed.ServiceAddr)
	}
}

// TestSaveOpsConfig_RoundTrip verifies save → load round-trips values.
func TestSaveOpsConfig_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console.yaml")
	cfg := defaultOpsConfig(1)
	cfg.Services.Stratum.DataDir = "/custom/data"
	cfg.Cluster = []ClusterNode{{ID: 1, GatewayAddr: "http://10.0.0.1:8081"}, {ID: 2, GatewayAddr: "http://10.0.0.2:8081"}}
	if err := saveOpsConfig(path, &cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := loadOpsConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Services.Stratum.DataDir != "/custom/data" {
		t.Errorf("data_dir = %q", got.Services.Stratum.DataDir)
	}
	if len(got.Cluster) != 2 || got.Cluster[1].ID != 2 {
		t.Errorf("cluster round-trip failed: %+v", got.Cluster)
	}
}

// TestWriteStratumConfig verifies the generated YAML uses the same schema
// cmd/stratum expects (node/raft/peers/storage/vecstore/embed/index…)
// and carries edited values.
func TestWriteStratumConfig(t *testing.T) {
	cfg := defaultOpsConfig(1)
	cfg.Services.Stratum.GRPCAddr = "0.0.0.0:7777"
	cfg.Services.Stratum.HeartbeatIntervalMS = 150
	cfg.Services.Stratum.ElectionTimeoutMinMS = 1500
	cfg.Services.Stratum.ElectionTimeoutMaxMS = 3500
	cfg.Services.Stratum.WriteMaxRetries = 9
	cfg.ConfigDir = t.TempDir()

	path, err := cfg.writeStratumConfig()
	if err != nil {
		t.Fatalf("writeStratumConfig: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("generated config missing: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Node struct {
			NodeID   int64  `yaml:"node_id"`
			GRPCAddr string `yaml:"grpc_addr"`
		} `yaml:"node"`
		Raft struct {
			Peers                []PeerEntry `yaml:"peers"`
			HeartbeatIntervalMS  int64       `yaml:"heartbeat_interval_ms"`
			ElectionTimeoutMinMS int64       `yaml:"election_timeout_min_ms"`
			ElectionTimeoutMaxMS int64       `yaml:"election_timeout_max_ms"`
		} `yaml:"raft"`
		WriteCoordinator struct {
			MaxRetries int `yaml:"max_retries"`
		} `yaml:"write_coordinator"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("generated config not valid YAML: %v", err)
	}
	if doc.Node.GRPCAddr != "0.0.0.0:7777" {
		t.Errorf("GRPCAddr = %q", doc.Node.GRPCAddr)
	}
	if doc.Raft.HeartbeatIntervalMS != 150 || doc.Raft.ElectionTimeoutMinMS != 1500 || doc.Raft.ElectionTimeoutMaxMS != 3500 {
		t.Errorf("raft timing = %d/%d/%d", doc.Raft.HeartbeatIntervalMS, doc.Raft.ElectionTimeoutMinMS, doc.Raft.ElectionTimeoutMaxMS)
	}
	if doc.WriteCoordinator.MaxRetries != 9 {
		t.Errorf("WriteMaxRetries = %d", doc.WriteCoordinator.MaxRetries)
	}
	if len(doc.Raft.Peers) != 1 || doc.Raft.Peers[0].ID != 1 {
		t.Errorf("peers = %+v", doc.Raft.Peers)
	}
}

// TestBinPath verifies bin resolution prefers the console bin dir.
func TestBinPath(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "stratum")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := defaultOpsConfig(1)
	cfg.BinDir = dir
	if p := cfg.binPath(ServiceStratum); p != fake {
		t.Errorf("binPath = %q, want %q", p, fake)
	}
	// Unmanaged binary falls back to the bare name (PATH).
	cfg.BinDir = t.TempDir()
	if p := cfg.binPath(ServiceEmbed); p != "mock-embed" {
		t.Errorf("binPath fallback = %q, want mock-embed", p)
	}
}
