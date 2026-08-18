package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLoadConfig_OverlaysDefaults verifies that a YAML config file overrides
// the single-node defaults while unset fields keep their defaults.
func TestLoadConfig_OverlaysDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	content := `node:
  node_id: 2
  grpc_addr: "0.0.0.0:7000"
  raft_addr: "0.0.0.0:8001"

raft:
  peers:
    - id: 1
      addr: "node1:8000"
      service_addr: "node1:7000"
    - id: 2
      addr: "node2:8001"
      service_addr: "node2:7001"
    - id: 3
      addr: "node3:8002"
      service_addr: "node3:7002"

storage:
  data_dir: "/var/lib/stratum/node2"

vecstore:
  grpc_addr: "mock-embed:8080"

embed:
  service_addr: "http://mock-embed:8080"

index_manager:
  lru_capacity: 32
  load_wait_timeout_ms: 8000
  callback_max_retries: 5
  callback_retry_base_interval_ms: 400

write_coordinator:
  max_retries: 6
  retry_base_interval_ms: 150

delete_coordinator:
  max_retries: 7
  retry_base_interval_ms: 600
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	if cfg.NodeID != 2 {
		t.Errorf("NodeID = %d, want 2", cfg.NodeID)
	}
	if cfg.GRPCAddr != "0.0.0.0:7000" {
		t.Errorf("GRPCAddr = %q", cfg.GRPCAddr)
	}
	if cfg.RaftAddr != "0.0.0.0:8001" {
		t.Errorf("RaftAddr = %q", cfg.RaftAddr)
	}
	if cfg.DataDir != "/var/lib/stratum/node2" {
		t.Errorf("DataDir = %q", cfg.DataDir)
	}
	if cfg.VecstoreGRPCAddr != "mock-embed:8080" {
		t.Errorf("VecstoreGRPCAddr = %q", cfg.VecstoreGRPCAddr)
	}
	if cfg.EmbedServiceAddr != "http://mock-embed:8080" {
		t.Errorf("EmbedServiceAddr = %q", cfg.EmbedServiceAddr)
	}
	if len(cfg.Peers) != 3 {
		t.Fatalf("Peers = %d, want 3", len(cfg.Peers))
	}
	if cfg.Peers[1].ID != 2 || cfg.Peers[1].RaftAddr != "node2:8001" || cfg.Peers[1].ServiceAddr != "node2:7001" {
		t.Errorf("Peers[1] = %+v", cfg.Peers[1])
	}
	if cfg.IndexLRUCapacity != 32 {
		t.Errorf("IndexLRUCapacity = %d, want 32", cfg.IndexLRUCapacity)
	}
	if cfg.IndexLoadWaitTimeout != 8*time.Second {
		t.Errorf("IndexLoadWaitTimeout = %v, want 8s", cfg.IndexLoadWaitTimeout)
	}
	if cfg.WriteMaxRetries != 6 || cfg.WriteRetryBaseMS != 150 {
		t.Errorf("write coordinator = %d/%d", cfg.WriteMaxRetries, cfg.WriteRetryBaseMS)
	}
	if cfg.DeleteMaxRetries != 7 || cfg.DeleteRetryBaseMS != 600 {
		t.Errorf("delete coordinator = %d/%d", cfg.DeleteMaxRetries, cfg.DeleteRetryBaseMS)
	}
}

// TestLoadConfig_UnsetFieldsKeepDefaults verifies unset YAML fields fall back
// to defaultConfig() values.
func TestLoadConfig_UnsetFieldsKeepDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("node:\n  node_id: 3\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	if cfg.NodeID != 3 {
		t.Errorf("NodeID = %d, want 3", cfg.NodeID)
	}
	// Unset fields keep single-node defaults.
	if cfg.GRPCAddr != "0.0.0.0:7000" {
		t.Errorf("GRPCAddr = %q, want default", cfg.GRPCAddr)
	}
	if len(cfg.Peers) != 1 {
		t.Errorf("Peers = %d, want default 1", len(cfg.Peers))
	}
	if cfg.IndexLRUCapacity != 16 {
		t.Errorf("IndexLRUCapacity = %d, want default 16", cfg.IndexLRUCapacity)
	}
}
