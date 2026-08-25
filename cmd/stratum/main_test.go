package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"stratum/internal/coordinator"
	"stratum/internal/index"
	"stratum/internal/types"
	"stratum/internal/wal"
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

bloom_filter:
  expected_items: 2000000
  false_positive_rate: 0.02

gc:
  sweep_interval_s: 1234
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
	if cfg.BloomExpectedItems != 2000000 || cfg.BloomFalsePositiveRate != 0.02 {
		t.Errorf("bloom = %d/%f, want 2000000/0.02", cfg.BloomExpectedItems, cfg.BloomFalsePositiveRate)
	}
	if cfg.GCSweepIntervalSec != 1234 {
		t.Errorf("GCSweepIntervalSec = %d, want 1234", cfg.GCSweepIntervalSec)
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

// TestRunCrashRecovery verifies the three-way dispatch of WAL
// PendingRecords through the coordinator layer.
func TestRunCrashRecovery(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	wc := coordinator.NewMockWriteCoordinator()
	dc := coordinator.NewMockDeleteCoordinator()
	dvc := coordinator.NewMockDeleteVersionCoordinator()
	w := wal.NewMockWAL()

	changes := []types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "hello"}}

	records := []types.PendingRecord{
		{Type: types.PendingRecordTypeDeleteMark, KBID: "kb-del"},
		{Type: types.PendingRecordTypeVersionDelete, KBID: "kb-vdel", VersionID: 7},
		{Type: types.PendingRecordTypeVersionWrite, KBID: "kb-w", VersionID: 9, ParentVersionID: 3, Changes: changes},
		// No replay input: must be skipped (counter bumped), not replayed.
		{Type: types.PendingRecordTypeVersionWrite, KBID: "kb-x", VersionID: 11},
	}

	if err := runCrashRecovery(ctx, logger, records, wc, dc, dvc, w); err != nil {
		t.Fatalf("runCrashRecovery: %v", err)
	}

	if got := dc.Calls(); len(got) != 1 || got[0] != "kb-del" {
		t.Errorf("DeleteCoordinator calls = %v, want [kb-del]", got)
	}
	if got := dvc.Calls(); len(got) != 1 || got[0] != "kb-vdel" {
		t.Errorf("DeleteVersionCoordinator calls = %v, want [kb-vdel]", got)
	}
	replays := wc.ReplayCalls()
	if len(replays) != 1 {
		t.Fatalf("ReplayVersionStorageWrites calls = %d, want 1", len(replays))
	}
	if replays[0].KBID != "kb-w" || replays[0].VersionID != 9 || replays[0].ParentVersionID != 3 || len(replays[0].Changes) != 1 {
		t.Errorf("replay call = %+v, want {kb-w, parent 3, version 9, 1 change}", replays[0])
	}

	// The input-less VersionWrite must have bumped the replay counter
	// instead of being replayed.
	got := w.GetReplayCounters()
	if len(got) != 1 || got[0].Record.Type != types.PendingRecordTypeVersionWrite || got[0].Record.VersionID != 11 || got[0].RetryCount != 1 {
		t.Errorf("replay counters = %+v, want 1 bump for {VersionWrite, 11}", got)
	}
}

// TestRunCrashRecovery_FailurePropagates verifies a failing coordinator
// call aborts recovery and bumps the counter.
func TestRunCrashRecovery_FailurePropagates(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	wc := coordinator.NewMockWriteCoordinator()
	dc := coordinator.NewMockDeleteCoordinator()
	dvc := coordinator.NewMockDeleteVersionCoordinator()
	w := wal.NewMockWAL()

	dc.SetExecuteResult(errors.New("boom"))

	records := []types.PendingRecord{
		{Type: types.PendingRecordTypeDeleteMark, KBID: "kb-del"},
	}
	err := runCrashRecovery(ctx, logger, records, wc, dc, dvc, w)
	if err == nil || !strings.Contains(err.Error(), "kb-del") {
		t.Fatalf("runCrashRecovery err = %v, want failure mentioning kb-del", err)
	}
	got := w.GetReplayCounters()
	if len(got) != 1 || got[0].Record.KBID != "kb-del" {
		t.Errorf("replay counters = %+v, want 1 bump for {DeleteMark, kb-del}", got)
	}
}

// reconcileRaftNode records status proposes and serves a fixed version list.
type reconcileRaftNode struct {
	kbs      []types.KnowledgeBaseMeta
	versions map[string][]types.VersionMeta
	proposed map[int64]types.IndexStatus // versionID -> proposed status
}

func (r *reconcileRaftNode) ListKnowledgeBases(_ context.Context) ([]types.KnowledgeBaseMeta, error) {
	return r.kbs, nil
}
func (r *reconcileRaftNode) ListVersions(_ context.Context, kbID string) ([]types.VersionMeta, error) {
	return r.versions[kbID], nil
}
func (r *reconcileRaftNode) ProposeUpdateVersionStatus(_ context.Context, versionID int64, status types.IndexStatus) error {
	if r.proposed == nil {
		r.proposed = make(map[int64]types.IndexStatus)
	}
	r.proposed[versionID] = status
	return nil
}
func (r *reconcileRaftNode) ProposeCreateKB(_ context.Context, kb types.KnowledgeBaseMeta) error {
	return nil
}
func (r *reconcileRaftNode) ProposeMarkKBDeleting(_ context.Context, kbID string) error { return nil }
func (r *reconcileRaftNode) ProposeMarkKBDeleteFailed(_ context.Context, kbID string) error {
	return nil
}
func (r *reconcileRaftNode) ProposeRemoveKBMeta(_ context.Context, kbID string) error { return nil }
func (r *reconcileRaftNode) ProposeCreateVersion(_ context.Context, kbID string, parentVersionID int64) (int64, error) {
	return 0, nil
}
func (r *reconcileRaftNode) ProposeUpdateVersionSummary(_ context.Context, versionID int64, docIDSetHash string) error {
	return nil
}
func (r *reconcileRaftNode) ProposeRollback(_ context.Context, kbID string, targetVersionID int64) error {
	return nil
}
func (r *reconcileRaftNode) ProposeMarkVersionDeleting(_ context.Context, kbID string, versionID int64) error {
	return nil
}
func (r *reconcileRaftNode) ProposeRemoveVersionMeta(_ context.Context, kbID string, versionID int64) error {
	return nil
}
func (r *reconcileRaftNode) GetKB(_ context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
	return types.KnowledgeBaseMeta{}, nil
}
func (r *reconcileRaftNode) GetClusterStatus(_ context.Context) (types.ClusterStatus, error) {
	return types.ClusterStatus{}, nil
}

// TestReconcileIndexStatus verifies the disk-fact decision table:
// PENDING+exists → READY proposed; PENDING+missing → build triggered;
// READY+missing → build triggered; READY+exists → untouched; FAILED → untouched.
func TestReconcileIndexStatus(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	rn := &reconcileRaftNode{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
		versions: map[string][]types.VersionMeta{
			"kb-1": {
				{VersionID: 1, KBID: "kb-1", IndexStatus: types.IndexStatusReady},   // exists: untouched
				{VersionID: 2, KBID: "kb-1", IndexStatus: types.IndexStatusPending}, // exists: derive READY
				{VersionID: 3, KBID: "kb-1", IndexStatus: types.IndexStatusPending}, // missing: build
				{VersionID: 4, KBID: "kb-1", IndexStatus: types.IndexStatusReady},   // missing: rebuild
				{VersionID: 5, KBID: "kb-1", IndexStatus: types.IndexStatusFailed},  // untouched
			},
		},
	}
	exists := map[int64]bool{1: true, 2: true}
	im := &reconcileIndexMgr{
		exists:    exists,
		triggered: map[int64]bool{},
	}

	reconcileIndexStatus(ctx, logger, rn, im, 0)

	if got := rn.proposed[2]; got != types.IndexStatusReady {
		t.Errorf("version 2 (PENDING+exists) proposed = %v, want READY", got)
	}
	if _, ok := rn.proposed[1]; ok {
		t.Errorf("version 1 (READY+exists) must not be re-proposed")
	}
	if _, ok := rn.proposed[5]; ok {
		t.Errorf("version 5 (FAILED) must not be touched")
	}
	if !im.triggered[3] {
		t.Error("version 3 (PENDING+missing) must trigger a build")
	}
	if !im.triggered[4] {
		t.Error("version 4 (READY+missing) must trigger a rebuild")
	}
	if im.triggered[1] || im.triggered[2] || im.triggered[5] {
		t.Errorf("builds must not be triggered for versions 1/2/5; got %v", im.triggered)
	}
}

// reconcileIndexMgr implements index.IndexManager for reconcile tests.
type reconcileIndexMgr struct {
	exists    map[int64]bool
	triggered map[int64]bool
}

func (m *reconcileIndexMgr) IndexExists(_ context.Context, _ string, versionID int64) (bool, error) {
	return m.exists[versionID], nil
}
func (m *reconcileIndexMgr) TriggerBuild(_ context.Context, _ string, versionID int64) error {
	m.triggered[versionID] = true
	return nil
}
func (m *reconcileIndexMgr) Search(_ context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) {
	return nil, nil
}
func (m *reconcileIndexMgr) RegisterBuildCallback(cb index.BuildCompleteCallback) {}
func (m *reconcileIndexMgr) Evict(_ context.Context, kbID string, versionID int64) error {
	return nil
}
func (m *reconcileIndexMgr) Discard(_ context.Context, kbID string, versionID int64) error {
	return nil
}
func (m *reconcileIndexMgr) EvictByKB(_ context.Context, kbID string) error { return nil }
func (m *reconcileIndexMgr) DeleteFilesByKB(_ context.Context, _ string) error {
	return nil
}
func (m *reconcileIndexMgr) EnforceDiskRetention(_ context.Context, _ string, _ []int64) error {
	return nil
}
func (m *reconcileIndexMgr) Ping(_ context.Context) error { return nil }
func (m *reconcileIndexMgr) LoadedCount() int             { return 0 }
