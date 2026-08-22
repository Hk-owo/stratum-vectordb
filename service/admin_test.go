package service

import (
	"context"
	"testing"

	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// adminHarness bundles dependencies for AdminService tests.
type adminHarness struct {
	svc      *AdminServiceImpl
	raftNode *raft.MockRaftNode
	indexMgr *index.MockIndexManager
	wal      *wal.MockWAL
}

func newAdminHarness() *adminHarness {
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	// Wire index deps with trivial in-memory data sources so the async
	// build goroutine TriggerBuild spawns never panics on nil callbacks.
	im := index.NewMockIndexManager(index.MockIndexManagerDeps{
		ListDocIDs:         func(ctx context.Context, kbID string, versionID int64) ([]string, error) { return nil, nil },
		ListChunkIDsByDocs: func(ctx context.Context, kbID string, docIDs []string) ([]string, error) { return nil, nil },
		ReadChunkVector:    func(ctx context.Context, kbID, chunkID string) ([]float32, error) { return nil, nil },
	}, 16, 0)
	svc := NewAdminService(1, rn, im, docstore.NewMockDocStore(), chunkstore.NewMockChunkStore(), w)
	return &adminHarness{svc: svc, raftNode: rn, indexMgr: im, wal: w}
}

// noLeaderRaft wraps MockRaftNode to report a leaderless cluster, for
// HealthCheck's degraded path.
type noLeaderRaft struct {
	*raft.MockRaftNode
}

func (r *noLeaderRaft) GetClusterStatus(context.Context) (types.ClusterStatus, error) {
	return types.ClusterStatus{HasLeader: false, MemberCount: 1}, nil
}

// failingTriggerBuild wraps MockIndexManager to make TriggerBuild fail.
type failingTriggerBuild struct {
	*index.MockIndexManager
}

func (m *failingTriggerBuild) TriggerBuild(context.Context, string, int64) error {
	return context.DeadlineExceeded
}

func TestAdmin_HealthCheck_Healthy(t *testing.T) {
	h := newAdminHarness()
	resp, err := h.svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if resp.Status != pb.HealthStatus_HEALTH_STATUS_HEALTHY {
		t.Errorf("status = %v, want HEALTHY", resp.Status)
	}
	if resp.Details != "ok" {
		t.Errorf("details = %q, want %q", resp.Details, "ok")
	}
}

func TestAdmin_HealthCheck_DegradedNoLeader(t *testing.T) {
	w := wal.NewMockWAL()
	rn := &noLeaderRaft{MockRaftNode: raft.NewMockRaftNode(w)}
	im := index.NewMockIndexManager(index.MockIndexManagerDeps{}, 16, 0)
	svc := NewAdminService(1, rn, im, docstore.NewMockDocStore(), chunkstore.NewMockChunkStore(), w)

	resp, err := svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if resp.Status != pb.HealthStatus_HEALTH_STATUS_DEGRADED {
		t.Errorf("status = %v, want DEGRADED", resp.Status)
	}
	if resp.Details == "" {
		t.Error("expected non-empty details for degraded health")
	}
}

func TestAdmin_HealthCheck_DegradedIndex(t *testing.T) {
	h := newAdminHarness()
	h.indexMgr.SetPingError(context.DeadlineExceeded)

	resp, err := h.svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if resp.Status != pb.HealthStatus_HEALTH_STATUS_DEGRADED {
		t.Errorf("status = %v, want DEGRADED", resp.Status)
	}
}

func TestAdmin_GetClusterStatus_Leader(t *testing.T) {
	h := newAdminHarness()
	resp, err := h.svc.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if resp.NodeId != 1 {
		t.Errorf("node_id = %d, want 1", resp.NodeId)
	}
	if !resp.HasLeader {
		t.Error("has_leader = false, want true")
	}
	if resp.LeaderId != 1 {
		t.Errorf("leader_id = %d, want 1", resp.LeaderId)
	}
	if resp.MemberCount != 1 {
		t.Errorf("member_count = %d, want 1", resp.MemberCount)
	}
}

func TestAdmin_GetClusterStatus_NoLeader(t *testing.T) {
	w := wal.NewMockWAL()
	rn := &noLeaderRaft{MockRaftNode: raft.NewMockRaftNode(w)}
	im := index.NewMockIndexManager(index.MockIndexManagerDeps{}, 16, 0)
	svc := NewAdminService(1, rn, im, docstore.NewMockDocStore(), chunkstore.NewMockChunkStore(), w)

	resp, err := svc.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if resp.HasLeader {
		t.Error("has_leader = true, want false")
	}
	if resp.LeaderId != 0 {
		t.Errorf("leader_id = %d, want 0", resp.LeaderId)
	}
}

func TestAdmin_GetSystemStatus_StuckAndFailed(t *testing.T) {
	h := newAdminHarness()
	ctx := context.Background()

	// kb-good: one healthy version.
	if err := h.raftNode.ProposeCreateKB(ctx, kbMeta("kb-good", types.KBStatusActive)); err != nil {
		t.Fatal(err)
	}
	goodV, err := h.raftNode.ProposeCreateVersion(ctx, "kb-good", 0)
	if err != nil {
		t.Fatal(err)
	}

	// kb-stuck: a version marked FAILED (index build failed).
	if err := h.raftNode.ProposeCreateKB(ctx, kbMeta("kb-stuck", types.KBStatusActive)); err != nil {
		t.Fatal(err)
	}
	stuckV, err := h.raftNode.ProposeCreateVersion(ctx, "kb-stuck", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.raftNode.ProposeUpdateVersionStatus(ctx, stuckV, types.IndexStatusFailed); err != nil {
		t.Fatal(err)
	}

	// kb-dead: delete failed.
	if err := h.raftNode.ProposeCreateKB(ctx, kbMeta("kb-dead", types.KBStatusDeleteFailed)); err != nil {
		t.Fatal(err)
	}

	// WAL replay counters: one delete-mark stuck, one version write stuck.
	h.wal.IncrementReplayCounter(types.PendingRecord{Type: types.PendingRecordTypeDeleteMark, KBID: "kb-dead"})
	h.wal.IncrementReplayCounter(types.PendingRecord{Type: types.PendingRecordTypeVersionWrite, VersionID: stuckV})

	resp, err := h.svc.GetSystemStatus(ctx, &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus: %v", err)
	}
	if resp.Health == nil {
		t.Fatal("expected health in system status")
	}

	// Stuck versions: exactly the FAILED one.
	if len(resp.StuckVersions) != 1 || resp.StuckVersions[0].KbId != "kb-stuck" || resp.StuckVersions[0].VersionId != stuckV {
		t.Errorf("stuck versions = %+v, want [kb-stuck v%d FAILED]", resp.StuckVersions, stuckV)
	}
	if resp.StuckVersions[0].IndexStatus != pb.IndexStatus_INDEX_STATUS_FAILED {
		t.Errorf("stuck index status = %v, want FAILED", resp.StuckVersions[0].IndexStatus)
	}

	// Delete-failed KBs.
	if len(resp.DeleteFailedKbs) != 1 || resp.DeleteFailedKbs[0] != "kb-dead" {
		t.Errorf("delete_failed_kbs = %v, want [kb-dead]", resp.DeleteFailedKbs)
	}

	// WAL alerts: two counters.
	if len(resp.WalAlerts) != 2 {
		t.Fatalf("wal_alerts = %+v, want 2 entries", resp.WalAlerts)
	}
	hasDeleteAlert := false
	for _, a := range resp.WalAlerts {
		if a.RetryCount == 1 && a.Description != "" {
			if a.Description == "delete mark for kb-dead" {
				hasDeleteAlert = true
			}
		}
	}
	if !hasDeleteAlert {
		t.Errorf("expected a delete-mark WAL alert, got %+v", resp.WalAlerts)
	}

	// A healthy version must not be reported as stuck.
	_ = goodV
}

func TestAdmin_RebuildIndex_Success(t *testing.T) {
	h := newAdminHarness()
	ctx := context.Background()
	if err := h.raftNode.ProposeCreateKB(ctx, kbMeta("kb-1", types.KBStatusActive)); err != nil {
		t.Fatal(err)
	}
	vID, err := h.raftNode.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.raftNode.ProposeUpdateVersionStatus(ctx, vID, types.IndexStatusFailed); err != nil {
		t.Fatal(err)
	}

	resp, err := h.svc.RebuildIndex(ctx, &pb.RebuildIndexRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       vID,
	})
	if err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if !resp.Success {
		t.Error("expected success=true")
	}

	// Status must have been reset to PENDING before the rebuild.
	versions, err := h.raftNode.ListVersions(ctx, "kb-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range versions {
		if v.VersionID == vID && v.IndexStatus != types.IndexStatusPending {
			t.Errorf("version status after RebuildIndex = %v, want PENDING", v.IndexStatus)
		}
	}
}

func TestAdmin_RebuildIndex_StatusUpdateError(t *testing.T) {
	h := newAdminHarness()
	ctx := context.Background()
	if err := h.raftNode.ProposeCreateKB(ctx, kbMeta("kb-1", types.KBStatusActive)); err != nil {
		t.Fatal(err)
	}
	vID, err := h.raftNode.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatal(err)
	}

	// An unknown version ID makes ProposeUpdateVersionStatus fail.
	if _, err := h.svc.RebuildIndex(ctx, &pb.RebuildIndexRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       vID + 999,
	}); err == nil {
		t.Fatal("expected RebuildIndex to fail when the status update fails")
	}
}

func TestAdmin_RebuildIndex_TriggerBuildError(t *testing.T) {
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	im := &failingTriggerBuild{MockIndexManager: index.NewMockIndexManager(index.MockIndexManagerDeps{}, 16, 0)}
	svc := NewAdminService(1, rn, im, docstore.NewMockDocStore(), chunkstore.NewMockChunkStore(), w)

	ctx := context.Background()
	if err := rn.ProposeCreateKB(ctx, kbMeta("kb-1", types.KBStatusActive)); err != nil {
		t.Fatal(err)
	}
	vID, err := rn.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := svc.RebuildIndex(ctx, &pb.RebuildIndexRequest{KnowledgeBaseId: "kb-1", VersionId: vID}); err == nil {
		t.Fatal("expected RebuildIndex to fail when TriggerBuild fails")
	}
}

func TestAdmin_WarmupVersion_Success(t *testing.T) {
	h := newAdminHarness()
	ctx := context.Background()
	if err := h.raftNode.ProposeCreateKB(ctx, kbMeta("kb-1", types.KBStatusActive)); err != nil {
		t.Fatal(err)
	}
	vID, err := h.raftNode.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.raftNode.ProposeUpdateVersionStatus(ctx, vID, types.IndexStatusReady); err != nil {
		t.Fatal(err)
	}

	resp, err := h.svc.WarmupVersion(context.Background(), &pb.WarmupVersionRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       vID,
	})
	if err != nil {
		t.Fatalf("WarmupVersion: %v", err)
	}
	if !resp.Success {
		t.Error("expected success=true")
	}

	// Status must have been reset to PENDING before the warmup build.
	versions, err := h.raftNode.ListVersions(ctx, "kb-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range versions {
		if v.VersionID == vID && v.IndexStatus != types.IndexStatusPending {
			t.Errorf("version status after WarmupVersion = %v, want PENDING", v.IndexStatus)
		}
	}
}

func TestAdmin_WarmupVersion_StatusUpdateError(t *testing.T) {
	h := newAdminHarness()
	ctx := context.Background()
	if err := h.raftNode.ProposeCreateKB(ctx, kbMeta("kb-1", types.KBStatusActive)); err != nil {
		t.Fatal(err)
	}
	vID, err := h.raftNode.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatal(err)
	}

	// An unknown version ID makes ProposeUpdateVersionStatus fail.
	if _, err := h.svc.WarmupVersion(ctx, &pb.WarmupVersionRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       vID + 999,
	}); err == nil {
		t.Fatal("expected WarmupVersion to fail when the status update fails")
	}
}

func TestAdmin_WarmupVersion_TriggerBuildError(t *testing.T) {
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	im := &failingTriggerBuild{MockIndexManager: index.NewMockIndexManager(index.MockIndexManagerDeps{}, 16, 0)}
	svc := NewAdminService(1, rn, im, docstore.NewMockDocStore(), chunkstore.NewMockChunkStore(), w)

	ctx := context.Background()
	if err := rn.ProposeCreateKB(ctx, kbMeta("kb-1", types.KBStatusActive)); err != nil {
		t.Fatal(err)
	}
	vID, err := rn.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := rn.ProposeUpdateVersionStatus(ctx, vID, types.IndexStatusReady); err != nil {
		t.Fatal(err)
	}

	if _, err := svc.WarmupVersion(ctx, &pb.WarmupVersionRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       vID,
	}); err == nil {
		t.Fatal("expected WarmupVersion to fail when TriggerBuild fails")
	}
}

func kbMeta(kbID string, status types.KBStatus) types.KnowledgeBaseMeta {
	return types.KnowledgeBaseMeta{
		KBID:             kbID,
		Name:             kbID,
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig:      types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"},
		Status:           status,
	}
}
