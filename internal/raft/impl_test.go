package raft

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// newTestRaftNodeImpl constructs a single-node RaftNodeImpl wired to a
// fresh in-memory WAL and a temp-dir-backed kvraft instance, already
// running and elected leader (single-node clusters always win their own
// election — see internal/kvraft's single-node fixes).
func newTestRaftNodeImpl(t *testing.T) (*RaftNodeImpl, *wal.MockWAL) {
	t.Helper()
	w := wal.NewMockWAL()
	impl, err := NewRaftNodeImpl(Config{
		NodeID:             1,
		DataDir:            t.TempDir(),
		RaftAddr:           freeLoopbackAddrForTest(t),
		WAL:                w,
		ElectionTimeoutMin: 30 * time.Millisecond,
		ElectionTimeoutMax: 60 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRaftNodeImpl: %v", err)
	}
	t.Cleanup(impl.Stop)

	if !waitForCond(t, 2*time.Second, impl.raft.IsLeader) {
		t.Fatalf("single-node RaftNodeImpl never became leader")
	}
	return impl, w
}

func freeLoopbackAddrForTest(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func waitForCond(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestRaftNodeImpl_CreateKBAndGetKB(t *testing.T) {
	ctx := context.Background()
	impl, _ := newTestRaftNodeImpl(t)

	kb := types.KnowledgeBaseMeta{KBID: "kb1", Name: "test-kb"}
	if err := impl.ProposeCreateKB(ctx, kb); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}

	got, err := impl.GetKB(ctx, "kb1")
	if err != nil {
		t.Fatalf("GetKB: %v", err)
	}
	if got.KBID != "kb1" || got.Name != "test-kb" {
		t.Fatalf("GetKB = %+v, want KBID=kb1 Name=test-kb", got)
	}
	if got.Status != types.KBStatusActive {
		t.Fatalf("GetKB.Status = %v, want Active", got.Status)
	}
}

func TestRaftNodeImpl_GetKB_NotFound(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	_, err := impl.GetKB(context.Background(), "missing")
	if !errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Fatalf("err = %v, want ErrKnowledgeBaseNotFound", err)
	}
}

func TestRaftNodeImpl_VersionIDMonotonic(t *testing.T) {
	ctx := context.Background()
	impl, _ := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")

	v1, err := impl.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion #1: %v", err)
	}
	mustUpdateStatusImpl(t, impl, v1, types.IndexStatusReady)

	v2, err := impl.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion #2: %v", err)
	}
	if v2 <= v1 {
		t.Fatalf("version IDs not monotonic: v1=%d v2=%d", v1, v2)
	}
}

func TestRaftNodeImpl_ParentMustBeSameKB(t *testing.T) {
	ctx := context.Background()
	impl, _ := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")
	mustCreateKBImpl(t, impl, "kb2")

	v1, err := impl.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	mustUpdateStatusImpl(t, impl, v1, types.IndexStatusReady)

	_, err = impl.ProposeCreateVersion(ctx, "kb2", v1)
	if !errors.Is(err, stratumerrors.ErrInvalidParentVersion) {
		t.Fatalf("err = %v, want ErrInvalidParentVersion (cross-KB parent)", err)
	}
}

func TestRaftNodeImpl_ParentMustNotBePending(t *testing.T) {
	ctx := context.Background()
	impl, _ := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")

	v1, err := impl.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	// v1 stays PENDING.

	_, err = impl.ProposeCreateVersion(ctx, "kb1", v1)
	if !errors.Is(err, stratumerrors.ErrInvalidParentVersion) {
		t.Fatalf("err = %v, want ErrInvalidParentVersion (PENDING parent)", err)
	}
}

func TestRaftNodeImpl_ForkingAllowed(t *testing.T) {
	ctx := context.Background()
	impl, _ := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")

	v1, err := impl.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	mustUpdateStatusImpl(t, impl, v1, types.IndexStatusReady)

	v2a, err := impl.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion (fork A): %v", err)
	}
	v2b, err := impl.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion (fork B): %v", err)
	}
	if v2a == v2b {
		t.Fatalf("two forks of the same parent got the same version ID: %d", v2a)
	}

	versions, err := impl.ListVersions(ctx, "kb1")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	parentCount := 0
	for _, v := range versions {
		if v.ParentVersionID == v1 {
			parentCount++
		}
	}
	if parentCount != 2 {
		t.Fatalf("expected 2 children of v1, found %d", parentCount)
	}
}

func TestRaftNodeImpl_ProposeRemoveKBMeta_Idempotent(t *testing.T) {
	ctx := context.Background()
	impl, _ := newTestRaftNodeImpl(t)

	if err := impl.ProposeRemoveKBMeta(ctx, "ghost"); err != nil {
		t.Fatalf("ProposeRemoveKBMeta on nonexistent KB = %v, want nil (idempotent)", err)
	}

	mustCreateKBImpl(t, impl, "kb1")
	if err := impl.ProposeRemoveKBMeta(ctx, "kb1"); err != nil {
		t.Fatalf("ProposeRemoveKBMeta #1: %v", err)
	}
	if err := impl.ProposeRemoveKBMeta(ctx, "kb1"); err != nil {
		t.Fatalf("ProposeRemoveKBMeta #2 (already removed) = %v, want nil (idempotent)", err)
	}
}

func TestRaftNodeImpl_ProposeCreateVersion_WritesWALBeforeReturning(t *testing.T) {
	ctx := context.Background()
	impl, w := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")

	versionID, err := impl.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}

	pending := w.PendingVersionIDs()
	found := false
	for _, id := range pending {
		if id == versionID {
			found = true
		}
	}
	if !found {
		t.Fatalf("WAL has no VERSION_ID record for newly created version %d; PendingVersionIDs = %v", versionID, pending)
	}
}

func TestRaftNodeImpl_Rollback(t *testing.T) {
	ctx := context.Background()
	impl, _ := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")

	v1, _ := impl.ProposeCreateVersion(ctx, "kb1", 0)
	mustUpdateStatusImpl(t, impl, v1, types.IndexStatusReady)
	v2, _ := impl.ProposeCreateVersion(ctx, "kb1", v1)
	mustUpdateStatusImpl(t, impl, v2, types.IndexStatusReady)

	if err := impl.ProposeRollback(ctx, "kb1", v1); err != nil {
		t.Fatalf("ProposeRollback: %v", err)
	}
	kb, err := impl.GetKB(ctx, "kb1")
	if err != nil {
		t.Fatalf("GetKB: %v", err)
	}
	if kb.ActiveVersionID != v1 {
		t.Fatalf("ActiveVersionID = %d, want %d", kb.ActiveVersionID, v1)
	}
}

func TestRaftNodeImpl_GetClusterStatus(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	status, err := impl.GetClusterStatus(context.Background())
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if !status.HasLeader || status.MemberCount < 1 {
		t.Fatalf("GetClusterStatus = %+v, want a healthy single-node status", status)
	}
}

// TestRaftNodeImpl_SingleNodeSurvivesRestart covers the Phase 3 test node
// "单节点 leader 切换后元数据不丢失": a single-node deployment's metadata
// (knowledge bases and versions) must survive the node going away and a
// new incarnation taking over leadership (simulated here as a full
// process restart against the same data directory).
func TestRaftNodeImpl_SingleNodeSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	w := wal.NewMockWAL()

	impl1, err := NewRaftNodeImpl(Config{
		NodeID:             1,
		DataDir:            dataDir,
		RaftAddr:           freeLoopbackAddrForTest(t),
		WAL:                w,
		ElectionTimeoutMin: 30 * time.Millisecond,
		ElectionTimeoutMax: 60 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRaftNodeImpl: %v", err)
	}
	if !waitForCond(t, 2*time.Second, impl1.raft.IsLeader) {
		t.Fatalf("never became leader")
	}

	mustCreateKBImpl(t, impl1, "kb1")
	v1, err := impl1.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	mustUpdateStatusImpl(t, impl1, v1, types.IndexStatusReady)

	impl1.Stop()

	// New incarnation, same data directory and WAL: this simulates the
	// "old leader process dies, a new one takes over" scenario for a
	// single-node deployment.
	impl2, err := NewRaftNodeImpl(Config{
		NodeID:             1,
		DataDir:            dataDir,
		RaftAddr:           freeLoopbackAddrForTest(t),
		WAL:                w,
		ElectionTimeoutMin: 30 * time.Millisecond,
		ElectionTimeoutMax: 60 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRaftNodeImpl (restart): %v", err)
	}
	t.Cleanup(impl2.Stop)
	if !waitForCond(t, 2*time.Second, impl2.raft.IsLeader) {
		t.Fatalf("new incarnation never became leader")
	}

	// Becoming leader triggers an asynchronous no-op proposal (see
	// internal/kvraft's SendVoteRequest doc comment) whose commit is what
	// actually makes pre-restart entries visible; this can lag slightly
	// behind IsLeader() becoming true, so poll rather than checking once.
	var kb types.KnowledgeBaseMeta
	if !waitForCond(t, 2*time.Second, func() bool {
		var err error
		kb, err = impl2.GetKB(ctx, "kb1")
		return err == nil
	}) {
		t.Fatalf("GetKB after restart never succeeded (KB metadata lost across restart)")
	}
	if kb.KBID != "kb1" {
		t.Fatalf("KB metadata lost across restart: %+v", kb)
	}

	versions, err := impl2.ListVersions(ctx, "kb1")
	if err != nil {
		t.Fatalf("ListVersions after restart: %v", err)
	}
	found := false
	for _, v := range versions {
		if v.VersionID == v1 && v.IndexStatus == types.IndexStatusReady {
			found = true
		}
	}
	if !found {
		t.Fatalf("version %d lost or status not preserved across restart: %+v", v1, versions)
	}

	// The version counter itself must also have survived (not reset to
	// 1), or a post-restart CreateVersion could collide with a
	// pre-restart version ID.
	v2, err := impl2.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion after restart: %v", err)
	}
	if v2 <= v1 {
		t.Fatalf("post-restart version ID %d is not greater than pre-restart version ID %d (counter did not survive restart)", v2, v1)
	}
}

func mustCreateKBImpl(t *testing.T, impl *RaftNodeImpl, kbID string) {
	t.Helper()
	if err := impl.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{KBID: kbID}); err != nil {
		t.Fatalf("ProposeCreateKB(%s): %v", kbID, err)
	}
}

func mustUpdateStatusImpl(t *testing.T, impl *RaftNodeImpl, versionID int64, status types.IndexStatus) {
	t.Helper()
	if err := impl.ProposeUpdateVersionStatus(context.Background(), versionID, status); err != nil {
		t.Fatalf("ProposeUpdateVersionStatus(%d, %v): %v", versionID, status, err)
	}
}
