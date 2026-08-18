package raft

import (
	"context"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// TestRaftNodeImpl_ProposeMarkKBDeleting covers the KB lifecycle
// transition to DELETING and its error path for unknown KBs.
func TestRaftNodeImpl_ProposeMarkKBDeleting(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	if err := impl.ProposeMarkKBDeleting(ctx, "kb-1"); err != nil {
		t.Fatalf("ProposeMarkKBDeleting: %v", err)
	}
	kb, err := impl.GetKB(ctx, "kb-1")
	if err != nil {
		t.Fatalf("GetKB: %v", err)
	}
	if kb.Status != types.KBStatusDeleting {
		t.Errorf("status = %v, want DELETING", kb.Status)
	}

	if err := impl.ProposeMarkKBDeleting(ctx, "nope"); err == nil {
		t.Error("expected error marking an unknown KB as deleting")
	}
}

// TestRaftNodeImpl_ProposeMarkKBDeleteFailed covers the transition to
// DELETE_FAILED surfaced by GetSystemStatus.
func TestRaftNodeImpl_ProposeMarkKBDeleteFailed(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	if err := impl.ProposeMarkKBDeleteFailed(ctx, "kb-1"); err != nil {
		t.Fatalf("ProposeMarkKBDeleteFailed: %v", err)
	}
	kb, _ := impl.GetKB(ctx, "kb-1")
	if kb.Status != types.KBStatusDeleteFailed {
		t.Errorf("status = %v, want DELETE_FAILED", kb.Status)
	}

	if err := impl.ProposeMarkKBDeleteFailed(ctx, "nope"); err == nil {
		t.Error("expected error marking an unknown KB delete-failed")
	}
}

// TestRaftNodeImpl_ListKnowledgeBases verifies the full-KB scan used by
// the console and GetSystemStatus, including the empty case.
func TestRaftNodeImpl_ListKnowledgeBases(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	kbs, err := impl.ListKnowledgeBases(ctx)
	if err != nil {
		t.Fatalf("ListKnowledgeBases on empty cluster: %v", err)
	}
	if len(kbs) != 0 {
		t.Fatalf("expected 0 KBs initially, got %d", len(kbs))
	}

	for _, id := range []string{"kb-a", "kb-b", "kb-c"} {
		if err := impl.ProposeCreateKB(ctx, testKB(id)); err != nil {
			t.Fatalf("ProposeCreateKB(%s): %v", id, err)
		}
	}

	kbs, err = impl.ListKnowledgeBases(ctx)
	if err != nil {
		t.Fatalf("ListKnowledgeBases: %v", err)
	}
	if len(kbs) != 3 {
		t.Fatalf("expected 3 KBs, got %d", len(kbs))
	}
	seen := map[string]bool{}
	for _, kb := range kbs {
		seen[kb.KBID] = true
	}
	for _, id := range []string{"kb-a", "kb-b", "kb-c"} {
		if !seen[id] {
			t.Errorf("ListKnowledgeBases missing %s", id)
		}
	}
}

// TestRaftNodeImpl_ProposeRemoveKBMeta_And_ListVersions verifies the
// delete flow's metadata removal clears versions, and that subsequent
// reads fail with the documented errors.
func TestRaftNodeImpl_ProposeRemoveKBMeta_And_ListVersions(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	vID, err := impl.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}

	versions, err := impl.ListVersions(ctx, "kb-1")
	if err != nil || len(versions) != 1 || versions[0].VersionID != vID {
		t.Fatalf("ListVersions = %v, %v; want [v%d]", versions, err, vID)
	}
	if _, err := impl.ListVersions(ctx, "nope"); !isKBNotFound(err) {
		t.Errorf("ListVersions on unknown KB: got %v, want ErrKnowledgeBaseNotFound", err)
	}

	if err := impl.ProposeRemoveKBMeta(ctx, "kb-1"); err != nil {
		t.Fatalf("ProposeRemoveKBMeta: %v", err)
	}
	if _, err := impl.GetKB(ctx, "kb-1"); !isKBNotFound(err) {
		t.Errorf("GetKB after removal: got %v", err)
	}
	// Idempotent second removal (crash-recovery re-execution).
	if err := impl.ProposeRemoveKBMeta(ctx, "kb-1"); err != nil {
		t.Errorf("second ProposeRemoveKBMeta must be idempotent, got %v", err)
	}
}

// TestRaftNodeImpl_SetOnVersionCreated_NoopBeforeStart ensures the
// callback can be registered and is not invoked for proposals made by
// this node (the proposer does its own storage writes inline).
func TestRaftNodeImpl_SetOnVersionCreated_NoopForProposer(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	called := make(chan struct{}, 1)
	impl.SetOnVersionCreated(func(kbID string, versionID int64) {
		called <- struct{}{}
	})

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	if _, err := impl.ProposeCreateVersion(ctx, "kb-1", 0); err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}

	select {
	case <-called:
		t.Fatal("onVersionCreated must not fire on the proposing node")
	default:
	}
}

func isKBNotFound(err error) bool {
	if err == nil {
		return false
	}
	return err == stratumerrors.ErrKnowledgeBaseNotFound
}

// TestRaftNodeImpl_LocalSnapshotCompaction verifies the locally-triggered
// snapshot path (handleSnapshotMsg with nil SnapshotData): once the log
// grows past MaxLogLength, the state machine serializes itself, kvraft
// compacts, and the node keeps serving correctly.
func TestRaftNodeImpl_LocalSnapshotCompaction(t *testing.T) {
	w := wal.NewMockWAL()
	impl, err := NewRaftNodeImpl(Config{
		NodeID:             1,
		DataDir:            t.TempDir(),
		RaftAddr:           freeLoopbackAddrForTest(t),
		WAL:                w,
		MaxLogLength:       8,
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

	ctx := context.Background()
	const n = 40 // comfortably past MaxLogLength=8 (each KB = 1 log entry)
	for i := 0; i < n; i++ {
		kb := testKB(kbIDFor(i))
		if err := impl.ProposeCreateKB(ctx, kb); err != nil {
			t.Fatalf("ProposeCreateKB #%d: %v", i, err)
		}
	}

	// Give the compaction / snapshot round-trip time to complete.
	if !waitForCond(t, 5*time.Second, func() bool {
		kbs, err := impl.ListKnowledgeBases(ctx)
		return err == nil && len(kbs) == n
	}) {
		t.Fatalf("only %d KBs visible after snapshot compaction", len(mustList(t, impl)))
	}

	// The state machine must still accept new writes after compaction.
	extra := testKB("kb-post-snapshot")
	if err := impl.ProposeCreateKB(ctx, extra); err != nil {
		t.Fatalf("propose after snapshot: %v", err)
	}
	if kb, err := impl.GetKB(ctx, "kb-post-snapshot"); err != nil || kb.KBID != "kb-post-snapshot" {
		t.Errorf("GetKB after snapshot = %+v, %v", kb, err)
	}
}

func mustList(t *testing.T, impl *RaftNodeImpl) []types.KnowledgeBaseMeta {
	t.Helper()
	kbs, err := impl.ListKnowledgeBases(context.Background())
	if err != nil {
		t.Fatalf("ListKnowledgeBases: %v", err)
	}
	return kbs
}

func kbIDFor(i int) string {
	return "kb-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
}

// TestRaftNodeImpl_ProposeUpdateVersionSummary covers committing the
// version's document-ID set digest and the error path for unknown versions.
func TestRaftNodeImpl_ProposeUpdateVersionSummary(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()
	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	vID, err := impl.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}

	if err := impl.ProposeUpdateVersionSummary(ctx, vID, "digest-123"); err != nil {
		t.Fatalf("ProposeUpdateVersionSummary: %v", err)
	}
	versions, err := impl.ListVersions(ctx, "kb-1")
	if err != nil || len(versions) != 1 {
		t.Fatalf("ListVersions = %v, %v", versions, err)
	}
	if versions[0].DocIDSetHash != "digest-123" {
		t.Errorf("DocIDSetHash = %q, want digest-123", versions[0].DocIDSetHash)
	}

	if err := impl.ProposeUpdateVersionSummary(ctx, vID+999, "x"); !isKBNotFound(err) && err == nil {
		t.Errorf("expected error for unknown version, got %v", err)
	}
}
