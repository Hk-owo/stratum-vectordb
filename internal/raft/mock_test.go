package raft

import (
	"context"
	"errors"
	"testing"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"stratum/internal/wal"
)

func newTestRaftNode() (*MockRaftNode, *wal.MockWAL) {
	w := wal.NewMockWAL()
	return NewMockRaftNode(w), w
}

func TestMockRaftNode_CreateKBAndGetKB(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRaftNode()

	kb := types.KnowledgeBaseMeta{KBID: "kb1", Name: "test"}
	if err := r.ProposeCreateKB(ctx, kb); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	got, err := r.GetKB(ctx, "kb1")
	if err != nil {
		t.Fatalf("GetKB: %v", err)
	}
	if got.KBID != "kb1" || got.Status != types.KBStatusActive {
		t.Fatalf("GetKB = %+v, want KBID=kb1 Status=Active", got)
	}
}

func TestMockRaftNode_GetKB_NotFound(t *testing.T) {
	r, _ := newTestRaftNode()
	_, err := r.GetKB(context.Background(), "missing")
	if !errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Fatalf("err = %v, want ErrKnowledgeBaseNotFound", err)
	}
}

func TestMockRaftNode_VersionIDMonotonic(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")

	v1, err := r.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion #1: %v", err)
	}
	// Mark v1 READY so it's a valid parent for the next version.
	mustUpdateStatus(t, r, v1, types.IndexStatusReady)

	v2, err := r.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion #2: %v", err)
	}
	if v2 <= v1 {
		t.Fatalf("version IDs not monotonic: v1=%d v2=%d", v1, v2)
	}
}

func TestMockRaftNode_ParentMustBeSameKB(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")
	mustCreateKB(t, r, "kb2")

	v1, err := r.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	mustUpdateStatus(t, r, v1, types.IndexStatusReady)

	_, err = r.ProposeCreateVersion(ctx, "kb2", v1)
	if !errors.Is(err, stratumerrors.ErrInvalidParentVersion) {
		t.Fatalf("err = %v, want ErrInvalidParentVersion (cross-KB parent)", err)
	}
}

func TestMockRaftNode_ParentMustNotBePending(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")

	v1, err := r.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	// v1 stays PENDING (not marked READY).

	_, err = r.ProposeCreateVersion(ctx, "kb1", v1)
	if !errors.Is(err, stratumerrors.ErrInvalidParentVersion) {
		t.Fatalf("err = %v, want ErrInvalidParentVersion (PENDING parent)", err)
	}
}

func TestMockRaftNode_ForkingAllowed(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")

	v1, err := r.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	mustUpdateStatus(t, r, v1, types.IndexStatusReady)

	v2a, err := r.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion (fork A): %v", err)
	}
	v2b, err := r.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion (fork B): %v", err)
	}
	if v2a == v2b {
		t.Fatalf("two forks of the same parent got the same version ID: %d", v2a)
	}

	versions, err := r.ListVersions(ctx, "kb1")
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

func TestMockRaftNode_ProposeCreateVersion_WritesWALBeforeStateMachine(t *testing.T) {
	ctx := context.Background()
	r, w := newTestRaftNode()
	mustCreateKB(t, r, "kb1")

	versionID, err := r.ProposeCreateVersion(ctx, "kb1", 0)
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

	if _, ok := r.GetVersion(versionID); !ok {
		t.Fatalf("state machine has no version %d after ProposeCreateVersion", versionID)
	}
}

func TestMockRaftNode_ProposeRemoveKBMeta_Idempotent(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRaftNode()

	// Knowledge base never existed: must still return success.
	if err := r.ProposeRemoveKBMeta(ctx, "ghost"); err != nil {
		t.Fatalf("ProposeRemoveKBMeta on nonexistent KB = %v, want nil (idempotent)", err)
	}

	mustCreateKB(t, r, "kb1")
	if err := r.ProposeRemoveKBMeta(ctx, "kb1"); err != nil {
		t.Fatalf("ProposeRemoveKBMeta #1: %v", err)
	}
	// Second call after removal must also succeed.
	if err := r.ProposeRemoveKBMeta(ctx, "kb1"); err != nil {
		t.Fatalf("ProposeRemoveKBMeta #2 (already removed) = %v, want nil (idempotent)", err)
	}
}

func TestMockRaftNode_Rollback(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")

	v1, _ := r.ProposeCreateVersion(ctx, "kb1", 0)
	mustUpdateStatus(t, r, v1, types.IndexStatusReady)
	v2, _ := r.ProposeCreateVersion(ctx, "kb1", v1)
	mustUpdateStatus(t, r, v2, types.IndexStatusReady)

	if err := r.ProposeRollback(ctx, "kb1", v1); err != nil {
		t.Fatalf("ProposeRollback: %v", err)
	}
	kb, err := r.GetKB(ctx, "kb1")
	if err != nil {
		t.Fatalf("GetKB: %v", err)
	}
	if kb.ActiveVersionID != v1 {
		t.Fatalf("ActiveVersionID = %d, want %d", kb.ActiveVersionID, v1)
	}
}

func TestMockRaftNode_GetClusterStatus(t *testing.T) {
	r, _ := newTestRaftNode()
	status, err := r.GetClusterStatus(context.Background())
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if !status.HasLeader || status.MemberCount < 1 {
		t.Fatalf("GetClusterStatus = %+v, want a healthy single-node status", status)
	}
}

func mustCreateKB(t *testing.T, r *MockRaftNode, kbID string) {
	t.Helper()
	if err := r.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{KBID: kbID}); err != nil {
		t.Fatalf("ProposeCreateKB(%s): %v", kbID, err)
	}
}

func mustUpdateStatus(t *testing.T, r *MockRaftNode, versionID int64, status types.IndexStatus) {
	t.Helper()
	if err := r.ProposeUpdateVersionStatus(context.Background(), versionID, status); err != nil {
		t.Fatalf("ProposeUpdateVersionStatus(%d, %v): %v", versionID, status, err)
	}
}
