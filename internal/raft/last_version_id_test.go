package raft

import (
	"context"
	"errors"
	"testing"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// TestRaftNodeImpl_LastVersionID_TracksTheSurvivingTail pins the extreme-value read:
// the control node answers in O(1) (the last element of the allocation-ordered slice),
// so what has to be true is that the answer is the highest SURVIVING id — including
// after a deletion splices the slice.
func TestRaftNodeImpl_LastVersionID_TracksTheSurvivingTail(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	if _, err := impl.LastVersionID(ctx, "kb-missing"); !errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Fatalf("LastVersionID(unknown kb) = %v, want ErrKnowledgeBaseNotFound", err)
	}

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	if tail, err := impl.LastVersionID(ctx, "kb-1"); err != nil || tail != 0 {
		t.Fatalf("LastVersionID(empty kb) = (%d, %v), want (0, nil)", tail, err)
	}

	// The chain is linear and a parent must not be PENDING, so each link is marked
	// READY before the next is created.
	var ids []int64
	for i := 0; i < 4; i++ {
		parent := int64(0)
		if i > 0 {
			parent = ids[i-1]
		}
		id, err := impl.ProposeCreateVersion(ctx, "kb-1", parent)
		if err != nil {
			t.Fatalf("create #%d: %v", i, err)
		}
		if err := impl.ProposeUpdateVersionStatus(ctx, id, types.IndexStatusReady, 0); err != nil {
			t.Fatalf("mark READY #%d: %v", i, err)
		}
		ids = append(ids, id)
	}
	if tail, err := impl.LastVersionID(ctx, "kb-1"); err != nil || tail != ids[3] {
		t.Fatalf("LastVersionID = (%d, %v), want (%d, nil)", tail, err, ids[3])
	}

	// Deleting a MIDDLE version splices it out of the ordered slice; the tail must
	// not move. (This is the case that would break an implementation that answered
	// with the largest id ever allocated instead of the largest surviving one.)
	if _, err := impl.ProposeMarkVersionDeleting(ctx, "kb-1", ids[1], types.VersionDeleteSingle); err != nil {
		t.Fatalf("delete middle: %v", err)
	}
	if err := impl.ProposeRemoveVersionMeta(ctx, "kb-1", ids[1]); err != nil {
		t.Fatalf("remove middle meta: %v", err)
	}
	if tail, err := impl.LastVersionID(ctx, "kb-1"); err != nil || tail != ids[3] {
		t.Errorf("after deleting a middle version: LastVersionID = (%d, %v), want (%d, nil)", tail, err, ids[3])
	}

	// Deleting the LAST surviving version must move the answer down to the new tail.
	if _, err := impl.ProposeMarkVersionDeleting(ctx, "kb-1", ids[3], types.VersionDeleteSingle); err != nil {
		t.Fatalf("delete last: %v", err)
	}
	if err := impl.ProposeRemoveVersionMeta(ctx, "kb-1", ids[3]); err != nil {
		t.Fatalf("remove last meta: %v", err)
	}
	if tail, err := impl.LastVersionID(ctx, "kb-1"); err != nil || tail != ids[2] {
		t.Errorf("after deleting the tail: LastVersionID = (%d, %v), want (%d, nil)", tail, err, ids[2])
	}
}
