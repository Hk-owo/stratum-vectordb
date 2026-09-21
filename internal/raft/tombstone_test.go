package raft

import (
	"context"
	"errors"
	"testing"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// createReadyChain creates n linked versions and marks each READY, so later
// versions can be created on top and deletions pass their admission checks.
func createReadyChain(t *testing.T, impl *RaftNodeImpl, ctx context.Context, kbID string, n int) []int64 {
	t.Helper()
	var ids []int64
	for i := 0; i < n; i++ {
		parent := int64(0)
		if i > 0 {
			parent = ids[i-1]
		}
		id, err := impl.ProposeCreateVersion(ctx, kbID, parent)
		if err != nil {
			t.Fatalf("ProposeCreateVersion #%d: %v", i, err)
		}
		if err := impl.ProposeUpdateVersionStatus(ctx, id, types.IndexStatusReady, 0); err != nil {
			t.Fatalf("ProposeUpdateVersionStatus #%d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// TestRaftNodeImpl_TombstonesRecordDeletedVersions pins the "confirmed deleted"
// answer. After a version's metadata is removed, DeletionsInRange says so while
// the live chain no longer mentions it — that pair is what lets a reconciler tell
// "gone, reclaim it" apart from "not found" (docs/known-gaps.md §B/§C).
func TestRaftNodeImpl_TombstonesRecordDeletedVersions(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	ids := createReadyChain(t, impl, ctx, "kb-1", 4)

	const everything = int64(1) << 40

	// Nothing deleted yet.
	if got, err := impl.DeletionsInRange(ctx, "kb-1", 0, everything); err != nil || len(got) != 0 {
		t.Fatalf("DeletionsInRange on a fresh KB = (%v, %v), want empty", got, err)
	}

	// Delete a MIDDLE version.
	if _, err := impl.ProposeMarkVersionDeleting(ctx, "kb-1", ids[1], types.VersionDeleteSingle); err != nil {
		t.Fatalf("ProposeMarkVersionDeleting: %v", err)
	}
	if err := impl.ProposeRemoveVersionMeta(ctx, "kb-1", ids[1]); err != nil {
		t.Fatalf("ProposeRemoveVersionMeta: %v", err)
	}

	got, err := impl.DeletionsInRange(ctx, "kb-1", 0, everything)
	if err != nil {
		t.Fatalf("DeletionsInRange: %v", err)
	}
	if len(got) != 1 || got[0] != ids[1] {
		t.Fatalf("DeletionsInRange = %v, want [%d]", got, ids[1])
	}

	// The live chain must not carry it: the tombstone answers a question the
	// version list cannot answer any more.
	if _, err := impl.GetVersion(ctx, "kb-1", ids[1]); !errors.Is(err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("GetVersion(deleted) = %v, want ErrVersionNotFound", err)
	}
	live, err := impl.ListVersions(ctx, "kb-1")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	for _, v := range live {
		if v.VersionID == ids[1] {
			t.Errorf("ListVersions still carries the removed version %d", v.VersionID)
		}
	}

	// A replayed removal must not record a second tombstone (the apply returns
	// early once the version is gone, which is what makes the entry idempotent).
	if err := impl.ProposeRemoveVersionMeta(ctx, "kb-1", ids[1]); err != nil {
		t.Fatalf("replay ProposeRemoveVersionMeta: %v", err)
	}
	if got, _ := impl.DeletionsInRange(ctx, "kb-1", 0, everything); len(got) != 1 {
		t.Errorf("after a replayed removal: DeletionsInRange = %v, want exactly one row", got)
	}

	// The range is (fromExclusive, toInclusive]: the deleted id is the bound, so
	// an exclusive lower bound of ids[1] must exclude it.
	if got, _ := impl.DeletionsInRange(ctx, "kb-1", ids[1], everything); len(got) != 0 {
		t.Errorf("DeletionsInRange(%d, ...] = %v, want empty (the from side is exclusive)", ids[1], got)
	}

	// "This knowledge base does not exist" and "nothing was deleted in it" call
	// for different actions, so the first is an error rather than an empty list.
	if _, err := impl.DeletionsInRange(ctx, "kb-missing", 0, everything); !errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Errorf("DeletionsInRange(unknown kb) = %v, want ErrKnowledgeBaseNotFound", err)
	}
}

// TestRaftNodeImpl_TombstonesSurviveASnapshotRoundTrip is the case that matters
// most. A node that was down when a removal was applied only ever sees the state
// through a snapshot, so a tombstone that does not travel with the snapshot
// disappears for exactly the reader that has no other way to learn it — turning
// "confirmed deleted" back into "not found".
func TestRaftNodeImpl_TombstonesSurviveASnapshotRoundTrip(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	ids := createReadyChain(t, impl, ctx, "kb-1", 2)
	if _, err := impl.ProposeMarkVersionDeleting(ctx, "kb-1", ids[0], types.VersionDeleteSingle); err != nil {
		t.Fatalf("ProposeMarkVersionDeleting: %v", err)
	}
	if err := impl.ProposeRemoveVersionMeta(ctx, "kb-1", ids[0]); err != nil {
		t.Fatalf("ProposeRemoveVersionMeta: %v", err)
	}

	data, err := impl.sm.serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	restored := newStateMachine()
	if err := restored.restore(data); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := restored.tombstones["kb-1"]; len(got) != 1 || got[0] != ids[0] {
		t.Errorf("tombstones after a round trip = %v, want [%d]", got, ids[0])
	}

	// A snapshot written before tombstones existed decodes to nil; restore has to
	// leave a usable map, or the first removal after a restore would write into a
	// nil map (the same handling versionsByRequest gets).
	legacy, err := encodeSnapshot(snapshotState{
		KBs: map[string]types.KnowledgeBaseMeta{"kb-1": {KBID: "kb-1"}},
	})
	if err != nil {
		t.Fatalf("encodeSnapshot(legacy): %v", err)
	}
	fresh := newStateMachine()
	if err := fresh.restore(legacy); err != nil {
		t.Fatalf("restore(legacy): %v", err)
	}
	if fresh.tombstones == nil {
		t.Fatal("restoring a pre-tombstone snapshot left tombstones nil")
	}
	fresh.tombstones["kb-1"] = append(fresh.tombstones["kb-1"], 1) // must not panic
}

// TestRaftNodeImpl_TombstonesAreDroppedWithTheKnowledgeBase: a tombstone exists so
// a reader can tell "deleted" from "absent" WITHIN a knowledge base. Once the KB is
// gone there is nothing to reconcile against, and keeping the rows would leak them
// into every later snapshot.
func TestRaftNodeImpl_TombstonesAreDroppedWithTheKnowledgeBase(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	ids := createReadyChain(t, impl, ctx, "kb-1", 2)
	if _, err := impl.ProposeMarkVersionDeleting(ctx, "kb-1", ids[0], types.VersionDeleteSingle); err != nil {
		t.Fatalf("ProposeMarkVersionDeleting: %v", err)
	}
	if err := impl.ProposeRemoveVersionMeta(ctx, "kb-1", ids[0]); err != nil {
		t.Fatalf("ProposeRemoveVersionMeta: %v", err)
	}
	if got := impl.sm.tombstones["kb-1"]; len(got) != 1 {
		t.Fatalf("precondition: tombstones = %v, want one row", got)
	}

	if err := impl.ProposeRemoveKBMeta(ctx, "kb-1"); err != nil {
		t.Fatalf("ProposeRemoveKBMeta: %v", err)
	}
	if got := impl.sm.tombstones["kb-1"]; len(got) != 0 {
		t.Errorf("tombstones after removing the knowledge base = %v, want none", got)
	}
}
