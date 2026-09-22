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
	fresh.tombstones["kb-1"] = append(fresh.tombstones["kb-1"], int64(1)) // must not panic
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

// TestRaftNodeImpl_PruneTombstones_UsesTheVersionWatermark pins the pruning rule.
//
// The watermark is a VERSION, and it applies to ONE knowledge base. That shape is the
// point: the caller derives it from the required replicas' DATA cursors
// (docs/known-gaps.md §B), because the nodes that still ask about a version are the
// ones BEHIND it — a log position would answer a different question, and a global
// watermark could drop rows nothing had justified.
func TestRaftNodeImpl_PruneTombstones_UsesTheVersionWatermark(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	if err := impl.ProposeCreateKB(ctx, testKB("kb-2")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	ids := createReadyChain(t, impl, ctx, "kb-1", 3)
	other := createReadyChain(t, impl, ctx, "kb-2", 2)

	deleteOne := func(kbID string, versionID int64) {
		t.Helper()
		if _, err := impl.ProposeMarkVersionDeleting(ctx, kbID, versionID, types.VersionDeleteSingle); err != nil {
			t.Fatalf("delete %s/%d: %v", kbID, versionID, err)
		}
		if err := impl.ProposeRemoveVersionMeta(ctx, kbID, versionID); err != nil {
			t.Fatalf("remove %s/%d: %v", kbID, versionID, err)
		}
	}

	deleteOne("kb-1", ids[0])
	deleteOne("kb-1", ids[2])
	deleteOne("kb-2", other[0])

	// The watermark covers ids[0] only, so kb-1 keeps the later removal and kb-2 is
	// not touched at all.
	if !impl.HasTombstonesAtOrBelow("kb-1", ids[0]) {
		t.Errorf("HasTombstonesAtOrBelow(kb-1, %d) = false; the row for it is right there", ids[0])
	}
	if impl.HasTombstonesAtOrBelow("kb-2", 0) {
		t.Errorf("HasTombstonesAtOrBelow(kb-2, 0) = true; that KB has no removal that low")
	}
	if err := impl.ProposePruneTombstones(ctx, "kb-1", ids[0]); err != nil {
		t.Fatalf("ProposePruneTombstones: %v", err)
	}
	got, err := impl.DeletionsInRange(ctx, "kb-1", 0, 1<<40)
	if err != nil {
		t.Fatalf("DeletionsInRange: %v", err)
	}
	if len(got) != 1 || got[0] != ids[2] {
		t.Fatalf("after pruning: DeletionsInRange = %v, want only [%d]", got, ids[2])
	}
	otherRows, err := impl.DeletionsInRange(ctx, "kb-2", 0, 1<<40)
	if err != nil {
		t.Fatalf("DeletionsInRange(kb-2): %v", err)
	}
	if len(otherRows) != 1 || otherRows[0] != other[0] {
		t.Errorf("pruning kb-1 changed kb-2: %v, want [%d]", otherRows, other[0])
	}

	// Idempotent at the same watermark; a higher one clears the rest.
	if err := impl.ProposePruneTombstones(ctx, "kb-1", ids[0]); err != nil {
		t.Fatalf("replay ProposePruneTombstones: %v", err)
	}
	if got, _ := impl.DeletionsInRange(ctx, "kb-1", 0, 1<<40); len(got) != 1 {
		t.Errorf("after a replayed prune: DeletionsInRange = %v, want one row", got)
	}
	if err := impl.ProposePruneTombstones(ctx, "kb-1", ids[2]); err != nil {
		t.Fatalf("ProposePruneTombstones (higher): %v", err)
	}
	if got, _ := impl.DeletionsInRange(ctx, "kb-1", 0, 1<<40); len(got) != 0 {
		t.Errorf("after pruning through the last removal: DeletionsInRange = %v, want none", got)
	}
	if impl.HasTombstonesAtOrBelow("kb-1", ids[2]) {
		t.Errorf("HasTombstonesAtOrBelow(kb-1, %d) = true after the rows went", ids[2])
	}
}

// TestRaftNodeImpl_PruneTombstones_NewLowRowIsDroppedImmediately pins what pruning
// does to a tombstone that is BRAND NEW while its version is OLD — the ordinary shape
// of "delete a historical version after the chain has moved on".
//
// It is pinned because the pruning rule has no notion of a row's AGE: the watermark is
// a version, the rule is "id <= watermark", so a row recorded a moment ago is dropped
// by the next pass just like one recorded a month ago. That is not by itself a defect —
// a watermark that high means every required replica has moved past that version, and
// the row is the answer to a question none of them will ask again.
//
// What is easy to assume closed, and is not, is the other side: the judgement "this
// version was deleted" dies with the row. The readers of that judgement which are NOT
// required replicas are exactly the ones the watermark never spoke for — a leftover a
// late push wrote back on a slow replica (push_after_cleanup_test.go), or a node that
// was offline when the cleanup broadcast went out — and once the row is gone nothing
// can name their leftovers until the knowledge base itself is dropped
// (docs/known-gaps.md §B: "the window is the bound").
//
// Whoever adds a protection period for rows younger than the watermark should make
// this case keep the row, and say so here, rather than delete the case.
func TestRaftNodeImpl_PruneTombstones_NewLowRowIsDroppedImmediately(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	// Four linked versions: the tail stands for "every required replica's cursor has
	// reached at least here", which is what the pruning watermark is derived from.
	ids := createReadyChain(t, impl, ctx, "kb-1", 4)
	historic := ids[1]
	tail := ids[len(ids)-1]
	if !(historic < tail) {
		t.Fatalf("fixture is wrong: the version to remove (%d) is not below the tail (%d)", historic, tail)
	}

	// Record the removal: the row is NEW, its version is far below the tail.
	if _, err := impl.ProposeMarkVersionDeleting(ctx, "kb-1", historic, types.VersionDeleteSingle); err != nil {
		t.Fatalf("ProposeMarkVersionDeleting: %v", err)
	}
	if err := impl.ProposeRemoveVersionMeta(ctx, "kb-1", historic); err != nil {
		t.Fatalf("ProposeRemoveVersionMeta: %v", err)
	}

	// While the row is there, "gone" is distinguishable from "never existed" — this is
	// the pair DeletedVersionLeftovers and the backfill judge from.
	got, err := impl.DeletionsInRange(ctx, "kb-1", historic-1, historic)
	if err != nil {
		t.Fatalf("DeletionsInRange: %v", err)
	}
	if len(got) != 1 || got[0] != historic {
		t.Fatalf("DeletionsInRange = %v, want [%d]: the removal must be recorded", got, historic)
	}

	// One pruning pass at the watermark the replicas have already reached — what the
	// next tick of runTombstonePruning proposes — takes the brand-new row with it, and
	// the pre-check is what makes that pass happen rather than being skipped as a no-op.
	if !impl.HasTombstonesAtOrBelow("kb-1", tail) {
		t.Fatalf("HasTombstonesAtOrBelow(kb-1, %d) = false; the row for %d is right there", tail, historic)
	}
	if err := impl.ProposePruneTombstones(ctx, "kb-1", tail); err != nil {
		t.Fatalf("ProposePruneTombstones: %v", err)
	}
	got, err = impl.DeletionsInRange(ctx, "kb-1", historic-1, historic)
	if err != nil {
		t.Fatalf("DeletionsInRange after pruning: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("DeletionsInRange after pruning = %v, want none: the row had no protection period — "+
			"if this now keeps the row, pruning grew an age rule and this assertion should be flipped", got)
	}
}

// TestRaftNodeImpl_PruneTombstones_RecordOrderDoesNotProtectALowRow pins that the pruning
// rule reads ONE dimension: the version number. Where a removal sits in the record order
// plays no part, so a row recorded a moment ago is treated exactly like one recorded long
// before it — including when its version is BELOW the watermark, which is the ordinary
// shape of "delete a historical version after the chain has moved on".
//
// The fixture makes the two orders disagree on purpose: the row with the HIGH version is
// recorded FIRST, the row with the LOW version SECOND. Pruning at the low version's own
// value therefore keeps the older (higher) row and drops the newer (lower) one — the
// opposite of what any order-aware rule would do.
//
// This is the case a positional guard changes, and it changes it only for a while. "Keep
// the last N rows and prune only the head of the queue" postpones this outcome: once N more
// removals push the low row out of that window, the version test decides again and the row
// goes. Position pins a WINDOW; it does not protect a ROW — which is why the case is pinned
// rather than assumed closed. Whoever implements a positional rule (a tail window, or an id
// with per-replica progress) should FLIP this assertion — the low row must then survive —
// rather than delete the case.
func TestRaftNodeImpl_PruneTombstones_RecordOrderDoesNotProtectALowRow(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	ids := createReadyChain(t, impl, ctx, "kb-1", 10)
	vHigh, vLow := ids[8], ids[2]

	remove := func(versionID int64) {
		t.Helper()
		if _, err := impl.ProposeMarkVersionDeleting(ctx, "kb-1", versionID, types.VersionDeleteSingle); err != nil {
			t.Fatalf("ProposeMarkVersionDeleting(%d): %v", versionID, err)
		}
		if err := impl.ProposeRemoveVersionMeta(ctx, "kb-1", versionID); err != nil {
			t.Fatalf("ProposeRemoveVersionMeta(%d): %v", versionID, err)
		}
	}

	// Recorded in the opposite order to their versions.
	remove(vHigh)
	remove(vLow)
	if got := impl.sm.tombstones["kb-1"]; len(got) != 2 || got[0] != vHigh || got[1] != vLow {
		t.Fatalf("tombstones = %v, want [%d %d]: the later removal must be the later entry",
			got, vHigh, vLow)
	}

	// Prune at the LOW version's own value: the version test keeps everything above it and
	// drops everything at or below it.
	if err := impl.ProposePruneTombstones(ctx, "kb-1", vLow); err != nil {
		t.Fatalf("ProposePruneTombstones: %v", err)
	}
	got, err := impl.DeletionsInRange(ctx, "kb-1", 0, 1<<40)
	if err != nil {
		t.Fatalf("DeletionsInRange: %v", err)
	}
	if len(got) != 1 || got[0] != vHigh {
		t.Fatalf("DeletionsInRange = %v, want only [%d]: the row recorded FIRST (version %d, above "+
			"the watermark) must survive while the one recorded LAST (version %d, at the "+
			"watermark) goes — under the current rule, record order decides nothing",
			got, vHigh, vHigh, vLow)
	}
}
