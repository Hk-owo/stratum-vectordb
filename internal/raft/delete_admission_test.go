package raft

import (
	"errors"
	"testing"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// deleteBlockedByPending decides whether DeleteVersion must refuse a version, and
// it reads the DATA side on purpose: the question is "may this version's storage
// writes still be in flight?", which a data-side terminal verdict answers with a
// definite no (Stratum_设计文档v13.md §10.1b).
func TestDeleteBlockedByPending_DecisionTable(t *testing.T) {
	cases := []struct {
		name string
		v    types.VersionMeta
		want bool
	}{
		{"data still writing", types.VersionMeta{IndexStatus: types.IndexStatusPending, DataStatus: types.DataStatusPending}, true},
		{"data durable, index building", types.VersionMeta{IndexStatus: types.IndexStatusPending, DataStatus: types.DataStatusDurable}, true},
		{
			// The case this rule exists for: the version will never finish
			// writing, and §10.6 has already reclaimed what had landed.
			"data terminal", types.VersionMeta{IndexStatus: types.IndexStatusPending, DataStatus: types.DataStatusFailedPermanent}, false,
		},
		{"index ready", types.VersionMeta{IndexStatus: types.IndexStatusReady, DataStatus: types.DataStatusDurable}, false},
		{"index failed (retryable)", types.VersionMeta{IndexStatus: types.IndexStatusFailed, DataStatus: types.DataStatusDurable}, false},
		{"index terminal", types.VersionMeta{IndexStatus: types.IndexStatusFailedPermanent, DataStatus: types.DataStatusPending}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := deleteBlockedByPending(tc.v); got != tc.want {
				t.Errorf("deleteBlockedByPending(%+v) = %v, want %v", tc.v, got, tc.want)
			}
		})
	}
}

// A version whose DATA side reached the terminal verdict must be deletable. Before
// this, both exits refused it — DeleteVersion read IndexStatus (still PENDING, since
// only one side is settled) and DiscardVersion demands a PENDING DATA side — so a
// data-side verdict stranded the version in the chain with no API able to remove it.
func TestStateMachine_Apply_MarkVersionDeleting_DataTerminalVersionIsDeletable(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := proposeCtx(t)
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	// v1 is the active version, so the delete below cannot be refused for that.
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v1.VersionID}, w, zap.NewNop())

	// v2: the write never landed, so the control layer recorded the terminal verdict
	// on the DATA side and its index side is untouched (still PENDING).
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	res := sm.apply(ctx, command{
		Type: cmdMarkVersionFailedPermanent, KBID: "kb-1", VersionID: v2.VersionID,
		FailureSide: types.FailureSideData, FailureReason: "replication fell short", FailureCount: 5,
	}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("MarkVersionFailedPermanent: %v", res.Err)
	}
	got := sm.versions[v2.VersionID]
	if got.IndexStatus != types.IndexStatusPending || got.DataStatus != types.DataStatusFailedPermanent {
		t.Fatalf("precondition: want a data-side verdict with the index side untouched, got index=%v data=%v",
			got.IndexStatus, got.DataStatus)
	}

	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v2.VersionID, Mode: types.VersionDeleteSubtree}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("MarkVersionDeleting on a data-terminal version: %v (this is the dead end §10.1 left open)", res.Err)
	}
	if !sm.versions[v2.VersionID].Deleting {
		t.Error("v2 should be marked Deleting")
	}
}

// The other half of the same rule: a survivor whose parent is being removed is
// refused only while it could still be WRITING. A survivor that reached the data
// side's terminal verdict has no write left to finish, and the reason the guard
// exists — its document set being derived from the parent's VersionDocList — does
// not apply to a version that will never produce one.
//
// The two checks must agree, which is why this test constructs a case where only
// the survivor's side is terminal: if the removed-set rule were relaxed alone, this
// delete would still fail with ErrVersionPending.
func TestStateMachine_Apply_MarkVersionDeleting_DataTerminalSurvivorIsAllowed(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := proposeCtx(t)
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	// Chain A: root -> a1 (READY, to be deleted) -> a2 (data-side verdict).
	rootA := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: rootA.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	a1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: rootA.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: a1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	a2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: a1.VersionID}, w, zap.NewNop())
	if res := sm.apply(ctx, command{
		Type: cmdMarkVersionFailedPermanent, KBID: "kb-1", VersionID: a2.VersionID,
		FailureSide: types.FailureSideData, FailureReason: "replication fell short", FailureCount: 5,
	}, w, zap.NewNop()); res.Err != nil {
		t.Fatalf("MarkVersionFailedPermanent(a2): %v", res.Err)
	}

	// Chain B is the control: same shape, but the survivor is still writing.
	rootB := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: rootB.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	b1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: rootB.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: b1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: b1.VersionID}, w, zap.NewNop())

	// Chain B: the survivor may still be writing, so the splice is refused.
	res := sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: b1.VersionID, Mode: types.VersionDeleteSingle}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionPending) {
		t.Errorf("MarkVersionDeleting(SINGLE, b1) = %v, want ErrVersionPending: a survivor that may still write still needs its parent", res.Err)
	}

	// Chain A: the survivor is terminal, so the splice goes through and a2 is
	// re-parented onto the root.
	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: a1.VersionID, Mode: types.VersionDeleteSingle}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("MarkVersionDeleting(SINGLE, a1): %v (the survivor's verdict must be read the same way on both sides)", res.Err)
	}
	if !sm.versions[a1.VersionID].Deleting {
		t.Error("a1 should be marked Deleting")
	}
	if got := sm.versions[a2.VersionID].ParentVersionID; got != rootA.VersionID {
		t.Errorf("a2.ParentVersionID = %d, want %d (spliced onto its grandparent)", got, rootA.VersionID)
	}
}

// applyCreateVersion keeps refusing a parent that is PENDING, and that is NOT
// relaxed by the rule above: a new version derives its document set from the
// parent's VersionDocList, and a parent whose data will never arrive cannot
// contribute one. Pinned here so the two rules are not "unified" by accident.
func TestStateMachine_Apply_CreateVersion_RefusesADataTerminalParent(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := proposeCtx(t)
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{
		Type: cmdMarkVersionFailedPermanent, KBID: "kb-1", VersionID: v1.VersionID,
		FailureSide: types.FailureSideData, FailureReason: "replication fell short", FailureCount: 5,
	}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrInvalidParentVersion) {
		t.Errorf("CreateVersion on a data-terminal parent = %v, want ErrInvalidParentVersion: it has no document set to inherit", res.Err)
	}
}
