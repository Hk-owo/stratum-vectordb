package raft

import (
	"errors"
	"testing"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// TestStateMachine_Apply_MarkVersionFailedPermanent pins the terminal verdict of
// Stratum_设计文档v13.md §10.1: the control layer records that ONE SIDE of a
// version will not be retried automatically, together with the cause chain an
// operator needs.
//
// The two sides land in different fields on purpose (§10.1b): a verdict that
// settles the data must leave the index's state alone, and vice versa. That
// separation is the whole point of the field split — one shared status could not
// say "the data will never arrive" without also claiming the index had failed.
func TestStateMachine_Apply_MarkVersionFailedPermanent(t *testing.T) {
	cases := []struct {
		name      string
		side      types.FailureSide
		wantData  types.DataStatus
		wantIndex types.IndexStatus
	}{
		{"data side", types.FailureSideData, types.DataStatusFailedPermanent, types.IndexStatusPending},
		{"index side", types.FailureSideIndex, types.DataStatusPending, types.IndexStatusFailedPermanent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sm, w := newTestSM(t)
			ctx := proposeCtx(t)
			sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
			v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
			if v.Err != nil {
				t.Fatalf("create version: %v", v.Err)
			}

			res := sm.apply(ctx, command{
				Type:          cmdMarkVersionFailedPermanent,
				KBID:          "kb-1",
				VersionID:     v.VersionID,
				FailureSide:   tc.side,
				FailureReason: "data unavailable on every replica",
				FailureCount:  4,
			}, w, zap.NewNop())
			if res.Err != nil {
				t.Fatalf("MarkVersionFailedPermanent: %v", res.Err)
			}

			got := sm.versions[v.VersionID]
			if got.DataStatus != tc.wantData {
				t.Errorf("data status = %v, want %v", got.DataStatus, tc.wantData)
			}
			if got.IndexStatus != tc.wantIndex {
				t.Errorf("index status = %v, want %v: the other side must be untouched", got.IndexStatus, tc.wantIndex)
			}
			if got.FailureReason != "data unavailable on every replica" {
				t.Errorf("reason = %q, want the recorded cause", got.FailureReason)
			}
			if got.FailureCount != 4 {
				t.Errorf("count = %d, want 4", got.FailureCount)
			}
		})
	}
}

// Re-applying overwrites the recorded cause instead of failing: a replayed log
// entry must converge.
func TestStateMachine_MarkVersionFailedPermanent_IsIdempotent(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := proposeCtx(t)
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())

	for i, reason := range []string{"first", "second"} {
		res := sm.apply(ctx, command{
			Type: cmdMarkVersionFailedPermanent, KBID: "kb-1", VersionID: v.VersionID,
			FailureReason: reason, FailureCount: int32(i + 1),
		}, w, zap.NewNop())
		if res.Err != nil {
			t.Fatalf("apply #%d: %v", i+1, res.Err)
		}
	}
	got := sm.versions[v.VersionID]
	if got.FailureReason != "second" || got.FailureCount != 2 {
		t.Errorf("recorded = (%q, %d), want the latest verdict", got.FailureReason, got.FailureCount)
	}
}

// An unknown version, or one belonging to another KB, is rejected rather than
// silently creating a phantom entry.
func TestStateMachine_MarkVersionFailedPermanent_RejectsUnknown(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := proposeCtx(t)
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-2"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdMarkVersionFailedPermanent, KBID: "kb-1", VersionID: 4242}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("unknown version = %v, want ErrVersionNotFound", res.Err)
	}

	res = sm.apply(ctx, command{Type: cmdMarkVersionFailedPermanent, KBID: "kb-2", VersionID: v.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("cross-KB version = %v, want ErrVersionNotFound", res.Err)
	}
	if sm.versions[v.VersionID].DataStatus == types.DataStatusFailedPermanent {
		t.Error("a rejected verdict must not change the version")
	}
}

// The verdict and its cause chain must survive a Raft snapshot, otherwise a
// restart would silently forget why a version is stuck.
//
// The DATA side is the one exercised here, which also pins that the new field is
// carried by the snapshot: a verdict kept only in memory would come back PENDING
// after a restart.
func TestStateMachine_FailedPermanentSurvivesSnapshot(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := proposeCtx(t)
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{
		Type: cmdMarkVersionFailedPermanent, KBID: "kb-1", VersionID: v.VersionID,
		FailureSide: types.FailureSideData, FailureReason: "quota rejected", FailureCount: 7,
	}, w, zap.NewNop())

	data, err := sm.serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	restored := newStateMachine()
	if err := restored.restore(data); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got := restored.versions[v.VersionID]
	if got.DataStatus != types.DataStatusFailedPermanent || got.FailureReason != "quota rejected" || got.FailureCount != 7 {
		t.Errorf("restored = (%v, %q, %d), want the recorded verdict", got.DataStatus, got.FailureReason, got.FailureCount)
	}
}
