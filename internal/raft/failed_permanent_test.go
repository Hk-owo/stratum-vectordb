package raft

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// TestStateMachine_Apply_MarkVersionFailedPermanent pins the terminal verdict
// of Stratum_设计文档v13.md §10.1: the control layer records that a version will
// not be retried automatically, together with the cause chain an operator
// needs.
func TestStateMachine_Apply_MarkVersionFailedPermanent(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	if v.Err != nil {
		t.Fatalf("create version: %v", v.Err)
	}

	res := sm.apply(ctx, command{
		Type:          cmdMarkVersionFailedPermanent,
		KBID:          "kb-1",
		VersionID:     v.VersionID,
		FailureReason: "data unavailable on every replica",
		FailureCount:  4,
	}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("MarkVersionFailedPermanent: %v", res.Err)
	}

	got := sm.versions[v.VersionID]
	if got.IndexStatus != types.IndexStatusFailedPermanent {
		t.Errorf("status = %v, want FAILED_PERMANENT", got.IndexStatus)
	}
	if got.FailureReason != "data unavailable on every replica" {
		t.Errorf("reason = %q, want the recorded cause", got.FailureReason)
	}
	if got.FailureCount != 4 {
		t.Errorf("count = %d, want 4", got.FailureCount)
	}
}

// Re-applying overwrites the recorded cause instead of failing: a replayed log
// entry must converge.
func TestStateMachine_MarkVersionFailedPermanent_IsIdempotent(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
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
	ctx := context.Background()
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
	if sm.versions[v.VersionID].IndexStatus == types.IndexStatusFailedPermanent {
		t.Error("a rejected verdict must not change the version")
	}
}

// The verdict and its cause chain must survive a Raft snapshot, otherwise a
// restart would silently forget why a version is stuck.
func TestStateMachine_FailedPermanentSurvivesSnapshot(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{
		Type: cmdMarkVersionFailedPermanent, KBID: "kb-1", VersionID: v.VersionID,
		FailureReason: "quota rejected", FailureCount: 7,
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
	if got.IndexStatus != types.IndexStatusFailedPermanent || got.FailureReason != "quota rejected" || got.FailureCount != 7 {
		t.Errorf("restored = (%v, %q, %d), want the recorded verdict", got.IndexStatus, got.FailureReason, got.FailureCount)
	}
}
