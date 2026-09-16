package raft

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// applyMarkDataDurable is the control layer's own data-side promotion
// (Stratum_设计文档v13.md §10.1b), driven by the startup reconcile's cursor
// report (§7.9). These tests drive the state machine directly: what matters is
// the transition rule, not who calls it.

// TestStateMachine_MarkDataDurablePromotesPending covers the ordinary case.
func TestStateMachine_MarkDataDurablePromotesPending(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdMarkDataDurable, VersionID: v.VersionID}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("MarkDataDurable: %v", res.Err)
	}
	got := sm.versions[v.VersionID]
	if got.DataStatus != types.DataStatusDurable {
		t.Errorf("data status = %v, want DATA_DURABLE", got.DataStatus)
	}
	// The index side answers a different question and must not have moved.
	if got.IndexStatus != types.IndexStatusPending {
		t.Errorf("index status = %v, want it untouched", got.IndexStatus)
	}
}

// A terminal data verdict wins over a later promotion: the reconcile snapshot is
// by construction older than the verdict, so re-applying it must not undo the
// control layer's decision.
func TestStateMachine_MarkDataDurableDoesNotUndoATerminalVerdict(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{
		Type: cmdMarkVersionFailedPermanent, KBID: "kb-1", VersionID: v.VersionID,
		FailureSide: types.FailureSideData, FailureReason: "gone", FailureCount: 5,
	}, w, zap.NewNop())

	if res := sm.apply(ctx, command{Type: cmdMarkDataDurable, VersionID: v.VersionID}, w, zap.NewNop()); res.Err != nil {
		t.Fatalf("MarkDataDurable: %v", res.Err)
	}
	got := sm.versions[v.VersionID]
	if got.DataStatus != types.DataStatusFailedPermanent {
		t.Errorf("data status = %v, want it to stay DATA_FAILED_PERMANENT", got.DataStatus)
	}
	if got.FailureReason != "gone" || got.FailureCount != 5 {
		t.Errorf("cause chain = (%q, %d), want it preserved", got.FailureReason, got.FailureCount)
	}
}

// Re-applying is a no-op rather than a failure: a replayed log entry converges.
func TestStateMachine_MarkDataDurableIsIdempotent(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())

	for i := 0; i < 2; i++ {
		if res := sm.apply(ctx, command{Type: cmdMarkDataDurable, VersionID: v.VersionID}, w, zap.NewNop()); res.Err != nil {
			t.Fatalf("apply #%d: %v", i+1, res.Err)
		}
	}
	if got := sm.versions[v.VersionID]; got.DataStatus != types.DataStatusDurable {
		t.Errorf("data status = %v, want DATA_DURABLE", got.DataStatus)
	}
}

// An unknown version is rejected rather than silently created.
func TestStateMachine_MarkDataDurableRejectsUnknownVersion(t *testing.T) {
	sm, w := newTestSM(t)
	res := sm.apply(context.Background(), command{Type: cmdMarkDataDurable, VersionID: 4242}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("unknown version = %v, want ErrVersionNotFound", res.Err)
	}
}

// The data side's state must survive a snapshot, or a restart would forget which
// versions were already known durable and hand §10.6's cleanup a different
// picture than the one it had.
func TestStateMachine_DataDurableSurvivesSnapshot(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdMarkDataDurable, VersionID: v.VersionID}, w, zap.NewNop())

	data, err := sm.serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	restored := newStateMachine()
	if err := restored.restore(data); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := restored.versions[v.VersionID]; got.DataStatus != types.DataStatusDurable {
		t.Errorf("restored data status = %v, want DATA_DURABLE", got.DataStatus)
	}
}
