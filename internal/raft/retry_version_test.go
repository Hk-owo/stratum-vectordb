package raft

import (
	"errors"
	"testing"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// retryFixture builds a KB with one version and applies the terminal verdicts the
// caller asks for, which is the state an operator faces when reaching for
// ForceRetryVersion.
func retryFixture(t *testing.T, sides ...types.FailureSide) (*stateMachine, *wal.MockWAL, int64) {
	t.Helper()
	sm, w := newTestSM(t)
	ctx := proposeCtx(t)
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	if v.Err != nil {
		t.Fatalf("create version: %v", v.Err)
	}
	for _, side := range sides {
		res := sm.apply(ctx, command{
			Type: cmdMarkVersionFailedPermanent, KBID: "kb-1", VersionID: v.VersionID,
			FailureSide: side, FailureReason: "replication fell short", FailureCount: 5,
		}, w, zap.NewNop())
		if res.Err != nil {
			t.Fatalf("mark %s side permanent: %v", side, res.Err)
		}
	}
	return sm, w, v.VersionID
}

// The operator's retry (§10.1): the index-side verdict is revoked, the side goes
// back to PENDING, and — because nothing is terminal any more — the cause chain of
// a failure that has just been revoked goes with it. Before this command existed the
// only route was RebuildIndex's bare status overwrite, which left the chain behind
// and kept GetSystemStatus describing a verdict that no longer held.
func TestStateMachine_Apply_RetryVersion_ClearsTheVerdictAndItsCauseChain(t *testing.T) {
	sm, w, versionID := retryFixture(t, types.FailureSideIndex)
	ctx := proposeCtx(t)

	res := sm.apply(ctx, command{Type: cmdRetryVersion, KBID: "kb-1", VersionID: versionID, FailureSide: types.FailureSideIndex}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("RetryVersion: %v", res.Err)
	}
	got := sm.versions[versionID]
	if got.IndexStatus != types.IndexStatusPending {
		t.Errorf("index status = %v, want PENDING: the rebuild has to start somewhere", got.IndexStatus)
	}
	if got.DataStatus != types.DataStatusPending {
		t.Errorf("data status = %v, want it untouched (PENDING): a retry settles one side", got.DataStatus)
	}
	if got.FailureReason != "" || got.FailureCount != 0 {
		t.Errorf("cause chain = (%q, %d), want it cleared once no side is terminal", got.FailureReason, got.FailureCount)
	}
}

// While the OTHER side is still terminal the cause chain stays: it is the diagnosis
// of a verdict still in force, and clearing it would erase exactly what an operator
// is reading the version for.
func TestStateMachine_Apply_RetryVersion_KeepsTheCauseChainWhileTheOtherSideIsDead(t *testing.T) {
	sm, w, versionID := retryFixture(t, types.FailureSideData, types.FailureSideIndex)
	ctx := proposeCtx(t)

	res := sm.apply(ctx, command{Type: cmdRetryVersion, KBID: "kb-1", VersionID: versionID, FailureSide: types.FailureSideIndex}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("RetryVersion: %v", res.Err)
	}
	got := sm.versions[versionID]
	if got.IndexStatus != types.IndexStatusPending {
		t.Errorf("index status = %v, want PENDING", got.IndexStatus)
	}
	if got.DataStatus != types.DataStatusFailedPermanent {
		t.Errorf("data status = %v, want the data-side verdict untouched", got.DataStatus)
	}
	if got.FailureReason == "" || got.FailureCount != 5 {
		t.Errorf("cause chain = (%q, %d), want it kept: the data side is still dead", got.FailureReason, got.FailureCount)
	}
}

// The three refusals. Each one is a decision, not a gap: a side that is not terminal
// has nothing to retry, the data side is never retryable (its verdict says the data
// will never arrive), and a version already on its way out has no state to revive.
func TestStateMachine_Apply_RetryVersion_Refusals(t *testing.T) {
	ctx := proposeCtx(t)

	t.Run("index side is not terminal", func(t *testing.T) {
		sm, w, versionID := retryFixture(t)
		sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: versionID, Status: types.IndexStatusReady}, w, zap.NewNop())

		res := sm.apply(ctx, command{Type: cmdRetryVersion, KBID: "kb-1", VersionID: versionID, FailureSide: types.FailureSideIndex}, w, zap.NewNop())
		if !errors.Is(res.Err, stratumerrors.ErrVersionNotFailedPermanent) {
			t.Errorf("RetryVersion on a READY version = %v, want ErrVersionNotFailedPermanent", res.Err)
		}
		if got := sm.versions[versionID].IndexStatus; got != types.IndexStatusReady {
			t.Errorf("index status = %v, want READY: a refused retry changes nothing", got)
		}
	})

	t.Run("data side is never retryable", func(t *testing.T) {
		sm, w, versionID := retryFixture(t, types.FailureSideData)

		res := sm.apply(ctx, command{Type: cmdRetryVersion, KBID: "kb-1", VersionID: versionID}, w, zap.NewNop())
		if !errors.Is(res.Err, stratumerrors.ErrVersionNotFailedPermanent) {
			t.Errorf("data-side retry = %v, want ErrVersionNotFailedPermanent", res.Err)
		}
		if got := sm.versions[versionID].DataStatus; got != types.DataStatusFailedPermanent {
			t.Errorf("data status = %v, want the verdict kept", got)
		}
	})

	t.Run("version is being deleted", func(t *testing.T) {
		sm, w, versionID := retryFixture(t, types.FailureSideIndex)
		res := sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: versionID, Mode: types.VersionDeleteSubtree}, w, zap.NewNop())
		if res.Err != nil {
			t.Fatalf("MarkVersionDeleting: %v", res.Err)
		}

		res = sm.apply(ctx, command{Type: cmdRetryVersion, KBID: "kb-1", VersionID: versionID, FailureSide: types.FailureSideIndex}, w, zap.NewNop())
		if !errors.Is(res.Err, stratumerrors.ErrVersionDeleting) {
			t.Errorf("RetryVersion on a Deleting version = %v, want ErrVersionDeleting", res.Err)
		}
	})

	t.Run("unknown and cross-KB versions", func(t *testing.T) {
		sm, w, versionID := retryFixture(t, types.FailureSideIndex)

		res := sm.apply(ctx, command{Type: cmdRetryVersion, KBID: "kb-1", VersionID: 999, FailureSide: types.FailureSideIndex}, w, zap.NewNop())
		if !errors.Is(res.Err, stratumerrors.ErrVersionNotFound) {
			t.Errorf("RetryVersion(999) = %v, want ErrVersionNotFound", res.Err)
		}

		sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-2"))}, w, zap.NewNop())
		res = sm.apply(ctx, command{Type: cmdRetryVersion, KBID: "kb-2", VersionID: versionID, FailureSide: types.FailureSideIndex}, w, zap.NewNop())
		if !errors.Is(res.Err, stratumerrors.ErrVersionNotFound) {
			t.Errorf("cross-KB RetryVersion = %v, want ErrVersionNotFound", res.Err)
		}
	})
}
