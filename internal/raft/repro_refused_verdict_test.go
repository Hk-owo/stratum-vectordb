package raft

import (
	"errors"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// TestRaftNodeImpl_RefusedVerdictOnADurableVersionDoesNotReclaim: the refusal only
// protects the data if the reclaim hook stays quiet too. A hook firing on it would delete
// the version on every replica, whatever the state machine recorded.
func TestRaftNodeImpl_RefusedVerdictOnADurableVersionDoesNotReclaim(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := proposeCtx(t)

	called := make(chan int64, 1)
	impl.SetOnVersionFailedPermanent(func(_ string, versionID int64) { called <- versionID })

	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	versionID, err := impl.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	if err := impl.ProposeUpdateVersionSummary(ctx, versionID, "digest"); err != nil {
		t.Fatalf("ProposeUpdateVersionSummary: %v", err)
	}

	err = impl.ProposeMarkVersionFailedPermanent(ctx, "kb-1", versionID, types.FailureSideData, "replication fell short", 5)
	if !errors.Is(err, stratumerrors.ErrVersionDataSettled) {
		t.Fatalf("err = %v, want ErrVersionDataSettled to reach the proposer", err)
	}

	select {
	case v := <-called:
		t.Fatalf("the reclaim hook fired for v%d after the verdict was refused", v)
	case <-time.After(200 * time.Millisecond):
	}
	got, err := impl.GetVersion(ctx, "kb-1", versionID)
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if got.DataStatus != types.DataStatusDurable {
		t.Errorf("data status = %v, want DATA_DURABLE", got.DataStatus)
	}
}
