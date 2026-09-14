package plane

import (
	"context"
	"errors"
	"testing"

	"stratum/internal/types"
)

// The control layer decides the terminal verdict, so it must keep the count
// itself: a version stays retryable until the budget is spent
// (Stratum_设计文档v13.md §10.1).
func TestLocalControlPlane_ReportVersionFailureDeclaresTerminalAfterBudget(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta, WithFailureBudget(3))
	ctx := context.Background()

	for i := 1; i <= 2; i++ {
		if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureTransient, "storage unreachable"); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if len(meta.permanentCalls) != 0 {
		t.Fatalf("declared the terminal verdict before the budget was spent: %+v", meta.permanentCalls)
	}

	if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureTransient, "storage unreachable"); err != nil {
		t.Fatalf("attempt 3: %v", err)
	}
	if len(meta.permanentCalls) != 1 {
		t.Fatalf("permanent calls = %d, want exactly one verdict", len(meta.permanentCalls))
	}
	got := meta.permanentCalls[0]
	if got.kbID != "kb-1" || got.versionID != 7 {
		t.Errorf("verdict on %s/v%d, want kb-1/v7", got.kbID, got.versionID)
	}
	if got.reason != "storage unreachable" {
		t.Errorf("reason = %q, want the reported cause", got.reason)
	}
	if got.count != 3 {
		t.Errorf("count = %d, want 3 (the number of failed attempts)", got.count)
	}
}

// A later success resets the budget: an unrelated failure afterwards must not
// inherit the old count and die early.
func TestLocalControlPlane_ReportDataDurableResetsTheBudget(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta, WithFailureBudget(3))
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureTransient, "transient"); err != nil {
			t.Fatalf("failure %d: %v", i, err)
		}
	}
	// Data lands: the version is durable, so the count starts over.
	if err := cp.ReportDataDurable(ctx, "kb-1", 7, "digest"); err != nil {
		t.Fatalf("ReportDataDurable: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureTransient, "transient"); err != nil {
			t.Fatalf("post-reset failure %d: %v", i, err)
		}
	}
	if len(meta.permanentCalls) != 0 {
		t.Fatalf("the budget was not reset by the successful report: %+v", meta.permanentCalls)
	}
}

// Counters are per version: one version spending its budget must not condemn
// another.
func TestLocalControlPlane_FailureBudgetIsPerVersion(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta, WithFailureBudget(2))
	ctx := context.Background()

	if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureTransient, "boom"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := cp.ReportVersionFailure(ctx, "kb-1", 8, types.FailureTransient, "boom"); err != nil {
			t.Fatal(err)
		}
	}
	if len(meta.permanentCalls) != 1 || meta.permanentCalls[0].versionID != 8 {
		t.Fatalf("verdicts = %+v, want only version 8", meta.permanentCalls)
	}
}

// The same version ID in another knowledge base is a different version.
func TestLocalControlPlane_FailureBudgetIsPerKnowledgeBase(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta, WithFailureBudget(2))
	ctx := context.Background()

	if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureTransient, "boom"); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.ReportVersionFailure(ctx, "kb-2", 7, types.FailureTransient, "boom"); err != nil {
		t.Fatal(err)
	}
	if len(meta.permanentCalls) != 0 {
		t.Fatalf("verdicts = %+v, want none: each KB has its own budget", meta.permanentCalls)
	}
}

// A proposal failure is surfaced rather than swallowed: the caller still owns
// retrying the version.
func TestLocalControlPlane_ReportVersionFailurePropagatesProposalError(t *testing.T) {
	boom := errors.New("not leader")
	meta := &stubMeta{permanentErr: boom}
	cp := NewLocalControlPlane(meta, WithFailureBudget(1))
	ctx := context.Background()

	_, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureTransient, "boom")
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the proposal error", err)
	}
}
