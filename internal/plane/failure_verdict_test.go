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
		if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "storage unreachable"); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if len(meta.permanentCalls) != 0 {
		t.Fatalf("declared the terminal verdict before the budget was spent: %+v", meta.permanentCalls)
	}

	if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "storage unreachable"); err != nil {
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
		if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "transient"); err != nil {
			t.Fatalf("failure %d: %v", i, err)
		}
	}
	// Data lands: the version is durable, so the count starts over.
	if err := cp.ReportDataDurable(ctx, "kb-1", 7, "digest"); err != nil {
		t.Fatalf("ReportDataDurable: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "transient"); err != nil {
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

	if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "boom"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := cp.ReportVersionFailure(ctx, "kb-1", 8, types.FailureSideData, types.FailureTransient, "boom"); err != nil {
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

	if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "boom"); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.ReportVersionFailure(ctx, "kb-2", 7, types.FailureSideData, types.FailureTransient, "boom"); err != nil {
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

	_, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "boom")
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the proposal error", err)
	}
}

// The two sides fail independently, so they must not share a counter: an index
// build that keeps failing would otherwise spend the budget the data write needs
// and declare the DATA dead — claiming the data will never arrive because an
// index would not build (Stratum_设计文档v13.md §10.1b).
func TestLocalControlPlane_FailureBudgetIsPerSide(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta, WithFailureBudget(2))
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "data"); err != nil {
			t.Fatal(err)
		}
	}
	if len(meta.permanentCalls) != 1 || meta.permanentCalls[0].side != types.FailureSideData {
		t.Fatalf("verdicts = %+v, want exactly one on the data side", meta.permanentCalls)
	}

	// One index-side failure is a fresh budget, not the data side's third.
	terminal, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideIndex, types.FailureTransient, "index")
	if err != nil {
		t.Fatal(err)
	}
	if terminal {
		t.Fatal("an index-side failure inherited the data side's spent budget")
	}
	if len(meta.permanentCalls) != 1 {
		t.Fatalf("verdicts = %+v, want the data side's one and nothing more", meta.permanentCalls)
	}

	// And the index side's own budget is spent after its own two failures.
	if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideIndex, types.FailureTransient, "index"); err != nil {
		t.Fatal(err)
	}
	if len(meta.permanentCalls) != 2 || meta.permanentCalls[1].side != types.FailureSideIndex {
		t.Fatalf("verdicts = %+v, want a second one on the index side", meta.permanentCalls)
	}
}

// A successful report resets only ITS side's counter: the other side's history is
// a different question with a different answer (§10.1b), and clearing both would
// let an index build's success wipe the data side's recorded failures.
func TestLocalControlPlane_SuccessResetsOnlyItsOwnSide(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta, WithFailureBudget(2))
	ctx := context.Background()

	// One failure on each side, then the INDEX succeeds.
	for _, side := range []types.FailureSide{types.FailureSideData, types.FailureSideIndex} {
		if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, side, types.FailureTransient, side.String()); err != nil {
			t.Fatal(err)
		}
	}
	if err := cp.ReportIndexReady(ctx, "kb-1", 7); err != nil {
		t.Fatalf("ReportIndexReady: %v", err)
	}

	// The index side starts over: one more failure is not terminal.
	if terminal, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideIndex, types.FailureTransient, "index"); err != nil || terminal {
		t.Fatalf("index side = (%v, %v), want a fresh budget", terminal, err)
	}
	// The data side kept its one failure, so a second one spends its budget.
	if terminal, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "data"); err != nil || !terminal {
		t.Fatalf("data side = (%v, %v), want the terminal verdict", terminal, err)
	}
}
