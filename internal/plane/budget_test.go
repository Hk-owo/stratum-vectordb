package plane

import (
	"context"
	"testing"

	"stratum/internal/types"
)

// A knowledge base's declared policy has to reach the layer that owns the
// verdict: the number is per-KB policy, the decision is the control layer's
// (Stratum_设计文档v13.md §10.1).
func TestLocalControlPlane_SetFailureBudgetOverridesPerKB(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta, WithFailureBudget(5))
	ctx := context.Background()

	if err := cp.SetFailureBudget(ctx, "kb-1", 2); err != nil {
		t.Fatalf("SetFailureBudget: %v", err)
	}

	// kb-1: the declared budget of 2 governs.
	if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "boom"); err != nil {
		t.Fatal(err)
	}
	if len(meta.permanentCalls) != 0 {
		t.Fatalf("declared after one failure: %+v", meta.permanentCalls)
	}
	if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "boom"); err != nil {
		t.Fatal(err)
	}
	if len(meta.permanentCalls) != 1 {
		t.Fatalf("kb-1's own budget (2) did not govern: %+v", meta.permanentCalls)
	}

	// kb-2 has no override, so the process default of 5 still stands.
	for i := 0; i < 4; i++ {
		if _, err := cp.ReportVersionFailure(ctx, "kb-2", 7, types.FailureSideData, types.FailureTransient, "boom"); err != nil {
			t.Fatal(err)
		}
	}
	if len(meta.permanentCalls) != 1 {
		t.Fatalf("kb-2 inherited kb-1's override: %+v", meta.permanentCalls)
	}
}

// A non-positive value means "no override", not "fail immediately": clearing
// the policy must not become a hair trigger.
func TestLocalControlPlane_SetFailureBudgetClearsOverride(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta, WithFailureBudget(3))
	ctx := context.Background()

	if err := cp.SetFailureBudget(ctx, "kb-1", 1); err != nil {
		t.Fatal(err)
	}
	if err := cp.SetFailureBudget(ctx, "kb-1", 0); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if _, err := cp.ReportVersionFailure(ctx, "kb-1", 7, types.FailureSideData, types.FailureTransient, "boom"); err != nil {
			t.Fatal(err)
		}
	}
	if len(meta.permanentCalls) != 0 {
		t.Fatalf("cleared budget behaved like a hair trigger: %+v", meta.permanentCalls)
	}
}
