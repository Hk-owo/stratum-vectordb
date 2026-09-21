package raft

import (
	"testing"

	"stratum/internal/types"
)

// TestRaftNodeImpl_ListVersionsInRange_NarrowsTheChain pins the bounds: they are what
// keeps a whole-chain answer (about 89 B per version) off the wire when a caller needs
// a few versions, while both-nil has to stay exactly ListVersions — that is how the
// rest of the system asks, and a range that changed that answer would be a trap.
func TestRaftNodeImpl_ListVersionsInRange_NarrowsTheChain(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := proposeCtx(t)
	if err := impl.ProposeCreateKB(ctx, testKB("kb-1")); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}

	// The chain is linear and a parent must not be PENDING, so each link is marked
	// READY before the next is created.
	var ids []int64
	for i := 0; i < 4; i++ {
		parent := int64(0)
		if i > 0 {
			parent = ids[i-1]
		}
		id, err := impl.ProposeCreateVersion(ctx, "kb-1", parent)
		if err != nil {
			t.Fatalf("create #%d: %v", i, err)
		}
		if err := impl.ProposeUpdateVersionStatus(ctx, id, types.IndexStatusReady, 0); err != nil {
			t.Fatalf("mark READY #%d: %v", i, err)
		}
		ids = append(ids, id)
	}

	idsOf := func(vs []types.VersionMeta) []int64 {
		out := make([]int64, 0, len(vs))
		for _, v := range vs {
			out = append(out, v.VersionID)
		}
		return out
	}

	// (ids[0], ids[2]] — the from side is EXCLUSIVE, the to side inclusive.
	from, to := ids[0], ids[2]
	got, err := impl.ListVersionsInRange(ctx, "kb-1", &from, &to)
	if err != nil {
		t.Fatalf("ListVersionsInRange: %v", err)
	}
	if want := []int64{ids[1], ids[2]}; len(got) != 2 || got[0].VersionID != want[0] || got[1].VersionID != want[1] {
		t.Errorf("range (%d,%d] = %v, want %v", from, to, idsOf(got), want)
	}

	// One-sided bounds: nil means "no bound on that side", not zero.
	got, err = impl.ListVersionsInRange(ctx, "kb-1", nil, &to)
	if err != nil {
		t.Fatalf("ListVersionsInRange(nil, to): %v", err)
	}
	if want := []int64{ids[0], ids[1], ids[2]}; len(got) != 3 {
		t.Errorf("(nil,%d] = %v, want %v", to, idsOf(got), want)
	}
	got, err = impl.ListVersionsInRange(ctx, "kb-1", &from, nil)
	if err != nil {
		t.Fatalf("ListVersionsInRange(from, nil): %v", err)
	}
	if len(got) != 3 {
		t.Errorf("(%d,nil] = %v, want the three above it", from, idsOf(got))
	}

	// Both nil: exactly ListVersions.
	all, err := impl.ListVersionsInRange(ctx, "kb-1", nil, nil)
	if err != nil {
		t.Fatalf("ListVersionsInRange(nil, nil): %v", err)
	}
	if len(all) != 4 {
		t.Errorf("unbounded = %v, want the whole chain", idsOf(all))
	}
}
