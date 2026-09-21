package plane

import (
	"context"
	"testing"

	"stratum/internal/types"
)

// chainTailCounter counts whole-chain reads, so a case can pin that ChainTail asks
// for the extreme value instead of the chain.
type chainTailCounter struct {
	*stubMeta
	listVersionsCalls int
}

func (c *chainTailCounter) ListVersions(ctx context.Context, kbID string) ([]types.VersionMeta, error) {
	c.listVersionsCalls++
	return c.stubMeta.ListVersions(ctx, kbID)
}

// TestLocalControlPlane_ChainTailAsksForTheTailNotTheChain: ChainTail runs once per
// knowledge base on EVERY cursor report (the leader fills chain_tails for whoever
// reported), so it must not be a whole-chain read. It is the extreme value — the same
// number ListVersions' maximum would give, without building the chain to find it.
func TestLocalControlPlane_ChainTailAsksForTheTailNotTheChain(t *testing.T) {
	meta := &chainTailCounter{stubMeta: &stubMeta{versions: map[string][]types.VersionMeta{
		"kb-1": {
			{KBID: "kb-1", VersionID: 3},
			{KBID: "kb-1", VersionID: 9},
			{KBID: "kb-1", VersionID: 7},
		},
	}}}
	cp := NewLocalControlPlane(meta)

	tail, ok := cp.ChainTail("kb-1")
	if !ok || tail != 9 {
		t.Fatalf("ChainTail(kb-1) = (%d, %v), want (9, true)", tail, ok)
	}
	if meta.listVersionsCalls != 0 {
		t.Errorf("ListVersions calls = %d, want 0: the tail is an extreme-value read, not a whole-chain read",
			meta.listVersionsCalls)
	}

	// A knowledge base with no versions is "no signal", never "nothing to catch up":
	// a zero must not be reported as a tail.
	if tail, ok := cp.ChainTail("kb-empty"); ok || tail != 0 {
		t.Errorf("ChainTail(kb-empty) = (%d, %v), want (0, false)", tail, ok)
	}
	if meta.listVersionsCalls != 0 {
		t.Errorf("ListVersions calls = %d after the second call, want 0", meta.listVersionsCalls)
	}
}
