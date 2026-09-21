package coordinator

import (
	"context"
	"testing"

	"stratum/internal/chunkdoc"
	"stratum/internal/types"
)

// gcTailCounter counts whole-chain reads, so a case can pin that a sweep asks for the
// EXTREME VALUE instead of the chain. That matters more here than elsewhere: a sweep
// runs per knowledge base every interval, and on a storage node its RaftNode is
// remote — so a whole-chain read was a chain-sized RPC per KB per sweep, for one
// number.
type gcTailCounter struct {
	*gcTestRaftNode
	listVersionsCalls int
}

func (c *gcTailCounter) ListVersions(ctx context.Context, kbID string) ([]types.VersionMeta, error) {
	c.listVersionsCalls++
	return c.gcTestRaftNode.ListVersions(ctx, kbID)
}

func TestChunkGarbageCollector_SweepAsksForTheTailNotTheChain(t *testing.T) {
	ctx := context.Background()

	rn := &gcTailCounter{gcTestRaftNode: &gcTestRaftNode{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
		versions: map[string][]types.VersionMeta{
			"kb-1": {{VersionID: 1, KBID: "kb-1"}, {VersionID: 7, KBID: "kb-1"}},
		},
	}}
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	defer cdm.Close()

	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode:         rn,
		ChunkDocMapper:   cdm,
		DocStore:         newTestDocStore(),
		ChunkStore:       newGCChunkStore(),
	})

	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if rn.listVersionsCalls != 0 {
		t.Errorf("ListVersions calls = %d, want 0: a sweep needs the KB's newest version, not its chain",
			rn.listVersionsCalls)
	}
}
