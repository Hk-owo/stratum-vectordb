package plane

import (
	"context"
	"testing"

	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// TestLocalControlPlane_ExistingVersionsCoversExactlyTheGap pins the boundary against the
// REAL control plane and a real state machine — not a hand-written stub, which would only
// restate whatever the implementation happens to do.
//
// Why the boundary matters: the backfill asks this about the gap it is about to replay,
// and an off-by-one that dropped a gap version would read as "that version was deleted".
// That is not a slow path — it hands the whole gap to the full-state transfer, so the
// node ends up holding only the snapshot version and the intermediate versions' records
// are never written at all.
func TestLocalControlPlane_ExistingVersionsCoversExactlyTheGap(t *testing.T) {
	rn := raft.NewMockRaftNode(wal.NewMockWAL())
	ctx := context.Background()
	if err := rn.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "kb-1", Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	var ids []int64
	for i := 0; i < 4; i++ {
		id, err := rn.ProposeCreateVersion(ctx, "kb-1", 0)
		if err != nil {
			t.Fatalf("ProposeCreateVersion #%d: %v", i, err)
		}
		ids = append(ids, id)
	}

	cp := NewLocalControlPlane(rn)
	got, err := cp.ExistingVersions(ctx, "kb-1", ids[0], ids[2])
	if err != nil {
		t.Fatalf("ExistingVersions: %v", err)
	}
	if len(got) != 2 || !got[ids[1]] || !got[ids[2]] {
		t.Fatalf("(%d,%d] = %v, want exactly v%d and v%d", ids[0], ids[2], got, ids[1], ids[2])
	}
	if got[ids[0]] || got[ids[3]] {
		t.Errorf("(%d,%d] = %v: the from side is exclusive and the to side is the bound", ids[0], ids[2], got)
	}
}
