package raft

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"stratum/internal/types"
)

// A version created with no document changes is durable on the DATA side from the
// moment it exists.
//
// Nothing fans out for such a version and no writer reports a digest about it, so
// the ordinary promotion path — a replica reporting a contiguous cursor, which §7.9
// turns into cmdMarkDataDurable — can never reach it: the cursor cannot step over a
// version the node does not "hold", holding it is judged from the DATA side, and the
// data side is waiting on that very cursor. Measured on a 3-node cluster before this:
// every replica restarted with cursor 0 for a knowledge base whose initial version
// was the first link of the chain, and each catch-up re-reported the same stalemate.
func TestCreateVersion_EmptyVersionIsDurableAtCreation(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", EmptyVersion: true}, w, zap.NewNop())
	if v.Err != nil {
		t.Fatalf("CreateVersion: %v", v.Err)
	}

	got := sm.versions[v.VersionID]
	if got.DataStatus != types.DataStatusDurable {
		t.Errorf("data status = %v, want DATA_DURABLE: a version with no document set has "+
			"nothing to persist, and PENDING here is what wedges a restarting replica's cursor",
			got.DataStatus)
	}
	// The index side answers a different question and must still start PENDING: an
	// index has to be built before the version can be served.
	if got.IndexStatus != types.IndexStatusPending {
		t.Errorf("index status = %v, want PENDING (an index still has to be built)", got.IndexStatus)
	}
}

// The promotion rule itself must not have been weakened: a version that DOES carry
// changes starts PENDING on the data side and stays there until a writer's digest or
// a cursor promotion settles it.
func TestCreateVersion_WithChangesStartsPending(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	if got := sm.versions[v.VersionID]; got.DataStatus != types.DataStatusPending {
		t.Errorf("data status = %v, want DATA_PENDING for a version carrying changes", got.DataStatus)
	}
}
