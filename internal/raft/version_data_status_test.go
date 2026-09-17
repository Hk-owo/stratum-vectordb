package raft

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"stratum/internal/types"
)

// A version created WITH changes starts PENDING on the data side: it stays there
// until a writer reports the version's document-set digest, or until a cursor
// promotion settles it (§7.9).
//
// Nothing about a version's data status is decided at creation any more. A
// version created with NO changes is refused outright by the coordinator
// (docs/cursor-persistence-plan.md §5) — a version's document set is inherited
// from its parent, so an empty changes list means "unchanged", not "empty" — so
// the only case the state machine has to get right here is the ordinary one.
func TestCreateVersion_WithChangesStartsPending(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	if got := sm.versions[v.VersionID]; got.DataStatus != types.DataStatusPending {
		t.Errorf("data status = %v, want DATA_PENDING for a version carrying changes", got.DataStatus)
	}
	// The index side always starts PENDING: an index has to be built before the
	// version can be served.
	if got := sm.versions[v.VersionID]; got.IndexStatus != types.IndexStatusPending {
		t.Errorf("index status = %v, want PENDING (an index still has to be built)", got.IndexStatus)
	}
}
