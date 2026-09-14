package index

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
)

// newRebuildTestManager builds a manager wired for §8.6(d) collection, with the
// given answer for "how many other replicas are serving".
func newRebuildTestManager(t *testing.T, others int) *IndexManagerImpl {
	t.Helper()
	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        time.Second,
		IndexDataDir:           t.TempDir(),
		GCEnabled:              true,
		IndexServingReplicaMin: 2,
		NodeID:                 1,
	})
	im.logger = zap.NewNop()
	im.SetGCReplicaCounter(&gcCountingCounter{others: others}, 1)
	return im
}

// TestRebuildGraphed_RefusesWhenTheShapeIsUnknownOrGraphFree: the rebuild exists
// for graphed artifacts only. Being asked to do anything else must fail loudly
// rather than pick a path — the two collections are not interchangeable.
func TestRebuildGraphed_RefusesWhenTheShapeIsUnknownOrGraphFree(t *testing.T) {
	im := newRebuildTestManager(t, 5)
	ctx := context.Background()

	if err := im.rebuildGraphed(ctx, "kb-1", 7); !errors.Is(err, stratumerrors.ErrInvalidArgument) {
		t.Fatalf("unknown shape: err = %v, want ErrInvalidArgument", err)
	}

	im.builtGraphFree[indexKey{"kb-1", 7}] = true
	if err := im.rebuildGraphed(ctx, "kb-1", 7); !errors.Is(err, stratumerrors.ErrInvalidArgument) {
		t.Fatalf("graph-free: err = %v, want ErrInvalidArgument (that artifact is edited, not rebuilt)", err)
	}
}

// TestRebuildGraphed_ReportsAMissingVecstoreClient: no client means "this node
// cannot do it" — not a panic, and not a silent success. The version must also be
// left in service, because nothing was rebuilt.
func TestRebuildGraphed_ReportsAMissingVecstoreClient(t *testing.T) {
	im := newRebuildTestManager(t, 5)
	im.builtGraphFree[indexKey{"kb-1", 7}] = false // graphed

	if err := im.rebuildGraphed(context.Background(), "kb-1", 7); err == nil {
		t.Fatal("without a vecstore client the rebuild must fail, not pretend to have run")
	}
	if im.inMaintenance(indexKey{"kb-1", 7}) {
		t.Fatal("a failed rebuild left the version in maintenance")
	}
}

// TestGraphRebuildRatio_Defaults pins the asymmetry §8.6(d) asks for: rebuilding a
// graph costs the whole graph, so its bar is far above the bar for editing a
// graph-free artifact — and the difference is the design, not a detail.
func TestGraphRebuildRatio_Defaults(t *testing.T) {
	im := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: time.Second})
	if got := im.graphRebuildRatio(); got != DefaultGCGraphRebuildRatio {
		t.Fatalf("default ratio = %v, want %v", got, DefaultGCGraphRebuildRatio)
	}
	if DefaultGCGraphRebuildRatio <= DefaultGCRatioThreshold {
		t.Fatalf("the rebuild bar (%v) must be higher than the edit bar (%v)",
			DefaultGCGraphRebuildRatio, DefaultGCRatioThreshold)
	}

	configured := NewIndexManager(IndexManagerConfig{
		LRUCapacity:         4,
		LoadWaitTimeout:     time.Second,
		GCGraphRebuildRatio: 0.8,
	})
	if got := configured.graphRebuildRatio(); got != 0.8 {
		t.Fatalf("configured ratio = %v, want 0.8", got)
	}
}

// TestCollectCandidates_SkipsAGraphedVersionThatIsNotDeadEnough is the conservative
// half of §8.6(d)'s "how much longer will this version be served?" call: a graphed
// version that is merely past the EDIT threshold is left alone, because the next
// version will replace it and start clean.
func TestCollectCandidates_SkipsAGraphedVersionThatIsNotDeadEnough(t *testing.T) {
	im := newRebuildTestManager(t, 5)
	im.builtGraphFree[indexKey{"kb-1", 7}] = false // graphed

	// 0.3 clears GCRatioThreshold (a graph-free artifact would be edited) but not
	// the rebuild bar.
	im.collectCandidates(context.Background(), []gcCandidate{{KBID: "kb-1", VersionID: 7, DeadShare: 0.3}})

	if im.inMaintenance(indexKey{"kb-1", 7}) {
		t.Fatal("a graphed version below the rebuild bar must be left serving")
	}
	if got := im.BlockedCollections(); got != nil {
		t.Fatalf("'not dead enough to rebuild' is not a blocked collection: %+v", got)
	}
}

// TestCollectCandidates_LeavesAGraphedVersionAloneWhenTheShapeIsUnknown: without a
// builtGraphFree entry this node never built the artifact, so neither collection
// can be chosen. Doing nothing is the only safe answer, and it must not put the
// version into maintenance.
func TestCollectCandidates_LeavesAGraphedVersionAloneWhenTheShapeIsUnknown(t *testing.T) {
	im := newRebuildTestManager(t, 5)

	im.collectCandidates(context.Background(), []gcCandidate{{KBID: "kb-1", VersionID: 7, DeadShare: 0.9}})

	if im.inMaintenance(indexKey{"kb-1", 7}) {
		t.Fatal("an unknown shape must not take the version out of service")
	}
	if got := im.BlockedCollections(); got != nil {
		t.Fatalf("nothing was attempted, so nothing is blocked: %+v", got)
	}
}
