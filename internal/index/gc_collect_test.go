package index

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
)

// gcCountingCounter is a ReplicaCounter that answers a fixed count and records
// who it was told to exclude.
type gcCountingCounter struct {
	others int
	err    error
	asked  []int64
}

func (c *gcCountingCounter) IndexReadyReplicaCount(_ context.Context, _ string, _ int64, except int64) (int, error) {
	c.asked = append(c.asked, except)
	return c.others, c.err
}

// newGCCollectManager builds a manager for the collection tests.
func newGCCollectManager(t *testing.T, cfg IndexManagerConfig) *IndexManagerImpl {
	t.Helper()
	im := NewIndexManager(cfg)
	im.logger = zap.NewNop()
	return im
}

// TestSearch_RefusesWhileTheVersionIsBeingCollected: the maintenance window has to
// be an explicit answer, not a hang and not a stale read. §8.6(d) takes a replica
// out of service on purpose, so it says so — and the sentinel is what lets the
// station tell "briefly out" apart from "gone" and retry elsewhere.
func TestSearch_RefusesWhileTheVersionIsBeingCollected(t *testing.T) {
	im := newGCCollectManager(t, IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: time.Second})
	key := indexKey{"kb-1", 7}

	im.setMaintenance(key, true)
	defer im.setMaintenance(key, false)

	_, err := im.Search(context.Background(), "kb-1", 7, []float32{1, 2}, 5)
	if !errors.Is(err, stratumerrors.ErrIndexMaintenance) {
		t.Fatalf("Search while collecting = %v, want ErrIndexMaintenance", err)
	}
}

// TestCollectCandidates_SkipsWhenTooFewReplicasWouldServe is the rolling rule: the
// control layer is asked how many OTHERS serve the version, and a shortfall means
// nothing is collected — the artifact keeps its dead weight, which is far cheaper
// than dropping a replica the readers needed.
func TestCollectCandidates_SkipsWhenTooFewReplicasWouldServe(t *testing.T) {
	im := newGCCollectManager(t, IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        time.Second,
		IndexDataDir:           t.TempDir(),
		GCEnabled:              true,
		IndexServingReplicaMin: 2,
		NodeID:                 1,
	})
	counter := &gcCountingCounter{others: 1} // one other would remain, two required
	im.SetGCReplicaCounter(counter, 1)

	im.collectCandidates(context.Background(), []gcCandidate{{KBID: "kb-1", VersionID: 7, DeadShare: 0.5}})

	if len(counter.asked) != 1 || counter.asked[0] != 1 {
		t.Fatalf("the control layer was asked to exclude %v, want [1] — the node asking must not count itself",
			counter.asked)
	}
	if im.inMaintenance(indexKey{"kb-1", 7}) {
		t.Fatal("a version skipped for lack of serving replicas must never enter maintenance")
	}
}

// TestCollectCandidates_DoesNothingWhenDisabled: the scanner is a reporter until an
// operator opts in. Asking the control layer at all would be work nobody wanted.
func TestCollectCandidates_DoesNothingWhenDisabled(t *testing.T) {
	im := newGCCollectManager(t, IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: time.Second,
		NodeID:          1,
	})
	counter := &gcCountingCounter{others: 9}
	im.SetGCReplicaCounter(counter, 1)

	im.collectCandidates(context.Background(), []gcCandidate{{KBID: "kb-1", VersionID: 7, DeadShare: 0.5}})

	if len(counter.asked) != 0 {
		t.Fatalf("the control layer must not be asked when collection is off: %v", counter.asked)
	}
}

// TestCollectCandidates_RefusesWithoutATopology: a node that does not know its own
// id cannot tell whether stepping out would leave anyone behind, so it does not
// step out.
func TestCollectCandidates_RefusesWithoutATopology(t *testing.T) {
	im := newGCCollectManager(t, IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        time.Second,
		GCEnabled:              true,
		IndexServingReplicaMin: 2,
		// NodeID deliberately left 0.
	})
	counter := &gcCountingCounter{others: 9}
	im.SetGCReplicaCounter(counter, 0)

	im.collectCandidates(context.Background(), []gcCandidate{{KBID: "kb-1", VersionID: 7, DeadShare: 0.5}})

	if len(counter.asked) != 0 {
		t.Fatalf("without a node identity there is no safe question to ask, got %v", counter.asked)
	}
}

// TestCollectGraphFree_RefusesAVersionOfUnknownShape: the shape check comes before
// the reopen, so a graphed (or unrecognised) version is never disturbed — faiss
// would reject the removal anyway, and reopening it only to fail would take it out
// of service for nothing.
func TestCollectGraphFree_RefusesAVersionOfUnknownShape(t *testing.T) {
	im := newGCCollectManager(t, IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: time.Second,
		IndexDataDir:    t.TempDir(),
	})

	err := im.collectGraphFree(context.Background(), "kb-1", 7, []string{gcChunkID(1)})
	if err == nil {
		t.Fatal("a version of unknown shape must not be reopened")
	}
	if !errors.Is(err, stratumerrors.ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument (a caller-actionable refusal)", err)
	}
	if im.inMaintenance(indexKey{"kb-1", 7}) {
		t.Fatal("refusing before the reopen must not have entered maintenance")
	}
}

// TestCollectGraphFree_ReportsAMissingVecstoreClient: refuses rather than panicking
// on a nil client — a misassembled node should say so, not crash mid-collection.
func TestCollectGraphFree_ReportsAMissingVecstoreClient(t *testing.T) {
	im := newGCCollectManager(t, IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: time.Second,
		IndexDataDir:    t.TempDir(),
	})
	im.mu.Lock()
	im.builtGraphFree[indexKey{"kb-1", 7}] = true
	im.mu.Unlock()

	err := im.collectGraphFree(context.Background(), "kb-1", 7, []string{gcChunkID(1)})
	if err == nil {
		t.Fatal("collecting without a vecstore client must fail")
	}
	if im.inMaintenance(indexKey{"kb-1", 7}) {
		t.Fatal("maintenance must not be left set when the collection never started")
	}
}

// TestMaintenanceFlag_SetAndClear pins the flag's lifecycle, which is what makes
// the refusal above temporary rather than permanent.
func TestMaintenanceFlag_SetAndClear(t *testing.T) {
	im := newGCCollectManager(t, IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: time.Second})
	key := indexKey{"kb-1", 7}

	if im.inMaintenance(key) {
		t.Fatal("a version starts out in service")
	}
	im.setMaintenance(key, true)
	if !im.inMaintenance(key) {
		t.Fatal("setMaintenance(true) must take the version out of service")
	}
	im.setMaintenance(key, false)
	if im.inMaintenance(key) {
		t.Fatal("setMaintenance(false) must put it back in service")
	}
}
