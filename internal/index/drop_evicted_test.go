package index

import (
	"context"
	"testing"
	"time"
)

// H5 of docs/code-review-2026-09-24.md: the vecstore holds its own object per
// resident index, and nothing in this node's bookkeeping pruning reached it — so
// a process's RSS tracked how many versions had been touched rather than how many
// it held. These tests pin the three paths that must tell it.

// newDropTestManager builds a manager over a doc source with one document, enough
// to build a version and watch what the manager tells the vecstore afterwards.
func newDropTestManager(t *testing.T, lruCapacity int) (*IndexManagerImpl, *mockVectorIndexClient, *docSource) {
	t.Helper()
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
	ds.addDoc(2, "doc-2", []string{"chunk-y"}, map[string][]float32{"chunk-y": {0.4, 0.6}})

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     lruCapacity,
		LoadWaitTimeout: 5 * time.Second,
		IndexDataDir:    t.TempDir(),
	})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector
	t.Cleanup(func() { _ = im.Close() })
	return im, vc, ds
}

// droppedKeys snapshots what the vecstore was told to release.
func (m *mockVectorIndexClient) droppedKeys() []indexKey {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]indexKey(nil), m.dropped...)
}

func TestEvictByKBFreesTheVecstoreIndexObject(t *testing.T) {
	im, vc, _ := newDropTestManager(t, 4)

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	waitLoaded(t, im, "kb-1", 1)

	if err := im.EvictByKB(context.Background(), "kb-1"); err != nil {
		t.Fatalf("EvictByKB: %v", err)
	}

	if got := vc.droppedKeys(); len(got) != 1 || got[0] != (indexKey{"kb-1", 1}) {
		t.Fatalf("vecstore drops after EvictByKB = %v, want [kb-1/1]", got)
	}
}

func TestDiscardFreesTheVecstoreIndexObject(t *testing.T) {
	im, vc, _ := newDropTestManager(t, 4)

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	waitLoaded(t, im, "kb-1", 1)

	if err := im.Discard(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("Discard: %v", err)
	}

	if got := vc.droppedKeys(); len(got) != 1 || got[0] != (indexKey{"kb-1", 1}) {
		t.Fatalf("vecstore drops after Discard = %v, want [kb-1/1]", got)
	}
}

// The LRU path is the one that runs on an ordinary busy node: a version falls out
// of the in-memory set and its vecstore object has to go with it.
func TestLRUEvictionFreesTheVecstoreIndexObject(t *testing.T) {
	im, vc, _ := newDropTestManager(t, 1)

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild(kb-1/1): %v", err)
	}
	waitLoaded(t, im, "kb-1", 1)

	// A second version with LRUCapacity=1 must evict the first to make room.
	if err := im.TriggerBuild(context.Background(), "kb-2", 2); err != nil {
		t.Fatalf("TriggerBuild(kb-2/2): %v", err)
	}
	waitLoaded(t, im, "kb-2", 2)

	got := vc.droppedKeys()
	if len(got) != 1 || got[0] != (indexKey{"kb-1", 1}) {
		t.Fatalf("vecstore drops after LRU eviction = %v, want [kb-1/1]", got)
	}
}

// A failed Drop is a leak, not a failed operation: the version is already gone
// from this node's view and its artifact is still on disk, so EvictByKB must not
// start failing its caller over it.
func TestEvictByKBStaysSuccessfulWhenTheVecstoreDropFails(t *testing.T) {
	im, vc, _ := newDropTestManager(t, 4)

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	waitLoaded(t, im, "kb-1", 1)

	vc.mu.Lock()
	vc.dropErr = context.DeadlineExceeded
	vc.mu.Unlock()

	if err := im.EvictByKB(context.Background(), "kb-1"); err != nil {
		t.Fatalf("EvictByKB must not fail because a best-effort Drop did: %v", err)
	}
	if im.LoadedCount() != 0 {
		t.Errorf("the local evict must still have happened, loaded=%d", im.LoadedCount())
	}
}

// The H5 fix must not create a worse bug than the leak it closes: eviction and the
// Drop that follows are not atomic with respect to a concurrent load. A version
// evicted here can be loaded again (by a query, or by the catch-up after a snapshot
// install) before the Drop runs, and dropping THAT object leaves this node
// believing it holds an index the vecstore no longer has — a node whose own state
// says READY answering "index not ready"
// (TestRealStack_ThreeNodeCluster_SnapshotPipeline is where it showed up).
func TestDropEvictedSkipsAVersionThatWasLoadedAgain(t *testing.T) {
	im, vc, _ := newDropTestManager(t, 4)

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	waitLoaded(t, im, "kb-1", 1)

	// Still loaded: exactly the state a concurrent load leaves behind.
	im.dropEvicted([]indexKey{{"kb-1", 1}})

	if vc.dropCalls != 0 {
		t.Fatalf("dropped a version that is loaded again (calls=%d)", vc.dropCalls)
	}
}

// And the ordinary case still drops: an eviction whose version really is gone from
// this node's view is memory to reclaim.
func TestDropEvictedDropsAVersionThatIsNoLongerLoaded(t *testing.T) {
	im, vc, _ := newDropTestManager(t, 4)

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	waitLoaded(t, im, "kb-1", 1)
	if err := im.Evict(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("Evict: %v", err)
	}

	im.dropEvicted([]indexKey{{"kb-1", 1}})

	if got := vc.droppedKeys(); len(got) != 1 || got[0] != (indexKey{"kb-1", 1}) {
		t.Fatalf("vecstore drops = %v, want [kb-1/1]", got)
	}
}

// waitLoaded blocks until the (asynchronous) build has published the version.
func waitLoaded(t *testing.T, im *IndexManagerImpl, kbID string, versionID int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if im.IsLoaded(kbID, versionID) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("index %s/%d was never loaded", kbID, versionID)
}
