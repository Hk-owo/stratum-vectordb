package index

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// testDeps wires a MockIndexManager to simple in-memory maps, standing in
// for VersionDocList / ChunkDocMapper / ChunkStore without pulling in
// those packages (keeping this package's tests dependency-light; cross-
// module wiring is exercised later in coordinator/index integration
// tests per Stratum_测试顺序.md T2-2).
type testDeps struct {
	mu       sync.Mutex
	docIDs   map[string][]string             // versionKey -> docIDs
	chunkIDs map[string][]string             // docID -> chunkIDs
	vectors  map[string]map[string][]float32 // kbID -> chunkID -> vector
}

func newTestDeps() *testDeps {
	return &testDeps{
		docIDs:   make(map[string][]string),
		chunkIDs: make(map[string][]string),
		vectors:  make(map[string]map[string][]float32),
	}
}

func versionKey(kbID string, versionID int64) string {
	return fmt.Sprintf("%s#%d", kbID, versionID)
}

func (d *testDeps) setVersionDocs(kbID string, versionID int64, docIDs []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.docIDs[versionKey(kbID, versionID)] = docIDs
}

func (d *testDeps) setDocChunks(docID string, chunkIDs []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.chunkIDs[docID] = chunkIDs
}

func (d *testDeps) setVector(kbID, chunkID string, vector []float32) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.vectors[kbID] == nil {
		d.vectors[kbID] = make(map[string][]float32)
	}
	d.vectors[kbID][chunkID] = vector
}

func (d *testDeps) deps() MockIndexManagerDeps {
	return MockIndexManagerDeps{
		ListDocIDs: func(_ context.Context, kbID string, versionID int64) ([]string, error) {
			d.mu.Lock()
			defer d.mu.Unlock()
			return d.docIDs[versionKey(kbID, versionID)], nil
		},
		ListChunkIDsByDocs: func(_ context.Context, kbID string, docIDs []string) ([]string, error) {
			d.mu.Lock()
			defer d.mu.Unlock()
			seen := make(map[string]struct{})
			var out []string
			for _, docID := range docIDs {
				for _, c := range d.chunkIDs[docID] {
					if _, ok := seen[c]; !ok {
						seen[c] = struct{}{}
						out = append(out, c)
					}
				}
			}
			return out, nil
		},
		ReadChunkVector: func(_ context.Context, kbID, chunkID string) ([]float32, error) {
			d.mu.Lock()
			defer d.mu.Unlock()
			v, ok := d.vectors[kbID][chunkID]
			if !ok {
				return nil, errors.New("chunk vector not found")
			}
			return v, nil
		},
	}
}

func setupSimpleKB(deps *testDeps, kbID string, versionID int64) {
	deps.setVersionDocs(kbID, versionID, []string{"doc1"})
	deps.setDocChunks("doc1", []string{"chunk1"})
	deps.setVector(kbID, "chunk1", []float32{1, 0, 0})
}

func waitForBuild(t *testing.T, m *MockIndexManager, kbID string, versionID int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.IsLoaded(kbID, versionID) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("build for %s/%d did not complete in time", kbID, versionID)
}

func TestMockIndexManager_TriggerBuildThenSearch(t *testing.T) {
	deps := newTestDeps()
	setupSimpleKB(deps, "kb1", 1)
	m := NewMockIndexManager(deps.deps(), 16, time.Second)

	if err := m.TriggerBuild(context.Background(), "kb1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	waitForBuild(t, m, "kb1", 1)

	results, err := m.Search(context.Background(), "kb1", 1, []float32{1, 0, 0}, 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 || results[0].ChunkID != "chunk1" {
		t.Fatalf("Search results = %+v, want [{chunk1, ...}]", results)
	}
	if results[0].Score < 0.99 {
		t.Fatalf("Score = %v, want ~1.0 for identical vector", results[0].Score)
	}
}

func TestMockIndexManager_SearchBeforeBuildFails(t *testing.T) {
	deps := newTestDeps()
	m := NewMockIndexManager(deps.deps(), 16, 100*time.Millisecond)

	_, err := m.Search(context.Background(), "kb1", 1, []float32{1, 0, 0}, 5)
	if !errors.Is(err, stratumerrors.ErrIndexNotReady) {
		t.Fatalf("err = %v, want ErrIndexNotReady", err)
	}
}

func TestMockIndexManager_LRUEviction(t *testing.T) {
	deps := newTestDeps()
	for v := int64(1); v <= 3; v++ {
		setupSimpleKB(deps, "kb1", v)
	}
	m := NewMockIndexManager(deps.deps(), 2, time.Second) // capacity 2

	for v := int64(1); v <= 2; v++ {
		if err := m.TriggerBuild(context.Background(), "kb1", v); err != nil {
			t.Fatalf("TriggerBuild(%d): %v", v, err)
		}
		waitForBuild(t, m, "kb1", v)
		// Touch it via Search so lastAccess ordering is deterministic.
		if _, err := m.Search(context.Background(), "kb1", v, []float32{1, 0, 0}, 1); err != nil {
			t.Fatalf("Search(%d): %v", v, err)
		}
		time.Sleep(2 * time.Millisecond) // ensure distinct lastAccess timestamps
	}

	if got := m.LoadedCount(); got != 2 {
		t.Fatalf("LoadedCount = %d, want 2 before exceeding capacity", got)
	}

	// Building a 3rd version should evict the least-recently-used (v1).
	if err := m.TriggerBuild(context.Background(), "kb1", 3); err != nil {
		t.Fatalf("TriggerBuild(3): %v", err)
	}
	waitForBuild(t, m, "kb1", 3)

	if got := m.LoadedCount(); got != 2 {
		t.Fatalf("LoadedCount after exceeding capacity = %d, want 2 (LRU evicted)", got)
	}
	if m.IsLoaded("kb1", 1) {
		t.Fatalf("version 1 (least recently used) should have been evicted")
	}
	if !m.IsLoaded("kb1", 3) {
		t.Fatalf("version 3 (just built) should be loaded")
	}
}

func TestMockIndexManager_RefCountProtectsFromEviction(t *testing.T) {
	deps := newTestDeps()
	for v := int64(1); v <= 3; v++ {
		setupSimpleKB(deps, "kb1", v)
	}
	m := NewMockIndexManager(deps.deps(), 1, time.Second) // capacity 1 to force contention

	if err := m.TriggerBuild(context.Background(), "kb1", 1); err != nil {
		t.Fatalf("TriggerBuild(1): %v", err)
	}
	waitForBuild(t, m, "kb1", 1)

	// Pin version 1 by acquiring it without releasing yet.
	key := indexKey{"kb1", 1}
	idx, err := m.acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = idx
	if got := m.RefCount("kb1", 1); got != 1 {
		t.Fatalf("RefCount = %d, want 1", got)
	}

	// Build version 2; with capacity 1 and version 1 pinned, eviction of
	// version 1 should NOT happen (it's protected by refcount > 0).
	if err := m.TriggerBuild(context.Background(), "kb1", 2); err != nil {
		t.Fatalf("TriggerBuild(2): %v", err)
	}
	waitForBuild(t, m, "kb1", 2)

	if !m.IsLoaded("kb1", 1) {
		t.Fatalf("version 1 was evicted despite refcount > 0")
	}

	m.release(key)
}

func TestMockIndexManager_ConcurrentSearchTriggersOneBuildOnly(t *testing.T) {
	deps := newTestDeps()
	setupSimpleKB(deps, "kb1", 1)
	m := NewMockIndexManager(deps.deps(), 16, time.Second)

	var buildCount int32
	var wg sync.WaitGroup

	// Wrap ReadChunkVector to count how many times the build pipeline
	// actually executes (a stand-in for "how many times Build was
	// triggered", since each build calls ReadChunkVector once per chunk).
	origDeps := deps.deps()
	countingDeps := MockIndexManagerDeps{
		ListDocIDs:         origDeps.ListDocIDs,
		ListChunkIDsByDocs: origDeps.ListChunkIDsByDocs,
		ReadChunkVector: func(ctx context.Context, kbID, chunkID string) ([]float32, error) {
			atomic.AddInt32(&buildCount, 1)
			return origDeps.ReadChunkVector(ctx, kbID, chunkID)
		},
	}
	m = NewMockIndexManager(countingDeps, 16, time.Second)

	if err := m.TriggerBuild(context.Background(), "kb1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}

	// Fire 20 concurrent searches while the build may still be in flight.
	const n = 20
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := m.Search(context.Background(), "kb1", 1, []float32{1, 0, 0}, 1)
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Search[%d]: %v", i, err)
		}
	}
	if got := atomic.LoadInt32(&buildCount); got != 1 {
		t.Fatalf("ReadChunkVector called %d times across concurrent searches, want exactly 1 (single build, shared by all waiters)", got)
	}
}

func TestMockIndexManager_LoadWaitTimeout(t *testing.T) {
	deps := newTestDeps()
	setupSimpleKB(deps, "kb1", 1)

	// ReadChunkVector blocks "forever" (until test cleanup) to keep the
	// build perpetually in flight, forcing Search to wait out the timeout.
	blockCh := make(chan struct{})
	t.Cleanup(func() { close(blockCh) })

	origDeps := deps.deps()
	slowDeps := MockIndexManagerDeps{
		ListDocIDs:         origDeps.ListDocIDs,
		ListChunkIDsByDocs: origDeps.ListChunkIDsByDocs,
		ReadChunkVector: func(ctx context.Context, kbID, chunkID string) ([]float32, error) {
			<-blockCh
			return origDeps.ReadChunkVector(ctx, kbID, chunkID)
		},
	}
	m := NewMockIndexManager(slowDeps, 16, 50*time.Millisecond)

	if err := m.TriggerBuild(context.Background(), "kb1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}

	start := time.Now()
	_, err := m.Search(context.Background(), "kb1", 1, []float32{1, 0, 0}, 1)
	elapsed := time.Since(start)

	if !errors.Is(err, stratumerrors.ErrIndexLoadTimeout) {
		t.Fatalf("err = %v, want ErrIndexLoadTimeout", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Search took %v, expected to time out near 50ms", elapsed)
	}
}

func TestMockIndexManager_SearchRespectsContextCancellation(t *testing.T) {
	deps := newTestDeps()
	setupSimpleKB(deps, "kb1", 1)

	blockCh := make(chan struct{})
	t.Cleanup(func() { close(blockCh) })

	origDeps := deps.deps()
	slowDeps := MockIndexManagerDeps{
		ListDocIDs:         origDeps.ListDocIDs,
		ListChunkIDsByDocs: origDeps.ListChunkIDsByDocs,
		ReadChunkVector: func(ctx context.Context, kbID, chunkID string) ([]float32, error) {
			<-blockCh
			return origDeps.ReadChunkVector(ctx, kbID, chunkID)
		},
	}
	m := NewMockIndexManager(slowDeps, 16, 10*time.Second) // long timeout; ctx cancellation should win

	if err := m.TriggerBuild(context.Background(), "kb1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := m.Search(ctx, "kb1", 1, []float32{1, 0, 0}, 1)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 1*time.Second {
		t.Fatalf("Search took %v, expected to return promptly on ctx cancellation", elapsed)
	}
}

func TestMockIndexManager_BuildCallback_SuccessAndFailure(t *testing.T) {
	deps := newTestDeps()
	setupSimpleKB(deps, "kb1", 1) // version 1: will succeed
	// version 2: deliberately leave chunk vector missing so build fails.
	deps.setVersionDocs("kb1", 2, []string{"doc2"})
	deps.setDocChunks("doc2", []string{"chunk-missing"})

	m := NewMockIndexManager(deps.deps(), 16, time.Second)

	var mu sync.Mutex
	results := make(map[int64]types.IndexStatus)
	done := make(chan struct{}, 2)
	m.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
		mu.Lock()
		results[versionID] = status
		mu.Unlock()
		done <- struct{}{}
		return nil
	})

	if err := m.TriggerBuild(context.Background(), "kb1", 1); err != nil {
		t.Fatalf("TriggerBuild(1): %v", err)
	}
	if err := m.TriggerBuild(context.Background(), "kb1", 2); err != nil {
		t.Fatalf("TriggerBuild(2): %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for build callbacks")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if results[1] != types.IndexStatusReady {
		t.Fatalf("version 1 callback status = %v, want Ready", results[1])
	}
	if results[2] != types.IndexStatusFailed {
		t.Fatalf("version 2 callback status = %v, want Failed (missing chunk vector)", results[2])
	}
}

func TestMockIndexManager_Ping(t *testing.T) {
	deps := newTestDeps()
	m := NewMockIndexManager(deps.deps(), 16, time.Second)

	if err := m.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := m.LoadedCount(); got != 0 {
		t.Fatalf("LoadedCount after Ping = %d, want 0 (Ping must not load anything)", got)
	}

	injected := errors.New("index manager unhealthy")
	m.SetPingError(injected)
	if err := m.Ping(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("Ping after SetPingError = %v, want injected error", err)
	}
}

func TestMockIndexManager_EvictAndEvictByKB(t *testing.T) {
	deps := newTestDeps()
	setupSimpleKB(deps, "kb1", 1)
	setupSimpleKB(deps, "kb1", 2)
	setupSimpleKB(deps, "kb2", 1)
	m := NewMockIndexManager(deps.deps(), 16, time.Second)

	for _, kv := range [][2]interface{}{{"kb1", int64(1)}, {"kb1", int64(2)}, {"kb2", int64(1)}} {
		kbID := kv[0].(string)
		vID := kv[1].(int64)
		if err := m.TriggerBuild(context.Background(), kbID, vID); err != nil {
			t.Fatalf("TriggerBuild(%s,%d): %v", kbID, vID, err)
		}
		waitForBuild(t, m, kbID, vID)
	}

	if err := m.Evict(context.Background(), "kb1", 1); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if m.IsLoaded("kb1", 1) {
		t.Fatalf("kb1/1 still loaded after Evict")
	}
	if !m.IsLoaded("kb1", 2) || !m.IsLoaded("kb2", 1) {
		t.Fatalf("Evict removed more than the targeted version")
	}

	if err := m.EvictByKB(context.Background(), "kb1"); err != nil {
		t.Fatalf("EvictByKB: %v", err)
	}
	if m.IsLoaded("kb1", 2) {
		t.Fatalf("kb1/2 still loaded after EvictByKB(kb1)")
	}
	if !m.IsLoaded("kb2", 1) {
		t.Fatalf("EvictByKB(kb1) incorrectly evicted kb2's index")
	}
}
