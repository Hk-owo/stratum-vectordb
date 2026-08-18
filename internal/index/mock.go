package index

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// indexKey identifies a single version's index slot.
type indexKey struct {
	kbID      string
	versionID int64
}

// builtIndex is the in-memory representation of a "built" mock index: just
// the chunk vectors, searched by brute-force cosine similarity. This
// stands in for the real VectorIndex (HNSW via vecstore gRPC) — brute
// force is fine for the data volumes unit tests use, and (unlike a real
// HNSW build) gives exact, reproducible nearest-neighbor results, which is
// actually more useful for assertions in WriteCoordinator/QueryService
// tests than approximate search would be.
type builtIndex struct {
	chunks     []types.Chunk
	vectors    map[string][]float32 // chunkID -> vector
	refCount   int
	lastAccess time.Time
}

// MockIndexManager is an in-memory IndexManager for use in unit tests of
// modules that depend on IndexManager (WriteCoordinator, QueryService,
// DeleteCoordinator, etc.). It implements real LRU eviction, reference-
// count eviction protection, and concurrent-load deduplication with
// timeout, because those behaviors are exactly what downstream modules
// (and IndexManager's own future T2-2 suite) need to exercise — a thin
// recorder would not give callers anything meaningful to assert on.
//
// Builds run synchronously inside TriggerBuild's goroutine using injected
// VersionDocList / ChunkDocMapper / ChunkStore-shaped callbacks (see
// NewMockIndexManager), reading whatever those dependencies currently
// contain — this mirrors the real build data flow documented on the
// IndexManager interface without requiring a real vecstore gRPC server.
//
// It is not a substitute for the real implementation's own tests against
// a real vecstore gRPC server with real HNSW (see T2-2 in
// Stratum_测试顺序.md).
type MockIndexManager struct {
	lruCapacity     int
	loadWaitTimeout time.Duration

	listDocIDs         func(ctx context.Context, kbID string, versionID int64) ([]string, error)
	listChunkIDsByDocs func(ctx context.Context, kbID string, docIDs []string) ([]string, error)
	readChunkVector    func(ctx context.Context, kbID, chunkID string) ([]float32, error)

	mu        sync.Mutex
	cond      *sync.Cond
	loaded    map[indexKey]*builtIndex
	loading   map[indexKey]bool // currently being built/loaded; concurrent Search calls wait on cond
	callbacks []BuildCompleteCallback

	pingErr error // injectable for tests exercising HealthCheck DEGRADED/UNHEALTHY paths
}

// MockIndexManagerDeps bundles the data-source callbacks MockIndexManager
// needs to mimic the real build data flow (VersionDocList.ListDocIDs ->
// ChunkDocMapper.ListChunkIDsByDocs -> ChunkStore reads). Tests typically
// wire these directly to *versiondoc.MockVersionDocList,
// *chunkdoc.MockChunkDocMapper, and *chunkstore.MockChunkStore.
type MockIndexManagerDeps struct {
	ListDocIDs         func(ctx context.Context, kbID string, versionID int64) ([]string, error)
	ListChunkIDsByDocs func(ctx context.Context, kbID string, docIDs []string) ([]string, error)
	ReadChunkVector    func(ctx context.Context, kbID, chunkID string) ([]float32, error)
}

// NewMockIndexManager constructs a MockIndexManager. lruCapacity mirrors
// index_manager.lru_capacity; loadWaitTimeout mirrors
// index_manager.load_wait_timeout_ms.
func NewMockIndexManager(deps MockIndexManagerDeps, lruCapacity int, loadWaitTimeout time.Duration) *MockIndexManager {
	m := &MockIndexManager{
		lruCapacity:        lruCapacity,
		loadWaitTimeout:    loadWaitTimeout,
		listDocIDs:         deps.ListDocIDs,
		listChunkIDsByDocs: deps.ListChunkIDsByDocs,
		readChunkVector:    deps.ReadChunkVector,
		loaded:             make(map[indexKey]*builtIndex),
		loading:            make(map[indexKey]bool),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// TriggerBuild synchronously gathers chunk vectors via the injected data-
// source callbacks and stores the resulting built index, then invokes any
// registered callbacks with the resulting status. Unlike the real
// implementation, this runs synchronously relative to the calling
// goroutine being spawned (it still launches the work in its own
// goroutine, so callers see TriggerBuild return before the build
// necessarily completes, mirroring the documented "schedules, does not
// wait" contract) — but with in-memory, low-latency dependencies, builds
// finish essentially immediately, which is what makes this mock useful for
// "TriggerBuild then Search" tests.
func (m *MockIndexManager) TriggerBuild(ctx context.Context, kbID string, versionID int64) error {
	key := indexKey{kbID, versionID}

	m.mu.Lock()
	m.loading[key] = true
	m.mu.Unlock()

	go func() {
		idx, buildErr := m.build(ctx, kbID, versionID)

		m.mu.Lock()
		delete(m.loading, key)
		status := types.IndexStatusReady
		if buildErr != nil {
			status = types.IndexStatusFailed
		} else {
			idx.lastAccess = time.Now()
			// Per the design doc ("索引管理器" 内存换入换出): make room
			// BEFORE inserting the newly built index, evicting from the
			// existing loaded set only. Evicting after insertion would let
			// the brand-new entry itself (refCount 0, since nothing has
			// acquired it yet) be picked as the LRU eviction candidate
			// ahead of genuinely older entries, which is backwards.
			m.makeRoomLocked()
			m.loaded[key] = idx
		}
		callbacks := append([]BuildCompleteCallback(nil), m.callbacks...)
		m.cond.Broadcast() // wake any Search calls waiting on this key
		m.mu.Unlock()

		for _, cb := range callbacks {
			_ = cb(kbID, versionID, status) // mock: callback retry/backoff is the real implementation's concern (see doc comment on IndexManager)
		}
	}()

	return nil
}

func (m *MockIndexManager) build(ctx context.Context, kbID string, versionID int64) (*builtIndex, error) {
	docIDs, err := m.listDocIDs(ctx, kbID, versionID)
	if err != nil {
		return nil, fmt.Errorf("index: ListDocIDs: %w", err)
	}
	chunkIDs, err := m.listChunkIDsByDocs(ctx, kbID, docIDs)
	if err != nil {
		return nil, fmt.Errorf("index: ListChunkIDsByDocs: %w", err)
	}

	vectors := make(map[string][]float32, len(chunkIDs))
	chunks := make([]types.Chunk, 0, len(chunkIDs))
	for _, chunkID := range chunkIDs {
		v, err := m.readChunkVector(ctx, kbID, chunkID)
		if err != nil {
			return nil, fmt.Errorf("index: read chunk vector %s: %w", chunkID, err)
		}
		vectors[chunkID] = v
		chunks = append(chunks, types.Chunk{ChunkID: chunkID})
	}

	return &builtIndex{chunks: chunks, vectors: vectors}, nil
}

// Search waits (if necessary) for the target version's index to be
// available, loading it via TriggerBuild-equivalent logic if it is not
// already in memory or being built, then runs an exact brute-force cosine
// search over the loaded vectors.
func (m *MockIndexManager) Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) {
	key := indexKey{kbID, versionID}

	idx, err := m.acquire(ctx, key)
	if err != nil {
		return nil, err
	}
	defer m.release(key)

	type scored struct {
		chunkID string
		score   float32
	}
	results := make([]scored, 0, len(idx.vectors))
	for chunkID, v := range idx.vectors {
		results = append(results, scored{chunkID: chunkID, score: cosineSimilarity(vector, v)})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].score > results[j].score })
	if topK > 0 && topK < len(results) {
		results = results[:topK]
	}

	out := make([]types.SearchResult, len(results))
	for i, r := range results {
		out[i] = types.SearchResult{ChunkID: r.chunkID, Score: r.score}
	}
	return out, nil
}

// acquire loads (if needed) and pins (ref count += 1) the index for key,
// waiting for an in-flight build/eviction-contention situation to resolve,
// bounded by loadWaitTimeout and ctx.Done() — whichever fires first.
func (m *MockIndexManager) acquire(ctx context.Context, key indexKey) (*builtIndex, error) {
	deadline := time.Now().Add(m.loadWaitTimeout)

	m.mu.Lock()
	for {
		if idx, ok := m.loaded[key]; ok {
			idx.refCount++
			idx.lastAccess = time.Now()
			m.mu.Unlock()
			return idx, nil
		}
		if m.loading[key] {
			// A build is already in flight for this exact key; wait for it.
			if !m.waitLocked(ctx, deadline) {
				m.mu.Unlock()
				return nil, m.waitTimeoutErr(ctx)
			}
			continue
		}
		// Not loaded and not currently building: this mock treats Search
		// against a never-built version as "nothing to load" rather than
		// silently kicking off a build, since the real IndexManager only
		// loads indexes that have already been built to disk at least
		// once (TriggerBuild is what creates that on-disk artifact).
		m.mu.Unlock()
		return nil, fmt.Errorf("index: no built index for kbID=%s versionID=%d: %w", key.kbID, key.versionID, stratumerrors.ErrIndexNotReady)
	}
}

// waitLocked blocks on m.cond until woken, ctx is done, or deadline
// passes, returning false in the latter two cases. Must be called with
// m.mu held; re-acquires it before returning, per sync.Cond.Wait's
// contract.
func (m *MockIndexManager) waitLocked(ctx context.Context, deadline time.Time) bool {
	woken := make(chan struct{})
	timer := time.AfterFunc(time.Until(deadline), func() {
		m.mu.Lock()
		m.cond.Broadcast()
		m.mu.Unlock()
	})
	defer timer.Stop()

	go func() {
		select {
		case <-ctx.Done():
			m.mu.Lock()
			m.cond.Broadcast()
			m.mu.Unlock()
		case <-woken:
		}
	}()
	defer close(woken)

	m.cond.Wait()

	if ctx.Err() != nil {
		return false
	}
	return time.Now().Before(deadline)
}

func (m *MockIndexManager) waitTimeoutErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return stratumerrors.ErrIndexLoadTimeout
}

func (m *MockIndexManager) release(key indexKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx, ok := m.loaded[key]; ok && idx.refCount > 0 {
		idx.refCount--
		m.cond.Broadcast() // a release may free up LRU eviction headroom
	}
}

// makeRoomLocked evicts least-recently-used, ref-count-zero indexes from
// the currently loaded set until there is room for one more entry (i.e.
// len(m.loaded) < lruCapacity), or until no further eviction is possible
// (every remaining index is pinned). It must be called with m.mu held,
// and BEFORE inserting the new entry — see the call site in TriggerBuild's
// completion handler for why ordering matters.
func (m *MockIndexManager) makeRoomLocked() {
	if m.lruCapacity <= 0 {
		return
	}
	for len(m.loaded) >= m.lruCapacity {
		var oldestKey indexKey
		var oldestTime time.Time
		found := false
		for k, idx := range m.loaded {
			if idx.refCount != 0 {
				continue
			}
			if !found || idx.lastAccess.Before(oldestTime) {
				oldestKey = k
				oldestTime = idx.lastAccess
				found = true
			}
		}
		if !found {
			return // every loaded index is pinned; cannot evict further
		}
		delete(m.loaded, oldestKey)
	}
}

func (m *MockIndexManager) RegisterBuildCallback(cb BuildCompleteCallback) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callbacks = append(m.callbacks, cb)
}

func (m *MockIndexManager) Evict(_ context.Context, kbID string, versionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.loaded, indexKey{kbID, versionID})
	return nil
}

func (m *MockIndexManager) EvictByKB(_ context.Context, kbID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.loaded {
		if k.kbID == kbID {
			delete(m.loaded, k)
		}
	}
	return nil
}

// Ping never loads an index or touches reference counts, per the
// documented contract. It returns pingErr if a test has configured one via
// SetPingError, to exercise HealthCheck's DEGRADED/UNHEALTHY paths.
func (m *MockIndexManager) Ping(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pingErr
}

// SetPingError configures the error Ping returns. Not part of the
// IndexManager interface; a test helper for exercising HealthCheck.
func (m *MockIndexManager) SetPingError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pingErr = err
}

// LoadedCount implements IndexManager: returns how many indexes are
// currently in memory.
func (m *MockIndexManager) LoadedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.loaded)
}

// IsLoaded reports whether (kbID, versionID)'s index is currently in
// memory. Not part of the IndexManager interface; a test helper.
func (m *MockIndexManager) IsLoaded(kbID string, versionID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.loaded[indexKey{kbID, versionID}]
	return ok
}

// RefCount returns the current reference count for (kbID, versionID)'s
// index, or 0 if it is not loaded. Not part of the IndexManager interface;
// a test helper for asserting reference-count eviction protection.
func (m *MockIndexManager) RefCount(kbID string, versionID int64) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx, ok := m.loaded[indexKey{kbID, versionID}]; ok {
		return idx.refCount
	}
	return 0
}

func cosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(normA) * math.Sqrt(normB)))
}

var _ IndexManager = (*MockIndexManager)(nil)
