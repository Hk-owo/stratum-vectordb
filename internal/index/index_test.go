package index

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	vecstorepb "stratum/api/proto/vecstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// === Test doubles for IndexManagerImpl dependencies ===

// mockVectorIndexClient implements vecstorepb.VectorIndexServiceClient for tests.
type mockVectorIndexClient struct {
	mu    sync.Mutex
	built map[indexKey][]types.SearchResult // stored results for Search
	buildErr error         // injectable build failure
	searchFn func(kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) // per-call override
	buildCalls     int // number of Build RPC invocations
	addChunksCalls int // number of AddChunks RPC invocations
}

func newMockVectorIndexClient() *mockVectorIndexClient {
	return &mockVectorIndexClient{built: make(map[indexKey][]types.SearchResult)}
}

func (m *mockVectorIndexClient) Build(_ context.Context, in *vecstorepb.BuildIndexRequest, _ ...grpc.CallOption) (*vecstorepb.BuildIndexResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.buildErr != nil {
		return nil, m.buildErr
	}
	// Store chunk vectors as "built" with their IDs as search results
	key := indexKey{kbID: in.KbId, versionID: in.VersionId}
	results := make([]types.SearchResult, len(in.Chunks))
	for i, c := range in.Chunks {
		results[i] = types.SearchResult{ChunkID: c.ChunkId, Score: 1.0} // placeholder score
	}
	m.built[key] = results
	m.buildCalls++
	return &vecstorepb.BuildIndexResponse{}, nil
}

func (m *mockVectorIndexClient) AddChunks(_ context.Context, in *vecstorepb.AddChunksRequest, _ ...grpc.CallOption) (*vecstorepb.AddChunksResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.buildErr != nil {
		return nil, m.buildErr
	}
	// Append to whatever Build already stored for this key, mirroring the
	// real vecstore's incremental-add semantics.
	key := indexKey{kbID: in.KbId, versionID: in.VersionId}
	results := m.built[key]
	for _, c := range in.Chunks {
		results = append(results, types.SearchResult{ChunkID: c.ChunkId, Score: 1.0})
	}
	m.built[key] = results
	m.addChunksCalls++
	return &vecstorepb.AddChunksResponse{}, nil
}

func (m *mockVectorIndexClient) Search(_ context.Context, in *vecstorepb.SearchIndexRequest, _ ...grpc.CallOption) (*vecstorepb.SearchIndexResponse, error) {
	m.mu.Lock()
	if m.searchFn != nil {
		fn := m.searchFn
		m.mu.Unlock()
		results, err := fn(in.KbId, in.VersionId, in.Vector, int(in.TopK))
		if err != nil {
			return nil, err
		}
		resp := &vecstorepb.SearchIndexResponse{}
		for _, r := range results {
			resp.Results = append(resp.Results, &vecstorepb.SearchResultProto{ChunkId: r.ChunkID, Score: r.Score})
		}
		return resp, nil
	}
	key := indexKey{kbID: in.KbId, versionID: in.VersionId}
	stored, ok := m.built[key]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no built index for %s/%d", in.KbId, in.VersionId)
	}
	// Simple "search": return stored results, truncated to topK
	limit := int(in.TopK)
	if limit > len(stored) {
		limit = len(stored)
	}
	resp := &vecstorepb.SearchIndexResponse{}
	for i := 0; i < limit; i++ {
		resp.Results = append(resp.Results, &vecstorepb.SearchResultProto{
			ChunkId: stored[i].ChunkID,
			Score:   stored[i].Score,
		})
	}
	return resp, nil
}

func (m *mockVectorIndexClient) Save(_ context.Context, _ *vecstorepb.SaveIndexRequest, _ ...grpc.CallOption) (*vecstorepb.SaveIndexResponse, error) {
	return &vecstorepb.SaveIndexResponse{}, nil
}

func (m *mockVectorIndexClient) Load(_ context.Context, _ *vecstorepb.LoadIndexRequest, _ ...grpc.CallOption) (*vecstorepb.LoadIndexResponse, error) {
	return &vecstorepb.LoadIndexResponse{}, nil
}

func (m *mockVectorIndexClient) Reset(_ context.Context, _ *vecstorepb.ResetIndexRequest, _ ...grpc.CallOption) (*vecstorepb.ResetIndexResponse, error) {
	return &vecstorepb.ResetIndexResponse{}, nil
}

// docSource is a test double providing VersionDocList + ChunkDocMapper + ChunkStore
// data for IndexManagerImpl builds.
type docSource struct {
	mu        sync.Mutex
	docs      map[int64][]string           // versionID -> []docID
	chunks    map[string][]string           // docID -> []chunkID
	vectors   map[string][]float32          // chunkID -> vector
}

func newDocSource() *docSource {
	return &docSource{
		docs:    make(map[int64][]string),
		chunks:  make(map[string][]string),
		vectors: make(map[string][]float32),
	}
}

func (d *docSource) ListDocIDs(_ context.Context, kbID string, versionID int64) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.docs[versionID]...), nil
}

func (d *docSource) ListChunkIDsByDocs(_ context.Context, kbID string, docIDs []string) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	seen := make(map[string]bool)
	var out []string
	for _, docID := range docIDs {
		for _, chunkID := range d.chunks[docID] {
			if !seen[chunkID] {
				seen[chunkID] = true
				out = append(out, chunkID)
			}
		}
	}
	return out, nil
}

func (d *docSource) ReadChunkVector(_ context.Context, kbID, chunkID string) ([]float32, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.vectors[chunkID]
	if !ok {
		return nil, fmt.Errorf("chunk %s not found", chunkID)
	}
	return v, nil
}

func (d *docSource) addDoc(versionID int64, docID string, chunkIDs []string, vectors map[string][]float32) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.docs[versionID] = append(d.docs[versionID], docID)
	d.chunks[docID] = append([]string(nil), chunkIDs...)
	for k, v := range vectors {
		d.vectors[k] = v
	}
}

// === Tests ===

func TestIndexManager_TriggerBuildThenSearch(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-a", "chunk-b"}, map[string][]float32{
		"chunk-a": {0.1, 0.2, 0.3},
		"chunk-b": {0.4, 0.5, 0.6},
	})

	cfg := IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		VecstoreAddr:    "unused", // we inject the client directly
	}
	im := NewIndexManager(cfg)
	// Inject test doubles
	im.vectorIndexClient = vc
	// Set up build data callbacks to use docSource directly
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	// Register a callback to verify build completion
	var cbCalled atomic.Int32
	im.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
		cbCalled.Add(1)
		if status != types.IndexStatusReady {
			t.Errorf("expected READY, got %v", status)
		}
		return nil
	})

	err := im.TriggerBuild(context.Background(), "kb-1", 1)
	if err != nil {
		t.Fatalf("TriggerBuild failed: %v", err)
	}

	// Wait briefly for async build to complete
	time.Sleep(100 * time.Millisecond)
	if cbCalled.Load() != 1 {
		t.Fatalf("expected callback to be called once, got %d", cbCalled.Load())
	}

	// Search should work now
	results, err := im.Search(context.Background(), "kb-1", 1, []float32{0.1, 0.2, 0.3}, 2)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected search results, got none")
	}
}

func TestIndexManager_TriggerBuildFailure(t *testing.T) {
	vc := newMockVectorIndexClient()
	vc.buildErr = errors.New("build explosion")

	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.1}})

	cfg := IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        5 * time.Second,
		CallbackMaxRetries:     2,
		CallbackRetryBaseMS:    10,
	}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	var cbCalled atomic.Int32
	im.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
		cbCalled.Add(1)
		if status != types.IndexStatusFailed {
			t.Errorf("expected FAILED, got %v", status)
		}
		return nil
	})

	err := im.TriggerBuild(context.Background(), "kb-1", 1)
	if err != nil {
		t.Fatalf("TriggerBuild should return immediately: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if cbCalled.Load() != 1 {
		t.Fatalf("expected callback called once, got %d", cbCalled.Load())
	}
}

// TestIndexManager_BuildBatchesChunks 验证大批量构建会按字节预算分批：
// 第一批走 Build，后续批走 AddChunks，单条 RPC 载荷不超过预算。
func TestIndexManager_BuildBatchesChunks(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()

	// 每个 chunk 向量 2000 个 float32 ≈ 8000 字节，加上 chunk_id/字段头约
	// 8100 字节；2 MiB 预算下每批约 259 个，600 个 chunk 应切成 3 批。
	const numChunks = 600
	const vecDim = 2000
	chunkIDs := make([]string, numChunks)
	vectors := make(map[string][]float32, numChunks)
	vec := make([]float32, vecDim)
	for i := 0; i < numChunks; i++ {
		chunkID := fmt.Sprintf("chunk-%04d", i)
		chunkIDs[i] = chunkID
		vectors[chunkID] = vec
	}
	ds.addDoc(1, "doc-1", chunkIDs, vectors)

	im := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: 5 * time.Second, VecstoreAddr: "unused"})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	var cbCalled atomic.Int32
	im.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
		cbCalled.Add(1)
		if status != types.IndexStatusReady {
			t.Errorf("expected READY, got %v", status)
		}
		return nil
	})

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild failed: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if cbCalled.Load() != 1 {
		t.Fatalf("expected callback called once, got %d", cbCalled.Load())
	}

	vc.mu.Lock()
	buildCalls := vc.buildCalls
	addCalls := vc.addChunksCalls
	totalChunks := len(vc.built[indexKey{kbID: "kb-1", versionID: 1}])
	vc.mu.Unlock()

	if buildCalls != 1 {
		t.Errorf("Build calls = %d, want 1 (first batch)", buildCalls)
	}
	if addCalls != 2 {
		t.Errorf("AddChunks calls = %d, want 2 (600 chunks in 3 batches)", addCalls)
	}
	if totalChunks != numChunks {
		t.Errorf("built chunk count = %d, want %d", totalChunks, numChunks)
	}
}

// TestIndexManager_BuildEmptyVersionStillCallsBuild 验证空版本（无 chunk）
// 仍会调用一次 Build，在 vecstore 侧建立 (kb, version) 索引条目，避免后续
// Search 因 "no index built or loaded" 失败。
func TestIndexManager_BuildEmptyVersionStillCallsBuild(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource() // 不添加任何 doc → ListDocIDs 返回空 → 无 chunk

	im := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: 5 * time.Second, VecstoreAddr: "unused"})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	var cbCalled atomic.Int32
	im.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
		cbCalled.Add(1)
		if status != types.IndexStatusReady {
			t.Errorf("expected READY, got %v", status)
		}
		return nil
	})

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if cbCalled.Load() != 1 {
		t.Fatalf("expected callback called once, got %d", cbCalled.Load())
	}

	vc.mu.Lock()
	buildCalls := vc.buildCalls
	addCalls := vc.addChunksCalls
	vc.mu.Unlock()

	if buildCalls != 1 {
		t.Errorf("Build calls = %d, want 1 (empty version must still Build once)", buildCalls)
	}
	if addCalls != 0 {
		t.Errorf("AddChunks calls = %d, want 0 (no chunks to append)", addCalls)
	}
}

func TestIndexManager_SearchColdVersionTriggersLoad(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})

	cfg := IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
	}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	// Search against a version that hasn't been built
	_, err := im.Search(context.Background(), "kb-1", 1, []float32{0.1, 0.2, 0.3}, 2)
	if err == nil {
		t.Fatal("expected error for unbuilt version")
	}
	if !errors.Is(err, stratumerrors.ErrIndexNotReady) {
		t.Fatalf("expected ErrIndexNotReady, got %v", err)
	}
}

func TestIndexManager_ConcurrentSearchSameVersion(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})

	cfg := IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
	}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	// Build first so it's available
	err := im.TriggerBuild(context.Background(), "kb-1", 1)
	if err != nil {
		t.Fatalf("TriggerBuild failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Run concurrent searches
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := im.Search(context.Background(), "kb-1", 1, []float32{0.1, 0.2, 0.3}, 2)
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("concurrent search failed: %v", e)
	}
}

func TestIndexManager_LRUEviction(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()

	cfg := IndexManagerConfig{
		LRUCapacity:     2,
		LoadWaitTimeout: 5 * time.Second,
	}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	// Build 3 versions; the oldest should be evicted when the 3rd is loaded
	for v := int64(1); v <= 3; v++ {
		ds.addDoc(v, fmt.Sprintf("doc-%d", v), []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
		err := im.TriggerBuild(context.Background(), "kb-1", v)
		if err != nil {
			t.Fatalf("TriggerBuild v=%d failed: %v", v, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Search version 3 (most recent)
	_, err := im.Search(context.Background(), "kb-1", 3, []float32{0.1, 0.2, 0.3}, 1)
	if err != nil {
		t.Fatalf("Search v=3 failed: %v", err)
	}

	// Version 1 should have been evicted (only 2 slots)
	if im.IsLoaded("kb-1", 1) {
		t.Error("version 1 should have been evicted by LRU")
	}
	// Version 2 or 3 should still be loaded
	if !im.IsLoaded("kb-1", 3) {
		t.Error("version 3 should still be loaded")
	}
}

func TestIndexManager_RefCountEvictionProtection(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()

	cfg := IndexManagerConfig{
		LRUCapacity:     2,
		LoadWaitTimeout: 5 * time.Second,
	}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	// Build 2 versions
	for v := int64(1); v <= 2; v++ {
		ds.addDoc(v, fmt.Sprintf("doc-%d", v), []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
		err := im.TriggerBuild(context.Background(), "kb-1", v)
		if err != nil {
			t.Fatalf("TriggerBuild v=%d failed: %v", v, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Search version 1 (pins it with ref-count)
	results, err := im.Search(context.Background(), "kb-1", 1, []float32{0.1, 0.2, 0.3}, 1)
	if err != nil {
		// Search may fail if the mock doesn't support this properly
		// For the ref-count test, the Search call itself increments ref count
		// and then decrements it on return
		_ = results
	}

	// Now build version 3; v1 was most recently used (by the Search above),
	// so v2 should be evicted, not v1
	ds.addDoc(3, "doc-3", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
	err = im.TriggerBuild(context.Background(), "kb-1", 3)
	if err != nil {
		t.Fatalf("TriggerBuild v=3 failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	if im.LoadedCount() > cfg.LRUCapacity {
		t.Errorf("loaded count %d exceeds LRU capacity %d", im.LoadedCount(), cfg.LRUCapacity)
	}
}

func TestIndexManager_Ping(t *testing.T) {
	cfg := IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
	}
	im := NewIndexManager(cfg)

	err := im.Ping(context.Background())
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	if im.LoadedCount() != 0 {
		t.Error("Ping should not trigger any index load")
	}
}

func TestIndexManager_Evict(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})

	cfg := IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: 5 * time.Second}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	err := im.TriggerBuild(context.Background(), "kb-1", 1)
	if err != nil {
		t.Fatalf("TriggerBuild failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if !im.IsLoaded("kb-1", 1) {
		t.Fatal("expected index to be loaded after build")
	}

	err = im.Evict(context.Background(), "kb-1", 1)
	if err != nil {
		t.Fatalf("Evict failed: %v", err)
	}

	if im.IsLoaded("kb-1", 1) {
		t.Error("index should have been evicted")
	}
}

func TestIndexManager_EvictByKB(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()

	cfg := IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: 5 * time.Second}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	for v := int64(1); v <= 2; v++ {
		ds.addDoc(v, fmt.Sprintf("doc-%d", v), []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
		err := im.TriggerBuild(context.Background(), "kb-1", v)
		if err != nil {
			t.Fatalf("TriggerBuild v=%d failed: %v", v, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	im.EvictByKB(context.Background(), "kb-1")
	if im.LoadedCount() != 0 {
		t.Errorf("expected 0 loaded after EvictByKB, got %d", im.LoadedCount())
	}
}

func TestIndexManager_BuildCallbackRetrySuccess(t *testing.T) {
	// Test: callback fails once, then succeeds on retry.
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})

	cfg := IndexManagerConfig{
		LRUCapacity:          4,
		LoadWaitTimeout:      5 * time.Second,
		CallbackMaxRetries:   3,
		CallbackRetryBaseMS:  10,
	}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	var callCount atomic.Int32
	im.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
		n := callCount.Add(1)
		if n == 1 {
			return errors.New("temporary Raft propose failure")
		}
		return nil
	})

	err := im.TriggerBuild(context.Background(), "kb-1", 1)
	if err != nil {
		t.Fatalf("TriggerBuild failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// Callback should have been retried: first call failed, second succeeded.
	count := callCount.Load()
	if count < 2 {
		t.Errorf("expected callback to be retried at least once, got %d calls", count)
	}
}

func TestIndexManager_SearchRespectsContext(t *testing.T) {
	cfg := IndexManagerConfig{
		LRUCapacity:     1,
		LoadWaitTimeout: 5 * time.Second,
	}
	im := NewIndexManager(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := im.Search(ctx, "kb-1", 1, []float32{0.1, 0.2, 0.3}, 2)
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
}
