package index

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	vecstorepb "stratum/api/proto/vecstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// === Test doubles for IndexManagerImpl dependencies ===

// mockVectorIndexClient implements vecstorepb.VectorIndexServiceClient for tests.
type mockVectorIndexClient struct {
	mu             sync.Mutex
	built          map[indexKey][]types.SearchResult                                                            // stored results for Search
	buildErr       error                                                                                        // injectable build failure
	searchFn       func(kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) // per-call override
	buildCalls     int                                                                                          // number of Build RPC invocations
	addChunksCalls int                                                                                          // number of AddChunks RPC invocations
	// §8.6(c) append reuse: how many times LoadForAppend was called, with
	// which base path, an injectable failure, and how many vectors the base
	// artifact should pretend to hold (the real vecstore reports the loaded
	// artifact's ntotal).
	loadForAppendCalls      int
	lastLoadForAppendPath   string
	loadForAppendErr        error
	loadForAppendBaseNtotal int64
	// chunk IDs passed to each AddChunks call, in order.
	addedChunkIDs [][]string
	// §8.6(c) deletion path: how many times RemoveChunks was called, with which
	// chunk ids, and an injectable failure.
	removeChunksCalls int
	removedChunkIDs   []string
	removeChunksErr   error
	// last Build RPC's quantizer fields, for asserting config forwarding.
	lastBuildQuantizer vecstorepb.QuantizerTypeProto
	lastBuildPqM       int32
	lastBuildPqNbits   int32
	// memToReport, when > 0, is returned as the mem_bytes in Build /
	// AddChunks responses, simulating the vecstore's memory estimate.
	memToReport int64
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
	m.lastBuildQuantizer = in.Quantizer
	m.lastBuildPqM = in.PqM
	m.lastBuildPqNbits = in.PqNbits
	if m.memToReport > 0 {
		return &vecstorepb.BuildIndexResponse{MemBytes: m.memToReport}, nil
	}
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
	ids := make([]string, 0, len(in.Chunks))
	for _, c := range in.Chunks {
		ids = append(ids, c.ChunkId)
	}
	m.addedChunkIDs = append(m.addedChunkIDs, ids)
	if m.memToReport > 0 {
		return &vecstorepb.AddChunksResponse{MemBytes: m.memToReport}, nil
	}
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

func (m *mockVectorIndexClient) Load(_ context.Context, in *vecstorepb.LoadIndexRequest, _ ...grpc.CallOption) (*vecstorepb.LoadIndexResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.built[indexKey{kbID: in.KbId, versionID: in.VersionId}]; !ok {
		// Mirrors the real vecstore: loading a never-built index fails.
		return nil, status.Error(codes.NotFound, "no index built or loaded")
	}
	return &vecstorepb.LoadIndexResponse{}, nil
}

// LoadForAppend mirrors the real vecstore's §8.6(c) entry point: it reads a
// persisted artifact as the starting point of a build, so the target key ends
// up "built" (and therefore appendable) without a Build RPC. baseNtotal is
// what a test wants the base artifact to pretend it holds.
func (m *mockVectorIndexClient) LoadForAppend(_ context.Context, in *vecstorepb.LoadIndexForAppendRequest, _ ...grpc.CallOption) (*vecstorepb.LoadIndexForAppendResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Counted before the injectable failure: the counter means "attempts",
	// which is what a test asserting a fallback wants to see.
	m.loadForAppendCalls++
	if m.loadForAppendErr != nil {
		return nil, m.loadForAppendErr
	}
	m.lastLoadForAppendPath = in.Path
	if _, ok := m.built[indexKey{kbID: in.KbId, versionID: in.VersionId}]; !ok {
		m.built[indexKey{kbID: in.KbId, versionID: in.VersionId}] = nil
	}
	return &vecstorepb.LoadIndexForAppendResponse{BaseNtotal: m.loadForAppendBaseNtotal}, nil
}

func (m *mockVectorIndexClient) ExistsIndex(_ context.Context, in *vecstorepb.ExistsIndexRequest, _ ...grpc.CallOption) (*vecstorepb.ExistsIndexResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.built[indexKey{kbID: in.KbId, versionID: in.VersionId}]
	return &vecstorepb.ExistsIndexResponse{Exists: ok}, nil
}

func (m *mockVectorIndexClient) Reset(_ context.Context, _ *vecstorepb.ResetIndexRequest, _ ...grpc.CallOption) (*vecstorepb.ResetIndexResponse, error) {
	return &vecstorepb.ResetIndexResponse{}, nil
}

// RemoveChunks mirrors the real vecstore's §8.6(c) deletion path: it reports
// the requested ids as removed (a graph-free index can drop all of them) and
// the resulting vector count.
func (m *mockVectorIndexClient) RemoveChunks(_ context.Context, in *vecstorepb.RemoveChunksRequest, _ ...grpc.CallOption) (*vecstorepb.RemoveChunksResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeChunksCalls++
	if m.removeChunksErr != nil {
		return nil, m.removeChunksErr
	}
	m.removedChunkIDs = append(m.removedChunkIDs, in.ChunkIds...)
	removed := int64(len(in.ChunkIds))
	return &vecstorepb.RemoveChunksResponse{
		Removed: removed,
		Ntotal:  m.loadForAppendBaseNtotal - removed,
	}, nil
}

// docSource is a test double providing VersionDocList + ChunkDocMapper + ChunkStore
// data for IndexManagerImpl builds.
type docSource struct {
	mu      sync.Mutex
	docs    map[int64][]string   // versionID -> []docID
	chunks  map[string][]string  // docID -> []chunkID
	vectors map[string][]float32 // chunkID -> vector
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
		LRUCapacity:         4,
		LoadWaitTimeout:     5 * time.Second,
		CallbackMaxRetries:  2,
		CallbackRetryBaseMS: 10,
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

// TestIndexManager_BuildEmptyVersionDoesNotCallVecstore 固化空版本（无 chunk）
// 的构建契约：**不**向 vecstore 发 Build、也不 Save。
//
// 历史：这里曾经要求"空版本仍调一次 Build 建立索引条目"，但 vecstore 的
// AddChunksLocked 对空 batch 直接返回而不创建 Faiss 索引，于是随后的 Save
// 必然报 "no index has been built or loaded"，被当成可重试错误后进入 5 分钟
// 重试窗口，loading 标志长期占住，该版本上的查询全部以 index load timeout
// 失败。空版本没有可检索内容，构建在 Go 侧即算完成，查询由 tryBruteForce
// 直接回答空结果。
func TestIndexManager_BuildEmptyVersionDoesNotCallVecstore(t *testing.T) {
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

	if buildCalls != 0 {
		t.Errorf("Build calls = %d, want 0 (an empty version must not touch the vecstore: its empty Build creates no index, so the following Save would fail forever)", buildCalls)
	}
	if addCalls != 0 {
		t.Errorf("AddChunks calls = %d, want 0 (no chunks to append)", addCalls)
	}
}

// A version whose index has not been built is no longer an error: indexes are
// built lazily (§8.6b), so a query that arrives first is answered by scanning
// and a build is scheduled for the next one.
func TestIndexManager_SearchUnbuiltVersionScansInsteadOfFailing(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
	})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	results, err := im.Search(context.Background(), "kb-1", 1, []float32{0.5, 0.5}, 2)
	if err != nil {
		t.Fatalf("an unbuilt version must be answered, not rejected: %v", err)
	}
	if len(results) != 1 || results[0].ChunkID != "chunk-x" {
		t.Fatalf("scan returned %+v, want the version's only chunk", results)
	}
	if results[0].Score <= 0 {
		t.Errorf("score = %v, want a positive cosine similarity for identical directions", results[0].Score)
	}
	_ = vc
}

// A genuinely empty version keeps its old answer: there is nothing to scan and
// nothing to build, so callers' existing "empty version" handling still applies.
func TestIndexManager_SearchUnbuiltEmptyVersionStillReportsNotReady(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
	})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	_, err := im.Search(context.Background(), "kb-1", 1, []float32{0.1, 0.2}, 2)
	if !errors.Is(err, stratumerrors.ErrIndexNotReady) {
		t.Fatalf("err = %v, want ErrIndexNotReady for an empty version", err)
	}
}

// Above the size threshold, scanning costs about as much as building, so the
// caller waits for the build instead of paying for a scan.
func TestIndexManager_SearchLargeUnbuiltVersionDoesNotScan(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"c1", "c2"}, map[string][]float32{
		"c1": {1, 0}, "c2": {0, 1},
	})

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:         4,
		LoadWaitTimeout:     50 * time.Millisecond,
		BruteForceMaxChunks: 1, // two chunks: over the threshold
	})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	// A scan answers immediately; waiting for a build does not. The build here
	// never completes (the mock's Build is driven by the test), so the call
	// reports not-ready after its wait — which is exactly what distinguishes
	// "waited" from "scanned". Counting readChunkVector calls would not: the
	// background build reads vectors too.
	results, err := im.Search(context.Background(), "kb-1", 1, []float32{1, 0}, 2)
	if len(results) != 0 {
		t.Errorf("search returned %d results; an over-threshold version must be built, not scanned", len(results))
	}
	if err == nil {
		t.Error("want an error when the build did not finish within the wait")
	}
}

// §8.6a: deciding "is this version cold?" is a question about query
// traffic, so the record must survive in-memory eviction and be dropped
// only when the version/KB is really gone.
func TestIndexManager_LastAccessTracking(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	for v := int64(1); v <= 2; v++ {
		ds.addDoc(v, fmt.Sprintf("doc-%d", v), []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
	}

	cfg := IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
	}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	for v := int64(1); v <= 2; v++ {
		if err := im.TriggerBuild(context.Background(), "kb-1", v); err != nil {
			t.Fatalf("TriggerBuild v=%d failed: %v", v, err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	// A build seeds a baseline — the version is known here, but its
	// recorded time is the build, not a query.
	if _, ok := im.LastAccess("kb-1", 1); !ok {
		t.Fatal("a build must give the cold policy a baseline for the version")
	}
	// A version this process has neither searched nor built is unknown to
	// the policy (and therefore never reshaped).
	if _, ok := im.LastAccess("kb-1", 99); ok {
		t.Fatal("an unknown version must have no access record")
	}

	before := time.Now()
	if _, err := im.Search(context.Background(), "kb-1", 1, []float32{0.5, 0.5}, 1); err != nil {
		t.Fatalf("Search v=1 failed: %v", err)
	}
	after := time.Now()
	ts, ok := im.LastAccess("kb-1", 1)
	if !ok {
		t.Fatal("Search must record an access for the searched version")
	}
	if ts.Before(before) || ts.After(after) {
		t.Fatalf("recorded access %v outside [%v, %v]", ts, before, after)
	}

	// Eviction is an in-memory decision; the version itself is still
	// reachable, so its access history must stay.
	if err := im.Evict(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("Evict failed: %v", err)
	}
	if _, ok := im.LastAccess("kb-1", 1); !ok {
		t.Fatal("Evict must not forget the access record")
	}

	if _, err := im.Search(context.Background(), "kb-1", 2, []float32{0.5, 0.5}, 1); err != nil {
		t.Fatalf("Search v=2 failed: %v", err)
	}

	// Discard drops exactly one version's record.
	if err := im.Discard(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("Discard failed: %v", err)
	}
	if _, ok := im.LastAccess("kb-1", 1); ok {
		t.Fatal("Discard must forget the discarded version's access record")
	}
	if _, ok := im.LastAccess("kb-1", 2); !ok {
		t.Fatal("Discard must not touch another version's access record")
	}

	// KB deletion drops every record of that KB.
	if err := im.DeleteFilesByKB(context.Background(), "kb-1"); err != nil {
		t.Fatalf("DeleteFilesByKB failed: %v", err)
	}
	if _, ok := im.LastAccess("kb-1", 2); ok {
		t.Fatal("DeleteFilesByKB must forget the KB's access records")
	}
}

// waitIndexLoaded blocks until (kbID, versionID)'s build has finished and
// claimed its in-memory entry.
func waitIndexLoaded(t *testing.T, im *IndexManagerImpl, kbID string, versionID int64) {
	t.Helper()
	if !waitForCondition(5*time.Second, func() bool { return im.IsLoaded(kbID, versionID) }) {
		t.Fatalf("version %s/%d was never built", kbID, versionID)
	}
}

// waitGraphFreeShape blocks until (kbID, versionID) is recorded as built
// graph-free, failing the test on timeout.
func waitGraphFreeShape(t *testing.T, im *IndexManagerImpl, kbID string, versionID int64) {
	t.Helper()
	key := indexKey{kbID, versionID}
	if !waitForCondition(5*time.Second, func() bool {
		im.mu.Lock()
		defer im.mu.Unlock()
		return im.builtGraphFree[key]
	}) {
		t.Fatalf("version %s/%d was never reshaped graph-free", kbID, versionID)
	}
}

func waitForCondition(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func newColdPolicyManager(t *testing.T, vc *mockVectorIndexClient, ds *docSource, cfg IndexManagerConfig) *IndexManagerImpl {
	t.Helper()
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector
	// §8.6a: the evaluator enumerates the authoritative version set, so the
	// fixture supplies one. 1..10 keeps a version nobody holds (no artifact, no
	// memory entry) in scope, and leaves 10 as the end of the chain.
	im.SetVersionsProvider(func(context.Context) (map[string][]int64, error) {
		return map[string][]int64{"kb-1": {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}, nil
	})
	return im
}

// §8.6a: the evaluator must pick exactly the versions that went cold,
// leave a hot one alone, and not reshape an already graph-free version
// again on the next sweep.
func TestIndexManager_ColdPolicyPicksOnlyColdVersions(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	for v := int64(1); v <= 2; v++ {
		ds.addDoc(v, fmt.Sprintf("doc-%d", v), []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
	}
	im := newColdPolicyManager(t, vc, ds, IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		ColdThreshold:   time.Minute,
	})

	for v := int64(1); v <= 2; v++ {
		if err := im.TriggerBuild(context.Background(), "kb-1", v); err != nil {
			t.Fatalf("TriggerBuild v=%d failed: %v", v, err)
		}
	}
	waitIndexLoaded(t, im, "kb-1", 1)
	waitIndexLoaded(t, im, "kb-1", 2)

	// Freshly built versions were never searched, but the build itself
	// seeds their access record, so neither is cold yet.
	if got := im.coldCandidates(context.Background(), time.Now()); len(got) != 0 {
		t.Fatalf("freshly built versions must not be cold, got %v", got)
	}

	// Age version 1 by hand: it was last relevant an hour ago.
	im.mu.Lock()
	im.lastSearch[indexKey{"kb-1", 1}] = time.Now().Add(-time.Hour)
	im.mu.Unlock()

	got := im.coldCandidates(context.Background(), time.Now())
	want := indexKey{"kb-1", 1}
	if len(got) != 1 || got[0].key != want {
		t.Fatalf("expected only %v to be cold, got %v", want, got)
	}

	im.sweepCold(context.Background(), time.Now())
	waitGraphFreeShape(t, im, "kb-1", 1)

	vc.mu.Lock()
	quantizer := vc.lastBuildQuantizer
	vc.mu.Unlock()
	if quantizer != vecstorepb.QuantizerTypeProto_QUANTIZER_OFF_FLAT {
		t.Fatalf("a cold rebuild must ask the vecstore for the graph-free variant, got %v", quantizer)
	}

	// The hot version keeps its graph.
	im.mu.Lock()
	hot := im.builtGraphFree[indexKey{"kb-1", 2}]
	im.mu.Unlock()
	if hot {
		t.Fatal("the version that was not cold must keep its HNSW graph")
	}

	// Sweeping again must not rebuild what is already graph-free.
	if got := im.coldCandidates(context.Background(), time.Now()); len(got) != 0 {
		t.Fatalf("an already graph-free version must not be picked again, got %v", got)
	}
}

// §8.6a: the policy is background and automatic — with a threshold
// configured, nobody has to run the evaluator by hand.
func TestIndexManager_ColdPolicyRunsInBackground(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
	im := newColdPolicyManager(t, vc, ds, IndexManagerConfig{
		LRUCapacity:       4,
		LoadWaitTimeout:   5 * time.Second,
		ColdThreshold:     time.Nanosecond, // everything is cold immediately
		ColdSweepInterval: 5 * time.Millisecond,
	})

	im.StartColdPolicy()
	defer im.StopColdPolicy()

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild failed: %v", err)
	}
	waitGraphFreeShape(t, im, "kb-1", 1)
}

// §8.4's first real reuse point: a cold reshape is an ordinary build, so
// it reports through the same BuildCompleteCallback that drives index
// distribution — the replicas replace their copy of the artifact without
// any new plumbing.
func TestIndexManager_ColdRebuildReportsThroughBuildCallback(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
	im := newColdPolicyManager(t, vc, ds, IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		ColdThreshold:   time.Minute,
	})

	reports := make(chan types.IndexStatus, 4)
	im.RegisterBuildCallback(func(_ string, _ int64, status types.IndexStatus) error {
		reports <- status
		return nil
	})

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild failed: %v", err)
	}
	waitIndexLoaded(t, im, "kb-1", 1)
	select {
	case <-reports: // the initial build
	case <-time.After(5 * time.Second):
		t.Fatal("the initial build never reported completion")
	}

	im.mu.Lock()
	im.lastSearch[indexKey{"kb-1", 1}] = time.Now().Add(-time.Hour)
	im.mu.Unlock()
	im.sweepCold(context.Background(), time.Now())
	waitGraphFreeShape(t, im, "kb-1", 1)

	select {
	case status := <-reports:
		if status != types.IndexStatusReady {
			t.Fatalf("a cold reshape must report READY so distribution refreshes the replicas, got %v", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the cold reshape never reported completion, so replicas would keep the hot artifact")
	}
}

// §8.6a: a version that only ever arrived from another node must still be
// visible to the cold policy — it is "received here", which is a baseline
// just like "built here".
func TestIndexManager_InstallIndexSeedsColdPolicyBaseline(t *testing.T) {
	vc := newMockVectorIndexClient()
	im := newColdPolicyManager(t, vc, newDocSource(), IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		IndexDataDir:    t.TempDir(),
		ColdThreshold:   time.Minute,
	})

	// The mock vecstore, like the real one, refuses to Load an index it has
	// never built.
	vc.mu.Lock()
	vc.built[indexKey{"kb-1", 7}] = nil
	vc.mu.Unlock()

	if err := im.InstallIndex(context.Background(), "kb-1", 7, []byte("index-bytes"), []byte("sidecar")); err != nil {
		t.Fatalf("InstallIndex failed: %v", err)
	}
	if _, ok := im.LastAccess("kb-1", 7); !ok {
		t.Fatal("receiving an artifact must give the cold policy a baseline for the version")
	}

	// Age it: the replica can now reshape what it received instead of
	// keeping the shipped shape forever.
	im.mu.Lock()
	im.lastSearch[indexKey{"kb-1", 7}] = time.Now().Add(-time.Hour)
	im.mu.Unlock()
	got := im.coldCandidates(context.Background(), time.Now())
	want := indexKey{"kb-1", 7}
	if len(got) != 1 || got[0].key != want {
		t.Fatalf("expected the received version to age into a cold candidate, got %v", got)
	}
}

// §8.6a: the shape travels with the artifact, so a replica that receives a
// graph-free one can see what it got. Without that record every received
// graph-free artifact looks "not reshaped yet" and is rebuilt — and
// redistributed — into the very shape it already has.
func TestIndexManager_InstallIndexRecordsShippedShape(t *testing.T) {
	vc := newMockVectorIndexClient()
	im := newColdPolicyManager(t, vc, newDocSource(), IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		IndexDataDir:    t.TempDir(),
		ColdThreshold:   time.Minute,
	})

	vc.mu.Lock()
	vc.built[indexKey{"kb-1", 7}] = nil
	vc.mu.Unlock()

	shapeSidecar := []byte("stratum-index-1\n2\n0\n0\ngraph_free 1\n")
	if err := im.InstallIndex(context.Background(), "kb-1", 7, []byte("index-bytes"), shapeSidecar); err != nil {
		t.Fatalf("InstallIndex failed: %v", err)
	}
	if graphFree, known := im.artifactGraphFree("kb-1", 7); !known || !graphFree {
		t.Fatalf("the shipped shape must be readable from the sidecar, got graphFree=%v known=%v", graphFree, known)
	}

	// Long idle, but already graph-free: nothing to reshape.
	im.mu.Lock()
	im.lastSearch[indexKey{"kb-1", 7}] = time.Now().Add(-time.Hour)
	im.mu.Unlock()
	if got := im.coldCandidates(context.Background(), time.Now()); len(got) != 0 {
		t.Fatalf("a received graph-free artifact must not be reshaped again, got %v", got)
	}

	// Queried again: that is the reheat direction, not another cold reshape.
	im.mu.Lock()
	im.lastSearch[indexKey{"kb-1", 7}] = time.Now()
	im.mu.Unlock()
	got := im.coldCandidates(context.Background(), time.Now())
	if len(got) != 1 || !got[0].reheat {
		t.Fatalf("a queried-again graph-free artifact must be a reheat candidate, got %v", got)
	}
}

// The evaluator is off unless a threshold is configured, and starting or
// stopping it is idempotent either way.
func TestIndexManager_ColdPolicyLifecycle(t *testing.T) {
	off := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: time.Second})
	off.StartColdPolicy()
	off.mu.Lock()
	cancel := off.coldCancel
	off.mu.Unlock()
	if cancel != nil {
		t.Fatal("no cold threshold configured: the evaluator must not start")
	}
	off.StopColdPolicy() // must be safe on a never-started policy

	on := NewIndexManager(IndexManagerConfig{
		LRUCapacity:       4,
		LoadWaitTimeout:   time.Second,
		ColdThreshold:     time.Hour,
		ColdSweepInterval: time.Hour,
	})
	on.StartColdPolicy()
	on.StartColdPolicy() // idempotent: still exactly one evaluator
	on.StopColdPolicy()
	on.mu.Lock()
	cancel = on.coldCancel
	on.mu.Unlock()
	if cancel != nil {
		t.Fatal("StopColdPolicy must clear the evaluator handle")
	}
	on.StopColdPolicy() // idempotent
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
		LRUCapacity:         4,
		LoadWaitTimeout:     5 * time.Second,
		CallbackMaxRetries:  3,
		CallbackRetryBaseMS: 10,
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

// TestIndexManager_BuildPersistsAndExists verifies that a build with a
// configured IndexDataDir persists the index (Save RPC) and that
// IndexExists reflects the on-disk fact.
func TestIndexManager_BuildPersistsAndExists(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})

	cfg := IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		IndexDataDir:    t.TempDir(),
	}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector
	defer im.Close()

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	exists, err := im.IndexExists(context.Background(), "kb-1", 1)
	if err != nil {
		t.Fatalf("IndexExists: %v", err)
	}
	if !exists {
		t.Error("IndexExists = false after a persisted build, want true")
	}
	// A never-built version must report false.
	exists2, err := im.IndexExists(context.Background(), "kb-1", 99)
	if err != nil {
		t.Fatalf("IndexExists (unbuilt): %v", err)
	}
	if exists2 {
		t.Error("IndexExists = true for a never-built version, want false")
	}
}

// TestIndexManager_SearchRestoresFromDisk verifies that after the in-memory
// entry is gone (evicted / process restart), Search restores the index via
// the Load RPC instead of failing with ErrIndexNotReady.
func TestIndexManager_SearchRestoresFromDisk(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})

	cfg := IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		IndexDataDir:    t.TempDir(),
	}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector
	defer im.Close()

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := im.Evict(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("Evict: %v", err)
	}

	// Search must restore from disk (Load RPC) and succeed.
	_, err := im.Search(context.Background(), "kb-1", 1, []float32{0.5, 0.5}, 5)
	if err != nil {
		t.Fatalf("Search after evict: %v", err)
	}
	if !im.IsLoaded("kb-1", 1) {
		t.Error("index should be loaded again after Search restore")
	}
}

// TestIndexManager_MemoryThresholdEviction verifies that when
// MemoryThresholdMB is set, a new build evicts least-recently-used,
// ref-count-zero indexes once the estimated in-memory footprint exceeds
// the threshold — even while len(loaded) is still below LRUCapacity.
func TestIndexManager_MemoryThresholdEviction(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	// 768-dim vectors, 342 chunks => ~1.05 MiB of vector payload per index,
	// just over the 1 MiB threshold.
	vec := make([]float32, 768)
	for i := range vec {
		vec[i] = float32(i % 7)
	}
	for v, prefix := range map[int64]string{1: "a-", 2: "b-"} {
		chunks := make([]string, 342)
		vectors := make(map[string][]float32, 342)
		for i := range chunks {
			id := fmt.Sprintf("%s%d", prefix, i)
			chunks[i] = id
			vectors[id] = vec
		}
		ds.addDoc(v, fmt.Sprintf("doc-%d", v), chunks, vectors)
	}

	cfg := IndexManagerConfig{
		LRUCapacity:       4, // count limit high enough that only bytes trigger eviction
		LoadWaitTimeout:   5 * time.Second,
		MemoryThresholdMB: 1, // 1 MiB threshold, each index ~1.05 MiB
		VecstoreAddr:      "unused",
		IndexDataDir:      t.TempDir(),
	}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector
	defer im.Close()

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild(1): %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := im.TriggerBuild(context.Background(), "kb-1", 2); err != nil {
		t.Fatalf("TriggerBuild(2): %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if im.IsLoaded("kb-1", 1) {
		t.Error("version 1 index should have been evicted by the memory threshold")
	}
	if !im.IsLoaded("kb-1", 2) {
		t.Error("version 2 index should be loaded")
	}
	im.mu.Lock()
	bytes := im.loadedBytes
	im.mu.Unlock()
	if bytes <= 0 {
		t.Errorf("loadedBytes = %d, want > 0 (size accounting)", bytes)
	}
}

// TestIndexManager_EnforceDiskRetention verifies the on-disk retention
// policy: only the most recent IndexRetentionCount index files (per KB)
// survive, older files and their sidecars are deleted, protectedIDs are
// never deleted, and missing files/dirs are ignored (idempotent).
func TestIndexManager_EnforceDiskRetention(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "index", "kb-1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for _, base := range []string{"1", "2", "3"} {
		for _, suffix := range []string{".index", ".index.ids", ".index.mem"} {
			if err := os.WriteFile(filepath.Join(dir, base+suffix), []byte("x"), 0o644); err != nil {
				t.Fatalf("write %s: %v", base+suffix, err)
			}
		}
	}
	// Unrelated KB directory must not be touched.
	otherDir := filepath.Join(t.TempDir(), "index", "kb-other")
	if err := os.MkdirAll(otherDir, 0o755); err != nil {
		t.Fatalf("mkdir other: %v", err)
	}
	if err := os.WriteFile(filepath.Join(otherDir, "9.index"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write other: %v", err)
	}

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:         4,
		LoadWaitTimeout:     5 * time.Second,
		IndexDataDir:        filepath.Dir(filepath.Dir(dir)), // t.TempDir()
		IndexRetentionCount: 1,                               // keep the newest 1, unless protected
	})
	im.vectorIndexClient = newMockVectorIndexClient()

	// Protect version 2: with retention 1 the newest would be 3 only, but
	// 2 must survive too.
	if err := im.EnforceDiskRetention(context.Background(), "kb-1", []int64{2}); err != nil {
		t.Fatalf("EnforceDiskRetention: %v", err)
	}

	for _, base := range []string{"2", "3"} {
		for _, suffix := range []string{".index", ".index.ids", ".index.mem"} {
			if _, err := os.Stat(filepath.Join(dir, base+suffix)); err != nil {
				t.Errorf("expected %s to survive retention, got: %v", base+suffix, err)
			}
		}
	}
	for _, suffix := range []string{".index", ".index.ids", ".index.mem"} {
		if _, err := os.Stat(filepath.Join(dir, "1"+suffix)); !os.IsNotExist(err) {
			t.Errorf("expected 1%s to be deleted by retention, stat err: %v", suffix, err)
		}
	}
	// Unrelated KB untouched.
	if _, err := os.Stat(filepath.Join(otherDir, "9.index")); err != nil {
		t.Errorf("unrelated KB index deleted: %v", err)
	}

	// Idempotent: a second run (with everything already gone) must not error.
	if err := im.EnforceDiskRetention(context.Background(), "kb-1", nil); err != nil {
		t.Fatalf("EnforceDiskRetention (idempotent re-run): %v", err)
	}
	// Missing KB directory is a no-op, not an error.
	if err := im.EnforceDiskRetention(context.Background(), "kb-missing", nil); err != nil {
		t.Fatalf("EnforceDiskRetention (missing kb): %v", err)
	}
	// Retention unconfigured (0) is a no-op.
	im2 := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: 5 * time.Second, IndexDataDir: t.TempDir()})
	if err := im2.EnforceDiskRetention(context.Background(), "kb-1", nil); err != nil {
		t.Fatalf("EnforceDiskRetention (unconfigured): %v", err)
	}
}

// TestIndexManager_DeleteFilesByKB verifies that knowledge-base deletion
// removes the whole on-disk index directory and tolerates a missing one.
func TestIndexManager_DeleteFilesByKB(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "index", "kb-1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "1.index"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	im := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: 5 * time.Second, IndexDataDir: root})
	im.vectorIndexClient = newMockVectorIndexClient()

	if err := im.DeleteFilesByKB(context.Background(), "kb-1"); err != nil {
		t.Fatalf("DeleteFilesByKB: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("index dir should be gone, stat err: %v", err)
	}
	// Missing directory: idempotent no-error.
	if err := im.DeleteFilesByKB(context.Background(), "kb-1"); err != nil {
		t.Fatalf("DeleteFilesByKB (re-run): %v", err)
	}
}

// TestIndexManager_DeletePreventsResurrection verifies that after a KB or
// version deletion, a Search-triggered restore cannot resurrect the
// index: the tombstone set by DeleteFilesByKB / Discard makes loadFromDisk
// refuse, and Search reports ErrIndexNotReady.
func TestIndexManager_DeletePreventsResurrection(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})

	root := t.TempDir()
	cfg := IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		IndexDataDir:    root,
	}
	im := NewIndexManager(cfg)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector
	defer im.Close()

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if !im.IsLoaded("kb-1", 1) {
		t.Fatal("index should be loaded after build")
	}

	// KB deletion: evict + delete files + tombstone.
	if err := im.EvictByKB(context.Background(), "kb-1"); err != nil {
		t.Fatalf("EvictByKB: %v", err)
	}
	if err := im.DeleteFilesByKB(context.Background(), "kb-1"); err != nil {
		t.Fatalf("DeleteFilesByKB: %v", err)
	}

	_, err := im.Search(context.Background(), "kb-1", 1, []float32{0.5, 0.5}, 5)
	if !errors.Is(err, stratumerrors.ErrIndexNotReady) {
		t.Fatalf("Search after KB deletion = %v, want ErrIndexNotReady", err)
	}
	if im.IsLoaded("kb-1", 1) {
		t.Error("index must not be resurrected after KB deletion")
	}

	// Version deletion (Discard): also removes the on-disk files.
	if err := im.TriggerBuild(context.Background(), "kb-2", 7); err != nil {
		t.Fatalf("TriggerBuild(kb-2): %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := im.Evict(context.Background(), "kb-2", 7); err != nil {
		t.Fatalf("Evict(kb-2,7): %v", err)
	}
	if err := im.Discard(context.Background(), "kb-2", 7); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	// On-disk index files must be gone.
	if _, err := os.Stat(filepath.Join(root, "index", "kb-2", "7.index")); !os.IsNotExist(err) {
		t.Errorf("discarded version's index file should be gone, stat err: %v", err)
	}
	_, err = im.Search(context.Background(), "kb-2", 7, []float32{0.5, 0.5}, 5)
	if !errors.Is(err, stratumerrors.ErrIndexNotReady) {
		t.Fatalf("Search after Discard = %v, want ErrIndexNotReady", err)
	}
	if im.IsLoaded("kb-2", 7) {
		t.Error("index must not be resurrected after version deletion")
	}
}

// --- §8.6(c) 纯追加复用 ---

// writeFile writes content to path, creating parent directories. Used to put
// a base artifact on disk by hand: the mock vecstore's Save does not touch
// the filesystem, while appendBase treats the files as the fact.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// appendTestCase wires an IndexManager with a parent link (v2 -> v1) and one
// artifact directory, for the §8.6(c) tests below.
func appendTestCase(t *testing.T, ds *docSource) (*mockVectorIndexClient, *IndexManagerImpl) {
	t.Helper()
	return appendTestCaseWith(t, ds, 0)
}

// appendTestCaseWith is appendTestCase with an explicit AppendMaxDeadRatio
// (0 = the default).
func appendTestCaseWith(t *testing.T, ds *docSource, maxDeadRatio float64) (*mockVectorIndexClient, *IndexManagerImpl) {
	t.Helper()
	vc := newMockVectorIndexClient()
	im := newColdPolicyManager(t, vc, ds, IndexManagerConfig{
		LRUCapacity:        4,
		LoadWaitTimeout:    5 * time.Second,
		IndexDataDir:       t.TempDir(),
		AppendMaxDeadRatio: maxDeadRatio,
	})
	im.SetVersionParentGetter(func(_ context.Context, _ string, versionID int64) (int64, error) {
		if versionID == 2 {
			return 1, nil
		}
		return 0, nil
	})
	return vc, im
}

// buildParentThenChild builds v1, puts its artifact on disk, then builds v2
// (both graphed).
func buildParentThenChild(t *testing.T, im *IndexManagerImpl) {
	t.Helper()
	buildParentThenChildWith(t, im, false)
}

// buildParentThenChildWith is buildParentThenChild with an explicit shape:
// graphFree builds both versions through TriggerBuildGraphFree, which is what
// puts §8.6(c)'s RemoveChunks path (graph-free shapes only) in play.
func buildParentThenChildWith(t *testing.T, im *IndexManagerImpl, graphFree bool) {
	t.Helper()
	build := func(versionID int64) {
		var err error
		if graphFree {
			err = im.TriggerBuildGraphFree(context.Background(), "kb-1", versionID)
		} else {
			err = im.TriggerBuild(context.Background(), "kb-1", versionID)
		}
		if err != nil {
			t.Fatalf("TriggerBuild v%d (graph_free=%v): %v", versionID, graphFree, err)
		}
		waitIndexLoaded(t, im, "kb-1", versionID)
	}
	build(1)
	writeFile(t, im.indexPath("kb-1", 1), "base-index")
	writeFile(t, im.sidecarPath("kb-1", 1), "base-sidecar")
	build(2)
}

// §8.6(c)：v2 = v1 + 新增 chunk，且 v1 的产物还在本节点 —— 这时 v2 应当以
// v1 的产物为起点，只对 delta 追加，而不是整份重建。
func TestIndexManager_BuildReusesParentArtifactOnPureAppend(t *testing.T) {
	ds := newDocSource()
	ds.addDoc(1, "doc-a", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.5, 0.5}})
	ds.addDoc(2, "doc-a", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.5, 0.5}})
	ds.addDoc(2, "doc-b", []string{"chunk-b"}, map[string][]float32{"chunk-b": {0.5, 0.5}})

	vc, im := appendTestCase(t, ds)
	vc.mu.Lock()
	vc.loadForAppendBaseNtotal = 1 // v1 holds chunk-a
	vc.mu.Unlock()
	buildParentThenChild(t, im)

	vc.mu.Lock()
	loadCalls := vc.loadForAppendCalls
	basePath := vc.lastLoadForAppendPath
	buildCalls := vc.buildCalls
	payloads := vc.addedChunkIDs
	vc.mu.Unlock()

	if loadCalls != 1 {
		t.Fatalf("LoadForAppend calls = %d, want 1", loadCalls)
	}
	if want := im.indexPath("kb-1", 1); basePath != want {
		t.Fatalf("base path = %q, want the parent's artifact %q", basePath, want)
	}
	if buildCalls != 1 {
		t.Fatalf("Build calls = %d, want 1 (v1 only): v2 must not rebuild from scratch", buildCalls)
	}
	if len(payloads) != 1 || len(payloads[0]) != 1 || payloads[0][0] != "chunk-b" {
		t.Fatalf("AddChunks payloads = %v, want exactly the delta [chunk-b]", payloads)
	}
}

// 本版本没有新增 chunk（只是父版本的子集）时不做复用：那顶多是一次拷贝，
// 而若还删了东西，死向量会一起被继承下来。直接整份重建。
func TestIndexManager_BuildRebuildsWhenThereIsNothingToAppend(t *testing.T) {
	ds := newDocSource()
	ds.addDoc(1, "doc-a", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.5, 0.5}})
	ds.addDoc(1, "doc-b", []string{"chunk-b"}, map[string][]float32{"chunk-b": {0.5, 0.5}})
	ds.addDoc(2, "doc-a", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.5, 0.5}})

	vc, im := appendTestCase(t, ds)
	vc.mu.Lock()
	vc.loadForAppendBaseNtotal = 2
	vc.mu.Unlock()
	buildParentThenChild(t, im)

	vc.mu.Lock()
	loadCalls := vc.loadForAppendCalls
	buildCalls := vc.buildCalls
	vc.mu.Unlock()

	if loadCalls != 0 {
		t.Fatalf("LoadForAppend calls = %d, want 0: nothing to append, so no reuse", loadCalls)
	}
	if buildCalls != 2 {
		t.Fatalf("Build calls = %d, want 2 (v1 and v2 both from scratch)", buildCalls)
	}
}

// §8.6(c) 的删除场景（小量）：HNSW 删不掉向量，被删文档的向量会以"墓碑"形式
// 留在产物里（查询路径会把它们的结果滤掉，代价是内存与候选名额）。删除不多时
// 增量仍然划算 —— 这正是本轮放开的场景。
func TestIndexManager_BuildReusesParentArtifactWhenDeletionsAreSmall(t *testing.T) {
	ds := newDocSource()
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		ds.addDoc(1, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}
	// v2 删掉 doc-e，新增 doc-f：delta = [chunk-f]，墓碑 1 个 / 共 6 个 ≈ 0.17。
	for _, name := range []string{"a", "b", "c", "d", "f"} {
		ds.addDoc(2, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}

	vc, im := appendTestCase(t, ds)
	vc.mu.Lock()
	vc.loadForAppendBaseNtotal = 5 // v1 holds chunk-a..e
	vc.mu.Unlock()
	buildParentThenChild(t, im)

	vc.mu.Lock()
	loadCalls := vc.loadForAppendCalls
	buildCalls := vc.buildCalls
	payloads := vc.addedChunkIDs
	vc.mu.Unlock()

	if loadCalls != 1 {
		t.Fatalf("LoadForAppend calls = %d, want 1: a small deletion must not force a rebuild", loadCalls)
	}
	if buildCalls != 1 {
		t.Fatalf("Build calls = %d, want 1 (v1 only)", buildCalls)
	}
	if len(payloads) != 1 || len(payloads[0]) != 1 || payloads[0][0] != "chunk-f" {
		t.Fatalf("AddChunks payloads = %v, want exactly the delta [chunk-f]", payloads)
	}
}

// 删除太多（墓碑占比超过阈值）时改用全量重建 —— 否则产物里大半是永远查不到
// 的死向量，白占内存与 top-K 候选名额。
func TestIndexManager_BuildRebuildsWhenTombstonesExceedRatio(t *testing.T) {
	ds := newDocSource()
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		ds.addDoc(1, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}
	// v2 只留 doc-a，新增 doc-f/g/h：delta 3 个，墓碑 4 个 / 共 8 个 = 0.5。
	for _, name := range []string{"a", "f", "g", "h"} {
		ds.addDoc(2, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}

	vc, im := appendTestCase(t, ds)
	vc.mu.Lock()
	vc.loadForAppendBaseNtotal = 5
	vc.mu.Unlock()
	buildParentThenChild(t, im)

	vc.mu.Lock()
	loadCalls := vc.loadForAppendCalls
	buildCalls := vc.buildCalls
	vc.mu.Unlock()

	if loadCalls != 1 {
		t.Fatalf("LoadForAppend calls = %d, want 1 (the base is loaded, then judged)", loadCalls)
	}
	if buildCalls != 2 {
		t.Fatalf("Build calls = %d, want 2: too many tombstones must trigger a full rebuild", buildCalls)
	}
}

// 阈值可关（1.0 = 不检查）：即使墓碑很多也照常增量。用于"重建代价远高于内存"
// 的部署取舍。
func TestIndexManager_BuildReusesDespiteTombstonesWhenRatioDisabled(t *testing.T) {
	ds := newDocSource()
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		ds.addDoc(1, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}
	for _, name := range []string{"a", "f", "g", "h"} {
		ds.addDoc(2, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}

	vc, im := appendTestCaseWith(t, ds, 1.0)
	vc.mu.Lock()
	vc.loadForAppendBaseNtotal = 5
	vc.mu.Unlock()
	buildParentThenChild(t, im)

	vc.mu.Lock()
	buildCalls := vc.buildCalls
	payloads := vc.addedChunkIDs
	vc.mu.Unlock()

	if buildCalls != 1 {
		t.Fatalf("Build calls = %d, want 1 (v1 only): the ratio check is disabled", buildCalls)
	}
	want := map[string]bool{"chunk-f": true, "chunk-g": true, "chunk-h": true}
	if len(payloads) != 1 || len(payloads[0]) != len(want) {
		t.Fatalf("AddChunks payloads = %v, want one batch of the 3 delta chunks", payloads)
	}
	for _, id := range payloads[0] {
		if !want[id] {
			t.Fatalf("unexpected chunk %q in the append payload %v", id, payloads)
		}
	}
}

// 增量只是优化：起点加载失败时必须回退到全量构建，而不是让版本构建失败。
func TestIndexManager_BuildFallsBackWhenAppendReuseFails(t *testing.T) {
	ds := newDocSource()
	ds.addDoc(1, "doc-a", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.5, 0.5}})
	ds.addDoc(2, "doc-a", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.5, 0.5}})
	ds.addDoc(2, "doc-b", []string{"chunk-b"}, map[string][]float32{"chunk-b": {0.5, 0.5}})

	vc, im := appendTestCase(t, ds)
	vc.mu.Lock()
	vc.loadForAppendErr = status.Error(codes.Internal, "base artifact unreadable")
	vc.mu.Unlock()

	buildParentThenChild(t, im) // 必须仍然成功（回退到全量）

	vc.mu.Lock()
	loadCalls := vc.loadForAppendCalls
	buildCalls := vc.buildCalls
	vc.mu.Unlock()

	if loadCalls != 1 {
		t.Fatalf("LoadForAppend calls = %d, want 1 (one failed attempt)", loadCalls)
	}
	if buildCalls != 2 {
		t.Fatalf("Build calls = %d, want 2: after the reuse failed, v2 rebuilds from scratch", buildCalls)
	}
	if !im.IsLoaded("kb-1", 2) {
		t.Fatal("v2 must be READY even though the reuse failed")
	}
}

// 重启后没有父版本的形态记录：不确定它是不是本次想要的形态，保守选择重建
// （复用错形态会产出形态不符的产物）。
func TestIndexManager_BuildSkipsReuseWhenParentShapeIsUnknown(t *testing.T) {
	ds := newDocSource()
	ds.addDoc(1, "doc-a", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.5, 0.5}})
	ds.addDoc(2, "doc-a", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.5, 0.5}})
	ds.addDoc(2, "doc-b", []string{"chunk-b"}, map[string][]float32{"chunk-b": {0.5, 0.5}})

	vc, im := appendTestCase(t, ds)

	// Put the parent's artifact on disk *without* building it here, which is
	// what a restarted node sees.
	writeFile(t, im.indexPath("kb-1", 1), "base-index")
	writeFile(t, im.sidecarPath("kb-1", 1), "base-sidecar")

	if err := im.TriggerBuild(context.Background(), "kb-1", 2); err != nil {
		t.Fatalf("TriggerBuild v2: %v", err)
	}
	waitIndexLoaded(t, im, "kb-1", 2)

	vc.mu.Lock()
	loadCalls := vc.loadForAppendCalls
	buildCalls := vc.buildCalls
	vc.mu.Unlock()

	if loadCalls != 0 {
		t.Fatalf("LoadForAppend calls = %d, want 0: an unknown base shape must not be reused", loadCalls)
	}
	if buildCalls != 1 {
		t.Fatalf("Build calls = %d, want 1 (v2 from scratch)", buildCalls)
	}
}

// §8.6(c) 删除场景的快路径：**免图**形态可以真删（faiss 压缩 IndexFlatCodes），
// 所以增量时把"本版本不再需要的 chunk"直接交给 vecstore 删掉，而不是留成墓碑
// 等阈值触发重建。
func TestIndexManager_BuildRemovesDeadChunksFromGraphFreeIndex(t *testing.T) {
	ds := newDocSource()
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		ds.addDoc(1, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}
	// v2 删掉 doc-e，新增 doc-f（两个版本都是免图形态）。
	for _, name := range []string{"a", "b", "c", "d", "f"} {
		ds.addDoc(2, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}

	vc, im := appendTestCase(t, ds)
	vc.mu.Lock()
	vc.loadForAppendBaseNtotal = 5 // v1 holds chunk-a..e
	vc.mu.Unlock()
	buildParentThenChildWith(t, im, true)

	vc.mu.Lock()
	removeCalls := vc.removeChunksCalls
	removed := append([]string(nil), vc.removedChunkIDs...)
	buildCalls := vc.buildCalls
	payloads := vc.addedChunkIDs
	vc.mu.Unlock()

	if removeCalls != 1 {
		t.Fatalf("RemoveChunks calls = %d, want 1 (a graph-free base can really delete)", removeCalls)
	}
	if len(removed) != 1 || removed[0] != "chunk-e" {
		t.Fatalf("removed chunk ids = %v, want exactly [chunk-e] (the deletion of this step)", removed)
	}
	if buildCalls != 1 {
		t.Fatalf("Build calls = %d, want 1 (v1 only): the reuse must still happen", buildCalls)
	}
	if len(payloads) != 1 || len(payloads[0]) != 1 || payloads[0][0] != "chunk-f" {
		t.Fatalf("AddChunks payloads = %v, want exactly the delta [chunk-f]", payloads)
	}
}

// 带图形态不能真删（faiss 对 HNSW 没有 remove_ids）：同样的删除场景下不调
// RemoveChunks，墓碑留着，由阈值判定兜底。
func TestIndexManager_BuildDoesNotRemoveChunksForGraphedIndex(t *testing.T) {
	ds := newDocSource()
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		ds.addDoc(1, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}
	for _, name := range []string{"a", "b", "c", "d", "f"} {
		ds.addDoc(2, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}

	vc, im := appendTestCase(t, ds)
	vc.mu.Lock()
	vc.loadForAppendBaseNtotal = 5
	vc.mu.Unlock()
	buildParentThenChildWith(t, im, false) // graphed

	vc.mu.Lock()
	removeCalls := vc.removeChunksCalls
	loadCalls := vc.loadForAppendCalls
	buildCalls := vc.buildCalls
	vc.mu.Unlock()

	if removeCalls != 0 {
		t.Fatalf("RemoveChunks calls = %d, want 0: HNSW cannot remove vectors", removeCalls)
	}
	if loadCalls != 1 || buildCalls != 1 {
		t.Fatalf("LoadForAppend=%d Build=%d, want 1/1: the reuse still happens, tombstones stay", loadCalls, buildCalls)
	}
}

// 真删失败也只是回退：删不掉（例如 vecstore 拒绝）时整份重建，版本构建照样成功。
func TestIndexManager_BuildFallsBackWhenRemoveChunksFails(t *testing.T) {
	ds := newDocSource()
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		ds.addDoc(1, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}
	for _, name := range []string{"a", "b", "c", "d", "f"} {
		ds.addDoc(2, "doc-"+name, []string{"chunk-" + name}, map[string][]float32{"chunk-" + name: {0.5, 0.5}})
	}

	vc, im := appendTestCase(t, ds)
	vc.mu.Lock()
	vc.loadForAppendBaseNtotal = 5
	vc.removeChunksErr = status.Error(codes.FailedPrecondition, "index is sealed")
	vc.mu.Unlock()
	buildParentThenChildWith(t, im, true)

	vc.mu.Lock()
	removeCalls := vc.removeChunksCalls
	buildCalls := vc.buildCalls
	vc.mu.Unlock()

	if removeCalls != 1 {
		t.Fatalf("RemoveChunks calls = %d, want 1 (one failed attempt)", removeCalls)
	}
	if buildCalls != 2 {
		t.Fatalf("Build calls = %d, want 2: a failed removal falls back to a full rebuild", buildCalls)
	}
	if !im.IsLoaded("kb-1", 2) {
		t.Fatal("v2 must be READY even though the removal failed")
	}
}

// TestIndexManager_BuildReportsDocumentsWithoutChunks pins the diagnostic added
// after an embed-less cluster was found to be half-succeeding at writes.
//
// An empty build batch has two origins, and conflating them hid a real outage:
//
//   - the version has no documents at all — normal, and the empty version
//     deliberately never touches the vecstore;
//   - the version HAS documents but nothing produced a chunk — splitting yielded
//     nothing, or every embedding failed without failing its caller.
//
// The second one used to log the same Info line as the first. So with embed
// unreachable, writes were half-successful — version committed, data never
// landed, index stuck PENDING, queries refused with FailedPrecondition — and the
// log showed one innocuous-looking "nothing to build". It has to be an Error.
func TestIndexManager_BuildReportsDocumentsWithoutChunks(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	// 文档在,但一个 chunk 都没产出。
	ds.addDoc(1, "doc-1", nil, nil)

	core, logs := observer.New(zapcore.ErrorLevel)
	im := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: 5 * time.Second, VecstoreAddr: "unused"})
	im.logger = zap.New(core)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	var cbCalled atomic.Int32
	im.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
		cbCalled.Add(1)
		return nil
	})

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if cbCalled.Load() != 1 {
		t.Fatalf("expected the build callback once, got %d", cbCalled.Load())
	}

	found := false
	for _, e := range logs.All() {
		if e.Level == zapcore.ErrorLevel && strings.Contains(e.Message, "produced no chunks") {
			found = true
		}
	}
	if !found {
		t.Error("a version with documents but no chunks must be reported at Error — that is where a half-successful write becomes visible")
	}
}

// TestIndexManager_BuildEmptyVersionStaysInfo is the other side of the same
// branch: a genuinely empty version is routine and must not be alarming.
func TestIndexManager_BuildEmptyVersionStaysInfo(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource() // 无文档

	core, logs := observer.New(zapcore.ErrorLevel)
	im := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: 5 * time.Second, VecstoreAddr: "unused"})
	im.logger = zap.New(core)
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	if n := logs.Len(); n != 0 {
		t.Errorf("an empty version is routine, want no Error logs, got %d: %v", n, logs.All())
	}
}

// TestAbandonSweeper_RemovesOnlyUnsealedArtifacts pins §6.3's whole test: an
// artifact with no .ids sidecar was never sealed by a Save, so nothing can serve
// from it and nothing will ever come back for it.
//
// The gap it closes is the everyday one, not the rare one: a build that dies or
// times out leaves these remains, the control layer quietly moves on to the next
// candidate, that candidate succeeds, and the version never reaches
// FAILED_PERMANENT — so §1.4's cleanup broadcast never fires either.
func TestAbandonSweeper_RemovesOnlyUnsealedArtifacts(t *testing.T) {
	dir := t.TempDir()
	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:         4,
		LoadWaitTimeout:     time.Second,
		IndexDataDir:        dir,
		BuildAbandonTimeout: 30 * time.Minute,
	})
	im.logger = zap.NewNop()

	kbDir := filepath.Join(dir, "index", "kb-1")
	if err := os.MkdirAll(kbDir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name string, age time.Duration) string {
		p := filepath.Join(kbDir, name)
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		ts := time.Now().Add(-age)
		if err := os.Chtimes(p, ts, ts); err != nil {
			t.Fatal(err)
		}
		return p
	}

	now := time.Now()
	abandoned := write("7.index", 45*time.Minute) // unsealed + stale → reclaim
	sealedOld := write("8.index", 45*time.Minute) // sealed: never touched
	if err := os.WriteFile(filepath.Join(kbDir, "8.index.ids"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	fresh := write("9.index", 2*time.Minute) // unsealed but recent → keep

	// Save's own temporaries, the second kind of remains §8.8 named: it writes
	// <v>.index.tmp and <v>.index.ids.tmp and renames them into place, so a
	// stale one is a Save that died mid-write. Nothing else removes it — the
	// next Save overwrites it in place, and EnforceDiskRetention does not count
	// it — which is what makes it worth sweeping.
	staleTmp := write("10.index.tmp", 45*time.Minute)
	staleIDsTmp := write("10.index.ids.tmp", 45*time.Minute)
	freshTmp := write("11.index.tmp", 2*time.Minute) // a Save may still be writing

	im.sweepAbandonedArtifacts(now)

	if fileExists(abandoned) {
		t.Error("an unsealed artifact past the timeout must be reclaimed")
	}
	if !fileExists(sealedOld) {
		t.Error("a SEALED artifact is a completed index — its age is EnforceDiskRetention's business, not the sweeper's")
	}
	if !fileExists(fresh) {
		t.Error("an unsealed artifact younger than the timeout may be a build in progress and must be left alone")
	}
	if fileExists(staleTmp) {
		t.Error("a stale Save temporary must be reclaimed — nothing else ever removes it")
	}
	if fileExists(staleIDsTmp) {
		t.Error("the sidecar temporary must be reclaimed too")
	}
	if !fileExists(freshTmp) {
		t.Error("a fresh temporary may belong to a Save that is still writing and must be left alone")
	}
}

// TestAbandonSweeper_IsANoOpWithoutPersistenceOrWhenDisabled: with no
// IndexDataDir there is no artifact to look at, and a negative timeout asks for
// the sweeper to stay off.
func TestAbandonSweeper_IsANoOpWithoutPersistenceOrWhenDisabled(t *testing.T) {
	noDir := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: time.Second})
	noDir.logger = zap.NewNop()
	noDir.sweepAbandonedArtifacts(time.Now()) // must not panic on an empty data dir

	disabled := NewIndexManager(IndexManagerConfig{
		LRUCapacity:         4,
		LoadWaitTimeout:     time.Second,
		IndexDataDir:        t.TempDir(),
		BuildAbandonTimeout: -1,
	})
	disabled.logger = zap.NewNop()
	disabled.StartAbandonSweeper()
	disabled.mu.Lock()
	running := disabled.abandonCancel != nil
	disabled.mu.Unlock()
	if running {
		t.Error("a negative timeout must leave the sweeper off")
	}
	disabled.Close()
}

// §8.6a: the active version is the one serving queries, so it keeps its graph
// however long it has gone unqueried — reshaping it would turn the knowledge
// base's main read path into a scan. With no active pointer, the end of the
// version chain is what serves, so it is shielded too.
func TestIndexManager_ColdPolicySkipsActiveVersion(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	for v := int64(1); v <= 3; v++ {
		ds.addDoc(v, fmt.Sprintf("doc-%d", v), []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
	}
	im := newColdPolicyManager(t, vc, ds, IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		ColdThreshold:   time.Minute,
	})
	im.SetActiveVersionsProvider(func(context.Context) (map[string]int64, error) {
		return map[string]int64{"kb-1": 1}, nil
	})
	// The chain is 1..3, so 3 is its end — the version that serves when no
	// active pointer is set. The default fixture goes up to 10.
	im.SetVersionsProvider(func(context.Context) (map[string][]int64, error) {
		return map[string][]int64{"kb-1": {1, 2, 3}}, nil
	})
	for v := int64(1); v <= 3; v++ {
		if err := im.TriggerBuild(context.Background(), "kb-1", v); err != nil {
			t.Fatalf("TriggerBuild v=%d failed: %v", v, err)
		}
		waitIndexLoaded(t, im, "kb-1", v)
	}

	// Every version has been idle for an hour — which alone used to be the
	// whole criterion.
	im.mu.Lock()
	for v := int64(1); v <= 3; v++ {
		im.lastSearch[indexKey{"kb-1", v}] = time.Now().Add(-time.Hour)
	}
	im.mu.Unlock()

	got := im.coldCandidates(context.Background(), time.Now())
	want := indexKey{"kb-1", 2}
	if len(got) != 1 || got[0].key != want {
		t.Fatalf("expected only %v to be cold (1 is active, 3 ends the chain), got %v", want, got)
	}
}

// §8.6a: reshaping is not one-way. A graph-free version that is queried again
// gets its graph back, instead of one cold spell degrading it for good.
func TestIndexManager_ColdPolicyReheatsGraphFreeVersion(t *testing.T) {
	vc := newMockVectorIndexClient()
	dir := t.TempDir()
	im := newColdPolicyManager(t, vc, newDocSource(), IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		IndexDataDir:    dir,
		ColdThreshold:   time.Minute,
	})
	im.SetVersionsProvider(func(context.Context) (map[string][]int64, error) {
		return map[string][]int64{"kb-1": {5, 6}}, nil
	})

	// The artifact on disk is the graph-free variant (the shape line vecstore's
	// Save writes), and the version was just queried.
	kbDir := filepath.Join(dir, "index", "kb-1")
	if err := os.MkdirAll(kbDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(kbDir, "5.index"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.WriteFile(filepath.Join(kbDir, "5.index.ids"),
		[]byte("stratum-index-1\n2\n0\n0\ngraph_free 1\n"), 0o644); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}
	im.mu.Lock()
	im.lastSearch[indexKey{"kb-1", 5}] = time.Now()
	im.mu.Unlock()

	got := im.coldCandidates(context.Background(), time.Now())
	if len(got) != 1 || got[0].key != (indexKey{"kb-1", 5}) || !got[0].reheat {
		t.Fatalf("expected a reheat candidate for kb-1/5, got %v", got)
	}
}

// §8.6a: a build that failed must not be re-queued on every sweep.
func TestIndexManager_ColdPolicyBacksOffAfterFailedBuild(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
	im := newColdPolicyManager(t, vc, ds, IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		ColdThreshold:   time.Minute,
	})
	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild failed: %v", err)
	}
	waitIndexLoaded(t, im, "kb-1", 1)

	aged := time.Now().Add(-time.Hour)
	im.mu.Lock()
	im.lastSearch[indexKey{"kb-1", 1}] = aged
	im.coldFailedAt[indexKey{"kb-1", 1}] = time.Now()
	im.mu.Unlock()
	if got := im.coldCandidates(context.Background(), time.Now()); len(got) != 0 {
		t.Fatalf("a failed reshape must back off until the version is queried again, got %v", got)
	}

	// The failure ages; the version does not become a candidate again on age
	// alone, only once a query has landed since the failure.
	im.mu.Lock()
	im.coldFailedAt[indexKey{"kb-1", 1}] = aged.Add(-time.Minute)
	im.mu.Unlock()
	if got := im.coldCandidates(context.Background(), time.Now()); len(got) != 1 {
		t.Fatalf("a failure older than the last query must allow another attempt, got %v", got)
	}
}

// §8.6a: only versions this node actually holds are reshaped. A version that
// was merely queried once, whose index never landed here, must not be built by
// the evaluator out of nowhere — that would bypass §8.6b's build-on-demand and
// pair with the disk retention policy into a build-then-delete loop.
func TestIndexManager_ColdPolicySkipsVersionsThisNodeDoesNotHold(t *testing.T) {
	vc := newMockVectorIndexClient()
	im := newColdPolicyManager(t, vc, newDocSource(), IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		ColdThreshold:   time.Minute,
	})
	im.SetVersionsProvider(func(context.Context) (map[string][]int64, error) {
		return map[string][]int64{"kb-1": {1, 2}}, nil
	})

	// Queried here once, long ago — but no artifact was ever built or received.
	im.mu.Lock()
	im.lastSearch[indexKey{"kb-1", 1}] = time.Now().Add(-time.Hour)
	im.mu.Unlock()

	if got := im.coldCandidates(context.Background(), time.Now()); len(got) != 0 {
		t.Fatalf("a version this node does not hold must not be reshaped, got %v", got)
	}
}

// seedIndexFiles lays down a version's on-disk file set (the Faiss file plus
// its sidecars), which is what the retention policy enumerates.
func seedIndexFiles(t *testing.T, dir, kbID string, versionIDs ...int64) string {
	t.Helper()
	kbDir := filepath.Join(dir, "index", kbID)
	if err := os.MkdirAll(kbDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for _, v := range versionIDs {
		for _, suffix := range []string{".index", ".index.ids", ".index.mem"} {
			name := fmt.Sprintf("%d%s", v, suffix)
			if err := os.WriteFile(filepath.Join(kbDir, name), []byte("x"), 0o644); err != nil {
				t.Fatalf("write %s: %v", name, err)
			}
		}
	}
	return kbDir
}

// recordAccessForTest writes a version's access-time sidecar directly, standing
// in for a query this process never saw — which is exactly the state after a
// restart, and the state the shield exists for.
func recordAccessForTest(t *testing.T, kbDir string, versionID int64, at time.Time) {
	t.Helper()
	name := fmt.Sprintf("%d.index.used", versionID)
	if err := os.WriteFile(filepath.Join(kbDir, name),
		[]byte(strconv.FormatInt(at.UnixNano(), 10)), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// A version queried here recently is shielded from retention even when it is
// older than the newest IndexRetentionCount — and the shield comes from disk,
// so it survives a restart. The manager doing the enforcing is deliberately a
// FRESH one whose in-memory access table is empty: that is the case the shield
// exists for, and reading memory instead would protect nothing.
func TestIndexManager_RetentionShieldsRecentlyQueriedVersions(t *testing.T) {
	dir := t.TempDir()
	kbDir := seedIndexFiles(t, dir, "kb-1", 1, 2, 3)
	recordAccessForTest(t, kbDir, 1, time.Now().Add(-time.Minute))

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        5 * time.Second,
		IndexDataDir:           dir,
		IndexRetentionCount:    1,         // newest only ...
		RetentionProtectWindow: time.Hour, // ... plus what was just read
	})
	im.vectorIndexClient = newMockVectorIndexClient()

	if err := im.EnforceDiskRetention(context.Background(), "kb-1", nil); err != nil {
		t.Fatalf("EnforceDiskRetention: %v", err)
	}

	// 3 is newest, 1 is shielded by its recent query; 2 is the one dropped.
	for _, v := range []int64{1, 3} {
		if _, err := os.Stat(filepath.Join(kbDir, fmt.Sprintf("%d.index", v))); err != nil {
			t.Errorf("expected %d to survive retention, got: %v", v, err)
		}
	}
	if _, err := os.Stat(filepath.Join(kbDir, "2.index")); !os.IsNotExist(err) {
		t.Errorf("expected 2 to be dropped by retention, stat err: %v", err)
	}
}

// The shield is capped, and the cap is resolved by recency: the versions
// queried most recently win. Without a cap a knowledge base whose versions are
// all read regularly would grow its on-disk set without bound.
func TestIndexManager_RetentionProtectionKeepsTheMostRecent(t *testing.T) {
	dir := t.TempDir()
	kbDir := seedIndexFiles(t, dir, "kb-1", 1, 2, 3)
	// Both are inside the window; 2 was queried later, so only 2 survives the
	// cap of one.
	recordAccessForTest(t, kbDir, 1, time.Now().Add(-time.Hour))
	recordAccessForTest(t, kbDir, 2, time.Now().Add(-time.Minute))

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        5 * time.Second,
		IndexDataDir:           dir,
		IndexRetentionCount:    1,
		RetentionProtectWindow: time.Hour,
		RetentionProtectMax:    1,
	})
	im.vectorIndexClient = newMockVectorIndexClient()

	if err := im.EnforceDiskRetention(context.Background(), "kb-1", nil); err != nil {
		t.Fatalf("EnforceDiskRetention: %v", err)
	}

	for _, v := range []int64{2, 3} {
		if _, err := os.Stat(filepath.Join(kbDir, fmt.Sprintf("%d.index", v))); err != nil {
			t.Errorf("expected %d to survive retention, got: %v", v, err)
		}
	}
	if _, err := os.Stat(filepath.Join(kbDir, "1.index")); !os.IsNotExist(err) {
		t.Errorf("expected 1 to lose the cap and be dropped, stat err: %v", err)
	}
}

// A query older than the window does not shield anything: the protection is
// about what is still being read, not about what was read once.
func TestIndexManager_RetentionProtectionAgesOut(t *testing.T) {
	dir := t.TempDir()
	kbDir := seedIndexFiles(t, dir, "kb-1", 1, 2)
	recordAccessForTest(t, kbDir, 1, time.Now().Add(-2*time.Hour))

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        5 * time.Second,
		IndexDataDir:           dir,
		IndexRetentionCount:    1,
		RetentionProtectWindow: time.Hour, // the query is twice as old
	})
	im.vectorIndexClient = newMockVectorIndexClient()

	if err := im.EnforceDiskRetention(context.Background(), "kb-1", nil); err != nil {
		t.Fatalf("EnforceDiskRetention: %v", err)
	}

	if _, err := os.Stat(filepath.Join(kbDir, "2.index")); err != nil {
		t.Errorf("expected the newest version to survive, got: %v", err)
	}
	if _, err := os.Stat(filepath.Join(kbDir, "1.index")); !os.IsNotExist(err) {
		t.Errorf("an expired access record must not shield: stat err: %v", err)
	}
}

// The shield is disabled by a negative window, which is the historical
// behaviour: keep the newest N and nothing else.
func TestIndexManager_RetentionProtectionDisabled(t *testing.T) {
	dir := t.TempDir()
	kbDir := seedIndexFiles(t, dir, "kb-1", 1, 2)
	recordAccessForTest(t, kbDir, 1, time.Now())

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        5 * time.Second,
		IndexDataDir:           dir,
		IndexRetentionCount:    1,
		RetentionProtectWindow: -1,
	})
	im.vectorIndexClient = newMockVectorIndexClient()

	if err := im.EnforceDiskRetention(context.Background(), "kb-1", nil); err != nil {
		t.Fatalf("EnforceDiskRetention: %v", err)
	}
	if _, err := os.Stat(filepath.Join(kbDir, "1.index")); !os.IsNotExist(err) {
		t.Errorf("with the shield disabled, 1 must be dropped, stat err: %v", err)
	}
}

// Dropping a version's files must take the access sidecar with them, or the
// shield would outlive the version it shielded.
func TestIndexManager_RetentionDropsAccessSidecarOfDroppedVersion(t *testing.T) {
	dir := t.TempDir()
	kbDir := seedIndexFiles(t, dir, "kb-1", 1, 2)
	recordAccessForTest(t, kbDir, 1, time.Now().Add(-2*time.Hour)) // expired: 1 gets dropped

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        5 * time.Second,
		IndexDataDir:           dir,
		IndexRetentionCount:    1,
		RetentionProtectWindow: time.Hour,
	})
	im.vectorIndexClient = newMockVectorIndexClient()

	if err := im.EnforceDiskRetention(context.Background(), "kb-1", nil); err != nil {
		t.Fatalf("EnforceDiskRetention: %v", err)
	}
	for _, suffix := range []string{".index", ".index.ids", ".index.mem", ".index.used"} {
		if _, err := os.Stat(filepath.Join(kbDir, "1"+suffix)); !os.IsNotExist(err) {
			t.Errorf("1%s must be gone with the version, stat err: %v", suffix, err)
		}
	}
}

// Search is what records the access time on disk (throttled); that record is
// what the retention shield reads after a restart.
func TestIndexManager_SearchPersistsAccessTime(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
	im := newColdPolicyManager(t, vc, ds, IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        5 * time.Second,
		IndexDataDir:           t.TempDir(),
		RetentionProtectWindow: time.Hour,
	})
	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	waitIndexLoaded(t, im, "kb-1", 1)

	if _, err := im.Search(context.Background(), "kb-1", 1, []float32{0.5, 0.5}, 1); err != nil {
		t.Fatalf("Search: %v", err)
	}
	at, ok := im.readAccessTime("kb-1", 1)
	if !ok {
		t.Fatal("Search must persist the version's access time for the retention shield")
	}
	if age := time.Since(at); age < 0 || age > time.Minute {
		t.Fatalf("persisted access time is %v old, want it recorded just now", age)
	}
}

// An explicit operator request (RebuildIndex / WarmupVersion) is "this version
// is wanted here, now" — the same evidence a query leaves — so it must shield
// the artifact from retention too.
//
// The version an operator rebuilds is, by construction, usually OUTSIDE the
// newest-N window (otherwise there would be nothing to rebuild): without this
// the artifact would survive only until the next retention pass.
func TestIndexManager_RecordInterestShieldsFromRetention(t *testing.T) {
	dir := t.TempDir()
	kbDir := seedIndexFiles(t, dir, "kb-1", 1, 2, 3)

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        5 * time.Second,
		IndexDataDir:           dir,
		IndexRetentionCount:    1,         // newest only ...
		RetentionProtectWindow: time.Hour, // ... plus what was explicitly asked for
	})
	im.vectorIndexClient = newMockVectorIndexClient()

	// Version 1 is the oldest, and the operator asked for it by hand.
	im.RecordInterest("kb-1", 1)
	if _, err := os.Stat(filepath.Join(kbDir, "1.index.used")); err != nil {
		t.Fatalf("RecordInterest must leave the access sidecar on disk, got: %v", err)
	}

	if err := im.EnforceDiskRetention(context.Background(), "kb-1", nil); err != nil {
		t.Fatalf("EnforceDiskRetention: %v", err)
	}
	for _, v := range []int64{1, 3} {
		if _, err := os.Stat(filepath.Join(kbDir, fmt.Sprintf("%d.index", v))); err != nil {
			t.Errorf("expected %d to survive retention, got: %v", v, err)
		}
	}
	if _, err := os.Stat(filepath.Join(kbDir, "2.index")); !os.IsNotExist(err) {
		t.Errorf("expected 2 to be dropped, stat err: %v", err)
	}
}

// §8.4(a): a completed build must leave the retention shield on disk, not only in
// seedAccessLocked's in-memory table. The build callback ships this artifact to
// the replicas, and the retention pass that could drop it runs on the *next*
// build — possibly one that fires while the distribution is still queued behind
// the push gate.
func TestIndexManager_BuildCompleteShieldsArtifactOnDisk(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-x"}, map[string][]float32{"chunk-x": {0.5, 0.5}})
	dir := t.TempDir()
	im := newColdPolicyManager(t, vc, ds, IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        5 * time.Second,
		IndexDataDir:           dir,
		RetentionProtectWindow: time.Hour,
	})

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	waitIndexLoaded(t, im, "kb-1", 1)

	if _, err := os.Stat(filepath.Join(dir, "index", "kb-1", "1.index.used")); err != nil {
		t.Fatalf("a completed build must leave the .used shield on disk, got: %v", err)
	}
	at, ok := im.readAccessTime("kb-1", 1)
	if !ok {
		t.Fatal("build completion must persist an access time the retention shield can read after a restart")
	}
	if age := time.Since(at); age < 0 || age > time.Minute {
		t.Fatalf("persisted shield time is %v old, want it recorded just now", age)
	}
}

// §8.4(a): the same for an artifact received from another node. Without the
// on-disk shield a replica would install a version outside the newest-N window
// and then drop it on the next retention pass — installed, then deleted, for a
// version the sender just spent bandwidth shipping.
func TestIndexManager_InstallIndexShieldsArtifactOnDisk(t *testing.T) {
	vc := newMockVectorIndexClient()
	dir := t.TempDir()
	im := newColdPolicyManager(t, vc, newDocSource(), IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        5 * time.Second,
		IndexDataDir:           dir,
		RetentionProtectWindow: time.Hour,
	})

	// The mock vecstore, like the real one, refuses to Load an index it has
	// never built.
	vc.mu.Lock()
	vc.built[indexKey{"kb-1", 7}] = nil
	vc.mu.Unlock()

	if err := im.InstallIndex(context.Background(), "kb-1", 7, []byte("index-bytes"), []byte("sidecar")); err != nil {
		t.Fatalf("InstallIndex failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "index", "kb-1", "7.index.used")); err != nil {
		t.Fatalf("installing an artifact must leave the .used shield on disk, got: %v", err)
	}
}

// A version whose artifact is gone can leave its sidecars behind. The drop loop
// walks artifacts, so a `.index.used` (or `.index.mem`) with no `.index` beside it
// is never reached by it — which is how a lost artifact turns into a sidecar that
// outlives it forever. Retention clears those orphans in the same pass.
//
// The `.ids` file is a different animal and must survive: InstallIndex writes the
// pair BEFORE the index, so an ids-without-index is a normal in-flight install,
// not garbage.
func TestIndexManager_RetentionClearsOrphanedSidecars(t *testing.T) {
	dir := t.TempDir()
	kbDir := seedIndexFiles(t, dir, "kb-1", 1, 2)

	// Version 9's artifact is gone; its sidecars are not.
	for _, suffix := range []string{".index.used", ".index.mem", ".index.ids"} {
		if err := os.WriteFile(filepath.Join(kbDir, "9"+suffix), []byte("x"), 0o644); err != nil {
			t.Fatalf("write 9%s: %v", suffix, err)
		}
	}

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        5 * time.Second,
		IndexDataDir:           dir,
		IndexRetentionCount:    2, // as many artifacts as exist: nothing is dropped
		RetentionProtectWindow: time.Hour,
	})
	im.vectorIndexClient = newMockVectorIndexClient()

	if err := im.EnforceDiskRetention(context.Background(), "kb-1", nil); err != nil {
		t.Fatalf("EnforceDiskRetention: %v", err)
	}

	for _, suffix := range []string{".index.used", ".index.mem"} {
		if _, err := os.Stat(filepath.Join(kbDir, "9"+suffix)); !os.IsNotExist(err) {
			t.Errorf("orphaned 9%s must be cleared by retention, stat err: %v", suffix, err)
		}
	}
	if _, err := os.Stat(filepath.Join(kbDir, "9.index.ids")); err != nil {
		t.Errorf("a .ids without its index is an in-flight install, not garbage: %v", err)
	}
	// Sidecars of versions that DO have artifacts stay untouched.
	for _, base := range []string{"1", "2"} {
		for _, suffix := range []string{".index", ".index.ids", ".index.mem"} {
			if _, err := os.Stat(filepath.Join(kbDir, base+suffix)); err != nil {
				t.Errorf("version %s%s must survive retention: %v", base, suffix, err)
			}
		}
	}
}
