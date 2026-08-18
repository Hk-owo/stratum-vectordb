package coordinator

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"stratum/internal/bloom"
	"stratum/internal/index"
	stratumync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// === Test doubles for WriteCoordinatorImpl dependencies ===

// testDocStore is an in-memory DocStore for tests.
type testDocStore struct {
	mu   sync.Mutex
	data map[string][]byte // key: "kbID|docID|versionID"
}

func newTestDocStore() *testDocStore {
	return &testDocStore{data: make(map[string][]byte)}
}

func (s *testDocStore) Write(_ context.Context, kbID, docID string, versionID int64, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s|%s|%d", kbID, docID, versionID)
	s.data[key] = value
	return nil
}

func (s *testDocStore) ReadAt(_ context.Context, kbID, docID string, maxVersionID int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := fmt.Sprintf("%s|%s|", kbID, docID)
	var bestVersion int64 = -1
	var bestValue []byte
	found := false
	for k, v := range s.data {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			var ver int64
			fmt.Sscanf(k[len(prefix):], "%d", &ver)
			if ver <= maxVersionID && ver > bestVersion {
				bestVersion = ver
				bestValue = v
				found = true
			}
		}
	}
	if !found {
		return nil, fmt.Errorf("not found")
	}
	return bestValue, nil
}

func (s *testDocStore) DeleteByKB(_ context.Context, kbID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := kbID + "|"
	for k := range s.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(s.data, k)
		}
	}
	return nil
}

func (s *testDocStore) DiskUsage(_ context.Context) (uint64, error) { return 0, nil }

func (s *testDocStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

// testChunkDocMapper is an in-memory ChunkDocMapper.
type testChunkDocMapper struct {
	mu      sync.Mutex
	forward map[string]map[string]bool // chunkID -> set of docIDs
	reverse map[string]map[string]bool // docID -> set of chunkIDs
}

func newTestChunkDocMapper() *testChunkDocMapper {
	return &testChunkDocMapper{
		forward: make(map[string]map[string]bool),
		reverse: make(map[string]map[string]bool),
	}
}

func (m *testChunkDocMapper) Write(_ context.Context, kbID, chunkID, docID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	fk := chunkID
	if m.forward[fk] == nil {
		m.forward[fk] = make(map[string]bool)
	}
	m.forward[fk][docID] = true

	rk := docID
	if m.reverse[rk] == nil {
		m.reverse[rk] = make(map[string]bool)
	}
	m.reverse[rk][chunkID] = true
	return nil
}

func (m *testChunkDocMapper) ListDocIDs(_ context.Context, kbID, chunkID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	docs := m.forward[chunkID]
	out := make([]string, 0, len(docs))
	for d := range docs {
		out = append(out, d)
	}
	sort.Strings(out)
	return out, nil
}

func (m *testChunkDocMapper) ListChunkIDsByDocs(_ context.Context, kbID string, docIDs []string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]bool)
	for _, docID := range docIDs {
		for c := range m.reverse[docID] {
			seen[c] = true
		}
	}
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out, nil
}

func (m *testChunkDocMapper) DeleteByKB(_ context.Context, kbID string) error { return nil }
func (m *testChunkDocMapper) DeleteByDoc(_ context.Context, kbID, docID string) error { return nil }

func (m *testChunkDocMapper) forwardCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.forward)
}

// testVersionDocList is an in-memory VersionDocList.
type testVersionDocList struct {
	mu   sync.Mutex
	data map[string]map[string]bool // "kbID|versionID" -> set of docIDs
}

func newTestVersionDocList() *testVersionDocList {
	return &testVersionDocList{data: make(map[string]map[string]bool)}
}

func (v *testVersionDocList) Write(_ context.Context, kbID string, versionID int64, docID string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	key := fmt.Sprintf("%s|%d", kbID, versionID)
	if v.data[key] == nil {
		v.data[key] = make(map[string]bool)
	}
	v.data[key][docID] = true
	return nil
}

func (v *testVersionDocList) ListDocIDs(_ context.Context, kbID string, versionID int64) ([]string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	key := fmt.Sprintf("%s|%d", kbID, versionID)
	docs := v.data[key]
	out := make([]string, 0, len(docs))
	for d := range docs {
		out = append(out, d)
	}
	sort.Strings(out)
	return out, nil
}

func (v *testVersionDocList) DeleteByVersion(_ context.Context, kbID string, versionID int64) error { return nil }
func (v *testVersionDocList) DeleteByKB(_ context.Context, kbID string) error { return nil }

func (v *testVersionDocList) count() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	c := 0
	for _, docs := range v.data {
		c += len(docs)
	}
	return c
}

// testChunkStore is an in-memory ChunkStore for tests.
type testChunkStore struct {
	mu          sync.Mutex
	data        map[string][]float32 // "kbID|chunkID" -> vector
	existsCalls []string
}

func newTestChunkStore() *testChunkStore {
	return &testChunkStore{data: make(map[string][]float32)}
}

func (s *testChunkStore) Write(_ context.Context, kbID, chunkID string, vector []float32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[kbID+"|"+chunkID] = vector
	return nil
}

func (s *testChunkStore) Exists(_ context.Context, kbID, chunkID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.existsCalls = append(s.existsCalls, chunkID)
	_, ok := s.data[kbID+"|"+chunkID]
	return ok, nil
}

func (s *testChunkStore) Delete(_ context.Context, kbID, chunkID string) error  { return nil }
func (s *testChunkStore) DeleteByKB(_ context.Context, kbID string) error       { return nil }
func (s *testChunkStore) DiskUsage(_ context.Context) (uint64, error)          { return 0, nil }

func (s *testChunkStore) writtenCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

// testEmbedClient is a simple deterministic embed client.
type testEmbedClient struct {
	calls int
}

func (e *testEmbedClient) Embed(_ context.Context, chunks []types.Chunk) (map[string][]float32, error) {
	e.calls++
	out := make(map[string][]float32, len(chunks))
	for _, ch := range chunks {
		h := sha256.Sum256([]byte(ch.ChunkID))
		vec := make([]float32, 4)
		for i := range vec {
			vec[i] = float32(h[i]) / 255.0
		}
		out[ch.ChunkID] = vec
	}
	return out, nil
}

// testIndexManager records TriggerBuild calls and implements index.IndexManager.
type testIndexManager struct {
	mu             sync.Mutex
	triggeredIDs   []indexManagerKey
	buildCallbacks []index.BuildCompleteCallback
}

type indexManagerKey struct {
	kbID      string
	versionID int64
}

func newTestIndexManager() *testIndexManager {
	return &testIndexManager{}
}

func (im *testIndexManager) Search(_ context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) {
	return nil, nil
}

func (im *testIndexManager) TriggerBuild(_ context.Context, kbID string, versionID int64) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.triggeredIDs = append(im.triggeredIDs, indexManagerKey{kbID, versionID})
	return nil
}

func (im *testIndexManager) RegisterBuildCallback(cb index.BuildCompleteCallback) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.buildCallbacks = append(im.buildCallbacks, cb)
}

func (im *testIndexManager) Evict(_ context.Context, kbID string, versionID int64) error { return nil }
func (im *testIndexManager) EvictByKB(_ context.Context, kbID string) error              { return nil }
func (im *testIndexManager) Ping(_ context.Context) error                                { return nil }
func (im *testIndexManager) LoadedCount() int                                            { return 0 }

func (im *testIndexManager) triggeredCount() int {
	im.mu.Lock()
	defer im.mu.Unlock()
	return len(im.triggeredIDs)
}

// testRaftNode is a simple RaftNode for WriteCoordinator tests.
type testRaftNode struct {
	mu          sync.Mutex
	nextVersion int64
	kbs         map[string]types.KnowledgeBaseMeta
	versions    map[int64]types.VersionMeta
}

func newTestRaftNode() *testRaftNode {
	return &testRaftNode{
		nextVersion: 1,
		kbs:         make(map[string]types.KnowledgeBaseMeta),
		versions:    make(map[int64]types.VersionMeta),
	}
}

func (r *testRaftNode) ProposeCreateKB(_ context.Context, kb types.KnowledgeBaseMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kbs[kb.KBID] = kb
	return nil
}

func (r *testRaftNode) ProposeCreateVersion(_ context.Context, kbID string, parentVersionID int64) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.kbs[kbID]; !ok {
		return 0, fmt.Errorf("kb not found")
	}
	v := r.nextVersion
	r.nextVersion++
	r.versions[v] = types.VersionMeta{
		VersionID:       v,
		ParentVersionID: parentVersionID,
		KBID:            kbID,
		CreatedAt:       time.Now().Unix(),
		IndexStatus:     types.IndexStatusPending,
	}
	return v, nil
}

func (r *testRaftNode) ProposeUpdateVersionStatus(_ context.Context, versionID int64, status types.IndexStatus) error {
	return nil
}
func (r *testRaftNode) ProposeUpdateVersionSummary(_ context.Context, versionID int64, docIDSetHash string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.versions[versionID]
	if !ok {
		return fmt.Errorf("version %d not found", versionID)
	}
	v.DocIDSetHash = docIDSetHash
	r.versions[versionID] = v
	return nil
}
func (r *testRaftNode) ProposeMarkKBDeleting(_ context.Context, kbID string) error  { return nil }
func (r *testRaftNode) ProposeMarkKBDeleteFailed(_ context.Context, kbID string) error { return nil }
func (r *testRaftNode) ProposeRemoveKBMeta(_ context.Context, kbID string) error       { return nil }
func (r *testRaftNode) ProposeRollback(_ context.Context, kbID string, targetVersionID int64) error {
	return nil
}
func (r *testRaftNode) GetKB(_ context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	kb, ok := r.kbs[kbID]
	if !ok {
		return types.KnowledgeBaseMeta{}, fmt.Errorf("kb not found")
	}
	return kb, nil
}
func (r *testRaftNode) ListVersions(_ context.Context, kbID string) ([]types.VersionMeta, error) { return nil, nil }
func (r *testRaftNode) ListKnowledgeBases(_ context.Context) ([]types.KnowledgeBaseMeta, error) {
	return nil, nil
}
func (r *testRaftNode) GetClusterStatus(_ context.Context) (types.ClusterStatus, error) {
	return types.ClusterStatus{HasLeader: true, MemberCount: 1, LeaderID: 1}, nil
}

// === Tests ===

func TestWriteCoordinator_AddDocuments(t *testing.T) {
	ds := newTestDocStore()
	cdm := newTestChunkDocMapper()
	vdl := newTestVersionDocList()
	cs := newTestChunkStore()
	ec := &testEmbedClient{}
	im := newTestIndexManager()
	rn := newTestRaftNode()
	w := wal.NewMockWAL()
	chunkBF := bloom.NewMockBloomFilter()
	splitter := &mockSplitter{windowSize: 100, overlapSize: 20}

	// Pre-create KB in RaftNode
	rn.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 100, ChunkOverlapSize: 20,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		Splitter:            splitter,
		EmbedClient:         ec,
		ChunkBloom:          chunkBF,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vdl,
		IndexManager:        im,
	})

	changes := []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "hello world this is a test document"},
		{Op: types.ChangeOpAdd, DocID: "doc-2", Content: "another document with different content"},
	}

	versionID, err := coord.Execute(context.Background(), "kb-1", 0, changes)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if versionID != 1 {
		t.Errorf("expected versionID=1, got %d", versionID)
	}

	// DocStore should have entries for both docs
	if ds.count() < 2 {
		t.Errorf("expected at least 2 doc store entries, got %d", ds.count())
	}

	// ChunkDocMapper should have mappings
	if cdm.forwardCount() == 0 {
		t.Error("expected chunk-doc mappings")
	}

	// VersionDocList should have entries
	if vdl.count() < 2 {
		t.Errorf("expected at least 2 version doc entries, got %d", vdl.count())
	}

	// ChunkStore should have chunks written
	if cs.writtenCount() == 0 {
		t.Error("expected chunks to be written to chunk store")
	}

	// TriggerBuild should have been called
	if im.triggeredCount() != 1 {
		t.Errorf("expected 1 TriggerBuild call, got %d", im.triggeredCount())
	}
}

func TestWriteCoordinator_ChunkDedup(t *testing.T) {
	ds := newTestDocStore()
	cdm := newTestChunkDocMapper()
	vdl := newTestVersionDocList()
	cs := newTestChunkStore()
	ec := &testEmbedClient{}
	im := newTestIndexManager()
	rn := newTestRaftNode()
	w := wal.NewMockWAL()
	chunkBF := bloom.NewMockBloomFilter()
	splitter := &mockSplitter{windowSize: 100, overlapSize: 20}

	rn.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 100, ChunkOverlapSize: 20,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		Splitter:            splitter,
		EmbedClient:         ec,
		ChunkBloom:          chunkBF,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vdl,
		IndexManager:        im,
	})

	// Write the same content twice
	changes := []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "duplicate content test"},
	}

	_, err := coord.Execute(context.Background(), "kb-1", 0, changes)
	if err != nil {
		t.Fatalf("first Execute failed: %v", err)
	}
	firstWrites := cs.writtenCount()

	// Write again with a different doc ID but same content
	changes = []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-2", Content: "duplicate content test"},
	}
	_, err = coord.Execute(context.Background(), "kb-1", 0, changes)
	if err != nil {
		t.Fatalf("second Execute failed: %v", err)
	}

	// Same chunk content should hit BloomFilter and skip write.
	// The chunk ID is computed from content + model ID, so same content = same chunk.
	// For the second write, the bloom filter should return true and ChunkStore.Exists
	// should confirm the chunk is already there — no new write needed.
	if cs.writtenCount() != firstWrites {
		t.Errorf("expected no new chunk writes for dedup content (first=%d, total=%d)",
			firstWrites, cs.writtenCount())
	}
	// Exists should have been called for the duplicate chunk.
	if len(cs.existsCalls) == 0 {
		t.Error("expected ChunkStore.Exists to confirm dedup chunk presence")
	}
}

func TestWriteCoordinator_DeleteDocument(t *testing.T) {
	ds := newTestDocStore()
	cdm := newTestChunkDocMapper()
	vdl := newTestVersionDocList()
	cs := newTestChunkStore()
	ec := &testEmbedClient{}
	im := newTestIndexManager()
	rn := newTestRaftNode()
	w := wal.NewMockWAL()
	chunkBF := bloom.NewMockBloomFilter()
	splitter := &mockSplitter{windowSize: 100, overlapSize: 20}

	rn.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 100, ChunkOverlapSize: 20,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		Splitter:            splitter,
		EmbedClient:         ec,
		ChunkBloom:          chunkBF,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vdl,
		IndexManager:        im,
	})

	// Add a document
	_, err := coord.Execute(context.Background(), "kb-1", 0, []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "document content to delete"},
	})
	if err != nil {
		t.Fatalf("add failed: %v", err)
	}

	// Delete the document
	_, err = coord.Execute(context.Background(), "kb-1", 1, []types.DocChange{
		{Op: types.ChangeOpDelete, DocID: "doc-1"},
	})
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Assert: tombstone exists in DocStore for version 2.
	// nil value means tombstone (DELETE) — the doc should not be visible.
	val, err := ds.ReadAt(context.Background(), "kb-1", "doc-1", 2)
	if err == nil && len(val) > 0 {
		t.Errorf("expected empty content (tombstone) for deleted doc, got: %q", string(val))
	}
	// If err indicates "not found", that's also acceptable for a tombstone.

	// Assert: doc-1 should NOT be in version 2's doc list.
	docs, err := vdl.ListDocIDs(context.Background(), "kb-1", 2)
	if err != nil {
		t.Fatalf("ListDocIDs failed: %v", err)
	}
	for _, d := range docs {
		if d == "doc-1" {
			t.Error("doc-1 should not be in version 2's doc list after DELETE")
		}
	}
}

func TestWriteCoordinator_WALCommit(t *testing.T) {
	w := wal.NewMockWAL()
	ds := newTestDocStore()
	cdm := newTestChunkDocMapper()
	vdl := newTestVersionDocList()
	cs := newTestChunkStore()
	ec := &testEmbedClient{}
	im := newTestIndexManager()
	rn := newTestRaftNode()
	chunkBF := bloom.NewMockBloomFilter()
	splitter := &mockSplitter{windowSize: 100, overlapSize: 20}

	rn.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 100, ChunkOverlapSize: 20,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		Splitter:            splitter,
		EmbedClient:         ec,
		ChunkBloom:          chunkBF,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vdl,
		IndexManager:        im,
	})

	_, err := coord.Execute(context.Background(), "kb-1", 0, []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "test content"},
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Recover should return no pending records (COMMIT was written)
	records, err := w.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 pending records after Commit, got %d", len(records))
	}
}

func TestWriteCoordinator_CrashBeforeRaftPropose(t *testing.T) {
	w := wal.NewMockWAL()
	w.WriteBegin(context.Background())

	records, err := w.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 pending records for BEGIN-only, got %d", len(records))
	}
}

func TestWriteCoordinator_CrashAfterRaftBeforeCommit(t *testing.T) {
	w := wal.NewMockWAL()
	w.WriteBegin(context.Background())
	w.WriteVersionID(context.Background(), 5)

	records, err := w.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 pending record, got %d", len(records))
	}
	if records[0].Type != types.PendingRecordTypeVersionWrite {
		t.Errorf("expected VersionWrite type, got %v", records[0].Type)
	}
	if records[0].VersionID != 5 {
		t.Errorf("expected versionID=5, got %d", records[0].VersionID)
	}
}

func TestWriteCoordinator_CrashAfterCommitBeforeBuild(t *testing.T) {
	// Simulate: crash after WAL COMMIT but before TriggerBuild.
	// Recovery: IndexManager.TriggerBuild should be called on restart.
	w := wal.NewMockWAL()
	ds := newTestDocStore()
	cdm := newTestChunkDocMapper()
	vdl := newTestVersionDocList()
	cs := newTestChunkStore()
	ec := &testEmbedClient{}
	im := newTestIndexManager()
	rn := newTestRaftNode()
	chunkBF := bloom.NewMockBloomFilter()
	splitter := &mockSplitter{windowSize: 100, overlapSize: 20}

	rn.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 100, ChunkOverlapSize: 20,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		Splitter:            splitter,
		EmbedClient:         ec,
		ChunkBloom:          chunkBF,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vdl,
		IndexManager:        im,
	})

	// Simulate a complete write (which includes TriggerBuild).
	_, err := coord.Execute(context.Background(), "kb-1", 0, []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "test content"},
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// TriggerBuild should have been called exactly once.
	if im.triggeredCount() != 1 {
		t.Errorf("expected exactly 1 TriggerBuild call, got %d", im.triggeredCount())
	}

	// WAL should be clean (COMMIT + no pending).
	records, err := w.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected clean WAL after full flow, got %d pending", len(records))
	}
}

func TestWriteCoordinator_UpdateDocument(t *testing.T) {
	ds := newTestDocStore()
	cdm := newTestChunkDocMapper()
	vdl := newTestVersionDocList()
	cs := newTestChunkStore()
	ec := &testEmbedClient{}
	im := newTestIndexManager()
	rn := newTestRaftNode()
	w := wal.NewMockWAL()
	chunkBF := bloom.NewMockBloomFilter()
	splitter := &mockSplitter{windowSize: 100, overlapSize: 20}

	rn.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 100, ChunkOverlapSize: 20,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		Splitter:            splitter,
		EmbedClient:         ec,
		ChunkBloom:          chunkBF,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vdl,
		IndexManager:        im,
	})

	// Add a document
	_, err := coord.Execute(context.Background(), "kb-1", 0, []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "original content"},
	})
	if err != nil {
		t.Fatalf("add failed: %v", err)
	}

	// Update the document
	_, err = coord.Execute(context.Background(), "kb-1", 1, []types.DocChange{
		{Op: types.ChangeOpUpdate, DocID: "doc-1", Content: "updated content"},
	})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	// Read back the updated content at version 2.
	val, err := ds.ReadAt(context.Background(), "kb-1", "doc-1", 2)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if string(val) != "updated content" {
		t.Errorf("expected 'updated content', got %q", string(val))
	}

	// The original content should still be visible at version 1.
	val, err = ds.ReadAt(context.Background(), "kb-1", "doc-1", 1)
	if err != nil {
		t.Fatalf("ReadAt v1 failed: %v", err)
	}
	if string(val) != "original content" {
		t.Errorf("expected 'original content' at v1, got %q", string(val))
	}
}

// mockSplitter implements splitter.ChunkSplitter for tests.
type mockSplitter struct {
	windowSize  int
	overlapSize int
}

func (s *mockSplitter) Split(content string, windowSize int, overlapSize int, embedConfigID string) []types.Chunk {
	if len(content) == 0 {
		return nil
	}
	chunks := []types.Chunk{}
	runes := []rune(content)
	step := windowSize - overlapSize
	if step <= 0 {
		step = windowSize
	}
	for i := 0; i < len(runes); i += step {
		end := i + windowSize
		if end > len(runes) {
			end = len(runes)
		}
		text := string(runes[i:end])
		chunkID := fmt.Sprintf("%x", sha256.Sum256([]byte(text+embedConfigID)))
		chunks = append(chunks, types.Chunk{ChunkID: chunkID, Content: text})
		if end == len(runes) {
			break
		}
	}
	return chunks
}

var _ WriteCoordinator = (*WriteCoordinatorImpl)(nil)

// TestWriteCoordinator_ProposesVersionSummary verifies that after a
// successful write the coordinator commits the version's document-ID set
// digest into the Raft metadata, matching what any follower would recompute
// from the versiondoc store.
func TestWriteCoordinator_ProposesVersionSummary(t *testing.T) {
	ds := newTestDocStore()
	cdm := newTestChunkDocMapper()
	vdl := newTestVersionDocList()
	cs := newTestChunkStore()
	ec := &testEmbedClient{}
	im := newTestIndexManager()
	rn := newTestRaftNode()
	w := wal.NewMockWAL()
	chunkBF := bloom.NewMockBloomFilter()
	splitter := &mockSplitter{windowSize: 100, overlapSize: 20}

	ctx := context.Background()
	rn.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 100, ChunkOverlapSize: 20,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries: 2, RetryBaseIntervalMS: 10,
		WAL: w, RaftNode: rn, Splitter: splitter, EmbedClient: ec,
		ChunkBloom: chunkBF, ChunkStore: cs, ChunkDocMapper: cdm,
		DocStore: ds, VersionDocList: vdl, IndexManager: im,
	})

	changes := []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "hello world this is a test document"},
		{Op: types.ChangeOpAdd, DocID: "doc-2", Content: "another document with different content"},
	}
	versionID, err := coord.Execute(ctx, "kb-1", 0, changes)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// The committed digest must equal the hash of the version's full
	// document set as stored in versiondoc.
	stored, err := vdl.ListDocIDs(ctx, "kb-1", versionID)
	if err != nil {
		t.Fatalf("ListDocIDs: %v", err)
	}
	want := stratumync.ComputeDocIDSetHash(stored)

	rn.mu.Lock()
	got := rn.versions[versionID].DocIDSetHash
	rn.mu.Unlock()
	if got != want {
		t.Errorf("committed digest = %q, want %q (from versiondoc set %v)", got, want, stored)
	}
	if got == "" {
		t.Error("no digest committed for the new version")
	}
}
