package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"

	"stratum/internal/index"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// deleteTestRaftNode is a RaftNode double for DeleteCoordinator tests.
type deleteTestRaftNode struct {
	mu              sync.Mutex
	removedMetadata []string
	deleteFailed    []string
	removeErr       error
}

func newDeleteTestRaftNode() *deleteTestRaftNode {
	return &deleteTestRaftNode{}
}

func (r *deleteTestRaftNode) ProposeCreateKB(_ context.Context, kb types.KnowledgeBaseMeta) error { return nil }
func (r *deleteTestRaftNode) ProposeCreateVersion(_ context.Context, kbID string, parentVersionID int64) (int64, error) {
	return 0, nil
}
func (r *deleteTestRaftNode) ProposeUpdateVersionStatus(_ context.Context, versionID int64, status types.IndexStatus) error {
	return nil
}
func (r *deleteTestRaftNode) ProposeUpdateVersionSummary(_ context.Context, versionID int64, docIDSetHash string) error {
	return nil
}
func (r *deleteTestRaftNode) ProposeMarkKBDeleting(_ context.Context, kbID string) error { return nil }
func (r *deleteTestRaftNode) ProposeMarkKBDeleteFailed(_ context.Context, kbID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleteFailed = append(r.deleteFailed, kbID)
	return nil
}
func (r *deleteTestRaftNode) ProposeRemoveKBMeta(_ context.Context, kbID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.removeErr != nil {
		return r.removeErr
	}
	r.removedMetadata = append(r.removedMetadata, kbID)
	return nil
}
func (r *deleteTestRaftNode) ProposeRollback(_ context.Context, kbID string, targetVersionID int64) error { return nil }
func (r *deleteTestRaftNode) GetKB(_ context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
	return types.KnowledgeBaseMeta{}, nil
}
func (r *deleteTestRaftNode) ListVersions(_ context.Context, kbID string) ([]types.VersionMeta, error) { return nil, nil }
func (r *deleteTestRaftNode) ListKnowledgeBases(_ context.Context) ([]types.KnowledgeBaseMeta, error) {
	return nil, nil
}
func (r *deleteTestRaftNode) GetClusterStatus(_ context.Context) (types.ClusterStatus, error) {
	return types.ClusterStatus{}, nil
}

// deleteTestDocStore tracks DeleteByKB calls.
type deleteTestDocStore struct {
	mu       sync.Mutex
	deleted  []string
	deleteErr error
}

func newDeleteTestDocStore() *deleteTestDocStore { return &deleteTestDocStore{} }
func (s *deleteTestDocStore) Write(_ context.Context, kbID, docID string, versionID int64, value []byte) error {
	return nil
}
func (s *deleteTestDocStore) ReadAt(_ context.Context, kbID, docID string, maxVersionID int64) ([]byte, error) {
	return nil, nil
}
func (s *deleteTestDocStore) DeleteByKB(_ context.Context, kbID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.deleted = append(s.deleted, kbID)
	return nil
}
func (s *deleteTestDocStore) DiskUsage(_ context.Context) (uint64, error) { return 0, nil }

// deleteTestChunkStore tracks DeleteByKB calls.
type deleteTestChunkStore struct {
	mu       sync.Mutex
	deleted  []string
	deleteErr error
}

func newDeleteTestChunkStore() *deleteTestChunkStore { return &deleteTestChunkStore{} }
func (s *deleteTestChunkStore) Write(_ context.Context, kbID, chunkID string, vector []float32) error {
	return nil
}
func (s *deleteTestChunkStore) Exists(_ context.Context, kbID, chunkID string) (bool, error) { return false, nil }
func (s *deleteTestChunkStore) Delete(_ context.Context, kbID, chunkID string) error         { return nil }
func (s *deleteTestChunkStore) DeleteByKB(_ context.Context, kbID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.deleted = append(s.deleted, kbID)
	return nil
}
func (s *deleteTestChunkStore) DiskUsage(_ context.Context) (uint64, error) { return 0, nil }

// deleteTestChunkDocMapper tracks DeleteByKB calls.
type deleteTestChunkDocMapper struct {
	mu       sync.Mutex
	deleted  []string
	deleteErr error
}

func newDeleteTestChunkDocMapper() *deleteTestChunkDocMapper { return &deleteTestChunkDocMapper{} }
func (m *deleteTestChunkDocMapper) Write(_ context.Context, kbID, chunkID, docID string) error {
	return nil
}
func (m *deleteTestChunkDocMapper) ListDocIDs(_ context.Context, kbID, chunkID string) ([]string, error) {
	return nil, nil
}
func (m *deleteTestChunkDocMapper) ListChunkIDsByDocs(_ context.Context, kbID string, docIDs []string) ([]string, error) {
	return nil, nil
}
func (m *deleteTestChunkDocMapper) DeleteByDoc(_ context.Context, kbID, docID string) error { return nil }
func (m *deleteTestChunkDocMapper) DeleteByKB(_ context.Context, kbID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteErr != nil {
		return m.deleteErr
	}
	m.deleted = append(m.deleted, kbID)
	return nil
}

// deleteTestVersionDocList tracks DeleteByKB calls.
type deleteTestVersionDocList struct {
	mu       sync.Mutex
	deleted  []string
	deleteErr error
}

func newDeleteTestVersionDocList() *deleteTestVersionDocList { return &deleteTestVersionDocList{} }
func (v *deleteTestVersionDocList) Write(_ context.Context, kbID string, versionID int64, docID string) error {
	return nil
}
func (v *deleteTestVersionDocList) ListDocIDs(_ context.Context, kbID string, versionID int64) ([]string, error) {
	return nil, nil
}
func (v *deleteTestVersionDocList) DeleteByVersion(_ context.Context, kbID string, versionID int64) error {
	return nil
}
func (v *deleteTestVersionDocList) DeleteByKB(_ context.Context, kbID string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.deleteErr != nil {
		return v.deleteErr
	}
	v.deleted = append(v.deleted, kbID)
	return nil
}

// deleteTestIndexManager tracks EvictByKB calls.
type deleteTestIndexManager struct {
	mu        sync.Mutex
	evictedKB []string
	evictErr  error
}

func newDeleteTestIndexManager() *deleteTestIndexManager { return &deleteTestIndexManager{} }
func (im *deleteTestIndexManager) Search(_ context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) {
	return nil, nil
}
func (im *deleteTestIndexManager) TriggerBuild(_ context.Context, kbID string, versionID int64) error { return nil }
func (im *deleteTestIndexManager) RegisterBuildCallback(cb index.BuildCompleteCallback) {}
func (im *deleteTestIndexManager) Evict(_ context.Context, kbID string, versionID int64) error       { return nil }
func (im *deleteTestIndexManager) EvictByKB(_ context.Context, kbID string) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	if im.evictErr != nil {
		return im.evictErr
	}
	im.evictedKB = append(im.evictedKB, kbID)
	return nil
}
func (im *deleteTestIndexManager) Ping(_ context.Context) error { return nil }
func (im *deleteTestIndexManager) LoadedCount() int               { return 0 }

// === Tests ===

func TestDeleteCoordinator_FullCleanup(t *testing.T) {
	w := wal.NewMockWAL()
	rn := newDeleteTestRaftNode()
	ds := newDeleteTestDocStore()
	cs := newDeleteTestChunkStore()
	cdm := newDeleteTestChunkDocMapper()
	vdl := newDeleteTestVersionDocList()
	im := newDeleteTestIndexManager()

	coord := NewDeleteCoordinatorImpl(DeleteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		VersionDocList:      vdl,
	})

	// Write delete mark first, as the Service layer would.
	w.WriteDeleteMark(context.Background(), "kb-1")

	err := coord.Execute(context.Background(), "kb-1")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// All stores should have been cleaned.
	if len(ds.deleted) != 1 || ds.deleted[0] != "kb-1" {
		t.Error("DocStore.DeleteByKB should have been called for kb-1")
	}
	if len(cs.deleted) != 1 || cs.deleted[0] != "kb-1" {
		t.Error("ChunkStore.DeleteByKB should have been called for kb-1")
	}
	if len(cdm.deleted) != 1 || cdm.deleted[0] != "kb-1" {
		t.Error("ChunkDocMapper.DeleteByKB should have been called for kb-1")
	}
	if len(vdl.deleted) != 1 || vdl.deleted[0] != "kb-1" {
		t.Error("VersionDocList.DeleteByKB should have been called for kb-1")
	}
	if len(rn.removedMetadata) != 1 || rn.removedMetadata[0] != "kb-1" {
		t.Error("RaftNode.ProposeRemoveKBMeta should have been called for kb-1")
	}
	if len(im.evictedKB) != 1 || im.evictedKB[0] != "kb-1" {
		t.Error("IndexManager.EvictByKB should have been called for kb-1")
	}

	// WAL should have DeleteComplete record.
	records, err := w.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 pending records after complete cleanup, got %d", len(records))
	}
}

func TestDeleteCoordinator_ProposeRemoveKBMetaIdempotent(t *testing.T) {
	w := wal.NewMockWAL()
	rn := newDeleteTestRaftNode()
	// Simulate KB metadata already removed.
	rn.removeErr = stratumerrors.ErrKnowledgeBaseNotFound // real impl treats this as success
	ds := newDeleteTestDocStore()
	cs := newDeleteTestChunkStore()
	cdm := newDeleteTestChunkDocMapper()
	vdl := newDeleteTestVersionDocList()
	im := newDeleteTestIndexManager()

	coord := NewDeleteCoordinatorImpl(DeleteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		VersionDocList:      vdl,
	})

	w.WriteDeleteMark(context.Background(), "kb-1")

	// Should complete successfully even though ProposeRemoveKBMeta
	// returns an error that indicates the KB is already gone.
	err := coord.Execute(context.Background(), "kb-1")
	if err != nil {
		t.Fatalf("Execute should handle absent KB metadata: %v", err)
	}
}

func TestDeleteCoordinator_WALDeleteComplete(t *testing.T) {
	w := wal.NewMockWAL()
	rn := newDeleteTestRaftNode()
	ds := newDeleteTestDocStore()
	cs := newDeleteTestChunkStore()
	cdm := newDeleteTestChunkDocMapper()
	vdl := newDeleteTestVersionDocList()
	im := newDeleteTestIndexManager()

	coord := NewDeleteCoordinatorImpl(DeleteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		VersionDocList:      vdl,
	})

	w.WriteDeleteMark(context.Background(), "kb-1")
	err := coord.Execute(context.Background(), "kb-1")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// After Execute, no pending records should remain.
	records, err := w.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected WAL to be clean after delete complete, got %d pending", len(records))
	}
}

func TestDeleteCoordinator_DiskFileNotExistIgnored(t *testing.T) {
	w := wal.NewMockWAL()
	rn := newDeleteTestRaftNode()
	ds := newDeleteTestDocStore()
	cs := newDeleteTestChunkStore()
	cdm := newDeleteTestChunkDocMapper()
	vdl := newDeleteTestVersionDocList()
	im := newDeleteTestIndexManager()

	coord := NewDeleteCoordinatorImpl(DeleteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		VersionDocList:      vdl,
	})

	w.WriteDeleteMark(context.Background(), "kb-1")

	// EvictByKB should succeed even with no loaded indexes (no-op).
	// All prefix-scan deletes on empty stores should succeed.
	err := coord.Execute(context.Background(), "kb-1")
	if err != nil {
		t.Fatalf("Execute should handle empty stores gracefully: %v", err)
	}

	// WAL should be clean.
	records, _ := w.Recover(context.Background())
	if len(records) != 0 {
		t.Errorf("expected clean WAL, got %d pending", len(records))
	}
}

func TestDeleteCoordinator_PerStepCrashRecovery(t *testing.T) {
	// Simulate crash after some cleanup steps are done but before completion.
	// Recovery should be able to resume and finish cleanup.
	w := wal.NewMockWAL()
	rn := newDeleteTestRaftNode()
	ds := newDeleteTestDocStore()
	cs := newDeleteTestChunkStore()
	cdm := newDeleteTestChunkDocMapper()
	vdl := newDeleteTestVersionDocList()
	im := newDeleteTestIndexManager()

	w.WriteDeleteMark(context.Background(), "kb-1")

	// Simulate that some steps already ran (e.g., EvictByKB and DocStore.DeleteByKB
	// completed before a crash). The remaining steps should still work.
	// We simulate this by calling partial steps directly, then running Execute.
	im.EvictByKB(context.Background(), "kb-1")
	ds.DeleteByKB(context.Background(), "kb-1")

	coord := NewDeleteCoordinatorImpl(DeleteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		VersionDocList:      vdl,
	})

	// Execute should complete the remaining steps.
	err := coord.Execute(context.Background(), "kb-1")
	if err != nil {
		t.Fatalf("resumed cleanup should succeed: %v", err)
	}

	// All stores should be cleaned.
	if len(cs.deleted) != 1 || cs.deleted[0] != "kb-1" {
		t.Error("ChunkStore should be cleaned on resume")
	}
	// WAL should have DeleteComplete.
	records, _ := w.Recover(context.Background())
	if len(records) != 0 {
		t.Errorf("expected clean WAL after resume, got %d pending", len(records))
	}
}

func TestDeleteCoordinator_RetryExhaustedMarksFailed(t *testing.T) {
	w := wal.NewMockWAL()
	rn := newDeleteTestRaftNode()
	ds := newDeleteTestDocStore()
	ds.deleteErr = errors.New("persistent disk error")
	cs := newDeleteTestChunkStore()
	cdm := newDeleteTestChunkDocMapper()
	vdl := newDeleteTestVersionDocList()
	im := newDeleteTestIndexManager()

	coord := NewDeleteCoordinatorImpl(DeleteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		VersionDocList:      vdl,
	})

	w.WriteDeleteMark(context.Background(), "kb-1")
	err := coord.Execute(context.Background(), "kb-1")
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}

	// RaftNode should have been told to mark the KB as delete-failed.
	if len(rn.deleteFailed) != 1 || rn.deleteFailed[0] != "kb-1" {
		t.Errorf("expected ProposeMarkKBDeleteFailed to be called, got %v", rn.deleteFailed)
	}
}
