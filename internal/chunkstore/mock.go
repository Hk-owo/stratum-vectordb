package chunkstore

import (
	"context"
	"fmt"
	"sync"
)

// MockChunkStore is an in-memory ChunkStore for use in unit tests of
// modules that depend on ChunkStore (WriteCoordinator, IndexManager,
// DeleteCoordinator, etc.). It is not a substitute for the real
// gRPC-backed implementation's own tests against vecstore (see T1-8 in
// Stratum_测试顺序.md, which targets the C++ ChunkStorage/VectorIndex
// directly, and the T2-* batch which exercises ChunkStore against a real
// vecstore gRPC server).
type MockChunkStore struct {
	mu sync.Mutex
	// vectors[kbID][chunkID] = vector
	vectors map[string]map[string][]float32

	writeCount int // number of Write calls, for dedup assertions
	flushCount int // number of FlushWAL calls, for durability-ordering assertions
}

// NewMockChunkStore constructs an empty MockChunkStore.
func NewMockChunkStore() *MockChunkStore {
	return &MockChunkStore{
		vectors: make(map[string]map[string][]float32),
	}
}

func (s *MockChunkStore) Write(_ context.Context, kbID, chunkID string, vector []float32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.vectors[kbID] == nil {
		s.vectors[kbID] = make(map[string][]float32)
	}
	stored := make([]float32, len(vector))
	copy(stored, vector)
	s.vectors[kbID][chunkID] = stored
	s.writeCount++
	return nil
}

func (s *MockChunkStore) Exists(_ context.Context, kbID, chunkID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.vectors[kbID][chunkID]
	return ok, nil
}

func (s *MockChunkStore) Delete(_ context.Context, kbID, chunkID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vectors[kbID], chunkID)
	return nil
}

func (s *MockChunkStore) DeleteByKB(_ context.Context, kbID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vectors, kbID)
	return nil
}

// DiskUsage implements ChunkStore: the in-memory mock has no disk
// footprint, so it always reports zero.
func (s *MockChunkStore) DiskUsage(_ context.Context) (uint64, error) {
	return 0, nil
}

// FlushWAL implements ChunkStore: the mock's "disk" is a map, so writes are
// already as durable as they are ever going to get. It counts calls anyway,
// because the ORDER of flush-before-commit is the property worth asserting in
// tests of the write path.
func (s *MockChunkStore) FlushWAL(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushCount++
	return nil
}

// FlushCount returns how many times FlushWAL has been called.
func (s *MockChunkStore) FlushCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushCount
}

// Read returns the stored vector for kbID + chunkID. Not part of the
// ChunkStore interface (the real interface only exposes Exists, not Read,
// at the Go layer — IndexManager reads chunk vectors via the C++ vecstore
// gRPC ChunkStorageService.Read directly per the interface design doc).
// Exposed here purely as a test assertion helper.
func (s *MockChunkStore) Read(kbID, chunkID string) ([]float32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.vectors[kbID][chunkID]
	if !ok {
		return nil, fmt.Errorf("chunkstore: %s/%s not found", kbID, chunkID)
	}
	out := make([]float32, len(v))
	copy(out, v)
	return out, nil
}

// WriteCount returns how many times Write has been called. Used by tests
// asserting that chunk dedup actually skips redundant writes (e.g. T2-1
// "chunk 去重": same-content document written twice should only call
// ChunkStore.Write once).
func (s *MockChunkStore) WriteCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeCount
}

// Reset clears all stored state and counters. Convenience for tests; not
// part of the ChunkStore interface.
func (s *MockChunkStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vectors = make(map[string]map[string][]float32)
	s.writeCount = 0
	s.flushCount = 0
}

var _ ChunkStore = (*MockChunkStore)(nil)
