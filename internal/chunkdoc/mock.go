package chunkdoc

import (
	"context"
	"sync"
)

// MockChunkDocMapper is an in-memory ChunkDocMapper for use in unit tests
// of modules that depend on ChunkDocMapper (WriteCoordinator, IndexManager,
// QueryService, etc.). It is not a substitute for the real PebbleDB-backed
// implementation's own tests (see T1-2 in Stratum_测试顺序.md).
type MockChunkDocMapper struct {
	mu sync.Mutex
	// forward[kbID][chunkID] = set of docIDs
	forward map[string]map[string]map[string]struct{}
	// reverse[kbID][docID] = set of chunkIDs
	reverse map[string]map[string]map[string]struct{}
}

// NewMockChunkDocMapper constructs an empty MockChunkDocMapper.
func NewMockChunkDocMapper() *MockChunkDocMapper {
	return &MockChunkDocMapper{
		forward: make(map[string]map[string]map[string]struct{}),
		reverse: make(map[string]map[string]map[string]struct{}),
	}
}

func (m *MockChunkDocMapper) Write(_ context.Context, kbID, chunkID, docID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.forward[kbID] == nil {
		m.forward[kbID] = make(map[string]map[string]struct{})
	}
	if m.forward[kbID][chunkID] == nil {
		m.forward[kbID][chunkID] = make(map[string]struct{})
	}
	m.forward[kbID][chunkID][docID] = struct{}{}

	if m.reverse[kbID] == nil {
		m.reverse[kbID] = make(map[string]map[string]struct{})
	}
	if m.reverse[kbID][docID] == nil {
		m.reverse[kbID][docID] = make(map[string]struct{})
	}
	m.reverse[kbID][docID][chunkID] = struct{}{}

	return nil
}

func (m *MockChunkDocMapper) ListDocIDs(_ context.Context, kbID, chunkID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	set := m.forward[kbID][chunkID]
	out := make([]string, 0, len(set))
	for docID := range set {
		out = append(out, docID)
	}
	return out, nil
}

func (m *MockChunkDocMapper) ListChunkIDsByDocs(_ context.Context, kbID string, docIDs []string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	merged := make(map[string]struct{})
	for _, docID := range docIDs {
		for chunkID := range m.reverse[kbID][docID] {
			merged[chunkID] = struct{}{}
		}
	}
	out := make([]string, 0, len(merged))
	for chunkID := range merged {
		out = append(out, chunkID)
	}
	return out, nil
}

func (m *MockChunkDocMapper) DeleteByKB(_ context.Context, kbID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.forward, kbID)
	delete(m.reverse, kbID)
	return nil
}

func (m *MockChunkDocMapper) DeleteByDoc(_ context.Context, kbID, docID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove docID from every chunk's forward set.
	chunkIDs := m.reverse[kbID][docID]
	for chunkID := range chunkIDs {
		if fwd, ok := m.forward[kbID][chunkID]; ok {
			delete(fwd, docID)
			if len(fwd) == 0 {
				delete(m.forward[kbID], chunkID)
			}
		}
	}
	// Remove the reverse entry for docID entirely.
	if rev, ok := m.reverse[kbID]; ok {
		delete(rev, docID)
	}
	return nil
}

// Reset clears all stored state. Convenience for tests; not part of the
// ChunkDocMapper interface.
func (m *MockChunkDocMapper) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.forward = make(map[string]map[string]map[string]struct{})
	m.reverse = make(map[string]map[string]map[string]struct{})
}

var _ ChunkDocMapper = (*MockChunkDocMapper)(nil)
