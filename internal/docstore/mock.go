package docstore

import (
	"context"
	"fmt"
	"sort"
	"sync"

	stratumerrors "stratum/internal/errors"
)

// MockDocStore is an in-memory DocStore implementation for use in unit
// tests of modules that depend on DocStore (WriteCoordinator, QueryService,
// etc.). It is not a substitute for the real PebbleDB-backed
// implementation's own tests (see T1-1 in Stratum_测试顺序.md) — those
// exercise PebbleDocStore directly.
type MockDocStore struct {
	mu sync.Mutex
	// entries[kbID][docID] is a version-sorted-on-read list of (versionID, value).
	// A nil value represents a tombstone.
	entries map[string]map[string][]mockDocEntry
}

type mockDocEntry struct {
	versionID int64
	value     []byte // nil means tombstone
}

// NewMockDocStore constructs an empty MockDocStore.
func NewMockDocStore() *MockDocStore {
	return &MockDocStore{
		entries: make(map[string]map[string][]mockDocEntry),
	}
}

func (m *MockDocStore) Write(_ context.Context, kbID, docID string, versionID int64, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.entries[kbID] == nil {
		m.entries[kbID] = make(map[string][]mockDocEntry)
	}
	docEntries := m.entries[kbID][docID]

	// Idempotent: writing the same (kbID, docID, versionID) again just
	// overwrites the existing entry for that version rather than
	// appending a duplicate.
	for i, e := range docEntries {
		if e.versionID == versionID {
			docEntries[i].value = value
			m.entries[kbID][docID] = docEntries
			return nil
		}
	}

	var stored []byte
	if value != nil {
		stored = make([]byte, len(value))
		copy(stored, value)
	}
	docEntries = append(docEntries, mockDocEntry{versionID: versionID, value: stored})
	sort.Slice(docEntries, func(i, j int) bool { return docEntries[i].versionID < docEntries[j].versionID })
	m.entries[kbID][docID] = docEntries
	return nil
}

func (m *MockDocStore) ReadAt(_ context.Context, kbID, docID string, maxVersionID int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	docEntries := m.entries[kbID][docID]
	var best *mockDocEntry
	for i := range docEntries {
		e := &docEntries[i]
		if e.versionID <= maxVersionID {
			if best == nil || e.versionID > best.versionID {
				best = e
			}
		}
	}
	if best == nil {
		return nil, fmt.Errorf("docstore: %s/%s not found at version %d: %w", kbID, docID, maxVersionID, stratumerrors.ErrVersionNotFound)
	}
	if best.value == nil {
		// Tombstone.
		return nil, fmt.Errorf("docstore: %s/%s deleted as of version %d: %w", kbID, docID, maxVersionID, stratumerrors.ErrVersionNotFound)
	}
	out := make([]byte, len(best.value))
	copy(out, best.value)
	return out, nil
}

func (m *MockDocStore) DeleteByKB(_ context.Context, kbID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, kbID)
	return nil
}

// DeleteByVersion implements DocStore: removes every entry for (kbID,
// versionID) across all documents; a document whose entry list becomes
// empty is dropped entirely. Idempotent.
func (m *MockDocStore) DeleteByVersion(_ context.Context, kbID string, versionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	docs := m.entries[kbID]
	for docID, entries := range docs {
		kept := entries[:0]
		for _, e := range entries {
			if e.versionID != versionID {
				kept = append(kept, e)
			}
		}
		if len(kept) == 0 {
			delete(docs, docID)
		} else {
			docs[docID] = kept
		}
	}
	return nil
}

// DiskUsage implements DocStore: the in-memory mock has no disk footprint,
// so it always reports zero.
func (m *MockDocStore) DiskUsage(_ context.Context) (uint64, error) {
	return 0, nil
}

// Reset clears all stored state. Convenience for tests; not part of the
// DocStore interface.
func (m *MockDocStore) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make(map[string]map[string][]mockDocEntry)
}

var _ DocStore = (*MockDocStore)(nil)
