package versiondoc

import (
	"context"
	"sync"
)

// versionKey is the in-memory composite key for a (kbID, versionID) pair.
type versionKey struct {
	kbID      string
	versionID int64
}

// MockVersionDocList is an in-memory VersionDocList for use in unit tests
// of modules that depend on VersionDocList (WriteCoordinator, IndexManager,
// QueryService, etc.). It is not a substitute for the real PebbleDB-backed
// implementation's own tests (see T1-3 in Stratum_测试顺序.md).
type MockVersionDocList struct {
	mu sync.Mutex
	// docs[versionKey] = set of docIDs
	docs map[versionKey]map[string]struct{}
}

// NewMockVersionDocList constructs an empty MockVersionDocList.
func NewMockVersionDocList() *MockVersionDocList {
	return &MockVersionDocList{
		docs: make(map[versionKey]map[string]struct{}),
	}
}

func (v *MockVersionDocList) Write(_ context.Context, kbID string, versionID int64, docID string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	key := versionKey{kbID, versionID}
	if v.docs[key] == nil {
		v.docs[key] = make(map[string]struct{})
	}
	v.docs[key][docID] = struct{}{}
	return nil
}

func (v *MockVersionDocList) ListDocIDs(_ context.Context, kbID string, versionID int64) ([]string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	set := v.docs[versionKey{kbID, versionID}]
	out := make([]string, 0, len(set))
	for docID := range set {
		out = append(out, docID)
	}
	return out, nil
}

func (v *MockVersionDocList) DeleteByVersion(_ context.Context, kbID string, versionID int64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.docs, versionKey{kbID, versionID})
	return nil
}

func (v *MockVersionDocList) DeleteByKB(_ context.Context, kbID string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	for key := range v.docs {
		if key.kbID == kbID {
			delete(v.docs, key)
		}
	}
	return nil
}

// Reset clears all stored state. Convenience for tests; not part of the
// VersionDocList interface.
func (v *MockVersionDocList) Reset() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.docs = make(map[versionKey]map[string]struct{})
}

var _ VersionDocList = (*MockVersionDocList)(nil)
