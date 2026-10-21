package index

import (
	"testing"
	"time"
)

// Resident is what the query path's background warm-up reads before asking any peer
// (service/query.go warmIndexInBackground), so all three states matter: absent (warm
// up), loading (a build is in flight here — its artifact is not on disk yet, so
// IndexExists would still say "absent"), and loaded. The key is per version: a sibling
// version being resident says nothing about this one.
func TestIndexManager_Resident(t *testing.T) {
	im := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: 50 * time.Millisecond})
	key := indexKey{"kb-1", 1}

	if im.Resident("kb-1", 1) {
		t.Fatal("nothing loaded or building: Resident must be false, or the warm-up never runs")
	}

	im.mu.Lock()
	im.loading[key] = true
	im.mu.Unlock()
	if !im.Resident("kb-1", 1) {
		t.Fatal("a build in flight must count as resident: the disk check cannot see it yet")
	}
	if im.Resident("kb-1", 2) || im.Resident("kb-2", 1) {
		t.Fatal("residency is per (kb, version)")
	}

	im.mu.Lock()
	delete(im.loading, key)
	im.loaded[key] = &loadedIndex{}
	im.mu.Unlock()
	if !im.Resident("kb-1", 1) {
		t.Fatal("a loaded index must count as resident")
	}
}

// The mock has to agree with the real implementation, or the service-layer cases built
// on it assert a warm-up decision production never makes.
func TestMockIndexManager_Resident(t *testing.T) {
	m := NewMockIndexManager(newTestDeps().deps(), 16, time.Second)
	key := indexKey{"kb-1", 1}

	if m.Resident("kb-1", 1) {
		t.Fatal("nothing loaded or building: Resident must be false")
	}

	m.mu.Lock()
	m.loading[key] = true
	m.mu.Unlock()
	if !m.Resident("kb-1", 1) {
		t.Fatal("a build in flight must count as resident")
	}
	if m.Resident("kb-1", 2) {
		t.Fatal("residency is per (kb, version)")
	}

	m.mu.Lock()
	delete(m.loading, key)
	m.loaded[key] = &builtIndex{}
	m.mu.Unlock()
	if !m.Resident("kb-1", 1) {
		t.Fatal("a loaded index must count as resident")
	}
}
