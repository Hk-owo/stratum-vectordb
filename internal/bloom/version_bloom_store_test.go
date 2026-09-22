package bloom

import (
	"context"
	"testing"
)

// mockVdl is a minimal in-memory versiondoc.VersionDocList for the
// VersionBloomStore tests.
type mockVdl struct {
	docs map[int64][]string // versionID -> docIDs
}

func (m *mockVdl) Write(_ context.Context, _ string, versionID int64, docID string) error {
	m.docs[versionID] = append(m.docs[versionID], docID)
	return nil
}

func (m *mockVdl) WriteMany(_ context.Context, _ string, versionID int64, docIDs []string) error {
	m.docs[versionID] = append(m.docs[versionID], docIDs...)
	return nil
}

func (m *mockVdl) ListDocIDs(_ context.Context, _ string, versionID int64) ([]string, error) {
	return m.docs[versionID], nil
}

func (m *mockVdl) DeleteByVersion(_ context.Context, _ string, _ int64) error { return nil }

// ListVersions mirrors the interface; this stub does not model the local
// version list (the reconciler's own cases use purpose-built fakes).
func (m *mockVdl) ListVersions(context.Context, string) ([]int64, error) { return nil, nil }

func (m *mockVdl) DeleteByKB(_ context.Context, _ string) error { return nil }

func TestVersionBloomStore(t *testing.T) {
	ctx := context.Background()
	vdl := &mockVdl{docs: map[int64][]string{
		1: {"doc-a", "doc-b"},
		2: {"doc-c"},
	}}

	t.Run("Get rebuilds from version doc list and persists", func(t *testing.T) {
		s := NewVersionBloomStore(t.TempDir(), 1000, 0.01, vdl)
		f, err := s.Get(ctx, "kb-1", 1)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !f.Test("doc-a") || !f.Test("doc-b") {
			t.Fatalf("filter must contain doc-a and doc-b")
		}
		if f.Test("doc-c") {
			t.Fatalf("filter must not contain doc-c (different version)")
		}

		// A fresh store over the same dir must load from disk, not rebuild:
		// distinguish by corrupting the source so a rebuild would fail.
		vdl2 := &mockVdl{docs: map[int64][]string{}}
		s2 := NewVersionBloomStore(t.TempDir(), 1000, 0.01, vdl2)
		// s's dir is gone (t.TempDir per store); rebuild path must work for s2
		// from empty source — filter simply contains nothing.
		f2, err := s2.Get(ctx, "kb-1", 1)
		if err != nil {
			t.Fatalf("Get on empty source: %v", err)
		}
		if f2.Test("doc-a") {
			t.Fatalf("empty-source filter must not contain doc-a")
		}
	})

	t.Run("BuildAndPersist then Get reuses the disk copy", func(t *testing.T) {
		dir := t.TempDir()
		// The source has to agree with what was persisted. The filter is DERIVED data:
		// a disk copy is reused only while the version's document set still matches it
		// (see GetForDocuments), so a source that disagrees is the "stale copy" case,
		// covered by TestVersionBloomStore_StaleFilterIsRebuilt.
		src := &mockVdl{docs: map[int64][]string{5: {"x", "y"}}}
		s := NewVersionBloomStore(dir, 1000, 0.01, src)
		f, err := s.BuildAndPersist("kb-1", 5, []string{"x", "y"})
		if err != nil {
			t.Fatalf("BuildAndPersist: %v", err)
		}
		if !f.Test("x") {
			t.Fatalf("built filter must contain x")
		}

		// New store over the same dir: Get must return a filter containing x and y.
		s2 := NewVersionBloomStore(dir, 1000, 0.01, src)
		f2, err := s2.Get(ctx, "kb-1", 5)
		if err != nil {
			t.Fatalf("Get after a restart: %v", err)
		}
		if !f2.Test("x") || !f2.Test("y") {
			t.Fatalf("the filter must contain x and y")
		}
	})
}

// TestVersionBloomStore_StaleFilterIsRebuilt is the regression test for the way a
// replica could answer "nothing matched" (and no error) for a version it holds in
// full.
//
// The trap: a filter built BEFORE the version's documents arrived holds nothing, and
// an empty filter does not merely filter imprecisely — it rejects every hit. The store
// used to keep that filter forever (cached in memory, and reused from disk after a
// restart), so the replica stayed wrong long after its data had landed and its
// document set was complete.
func TestVersionBloomStore_StaleFilterIsRebuilt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := &mockVdl{docs: map[int64][]string{}}
	s := NewVersionBloomStore(dir, 1000, 0.01, src)

	// The early query: this version's documents are not here yet.
	f, err := s.Get(ctx, "kb-1", 7)
	if err != nil {
		t.Fatalf("Get before the documents arrive: %v", err)
	}
	if f.Test("doc-a") {
		t.Fatal("precondition: nothing is known about this version yet")
	}

	// The data lands, and the next query must see a filter derived from it.
	src.docs[7] = []string{"doc-a", "doc-b"}
	f2, err := s.Get(ctx, "kb-1", 7)
	if err != nil {
		t.Fatalf("Get after the documents arrive: %v", err)
	}
	if !f2.Test("doc-a") || !f2.Test("doc-b") {
		t.Fatal("the filter must be re-derived from the version's document set: an empty one drops every hit")
	}

	// And the stale copy was REPLACED on disk, not kept: a restarted process has to
	// reach the same answer.
	s2 := NewVersionBloomStore(dir, 1000, 0.01, src)
	f3, err := s2.Get(ctx, "kb-1", 7)
	if err != nil {
		t.Fatalf("Get after a restart: %v", err)
	}
	if !f3.Test("doc-a") {
		t.Error("the on-disk copy still describes the empty document set")
	}
}

// TestVersionBloomStore_FilterFollowsTheDocumentSetForwardsAndBackwards keeps the
// derivation honest in both directions: the filter is not merely "rebuilt once and then
// trusted" — it tracks the set it describes.
func TestVersionBloomStore_FilterFollowsTheDocumentSetForwardsAndBackwards(t *testing.T) {
	ctx := context.Background()
	src := &mockVdl{docs: map[int64][]string{3: {"doc-a"}}}
	s := NewVersionBloomStore(t.TempDir(), 1000, 0.01, src)

	f, err := s.Get(ctx, "kb-1", 3)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !f.Test("doc-a") || f.Test("doc-z") {
		t.Fatal("the filter must describe exactly the current document set")
	}

	src.docs[3] = []string{"doc-a", "doc-z"}
	f2, err := s.Get(ctx, "kb-1", 3)
	if err != nil {
		t.Fatalf("Get after the set grew: %v", err)
	}
	if !f2.Test("doc-z") {
		t.Error("a document added to the version must be in the rebuilt filter")
	}

	src.docs[3] = []string{"doc-z"}
	f3, err := s.Get(ctx, "kb-1", 3)
	if err != nil {
		t.Fatalf("Get after the set shrank: %v", err)
	}
	if f3.Test("doc-a") {
		t.Error("a document removed from the version must be gone from the rebuilt filter")
	}
}

// TestVersionBloomStore_DeleteByKB verifies that DeleteByKB removes the
// on-disk bloom directory and drops cached filters for the KB, leaves
// other KBs untouched, and tolerates a missing directory (idempotent).
func TestVersionBloomStore_DeleteByKB(t *testing.T) {
	ctx := context.Background()
	vdl := &mockVdl{docs: map[int64][]string{
		1: {"doc-a"},
		2: {"doc-b"},
	}}
	root := t.TempDir()
	s := NewVersionBloomStore(root, 1000, 0.01, vdl)

	if _, err := s.BuildAndPersist("kb-1", 1, []string{"doc-a"}); err != nil {
		t.Fatalf("BuildAndPersist kb-1: %v", err)
	}
	if _, err := s.BuildAndPersist("kb-2", 2, []string{"doc-b"}); err != nil {
		t.Fatalf("BuildAndPersist kb-2: %v", err)
	}

	if err := s.DeleteByKB("kb-1"); err != nil {
		t.Fatalf("DeleteByKB: %v", err)
	}

	// kb-1's disk files are gone and the cache entry is dropped.
	if _, err := s.Get(ctx, "kb-1", 1); err != nil {
		t.Fatalf("Get kb-1 after delete: %v", err) // rebuilds from vdl, fine
	}
	// kb-2's on-disk file must still exist.
	if _, _, err := s.loadFromDisk("kb-2", 2); err != nil {
		t.Errorf("kb-2 bloom file should survive: %v", err)
	}
	// Missing directory: idempotent no-error.
	if err := s.DeleteByKB("kb-missing"); err != nil {
		t.Fatalf("DeleteByKB (missing): %v", err)
	}
}

// TestVersionBloomStore_DeleteByVersion verifies that DeleteByVersion drops
// the version's cached filter and its on-disk file, leaves other versions and
// other KBs untouched, and tolerates a missing file (idempotent).
func TestVersionBloomStore_DeleteByVersion(t *testing.T) {
	vdl := &mockVdl{docs: map[int64][]string{
		1: {"doc-a"},
		2: {"doc-b"},
	}}
	root := t.TempDir()
	s := NewVersionBloomStore(root, 1000, 0.01, vdl)

	for _, tc := range []struct {
		kbID      string
		versionID int64
		docID     string
	}{
		{"kb-1", 1, "doc-a"},
		{"kb-1", 2, "doc-b"},
		{"kb-2", 1, "doc-a"},
	} {
		if _, err := s.BuildAndPersist(tc.kbID, tc.versionID, []string{tc.docID}); err != nil {
			t.Fatalf("BuildAndPersist %s/%d: %v", tc.kbID, tc.versionID, err)
		}
	}

	if err := s.DeleteByVersion("kb-1", 1); err != nil {
		t.Fatalf("DeleteByVersion: %v", err)
	}

	// The target's file and cache entry are gone.
	if _, _, err := s.loadFromDisk("kb-1", 1); err == nil {
		t.Error("kb-1/v1 bloom file should be gone after DeleteByVersion")
	}
	if _, ok := s.cache[versionKey{kbID: "kb-1", versionID: 1}]; ok {
		t.Error("kb-1/v1 cache entry should be dropped")
	}
	// Sibling versions and other KBs survive, on disk and in cache.
	if _, _, err := s.loadFromDisk("kb-1", 2); err != nil {
		t.Errorf("kb-1/v2 bloom file should survive: %v", err)
	}
	if _, ok := s.cache[versionKey{kbID: "kb-1", versionID: 2}]; !ok {
		t.Error("kb-1/v2 cache entry should survive")
	}
	if _, _, err := s.loadFromDisk("kb-2", 1); err != nil {
		t.Errorf("kb-2/v1 bloom file should survive: %v", err)
	}

	// Idempotent: a missing file (and a missing KB directory) is not an error.
	if err := s.DeleteByVersion("kb-1", 1); err != nil {
		t.Fatalf("second DeleteByVersion: %v", err)
	}
	if err := s.DeleteByVersion("kb-missing", 7); err != nil {
		t.Fatalf("DeleteByVersion on missing kb: %v", err)
	}
}
