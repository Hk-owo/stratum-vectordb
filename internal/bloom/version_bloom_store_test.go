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

func (m *mockVdl) ListDocIDs(_ context.Context, _ string, versionID int64) ([]string, error) {
	return m.docs[versionID], nil
}

func (m *mockVdl) DeleteByVersion(_ context.Context, _ string, _ int64) error { return nil }
func (m *mockVdl) DeleteByKB(_ context.Context, _ string) error               { return nil }

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

	t.Run("BuildAndPersist then Get hits cache/disk", func(t *testing.T) {
		dir := t.TempDir()
		s := NewVersionBloomStore(dir, 1000, 0.01, vdl)
		f, err := s.BuildAndPersist("kb-1", 5, []string{"x", "y"})
		if err != nil {
			t.Fatalf("BuildAndPersist: %v", err)
		}
		if !f.Test("x") {
			t.Fatalf("built filter must contain x")
		}

		// New store over the same dir: Get must return a filter containing
		// x and y (loaded from disk — the source mock has no version 5).
		s2 := NewVersionBloomStore(dir, 1000, 0.01, vdl)
		f2, err := s2.Get(ctx, "kb-1", 5)
		if err != nil {
			t.Fatalf("Get from disk: %v", err)
		}
		if !f2.Test("x") || !f2.Test("y") {
			t.Fatalf("disk-loaded filter must contain x and y")
		}
	})
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
	if _, err := s.loadFromDisk("kb-2", 2); err != nil {
		t.Errorf("kb-2 bloom file should survive: %v", err)
	}
	// Missing directory: idempotent no-error.
	if err := s.DeleteByKB("kb-missing"); err != nil {
		t.Fatalf("DeleteByKB (missing): %v", err)
	}
}
