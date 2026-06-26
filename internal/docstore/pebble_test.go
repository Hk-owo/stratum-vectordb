package docstore

import (
	"context"
	"errors"
	"testing"

	stratumerrors "stratum/internal/errors"
)

// TestPebbleDocStore follows the T1-1 case table in Stratum_测试顺序.md
// exactly, run against the real PebbleDB-backed implementation (as opposed
// to mock_test.go's sanity checks against MockDocStore).
func TestPebbleDocStore(t *testing.T) {
	ctx := context.Background()

	t.Run("write then read back", func(t *testing.T) {
		s := newTestPebbleDocStore(t)
		if err := s.Write(ctx, "kb1", "doc1", 1, []byte("content")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		got, err := s.ReadAt(ctx, "kb1", "doc1", 1)
		if err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
		if string(got) != "content" {
			t.Fatalf("ReadAt = %q, want %q", got, "content")
		}
	})

	t.Run("MVCC: read old version", func(t *testing.T) {
		s := newTestPebbleDocStore(t)
		mustWritePebble(t, s, "kb1", "doc1", 1, "v1")
		mustWritePebble(t, s, "kb1", "doc1", 2, "v2")
		got, err := s.ReadAt(ctx, "kb1", "doc1", 1)
		if err != nil {
			t.Fatalf("ReadAt(maxV=1): %v", err)
		}
		if string(got) != "v1" {
			t.Fatalf("ReadAt(maxV=1) = %q, want %q", got, "v1")
		}
	})

	t.Run("MVCC: read latest version", func(t *testing.T) {
		s := newTestPebbleDocStore(t)
		mustWritePebble(t, s, "kb1", "doc1", 1, "v1")
		mustWritePebble(t, s, "kb1", "doc1", 2, "v2")
		got, err := s.ReadAt(ctx, "kb1", "doc1", 5)
		if err != nil {
			t.Fatalf("ReadAt(maxV=5): %v", err)
		}
		if string(got) != "v2" {
			t.Fatalf("ReadAt(maxV=5) = %q, want %q", got, "v2")
		}
	})

	t.Run("tombstone marker hides document", func(t *testing.T) {
		s := newTestPebbleDocStore(t)
		mustWritePebble(t, s, "kb1", "doc1", 1, "content")
		if err := s.Write(ctx, "kb1", "doc1", 2, nil); err != nil {
			t.Fatalf("Write tombstone: %v", err)
		}
		_, err := s.ReadAt(ctx, "kb1", "doc1", 2)
		if !errors.Is(err, stratumerrors.ErrVersionNotFound) {
			t.Fatalf("ReadAt(maxV=2) err = %v, want ErrVersionNotFound", err)
		}
	})

	t.Run("tombstone does not affect earlier version", func(t *testing.T) {
		s := newTestPebbleDocStore(t)
		mustWritePebble(t, s, "kb1", "doc1", 1, "content")
		if err := s.Write(ctx, "kb1", "doc1", 2, nil); err != nil {
			t.Fatalf("Write tombstone: %v", err)
		}
		got, err := s.ReadAt(ctx, "kb1", "doc1", 1)
		if err != nil {
			t.Fatalf("ReadAt(maxV=1): %v", err)
		}
		if string(got) != "content" {
			t.Fatalf("ReadAt(maxV=1) = %q, want %q", got, "content")
		}
	})

	t.Run("DeleteByKB clears all entries", func(t *testing.T) {
		s := newTestPebbleDocStore(t)
		mustWritePebble(t, s, "kb1", "doc1", 1, "a")
		mustWritePebble(t, s, "kb1", "doc2", 1, "b")
		mustWritePebble(t, s, "kb1", "doc1", 2, "a-v2")
		if err := s.DeleteByKB(ctx, "kb1"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}
		if _, err := s.ReadAt(ctx, "kb1", "doc1", 5); !errors.Is(err, stratumerrors.ErrVersionNotFound) {
			t.Fatalf("ReadAt after DeleteByKB err = %v, want ErrVersionNotFound", err)
		}
		if _, err := s.ReadAt(ctx, "kb1", "doc2", 5); !errors.Is(err, stratumerrors.ErrVersionNotFound) {
			t.Fatalf("ReadAt(doc2) after DeleteByKB err = %v, want ErrVersionNotFound", err)
		}
	})

	t.Run("DeleteByKB does not affect other knowledge bases", func(t *testing.T) {
		s := newTestPebbleDocStore(t)
		mustWritePebble(t, s, "kb1", "doc1", 1, "a")
		mustWritePebble(t, s, "kb2", "doc1", 1, "b")
		if err := s.DeleteByKB(ctx, "kb1"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}
		got, err := s.ReadAt(ctx, "kb2", "doc1", 1)
		if err != nil {
			t.Fatalf("ReadAt(kb2): %v", err)
		}
		if string(got) != "b" {
			t.Fatalf("ReadAt(kb2) = %q, want %q", got, "b")
		}
	})

	t.Run("idempotent write: last write for a version wins", func(t *testing.T) {
		s := newTestPebbleDocStore(t)
		mustWritePebble(t, s, "kb1", "doc1", 1, "first")
		mustWritePebble(t, s, "kb1", "doc1", 1, "second")
		got, err := s.ReadAt(ctx, "kb1", "doc1", 1)
		if err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
		if string(got) != "second" {
			t.Fatalf("ReadAt = %q, want %q", got, "second")
		}
	})
}

// TestPebbleDocStore_KeyBoundaryEdgeCases covers cases beyond the T1-1
// table that matter specifically for the real length-prefixed key
// encoding (see internal/pebbleutil), since a naive concatenation-based
// encoding could let one knowledge base's or document's data bleed into
// another's.
func TestPebbleDocStore_KeyBoundaryEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("no boundary collision between similar kbID/docID splits", func(t *testing.T) {
		s := newTestPebbleDocStore(t)
		// kbID="ab"+docID="c" must not collide with kbID="a"+docID="bc".
		mustWritePebble(t, s, "ab", "c", 1, "first")
		mustWritePebble(t, s, "a", "bc", 1, "second")

		got1, err := s.ReadAt(ctx, "ab", "c", 1)
		if err != nil {
			t.Fatalf("ReadAt(ab,c): %v", err)
		}
		got2, err := s.ReadAt(ctx, "a", "bc", 1)
		if err != nil {
			t.Fatalf("ReadAt(a,bc): %v", err)
		}
		if string(got1) != "first" || string(got2) != "second" {
			t.Fatalf("key boundary collision: got1=%q got2=%q", got1, got2)
		}
	})

	t.Run("empty string content distinct from tombstone", func(t *testing.T) {
		s := newTestPebbleDocStore(t)
		if err := s.Write(ctx, "kb1", "doc1", 1, []byte{}); err != nil {
			t.Fatalf("Write empty content: %v", err)
		}
		got, err := s.ReadAt(ctx, "kb1", "doc1", 1)
		if err != nil {
			t.Fatalf("ReadAt after empty-content write returned error (should be a live empty doc, not a tombstone): %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("ReadAt = %q, want empty content", got)
		}
	})

	t.Run("read at version below any write returns not found", func(t *testing.T) {
		s := newTestPebbleDocStore(t)
		mustWritePebble(t, s, "kb1", "doc1", 5, "content")
		_, err := s.ReadAt(ctx, "kb1", "doc1", 4)
		if !errors.Is(err, stratumerrors.ErrVersionNotFound) {
			t.Fatalf("err = %v, want ErrVersionNotFound", err)
		}
	})

	t.Run("close and reopen preserves data (real persistence)", func(t *testing.T) {
		dir := t.TempDir()
		s1, err := NewPebbleDocStore(dir)
		if err != nil {
			t.Fatalf("NewPebbleDocStore: %v", err)
		}
		mustWritePebble(t, s1, "kb1", "doc1", 1, "persisted")
		if err := s1.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		s2, err := NewPebbleDocStore(dir)
		if err != nil {
			t.Fatalf("reopen NewPebbleDocStore: %v", err)
		}
		defer s2.Close()
		got, err := s2.ReadAt(ctx, "kb1", "doc1", 1)
		if err != nil {
			t.Fatalf("ReadAt after reopen: %v", err)
		}
		if string(got) != "persisted" {
			t.Fatalf("ReadAt after reopen = %q, want %q", got, "persisted")
		}
	})
}

func newTestPebbleDocStore(t *testing.T) *PebbleDocStore {
	t.Helper()
	s, err := NewPebbleDocStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewPebbleDocStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustWritePebble(t *testing.T, s *PebbleDocStore, kbID, docID string, versionID int64, content string) {
	t.Helper()
	if err := s.Write(context.Background(), kbID, docID, versionID, []byte(content)); err != nil {
		t.Fatalf("Write(%s,%s,%d): %v", kbID, docID, versionID, err)
	}
}
