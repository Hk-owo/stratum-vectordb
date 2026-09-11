package docstore

import (
	"context"
	"errors"
	"testing"

	stratumerrors "stratum/internal/errors"
)

// TestMockDocStore_MVCC sanity-checks the in-memory mock's MVCC semantics.
// This is not the formal T1-1 suite (that targets the real PebbleDB-backed
// implementation in Phase 1) — it exists so that other modules' tests can
// trust MockDocStore's behavior matches the documented contract.
func TestMockDocStore_MVCC(t *testing.T) {
	ctx := context.Background()

	t.Run("write then read back", func(t *testing.T) {
		s := NewMockDocStore()
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

	t.Run("read old version", func(t *testing.T) {
		s := NewMockDocStore()
		mustWrite(t, s, "kb1", "doc1", 1, "v1")
		mustWrite(t, s, "kb1", "doc1", 2, "v2")
		got, err := s.ReadAt(ctx, "kb1", "doc1", 1)
		if err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
		if string(got) != "v1" {
			t.Fatalf("ReadAt(maxV=1) = %q, want %q", got, "v1")
		}
	})

	t.Run("read latest version", func(t *testing.T) {
		s := NewMockDocStore()
		mustWrite(t, s, "kb1", "doc1", 1, "v1")
		mustWrite(t, s, "kb1", "doc1", 2, "v2")
		got, err := s.ReadAt(ctx, "kb1", "doc1", 5)
		if err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
		if string(got) != "v2" {
			t.Fatalf("ReadAt(maxV=5) = %q, want %q", got, "v2")
		}
	})

	t.Run("tombstone hides document", func(t *testing.T) {
		s := NewMockDocStore()
		mustWrite(t, s, "kb1", "doc1", 1, "content")
		if err := s.Write(ctx, "kb1", "doc1", 2, nil); err != nil {
			t.Fatalf("Write tombstone: %v", err)
		}
		_, err := s.ReadAt(ctx, "kb1", "doc1", 2)
		if !errors.Is(err, stratumerrors.ErrVersionNotFound) {
			t.Fatalf("ReadAt after tombstone err = %v, want ErrVersionNotFound", err)
		}
	})

	t.Run("tombstone does not affect earlier version", func(t *testing.T) {
		s := NewMockDocStore()
		mustWrite(t, s, "kb1", "doc1", 1, "content")
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

	t.Run("DeleteByKB clears all", func(t *testing.T) {
		s := NewMockDocStore()
		mustWrite(t, s, "kb1", "doc1", 1, "a")
		mustWrite(t, s, "kb1", "doc2", 1, "b")
		if err := s.DeleteByKB(ctx, "kb1"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}
		if _, err := s.ReadAt(ctx, "kb1", "doc1", 1); !errors.Is(err, stratumerrors.ErrVersionNotFound) {
			t.Fatalf("ReadAt after DeleteByKB err = %v, want ErrVersionNotFound", err)
		}
	})

	t.Run("idempotent write", func(t *testing.T) {
		s := NewMockDocStore()
		mustWrite(t, s, "kb1", "doc1", 1, "first")
		mustWrite(t, s, "kb1", "doc1", 1, "second")
		got, err := s.ReadAt(ctx, "kb1", "doc1", 1)
		if err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
		if string(got) != "second" {
			t.Fatalf("ReadAt = %q, want %q (last write wins)", got, "second")
		}
	})
}

func mustWrite(t *testing.T, s *MockDocStore, kbID, docID string, versionID int64, content string) {
	t.Helper()
	if err := s.Write(context.Background(), kbID, docID, versionID, []byte(content)); err != nil {
		t.Fatalf("Write(%s,%s,%d): %v", kbID, docID, versionID, err)
	}
}

// TestMockDocStore_DeleteByVersionExceptVisibleFrom keeps the in-memory mock
// in lockstep with the Pebble implementation for the dependency-aware reclaim
// the DeleteVersion cleanup uses (see
// TestPebbleDocStore_DeleteByVersionExceptVisibleFrom).
func TestMockDocStore_DeleteByVersionExceptVisibleFrom(t *testing.T) {
	ctx := context.Background()
	s := NewMockDocStore()

	mustWrite(t, s, "kb1", "doc1", 1, "d1-v1")
	mustWrite(t, s, "kb1", "doc1", 2, "d1-v2")
	mustWrite(t, s, "kb1", "doc1", 3, "d1-v3")
	mustWrite(t, s, "kb1", "doc2", 1, "d2-v1")
	mustWrite(t, s, "kb1", "doc2", 2, "d2-v2")
	// doc3 is deleted at v2 (tombstone); v3 must not resurrect it.
	mustWrite(t, s, "kb1", "doc3", 1, "d3-v1")
	if err := s.Write(ctx, "kb1", "doc3", 2, nil); err != nil {
		t.Fatalf("Write doc3 tombstone: %v", err)
	}

	if err := s.DeleteByVersionExceptVisibleFrom(ctx, "kb1", 2, 3); err != nil {
		t.Fatalf("DeleteByVersionExceptVisibleFrom(kb1, 2, anchor=3): %v", err)
	}
	if got, err := s.ReadAt(ctx, "kb1", "doc2", 3); err != nil || string(got) != "d2-v2" {
		t.Errorf("doc2 at v3 after delete = (%q, %v), want (\"d2-v2\", nil)", got, err)
	}
	if _, err := s.ReadAt(ctx, "kb1", "doc3", 3); err == nil {
		t.Error("doc3 at v3 after delete = nil error, want a not-found error (tombstone kept)")
	}
	if got, err := s.ReadAt(ctx, "kb1", "doc1", 2); err != nil || string(got) != "d1-v1" {
		t.Errorf("doc1 at v2 after delete = (%q, %v), want (\"d1-v1\", nil)", got, err)
	}
	if got, err := s.ReadAt(ctx, "kb1", "doc1", 3); err != nil || string(got) != "d1-v3" {
		t.Errorf("doc1 at v3 after delete = (%q, %v), want (\"d1-v3\", nil)", got, err)
	}

	if err := s.DeleteByVersionExceptVisibleFrom(ctx, "kb1", 2, 0); err != nil {
		t.Fatalf("DeleteByVersionExceptVisibleFrom(kb1, 2, anchor=0): %v", err)
	}
	if got, err := s.ReadAt(ctx, "kb1", "doc2", 3); err != nil || string(got) != "d2-v1" {
		t.Errorf("doc2 at v3 after anchor=0 delete = (%q, %v), want (\"d2-v1\", nil)", got, err)
	}
}
