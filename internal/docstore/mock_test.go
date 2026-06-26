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
