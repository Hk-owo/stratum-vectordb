package chunkstore

import (
	"context"
	"testing"
)

func TestMockChunkStore(t *testing.T) {
	ctx := context.Background()

	t.Run("write then exists", func(t *testing.T) {
		s := NewMockChunkStore()
		if err := s.Write(ctx, "kb1", "c1", []float32{1, 2, 3}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		ok, err := s.Exists(ctx, "kb1", "c1")
		if err != nil {
			t.Fatalf("Exists: %v", err)
		}
		if !ok {
			t.Fatalf("Exists = false, want true after Write")
		}
	})

	t.Run("exists false before write, false after delete", func(t *testing.T) {
		s := NewMockChunkStore()
		ok, _ := s.Exists(ctx, "kb1", "c1")
		if ok {
			t.Fatalf("Exists = true before any Write")
		}
		s.Write(ctx, "kb1", "c1", []float32{1})
		if err := s.Delete(ctx, "kb1", "c1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		ok, _ = s.Exists(ctx, "kb1", "c1")
		if ok {
			t.Fatalf("Exists = true after Delete")
		}
	})

	t.Run("DeleteByKB clears prefix", func(t *testing.T) {
		s := NewMockChunkStore()
		s.Write(ctx, "kb1", "c1", []float32{1})
		s.Write(ctx, "kb1", "c2", []float32{2})
		s.Write(ctx, "kb2", "c3", []float32{3})
		if err := s.DeleteByKB(ctx, "kb1"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}
		ok1, _ := s.Exists(ctx, "kb1", "c1")
		ok2, _ := s.Exists(ctx, "kb1", "c2")
		ok3, _ := s.Exists(ctx, "kb2", "c3")
		if ok1 || ok2 {
			t.Fatalf("kb1 chunks survived DeleteByKB")
		}
		if !ok3 {
			t.Fatalf("kb2 chunk incorrectly removed by DeleteByKB(kb1)")
		}
	})

	t.Run("read round-trips vector content", func(t *testing.T) {
		s := NewMockChunkStore()
		want := []float32{0.1, 0.2, 0.3}
		s.Write(ctx, "kb1", "c1", want)
		got, err := s.Read("kb1", "c1")
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("Read len = %d, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("Read[%d] = %v, want %v", i, got[i], want[i])
			}
		}
	})

	t.Run("write count tracks calls for dedup assertions", func(t *testing.T) {
		s := NewMockChunkStore()
		s.Write(ctx, "kb1", "c1", []float32{1})
		s.Write(ctx, "kb1", "c1", []float32{1}) // same key written twice
		if got := s.WriteCount(); got != 2 {
			t.Fatalf("WriteCount = %d, want 2 (MockChunkStore itself does not dedup; that's WriteCoordinator's job)", got)
		}
	})
}
