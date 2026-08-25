package chunkdoc

import (
	"context"
	"sort"
	"testing"
)

// TestPebbleChunkDocMapper follows the T1-2 case table in
// Stratum_测试顺序.md exactly, run against the real PebbleDB-backed
// implementation. Written before PebbleChunkDocMapper exists (TDD): this
// file does not compile until pebble.go is added.
func TestPebbleChunkDocMapper(t *testing.T) {
	ctx := context.Background()

	t.Run("forward write then read back", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk1", "doc1")
		got := mustListDocIDsPebble(t, m, "kb1", "chunk1")
		assertSetEqualPebble(t, got, []string{"doc1"})
	})

	t.Run("one chunk maps to multiple docs", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk1", "doc1")
		mustWritePebble(t, m, "kb1", "chunk1", "doc2")
		got := mustListDocIDsPebble(t, m, "kb1", "chunk1")
		assertSetEqualPebble(t, got, []string{"doc1", "doc2"})
	})

	t.Run("reverse batch query", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk1", "doc1")
		mustWritePebble(t, m, "kb1", "chunk2", "doc1")
		got, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{"doc1"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		assertSetEqualPebble(t, got, []string{"chunk1", "chunk2"})
	})

	t.Run("ListChunkIDs returns every distinct chunk per KB, deduped", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk1", "doc1")
		mustWritePebble(t, m, "kb1", "chunk1", "doc2") // shared chunk: must dedupe
		mustWritePebble(t, m, "kb1", "chunk2", "doc1")
		mustWritePebble(t, m, "kb2", "chunkA", "docX") // other KB: must not leak in
		got, err := m.ListChunkIDs(ctx, "kb1")
		if err != nil {
			t.Fatalf("ListChunkIDs: %v", err)
		}
		assertSetEqualPebble(t, got, []string{"chunk1", "chunk2"})
	})

	t.Run("multi-doc reverse batch query dedups", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk1", "doc1")
		mustWritePebble(t, m, "kb1", "chunk1", "doc2")
		got, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{"doc1", "doc2"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		assertSetEqualPebble(t, got, []string{"chunk1"})
	})

	t.Run("idempotent write does not duplicate", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk1", "doc1")
		mustWritePebble(t, m, "kb1", "chunk1", "doc1")
		got := mustListDocIDsPebble(t, m, "kb1", "chunk1")
		assertSetEqualPebble(t, got, []string{"doc1"})
	})

	t.Run("DeleteByDoc clears both directions", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk1", "doc1")
		mustWritePebble(t, m, "kb1", "chunk2", "doc1")
		if err := m.DeleteByDoc(ctx, "kb1", "doc1"); err != nil {
			t.Fatalf("DeleteByDoc: %v", err)
		}
		got := mustListDocIDsPebble(t, m, "kb1", "chunk1")
		assertSetEqualPebble(t, got, nil)
		got2 := mustListDocIDsPebble(t, m, "kb1", "chunk2")
		assertSetEqualPebble(t, got2, nil)
		rev, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{"doc1"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		assertSetEqualPebble(t, rev, nil)
	})

	t.Run("DeleteByKB clears everything", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk1", "doc1")
		mustWritePebble(t, m, "kb1", "chunk2", "doc2")
		if err := m.DeleteByKB(ctx, "kb1"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}
		got := mustListDocIDsPebble(t, m, "kb1", "chunk1")
		assertSetEqualPebble(t, got, nil)
		got2 := mustListDocIDsPebble(t, m, "kb1", "chunk2")
		assertSetEqualPebble(t, got2, nil)
	})
}

// TestPebbleChunkDocMapper_EdgeCases covers cases beyond the T1-2 table
// that matter specifically for the real key encoding and persistence,
// mirroring the equivalent edge-case suite for PebbleDocStore.
func TestPebbleChunkDocMapper_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("DeleteByDoc preserves other docs on a shared chunk", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk1", "doc1")
		mustWritePebble(t, m, "kb1", "chunk1", "doc2")
		if err := m.DeleteByDoc(ctx, "kb1", "doc1"); err != nil {
			t.Fatalf("DeleteByDoc: %v", err)
		}
		got := mustListDocIDsPebble(t, m, "kb1", "chunk1")
		assertSetEqualPebble(t, got, []string{"doc2"})
	})

	t.Run("DeleteByKB does not affect other knowledge bases", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk1", "doc1")
		mustWritePebble(t, m, "kb2", "chunk1", "doc1")
		if err := m.DeleteByKB(ctx, "kb1"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}
		got := mustListDocIDsPebble(t, m, "kb2", "chunk1")
		assertSetEqualPebble(t, got, []string{"doc1"})
	})

	t.Run("no boundary collision between similar kbID/chunkID/docID splits", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		// kbID="ab"+chunkID="c"+docID="d" must not collide with
		// kbID="a"+chunkID="bc"+docID="d", etc.
		mustWritePebble(t, m, "ab", "c", "d1")
		mustWritePebble(t, m, "a", "bc", "d2")
		got1 := mustListDocIDsPebble(t, m, "ab", "c")
		got2 := mustListDocIDsPebble(t, m, "a", "bc")
		assertSetEqualPebble(t, got1, []string{"d1"})
		assertSetEqualPebble(t, got2, []string{"d2"})
	})

	t.Run("close and reopen preserves both directions (real persistence)", func(t *testing.T) {
		dir := t.TempDir()
		m1, err := NewPebbleChunkDocMapper(dir)
		if err != nil {
			t.Fatalf("NewPebbleChunkDocMapper: %v", err)
		}
		mustWritePebble(t, m1, "kb1", "chunk1", "doc1")
		if err := m1.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		m2, err := NewPebbleChunkDocMapper(dir)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer m2.Close()
		got := mustListDocIDsPebble(t, m2, "kb1", "chunk1")
		assertSetEqualPebble(t, got, []string{"doc1"})
		rev, err := m2.ListChunkIDsByDocs(ctx, "kb1", []string{"doc1"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs after reopen: %v", err)
		}
		assertSetEqualPebble(t, rev, []string{"chunk1"})
	})

	t.Run("ListDocIDs / ListChunkIDsByDocs on nonexistent key returns empty, not error", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		got := mustListDocIDsPebble(t, m, "kb1", "no-such-chunk")
		if len(got) != 0 {
			t.Fatalf("ListDocIDs on nonexistent chunk = %v, want empty", got)
		}
		rev, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{"no-such-doc"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		if len(rev) != 0 {
			t.Fatalf("ListChunkIDsByDocs on nonexistent doc = %v, want empty", rev)
		}
	})
}

func newTestPebbleChunkDocMapper(t *testing.T) *PebbleChunkDocMapper {
	t.Helper()
	m, err := NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("NewPebbleChunkDocMapper: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func mustWritePebble(t *testing.T, m *PebbleChunkDocMapper, kbID, chunkID, docID string) {
	t.Helper()
	if err := m.Write(context.Background(), kbID, chunkID, docID); err != nil {
		t.Fatalf("Write(%s,%s,%s): %v", kbID, chunkID, docID, err)
	}
}

func mustListDocIDsPebble(t *testing.T, m *PebbleChunkDocMapper, kbID, chunkID string) []string {
	t.Helper()
	got, err := m.ListDocIDs(context.Background(), kbID, chunkID)
	if err != nil {
		t.Fatalf("ListDocIDs(%s,%s): %v", kbID, chunkID, err)
	}
	return got
}

func assertSetEqualPebble(t *testing.T, got, want []string) {
	t.Helper()
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	if len(g) != len(w) {
		t.Fatalf("set = %v, want %v", g, w)
	}
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("set = %v, want %v", g, w)
		}
	}
}
