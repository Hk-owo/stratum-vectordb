package chunkdoc

import (
	"context"
	"sort"
	"testing"
)

// TestMockChunkDocMapper sanity-checks the in-memory mock's bidirectional
// mapping behavior, mirroring the T1-2 case shapes in
// Stratum_测试顺序.md. This is not a substitute for the real PebbleDB-backed
// implementation's own T1-2 suite.
func TestMockChunkDocMapper(t *testing.T) {
	ctx := context.Background()

	t.Run("forward write then read back", func(t *testing.T) {
		m := NewMockChunkDocMapper()
		mustWrite(t, m, "kb1", "chunk1", "doc1")
		got := mustListDocIDs(t, m, "kb1", "chunk1")
		assertSetEqual(t, got, []string{"doc1"})
	})

	t.Run("one chunk maps to multiple docs", func(t *testing.T) {
		m := NewMockChunkDocMapper()
		mustWrite(t, m, "kb1", "chunk1", "doc1")
		mustWrite(t, m, "kb1", "chunk1", "doc2")
		got := mustListDocIDs(t, m, "kb1", "chunk1")
		assertSetEqual(t, got, []string{"doc1", "doc2"})
	})

	t.Run("reverse batch query", func(t *testing.T) {
		m := NewMockChunkDocMapper()
		mustWrite(t, m, "kb1", "chunk1", "doc1")
		mustWrite(t, m, "kb1", "chunk2", "doc1")
		got, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{"doc1"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		assertSetEqual(t, got, []string{"chunk1", "chunk2"})
	})

	t.Run("multi-doc reverse batch query dedups", func(t *testing.T) {
		m := NewMockChunkDocMapper()
		mustWrite(t, m, "kb1", "chunk1", "doc1")
		mustWrite(t, m, "kb1", "chunk1", "doc2")
		got, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{"doc1", "doc2"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		assertSetEqual(t, got, []string{"chunk1"})
	})

	t.Run("idempotent write does not duplicate", func(t *testing.T) {
		m := NewMockChunkDocMapper()
		mustWrite(t, m, "kb1", "chunk1", "doc1")
		mustWrite(t, m, "kb1", "chunk1", "doc1")
		got := mustListDocIDs(t, m, "kb1", "chunk1")
		assertSetEqual(t, got, []string{"doc1"})
	})

	t.Run("DeleteByDoc clears both directions", func(t *testing.T) {
		m := NewMockChunkDocMapper()
		mustWrite(t, m, "kb1", "chunk1", "doc1")
		mustWrite(t, m, "kb1", "chunk2", "doc1")
		if err := m.DeleteByDoc(ctx, "kb1", "doc1"); err != nil {
			t.Fatalf("DeleteByDoc: %v", err)
		}
		got := mustListDocIDs(t, m, "kb1", "chunk1")
		assertSetEqual(t, got, nil)
		rev, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{"doc1"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		assertSetEqual(t, rev, nil)
	})

	t.Run("DeleteByDoc preserves other docs on shared chunk", func(t *testing.T) {
		m := NewMockChunkDocMapper()
		mustWrite(t, m, "kb1", "chunk1", "doc1")
		mustWrite(t, m, "kb1", "chunk1", "doc2")
		if err := m.DeleteByDoc(ctx, "kb1", "doc1"); err != nil {
			t.Fatalf("DeleteByDoc: %v", err)
		}
		got := mustListDocIDs(t, m, "kb1", "chunk1")
		assertSetEqual(t, got, []string{"doc2"})
	})

	t.Run("DeleteByKB clears everything", func(t *testing.T) {
		m := NewMockChunkDocMapper()
		mustWrite(t, m, "kb1", "chunk1", "doc1")
		mustWrite(t, m, "kb1", "chunk2", "doc2")
		if err := m.DeleteByKB(ctx, "kb1"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}
		got := mustListDocIDs(t, m, "kb1", "chunk1")
		assertSetEqual(t, got, nil)
	})
}

func mustWrite(t *testing.T, m *MockChunkDocMapper, kbID, chunkID, docID string) {
	t.Helper()
	if err := m.Write(context.Background(), kbID, chunkID, docID); err != nil {
		t.Fatalf("Write(%s,%s,%s): %v", kbID, chunkID, docID, err)
	}
}

func mustListDocIDs(t *testing.T, m *MockChunkDocMapper, kbID, chunkID string) []string {
	t.Helper()
	got, err := m.ListDocIDs(context.Background(), kbID, chunkID)
	if err != nil {
		t.Fatalf("ListDocIDs(%s,%s): %v", kbID, chunkID, err)
	}
	return got
}

func assertSetEqual(t *testing.T, got, want []string) {
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
