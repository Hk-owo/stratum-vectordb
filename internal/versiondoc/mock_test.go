package versiondoc

import (
	"context"
	"sort"
	"testing"
)

// TestMockVersionDocList sanity-checks the in-memory mock against the T1-3
// case shapes in Stratum_测试顺序.md. This is not a substitute for the real
// PebbleDB-backed implementation's own T1-3 suite.
func TestMockVersionDocList(t *testing.T) {
	ctx := context.Background()

	t.Run("write then read back", func(t *testing.T) {
		v := NewMockVersionDocList()
		mustWrite(t, v, "kb1", 1, "doc1")
		got := mustList(t, v, "kb1", 1)
		assertSetEqual(t, got, []string{"doc1"})
	})

	t.Run("version isolation", func(t *testing.T) {
		v := NewMockVersionDocList()
		mustWrite(t, v, "kb1", 1, "doc1")
		mustWrite(t, v, "kb1", 2, "doc2")
		got := mustList(t, v, "kb1", 1)
		assertSetEqual(t, got, []string{"doc1"})
	})

	t.Run("full document set for a version", func(t *testing.T) {
		v := NewMockVersionDocList()
		mustWrite(t, v, "kb1", 2, "doc1")
		mustWrite(t, v, "kb1", 2, "doc2")
		got := mustList(t, v, "kb1", 2)
		assertSetEqual(t, got, []string{"doc1", "doc2"})
	})

	t.Run("DeleteByVersion", func(t *testing.T) {
		v := NewMockVersionDocList()
		mustWrite(t, v, "kb1", 1, "doc1")
		if err := v.DeleteByVersion(ctx, "kb1", 1); err != nil {
			t.Fatalf("DeleteByVersion: %v", err)
		}
		got := mustList(t, v, "kb1", 1)
		assertSetEqual(t, got, nil)
	})

	t.Run("DeleteByKB clears all versions", func(t *testing.T) {
		v := NewMockVersionDocList()
		mustWrite(t, v, "kb1", 1, "doc1")
		mustWrite(t, v, "kb1", 2, "doc2")
		mustWrite(t, v, "kb1", 3, "doc3")
		if err := v.DeleteByKB(ctx, "kb1"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}
		for _, ver := range []int64{1, 2, 3} {
			got := mustList(t, v, "kb1", ver)
			assertSetEqual(t, got, nil)
		}
	})

	t.Run("DeleteByKB does not affect other knowledge bases", func(t *testing.T) {
		v := NewMockVersionDocList()
		mustWrite(t, v, "kb1", 1, "doc1")
		mustWrite(t, v, "kb2", 1, "doc2")
		if err := v.DeleteByKB(ctx, "kb1"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}
		got := mustList(t, v, "kb2", 1)
		assertSetEqual(t, got, []string{"doc2"})
	})

	t.Run("idempotent write", func(t *testing.T) {
		v := NewMockVersionDocList()
		mustWrite(t, v, "kb1", 1, "doc1")
		mustWrite(t, v, "kb1", 1, "doc1")
		got := mustList(t, v, "kb1", 1)
		assertSetEqual(t, got, []string{"doc1"})
	})
}

func mustWrite(t *testing.T, v *MockVersionDocList, kbID string, versionID int64, docID string) {
	t.Helper()
	if err := v.Write(context.Background(), kbID, versionID, docID); err != nil {
		t.Fatalf("Write(%s,%d,%s): %v", kbID, versionID, docID, err)
	}
}

func mustList(t *testing.T, v *MockVersionDocList, kbID string, versionID int64) []string {
	t.Helper()
	got, err := v.ListDocIDs(context.Background(), kbID, versionID)
	if err != nil {
		t.Fatalf("ListDocIDs(%s,%d): %v", kbID, versionID, err)
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
