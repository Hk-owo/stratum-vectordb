package versiondoc

import (
	"context"
	"sort"
	"testing"
)

// TestPebbleVersionDocList follows the T1-3 case table in
// Stratum_测试顺序.md exactly, run against the real PebbleDB-backed
// implementation. Written before PebbleVersionDocList exists (TDD): this
// file does not compile until pebble.go is added.
func TestPebbleVersionDocList(t *testing.T) {
	ctx := context.Background()

	t.Run("write then read back", func(t *testing.T) {
		v := newTestPebbleVersionDocList(t)
		mustWritePebbleVDL(t, v, "kb1", 1, "doc1")
		got := mustListPebbleVDL(t, v, "kb1", 1)
		assertSetEqualVDL(t, got, []string{"doc1"})
	})

	t.Run("version isolation", func(t *testing.T) {
		v := newTestPebbleVersionDocList(t)
		mustWritePebbleVDL(t, v, "kb1", 1, "doc1")
		mustWritePebbleVDL(t, v, "kb1", 2, "doc2")
		got := mustListPebbleVDL(t, v, "kb1", 1)
		assertSetEqualVDL(t, got, []string{"doc1"})
	})

	t.Run("full document set for a version", func(t *testing.T) {
		v := newTestPebbleVersionDocList(t)
		mustWritePebbleVDL(t, v, "kb1", 2, "doc1")
		mustWritePebbleVDL(t, v, "kb1", 2, "doc2")
		got := mustListPebbleVDL(t, v, "kb1", 2)
		assertSetEqualVDL(t, got, []string{"doc1", "doc2"})
	})

	t.Run("DeleteByVersion", func(t *testing.T) {
		v := newTestPebbleVersionDocList(t)
		mustWritePebbleVDL(t, v, "kb1", 1, "doc1")
		if err := v.DeleteByVersion(ctx, "kb1", 1); err != nil {
			t.Fatalf("DeleteByVersion: %v", err)
		}
		got := mustListPebbleVDL(t, v, "kb1", 1)
		assertSetEqualVDL(t, got, nil)
	})

	t.Run("DeleteByKB clears all versions", func(t *testing.T) {
		v := newTestPebbleVersionDocList(t)
		mustWritePebbleVDL(t, v, "kb1", 1, "doc1")
		mustWritePebbleVDL(t, v, "kb1", 2, "doc2")
		mustWritePebbleVDL(t, v, "kb1", 3, "doc3")
		if err := v.DeleteByKB(ctx, "kb1"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}
		for _, ver := range []int64{1, 2, 3} {
			got := mustListPebbleVDL(t, v, "kb1", ver)
			assertSetEqualVDL(t, got, nil)
		}
	})

	t.Run("idempotent write", func(t *testing.T) {
		v := newTestPebbleVersionDocList(t)
		mustWritePebbleVDL(t, v, "kb1", 1, "doc1")
		mustWritePebbleVDL(t, v, "kb1", 1, "doc1")
		got := mustListPebbleVDL(t, v, "kb1", 1)
		assertSetEqualVDL(t, got, []string{"doc1"})
	})
}

// TestPebbleVersionDocList_EdgeCases covers cases beyond the T1-3 table,
// mirroring the equivalent edge-case suites for PebbleDocStore and
// PebbleChunkDocMapper.
func TestPebbleVersionDocList_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("DeleteByKB does not affect other knowledge bases", func(t *testing.T) {
		v := newTestPebbleVersionDocList(t)
		mustWritePebbleVDL(t, v, "kb1", 1, "doc1")
		mustWritePebbleVDL(t, v, "kb2", 1, "doc2")
		if err := v.DeleteByKB(ctx, "kb1"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}
		got := mustListPebbleVDL(t, v, "kb2", 1)
		assertSetEqualVDL(t, got, []string{"doc2"})
	})

	t.Run("DeleteByVersion does not affect other versions in same KB", func(t *testing.T) {
		v := newTestPebbleVersionDocList(t)
		mustWritePebbleVDL(t, v, "kb1", 1, "doc1")
		mustWritePebbleVDL(t, v, "kb1", 2, "doc2")
		if err := v.DeleteByVersion(ctx, "kb1", 1); err != nil {
			t.Fatalf("DeleteByVersion: %v", err)
		}
		got := mustListPebbleVDL(t, v, "kb1", 2)
		assertSetEqualVDL(t, got, []string{"doc2"})
	})

	t.Run("no boundary collision between similar kbID/docID splits", func(t *testing.T) {
		v := newTestPebbleVersionDocList(t)
		mustWritePebbleVDL(t, v, "ab", 1, "c")
		mustWritePebbleVDL(t, v, "a", 1, "...") // distinct docID, same version number, different kbID split
		got1 := mustListPebbleVDL(t, v, "ab", 1)
		assertSetEqualVDL(t, got1, []string{"c"})
	})

	t.Run("close and reopen preserves data (real persistence)", func(t *testing.T) {
		dir := t.TempDir()
		v1, err := NewPebbleVersionDocList(dir)
		if err != nil {
			t.Fatalf("NewPebbleVersionDocList: %v", err)
		}
		mustWritePebbleVDL(t, v1, "kb1", 1, "doc1")
		if err := v1.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		v2, err := NewPebbleVersionDocList(dir)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer v2.Close()
		got := mustListPebbleVDL(t, v2, "kb1", 1)
		assertSetEqualVDL(t, got, []string{"doc1"})
	})

	t.Run("ListDocIDs on nonexistent version returns empty, not error", func(t *testing.T) {
		v := newTestPebbleVersionDocList(t)
		got := mustListPebbleVDL(t, v, "kb1", 999)
		if len(got) != 0 {
			t.Fatalf("ListDocIDs on nonexistent version = %v, want empty", got)
		}
	})

	t.Run("version IDs spanning the full int64 range do not collide in ordering", func(t *testing.T) {
		v := newTestPebbleVersionDocList(t)
		mustWritePebbleVDL(t, v, "kb1", 1, "doc-low")
		mustWritePebbleVDL(t, v, "kb1", 1<<32, "doc-mid")
		mustWritePebbleVDL(t, v, "kb1", 1<<62, "doc-high")
		assertSetEqualVDL(t, mustListPebbleVDL(t, v, "kb1", 1), []string{"doc-low"})
		assertSetEqualVDL(t, mustListPebbleVDL(t, v, "kb1", 1<<32), []string{"doc-mid"})
		assertSetEqualVDL(t, mustListPebbleVDL(t, v, "kb1", 1<<62), []string{"doc-high"})
	})
}

func newTestPebbleVersionDocList(t *testing.T) *PebbleVersionDocList {
	t.Helper()
	v, err := NewPebbleVersionDocList(t.TempDir())
	if err != nil {
		t.Fatalf("NewPebbleVersionDocList: %v", err)
	}
	t.Cleanup(func() { v.Close() })
	return v
}

func mustWritePebbleVDL(t *testing.T, v *PebbleVersionDocList, kbID string, versionID int64, docID string) {
	t.Helper()
	if err := v.Write(context.Background(), kbID, versionID, docID); err != nil {
		t.Fatalf("Write(%s,%d,%s): %v", kbID, versionID, docID, err)
	}
}

func mustListPebbleVDL(t *testing.T, v *PebbleVersionDocList, kbID string, versionID int64) []string {
	t.Helper()
	got, err := v.ListDocIDs(context.Background(), kbID, versionID)
	if err != nil {
		t.Fatalf("ListDocIDs(%s,%d): %v", kbID, versionID, err)
	}
	return got
}

func assertSetEqualVDL(t *testing.T, got, want []string) {
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
