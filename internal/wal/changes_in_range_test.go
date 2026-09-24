package wal

import (
	"context"
	"testing"

	"stratum/internal/types"
)

// TestFileWAL_ChangesInRange_ReturnsOnlyWhatItHolds pins §7.5's interval read: a
// lagging peer asks for (from, to] and gets exactly the versions this node
// recorded — with gaps left as MISSING keys, because an incomplete interval has
// to be visible (it falls back to a full state transfer) rather than look like a
// run of versions that changed nothing.
func TestFileWAL_ChangesInRange_ReturnsOnlyWhatItHolds(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/wal.log"
	w := mustOpenFileWAL(t, path)

	// Versions 2 and 3 recorded; version 4 deliberately missing (the gap).
	for _, v := range []int64{2, 3} {
		content := "v" + string(rune('0'+v))
		if err := w.WriteBegin(ctx, "kb-1", v-1, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "d", Content: content}}); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteVersionID(ctx, "kb-1", v); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteCommit(ctx, v); err != nil {
			t.Fatal(err)
		}
	}
	// Another knowledge base's version must never leak into this KB's answer.
	if err := w.WriteBegin(ctx, "kb-2", 1, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "x", Content: "other"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteVersionID(ctx, "kb-2", 9); err != nil {
		t.Fatal(err)
	}

	got, err := w.ChangesInRange(ctx, "kb-1", 1, 4)
	if err != nil {
		t.Fatalf("ChangesInRange: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("range (1,4] = %v, want exactly versions 2 and 3", got)
	}
	if _, ok := got[4]; ok {
		t.Error("version 4 was never written here: the gap must show up as a missing key")
	}
	if _, ok := got[9]; ok {
		t.Error("another knowledge base's version must not appear")
	}
	if len(got[2].Changes) != 1 || got[2].Changes[0].Content != "v2" {
		t.Errorf("v2 delta = %+v, want the recorded changes", got[2])
	}
	if got[2].ParentVersionID != 1 {
		t.Errorf("v2 parent = %d, want 1 — the parent is authoritative in the record, not inferred", got[2].ParentVersionID)
	}
	if len(got[3].Changes) != 1 || got[3].Changes[0].Content != "v3" || got[3].ParentVersionID != 2 {
		t.Errorf("v3 delta = %+v, want the recorded changes with parent 2", got[3])
	}

	// The peer may arrive long after this node restarted: the range has to come
	// back from disk, not from the process that wrote it.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	w2 := mustOpenFileWAL(t, path)
	got2, err := w2.ChangesInRange(ctx, "kb-1", 1, 4)
	if err != nil {
		t.Fatalf("ChangesInRange after restart: %v", err)
	}
	if len(got2) != 2 || got2[2].Changes[0].Content != "v2" || got2[3].Changes[0].Content != "v3" {
		t.Fatalf("after restart range (1,4] = %v, want versions 2 and 3 with their changes", got2)
	}
}

// An empty (or inverted) range is an empty answer, not an error.
func TestFileWAL_ChangesInRange_EmptyRange(t *testing.T) {
	ctx := context.Background()
	w := mustOpenFileWAL(t, t.TempDir()+"/wal.log")

	if err := w.WriteBegin(ctx, "kb-1", 1, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "d"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteVersionID(ctx, "kb-1", 2); err != nil {
		t.Fatal(err)
	}

	got, err := w.ChangesInRange(ctx, "kb-1", 2, 2)
	if err != nil {
		t.Fatalf("ChangesInRange: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("range (2,2] = %v, want empty", got)
	}
}
