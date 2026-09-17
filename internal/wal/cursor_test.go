package wal

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestFileWAL_CursorRoundTrip pins the record's whole reason for existing: a
// cursor written before a restart is read back after one, with no inference in
// between (docs/cursor-persistence-plan.md §3).
func TestFileWAL_CursorRoundTrip(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpenFileWAL(t, path)
	ctx := context.Background()

	for _, kbID := range []string{"kb-1", "kb-2"} {
		if err := w.WriteCursor(ctx, kbID, 41); err != nil {
			t.Fatalf("WriteCursor(%s): %v", kbID, err)
		}
	}
	if err := w.WriteCursor(ctx, "kb-1", 42); err != nil {
		t.Fatalf("WriteCursor(kb-1, 42): %v", err)
	}
	mustCloseFileWAL(t, w)

	reopened := mustOpenFileWAL(t, path)
	defer mustCloseFileWAL(t, reopened)

	got, err := reopened.RecoverCursors(ctx)
	if err != nil {
		t.Fatalf("RecoverCursors: %v", err)
	}
	want := map[string]int64{"kb-1": 42, "kb-2": 41}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RecoverCursors() = %v, want %v", got, want)
	}
}

// A cursor is a scalar and only moves forward, so a lower value says strictly
// less than the one already recorded: it is dropped instead of written. That is
// also what keeps the record count at one per knowledge base, and what makes a
// burst of advances cost one record rather than N.
func TestFileWAL_CursorIsMonotone(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpenFileWAL(t, path)
	ctx := context.Background()

	if err := w.WriteCursor(ctx, "kb-1", 9); err != nil {
		t.Fatalf("WriteCursor(9): %v", err)
	}
	if err := w.WriteCursor(ctx, "kb-1", 4); err != nil {
		t.Fatalf("WriteCursor(4): %v", err)
	}
	mustCloseFileWAL(t, w)

	if n := countCursorRecords(t, path); n != 1 {
		t.Fatalf("CURSOR records on disk = %d, want 1 — a value below the recorded one must not be written", n)
	}

	reopened := mustOpenFileWAL(t, path)
	defer mustCloseFileWAL(t, reopened)
	got, err := reopened.RecoverCursors(ctx)
	if err != nil {
		t.Fatalf("RecoverCursors: %v", err)
	}
	if got["kb-1"] != 9 {
		t.Fatalf("cursor = %d, want 9 (the higher value is the one that stands)", got["kb-1"])
	}
}

// TestFileWAL_CompactKeepsTheNewestCursorAndIgnoresTheReclaimWatermark covers
// both halves of docs/cursor-persistence-plan.md §3.4 in one log: the newest
// cursor of a knowledge base survives compaction even when EVERY change of that
// knowledge base is reclaimed, and the older cursor records do not.
func TestFileWAL_CompactKeepsTheNewestCursorAndIgnoresTheReclaimWatermark(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	ctx := context.Background()

	writeCommittedVersion(t, w, "kb-1", 0, 1)
	writeCommittedVersion(t, w, "kb-1", 1, 2)
	for _, v := range []int64{1, 2, 3} {
		if err := w.WriteCursor(ctx, "kb-1", v); err != nil {
			t.Fatalf("WriteCursor(v%d): %v", v, err)
		}
	}
	// The watermark covers every version of the knowledge base, so every change is
	// reclaimable. A cursor is NOT a change and must not be swept along with them.
	if err := w.Compact(ctx, map[string]int64{"kb-1": 2}); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if n := countCursorRecords(t, path); n != 1 {
		t.Fatalf("CURSOR records after compaction = %d, want 1 (the newest one)", n)
	}

	reopened, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.RecoverCursors(ctx)
	if err != nil {
		t.Fatalf("RecoverCursors: %v", err)
	}
	if got["kb-1"] != 3 {
		t.Fatalf("cursor after compaction = %d, want 3", got["kb-1"])
	}
	// Not a vacuous pass: the changes really were reclaimed, so the cursor survived
	// a compaction that did drop records for this knowledge base.
	if _, ok, err := reopened.ChangesFor(ctx, "kb-1", 1); err != nil {
		t.Fatalf("ChangesFor: %v", err)
	} else if ok {
		t.Error("v1's changes are still present: the compaction test would then prove nothing about cursors")
	}
}

// An older log has no CURSOR records at all. Recovery must answer "nothing
// recorded" rather than fail — and must not invent a 0 for the knowledge bases it
// finds in the other records, because "no record" and "cursor 0" mean different
// things to the caller (docs/cursor-persistence-plan.md §3.5).
func TestFileWAL_RecoverCursorsOnAnOlderLogIsEmpty(t *testing.T) {
	path := tempWALPath(t)
	w := mustOpenFileWAL(t, path)
	ctx := context.Background()

	if err := w.WriteBegin(ctx, "kb-1", 0, changesFixture("doc-1")); err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	if err := w.WriteVersionID(ctx, 3); err != nil {
		t.Fatalf("WriteVersionID: %v", err)
	}
	if err := w.WriteCommit(ctx, 3); err != nil {
		t.Fatalf("WriteCommit: %v", err)
	}
	mustCloseFileWAL(t, w)

	reopened := mustOpenFileWAL(t, path)
	defer mustCloseFileWAL(t, reopened)

	got, err := reopened.RecoverCursors(ctx)
	if err != nil {
		t.Fatalf("RecoverCursors: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("RecoverCursors() = %v, want empty: a log written before the record type existed has no cursor", got)
	}
	if len(reopened.KnowledgeBases()) == 0 {
		t.Error("precondition: the log does hold records for kb-1, so the empty answer is about cursors and not about the log being unreadable")
	}
}

// countCursorRecords counts the CURSOR records physically present in the log at
// path. It reads the file rather than the in-memory index on purpose: what a
// restart sees is the point of these tests.
func countCursorRecords(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	var n int
	reader := bufio.NewReader(f)
	for {
		rec, _, err := readRawRecord(reader)
		if err != nil {
			break
		}
		if rec.kind == recordTypeCursor {
			n++
		}
	}
	return n
}
