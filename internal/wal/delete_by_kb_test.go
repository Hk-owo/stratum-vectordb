package wal

import (
	"context"
	"path/filepath"
	"testing"

	"stratum/internal/types"
)

// walReader is the read surface both implementations share, so one assertion can
// cover the real log and the in-memory one.
type walReader interface {
	ChangesInRange(ctx context.Context, kbID string, fromExclusive, toInclusive int64) (map[int64]VersionDelta, error)
	ChangesFor(ctx context.Context, kbID string, versionID int64) ([]types.DocChange, bool, error)
	RecoverCursors(ctx context.Context) (map[string]int64, error)
	Recover(ctx context.Context) ([]types.PendingRecord, error)
}

// assertOneKnowledgeBaseDropped checks both halves of the rewrite: the deleted
// knowledge base is gone from every read, and the other one is untouched.
//
// The version ids are deliberately interleaved (kb-drop owns 1 and 3, kb-keep owns 2
// and 4) so that membership has to be decided by what the records say rather than by
// a numeric range.
func assertOneKnowledgeBaseDropped(t *testing.T, w walReader, dropped, kept string, keptVersions []int64) {
	t.Helper()
	ctx := context.Background()

	if deltas, err := w.ChangesInRange(ctx, dropped, 0, 100); err != nil {
		t.Fatalf("ChangesInRange(%s): %v", dropped, err)
	} else if len(deltas) != 0 {
		t.Errorf("dropped knowledge base still has changes: %v", deltas)
	}
	cursors, err := w.RecoverCursors(ctx)
	if err != nil {
		t.Fatalf("RecoverCursors: %v", err)
	}
	if _, ok := cursors[dropped]; ok {
		t.Errorf("cursor for the dropped knowledge base survived: %v", cursors)
	}
	if _, ok := cursors[kept]; !ok {
		t.Errorf("cursor for the kept knowledge base was dropped: %v", cursors)
	}
	recs, err := w.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	for _, rec := range recs {
		if rec.KBID == dropped {
			t.Errorf("a pending record survived for the dropped knowledge base: %+v", rec)
		}
	}

	// The half a too-wide rewrite would break: the other knowledge base's replay
	// input is what a lagging peer asks for.
	for _, v := range keptVersions {
		if changes, ok, err := w.ChangesFor(ctx, kept, v); err != nil {
			t.Fatalf("ChangesFor(%s, %d): %v", kept, v, err)
		} else if !ok || len(changes) == 0 {
			t.Errorf("kept knowledge base lost the replay input for v%d (ok=%v, %d changes)", v, ok, len(changes))
		}
	}
}

// A knowledge-base deletion has to take its WAL records with it — and only its own.
// The log is shared by every knowledge base on the node, so a rewrite that
// over-matches would silently destroy another one's replay input.
func TestFileWAL_DeleteByKBDropsOneKnowledgeBaseOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	ctx := context.Background()

	seed, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	writeCommittedVersion(t, seed, "kb-drop", 0, 1)
	writeCommittedVersion(t, seed, "kb-drop", 1, 3)
	writeCommittedVersion(t, seed, "kb-keep", 0, 2)
	writeCommittedVersion(t, seed, "kb-keep", 2, 4)
	for _, c := range []struct {
		kb string
		v  int64
	}{{"kb-drop", 3}, {"kb-keep", 4}} {
		if err := seed.WriteCursor(ctx, c.kb, c.v); err != nil {
			t.Fatalf("WriteCursor(%s): %v", c.kb, err)
		}
	}
	// An unfinished delete mark would be a pending record on the NEXT open, which is
	// what the assertion below has to find gone.
	if err := seed.WriteDeleteMark(ctx, "kb-drop"); err != nil {
		t.Fatalf("WriteDeleteMark: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Delete through a freshly opened log, the way a restarted node reaches it.
	w, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := w.DeleteByKB(ctx, "kb-drop"); err != nil {
		t.Fatalf("DeleteByKB: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close after rewrite: %v", err)
	}

	// Read everything back from the FILE — the test must not trust the in-memory
	// index the call just trimmed.
	got, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("reopen after rewrite: %v", err)
	}
	defer got.Close()
	assertOneKnowledgeBaseDropped(t, got, "kb-drop", "kb-keep", []int64{2, 4})
}

// The mock keeps the same contract, so module tests can rely on it.
func TestMockWAL_DeleteByKBDropsOneKnowledgeBaseOnly(t *testing.T) {
	ctx := context.Background()
	w := NewMockWAL()

	write := func(kb string, parent, version int64) {
		t.Helper()
		if err := w.WriteBegin(ctx, kb, parent, changesFixture("doc")); err != nil {
			t.Fatalf("WriteBegin(%s): %v", kb, err)
		}
		if err := w.WriteVersionID(ctx, kb, version); err != nil {
			t.Fatalf("WriteVersionID(v%d): %v", version, err)
		}
		if err := w.WriteCommit(ctx, version); err != nil {
			t.Fatalf("WriteCommit(v%d): %v", version, err)
		}
	}
	write("kb-drop", 0, 1)
	write("kb-drop", 1, 3)
	write("kb-keep", 0, 2)
	write("kb-keep", 2, 4)
	if err := w.WriteCursor(ctx, "kb-drop", 3); err != nil {
		t.Fatalf("WriteCursor(kb-drop): %v", err)
	}
	if err := w.WriteCursor(ctx, "kb-keep", 4); err != nil {
		t.Fatalf("WriteCursor(kb-keep): %v", err)
	}
	if err := w.WriteDeleteMark(ctx, "kb-drop"); err != nil {
		t.Fatalf("WriteDeleteMark: %v", err)
	}

	if err := w.DeleteByKB(ctx, "kb-drop"); err != nil {
		t.Fatalf("DeleteByKB: %v", err)
	}
	assertOneKnowledgeBaseDropped(t, w, "kb-drop", "kb-keep", []int64{2, 4})
}
