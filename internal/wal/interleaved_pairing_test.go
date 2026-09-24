package wal

import (
	"context"
	"path/filepath"
	"testing"

	"stratum/internal/types"
)

// M7 of docs/code-review-2026-09-24.md: once writes to different knowledge bases
// run concurrently (they are serialized per KB, not globally), their
// BEGIN/VERSION_ID pairs interleave in the log. Pairing by "the most recent unpaired
// BEGIN" then binds KB A's version to KB B's replay input — so a lagging peer asking
// for A's changes would be handed B's.
func TestFileWAL_PairsInterleavedKnowledgeBases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "interleaved.wal")
	w, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	ctx := context.Background()

	// The interleaving a concurrent pair of writes produces: both BEGINs land
	// before either VERSION_ID (the ids come from the Raft apply loop, which is
	// serial and runs after both proposals are in flight).
	if err := w.WriteBegin(ctx, "kb-a", 0, changesFixture("doc-a")); err != nil {
		t.Fatalf("WriteBegin(kb-a): %v", err)
	}
	if err := w.WriteBegin(ctx, "kb-b", 0, changesFixture("doc-b")); err != nil {
		t.Fatalf("WriteBegin(kb-b): %v", err)
	}
	if err := w.WriteVersionID(ctx, "kb-a", 1); err != nil {
		t.Fatalf("WriteVersionID(kb-a): %v", err)
	}
	if err := w.WriteVersionID(ctx, "kb-b", 2); err != nil {
		t.Fatalf("WriteVersionID(kb-b): %v", err)
	}
	for _, v := range []int64{1, 2} {
		if err := w.WriteCommit(ctx, v); err != nil {
			t.Fatalf("WriteCommit(v%d): %v", v, err)
		}
	}

	// Read back from the FILE: the runtime binding is a convenience, the pairing
	// that matters is the one a restart performs.
	reopened, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	assertReplayInput(t, reopened, "kb-a", 1, "doc-a")
	assertReplayInput(t, reopened, "kb-b", 2, "doc-b")
}

// The same interleaving has to survive compaction, which rewrites the log and
// decides which pairs to keep or drop — with a pairing rule of its own.
func TestFileWAL_CompactKeepsInterleavedPairings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "interleaved-compact.wal")
	w, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	ctx := context.Background()

	write := func(kb string, version int64, doc string) {
		t.Helper()
		if err := w.WriteBegin(ctx, kb, 0, changesFixture(doc)); err != nil {
			t.Fatalf("WriteBegin(%s): %v", kb, err)
		}
		if err := w.WriteVersionID(ctx, kb, version); err != nil {
			t.Fatalf("WriteVersionID(%s): %v", kb, err)
		}
		if err := w.WriteCommit(ctx, version); err != nil {
			t.Fatalf("WriteCommit(%s): %v", kb, err)
		}
	}
	// Interleaved BEGINs, then the ids in the opposite order — nothing about the
	// pairing may depend on which order they happen to be in.
	if err := w.WriteBegin(ctx, "kb-a", 0, changesFixture("doc-a")); err != nil {
		t.Fatalf("WriteBegin(kb-a): %v", err)
	}
	if err := w.WriteBegin(ctx, "kb-b", 0, changesFixture("doc-b")); err != nil {
		t.Fatalf("WriteBegin(kb-b): %v", err)
	}
	if err := w.WriteVersionID(ctx, "kb-b", 2); err != nil {
		t.Fatalf("WriteVersionID(kb-b): %v", err)
	}
	if err := w.WriteVersionID(ctx, "kb-a", 1); err != nil {
		t.Fatalf("WriteVersionID(kb-a): %v", err)
	}
	for _, v := range []int64{1, 2} {
		if err := w.WriteCommit(ctx, v); err != nil {
			t.Fatalf("WriteCommit(v%d): %v", v, err)
		}
	}
	write("kb-a", 3, "doc-a3")

	// Compact with a watermark that reclaims nothing: every pairing must survive
	// the rewrite, which is where a wrong pairing would silently drop a live
	// transaction's replay input.
	if err := w.Compact(ctx, map[string]int64{"kb-a": 0, "kb-b": 0}); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	assertReplayInput(t, reopened, "kb-a", 1, "doc-a")
	assertReplayInput(t, reopened, "kb-b", 2, "doc-b")
	assertReplayInput(t, reopened, "kb-a", 3, "doc-a3")
}

func assertReplayInput(t *testing.T, w *FileWAL, kbID string, versionID int64, wantDocID string) {
	t.Helper()
	changes, ok, err := w.ChangesFor(context.Background(), kbID, versionID)
	if err != nil {
		t.Fatalf("ChangesFor(%s, %d): %v", kbID, versionID, err)
	}
	if !ok {
		t.Fatalf("ChangesFor(%s, %d): the replay input is MISSING (wrong pairing drops a live transaction)", kbID, versionID)
	}
	if len(changes) != 1 || changes[0].DocID != wantDocID {
		t.Fatalf("ChangesFor(%s, %d) = %+v, want the %s transaction's changes",
			kbID, versionID, changes, wantDocID)
	}
}

var _ = types.ChangeOpAdd
