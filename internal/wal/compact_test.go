package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"stratum/internal/types"
)

func changesFixture(docID string) []types.DocChange {
	return []types.DocChange{{Op: types.ChangeOpAdd, DocID: docID, Content: docID + "-content"}}
}

// writeCommittedVersion appends a complete BEGIN/VERSION_ID/COMMIT triple, the
// shape a successful CreateVersion leaves behind.
func writeCommittedVersion(t *testing.T, w *FileWAL, kbID string, parentVersionID, versionID int64) {
	t.Helper()
	ctx := context.Background()
	if err := w.WriteBegin(ctx, kbID, parentVersionID, changesFixture("doc")); err != nil {
		t.Fatalf("WriteBegin(v%d): %v", versionID, err)
	}
	if err := w.WriteVersionID(ctx, versionID); err != nil {
		t.Fatalf("WriteVersionID(v%d): %v", versionID, err)
	}
	if err := w.WriteCommit(ctx, versionID); err != nil {
		t.Fatalf("WriteCommit(v%d): %v", versionID, err)
	}
}

// A committed version at or below the watermark is dropped: after compaction and a
// reopen, its changes are GONE — reported as a missing key, which is what tells a
// lagging peer to fall back to a full-state transfer (§7.5).
func TestFileWAL_CompactDropsReclaimedVersions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	writeCommittedVersion(t, w, "kb-1", 0, 1)
	writeCommittedVersion(t, w, "kb-1", 1, 2)
	writeCommittedVersion(t, w, "kb-1", 2, 3)

	if err := w.Compact(ctx, map[string]int64{"kb-1": 2}); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Reopen: the test must not trust the in-memory index it just trimmed.
	reopened, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	for _, v := range []int64{1, 2} {
		if _, ok, err := reopened.ChangesFor(ctx, "kb-1", v); err != nil {
			t.Fatalf("ChangesFor(v%d): %v", v, err)
		} else if ok {
			t.Errorf("v%d is still present after compaction; a reclaimed version must read as a MISSING key", v)
		}
	}
	if _, ok, err := reopened.ChangesFor(ctx, "kb-1", 3); err != nil {
		t.Fatalf("ChangesFor(v3): %v", err)
	} else if !ok {
		t.Error("v3 is above the watermark and must survive compaction")
	}
}

// An uncommitted flow is never dropped, however far below the watermark it sits:
// its BEGIN record is the only place Recover can find the replay input.
func TestFileWAL_CompactKeepsUncommittedFlows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	writeCommittedVersion(t, w, "kb-1", 0, 1)
	// v2 never commits — the crash-recovery case.
	if err := w.WriteBegin(ctx, "kb-1", 1, changesFixture("doc-v2")); err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	if err := w.WriteVersionID(ctx, 2); err != nil {
		t.Fatalf("WriteVersionID: %v", err)
	}

	// A watermark well above v2, which would reclaim it if "committed" were ignored.
	if err := w.Compact(ctx, map[string]int64{"kb-1": 99}); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	reopened, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	pending, err := reopened.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(pending) == 0 {
		t.Fatal("an uncommitted flow was reclaimed; its replay input is gone and the crash can no longer be repaired")
	}
	if _, ok, err := reopened.ChangesFor(ctx, "kb-1", 2); err != nil {
		t.Fatalf("ChangesFor(v2): %v", err)
	} else if !ok {
		t.Error("the uncommitted version's changes were dropped")
	}
}

// A BEGIN with no VERSION_ID yet is an in-flight transaction: it must survive, or a
// crash right after compaction would have nothing to replay.
func TestFileWAL_CompactKeepsTheTrailingInflightBegin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	writeCommittedVersion(t, w, "kb-1", 0, 1)
	if err := w.WriteBegin(ctx, "kb-1", 1, changesFixture("in-flight")); err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}

	if err := w.Compact(ctx, map[string]int64{"kb-1": 99}); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	reopened, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	// Recover does not report an orphan BEGIN (it tracks uncommitted VERSION_IDs, a
	// different case), so the log itself is the evidence: had the in-flight BEGIN
	// been dropped, the file would be empty — v1 was reclaimed and nothing else was
	// written.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("the in-flight BEGIN was dropped by compaction; a crash would have nothing to replay")
	}
	if _, err := reopened.Recover(context.Background()); err != nil {
		t.Fatalf("Recover after compaction: %v", err)
	}
}

// A knowledge base with no watermark keeps everything: compaction is driven by a
// measured decision, never by a default.
func TestFileWAL_CompactKeepsKnowledgeBasesWithoutAWatermark(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	writeCommittedVersion(t, w, "kb-1", 0, 1)
	// A different version number, because version IDs are globally unique: reusing
	// kb-1's here would build a state the real system cannot produce.
	writeCommittedVersion(t, w, "kb-2", 0, 100)

	if err := w.Compact(ctx, map[string]int64{"kb-1": 1}); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	reopened, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if _, ok, _ := reopened.ChangesFor(ctx, "kb-1", 1); ok {
		t.Error("kb-1 was at its watermark and should have been reclaimed")
	}
	if _, ok, _ := reopened.ChangesFor(ctx, "kb-2", 100); !ok {
		t.Error("kb-2 had no watermark and must keep its records")
	}
}

// After compaction the log must still be an appendable, valid log: the next write
// has to land and be readable, otherwise compaction would corrupt the WAL it just
// rewrote.
func TestFileWAL_CompactLeavesAnAppendableLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	writeCommittedVersion(t, w, "kb-1", 0, 1)
	if err := w.Compact(ctx, map[string]int64{"kb-1": 1}); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	writeCommittedVersion(t, w, "kb-1", 1, 2)

	reopened, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if _, ok, err := reopened.ChangesFor(ctx, "kb-1", 2); err != nil || !ok {
		t.Fatalf("a version written after compaction is unreadable: ok=%v err=%v", ok, err)
	}
	// The temporary file must not be left behind.
	if _, err := os.Stat(path + ".compact"); !os.IsNotExist(err) {
		t.Errorf("the compaction scratch file was left behind: %v", err)
	}
}
