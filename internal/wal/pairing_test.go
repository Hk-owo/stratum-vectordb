package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"stratum/internal/types"
)

// TestFileWAL_PairsBeginAndVersionIDInEitherOrder pins that a WAL whose
// VERSION_ID record precedes its BEGIN record — the order the split write path
// produces once the storage layer owns the write transaction
// (Stratum_设计文档v13.md §7.12: the control layer allocates the version ID
// first, then the storage layer writes BEGIN → data → COMMIT) — still pairs
// the two records on reopen, so an uncommitted version can be replayed exactly
// as with the historical BEGIN-first order.
func TestFileWAL_PairsBeginAndVersionIDInEitherOrder(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reversed.wal")
	changes := []types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "hello"}}

	w, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	// Version ID first (the control layer allocated it), BEGIN second (the
	// storage layer's transaction). No COMMIT: the transaction is
	// interrupted, which is exactly what recovery must resolve.
	if err := w.WriteVersionID(ctx, "kb-1", 7); err != nil {
		t.Fatalf("WriteVersionID: %v", err)
	}
	if err := w.WriteBegin(ctx, "kb-1", 3, changes); err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	records, err := reopened.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	for _, rec := range records {
		if rec.Type != types.PendingRecordTypeVersionWrite || rec.VersionID != 7 {
			continue
		}
		if rec.KBID != "kb-1" || rec.ParentVersionID != 3 {
			t.Fatalf("replayed record = %+v, want kb-1 / parent 3", rec)
		}
		if len(rec.Changes) != 1 || rec.Changes[0].DocID != "doc-1" {
			t.Fatalf("replayed changes = %+v, want the BEGIN payload", rec.Changes)
		}
		return
	}
	t.Fatalf("v7 was not reported as a pending version write; records = %+v", records)
}

// TestFileWAL_RuntimePairsVersionIDFirstWithoutRestart pins the RUNTIME half of
// C5 of docs/code-review-2026-10-18.md.
//
// TestFileWAL_PairsBeginAndVersionIDInEitherOrder above covers the reopen path
// (rebuildIndex), which always paired both orders correctly. The in-memory path
// did not: it kept a single BEGIN-side slot, so under the dispatch topology —
// where apply writes VERSION_ID first and the storage write lands BEGIN later —
// version N+1's VERSION_ID bound to version N's BEGIN. ChangesInRange then handed
// a lagging peer the wrong replay input (a backfill applies it verbatim, with no
// digest check on that path), and nothing detected it.
func TestFileWAL_RuntimePairsVersionIDFirstWithoutRestart(t *testing.T) {
	ctx := context.Background()
	w := mustOpenFileWAL(t, filepath.Join(t.TempDir(), "runtime.wal"))

	// Two consecutive transactions in the dispatch order: the version ID is
	// applied first, the BEGIN arrives with the storage write.
	for id := int64(1); id <= 2; id++ {
		if err := w.WriteVersionID(ctx, "kb-1", id); err != nil {
			t.Fatalf("WriteVersionID(%d): %v", id, err)
		}
		changes := []types.DocChange{{
			Op:      types.ChangeOpAdd,
			DocID:   fmt.Sprintf("doc-%d", id),
			Content: fmt.Sprintf("c%d", id),
		}}
		if err := w.WriteBegin(ctx, "kb-1", id-1, changes); err != nil {
			t.Fatalf("WriteBegin(%d): %v", id, err)
		}
		if err := w.WriteCommit(ctx, id); err != nil {
			t.Fatalf("WriteCommit(%d): %v", id, err)
		}
	}

	got, err := w.ChangesInRange(ctx, "kb-1", 0, 2)
	if err != nil {
		t.Fatalf("ChangesInRange: %v", err)
	}
	for id := int64(1); id <= 2; id++ {
		delta, ok := got[id]
		if !ok {
			t.Fatalf("version %d has no recorded replay input; got %v", id, got)
		}
		if delta.ParentVersionID != id-1 {
			t.Errorf("v%d parent = %d, want %d — it bound to another version's BEGIN",
				id, delta.ParentVersionID, id-1)
		}
		want := fmt.Sprintf("c%d", id)
		if len(delta.Changes) != 1 || delta.Changes[0].Content != want {
			t.Errorf("v%d changes = %+v, want %s", id, delta.Changes, want)
		}
	}
}

// TestFileWAL_FailsClosedWhenAPartialRecordCannotBeRolledBack pins the fail-stop
// half of H6 of docs/code-review-2026-10-18.md: a write that fails part-way is
// rewound, and when the rewind itself fails the log refuses all further writes.
// Appending after a partial record of unknown length would only surface on the
// next restart — as silently discarded history.
func TestFileWAL_FailsClosedWhenAPartialRecordCannotBeRolledBack(t *testing.T) {
	full, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		t.Skipf("/dev/full unavailable: %v", err)
	}
	defer func() { _ = full.Close() }()

	w := mustOpenFileWAL(t, filepath.Join(t.TempDir(), "wal.log"))
	// Swap in a device that fails every write (and cannot be truncated). Not a
	// WAL file, but this test is about the write path's failure handling, not
	// about which file it is pointed at.
	if err := w.file.Close(); err != nil {
		t.Fatalf("close real file: %v", err)
	}
	w.file = full

	if err := w.WriteVersionID(context.Background(), "kb-1", 1); err == nil {
		t.Fatal("a write to a full device must report an error")
	}
	if w.broken == nil {
		t.Fatal("the log must be marked broken when the partial record cannot be rolled back")
	}
	if err := w.WriteCommit(context.Background(), 1); err == nil {
		t.Error("a broken log must refuse further writes instead of appending after a partial record")
	}
}
