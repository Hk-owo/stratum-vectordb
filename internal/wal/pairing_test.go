package wal

import (
	"context"
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
	if err := w.WriteVersionID(ctx, 7); err != nil {
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
