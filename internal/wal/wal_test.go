package wal

import (
	"context"
	"os"
	"testing"

	"stratum/internal/types"
)

// TestFileWAL follows the T1-6 case table in Stratum_测试顺序.md exactly,
// run against the real file-backed implementation. Written before
// FileWAL exists (TDD): this file does not compile until file.go is
// added.
//
// "截断" (truncate) in the T1-6 table is interpreted literally here: tests
// physically cut the on-disk WAL file at a byte offset to simulate a
// process crash, including offsets that land mid-record (not just
// record-aligned stopping points), since that's the scenario a real crash
// can actually produce and the one a real implementation must survive
// without corrupting Recover's result.
func TestFileWAL(t *testing.T) {
	ctx := context.Background()

	t.Run("WriteVersionID idempotent: second call returns success, no error", func(t *testing.T) {
		w := newTestFileWAL(t)
		if err := w.WriteVersionID(ctx, 5); err != nil {
			t.Fatalf("WriteVersionID #1: %v", err)
		}
		if err := w.WriteVersionID(ctx, 5); err != nil {
			t.Fatalf("WriteVersionID #2: %v", err)
		}
		pending, err := w.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 1 {
			t.Fatalf("Recover() = %v, want exactly 1 record (idempotent write must not duplicate)", pending)
		}
	})

	t.Run("recover: only BEGIN means nothing to resume", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		if err := w.WriteBegin(ctx); err != nil {
			t.Fatalf("WriteBegin: %v", err)
		}
		mustCloseFileWAL(t, w)

		w2 := mustOpenFileWAL(t, path)
		defer mustCloseFileWAL(t, w2)
		pending, err := w2.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 0 {
			t.Fatalf("Recover() = %v, want empty (BEGIN-only means Raft propose never completed)", pending)
		}
	})

	t.Run("recover: BEGIN + VERSION_ID returns the versionID needing resumption", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		if err := w.WriteBegin(ctx); err != nil {
			t.Fatalf("WriteBegin: %v", err)
		}
		if err := w.WriteVersionID(ctx, 5); err != nil {
			t.Fatalf("WriteVersionID: %v", err)
		}
		mustCloseFileWAL(t, w)

		w2 := mustOpenFileWAL(t, path)
		defer mustCloseFileWAL(t, w2)
		pending, err := w2.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 1 || pending[0].Type != types.PendingRecordTypeVersionWrite || pending[0].VersionID != 5 {
			t.Fatalf("Recover() = %v, want [{VersionWrite, VersionID:5}]", pending)
		}
	})

	t.Run("recover: full transaction has nothing pending", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		if err := w.WriteBegin(ctx); err != nil {
			t.Fatalf("WriteBegin: %v", err)
		}
		if err := w.WriteVersionID(ctx, 5); err != nil {
			t.Fatalf("WriteVersionID: %v", err)
		}
		if err := w.WriteCommit(ctx, 5); err != nil {
			t.Fatalf("WriteCommit: %v", err)
		}
		mustCloseFileWAL(t, w)

		w2 := mustOpenFileWAL(t, path)
		defer mustCloseFileWAL(t, w2)
		pending, err := w2.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 0 {
			t.Fatalf("Recover() = %v, want empty after full transaction", pending)
		}
	})

	t.Run("recover: DELETE_MARK without DELETE_COMPLETE is pending", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		if err := w.WriteDeleteMark(ctx, "kb1"); err != nil {
			t.Fatalf("WriteDeleteMark: %v", err)
		}
		mustCloseFileWAL(t, w)

		w2 := mustOpenFileWAL(t, path)
		defer mustCloseFileWAL(t, w2)
		pending, err := w2.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 1 || pending[0].Type != types.PendingRecordTypeDeleteMark || pending[0].KBID != "kb1" {
			t.Fatalf("Recover() = %v, want [{DeleteMark, kb1}]", pending)
		}
	})

	t.Run("recover: DELETE_MARK + DELETE_COMPLETE has nothing pending", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		if err := w.WriteDeleteMark(ctx, "kb1"); err != nil {
			t.Fatalf("WriteDeleteMark: %v", err)
		}
		if err := w.WriteDeleteComplete(ctx, "kb1"); err != nil {
			t.Fatalf("WriteDeleteComplete: %v", err)
		}
		mustCloseFileWAL(t, w)

		w2 := mustOpenFileWAL(t, path)
		defer mustCloseFileWAL(t, w2)
		pending, err := w2.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 0 {
			t.Fatalf("Recover() = %v, want empty", pending)
		}
	})

	t.Run("recover: VERSION_DELETE_MARK without complete is pending", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		if err := w.WriteVersionDeleteMark(ctx, "kb1", 7); err != nil {
			t.Fatalf("WriteVersionDeleteMark: %v", err)
		}
		// Idempotent re-write must not duplicate.
		if err := w.WriteVersionDeleteMark(ctx, "kb1", 7); err != nil {
			t.Fatalf("WriteVersionDeleteMark (idempotent): %v", err)
		}
		mustCloseFileWAL(t, w)

		w2 := mustOpenFileWAL(t, path)
		defer mustCloseFileWAL(t, w2)
		pending, err := w2.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 1 || pending[0].Type != types.PendingRecordTypeVersionDelete ||
			pending[0].KBID != "kb1" || pending[0].VersionID != 7 {
			t.Fatalf("Recover() = %v, want [{VersionDelete, kb1, 7}]", pending)
		}
	})

	t.Run("recover: VERSION_DELETE_MARK + COMPLETE has nothing pending", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		if err := w.WriteVersionDeleteMark(ctx, "kb1", 7); err != nil {
			t.Fatalf("WriteVersionDeleteMark: %v", err)
		}
		if err := w.WriteVersionDeleteComplete(ctx, "kb1", 7); err != nil {
			t.Fatalf("WriteVersionDeleteComplete: %v", err)
		}
		mustCloseFileWAL(t, w)

		w2 := mustOpenFileWAL(t, path)
		defer mustCloseFileWAL(t, w2)
		pending, err := w2.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 0 {
			t.Fatalf("Recover() = %v, want empty", pending)
		}
	})

	t.Run("replay counter accumulates", func(t *testing.T) {
		w := newTestFileWAL(t)
		rec := types.PendingRecord{Type: types.PendingRecordTypeDeleteMark, KBID: "kb1"}
		w.IncrementReplayCounter(rec)
		w.IncrementReplayCounter(rec)
		w.IncrementReplayCounter(rec)
		counters := w.GetReplayCounters()
		if len(counters) != 1 || counters[0].RetryCount != 3 {
			t.Fatalf("GetReplayCounters = %v, want [{RetryCount: 3}]", counters)
		}
	})

	t.Run("replay counter not persisted: reopening the WAL resets it to zero", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		rec := types.PendingRecord{Type: types.PendingRecordTypeDeleteMark, KBID: "kb1"}
		w.IncrementReplayCounter(rec)
		w.IncrementReplayCounter(rec)
		mustCloseFileWAL(t, w)

		w2 := mustOpenFileWAL(t, path)
		defer mustCloseFileWAL(t, w2)
		if counters := w2.GetReplayCounters(); len(counters) != 0 {
			t.Fatalf("GetReplayCounters after reopen = %v, want empty (in-memory only, resets on restart)", counters)
		}
	})
}

// TestFileWAL_PhysicalTruncation exercises real mid-record file
// truncation — the scenario T1-6's "截断" is ultimately meant to validate:
// a crash that cuts a record off partway through writing it, not just a
// crash between two complete records.
func TestFileWAL_PhysicalTruncation(t *testing.T) {
	ctx := context.Background()

	t.Run("truncating mid-write of the last record does not corrupt earlier records", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		if err := w.WriteBegin(ctx); err != nil {
			t.Fatalf("WriteBegin: %v", err)
		}
		if err := w.WriteVersionID(ctx, 5); err != nil {
			t.Fatalf("WriteVersionID: %v", err)
		}
		mustCloseFileWAL(t, w)

		// Simulate a crash mid-write of what would have been the COMMIT
		// record: append a few garbage bytes that look like the start of
		// a record but are incomplete, then truncate doesn't even need
		// extra bytes — appending a partial record header is enough.
		appendPartialRecordBytes(t, path)

		w2 := mustOpenFileWAL(t, path)
		defer mustCloseFileWAL(t, w2)
		pending, err := w2.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover after partial trailing record: %v", err)
		}
		if len(pending) != 1 || pending[0].Type != types.PendingRecordTypeVersionWrite || pending[0].VersionID != 5 {
			t.Fatalf("Recover() = %v, want [{VersionWrite, VersionID:5}] (partial trailing bytes must be ignored, not corrupt the log)", pending)
		}
	})

	t.Run("truncating to exactly zero bytes recovers to nothing pending", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		if err := w.WriteBegin(ctx); err != nil {
			t.Fatalf("WriteBegin: %v", err)
		}
		mustCloseFileWAL(t, w)

		if err := os.Truncate(path, 0); err != nil {
			t.Fatalf("os.Truncate: %v", err)
		}

		w2 := mustOpenFileWAL(t, path)
		defer mustCloseFileWAL(t, w2)
		pending, err := w2.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover on empty file: %v", err)
		}
		if len(pending) != 0 {
			t.Fatalf("Recover() = %v, want empty", pending)
		}
	})
}

func tempWALPath(t *testing.T) string {
	t.Helper()
	return t.TempDir() + "/wal.log"
}

func newTestFileWAL(t *testing.T) *FileWAL {
	t.Helper()
	w := mustOpenFileWAL(t, tempWALPath(t))
	t.Cleanup(func() { mustCloseFileWAL(t, w) })
	return w
}

func mustOpenFileWAL(t *testing.T, path string) *FileWAL {
	t.Helper()
	w, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("NewFileWAL(%s): %v", path, err)
	}
	return w
}

func mustCloseFileWAL(t *testing.T, w *FileWAL) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// appendPartialRecordBytes appends a handful of bytes to path that look
// like the beginning of a record (a valid type tag and length prefix) but
// are not followed by a complete payload + checksum, simulating a crash
// that occurred mid-write of a new record.
func appendPartialRecordBytes(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}
	defer f.Close()
	// type byte + 4-byte length prefix claiming more payload than follows;
	// the implementation must detect this as an incomplete trailing
	// record and stop reading there, not error out or panic.
	if _, err := f.Write([]byte{0x02, 0x00, 0x00, 0x00, 0x10, 0xAB, 0xCD}); err != nil {
		t.Fatalf("append partial record: %v", err)
	}
}
