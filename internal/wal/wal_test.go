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
		if err := w.WriteVersionID(ctx, "kb-1", 5); err != nil {
			t.Fatalf("WriteVersionID #1: %v", err)
		}
		if err := w.WriteVersionID(ctx, "kb-1", 5); err != nil {
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
		if err := w.WriteBegin(ctx, "", 0, nil); err != nil {
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
		if err := w.WriteBegin(ctx, "", 0, nil); err != nil {
			t.Fatalf("WriteBegin: %v", err)
		}
		if err := w.WriteVersionID(ctx, "", 5); err != nil {
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

	t.Run("recover: BEGIN carries replay input (kbID/parentVersionID/changes)", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		changes := []types.DocChange{
			{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "hello world"},
			{Op: types.ChangeOpUpdate, DocID: "doc-2", Content: "更新后的文档内容"},
			{Op: types.ChangeOpDelete, DocID: "doc-3"},
		}
		if err := w.WriteBegin(ctx, "kb-1", 7, changes); err != nil {
			t.Fatalf("WriteBegin: %v", err)
		}
		if err := w.WriteVersionID(ctx, "kb-1", 5); err != nil {
			t.Fatalf("WriteVersionID: %v", err)
		}
		mustCloseFileWAL(t, w)

		// Reopen: rebuildIndex must re-derive the BEGIN→VERSION_ID binding
		// from the on-disk record order.
		w2 := mustOpenFileWAL(t, path)
		defer mustCloseFileWAL(t, w2)
		pending, err := w2.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 1 {
			t.Fatalf("Recover() = %v, want exactly 1 pending record", pending)
		}
		got := pending[0]
		if got.Type != types.PendingRecordTypeVersionWrite || got.VersionID != 5 ||
			got.KBID != "kb-1" || got.ParentVersionID != 7 || len(got.Changes) != 3 {
			t.Fatalf("Recover() = %+v, want {VersionWrite, VersionID:5, KBID:kb-1, ParentVersionID:7, Changes:3 entries}", got)
		}
		if got.Changes[0].Op != types.ChangeOpAdd || got.Changes[0].DocID != "doc-1" || got.Changes[0].Content != "hello world" {
			t.Fatalf("Changes[0] = %+v, want {Add, doc-1, hello world}", got.Changes[0])
		}
		if got.Changes[1].Op != types.ChangeOpUpdate || got.Changes[1].Content != "更新后的文档内容" {
			t.Fatalf("Changes[1] = %+v, want {Update, doc-2, 更新后的文档内容}", got.Changes[1])
		}
		if got.Changes[2].Op != types.ChangeOpDelete || got.Changes[2].DocID != "doc-3" {
			t.Fatalf("Changes[2] = %+v, want {Delete, doc-3}", got.Changes[2])
		}
	})

	t.Run("begin payload round-trips through encode/decode", func(t *testing.T) {
		changes := []types.DocChange{
			{Op: types.ChangeOpAdd, DocID: "a", Content: "内容 A"},
			{Op: types.ChangeOpDelete, DocID: "b"},
		}
		payload := encodeBeginPayload("kb-9", 42, changes)
		got, err := decodeBeginPayload(payload)
		if err != nil {
			t.Fatalf("decodeBeginPayload: %v", err)
		}
		if got.kbID != "kb-9" || got.parentVersionID != 42 || len(got.changes) != 2 {
			t.Fatalf("decoded = %+v, want {kb-9, 42, 2 changes}", got)
		}
		if got.changes[0] != changes[0] || got.changes[1] != changes[1] {
			t.Fatalf("decoded changes = %+v, want %+v", got.changes, changes)
		}
	})

	t.Run("begin payload: legacy empty payload decodes to zero data", func(t *testing.T) {
		got, err := decodeBeginPayload(nil)
		if err != nil {
			t.Fatalf("decodeBeginPayload(nil): %v", err)
		}
		if got.kbID != "" || got.parentVersionID != 0 || got.changes != nil {
			t.Fatalf("decoded = %+v, want all-zero beginData (legacy BEGIN)", got)
		}
	})

	t.Run("recover: full transaction has nothing pending", func(t *testing.T) {
		path := tempWALPath(t)
		w := mustOpenFileWAL(t, path)
		if err := w.WriteBegin(ctx, "", 0, nil); err != nil {
			t.Fatalf("WriteBegin: %v", err)
		}
		if err := w.WriteVersionID(ctx, "", 5); err != nil {
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
		if err := w.WriteBegin(ctx, "", 0, nil); err != nil {
			t.Fatalf("WriteBegin: %v", err)
		}
		if err := w.WriteVersionID(ctx, "", 5); err != nil {
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
		if err := w.WriteBegin(ctx, "", 0, nil); err != nil {
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

// TestFileWAL_RepeatedRecordsAreNotAppendedTwice pins the idempotence every Write*
// promises: a retry (after a timeout, a replay, a re-run of a delete flow) must cost
// nothing and must not grow the log. The log is append-and-replay, so a duplicated
// record is not just wasted space — Recover would report the same pending flow twice.
//
// The counter is the group-commit sequence number (how many records are in the log),
// which is exactly the "did anything get appended" question; the on-disk size would
// answer it too, but only indirectly.
func TestFileWAL_RepeatedRecordsAreNotAppendedTwice(t *testing.T) {
	ctx := context.Background()

	writes := map[string]func(w *FileWAL) error{
		"WriteCommit":            func(w *FileWAL) error { return w.WriteCommit(ctx, 9) },
		"WriteDeleteMark":        func(w *FileWAL) error { return w.WriteDeleteMark(ctx, "kb1") },
		"WriteDeleteComplete":    func(w *FileWAL) error { return w.WriteDeleteComplete(ctx, "kb1") },
		"WriteVersionDeleteMark": func(w *FileWAL) error { return w.WriteVersionDeleteMark(ctx, "kb1", 7) },
		"WriteVersionDeleteComplete": func(w *FileWAL) error {
			return w.WriteVersionDeleteComplete(ctx, "kb1", 7)
		},
		"WriteCursor": func(w *FileWAL) error { return w.WriteCursor(ctx, "kb1", 5) },
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			w := newTestFileWAL(t)

			before := w.appendedSeq.Load()
			if err := write(w); err != nil {
				t.Fatalf("%s #1: %v", name, err)
			}
			afterFirst := w.appendedSeq.Load()
			if afterFirst != before+1 {
				t.Fatalf("%s #1 appended %d record(s), want 1", name, afterFirst-before)
			}

			if err := write(w); err != nil {
				t.Fatalf("%s #2 (idempotent repeat): %v", name, err)
			}
			if got := w.appendedSeq.Load(); got != afterFirst {
				t.Fatalf("%s #2 appended %d record(s), want 0", name, got-afterFirst)
			}

			// The repeat has to leave a log that still replays to the same state.
			if _, err := w.Recover(ctx); err != nil {
				t.Fatalf("Recover: %v", err)
			}
		})
	}

	t.Run("WriteCursor does not move backwards", func(t *testing.T) {
		w := newTestFileWAL(t)
		if err := w.WriteCursor(ctx, "kb1", 5); err != nil {
			t.Fatalf("WriteCursor(5): %v", err)
		}
		before := w.appendedSeq.Load()
		if err := w.WriteCursor(ctx, "kb1", 4); err != nil {
			t.Fatalf("WriteCursor(4): %v", err)
		}
		if got := w.appendedSeq.Load(); got != before {
			t.Fatalf("WriteCursor(4) after WriteCursor(5) appended %d record(s), want 0", got-before)
		}
		cursors, err := w.RecoverCursors(ctx)
		if err != nil {
			t.Fatalf("RecoverCursors: %v", err)
		}
		if got := cursors["kb1"]; got != 5 {
			t.Fatalf("cursor = %d, want 5 (it must not move backwards)", got)
		}
	})
}

// TestFileWAL_FailedAppendFailsClosed pins what a Write* does when the append itself
// fails: report the error and poison the log. The second half is the part that is
// easy to get wrong — a log that keeps accepting writes after a failed append can
// leave a partial record behind, and a partial record silently discards every valid
// record after it on the next start (callers of those records were already told the
// write succeeded).
//
// The failure is injected by pointing the WAL at a read-only descriptor of its own
// file: the append's Seek succeeds and its Write fails, which is the shape a full
// disk takes. The rollback cannot succeed either (a read-only descriptor cannot be
// truncated), so this exercises the fail-stop path rather than the clean one: the
// log must refuse every later write, because the bytes of the failed record may
// still be in the middle of the file.
func TestFileWAL_FailedAppendFailsClosed(t *testing.T) {
	ctx := context.Background()

	writes := map[string]func(w *FileWAL) error{
		"WriteBegin": func(w *FileWAL) error {
			return w.WriteBegin(ctx, "kb1", 0, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "d1", Content: "c"}})
		},
		"WriteCommit":                func(w *FileWAL) error { return w.WriteCommit(ctx, 1) },
		"WriteDeleteMark":            func(w *FileWAL) error { return w.WriteDeleteMark(ctx, "kb1") },
		"WriteDeleteComplete":        func(w *FileWAL) error { return w.WriteDeleteComplete(ctx, "kb1") },
		"WriteVersionDeleteMark":     func(w *FileWAL) error { return w.WriteVersionDeleteMark(ctx, "kb1", 7) },
		"WriteVersionDeleteComplete": func(w *FileWAL) error { return w.WriteVersionDeleteComplete(ctx, "kb1", 7) },
		"WriteCursor":                func(w *FileWAL) error { return w.WriteCursor(ctx, "kb1", 3) },
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			// Not newTestFileWAL: its cleanup would close the descriptors a second time
			// and the test swaps them on purpose.
			path := tempWALPath(t)
			w := mustOpenFileWAL(t, path)
			original := w.file
			readOnly, err := os.OpenFile(path, os.O_RDONLY, 0)
			if err != nil {
				t.Fatalf("open the log read-only: %v", err)
			}
			t.Cleanup(func() {
				readOnly.Close()
				original.Close()
			})
			w.file = readOnly

			if err := write(w); err == nil {
				t.Fatalf("%s on a log whose writes fail returned nil", name)
			}
			if w.broken == nil {
				t.Fatalf("%s left the log usable after a failed append", name)
			}
			// And the log stays closed for business: a later write must not be told
			// success by the in-memory index.
			if err := w.WriteCommit(ctx, 2); err == nil {
				t.Fatal("a poisoned log accepted a later write")
			}
		})
	}
}
