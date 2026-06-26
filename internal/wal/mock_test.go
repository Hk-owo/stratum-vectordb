package wal

import (
	"context"
	"testing"

	"stratum/internal/types"
)

// TestMockWAL follows the T1-6 case table in Stratum_测试顺序.md, adapted
// to the in-memory record log MockWAL exposes (Truncate / PendingVersionIDs)
// in place of real file truncation.
func TestMockWAL(t *testing.T) {
	ctx := context.Background()

	t.Run("WriteVersionID is idempotent", func(t *testing.T) {
		w := NewMockWAL()
		if err := w.WriteVersionID(ctx, 5); err != nil {
			t.Fatalf("WriteVersionID #1: %v", err)
		}
		if err := w.WriteVersionID(ctx, 5); err != nil {
			t.Fatalf("WriteVersionID #2: %v", err)
		}
		pending := w.PendingVersionIDs()
		if len(pending) != 1 || pending[0] != 5 {
			t.Fatalf("PendingVersionIDs = %v, want [5] (no duplicate record from idempotent write)", pending)
		}
	})

	t.Run("recover: only BEGIN means nothing to resume", func(t *testing.T) {
		w := NewMockWAL()
		if err := w.WriteBegin(ctx); err != nil {
			t.Fatalf("WriteBegin: %v", err)
		}
		pending, err := w.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 0 {
			t.Fatalf("Recover() = %v, want empty (BEGIN-only means Raft propose never completed)", pending)
		}
		if vids := w.PendingVersionIDs(); len(vids) != 0 {
			t.Fatalf("PendingVersionIDs = %v, want empty", vids)
		}
	})

	t.Run("recover: BEGIN + VERSION_ID means resume from versionID", func(t *testing.T) {
		w := NewMockWAL()
		mustWriteBegin(t, w)
		mustWriteVersionID(t, w, 5)

		pending, err := w.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 1 || pending[0].Type != types.PendingRecordTypeVersionWrite || pending[0].VersionID != 5 {
			t.Fatalf("Recover() = %v, want [{VersionWrite, VersionID:5}]", pending)
		}

		// PendingVersionIDs is a convenience accessor over the same info.
		ids := w.PendingVersionIDs()
		if len(ids) != 1 || ids[0] != 5 {
			t.Fatalf("PendingVersionIDs = %v, want [5]", ids)
		}
	})

	t.Run("recover: full transaction has nothing pending", func(t *testing.T) {
		w := NewMockWAL()
		mustWriteBegin(t, w)
		mustWriteVersionID(t, w, 5)
		if err := w.WriteCommit(ctx, 5); err != nil {
			t.Fatalf("WriteCommit: %v", err)
		}
		if vids := w.PendingVersionIDs(); len(vids) != 0 {
			t.Fatalf("PendingVersionIDs = %v, want empty after COMMIT", vids)
		}
	})

	t.Run("recover: DELETE_MARK without DELETE_COMPLETE is pending", func(t *testing.T) {
		w := NewMockWAL()
		if err := w.WriteDeleteMark(ctx, "kb1"); err != nil {
			t.Fatalf("WriteDeleteMark: %v", err)
		}
		pending, err := w.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 1 || pending[0].Type != types.PendingRecordTypeDeleteMark || pending[0].KBID != "kb1" {
			t.Fatalf("Recover() = %v, want [{DeleteMark, kb1}]", pending)
		}
	})

	t.Run("recover: DELETE_MARK + DELETE_COMPLETE has nothing pending", func(t *testing.T) {
		w := NewMockWAL()
		if err := w.WriteDeleteMark(ctx, "kb1"); err != nil {
			t.Fatalf("WriteDeleteMark: %v", err)
		}
		if err := w.WriteDeleteComplete(ctx, "kb1"); err != nil {
			t.Fatalf("WriteDeleteComplete: %v", err)
		}
		pending, err := w.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 0 {
			t.Fatalf("Recover() = %v, want empty", pending)
		}
	})

	t.Run("recover: returns both VersionWrite and DeleteMark records when both are pending", func(t *testing.T) {
		w := NewMockWAL()
		mustWriteBegin(t, w)
		mustWriteVersionID(t, w, 7) // no COMMIT: pending VersionWrite
		if err := w.WriteDeleteMark(ctx, "kb1"); err != nil {
			t.Fatalf("WriteDeleteMark: %v", err) // no DELETE_COMPLETE: pending DeleteMark
		}

		pending, err := w.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if len(pending) != 2 {
			t.Fatalf("Recover() returned %d records, want 2: %v", len(pending), pending)
		}

		var sawVersionWrite, sawDeleteMark bool
		for _, rec := range pending {
			switch rec.Type {
			case types.PendingRecordTypeVersionWrite:
				if rec.VersionID != 7 {
					t.Fatalf("VersionWrite record VersionID = %d, want 7", rec.VersionID)
				}
				sawVersionWrite = true
			case types.PendingRecordTypeDeleteMark:
				if rec.KBID != "kb1" {
					t.Fatalf("DeleteMark record KBID = %q, want kb1", rec.KBID)
				}
				sawDeleteMark = true
			}
		}
		if !sawVersionWrite || !sawDeleteMark {
			t.Fatalf("Recover() = %v, want one VersionWrite and one DeleteMark record", pending)
		}
	})

	t.Run("replay counter accumulates and is not persisted across Reset", func(t *testing.T) {
		w := NewMockWAL()
		rec := types.PendingRecord{Type: types.PendingRecordTypeDeleteMark, KBID: "kb1"}
		w.IncrementReplayCounter(rec)
		w.IncrementReplayCounter(rec)
		w.IncrementReplayCounter(rec)
		counters := w.GetReplayCounters()
		if len(counters) != 1 || counters[0].RetryCount != 3 {
			t.Fatalf("GetReplayCounters = %v, want [{retryCount: 3}]", counters)
		}
		w.Reset() // simulates process restart: in-memory counters reset to zero
		if counters := w.GetReplayCounters(); len(counters) != 0 {
			t.Fatalf("GetReplayCounters after Reset = %v, want empty", counters)
		}
	})

	t.Run("truncate simulates crash mid-write", func(t *testing.T) {
		w := NewMockWAL()
		mustWriteBegin(t, w)
		mustWriteVersionID(t, w, 7)
		if err := w.WriteCommit(ctx, 7); err != nil {
			t.Fatalf("WriteCommit: %v", err)
		}
		// Simulate a crash that lost the last record (COMMIT) before it
		// hit disk.
		w.Truncate(1)
		pending := w.PendingVersionIDs()
		if len(pending) != 1 || pending[0] != 7 {
			t.Fatalf("after truncating COMMIT, PendingVersionIDs = %v, want [7]", pending)
		}
	})
}

func mustWriteBegin(t *testing.T, w *MockWAL) {
	t.Helper()
	if err := w.WriteBegin(context.Background()); err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
}

func mustWriteVersionID(t *testing.T, w *MockWAL, versionID int64) {
	t.Helper()
	if err := w.WriteVersionID(context.Background(), versionID); err != nil {
		t.Fatalf("WriteVersionID(%d): %v", versionID, err)
	}
}
