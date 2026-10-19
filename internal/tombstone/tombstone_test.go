package tombstone

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// stores runs the same contract against both implementations: a durable one and
// the in-process one. The table is only useful if they agree, and the durable one
// is the one that has to survive a restart.
func stores(t *testing.T) map[string]func(t *testing.T) Store {
	t.Helper()
	return map[string]func(t *testing.T) Store{
		"memory": func(t *testing.T) Store {
			t.Helper()
			return NewMemoryStore()
		},
		"pebble": func(t *testing.T) Store {
			t.Helper()
			store, err := NewPebbleStore(filepath.Join(t.TempDir(), "tombstone"))
			if err != nil {
				t.Fatalf("NewPebbleStore: %v", err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		},
	}
}

func mustContains(t *testing.T, s Store, kbID string, versionID int64, want bool) {
	t.Helper()
	got, err := s.Contains(context.Background(), kbID, versionID)
	if err != nil {
		t.Fatalf("Contains(%s, %d): %v", kbID, versionID, err)
	}
	if got != want {
		t.Fatalf("Contains(%s, %d) = %v, want %v", kbID, versionID, got, want)
	}
}

// A notification makes the version refused from the moment it arrives — before the
// reclaim runs, because "told, not cleaned up yet" is not a reason to accept more
// of its data.
func TestStore_NoteRefusesDataImmediately(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			now := time.Now()
			mustContains(t, s, "kb-1", 7, false)

			if err := s.Note(context.Background(), "kb-1", 7, now); err != nil {
				t.Fatalf("Note: %v", err)
			}
			mustContains(t, s, "kb-1", 7, true)
			// Another version of the same knowledge base is untouched.
			mustContains(t, s, "kb-1", 8, false)
			mustContains(t, s, "kb-2", 7, false)
		})
	}
}

// The coordinator retries, so Note arrives more than once. A repeat must not reset
// a finished reclaim back to PENDING: the retry would then leave work that is
// already done sitting in the retry list forever.
func TestStore_RepeatedNoteDoesNotUndoAFinishedReclaim(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			ctx := context.Background()
			now := time.Now()

			if err := s.Note(ctx, "kb-1", 7, now); err != nil {
				t.Fatalf("Note: %v", err)
			}
			if err := s.MarkDone(ctx, "kb-1", 7, now); err != nil {
				t.Fatalf("MarkDone: %v", err)
			}
			if pending, err := s.Pending(ctx); err != nil || len(pending) != 0 {
				t.Fatalf("Pending after MarkDone = %v (err %v), want none", pending, err)
			}

			if err := s.Note(ctx, "kb-1", 7, now.Add(time.Second)); err != nil {
				t.Fatalf("Note (retry): %v", err)
			}
			pending, err := s.Pending(ctx)
			if err != nil {
				t.Fatalf("Pending: %v", err)
			}
			if len(pending) != 0 {
				t.Fatalf("Pending after a retried Note = %v, want none — the reclaim is still finished", pending)
			}
			mustContains(t, s, "kb-1", 7, true)
		})
	}
}

// Only unfinished entries come back for retry, and in a deterministic order.
func TestStore_PendingListsUnfinishedEntriesInOrder(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			ctx := context.Background()
			now := time.Now()

			for _, v := range []int64{9, 3} {
				if err := s.Note(ctx, "kb-1", v, now); err != nil {
					t.Fatalf("Note: %v", err)
				}
			}
			if err := s.Note(ctx, "kb-2", 1, now); err != nil {
				t.Fatalf("Note: %v", err)
			}
			if err := s.MarkDone(ctx, "kb-1", 9, now); err != nil {
				t.Fatalf("MarkDone: %v", err)
			}

			pending, err := s.Pending(ctx)
			if err != nil {
				t.Fatalf("Pending: %v", err)
			}
			if len(pending) != 2 {
				t.Fatalf("Pending = %+v, want 2 entries", pending)
			}
			if name == "memory" || name == "pebble" {
				// Both implementations promise the same order.
				if pending[0].KBID != "kb-1" || pending[0].VersionID != 3 || pending[1].KBID != "kb-2" || pending[1].VersionID != 1 {
					t.Fatalf("Pending = %+v, want kb-1/v3 then kb-2/v1", pending)
				}
			}
		})
	}
}

// MarkDone for a version nobody noted is not an error: the reverse reconciliation
// reclaims those, and inventing a tombstone here would record a notification that
// never arrived.
func TestStore_MarkDoneWithoutANoteIsANoOp(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			if err := s.MarkDone(context.Background(), "kb-1", 5, time.Now()); err != nil {
				t.Fatalf("MarkDone: %v", err)
			}
			mustContains(t, s, "kb-1", 5, false)
		})
	}
}

// Finished entries outlive their usefulness after the grace window and must go, or
// the table grows with the cluster's whole deletion history. Unfinished ones are
// never pruned: they are still work.
func TestStore_PruneDropsOnlyOldFinishedEntries(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			ctx := context.Background()
			base := time.Now()

			// old-done, fresh-done, pending (old and fresh, both kept)
			if err := s.Note(ctx, "kb-1", 1, base.Add(-3*time.Hour)); err != nil {
				t.Fatalf("Note: %v", err)
			}
			if err := s.MarkDone(ctx, "kb-1", 1, base.Add(-2*time.Hour)); err != nil {
				t.Fatalf("MarkDone: %v", err)
			}
			if err := s.Note(ctx, "kb-1", 2, base); err != nil {
				t.Fatalf("Note: %v", err)
			}
			if err := s.MarkDone(ctx, "kb-1", 2, base); err != nil {
				t.Fatalf("MarkDone: %v", err)
			}
			if err := s.Note(ctx, "kb-1", 3, base.Add(-2*time.Hour)); err != nil {
				t.Fatalf("Note: %v", err)
			}

			dropped, err := s.Prune(ctx, base, time.Hour)
			if err != nil {
				t.Fatalf("Prune: %v", err)
			}
			if dropped != 1 {
				t.Fatalf("Prune dropped %d entries, want 1 (only the old finished one)", dropped)
			}
			mustContains(t, s, "kb-1", 1, false)
			mustContains(t, s, "kb-1", 2, true)
			mustContains(t, s, "kb-1", 3, true)
		})
	}
}

// The whole reason for a DURABLE table: the fact has to survive the process. A node
// that restarts between "told" and "cleaned up" resumes from here.
func TestPebbleStore_TombstonesSurviveAReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tombstone")

	first, err := NewPebbleStore(path)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	ctx := context.Background()
	now := time.Now()
	if err := first.Note(ctx, "kb-1", 7, now); err != nil {
		t.Fatalf("Note: %v", err)
	}
	if err := first.Note(ctx, "kb-1", 8, now); err != nil {
		t.Fatalf("Note: %v", err)
	}
	if err := first.MarkDone(ctx, "kb-1", 8, now); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := NewPebbleStore(path)
	if err != nil {
		t.Fatalf("NewPebbleStore (reopen): %v", err)
	}
	defer second.Close()

	mustContains(t, second, "kb-1", 7, true)
	mustContains(t, second, "kb-1", 8, true)

	// And the states are the ones that were written: only v7 is still work.
	pending, err := second.Pending(ctx)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 || pending[0].VersionID != 7 {
		t.Fatalf("Pending after reopen = %+v, want only v7", pending)
	}
}

// The watermark and the entries it describes move together. Entries without a moved
// watermark cost a re-read; a MOVED watermark without its entries loses those
// deletions permanently, because the control layer only serves a range once.
func TestStore_NoteBatchRecordsThePageAndTheWatermark(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			ctx := context.Background()

			if _, ok, err := s.SyncSeq(ctx); err != nil || ok {
				t.Fatalf("SyncSeq on a fresh table = (%v, %v), want none", ok, err)
			}

			page := []DeletedVersion{
				{Seq: 1, KBID: "kb-1", VersionID: 7},
				{Seq: 2, KBID: "kb-2", VersionID: 0}, // the whole knowledge base
			}
			if err := s.NoteBatch(ctx, SyncState{Epoch: "e1", Seq: 2}, page); err != nil {
				t.Fatalf("NoteBatch: %v", err)
			}

			state, ok, err := s.SyncSeq(ctx)
			if err != nil || !ok {
				t.Fatalf("SyncSeq = (%v, %v), want the watermark", ok, err)
			}
			if state.Epoch != "e1" || state.Seq != 2 {
				t.Fatalf("watermark = %+v, want {e1 2}", state)
			}
			mustContains(t, s, "kb-1", 7, true)
			if gone, err := s.ContainsKB(ctx, "kb-2"); err != nil || !gone {
				t.Fatalf("ContainsKB(kb-2) = (%v, %v), want true — version 0 names the KB itself", gone, err)
			}
			pending, err := s.Pending(ctx)
			if err != nil {
				t.Fatalf("Pending: %v", err)
			}
			if len(pending) != 2 {
				t.Fatalf("Pending = %+v, want both entries owed", pending)
			}
		})
	}
}

// A KB-level entry must not be mistaken for a version: Contains(kb, 0) is not the
// question anyone asks, ContainsKB is.
func TestStore_KBEntryIsNotAVersionTombstone(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			ctx := context.Background()
			if err := s.NoteBatch(ctx, SyncState{Epoch: "e", Seq: 1}, []DeletedVersion{{Seq: 1, KBID: "kb-1", VersionID: 0}}); err != nil {
				t.Fatalf("NoteBatch: %v", err)
			}
			if gone, err := s.Contains(ctx, "kb-1", 5); err != nil || gone {
				t.Fatalf("Contains(kb-1, 5) = (%v, %v), want false — the KB-level entry is a different question", gone, err)
			}
			if gone, err := s.ContainsKB(ctx, "kb-1"); err != nil || !gone {
				t.Fatalf("ContainsKB(kb-1) = (%v, %v), want true", gone, err)
			}
		})
	}
}

// The watermark is what makes the incremental read possible across a restart.
func TestPebbleStore_SyncWatermarkSurvivesAReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tombstone")
	first, err := NewPebbleStore(path)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	ctx := context.Background()
	if err := first.NoteBatch(ctx, SyncState{Epoch: "epoch-1", Seq: 42}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := NewPebbleStore(path)
	if err != nil {
		t.Fatalf("NewPebbleStore (reopen): %v", err)
	}
	defer second.Close()
	state, ok, err := second.SyncSeq(ctx)
	if err != nil || !ok {
		t.Fatalf("SyncSeq after reopen = (%v, %v), want the watermark", ok, err)
	}
	if state.Epoch != "epoch-1" || state.Seq != 42 {
		t.Fatalf("watermark = %+v, want {epoch-1 42}", state)
	}
}
