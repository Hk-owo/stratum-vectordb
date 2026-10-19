package plane

import (
	"context"
	"errors"
	"testing"
	"time"

	"stratum/internal/tombstone"
)

// newTombstonePlane builds a plane with the tombstone table wired and a puller that
// records what it was asked for, so a test can tell "skipped" from "fetched".
func newTombstonePlane(t *testing.T, store VersionTombstones, dropper VersionDataDropper) (*LocalDataPlane, *recordingPuller) {
	t.Helper()
	puller := &recordingPuller{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		Puller:       puller,
		Verify:       func(context.Context, string, int64) bool { return true },
		Resolve:      func(context.Context, string, int64) (string, bool, error) { return "peer-a", true, nil },
		DataDropper:  dropper,
	})
	dp.SetVersionTombstones(store)
	return dp, puller
}

// The landing paths must not fetch a version this node knows was deleted, and must
// not claim it either: EnsureIndex is asked again and again by the catch-up loop, so
// "no data, but the cursor moved" would be exactly the lie that keeps a replica from
// ever fetching a version it really does need.
func TestLocalDataPlane_EnsureIndexSkipsAVersionItKnowsWasDeleted(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.Note(ctx, "kb-1", 5, time.Now()); err != nil {
		t.Fatalf("Note: %v", err)
	}

	dp, puller := newTombstonePlane(t, store, &stubDropper{})
	if err := dp.EnsureIndex(ctx, "kb-1", 5); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if len(puller.pulled) != 0 {
		t.Fatalf("pulled %v, want nothing — the version is known deleted here", puller.pulled)
	}
	if got := dp.localVersionOf("kb-1"); got != 0 {
		t.Fatalf("cursor = %d, want 0: this node does not hold the version", got)
	}
}

// FetchVersionData is the explicit "bring this version here" call. For a version
// this node knows is gone it answers without fetching and WITHOUT claiming it.
func TestLocalDataPlane_FetchVersionDataSkipsAVersionItKnowsWasDeleted(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.Note(ctx, "kb-1", 5, time.Now()); err != nil {
		t.Fatalf("Note: %v", err)
	}

	dp, puller := newTombstonePlane(t, store, &stubDropper{})
	if err := dp.FetchVersionData(ctx, "kb-1", 5); err != nil {
		t.Fatalf("FetchVersionData: %v", err)
	}
	if len(puller.pulled) != 0 {
		t.Fatalf("pulled %v, want nothing", puller.pulled)
	}
	if got := dp.localVersionOf("kb-1"); got != 0 {
		t.Fatalf("cursor = %d, want 0: dataLanded must not run for a skipped version", got)
	}
}

// The delta backfill path is the one that can be handed a deleted version's changes
// (the source's WAL keeps them until every replica reports a cursor past them), so it
// refuses on its own rather than trusting the caller.
func TestLocalDataPlane_BackfillApplySkipsAVersionItKnowsWasDeleted(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.Note(ctx, "kb-1", 5, time.Now()); err != nil {
		t.Fatalf("Note: %v", err)
	}

	dp, _ := newTombstonePlane(t, store, &stubDropper{})
	if err := dp.ApplyBackfillChanges(ctx, "kb-1", 5, 4, nil); err != nil {
		t.Fatalf("ApplyBackfillChanges: %v", err)
	}
	if got := dp.localVersionOf("kb-1"); got != 0 {
		t.Fatalf("cursor = %d, want 0: a skipped version is not accounted for", got)
	}
}

// The retry loop is why a tombstone is durable: a reclaim that did not finish is
// owed by this node alone, and no one is going to repeat the notification.
func TestLocalDataPlane_RetryPendingDeletionsFinishesWhatTheNotificationLeft(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.Note(ctx, "kb-1", 5, time.Now()); err != nil {
		t.Fatalf("Note: %v", err)
	}
	if err := store.Note(ctx, "kb-2", 8, time.Now()); err != nil {
		t.Fatalf("Note: %v", err)
	}

	dropper := &stubDropper{}
	dp, _ := newTombstonePlane(t, store, dropper)

	reclaimed, err := dp.RetryPendingDeletions(ctx, tombstone.DefaultGrace)
	if err != nil {
		t.Fatalf("RetryPendingDeletions: %v", err)
	}
	if reclaimed != 2 {
		t.Fatalf("reclaimed = %d, want 2", reclaimed)
	}
	if len(dropper.dropped) != 2 {
		t.Fatalf("dropped = %v, want both versions", dropper.dropped)
	}
	pending, err := store.Pending(ctx)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("Pending = %+v, want none — both reclaims succeeded", pending)
	}
}

// A reclaim that fails again stays owed: the entry must not be marked done on the
// strength of "we tried", or the deletion would be forgotten with the bytes still
// present.
func TestLocalDataPlane_RetryPendingDeletionsKeepsAFailedEntryOwed(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.Note(ctx, "kb-1", 5, time.Now()); err != nil {
		t.Fatalf("Note: %v", err)
	}

	dp, _ := newTombstonePlane(t, store, &stubDropper{err: errors.New("disk refused the delete")})
	reclaimed, err := dp.RetryPendingDeletions(ctx, tombstone.DefaultGrace)
	if err == nil {
		t.Fatal("expected the failure to be reported")
	}
	if reclaimed != 0 {
		t.Fatalf("reclaimed = %d, want 0", reclaimed)
	}
	pending, perr := store.Pending(ctx)
	if perr != nil {
		t.Fatalf("Pending: %v", perr)
	}
	if len(pending) != 1 {
		t.Fatalf("Pending = %+v, want the entry to stay owed", pending)
	}
}

// A finished entry is dropped once it is older than the grace window, so the table
// is bounded by deletions-in-the-window rather than by the cluster's history.
func TestLocalDataPlane_RetryPendingDeletionsPrunesFinishedEntries(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour)
	if err := store.Note(ctx, "kb-1", 5, old); err != nil {
		t.Fatalf("Note: %v", err)
	}
	if err := store.MarkDone(ctx, "kb-1", 5, old); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}

	dp, _ := newTombstonePlane(t, store, &stubDropper{})
	if _, err := dp.RetryPendingDeletions(ctx, time.Minute); err != nil {
		t.Fatalf("RetryPendingDeletions: %v", err)
	}
	gone, err := store.Contains(ctx, "kb-1", 5)
	if err != nil {
		t.Fatalf("Contains: %v", err)
	}
	if gone {
		t.Fatal("a finished entry past its grace window must be pruned")
	}
}

// An unwired table changes nothing: the plane behaves exactly as it did before the
// table existed, which is what makes this addition safe to leave off.
func TestLocalDataPlane_WithoutTombstonesStillFetches(t *testing.T) {
	dp, puller := newTombstonePlane(t, nil, &stubDropper{})
	if err := dp.EnsureIndex(context.Background(), "kb-1", 5); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if len(puller.pulled) != 1 || puller.pulled[0] != 5 {
		t.Fatalf("pulled = %v, want [5]", puller.pulled)
	}
}
