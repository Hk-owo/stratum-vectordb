package raft

import (
	"context"
	"testing"
	"time"

	"stratum/internal/types"
)

// The watcher is what takes await's latency from "up to one poll" to "as soon as
// the apply lands" (docs/await-version-plan.md §12 item 1). What it has to get
// right is its edges: it fires on state changes, it stops when told, and it never
// blocks the apply loop.

func TestWatchVersion_FiresOnStateChanges(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()
	versionID := newVersionForWatchTest(t, impl)

	changed, stop := impl.WatchVersion(versionID)
	defer stop()

	if err := impl.ProposeUpdateVersionStatus(ctx, versionID, types.IndexStatusReady, 0); err != nil {
		t.Fatalf("ProposeUpdateVersionStatus: %v", err)
	}
	select {
	case <-changed:
	case <-time.After(3 * time.Second):
		t.Fatal("no notification after the version's index status changed")
	}
}

// A version being removed is a state change a waiter cares about too: it is how
// await learns that what it is watching went away.
func TestWatchVersion_FiresWhenTheVersionIsRemoved(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()
	versionID := newVersionForWatchTest(t, impl)

	changed, stop := impl.WatchVersion(versionID)
	defer stop()

	if err := impl.ProposeDiscardVersion(ctx, "kb-1", versionID); err != nil {
		t.Fatalf("ProposeDiscardVersion: %v", err)
	}
	select {
	case <-changed:
	case <-time.After(3 * time.Second):
		t.Fatal("no notification after the version was discarded")
	}
}

func TestWatchVersion_StopsAfterStop(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()
	versionID := newVersionForWatchTest(t, impl)

	changed, stop := impl.WatchVersion(versionID)
	stop()
	stop() // idempotent: the await path calls it from several exits

	if err := impl.ProposeUpdateVersionStatus(ctx, versionID, types.IndexStatusReady, 0); err != nil {
		t.Fatalf("ProposeUpdateVersionStatus: %v", err)
	}
	select {
	case <-changed:
		t.Error("a stopped watcher was notified")
	case <-time.After(300 * time.Millisecond):
	}

	// And it left nothing behind: a registration that outlives its wait would
	// accumulate for the life of the process.
	impl.versionWatchersMu.Lock()
	registrations := len(impl.versionWatchers)
	impl.versionWatchersMu.Unlock()
	if registrations != 0 {
		t.Errorf("stopping left %d registrations behind", registrations)
	}
}

// newVersionForWatchTest creates a knowledge base and one version in it.
func newVersionForWatchTest(t *testing.T, impl *RaftNodeImpl) int64 {
	t.Helper()
	ctx := context.Background()
	if err := impl.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{KBID: "kb-1", Name: "kb-1"}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	versionID, err := impl.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	return versionID
}
