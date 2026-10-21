package plane

import (
	"context"
	"testing"
	"time"

	"stratum/internal/tombstone"
	"stratum/internal/types"
)

// The start-up pass is authorised by reconcile.deleted_versions, not by the periodic
// switch — and it is the only pass that can establish this node's watermark. A node
// configured with the periodic full pass OFF ("only the cheap incremental path,
// please") would otherwise never get a watermark at all: every incremental read falls
// back to the chain, and the chain is switched off, so the sweep reports "skipped"
// forever and no deletion ever reaches the node.
func TestLocalDataPlane_StartupSweepAlignsEvenWithThePeriodicPassOff(t *testing.T) {
	ctx := context.Background()
	store := tombstone.NewMemoryStore()
	dp := newSweepPlane(t, store, &fakeDeletionLog{latest: 9, oldest: 1, epoch: "e1"})
	dp.lastChainReconcile = time.Time{} // a process that has not swept yet
	dp.chainReconcileEnabled = false    // reconcile.deleted_versions_periodic: false

	res, err := dp.ReconcileDeletionsAtStartup(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletionsAtStartup: %v", err)
	}
	if res.Source != "chain" || res.Fallback != "no-watermark" {
		t.Fatalf("res = %+v, want the live-chain pass with fallback no-watermark", res)
	}

	state, ok, err := store.SyncSeq(ctx)
	if err != nil {
		t.Fatalf("SyncSeq: %v", err)
	}
	if !ok || state.Seq != 9 || state.Epoch != "e1" {
		t.Fatalf("watermark = (%v, %+v), want (true, {e1 9}) — the start-up pass must align it", ok, state)
	}

	// The contrast that gives the case its meaning: the PERIODIC pass under the same
	// configuration has no way to establish a watermark, so it declines rather than
	// pretending the node is up to date.
	other := newSweepPlane(t, tombstone.NewMemoryStore(), &fakeDeletionLog{latest: 9, oldest: 1, epoch: "e1"})
	other.lastChainReconcile = time.Time{}
	other.chainReconcileEnabled = false
	periodic, err := other.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if periodic.Source != "skipped" || periodic.Fallback != chainDisabled {
		t.Fatalf("periodic res = %+v, want skipped with chain-disabled", periodic)
	}
}

// The alignment is what makes the incremental path usable at all: with the start-up
// pass having run, the next tick reads one page of the log instead of repeating the
// full live-chain scan the old start-up call left it to do.
func TestLocalDataPlane_StartupSweepMakesTheNextTickIncremental(t *testing.T) {
	ctx := context.Background()
	store := tombstone.NewMemoryStore()
	log := &fakeDeletionLog{latest: 3, oldest: 1, epoch: "e1"}
	dp := newSweepPlane(t, store, log)
	dp.lastChainReconcile = time.Time{}

	if _, err := dp.ReconcileDeletionsAtStartup(ctx, sweepMeta()); err != nil {
		t.Fatalf("ReconcileDeletionsAtStartup: %v", err)
	}

	// A deletion that lands after boot is what the next tick has to see, and the floor
	// on the full pass has just been re-armed by the start-up pass.
	log.entries = []types.DeletedVersion{{Seq: 4, KBID: "kb-1", VersionID: 2}}
	log.latest, log.oldest = 4, 1
	dp.lastChainReconcile = time.Now()

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "incremental" {
		t.Fatalf("res = %+v, want the incremental path", res)
	}
	if res.Entries != 1 {
		t.Fatalf("entries = %d, want the one deletion above the watermark", res.Entries)
	}

	state, _, err := store.SyncSeq(ctx)
	if err != nil {
		t.Fatalf("SyncSeq: %v", err)
	}
	if state.Seq != 4 {
		t.Fatalf("watermark = %d, want 4", state.Seq)
	}
	if gone, err := store.Contains(ctx, "kb-1", 2); err != nil || !gone {
		t.Fatalf("Contains(kb-1, v2) = (%v, %v), want the entry recorded", gone, err)
	}
}

// reconcile.deleted_versions_incremental_interval_s documents "<= 0 takes the default,
// 60 s". The default is a contract with the config file, not an implementation detail:
// it was an hour while a tick meant a per-knowledge-base range scan, and leaving it
// there would make a tick that is now one page read run sixty times too rarely.
func TestDeletionSweepDefaultIntervalIsOneMinute(t *testing.T) {
	if DefaultDeletionSweepInterval != time.Minute {
		t.Fatalf("DefaultDeletionSweepInterval = %s, want 1m (see configs/config1.yaml)", DefaultDeletionSweepInterval)
	}
}

// A node whose tombstone table is not wired still honours reconcile.deleted_versions:
// the table is an accelerator and a cross-restart ledger, never the authorisation, so
// the start-up sweep falls back to the live-chain judgement instead of reporting that
// there was nothing to do.
func TestLocalDataPlane_StartupSweepWithoutATombstoneTableStillJudges(t *testing.T) {
	dp := newSweepPlane(t, nil, &fakeDeletionLog{latest: 4, oldest: 1, epoch: "e1"})
	if dp.tombstones != nil {
		t.Fatal("harness error: this case is about a node with no table")
	}

	res, err := dp.ReconcileDeletionsAtStartup(context.Background(), sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletionsAtStartup: %v", err)
	}
	if res.Source != "chain" || res.Fallback != "no-tombstone-table" {
		t.Fatalf("res = %+v, want the chain judgement with fallback no-tombstone-table", res)
	}
}
