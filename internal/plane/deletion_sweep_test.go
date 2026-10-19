package plane

import (
	"context"
	"errors"
	"testing"
	"time"

	"stratum/internal/tombstone"
	"stratum/internal/types"
)

// fakeDeletionLog is the control layer's log as the sweep sees it: a page, the window
// bounds, an epoch, and an optionally injected failure.
type fakeDeletionLog struct {
	entries []types.DeletedVersion
	latest  int64
	oldest  int64
	epoch   string
	err     error
	calls   int
}

func (f *fakeDeletionLog) DeletionsSince(_ context.Context, afterSeq int64, limit int) ([]types.DeletedVersion, int64, int64, string, error) {
	f.calls++
	if f.err != nil {
		return nil, 0, 0, "", f.err
	}
	out := make([]types.DeletedVersion, 0, len(f.entries))
	for _, e := range f.entries {
		if e.Seq <= afterSeq {
			continue
		}
		if limit > 0 && len(out) >= limit {
			break
		}
		out = append(out, e)
	}
	return out, f.latest, f.oldest, f.epoch, nil
}

// newSweepPlane wires a plane whose live-chain path finds nothing (no local versions,
// no liveness) so these cases are about the sweep's CHOICE of path and about the
// watermark, not about reclaiming.
type stubLiveness struct{}

func (stubLiveness) VersionLiveness(context.Context, string, *int64, *int64) ([]int64, int64, error) {
	return nil, 0, nil
}

func newSweepPlane(t *testing.T, store VersionTombstones, log DeletionLogSource) *LocalDataPlane {
	t.Helper()
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:  &stubIndexStore{},
		LocalVersions: &stubLocalVersions{},
		Liveness:      stubLiveness{},
		DataDropper:   &stubDropper{},
		Verify:        func(context.Context, string, int64) bool { return true },
		Resolve:       func(context.Context, string, int64) (string, bool, error) { return "peer-a", true, nil },
	})
	dp.SetVersionTombstones(store)
	dp.deletions = log
	// As if a full pass had just run: these cases are about the incremental DECISION.
	// A fresh process starts with the zero value, which forces one full pass — the
	// safe direction, and its own case below.
	dp.lastChainReconcile = time.Now()
	return dp
}

func sweepMeta() MetadataLister { return &stubMetadata{} }

// No watermark means the node cannot be served incrementally, and must not be told
// "you are up to date" either — it re-judges from the live chain and then aligns.
func TestLocalDataPlane_DeletionSweepWithoutAWatermarkTakesTheChain(t *testing.T) {
	store := tombstone.NewMemoryStore()
	log := &fakeDeletionLog{latest: 9, oldest: 5, epoch: "e1"}
	dp := newSweepPlane(t, store, log)

	res, err := dp.ReconcileDeletions(context.Background(), sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "chain" || res.Fallback != "no-watermark" {
		t.Fatalf("res = %+v, want the chain path with fallback no-watermark", res)
	}
	state, ok, err := store.SyncSeq(context.Background())
	if err != nil || !ok {
		t.Fatalf("SyncSeq = (%v, %v), want an aligned watermark", ok, err)
	}
	if state.Seq != 9 || state.Epoch != "e1" {
		t.Fatalf("watermark = %+v, want {e1 9} — the chain pass just covered everything the log had pruned", state)
	}
}

// Caught up: nothing new was deleted, so the sweep does nothing at all. This is the
// case the log exists for — one small read instead of a range scan.
func TestLocalDataPlane_DeletionSweepSkipsWhenCaughtUp(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e1", Seq: 4}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	log := &fakeDeletionLog{latest: 4, oldest: 4, epoch: "e1"}
	dp := newSweepPlane(t, store, log)

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "skipped" {
		t.Fatalf("res = %+v, want skipped", res)
	}
	if log.calls != 1 {
		t.Fatalf("log calls = %d, want exactly 1 (no pagination when there is nothing)", log.calls)
	}
}

// A replica that has applied fewer entries than this node already consumed is BEHIND,
// not a rollback: the epoch matched. Skipping keeps the watermark; resetting it would
// re-read everything on every pass against a lagging reader.
func TestLocalDataPlane_DeletionSweepSkipsWhenTheReplicaIsBehind(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e1", Seq: 10}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dp := newSweepPlane(t, store, &fakeDeletionLog{latest: 3, oldest: 1, epoch: "e1"})

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "skipped" || !res.BehindLeader {
		t.Fatalf("res = %+v, want skipped with behind_leader set", res)
	}
	state, _, err := store.SyncSeq(ctx)
	if err != nil {
		t.Fatalf("SyncSeq: %v", err)
	}
	if state.Seq != 10 {
		t.Fatalf("watermark = %d, want 10 — a lagging reader must not pull it back", state.Seq)
	}
}

// A different epoch is another cluster's numbering: the watermark is void.
func TestLocalDataPlane_DeletionSweepFallsBackOnAnEpochMismatch(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "old", Seq: 50}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dp := newSweepPlane(t, store, &fakeDeletionLog{latest: 2, oldest: 1, epoch: "new"})

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "chain" || res.Fallback != deletionLogEpochMismatch {
		t.Fatalf("res = %+v, want the chain path on an epoch mismatch", res)
	}
	state, _, err := store.SyncSeq(ctx)
	if err != nil {
		t.Fatalf("SyncSeq: %v", err)
	}
	if state.Epoch != "new" || state.Seq != 2 {
		t.Fatalf("watermark = %+v, want the new epoch's", state)
	}
}

// Truncated: the entries this node needs are gone. Same treatment.
func TestLocalDataPlane_DeletionSweepFallsBackWhenTruncated(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e1", Seq: 3}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dp := newSweepPlane(t, store, &fakeDeletionLog{latest: 20, oldest: 15, epoch: "e1"})

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "chain" || res.Fallback != "log-truncated" {
		t.Fatalf("res = %+v, want the chain path for a truncated log", res)
	}
	if !res.Truncated {
		t.Fatal("res.Truncated = false, want the reading to say why")
	}
}

// An empty log with live sequence numbers is ALSO truncated: the entries are gone, and
// treating it as "nothing happened" would leave the watermark stuck forever.
func TestLocalDataPlane_DeletionSweepTreatsAnEmptyLogAsTruncated(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e1", Seq: 3}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dp := newSweepPlane(t, store, &fakeDeletionLog{latest: 20, oldest: 0, epoch: "e1"})

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "chain" || res.Fallback != "log-truncated" {
		t.Fatalf("res = %+v, want the chain path — an empty log with latest > watermark is truncation", res)
	}
}

// The incremental path: the page is recorded (entries + watermark atomically) and the
// entries are drained locally, so a deletion lands without any range read.
func TestLocalDataPlane_DeletionSweepAppliesAnIncrementalPage(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e1", Seq: 1}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dropper := &stubDropper{}
	log := &fakeDeletionLog{
		entries: []types.DeletedVersion{
			{Seq: 2, KBID: "kb-1", VersionID: 7},
			{Seq: 3, KBID: "kb-2", VersionID: 0},
		},
		latest: 3, oldest: 1, epoch: "e1",
	}
	dp := newSweepPlane(t, store, log)
	// Both droppers, because the page holds one of each kind: a KB-level entry without a
	// KB dropper stays owed (the correct behaviour, but it would hide what this case is
	// about), and the version dropper is what proves the version-level one ran.
	dp.kbDropper = &stubKBDropper{}
	dp.dropper = dropper

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "incremental" || res.Entries != 2 {
		t.Fatalf("res = %+v, want the incremental path with two entries", res)
	}
	if res.Reclaimed != 2 {
		t.Fatalf("reclaimed = %d, want 2 (both drained after being recorded)", res.Reclaimed)
	}
	state, _, err := store.SyncSeq(ctx)
	if err != nil {
		t.Fatalf("SyncSeq: %v", err)
	}
	if state.Seq != 3 {
		t.Fatalf("watermark = %d, want 3", state.Seq)
	}
	if len(dropper.dropped) != 1 || dropper.dropped[0] != "kb-1/7" {
		t.Fatalf("version reclaims = %v, want kb-1/7", dropper.dropped)
	}

	// A second pass is caught up and does nothing.
	again, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions (caught up): %v", err)
	}
	if again.Source != "skipped" {
		t.Fatalf("second pass = %+v, want skipped", again)
	}
}

type stubKBDropper struct{ dropped []string }

func (s *stubKBDropper) DropKnowledgeBase(_ context.Context, kbID string) error {
	s.dropped = append(s.dropped, kbID)
	return nil
}

// The forced full pass cannot be starved by incremental traffic.
func TestLocalDataPlane_DeletionSweepForcesAFullPassAfterTheAgeBound(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e1", Seq: 2}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dp := newSweepPlane(t, store, &fakeDeletionLog{latest: 2, oldest: 1, epoch: "e1"})
	dp.lastChainReconcile = time.Now().Add(-25 * time.Hour)

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "chain" || res.Fallback != "chain-reconcile-due" {
		t.Fatalf("res = %+v, want the forced chain pass", res)
	}
}

// A local landing forces it too: the log cannot describe the leftovers a late push
// creates, and "something landed here" is the only signal this node gets.
func TestLocalDataPlane_DeletionSweepRunsTheChainWhenLocalHoldingsChanged(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e1", Seq: 2}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dp := newSweepPlane(t, store, &fakeDeletionLog{latest: 2, oldest: 1, epoch: "e1"})
	// The pass runs on a minute tick, so "something landed" is filtered by a floor:
	// a busy node would otherwise range-scan every minute, which is the cost this
	// mechanism removes. Six minutes is past that floor.
	dp.lastChainReconcile = time.Now().Add(-6 * time.Minute)
	dp.noteHoldingsChanged()

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "chain" || res.Fallback != "local-holdings-changed" {
		t.Fatalf("res = %+v, want the chain pass triggered by a local landing", res)
	}
	if dp.peekDeletedReconcileDirty() {
		t.Fatal("the dirty flag must be consumed by the pass that acted on it")
	}
}

// The floor, from the other side: a landing right after a full pass does not buy
// another one, and the trigger stays pending for when the floor expires.
func TestLocalDataPlane_DeletionSweepHoldsTheChainTriggerUntilTheFloorExpires(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e1", Seq: 2}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dp := newSweepPlane(t, store, &fakeDeletionLog{latest: 2, oldest: 1, epoch: "e1"})
	dp.noteHoldingsChanged() // lastChainReconcile is "just now" from the harness

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "skipped" {
		t.Fatalf("res = %+v, want skipped — the full pass ran moments ago", res)
	}
	if !dp.peekDeletedReconcileDirty() {
		t.Fatal("the trigger must still be pending: the pass that would consume it did not run")
	}
}

// With the periodic pass switched off, a re-judge is reported rather than performed.
func TestLocalDataPlane_DeletionSweepReportsWhenTheChainIsDisabled(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e2", Seq: 2}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dp := newSweepPlane(t, store, &fakeDeletionLog{latest: 9, oldest: 5, epoch: "different"})
	dp.chainReconcileEnabled = false

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Source != "skipped" || res.Fallback != chainDisabled {
		t.Fatalf("res = %+v, want skipped with the chain-disabled reason", res)
	}
}

// A failing control-tier read is reported and changes nothing.
func TestLocalDataPlane_DeletionSweepSurfacesALogReadFailure(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e1", Seq: 2}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	boom := errors.New("control tier unreachable")
	dp := newSweepPlane(t, store, &fakeDeletionLog{err: boom})
	dp.lastChainReconcile = time.Now()

	if _, err := dp.ReconcileDeletions(ctx, sweepMeta()); !errors.Is(err, boom) {
		t.Fatalf("ReconcileDeletions = %v, want the read failure", err)
	}
	state, _, serr := store.SyncSeq(ctx)
	if serr != nil {
		t.Fatalf("SyncSeq: %v", serr)
	}
	if state.Seq != 2 {
		t.Fatalf("watermark = %d, want it untouched", state.Seq)
	}
}
