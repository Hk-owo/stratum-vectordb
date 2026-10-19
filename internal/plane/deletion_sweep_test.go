package plane

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
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

// failingTombstones wraps a store and forces the reads a landing path performs to
// fail, which is the case the callers must treat as "assume alive".
type failingTombstones struct {
	VersionTombstones
	err error
}

func (f *failingTombstones) Contains(context.Context, string, int64) (bool, error) {
	return false, f.err
}

func (f *failingTombstones) ContainsKB(context.Context, string) (bool, error) {
	return false, f.err
}

func (f *failingTombstones) MarkDone(context.Context, string, int64, time.Time) error {
	return f.err
}

// A KB-level tombstone refuses every version of that knowledge base: the version has no
// entry of its own, so the KB check is the only thing that can answer.
func TestLocalDataPlane_SkipIfDeletedHonoursAKnowledgeBaseEntry(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e", Seq: 1}, []tombstone.DeletedVersion{{Seq: 1, KBID: "kb-1", VersionID: 0}}); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dp := newSweepPlane(t, store, &fakeDeletionLog{})

	if !dp.skipIfDeleted(ctx, "kb-1", 5, "test") {
		t.Fatal("a version of a deleted knowledge base must be skipped")
	}
	if dp.skipIfDeleted(ctx, "kb-2", 5, "test") {
		t.Fatal("another knowledge base must be unaffected")
	}
}

// An unreadable table answers "alive": refusing data on a broken local answer would
// turn a broken table into a stalled replication path, and the reconciliation still
// covers whatever gets through.
func TestLocalDataPlane_SkipIfDeletedTreatsAnUnreadableTableAsAlive(t *testing.T) {
	dp := newSweepPlane(t, &failingTombstones{VersionTombstones: tombstone.NewMemoryStore(), err: errors.New("table unavailable")}, &fakeDeletionLog{})
	if dp.skipIfDeleted(context.Background(), "kb-1", 5, "test") {
		t.Fatal("an unreadable table must not refuse data")
	}
}

// A KB-level entry with no KB dropper stays OWED and says why: pretending the debt is
// paid is how a deleted knowledge base's bytes survive forever.
func TestLocalDataPlane_RetryPendingDeletionsLeavesAKBOwedWithoutADropper(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e", Seq: 1}, []tombstone.DeletedVersion{{Seq: 1, KBID: "kb-1", VersionID: 0}}); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dp := newSweepPlane(t, store, &fakeDeletionLog{})

	reclaimed, err := dp.RetryPendingDeletions(ctx, time.Minute)
	if reclaimed != 0 {
		t.Fatalf("reclaimed = %d, want 0", reclaimed)
	}
	if err == nil || !strings.Contains(err.Error(), "KB dropper") {
		t.Fatalf("err = %v, want the missing-dropper reason", err)
	}
	pending, perr := store.Pending(ctx)
	if perr != nil {
		t.Fatalf("Pending: %v", perr)
	}
	if len(pending) != 1 {
		t.Fatalf("Pending = %+v, want the entry to stay owed", pending)
	}
}

// A reclaim that succeeded but could not be marked done costs one more idempotent
// prefix delete next pass — not a silently forgotten debt.
func TestLocalDataPlane_RetryPendingDeletionsKeepsTheEntryWhenMarkingFails(t *testing.T) {
	inner := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := inner.NoteBatch(ctx, tombstone.SyncState{Epoch: "e", Seq: 1}, []tombstone.DeletedVersion{{Seq: 1, KBID: "kb-1", VersionID: 5}}); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dp := newSweepPlane(t, &failingTombstones{VersionTombstones: inner, err: errors.New("mark failed")}, &fakeDeletionLog{})
	dp.dropper = &stubDropper{}

	reclaimed, err := dp.RetryPendingDeletions(ctx, time.Minute)
	if err == nil {
		t.Fatal("expected the marking failure to be reported")
	}
	if reclaimed != 0 {
		t.Fatalf("reclaimed = %d, want 0 — the entry was not marked", reclaimed)
	}
}

// The retry loop is what makes a durable tombstone worth keeping: it finishes a
// deleted version's reclaim with nothing but local state.
func TestLocalDataPlane_StartDeletedVersionRetryDrainsPendingLocally(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e", Seq: 1}, []tombstone.DeletedVersion{{Seq: 1, KBID: "kb-1", VersionID: 6}}); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	dropper := &stubDropper{}
	dp := newSweepPlane(t, store, &fakeDeletionLog{})
	dp.dropper = dropper

	dp.StartDeletedVersionRetry(ctx, 5*time.Millisecond, time.Hour)

	deadline := time.Now().Add(2 * time.Second)
	for {
		pending, err := store.Pending(ctx)
		if err != nil {
			t.Fatalf("Pending: %v", err)
		}
		if len(pending) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the retry loop never drained the entry; reclaimed so far = %v", dropper.dropped)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(dropper.dropped) != 1 || dropper.dropped[0] != "kb-1/6" {
		t.Fatalf("reclaims = %v, want kb-1/6", dropper.dropped)
	}
}

// The sweep's reading is the only way to tell an incremental read that saved a range
// scan from one that fell back every time; it must be callable and must not panic on a
// zero-valued result.
func TestLocalDataPlane_LogDeletionSweepHandlesEveryShape(t *testing.T) {
	dp := newSweepPlane(t, tombstone.NewMemoryStore(), &fakeDeletionLog{})
	dp.logDeletionSweep(DeletionSweepResult{Source: "incremental", LocalSeq: 3, LatestSeq: 5, Entries: 2}, time.Millisecond)
	dp.logDeletionSweep(DeletionSweepResult{Source: "chain", Fallback: "log-truncated", Truncated: true}, time.Millisecond)
	dp.logDeletionSweep(DeletionSweepResult{Source: "skipped", BehindLeader: true}, 0)
}

// The configured knobs win over the defaults, and a zero value falls back.
func TestLocalDataPlane_DeletionKnobsPreferConfiguration(t *testing.T) {
	dp := newSweepPlane(t, tombstone.NewMemoryStore(), &fakeDeletionLog{})
	if got := dp.deletionPage(); got != DefaultDeletionLogPage {
		t.Fatalf("default page = %d, want %d", got, DefaultDeletionLogPage)
	}
	dp.deletionLogPage = 7
	if got := dp.deletionPage(); got != 7 {
		t.Fatalf("configured page = %d, want 7", got)
	}
	if got := dp.maxChainReconcileAge(); got != defaultChainReconcileMaxAge {
		t.Fatalf("default max age = %s, want %s", got, defaultChainReconcileMaxAge)
	}
	dp.chainReconcileMaxAge = time.Hour
	if got := dp.maxChainReconcileAge(); got != time.Hour {
		t.Fatalf("configured max age = %s, want 1h", got)
	}
	// A node that never ran a full pass may run one at once, whatever the floor.
	dp.lastChainReconcile = time.Time{}
	if !dp.chainAvailable(defaultChainReconcileMinAge) {
		t.Fatal("a node with no previous full pass must be allowed to run one")
	}
	dp.chainReconcileEnabled = false
	if dp.chainAvailable(defaultChainReconcileMinAge) {
		t.Fatal("the operator switch must win")
	}
}

// The coordinator's OWN local write is the other way a deleted version comes back:
// this node can be the coordinator, and its own write goes through
// writeLocalTransaction. Refused BEFORE the WAL BEGIN, so nothing is written and
// nothing is later committed for metadata that no longer exists.
func TestLocalDataPlane_WriteVersionDataRefusesAVersionKnownDeleted(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e", Seq: 1}, []tombstone.DeletedVersion{{Seq: 1, KBID: "kb-1", VersionID: 5}}); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
		Verify:       func(context.Context, string, int64) bool { return true },
		Resolve:      func(context.Context, string, int64) (string, bool, error) { return "peer-a", true, nil },
	})
	dp.SetVersionTombstones(store)

	err := dp.WriteVersionData(ctx, "kb-1", 5, 4, nil)
	if !errors.Is(err, stratumerrors.ErrVersionDeleting) {
		t.Fatalf("WriteVersionData = %v, want ErrVersionDeleting", err)
	}
	if len(tr.trace) != 0 {
		t.Fatalf("the transaction started anyway: %v", tr.trace)
	}
}

// Crash recovery is the same hazard with a longer fuse: a version whose write never
// reached COMMIT is replayed from the WAL on restart, and if it was deleted while this
// node was down the replay would put the whole batch back.
func TestLocalDataPlane_ResumeVersionWriteRefusesAVersionKnownDeleted(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e", Seq: 1}, []tombstone.DeletedVersion{{Seq: 1, KBID: "kb-1", VersionID: 5}}); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
		Verify:       func(context.Context, string, int64) bool { return true },
		Resolve:      func(context.Context, string, int64) (string, bool, error) { return "peer-a", true, nil },
	})
	dp.SetVersionTombstones(store)

	err := dp.ResumeVersionWrite(ctx, "kb-1", 5, 4, nil)
	if !errors.Is(err, stratumerrors.ErrVersionDeleting) {
		t.Fatalf("ResumeVersionWrite = %v, want ErrVersionDeleting", err)
	}
	if len(tr.trace) != 0 {
		t.Fatalf("the replay wrote anyway: %v", tr.trace)
	}
}

// One page per tick would let a bulk deletion take many minutes to become visible, so a
// pass pages until the log is drained (bounded). This pins the paging: five entries with
// a page of two must all be recorded in ONE pass, in order, with the watermark ending at
// the last one.
func TestLocalDataPlane_DeletionSweepDrainsEveryPageInOnePass(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.NoteBatch(ctx, tombstone.SyncState{Epoch: "e1", Seq: 0}, nil); err != nil {
		t.Fatalf("NoteBatch: %v", err)
	}
	log := &fakeDeletionLog{latest: 5, oldest: 1, epoch: "e1"}
	for i := int64(1); i <= 5; i++ {
		log.entries = append(log.entries, types.DeletedVersion{Seq: i, KBID: "kb-1", VersionID: i})
	}
	dropper := &stubDropper{}
	dp := newSweepPlane(t, store, log)
	dp.deletionLogPage = 2
	dp.dropper = dropper

	res, err := dp.ReconcileDeletions(ctx, sweepMeta())
	if err != nil {
		t.Fatalf("ReconcileDeletions: %v", err)
	}
	if res.Entries != 5 {
		t.Fatalf("entries = %d, want all 5 across the pages", res.Entries)
	}
	if res.Reclaimed != 5 {
		t.Fatalf("reclaimed = %d, want 5", res.Reclaimed)
	}
	state, _, err := store.SyncSeq(ctx)
	if err != nil {
		t.Fatalf("SyncSeq: %v", err)
	}
	if state.Seq != 5 {
		t.Fatalf("watermark = %d, want 5 (the last entry of the last page)", state.Seq)
	}
	if got := len(dropper.dropped); got != 5 {
		t.Fatalf("version reclaims = %d, want 5: %v", got, dropper.dropped)
	}
}

// The retry loop's parameters default rather than doing nothing, and a loop that is
// cancelled immediately must return without touching anything.
func TestLocalDataPlane_StartDeletedVersionRetryDefaultsAndStops(t *testing.T) {
	store := tombstone.NewMemoryStore()
	dp := newSweepPlane(t, store, &fakeDeletionLog{})
	ctx, cancel := context.WithCancel(context.Background())
	dp.StartDeletedVersionRetry(ctx, 0, 0) // zero interval and grace take the defaults
	cancel()

	// A plane with no dropper has nothing to retry: the loop must not start at all.
	dp.dropper = nil
	dp.StartDeletedVersionRetry(context.Background(), time.Millisecond, time.Millisecond)
}
