package plane

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// stubVersionExistence stands in for the replicated metadata. It counts LIST CALLS,
// not per-version lookups: the whole point of the set-returning shape is that a long
// gap costs one read.
type stubVersionExistence struct {
	exists map[int64]bool
	err    error
	calls  int
}

func (s *stubVersionExistence) ExistingVersions(_ context.Context, _ string) (map[int64]bool, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.exists, nil
}

// snapshotPuller records the two transfer paths separately, so a test can tell
// "filled the gap version by version" from "fell back to a full-state transfer".
type snapshotPuller struct {
	versionPulls []int64
	dataPulls    []int64
	versionErr   map[int64]error
	dataErr      error
}

func (p *snapshotPuller) PullVersion(_ context.Context, _, _ string, versionID int64) error {
	p.versionPulls = append(p.versionPulls, versionID)
	return p.versionErr[versionID]
}

func (p *snapshotPuller) PullVersionData(_ context.Context, _, _ string, versionID int64) error {
	p.dataPulls = append(p.dataPulls, versionID)
	return p.dataErr
}

// existenceBackfillPlane wires a cursor plane with a metadata stub.
func existenceBackfillPlane(puller VersionPuller, existence VersionExistenceChecker) *LocalDataPlane {
	dp := newCursorPlane(puller)
	dp.versionExists = existence
	return dp
}

// §6.4: a CONFIRMED-deleted version in the gap cannot be transferred version by
// version, so the node takes a full-state transfer and moves its cursor straight to it.
func TestLocalDataPlane_DeletedVersionTriggersFullStateTransfer(t *testing.T) {
	puller := &snapshotPuller{}
	// v3 is gone. The snapshot is versionID (5), NOT versionID-1 (4) — see the
	// deleted-version test below for why that distinction matters.
	existence := &stubVersionExistence{exists: map[int64]bool{3: false, 5: true}}
	dp := existenceBackfillPlane(puller, existence)
	dp.advanceLocalVersion("kb-1", 2)

	if err := dp.backfillTo(context.Background(), "peer:7001", "kb-1", 5); err != nil {
		t.Fatalf("backfillTo: %v", err)
	}

	// The deleted version was never fetched: the check runs before the pull, so a
	// version we know is gone costs no transfer. This is also the observable that
	// proves the full-state path ran rather than the version-by-version one.
	if len(puller.versionPulls) != 0 {
		t.Errorf("version pulls = %v, want none (a confirmed-deleted version is not fetched)", puller.versionPulls)
	}
	if len(puller.dataPulls) != 1 || puller.dataPulls[0] != 5 {
		t.Errorf("full-state pulls = %v, want [5] (the version being applied)", puller.dataPulls)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 5 {
		t.Errorf("localVersion = %d, want 5 (the cursor moves to the snapshot)", got)
	}
	// One metadata read for the whole gap, not one per version.
	if existence.calls != 1 {
		t.Errorf("metadata reads = %d, want 1 (the set is read once, not per version)", existence.calls)
	}
}

// The case that forced the snapshot rule: the gap is unfillable BECAUSE
// versionID-1 was deleted. Taking versionID-1 as the snapshot would name a version
// known to be gone — the transfer would fail exactly when this path is needed.
func TestLocalDataPlane_SnapshotIsTheAppliedVersionNotTheDeletedOne(t *testing.T) {
	puller := &snapshotPuller{}
	// v4 — the last version in the gap — is the deleted one.
	existence := &stubVersionExistence{exists: map[int64]bool{4: false, 5: true}}
	dp := existenceBackfillPlane(puller, existence)
	dp.advanceLocalVersion("kb-1", 3)

	if err := dp.backfillTo(context.Background(), "peer:7001", "kb-1", 5); err != nil {
		t.Fatalf("backfillTo: %v", err)
	}
	if len(puller.dataPulls) != 1 || puller.dataPulls[0] != 5 {
		t.Errorf("full-state pulls = %v, want [5] — never the version we know is deleted (4)", puller.dataPulls)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 5 {
		t.Errorf("localVersion = %d, want 5", got)
	}
}

// If even the full-state transfer cannot run, the failure must name the deleted
// version — that is what an operator needs to see.
func TestLocalDataPlane_DeletedVersionAndFailedSnapshotReportsBoth(t *testing.T) {
	puller := &snapshotPuller{dataErr: errors.New("peer refused the full-state transfer")}
	existence := &stubVersionExistence{exists: map[int64]bool{2: false}}
	dp := existenceBackfillPlane(puller, existence)
	dp.advanceLocalVersion("kb-1", 1)

	err := dp.backfillTo(context.Background(), "peer:7001", "kb-1", 3)
	if err == nil {
		t.Fatal("backfillTo must fail when even the full-state transfer cannot run")
	}
	msg := err.Error()
	if !strings.Contains(msg, "deleted") {
		t.Errorf("error must say a version was deleted: %v", err)
	}
	if !strings.Contains(msg, "full-state transfer") {
		t.Errorf("error must say the full-state transfer was attempted: %v", err)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 1 {
		t.Errorf("localVersion = %d, want 1 (unchanged: nothing was obtained)", got)
	}
}

// An empty version is legal and must still advance the cursor — otherwise the check
// would turn every empty version into a deleted one.
func TestLocalDataPlane_EmptyButExistingVersionStillAdvances(t *testing.T) {
	puller := &snapshotPuller{}
	existence := &stubVersionExistence{exists: map[int64]bool{3: true, 4: true}}
	dp := existenceBackfillPlane(puller, existence)
	dp.advanceLocalVersion("kb-1", 2)

	if err := dp.backfillTo(context.Background(), "peer:7001", "kb-1", 5); err != nil {
		t.Fatalf("backfillTo: %v", err)
	}
	if len(puller.dataPulls) != 0 {
		t.Errorf("full-state pulls = %v, want none: nothing was deleted", puller.dataPulls)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 4 {
		t.Errorf("localVersion = %d, want 4", got)
	}
	// Two versions in the gap, still ONE metadata read.
	if existence.calls != 1 {
		t.Errorf("metadata reads = %d, want 1", existence.calls)
	}
}

// A metadata read failure must NOT be read as "the version is gone" — unknown stops
// the backfill, it does not trigger a skip.
func TestLocalDataPlane_ExistenceCheckFailureStopsTheBackfill(t *testing.T) {
	puller := &snapshotPuller{}
	existence := &stubVersionExistence{err: errors.New("metadata unavailable")}
	dp := existenceBackfillPlane(puller, existence)
	dp.advanceLocalVersion("kb-1", 2)

	if err := dp.backfillTo(context.Background(), "peer:7001", "kb-1", 4); err == nil {
		t.Fatal("a metadata failure must stop the backfill")
	}
	if len(puller.dataPulls) != 0 {
		t.Errorf("full-state pulls = %v, want none: an unknown is not a deletion", puller.dataPulls)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 2 {
		t.Errorf("localVersion = %d, want 2", got)
	}
}

// A transport failure is likewise not a deletion: the original invariant (abort the
// apply rather than skip silently) stays intact.
func TestLocalDataPlane_PullFailureStillAbortsRatherThanSkipping(t *testing.T) {
	puller := &snapshotPuller{versionErr: map[int64]error{2: errors.New("pull failed")}}
	existence := &stubVersionExistence{exists: map[int64]bool{2: true, 3: true}}
	dp := existenceBackfillPlane(puller, existence)
	dp.advanceLocalVersion("kb-1", 1)

	if err := dp.backfillTo(context.Background(), "peer:7001", "kb-1", 3); err == nil {
		t.Fatal("a failed pull must abort the apply")
	}
	if len(puller.dataPulls) != 0 {
		t.Errorf("full-state pulls = %v, want none: a transport failure is not a deletion", puller.dataPulls)
	}
}

// With no checker wired the plane keeps its previous behaviour: this is what keeps
// every existing deployment and test stack working unchanged.
func TestLocalDataPlane_NoExistenceCheckerKeepsOldBehaviour(t *testing.T) {
	puller := &snapshotPuller{}
	dp := newCursorPlane(puller) // versionExists is nil
	dp.advanceLocalVersion("kb-1", 2)

	if err := dp.backfillTo(context.Background(), "peer:7001", "kb-1", 4); err != nil {
		t.Fatalf("backfillTo: %v", err)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 3 {
		t.Errorf("localVersion = %d, want 3 (unchanged behaviour)", got)
	}
	if len(puller.dataPulls) != 0 {
		t.Errorf("full-state pulls = %v, want none", puller.dataPulls)
	}
}
