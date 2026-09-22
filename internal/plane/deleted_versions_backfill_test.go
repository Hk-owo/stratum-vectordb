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
type stubDeletedVersions struct {
	// deleted lists the version ids the metadata handed out and no longer has.
	deleted []int64
	// lastAllocated overrides the allocation bound; 0 means "derive it from the range and
	// the removed ids".
	lastAllocated int64
	err           error
	calls         int
	// gotFrom / gotTo record the range asked about, so a test can pin "it asked about
	// its gap" rather than merely "it asked".
	gotFrom, gotTo int64
}

func (s *stubDeletedVersions) VersionLiveness(_ context.Context, _ string, fromExclusive, toInclusive *int64) ([]int64, int64, error) {
	s.calls++
	if fromExclusive != nil {
		s.gotFrom = *fromExclusive
	}
	if toInclusive != nil {
		s.gotTo = *toInclusive
	}
	if s.err != nil {
		return nil, 0, s.err
	}
	bound := s.lastAllocated
	for _, id := range s.deleted {
		if id > bound {
			bound = id
		}
	}
	if toInclusive != nil && *toInclusive > bound {
		bound = *toInclusive
	}
	removed := make(map[int64]bool, len(s.deleted))
	for _, id := range s.deleted {
		removed[id] = true
	}
	// Narrowed like the real one: a stub answering everything would let a caller
	// read outside its gap without any test noticing.
	alive := make([]int64, 0, bound)
	for id := int64(1); id <= bound; id++ {
		if fromExclusive != nil && id <= *fromExclusive {
			continue
		}
		if toInclusive != nil && id > *toInclusive {
			break
		}
		if !removed[id] {
			alive = append(alive, id)
		}
	}
	return alive, bound, nil
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
func deletedBackfillPlane(puller VersionPuller, deleted VersionLivenessLister) *LocalDataPlane {
	dp := newCursorPlane(puller)
	dp.liveness = deleted
	return dp
}

// §6.4: a CONFIRMED-deleted version in the gap cannot be transferred version by
// version, so the node takes a full-state transfer and moves its cursor straight to it.
func TestLocalDataPlane_DeletedVersionTriggersFullStateTransfer(t *testing.T) {
	puller := &snapshotPuller{}
	// v3 is gone. The snapshot is versionID (5), NOT versionID-1 (4) — see the
	// deleted-version test below for why that distinction matters.
	deleted := &stubDeletedVersions{deleted: []int64{3}}
	dp := deletedBackfillPlane(puller, deleted)
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
	if deleted.calls != 1 {
		t.Errorf("metadata reads = %d, want 1 (the set is read once, not per version)", deleted.calls)
	}
}

// The case that forced the snapshot rule: the gap is unfillable BECAUSE
// versionID-1 was deleted. Taking versionID-1 as the snapshot would name a version
// known to be gone — the transfer would fail exactly when this path is needed.
func TestLocalDataPlane_SnapshotIsTheAppliedVersionNotTheDeletedOne(t *testing.T) {
	puller := &snapshotPuller{}
	// v4 — the last version in the gap — is the deleted one.
	deleted := &stubDeletedVersions{deleted: []int64{4}}
	dp := deletedBackfillPlane(puller, deleted)
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
	deleted := &stubDeletedVersions{deleted: []int64{2}}
	dp := deletedBackfillPlane(puller, deleted)
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
	deleted := &stubDeletedVersions{deleted: []int64{}}
	dp := deletedBackfillPlane(puller, deleted)
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
	if deleted.calls != 1 {
		t.Errorf("metadata reads = %d, want 1", deleted.calls)
	}
}

// A metadata read failure must NOT be read as "the version is gone" — unknown stops
// the backfill, it does not trigger a skip.
func TestLocalDataPlane_ExistenceCheckFailureStopsTheBackfill(t *testing.T) {
	puller := &snapshotPuller{}
	deleted := &stubDeletedVersions{err: errors.New("metadata unavailable")}
	dp := deletedBackfillPlane(puller, deleted)
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
	deleted := &stubDeletedVersions{deleted: []int64{}}
	dp := deletedBackfillPlane(puller, deleted)
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
