package plane

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"stratum/internal/types"
)

// stubLocalVersions is the local version list a reconciler enumerates.
type stubLocalVersions struct {
	versions map[string][]int64
	err      error
}

func (s *stubLocalVersions) ListVersions(_ context.Context, kbID string) ([]int64, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.versions[kbID], nil
}

var _ LocalVersionLister = (*stubLocalVersions)(nil)

// stubDeletions serves the tombstones and records the range it was asked about,
// so a case can pin that the question is narrowed to what this node holds.
type stubDeletions struct {
	deleted map[string][]int64
	err     error
	from    []int64
	to      []int64
}

func (s *stubDeletions) DeletionsInRange(_ context.Context, kbID string, fromExclusive, toInclusive int64) ([]int64, error) {
	s.from = append(s.from, fromExclusive)
	s.to = append(s.to, toInclusive)
	if s.err != nil {
		return nil, s.err
	}
	var out []int64
	for _, id := range s.deleted[kbID] {
		if id > fromExclusive && id <= toInclusive {
			out = append(out, id)
		}
	}
	return out, nil
}

var _ DeletionLister = (*stubDeletions)(nil)

// failingOnceDropper fails its first call and succeeds afterwards, so a case can
// pin that one stuck reclaim does not stop the rest.
type failingOnceDropper struct {
	dropped []string
	calls   int
}

func (d *failingOnceDropper) DropVersionStorage(_ context.Context, kbID string, versionID int64) error {
	d.calls++
	if d.calls == 1 {
		return errors.New("disk busy")
	}
	d.dropped = append(d.dropped, fmt.Sprintf("%s/%d", kbID, versionID))
	return nil
}

func reconcileFixture(local *stubLocalVersions, del *stubDeletions, dropper VersionDataDropper) *LocalDataPlane {
	return NewLocalDataPlane(LocalDataPlaneConfig{
		LocalVersions: local,
		Tombstones:    del,
		DataDropper:   dropper,
	})
}

func kbList(ids ...string) *stubMeta {
	meta := &stubMeta{}
	for _, id := range ids {
		meta.kbs = append(meta.kbs, types.KnowledgeBaseMeta{KBID: id})
	}
	return meta
}

// TestLocalDataPlane_ReconcileDeletedVersions_ReclaimsWhatTheTombstonesName is the
// happy path: a version this node still holds, which the control layer has recorded
// as deleted, gets reclaimed — and the question put to the tombstones is narrowed
// to the range this node actually covers.
func TestLocalDataPlane_ReconcileDeletedVersions_ReclaimsWhatTheTombstonesName(t *testing.T) {
	local := &stubLocalVersions{versions: map[string][]int64{"kb-1": {1, 2, 3, 9}}}
	del := &stubDeletions{deleted: map[string][]int64{"kb-1": {2}}}
	dropper := &stubDropper{}
	dp := reconcileFixture(local, del, dropper)

	reclaimed, err := dp.ReconcileDeletedVersions(context.Background(), kbList("kb-1"))
	if err != nil {
		t.Fatalf("ReconcileDeletedVersions: %v", err)
	}
	if reclaimed != 1 {
		t.Errorf("reclaimed = %d, want 1", reclaimed)
	}
	if len(dropper.dropped) != 1 || dropper.dropped[0] != "kb-1/2" {
		t.Errorf("drops = %v, want [kb-1/2]", dropper.dropped)
	}
	if len(del.from) != 1 || del.from[0] != 0 || del.to[0] != 9 {
		t.Errorf("tombstone range = (%v, %v], want (0, 9] (this node's own coverage)", del.from, del.to)
	}
}

// A knowledge base this node holds nothing for is skipped without even asking for
// tombstones: there is nothing to reclaim, and the question would be wasted.
func TestLocalDataPlane_ReconcileDeletedVersions_SkipsKnowledgeBasesItHoldsNothingFor(t *testing.T) {
	local := &stubLocalVersions{versions: map[string][]int64{}}
	del := &stubDeletions{deleted: map[string][]int64{"kb-1": {2}}}
	dropper := &stubDropper{}
	dp := reconcileFixture(local, del, dropper)

	if _, err := dp.ReconcileDeletedVersions(context.Background(), kbList("kb-1")); err != nil {
		t.Fatalf("ReconcileDeletedVersions: %v", err)
	}
	if len(del.from) != 0 {
		t.Errorf("tombstones were asked about a knowledge base this node holds nothing for: %v", del.from)
	}
	if len(dropper.dropped) != 0 {
		t.Errorf("drops = %v, want none", dropper.dropped)
	}
}

// "I cannot enumerate my own versions" is not "there is nothing here" — the
// knowledge base is skipped and the caller hears about it, because acting on an
// unreadable list would mean either skipping the cleanup silently or treating
// everything as a leftover.
func TestLocalDataPlane_ReconcileDeletedVersions_SkipsWhenTheLocalListFails(t *testing.T) {
	local := &stubLocalVersions{err: errors.New("pebble closed")}
	del := &stubDeletions{deleted: map[string][]int64{"kb-1": {2}}}
	dropper := &stubDropper{}
	dp := reconcileFixture(local, del, dropper)

	reclaimed, err := dp.ReconcileDeletedVersions(context.Background(), kbList("kb-1"))
	if err == nil {
		t.Fatal("an unreadable local version list must be reported, not silently skipped")
	}
	if reclaimed != 0 || len(dropper.dropped) != 0 {
		t.Errorf("reclaimed = %d, drops = %v, want nothing reclaimed", reclaimed, dropper.dropped)
	}
	if len(del.from) != 0 {
		t.Errorf("tombstones should not be consulted for a knowledge base whose local list failed")
	}
}

// The same for the tombstones: a verdict that could not be read is not a verdict.
// This is the distinction §B exists for, so the case is pinned explicitly.
func TestLocalDataPlane_ReconcileDeletedVersions_SkipsWhenTheTombstonesFail(t *testing.T) {
	local := &stubLocalVersions{versions: map[string][]int64{"kb-1": {1, 2}}}
	del := &stubDeletions{err: errors.New("state machine unavailable")}
	dropper := &stubDropper{}
	dp := reconcileFixture(local, del, dropper)

	reclaimed, err := dp.ReconcileDeletedVersions(context.Background(), kbList("kb-1"))
	if err == nil {
		t.Fatal("unreadable tombstones must be reported, not treated as an empty verdict")
	}
	if reclaimed != 0 || len(dropper.dropped) != 0 {
		t.Errorf("reclaimed = %d, drops = %v, want nothing reclaimed", reclaimed, dropper.dropped)
	}
}

// One stuck reclaim must not stop the others: the delete is idempotent and the
// next pass retries it, but the rest of the work still happens now.
func TestLocalDataPlane_ReconcileDeletedVersions_KeepsGoingAfterAFailedReclaim(t *testing.T) {
	local := &stubLocalVersions{versions: map[string][]int64{"kb-1": {1, 2, 3}}}
	del := &stubDeletions{deleted: map[string][]int64{"kb-1": {2, 3}}}
	dropper := &failingOnceDropper{}
	dp := reconcileFixture(local, del, dropper)

	reclaimed, err := dp.ReconcileDeletedVersions(context.Background(), kbList("kb-1"))
	if err == nil {
		t.Fatal("a failed reclaim must be reported")
	}
	if dropper.calls != 2 {
		t.Errorf("dropper calls = %d, want 2 (the second reclaim must still be attempted)", dropper.calls)
	}
	if reclaimed != 1 || len(dropper.dropped) != 1 {
		t.Errorf("reclaimed = %d, drops = %v, want the second version reclaimed", reclaimed, dropper.dropped)
	}
}

// Unwired is an error, never a silent no-op: a reconciler that quietly does nothing
// is how leftovers survive (the same rule DropVersionData follows).
func TestLocalDataPlane_ReconcileDeletedVersions_ReportsWhenNotWired(t *testing.T) {
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		LocalVersions: &stubLocalVersions{},
		// Tombstones deliberately missing.
		DataDropper: &stubDropper{},
	})
	if _, err := dp.ReconcileDeletedVersions(context.Background(), kbList("kb-1")); err == nil {
		t.Fatal("a reconciler with no tombstone source must report that it cannot run")
	}
}

// TestLocalDataPlane_DeletedVersionLeftovers_ReportsWithoutTouchingStorage: the read
// half must stand on its own, because that is what makes "report now, reclaim only
// when told to" possible.
func TestLocalDataPlane_DeletedVersionLeftovers_ReportsWithoutTouchingStorage(t *testing.T) {
	local := &stubLocalVersions{versions: map[string][]int64{"kb-1": {1, 2, 3}}}
	del := &stubDeletions{deleted: map[string][]int64{"kb-1": {2}}}
	dropper := &stubDropper{}
	dp := reconcileFixture(local, del, dropper)

	leftovers, err := dp.DeletedVersionLeftovers(context.Background(), kbList("kb-1"))
	if err != nil {
		t.Fatalf("DeletedVersionLeftovers: %v", err)
	}
	if len(leftovers) != 1 || len(leftovers["kb-1"]) != 1 || leftovers["kb-1"][0] != 2 {
		t.Fatalf("leftovers = %v, want {kb-1: [2]}", leftovers)
	}
	if len(dropper.dropped) != 0 {
		t.Errorf("the read half touched storage: %v", dropper.dropped)
	}
}

// The scan's failures surface as an error while the map stays empty for the
// knowledge bases it could not judge: a caller reporting the state must not
// mistake "could not read" for "clean".
func TestLocalDataPlane_DeletedVersionLeftovers_SurfacesReadFailures(t *testing.T) {
	local := &stubLocalVersions{versions: map[string][]int64{"kb-1": {1, 2}}}
	del := &stubDeletions{err: errors.New("state machine unavailable")}
	dp := reconcileFixture(local, del, &stubDropper{})

	leftovers, err := dp.DeletedVersionLeftovers(context.Background(), kbList("kb-1"))
	if err == nil {
		t.Fatal("an unreadable tombstone source must be reported")
	}
	if len(leftovers) != 0 {
		t.Errorf("leftovers = %v, want none (the verdict could not be read)", leftovers)
	}
}
