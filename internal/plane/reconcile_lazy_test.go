package plane

import (
	"context"
	"testing"

	"stratum/internal/types"
)

// TestReconcileIndexes_RebuildsOnlyPendingAndActive pins the policy reconcile must
// follow INSIDE the retention window — the same policy the branch just below the
// window already follows.
//
// Leaving an artifact absent is always a valid answer, because EnsureIndex rebuilds
// on demand the moment anyone actually asks for the version. Reconcile's job is to
// give a HEAD START to the versions that will certainly be asked for:
//
//   - PENDING — the writer is waiting for this build; without it the write stalls.
//   - the active version — every query for that knowledge base hits it.
//
// Everything else that is merely missing can wait. The old behaviour (rebuild every
// missing artifact inside the window) contradicted the lazy-build rule and, after a
// restart over a populated volume, produced a rebuild storm that starved a fresh
// write of its build: 601 seconds with no READY, logged as
// "(re)building missing index" once per historical version.
func TestReconcileIndexes_RebuildsOnlyPendingAndActive(t *testing.T) {
	meta := &stubMeta{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1", ActiveVersionID: 3}},
		versions: map[string][]types.VersionMeta{
			"kb-1": {
				// PENDING and missing: the writer is waiting on exactly this.
				{VersionID: 1, KBID: "kb-1", IndexStatus: types.IndexStatusPending},
				// READY and missing, but NOT active: leave it absent. Whoever wants
				// it triggers the build through EnsureIndex.
				{VersionID: 2, KBID: "kb-1", IndexStatus: types.IndexStatusReady},
				// ACTIVE, READY, missing: worth a head start, every query hits it.
				{VersionID: 3, KBID: "kb-1", IndexStatus: types.IndexStatusReady},
				// READY, missing, not active — same as v2.
				{VersionID: 4, KBID: "kb-1", IndexStatus: types.IndexStatusReady},
			},
		},
	}
	store := &stubIndexStore{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: store})

	if _, err := dp.ReconcileIndexes(context.Background(), meta, 0); err != nil {
		t.Fatalf("ReconcileIndexes: %v", err)
	}

	triggered := map[int64]bool{}
	for _, id := range store.triggered {
		triggered[id] = true
	}
	if !triggered[1] {
		t.Errorf("triggered = %v, want v1 — it is PENDING and the writer is waiting", store.triggered)
	}
	if !triggered[3] {
		t.Errorf("triggered = %v, want v3 — it is the active version", store.triggered)
	}
	if triggered[2] || triggered[4] {
		t.Errorf("triggered = %v, want neither v2 nor v4: a missing non-active READY "+
			"artifact is rebuilt on demand, not eagerly at reconcile", store.triggered)
	}
}

// TestReconcileIndexes_DoesNotRebuildInactiveReadyAfterRestart is the scenario from
// the pressure test, reduced to its essentials: a knowledge base whose history lost
// every artifact, plus one knowledge base being written to right now. Reconcile must
// not bury the live build under the backlog — which is what happens when it treats
// "missing" as "rebuild me" for the whole history at once.
func TestReconcileIndexes_DoesNotRebuildInactiveReadyAfterRestart(t *testing.T) {
	history := make([]types.VersionMeta, 0, 40)
	for id := int64(1); id <= 40; id++ {
		history = append(history, types.VersionMeta{
			VersionID: id, KBID: "kb-old", IndexStatus: types.IndexStatusReady,
		})
	}
	meta := &stubMeta{
		kbs: []types.KnowledgeBaseMeta{
			{KBID: "kb-old", ActiveVersionID: 40}, // serving the newest version of an old KB
			{KBID: "kb-new"},                      // no active version yet: being created
		},
		versions: map[string][]types.VersionMeta{
			"kb-old": history,
			"kb-new": {{VersionID: 41, KBID: "kb-new", IndexStatus: types.IndexStatusPending}},
		},
	}
	store := &stubIndexStore{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: store})

	if _, err := dp.ReconcileIndexes(context.Background(), meta, 0); err != nil {
		t.Fatalf("ReconcileIndexes: %v", err)
	}

	// Two rebuilds' worth of work: the active version of the old KB and the
	// pending write. Thirty-nine historical artifacts stay absent.
	if len(store.triggered) != 2 {
		t.Fatalf("triggered %d builds (%v), want exactly 2 — the active version of "+
			"kb-old and the PENDING write to kb-new", len(store.triggered), store.triggered)
	}
	triggered := map[int64]bool{}
	for _, id := range store.triggered {
		triggered[id] = true
	}
	if !triggered[40] || !triggered[41] {
		t.Errorf("triggered = %v, want v40 (active) and v41 (pending)", store.triggered)
	}
}

// The retention window is counted over artifacts ON DISK, not over the control
// layer's version set — EnforceDiskRetention works on the `.index` files it
// finds, and a version that never produced one here is not in its window either.
//
// The distinction is not cosmetic. The retention branch runs BEFORE the
// PENDING/active branch, so a version misclassified as "dropped by retention" is
// a version reconcile refuses to rebuild — and that can be a PENDING write, or a
// rolled-back active version that every query lands on, both of which
// EnforceDiskRetention shields on disk.
//
// Here the set holds versions 1..10, only 8..10 ever produced an artifact, and the
// policy keeps 3. Counted over the version set the window would start at version 8
// — so the PENDING v2 would look "dropped" and be skipped. Counted over what is on
// disk there is nothing to drop (the three artifacts fit inside the window), and
// the PENDING write keeps its head start.
func TestReconcileIndexes_WindowFollowsArtifactsNotTheVersionSet(t *testing.T) {
	versions := make([]types.VersionMeta, 0, 10)
	for id := int64(1); id <= 10; id++ {
		status := types.IndexStatusReady
		if id == 2 {
			status = types.IndexStatusPending // the writer is waiting on exactly this
		}
		versions = append(versions, types.VersionMeta{VersionID: id, KBID: "kb-1", IndexStatus: status})
	}
	meta := &stubMeta{
		kbs:      []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
		versions: map[string][]types.VersionMeta{"kb-1": versions},
	}
	// Only 8..10 are on disk; 1..7 never had an artifact here.
	store := &stubIndexStore{exists: map[int64]bool{8: true, 9: true, 10: true}}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: store})

	// Guard the premise: counted over the version set, the window would start at
	// version 8 and the PENDING v2 would be skipped as "retention-dropped". If
	// that stops holding, the assertions below stop proving anything.
	versionIDs := make([]int64, 0, len(versions))
	for _, v := range versions {
		versionIDs = append(versionIDs, v.VersionID)
	}
	if old := retentionCutoffOf(versionIDs, 3); old != 8 {
		t.Fatalf("premise changed: counting the window over the version set gives cutoff %d, want 8", old)
	}

	durable, err := dp.ReconcileIndexes(context.Background(), meta, 3)
	if err != nil {
		t.Fatalf("ReconcileIndexes: %v", err)
	}
	if len(durable) != 3 {
		t.Fatalf("durable = %v, want exactly the three artifacts on disk", durable)
	}

	if len(store.triggered) != 1 || store.triggered[0] != 2 {
		t.Errorf("triggered = %v, want only v2: it is PENDING, and once the window is "+
			"counted over on-disk artifacts there is nothing below it to call dropped", store.triggered)
	}
}

// retentionCutoffOf counts files, which is why the same retentionCount yields
// different answers for different on-disk sets.
func TestRetentionCutoffOf(t *testing.T) {
	cases := []struct {
		name   string
		onDisk []int64
		keep   int
		want   int64
	}{
		{"policy off", []int64{1, 2, 3}, 0, -1},
		{"nothing on disk", nil, 3, -1},
		{"fewer artifacts than the window keeps", []int64{8, 9, 10}, 3, -1},
		{"window starts inside the on-disk set", []int64{1, 2, 3, 4, 5}, 2, 4},
		{"unsorted input", []int64{5, 1, 4, 2, 3}, 2, 4},
		{"single artifact kept", []int64{1, 2, 3}, 1, 3},
	}
	for _, tc := range cases {
		if got := retentionCutoffOf(tc.onDisk, tc.keep); got != tc.want {
			t.Errorf("%s: retentionCutoffOf(%v, %d) = %d, want %d",
				tc.name, tc.onDisk, tc.keep, got, tc.want)
		}
	}
}

// A version under deletion is not reconciled at all: not reported durable, not given a
// build. The deletion flow owns its bytes, and its metadata is what names the work, so
// there is nothing here to repair — while building it would compete with the reclaim for
// the same artifact and could re-report READY for a version about to disappear.
//
// It is deliberately NOT a "skip because failed" case: the version is perfectly healthy,
// which is exactly why it would otherwise qualify (v1 is the active version, v2 is
// PENDING — the two kinds reconcile gives a head start to).
func TestReconcileIndexes_SkipsVersionsUnderDeletion(t *testing.T) {
	meta := &stubMeta{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1", ActiveVersionID: 1}},
		versions: map[string][]types.VersionMeta{
			"kb-1": {
				// ACTIVE and READY: every other rule in this file would give it a head start.
				{VersionID: 1, KBID: "kb-1", IndexStatus: types.IndexStatusReady, Deleting: true},
				// PENDING and missing: the writer-waiting case — but the version is going away.
				{VersionID: 2, KBID: "kb-1", IndexStatus: types.IndexStatusPending, Deleting: true},
				// The control: PENDING, alive, so reconcile must still schedule it.
				{VersionID: 3, KBID: "kb-1", IndexStatus: types.IndexStatusPending},
			},
		},
	}
	// v1's artifact is on disk; a Deleting version must not be reported durable from it.
	store := &stubIndexStore{exists: map[int64]bool{1: true}}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: store})

	durable, err := dp.ReconcileIndexes(context.Background(), meta, 0)
	if err != nil {
		t.Fatalf("ReconcileIndexes: %v", err)
	}

	if len(store.triggered) != 1 || store.triggered[0] != 3 {
		t.Fatalf("triggered = %v, want only [3]: a version under deletion gets no head start", store.triggered)
	}
	for _, ref := range durable {
		if ref.VersionID == 1 || ref.VersionID == 2 {
			t.Fatalf("durable = %+v, want no version under deletion — reporting READY for one "+
				"contradicts the deletion that is removing it", durable)
		}
	}
}
