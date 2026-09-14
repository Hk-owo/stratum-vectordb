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
