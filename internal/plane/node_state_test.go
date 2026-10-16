package plane

import (
	"context"
	"testing"
	"time"

	"stratum/internal/nodestate"
)

// TestLocalDataPlane_ReportsVersionEventsToTheStateMachine pins the wiring in
// docs/storage-node-state-machine-plan.md §7: the version-layer events only this
// layer can observe reach the machine — and, just as importantly, the two "the
// cursor moved" paths report OPPOSITE facts. A landing and a version that will
// never arrive both advance the cursor, and telling them apart is the whole
// reason the machine hears about either.
func TestLocalDataPlane_ReportsVersionEventsToTheStateMachine(t *testing.T) {
	m := nodestate.New(nodestate.Options{})
	dp := NewLocalDataPlane(LocalDataPlaneConfig{State: m})

	// A version whose records landed: the data is here, nothing is built yet.
	dp.MarkVersionContiguous("kb-1", 1)
	if got := m.VersionState("kb-1", 1); got != nodestate.VersionDataPresent {
		t.Fatalf("after a landing = %s, want DATA_PRESENT", got)
	}

	// Scheduling a build from HERE must leave the version layer alone: the index
	// manager owns that transition (docs/…plan.md §7 item 4), because it is the
	// single entry point every build passes through — a second reporter would be
	// exactly the two-sources-for-one-fact problem the machine exists to end.
	//
	// ensureOwnIndex is the plane's build entry point (§8.4/§8.6b), and it must
	// stay as silent about the state machine as scheduleIndexBuild was.
	_ = dp.ensureOwnIndex(context.Background(), "kb-1", 1)
	if got := m.VersionState("kb-1", 1); got != nodestate.VersionDataPresent {
		t.Fatalf("after the plane scheduled a build = %s, want DATA_PRESENT (the plane does not report builds)", got)
	}

	// The reclaim path steps the cursor over the same version for the opposite
	// reason — the version will never arrive — and that must not be recorded as
	// a landing.
	dp.cursorStepsPast("kb-1", 1)
	if got := m.VersionState("kb-1", 1); got != nodestate.VersionAbsent {
		t.Fatalf("after the cursor stepped past = %s, want ABSENT", got)
	}

	// And the cursor really did move: the reclaim path is not a no-op on the
	// cursor either, so the two sides stay consistent.
	if got := dp.LocalVersionOf("kb-1"); got != 1 {
		t.Fatalf("cursor = %d, want 1", got)
	}
}

// TestLocalDataPlane_StateMachineIsOptional: without one the events are simply
// not recorded. That is the behaviour before the machine existed, and the
// default in tests and in single-node deployments that wire nothing.
func TestLocalDataPlane_StateMachineIsOptional(t *testing.T) {
	dp := NewLocalDataPlane(LocalDataPlaneConfig{})

	dp.MarkVersionContiguous("kb-1", 1)
	_ = dp.ensureOwnIndex(context.Background(), "kb-1", 1)
	dp.cursorStepsPast("kb-1", 1)

	if got := dp.LocalVersionOf("kb-1"); got != 1 {
		t.Fatalf("cursor = %d, want 1 — the cursor must move with or without a state machine", got)
	}
}

// TestLagCatchup_DeclaresCatchUpToTheStateMachine: a knowledge base that starts
// catching up is declared WHILE it runs, and the declaration is cleared when it
// finishes. Without the clear, a node would report "catching up" forever after
// its first catch-up — a status line that stops meaning anything — and the
// component would never pick that knowledge base up again, since it skips what
// is already declared.
func TestLagCatchup_DeclaresCatchUpToTheStateMachine(t *testing.T) {
	m := nodestate.New(nodestate.Options{})
	done := make(chan struct{})

	l := NewLagCatchup(LagCatchupConfig{
		MinLagVersions: 1,
		Ensure: func(_ context.Context, kbID string, _ int64) error {
			if !m.IsCatchingUp(kbID) {
				t.Errorf("%s ran its catch-up without being declared as catching up", kbID)
			}
			close(done)
			return nil
		},
		Cursor:    func() map[string]int64 { return map[string]int64{"kb-behind": 3, "kb-level": 9} },
		NodeState: m,
	})

	l.SetChainTails(map[string]int64{"kb-behind": 9, "kb-level": 9})

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the knowledge base behind the tail was never picked up")
	}

	// The clear happens after Ensure returns, so wait for it.
	deadline := time.Now().Add(2 * time.Second)
	for m.IsCatchingUp("kb-behind") {
		if time.Now().After(deadline) {
			t.Fatal("the catch-up declaration was never cleared")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// And only the lagging one was ever declared.
	if got := m.CatchingUp(); len(got) != 0 {
		t.Fatalf("CatchingUp = %v, want none", got)
	}
}

// TestLagCatchup_DeclaresNothingWhenNothingIsBehind: a report that finds every
// knowledge base level declares no catch-up at all, rather than declaring and
// immediately clearing one per knowledge base.
func TestLagCatchup_DeclaresNothingWhenNothingIsBehind(t *testing.T) {
	m := nodestate.New(nodestate.Options{})
	l := NewLagCatchup(LagCatchupConfig{
		MinLagVersions: 1,
		Ensure:         func(context.Context, string, int64) error { return nil },
		Cursor:         func() map[string]int64 { return map[string]int64{"kb-level": 9} },
		NodeState:      m,
	})

	l.SetChainTails(map[string]int64{"kb-level": 9})
	time.Sleep(50 * time.Millisecond)

	if got := m.CatchingUp(); len(got) != 0 {
		t.Fatalf("CatchingUp = %v, want none", got)
	}
}

// TestLocalHealthView_StateIsThreeValued: "no evidence" and "evidence it is up"
// are different answers. A caller that has to COUNT reachable replicas cannot
// treat a peer it has never called as one it reached.
func TestLocalHealthView_StateIsThreeValued(t *testing.T) {
	now := time.Now()
	v := NewLocalHealthView(HealthViewConfig{
		DemoteAfter: 3,
		DemoteFor:   30 * time.Second,
		Now:         func() time.Time { return now },
	})

	if got := v.State("peer-a"); got != PeerUnknown {
		t.Fatalf("a peer never called = %v, want PeerUnknown", got)
	}

	v.Observe("peer-a", true)
	if got := v.State("peer-a"); got != PeerReachable {
		t.Fatalf("after a success = %v, want PeerReachable", got)
	}

	v.Observe("peer-b", false)
	v.Observe("peer-b", false)
	if got := v.State("peer-b"); got != PeerReachable {
		t.Fatalf("two failures = %v, want PeerReachable (below the threshold)", got)
	}
	v.Observe("peer-b", false) // the third is what §7.13.5 acts on
	if got := v.State("peer-b"); got != PeerDemoted {
		t.Fatalf("three failures = %v, want PeerDemoted", got)
	}

	// The expiry is Rank's, from the same clock: a demotion that has outlived
	// demoteFor must stop being reported here too, or the two would disagree
	// about one peer at one moment.
	now = now.Add(31 * time.Second)
	if got := v.State("peer-b"); got != PeerReachable {
		t.Fatalf("after the demotion expired = %v, want PeerReachable", got)
	}
}

// TestCoordinatorDispatcher_RedundancyComesFromLocalObservations pins the whole
// verdict: it is derived from the peers this node reached, it stays UNKNOWN
// while any required peer has never been observed, and it needs only a quorum
// rather than everyone.
func TestCoordinatorDispatcher_RedundancyComesFromLocalObservations(t *testing.T) {
	type verdict struct {
		known         bool
		live, quorum  int
		detailIsEmpty bool
	}
	var got []verdict
	view := NewLocalHealthView(HealthViewConfig{DemoteAfter: 3})
	d := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{
		SelfAddr: "self",
		Health:   view,
		Redundancy: func(known bool, live, quorum int, detail string) {
			got = append(got, verdict{known, live, quorum, detail == ""})
		},
	})

	// Nothing observed yet. This node itself counts as reached, so it is two of
	// three short — but the sample is incomplete, which is the answer that
	// matters.
	d.reportRedundancy([]string{"peer-a", "peer-b", "self"})

	// One peer reached, the other still never called: still not a verdict.
	view.Observe("peer-a", true)
	d.reportRedundancy([]string{"peer-a", "peer-b", "self"})

	// Every required peer observed, and two of three live is a quorum.
	view.Observe("peer-b", true)
	d.reportRedundancy([]string{"peer-a", "peer-b", "self"})

	// A peer demoted out of three still leaves a quorum (self + one).
	for i := 0; i < 3; i++ {
		view.Observe("peer-b", false)
	}
	d.reportRedundancy([]string{"peer-a", "peer-b", "self"})

	// With no peer left, the node is below quorum — DEGRADED, not UNAVAILABLE,
	// because this node itself is still serving.
	for i := 0; i < 3; i++ {
		view.Observe("peer-a", false)
	}
	d.reportRedundancy([]string{"peer-a", "peer-b", "self"})

	if len(got) != 5 {
		t.Fatalf("verdicts = %d, want 5", len(got))
	}
	want := []verdict{
		{known: false, live: 1, quorum: 2},
		{known: false, live: 2, quorum: 2},
		{known: true, live: 3, quorum: 2, detailIsEmpty: true},
		{known: true, live: 2, quorum: 2, detailIsEmpty: true},
		{known: true, live: 1, quorum: 2},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("verdict %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestCoordinatorDispatcher_RedundancyWithoutSelfIsUnavailable: a node that does
// not count itself (no local write wired, so it is not a candidate) can see
// every peer demoted, which is the UNAVAILABLE reading — "not one required
// replica is live".
func TestCoordinatorDispatcher_RedundancyWithoutSelfIsUnavailable(t *testing.T) {
	var known bool
	var live, quorum int
	view := NewLocalHealthView(HealthViewConfig{DemoteAfter: 1})
	view.Observe("peer-a", false)

	d := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{
		SelfAddr: "self",
		Health:   view,
		Redundancy: func(k bool, l, q int, _ string) {
			known, live, quorum = k, l, q
		},
	})

	d.reportRedundancy([]string{"peer-a"})

	if !known {
		t.Fatal("known = false, want a verdict: the one required peer WAS observed")
	}
	if live != 0 || quorum != 1 {
		t.Fatalf("live/quorum = %d/%d, want 0/1", live, quorum)
	}
}

// TestCoordinatorDispatcher_RedundancyIsOptional: without a sink nothing is
// reported, which is the default in tests and in a deployment that wires no
// state machine.
func TestCoordinatorDispatcher_RedundancyIsOptional(t *testing.T) {
	d := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{SelfAddr: "self"})
	d.reportRedundancy([]string{"peer-a", "self"})
}
