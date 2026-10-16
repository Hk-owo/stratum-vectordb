package nodestate

import "testing"

// The wire names are a contract: service/admin.go reports NodeState.String()
// and VersionState.String() verbatim, so a change here silently changes what an
// operator reads. This test is what makes such a change deliberate.
func TestStates_StringSpellings(t *testing.T) {
	node := map[NodeState]string{
		NodeStarting:    "STARTING",
		NodeReconciling: "RECONCILING",
		NodeServing:     "SERVING",
		NodeStopping:    "STOPPING",
	}
	for state, want := range node {
		if got := state.String(); got != want {
			t.Errorf("NodeState(%d).String() = %q, want %q", state, got, want)
		}
	}

	version := map[VersionState]string{
		VersionAbsent:      "ABSENT",
		VersionDataPresent: "DATA_PRESENT",
		VersionBuilding:    "BUILDING",
		VersionReady:       "READY",
		VersionBuildFailed: "BUILD_FAILED",
	}
	for state, want := range version {
		if got := state.String(); got != want {
			t.Errorf("VersionState(%d).String() = %q, want %q", state, got, want)
		}
	}

	redundancy := map[Redundancy]string{
		RedundancyUnknown:     "UNKNOWN",
		RedundancyHealthy:     "HEALTHY",
		RedundancyDegraded:    "DEGRADED",
		RedundancyUnavailable: "UNAVAILABLE",
	}
	for state, want := range redundancy {
		if got := state.String(); got != want {
			t.Errorf("Redundancy(%d).String() = %q, want %q", state, got, want)
		}
	}
}

// Unknown values are reported as UNKNOWN rather than as the zero value's name.
// A caller that somehow receives a value this package never produced must not
// be told it is STARTING (or ABSENT) — that is a plausible-looking lie.
func TestStates_UnknownValueIsNotMistakenForTheZero(t *testing.T) {
	if got := NodeState(99).String(); got != "UNKNOWN" {
		t.Errorf("NodeState(99).String() = %q, want UNKNOWN", got)
	}
	if got := VersionState(99).String(); got != "UNKNOWN" {
		t.Errorf("VersionState(99).String() = %q, want UNKNOWN", got)
	}
}

// TestNodeState_TransitionTable pins the whole table, both directions. The
// interesting entries are the refusals: a node that reached SERVING must not
// slide back into RECONCILING, because reconciling again is work the reconciler
// does on its own schedule — claiming the phase would claim the node stopped
// serving.
func TestNodeState_TransitionTable(t *testing.T) {
	all := []NodeState{NodeStarting, NodeReconciling, NodeServing, NodeStopping}
	legal := map[NodeState]map[NodeState]bool{
		NodeStarting:    {NodeReconciling: true, NodeStopping: true},
		NodeReconciling: {NodeServing: true, NodeStopping: true},
		NodeServing:     {NodeStopping: true},
		NodeStopping:    {},
	}

	for _, from := range all {
		for _, to := range all {
			want := legal[from][to]
			if got := from.canTransitionTo(to); got != want {
				t.Errorf("%s -> %s = %v, want %v", from, to, got, want)
			}
		}
	}
}

// TestVersionState_TransitionTable pins the version layer's table. Three
// refusals carry the design's meaning and are called out by name below.
func TestVersionState_TransitionTable(t *testing.T) {
	all := []VersionState{
		VersionAbsent, VersionDataPresent, VersionBuilding, VersionReady, VersionBuildFailed,
	}
	legal := map[VersionState]map[VersionState]bool{
		VersionAbsent:      {VersionDataPresent: true},
		VersionDataPresent: {VersionBuilding: true},
		VersionBuilding:    {VersionReady: true, VersionBuildFailed: true},
		VersionReady:       {VersionDataPresent: true},
		VersionBuildFailed: {VersionBuilding: true},
	}

	for _, from := range all {
		for _, to := range all {
			want := legal[from][to]
			if got := from.canTransitionTo(to); got != want {
				t.Errorf("%s -> %s = %v, want %v", from, to, got, want)
			}
		}
	}

	// Refusal 1: without data there is nothing to index, so ABSENT cannot jump
	// into the build path.
	if VersionAbsent.canTransitionTo(VersionBuilding) || VersionAbsent.canTransitionTo(VersionReady) {
		t.Error("ABSENT must not reach BUILDING or READY: data comes first")
	}
	// Refusal 2: DATA_PRESENT -> READY would mean "not building is the same as
	// built".
	if VersionDataPresent.canTransitionTo(VersionReady) {
		t.Error("DATA_PRESENT -> READY must be refused: BUILDING is not optional")
	}
	// Refusal 3: rebuilding a READY version goes through DATA_PRESENT first, so
	// the artifact's disappearance is recorded rather than overwritten.
	if VersionReady.canTransitionTo(VersionBuilding) {
		t.Error("READY -> BUILDING must be refused: a rebuild passes through DATA_PRESENT")
	}
	// And the backwards edges that are real, kept explicit so nobody "tidies"
	// them away.
	if !VersionReady.canTransitionTo(VersionDataPresent) {
		t.Error("READY -> DATA_PRESENT must be legal: the retention policy drops artifacts")
	}
	if !VersionBuildFailed.canTransitionTo(VersionBuilding) {
		t.Error("BUILD_FAILED -> BUILDING must be legal: that is a retry")
	}
}
