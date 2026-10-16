// Package nodestate is a storage node's explicit state machine: the single
// place that answers "what state is THIS node in, and where is THIS version on
// it".
//
// It exists because the answers were previously computed on demand, in six
// unrelated places (docs/storage-node-state-machine-plan.md §1): the process
// phase was not represented anywhere at all, storage redundancy was re-derived
// from a report aggregate on every call, lag was a comparison repeated at each
// site, and "index build in progress" lived in a private map inside the index
// manager with no way to ask about it. Two consequences followed. There was no
// single answer to "is this node healthy" — it depended on which caller you
// asked, and the answers used different inputs. And the intermediate states
// (reconciling, building) were not states at all, just stretches of control
// flow that nothing could query, assert on, or test.
//
// The machine has two layers, deliberately separate:
//
//   - a NODE layer (NodeState): the mutually exclusive lifecycle phase, plus
//     two orthogonal dimensions that are NOT phases — redundancy (this node's
//     own view of whether enough replicas are live) and activity (which
//     knowledge bases are catching up, how many builds are running). Merging
//     those into the phase enum would create the false implication "catching
//     up => not serving"; a node routinely does both, and the two questions
//     ("is it serving" and "is it behind") both need separate answers.
//   - a VERSION layer (VersionState): per (kbID, versionID), where this node
//     is on the absent -> data-present -> building -> ready path.
//
// Three properties are load-bearing:
//
//   - Transitions are checked. ABSENT -> READY is rejected rather than being
//     merely unreachable: today no code path does that, which is not the same
//     as a guarantee. The one entry point that bypasses the table is
//     RestoreVersion, and it accepts only the two states disk facts can
//     express (see its comment).
//   - It is in memory, and it is rebuilt from disk facts at startup
//     (docs/storage-node-state-machine-plan.md §5.1). Nothing is persisted:
//     a state file would introduce "the file disagrees with the disk" as a new
//     failure mode, and the rebuild already exists (ReconcileIndexes).
//   - It does not leave the node. This is one node's own knowledge, fed by
//     events that already happen here. It is not reported to the control
//     layer, not shared with the service station, and never consensed on —
//     the same rule as docs/coordinator-selection-and-node-liveness-design.md
//     §5.1/§5.4: a health view that needs a maintainer needs an election, and
//     a local view disagreeing with another node's costs nothing.
//
// Nothing here does IO. Every fact is judged by the caller — on the disk, at
// the vecstore, on the wire — and handed over as something that has already
// happened. Holding a lock across IO is what the storage layer's cursor
// comments rule out (internal/plane/local_data_plane.go's cursorMu), and a
// state machine that queried the disk inside a transition would put that back.
package nodestate

import "fmt"

// NodeState is the node layer's mutually exclusive lifecycle phase.
//
// The phase says what the node can be asked to do right now; it deliberately
// says nothing about how well the node is doing (Redundancy) or what it is
// working on (Snapshot.Actions). See the package comment for why those are not
// phases.
type NodeState int

const (
	// NodeStarting: the process is up but the storage stack is not open yet.
	// Nothing in this state can serve or report.
	NodeStarting NodeState = iota

	// NodeReconciling: the storage stack is open and startup reconciliation is
	// running — cursors recovered, indexes reconciled against disk facts, the
	// epoch payload published. This is the window in which the version layer is
	// REBUILT rather than updated (RestoreVersion), and the only window in
	// which a version may be restored without a transition having produced it.
	NodeReconciling

	// NodeServing: reconciliation finished and the gRPC server is accepting
	// work. A node may well be catching up or short a replica while SERVING —
	// those are the orthogonal dimensions, not phases.
	NodeServing

	// NodeStopping: shutdown began. Terminal: a node that stops does not
	// resume, it restarts into a new process (and a fresh machine).
	NodeStopping
)

// String returns a human-readable name. These spellings are the wire names
// (service/admin.go reports them verbatim), so they are part of the contract
// and only ever get added to.
func (s NodeState) String() string {
	switch s {
	case NodeStarting:
		return "STARTING"
	case NodeReconciling:
		return "RECONCILING"
	case NodeServing:
		return "SERVING"
	case NodeStopping:
		return "STOPPING"
	default:
		return "UNKNOWN"
	}
}

// nodeTransitions is the legal phase transitions.
//
// Forward only, except for stopping: a node that reached SERVING does not go
// back to RECONCILING. Re-running reconciliation is not a phase change — the
// work is done by the reconciler on its own schedule, and re-entering the
// phase would claim the node has stopped serving when it has not.
var nodeTransitions = map[NodeState][]NodeState{
	NodeStarting:    {NodeReconciling, NodeStopping},
	NodeReconciling: {NodeServing, NodeStopping},
	NodeServing:     {NodeStopping},
	NodeStopping:    nil,
}

// canTransitionTo reports whether a phase change is legal.
func (s NodeState) canTransitionTo(next NodeState) bool {
	for _, allowed := range nodeTransitions[s] {
		if next == allowed {
			return true
		}
	}
	return false
}

// Redundancy is this node's own view of whether enough of the replicas that
// must hold a version are live — the orthogonal dimension of the node layer.
//
// It is NOT plane.StorageState, and the two must not be merged. That one is
// the control leader's aggregate over every replica's reports, and a node that
// is not the leader answers "unknown" for all of it. This one is what THIS
// node knows, which for a non-leader is less, and the honest encoding of "I
// cannot see the others" is RedundancyUnknown rather than borrowing a global
// verdict it did not compute (docs/storage-node-state-machine-plan.md §3.2).
type Redundancy int

const (
	// RedundancyUnknown: this node has no basis for a verdict — it is not the
	// leader, the topology is not wired, or nothing has reported yet.
	//
	// It is the zero value, and it must never be treated as a problem: the
	// existing degradation signal fails OPEN on exactly this input
	// (docs/storage-degradation-signal-plan.md §3.3), and a write gate that
	// turned "I cannot tell" into a refusal would reject writes that would
	// have succeeded.
	RedundancyUnknown Redundancy = iota

	// RedundancyHealthy: enough replicas are live for a write to reach its
	// durability target.
	RedundancyHealthy

	// RedundancyDegraded: fewer than a quorum are live, but at least one is.
	// Reads are unaffected — a replica holding the data still serves it.
	RedundancyDegraded

	// RedundancyUnavailable: not one required replica is live.
	RedundancyUnavailable
)

// String returns a human-readable name. Like NodeState's, these are wire names.
func (r Redundancy) String() string {
	switch r {
	case RedundancyUnknown:
		return "UNKNOWN"
	case RedundancyHealthy:
		return "HEALTHY"
	case RedundancyDegraded:
		return "DEGRADED"
	case RedundancyUnavailable:
		return "UNAVAILABLE"
	default:
		return "UNKNOWN"
	}
}

// VersionState is the version layer: where one (kbID, versionID) stands ON
// THIS NODE.
//
// It is not types.DataStatus or types.IndexStatus, and the three answer
// different questions (docs/storage-node-state-machine-plan.md §4.2). Those
// two are the control layer's replicated metadata — what the version's Saga
// has reached cluster-wide, identical on every node. This one is a local fact:
// the same version is routinely READY on one replica, BUILDING on the next,
// and DATA_PRESENT on the third, and that is healthy rather than divergent.
//
// Two consequences of the split, both deliberate:
//
//   - The control layer's FAILED_PERMANENT does not appear here. That verdict
//     is the retry budget's conclusion (Stratum_设计文档v13.md §10.1), decided
//     by the control layer; this node knows only that its own build failed.
//   - VersionAbsent means "this node does not have it", NOT "it does not
//     exist". Whether the version exists is the control layer's answer, and
//     §7.5's invariant (a cursor must not step over a version the node is
//     missing) needs that distinction — see
//     docs/storage-node-state-machine-plan.md §7, item 3.
type VersionState int

const (
	// VersionAbsent: nothing of this version is on this node. Also what an
	// unknown key reports, which is correct rather than a default: entries are
	// created when data lands, so "no entry" and "no data" are the same fact
	// and no disk probe is needed to tell them apart.
	VersionAbsent VersionState = iota

	// VersionDataPresent: the data is here, the index is not — never built, or
	// built and then dropped by the disk retention policy ("rebuild on
	// demand").
	VersionDataPresent

	// VersionBuilding: an index build is queued or running.
	VersionBuilding

	// VersionReady: an index is on disk and this node can serve queries for
	// the version.
	VersionReady

	// VersionBuildFailed: the last build attempt failed. Transient by
	// construction — a retry moves it back to VersionBuilding.
	VersionBuildFailed
)

// String returns a human-readable name. Like NodeState's, these are wire names.
func (s VersionState) String() string {
	switch s {
	case VersionAbsent:
		return "ABSENT"
	case VersionDataPresent:
		return "DATA_PRESENT"
	case VersionBuilding:
		return "BUILDING"
	case VersionReady:
		return "READY"
	case VersionBuildFailed:
		return "BUILD_FAILED"
	default:
		return "UNKNOWN"
	}
}

// versionTransitions is the legal version-layer transitions, excluding entry
// removal (VersionDropped) which is not a transition but the deletion of a
// fact.
//
// The backwards edges are real, not tolerated accidents:
//   - READY -> DATA_PRESENT is the retention policy dropping an artifact, or
//     the on-disk index being lost (a replaced vecstore data directory). The
//     version stays queryable in the sense that it can be rebuilt on demand.
//   - BUILD_FAILED -> BUILDING is a retry.
var versionTransitions = map[VersionState][]VersionState{
	VersionAbsent:      {VersionDataPresent},
	VersionDataPresent: {VersionBuilding},
	VersionBuilding:    {VersionReady, VersionBuildFailed},
	VersionReady:       {VersionDataPresent},
	VersionBuildFailed: {VersionBuilding},
}

// canTransitionTo reports whether a version-layer state change is legal.
func (s VersionState) canTransitionTo(next VersionState) bool {
	for _, allowed := range versionTransitions[s] {
		if next == allowed {
			return true
		}
	}
	return false
}

// transitionError reports a rejected transition, naming both ends so the log
// line is enough to find the caller that produced it.
type transitionError struct {
	what string
	from fmt.Stringer
	to   fmt.Stringer
}

func (e *transitionError) Error() string {
	return fmt.Sprintf("nodestate: illegal %s transition %s -> %s", e.what, e.from, e.to)
}
