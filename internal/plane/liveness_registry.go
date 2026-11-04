package plane

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// LivenessRegistry is the control leader's in-memory record of when each storage node
// last sent a heartbeat. Storage redundancy is judged from it (Degrade).
//
// **Hard boundary — this is SOFT state.** It is never written to Raft and never feeds a
// correctness decision: a heartbeat that was never received must not be read as "the node
// is gone", which is why the verdict it feeds only ever costs a retry.
//
// It is also scoped to one leadership term: a new leader starts empty rather than
// inheriting a predecessor's view.
type LivenessRegistry struct {
	mu     sync.RWMutex
	byNode map[int64]time.Time
}

// NewLivenessRegistry returns an empty registry.
func NewLivenessRegistry() *LivenessRegistry {
	return &LivenessRegistry{byNode: make(map[int64]time.Time)}
}

// Record notes that nodeID sent a heartbeat just now.
func (r *LivenessRegistry) Record(nodeID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byNode[nodeID] = time.Now()
}

// ReportedAt returns when nodeID last sent a heartbeat, and whether it ever has.
func (r *LivenessRegistry) ReportedAt(nodeID int64) (time.Time, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	at, ok := r.byNode[nodeID]
	return at, ok
}

// Reset drops every node's record. A leadership change calls it: the record is per-term
// soft state, and inheriting it would mean judging from heartbeats the new leader never
// received.
func (r *LivenessRegistry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byNode = make(map[int64]time.Time)
}

// DefaultStorageSilenceWindow is how long a required replica may go without
// a heartbeat before this design stops counting it as live
// (docs/storage-degradation-signal-plan.md §3.2).
//
// Three heartbeat intervals — sync.DefaultHeartbeatInterval is 5s — and the
// third interval is the point: the window is what supplies the hysteresis §3.2
// asks for, because a verdict that flips on a single delayed heartbeat is worse
// than no verdict. A write refused over one hiccup costs a caller a retry it
// did not need, and the caller cannot tell that refusal apart from a real
// outage. Like the other §10.4 numbers this is a starting point to be
// calibrated; cmd/stratum's -storage-silence-window overrides it.
const DefaultStorageSilenceWindow = 15 * time.Second

// StorageState is the storage layer's ability to meet a write's durability
// contract for a knowledge base, as far as the leader's heartbeat record can
// tell (docs/storage-degradation-signal-plan.md §3).
type StorageState int

const (
	// StorageHealthy: a quorum of the required replicas is live, so a write has
	// somewhere to reach quorum.
	StorageHealthy StorageState = iota

	// StorageDegraded: fewer than a quorum of the required replicas is live, but
	// at least one is, so a write cannot reach its durability target. Refusing it
	// up front is the whole point of this signal: otherwise the attempt spends a
	// version number, a Raft entry and a retry budget before failing at fan-out
	// (§1.1).
	//
	// Reads are deliberately NOT affected — a replica that still holds the data
	// can still serve it, and this design keeps that trade-off (§2, non-goals).
	StorageDegraded

	// StorageUnavailable: NOT ONE required replica is live.
	//
	// It is the same "cannot write" answer as StorageDegraded, split out because
	// "the storage layer is gone" and "it is one replica short of a quorum" are
	// different diagnoses for whoever reads the error, and §4.1 names them
	// separately. The refusal itself is identical (retryable, codes.Unavailable) —
	// only the name and the detail differ.
	//
	// NARROWER than it sounds: with a cluster-wide replica topology there is no
	// knowledge base this could be true for and another it could not, so it fires
	// only in the extreme (§3.1, §12.3). Per-KB placement (§10.2) is what would
	// make the distinction between this and StorageDegraded routine.
	StorageUnavailable
)

// String renders a StorageState for logs and error details.
func (s StorageState) String() string {
	switch s {
	case StorageHealthy:
		return "HEALTHY"
	case StorageDegraded:
		return "DEGRADED"
	case StorageUnavailable:
		return "UNAVAILABLE"
	default:
		return "UNKNOWN"
	}
}

// Degradation is the redundancy verdict for one knowledge base.
//
// It is DERIVED state and is written nowhere: what it comes from is soft state,
// so the verdict can be stale in both directions, and the plan forbids it from
// feeding a correctness decision. No version state changes, no read is refused,
// and §10.6's cleanup still confirms against each node directly — see the hard
// boundary on LivenessRegistry above.
type Degradation struct {
	// State is the verdict. Meaningful only when Known.
	State StorageState

	// Known is false when the judgement cannot be made at all: this record has
	// not received a single heartbeat yet this term (a fresh leadership term starts
	// empty — see Reset), or the caller had no replica set to judge against.
	//
	// Callers MUST read unknown as "allow". This is an expression-layer signal
	// rather than a correctness gate, so a wrong answer must cost what its
	// direction costs: refusing writes for the length of a leadership change
	// would be an outage, while allowing a write that then fails at fan-out is
	// merely today's behaviour (§3.3, fail-open).
	Known bool

	// Live is how many of the required replicas reported inside the window.
	Live int
	// Required is the size of the replica set a write's acknowledgement must
	// reach.
	Required int
	// Quorum is how many of those must be live (QuorumSize).
	Quorum int

	// Silent names the required replicas that sent no heartbeat inside the window,
	// in ascending order. A node that has never sent one is among them: absence of
	// a heartbeat is evidence of absence of a heartbeat, and of nothing else.
	Silent []int64

	// Detail is the diagnosis a refusal carries: how many replicas are live, the
	// quorum they must reach, and how long the silent ones have been quiet. A
	// refusal nobody can act on is worse than the retry it saves (§4.3).
	Detail string
}

// Degrade evaluates whether the required replicas still form a quorum, and says
// why when they do not.
//
// required is the replica set a write must reach, and it comes from the caller's
// topology — never derived from this record. Judging "who should have it" by
// "who has reported" would let a silent node drop out of its own requirement
// (the same rule LocalControlPlane.requiredReplicas documents for §7.5).
//
// now is a parameter rather than time.Now() so one evaluation is consistent with
// itself and tests can move the clock.
func (r *LivenessRegistry) Degrade(required []int64, now time.Time, window time.Duration) Degradation {
	if window <= 0 {
		window = DefaultStorageSilenceWindow
	}
	quorum := QuorumSize(len(required))

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.byNode) == 0 || len(required) == 0 {
		// Nothing to judge from: an empty record (this node's first moments as
		// leader, or a node that has never led) or no replica set to compare it
		// against. Both are UNKNOWN, never "unavailable" — §3.3 requires the
		// caller to let the write through, and a brand-new leader would otherwise
		// refuse every write until its first heartbeats land.
		return Degradation{
			State:    StorageHealthy,
			Known:    false,
			Required: len(required),
			Quorum:   quorum,
			Detail:   "storage redundancy unknown: no heartbeats to judge against",
		}
	}

	live := 0
	silent := make([]int64, 0, len(required))
	// quiet describes each silent replica for a human: which node, and how long it
	// has been quiet (or that it has never spoken at all). A refusal that names
	// the nodes is one an operator can act on.
	quiet := make([]string, 0, len(required))
	for _, nodeID := range required {
		reportedAt, ok := r.byNode[nodeID]
		if ok && now.Sub(reportedAt) <= window {
			live++
			continue
		}
		silent = append(silent, nodeID)
		if ok {
			quiet = append(quiet, fmt.Sprintf("%d (last heartbeat %s ago)",
				nodeID, silenceFor(now, reportedAt)))
		} else {
			quiet = append(quiet, fmt.Sprintf("%d (no heartbeat yet)", nodeID))
		}
	}
	sort.Slice(silent, func(i, j int) bool { return silent[i] < silent[j] })

	d := Degradation{
		State:    StorageHealthy,
		Known:    true,
		Live:     live,
		Required: len(required),
		Quorum:   quorum,
		Silent:   silent,
		Detail: fmt.Sprintf("%d of %d required replicas live, quorum is %d",
			live, len(required), quorum),
	}
	if live < quorum {
		d.State = StorageDegraded
		d.Detail = fmt.Sprintf(
			"%d of %d required replicas live, below the quorum of %d; silent: %s",
			live, len(required), quorum, joinLabels(quiet))
	}
	// Not one replica answering is a different sentence from "one short", so it
	// gets its own state (and its own sentinel upstream). It is still the same
	// refusal: retryable, and it costs the caller nothing but the round trip.
	if live == 0 {
		d.State = StorageUnavailable
		d.Detail = fmt.Sprintf(
			"no required replica is live (the quorum is %d); silent: %s",
			quorum, joinLabels(quiet))
	}
	return d
}

// silenceFor is how long ago reportedAt was, clamped at zero.
//
// A heartbeat stamped in the future is clock skew between nodes, not a node that
// somehow reported before now; clamping keeps a diagnosis honest instead of
// printing a negative age that reads like a bug in the reporter.
func silenceFor(now, reportedAt time.Time) time.Duration {
	d := now.Sub(reportedAt)
	if d < 0 {
		return 0
	}
	return d.Round(time.Second)
}

// joinLabels renders a list for a diagnosis, or "none" when it is empty.
func joinLabels(labels []string) string {
	if len(labels) == 0 {
		return "none"
	}
	out := labels[0]
	for _, label := range labels[1:] {
		out += ", " + label
	}
	return out
}
