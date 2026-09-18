package plane

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// DataVersionRegistry is the control leader's in-memory aggregate of the storage
// layer's periodic reports (§7.13.4): which version each node holds, per
// knowledge base.
//
// **Hard boundary — this is SOFT state.** It is never written to Raft and never
// feeds a correctness decision. §10.6's cleanup deletes real data, so acting on a
// stale "nobody has it" would delete the only copy; conversely a report that was
// never received must not be read as "the node does not have it". The registry
// answers "which node should I ask to serve version V", and nothing more. Whoever
// decides to delete must confirm against each node directly.
//
// It is also scoped to one leadership term: a new leader starts empty rather than
// inheriting a predecessor's view (which may describe nodes that have since
// restarted with wiped disks).
type DataVersionRegistry struct {
	mu     sync.RWMutex
	byNode map[int64]*nodeDataVersions
}

type nodeDataVersions struct {
	cursors    map[string]int64
	address    string
	reportedAt time.Time
}

// Holder is one entry of the aggregate: a node that reported holding a version,
// with the address it reported for itself.
//
// The address travels with the node id because the question this registry exists
// to answer is "who should serve version V" — and whoever asks then has to talk
// to the answer. Returning bare ids would push an id→address map onto every
// consumer, which is the duplication §2.2 keeps out of this design. The registry
// stores what the node said about itself and vouches for none of it beyond that.
type Holder struct {
	NodeID  int64
	Address string
}

// NewDataVersionRegistry returns an empty registry.
func NewDataVersionRegistry() *DataVersionRegistry {
	return &DataVersionRegistry{byNode: make(map[int64]*nodeDataVersions)}
}

// Record stores one node's report, replacing its previous one wholesale: each
// report is a complete statement of that node's cursors (the reporter sends every
// KB it knows), so merging would keep resurrecting knowledge bases it has since
// dropped.
//
// address is the node's own storage-layer gRPC address. It is stored verbatim:
// the registry has no registry of its own to check it against, and the alternative
// — trusting nothing and making callers supply the mapping — is what Holder exists
// to avoid.
func (r *DataVersionRegistry) Record(nodeID int64, address string, dataVersions map[string]int64) {
	cursors := make(map[string]int64, len(dataVersions))
	for kbID, version := range dataVersions {
		cursors[kbID] = version
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byNode[nodeID] = &nodeDataVersions{cursors: cursors, address: address, reportedAt: time.Now()}
}

// Holders returns, in ascending node order, the nodes whose recorded cursor
// reaches versionID for kbID, each with the address it reported. A node that has
// never reported is simply absent — which is why an empty result means "no one I
// have heard from", never "no one has it".
func (r *DataVersionRegistry) Holders(kbID string, versionID int64) []Holder {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var holders []Holder
	for nodeID, data := range r.byNode {
		if cursor, ok := data.cursors[kbID]; ok && cursor >= versionID {
			holders = append(holders, Holder{NodeID: nodeID, Address: data.address})
		}
	}
	sort.Slice(holders, func(i, j int) bool { return holders[i].NodeID < holders[j].NodeID })
	return holders
}

// KnowledgeBases returns every knowledge base any node has reported a cursor for.
//
// The union across nodes, not any one node's set: a node that missed a whole chain
// names it in no map at all, so "which chains exist" cannot be answered from a
// single report. That asymmetry is why this method exists — the leader is the only
// party that can tell such a node what it is behind on
// (docs/active-lag-detection-design.md).
func (r *DataVersionRegistry) KnowledgeBases() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]struct{})
	for _, data := range r.byNode {
		for kbID := range data.cursors {
			seen[kbID] = struct{}{}
		}
	}
	kbs := make([]string, 0, len(seen))
	for kbID := range seen {
		kbs = append(kbs, kbID)
	}
	sort.Strings(kbs)
	return kbs
}

// HolderAddresses is Holders flattened to just the addresses, in the same
// ascending node-id order.
//
// It exists because both consumers of this fact — the wire form on a report's
// response, and the reporter's sink on the other end — work in addresses, and
// neither package can name plane.Holder.
func (r *DataVersionRegistry) HolderAddresses(kbID string, versionID int64) []string {
	holders := r.Holders(kbID, versionID)
	addrs := make([]string, 0, len(holders))
	for _, holder := range holders {
		if holder.Address != "" {
			addrs = append(addrs, holder.Address)
		}
	}
	return addrs
}

// Cursor returns the highest version nodeID reported for kbID, and whether that
// node has reported at all.
func (r *DataVersionRegistry) Cursor(nodeID int64, kbID string) (int64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	data, ok := r.byNode[nodeID]
	if !ok {
		return 0, false
	}
	cursor, ok := data.cursors[kbID]
	return cursor, ok
}

// ReportedAt returns when nodeID last reported, and whether it has reported.
func (r *DataVersionRegistry) ReportedAt(nodeID int64) (time.Time, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	data, ok := r.byNode[nodeID]
	if !ok {
		return time.Time{}, false
	}
	return data.reportedAt, true
}

// Nodes returns the nodes that have reported, in ascending order.
func (r *DataVersionRegistry) Nodes() []int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]int64, 0, len(r.byNode))
	for nodeID := range r.byNode {
		nodes = append(nodes, nodeID)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

// Forget drops one node's view. The caller uses it when a node leaves the
// cluster: keeping its last report would let a departed node keep "vouching" for
// versions it no longer holds.
func (r *DataVersionRegistry) Forget(nodeID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byNode, nodeID)
}

// Reset drops every node's view. A leadership change calls it: the aggregate is
// per-term soft state, and inheriting it would mean answering "who holds V" from
// reports the new leader never received.
func (r *DataVersionRegistry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byNode = make(map[int64]*nodeDataVersions)
}

// DefaultStorageSilenceWindow is how long a required replica may go without
// reporting before this design stops counting it as live
// (docs/storage-degradation-signal-plan.md §3.2).
//
// Three report intervals — sync.DefaultDataVersionReportInterval is 5s — and the
// third interval is the point: the window is what supplies the hysteresis §3.2
// asks for, because a verdict that flips on a single delayed report is worse
// than no verdict. A write refused over one hiccup costs a caller a retry it
// did not need, and the caller cannot tell that refusal apart from a real
// outage. Like the other §10.4 numbers this is a starting point to be
// calibrated; cmd/stratum's -storage-silence-window overrides it.
const DefaultStorageSilenceWindow = 15 * time.Second

// StorageState is the storage layer's ability to meet a write's durability
// contract for a knowledge base, as far as the leader's §7.13.4 aggregate can
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
// boundary on DataVersionRegistry above.
type Degradation struct {
	// State is the verdict. Meaningful only when Known.
	State StorageState

	// Known is false when the judgement cannot be made at all: this aggregate has
	// not folded a single report yet this term (a fresh leadership term starts
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

	// Silent names the required replicas that did not report inside the window,
	// in ascending order. A node that has never reported is among them: absence of
	// a report is evidence of absence of a report, and of nothing else.
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
// topology — never derived from this aggregate. Judging "who should have it" by
// "who has reported" would let a silent node drop out of its own requirement
// (the same rule LocalControlPlane.requiredReplicas documents for §7.5).
//
// now is a parameter rather than time.Now() so one evaluation is consistent with
// itself and tests can move the clock.
func (r *DataVersionRegistry) Degrade(required []int64, now time.Time, window time.Duration) Degradation {
	if window <= 0 {
		window = DefaultStorageSilenceWindow
	}
	quorum := QuorumSize(len(required))

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.byNode) == 0 || len(required) == 0 {
		// Nothing to judge from: an empty aggregate (this node's first moments as
		// leader, or a node that has never led) or no replica set to compare it
		// against. Both are UNKNOWN, never "unavailable" — §3.3 requires the
		// caller to let the write through, and a brand-new leader would otherwise
		// refuse every write until its first reports land.
		return Degradation{
			State:    StorageHealthy,
			Known:    false,
			Required: len(required),
			Quorum:   quorum,
			Detail:   "storage redundancy unknown: no replica reports to judge against",
		}
	}

	live := 0
	silent := make([]int64, 0, len(required))
	// quiet describes each silent replica for a human: which node, and how long it
	// has been quiet (or that it has never spoken at all). A refusal that names
	// the nodes is one an operator can act on.
	quiet := make([]string, 0, len(required))
	for _, nodeID := range required {
		data, ok := r.byNode[nodeID]
		if ok && now.Sub(data.reportedAt) <= window {
			live++
			continue
		}
		silent = append(silent, nodeID)
		if ok {
			quiet = append(quiet, fmt.Sprintf("%d (last report %s ago)",
				nodeID, silenceFor(now, data.reportedAt)))
		} else {
			quiet = append(quiet, fmt.Sprintf("%d (never reported)", nodeID))
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
// A report stamped in the future is clock skew between nodes, not a node that
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
