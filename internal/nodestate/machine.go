package nodestate

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Options tunes a Machine. The zero value is usable: a machine with no clock
// takes time.Now and one with no logger is silent.
type Options struct {
	// Now is injectable so tests can control the timestamps a transition
	// records, instead of asserting on a moving target.
	Now func() time.Time

	// Logger receives transitions. Optional; nil means zap.NewNop.
	Logger *zap.Logger
}

// versionKey identifies one version on this node. Private: callers name
// versions by (kbID, versionID) and get VersionEntry back, so the key's shape
// is not part of the API and can change.
type versionKey struct {
	kbID      string
	versionID int64
}

// versionEntry is one version's record. Kept to 8 bytes of payload plus the
// key and two words of time so a node holding many versions does not pay for
// the bookkeeping (docs/storage-node-state-machine-plan.md §4.3).
type versionEntry struct {
	state  VersionState
	since  time.Time
	detail string
}

// Machine is the node's state machine. It is safe for concurrent use.
type Machine struct {
	now    func() time.Time
	logger *zap.Logger

	mu    sync.RWMutex
	state NodeState
	since time.Time

	redundancy       Redundancy
	redundancyDetail string

	// catchingUp is the set of knowledge bases this node is catching up on.
	// It is a node-level fact rather than a version-level aggregate: a
	// knowledge base may be catching up while nothing has moved yet (it is
	// waiting on a pull), so no version entry would exist to aggregate from
	// (docs/storage-node-state-machine-plan.md §3.3).
	catchingUp map[string]struct{}

	// versions holds the entries. A version with no entry is VersionAbsent —
	// which is why deletion removes the entry instead of storing VersionAbsent,
	// and why absence needs no disk probe to report.
	versions map[versionKey]*versionEntry

	// cursors, heldAbove and announcedAbove are the DATA side: this node's
	// contiguous cursor per knowledge base, and the two accounts it advances
	// over. See data.go for what they mean and why they live here.
	cursors        map[string]int64
	heldAbove      map[string]map[int64]struct{}
	announcedAbove map[string]map[int64]struct{}
}

// New returns a machine in NodeStarting with no versions.
func New(opts Options) *Machine {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Machine{
		now:            now,
		logger:         logger,
		state:          NodeStarting,
		since:          now(),
		catchingUp:     make(map[string]struct{}),
		versions:       make(map[versionKey]*versionEntry),
		cursors:        make(map[string]int64),
		heldAbove:      make(map[string]map[int64]struct{}),
		announcedAbove: make(map[string]map[int64]struct{}),
	}
}

// ---------------------------------------------------------------- node layer

// State reports the current lifecycle phase.
func (m *Machine) State() NodeState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// StateSince reports when the current phase was entered.
func (m *Machine) StateSince() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.since
}

// Transition moves the node to a new lifecycle phase.
//
// A transition to the phase the node is already in is a no-op returning nil,
// not an error: the events that drive this are reported by control flow that
// can legitimately reach the same conclusion twice (an all-in-one node runs
// both the storage assembly and the control assembly), and a caller should not
// have to check first. An ILLEGAL transition — SERVING back to RECONCILING,
// or anything out of the terminal STOPPING — returns an error naming both ends.
func (m *Machine) Transition(to NodeState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	from := m.state
	if from == to {
		return nil
	}
	if !from.canTransitionTo(to) {
		return &transitionError{what: "node", from: from, to: to}
	}
	m.state = to
	m.since = m.now()
	m.logger.Info("nodestate: node phase",
		zap.String("from", from.String()), zap.String("to", to.String()))
	return nil
}

// SetRedundancy records this node's own view of storage redundancy and the
// detail behind it.
//
// It is set, not transitioned: the verdict moves in both directions with the
// reports this node has seen, and there is no meaningful "illegal" order to
// it. An empty detail on any state other than RedundancyUnknown is normal
// (the healthy verdict has nothing to explain).
func (m *Machine) SetRedundancy(r Redundancy, detail string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.redundancy = r
	m.redundancyDetail = detail
}

// Redundancy reports the current redundancy verdict and its detail.
func (m *Machine) Redundancy() (Redundancy, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.redundancy, m.redundancyDetail
}

// SetPeerRedundancy records this node's own view of whether a write can reach
// its durability target, expressed as the counts that view was derived from.
//
// The counts are the FACT — this node reached `live` of `required` replicas, and
// a write needs `quorum` — and the verdict is this machine's reading of them.
// Taking the fact rather than a verdict is what keeps a caller from having to
// mirror this package's enum and keep two vocabularies for three states in step.
//
// known=false means the sample was INCOMPLETE: at least one required replica has
// never been reached, so the honest reading is UNKNOWN rather than a verdict
// extrapolated from the others. That is not a problem state — the write gates
// fail OPEN on it (docs/storage-degradation-signal-plan.md §3.3).
func (m *Machine) SetPeerRedundancy(known bool, live, quorum int, detail string) {
	switch {
	case !known:
		m.SetRedundancy(RedundancyUnknown, detail)
	case live >= quorum:
		m.SetRedundancy(RedundancyHealthy, "")
	case live > 0:
		m.SetRedundancy(RedundancyDegraded, detail)
	default:
		m.SetRedundancy(RedundancyUnavailable, detail)
	}
}

// MarkCatchingUp declares whether this node is currently catching up on kbID.
//
// DECLARATION, not derivation: a knowledge base can be behind while no single
// version of it has moved here yet, so this cannot be read off the version
// layer (§3.3). Declaring a knowledge base that is not behind costs a line in
// a status response; it is not a correctness input anywhere.
func (m *Machine) MarkCatchingUp(kbID string, catching bool) {
	if kbID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if catching {
		m.catchingUp[kbID] = struct{}{}
		return
	}
	delete(m.catchingUp, kbID)
}

// IsCatchingUp reports whether this node has declared kbID as catching up.
//
// It is the query side of MarkCatchingUp, and it exists so the component that
// PICKS the knowledge bases does not have to keep a copy of the set: one
// component deciding and another remembering is exactly the drift the machine
// is here to end.
func (m *Machine) IsCatchingUp(kbID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.catchingUp[kbID]
	return ok
}

// CatchingUpCount reports how many knowledge bases are catching up, without
// building the sorted list CatchingUp returns — this is the concurrency bound's
// input, and it runs on every report.
func (m *Machine) CatchingUpCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.catchingUp)
}

// CatchingUp returns the knowledge bases this node declared it is catching up
// on, sorted so the answer is stable across calls.
func (m *Machine) CatchingUp() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.catchingUpLocked()
}

func (m *Machine) catchingUpLocked() []string {
	if len(m.catchingUp) == 0 {
		return nil
	}
	out := make([]string, 0, len(m.catchingUp))
	for kbID := range m.catchingUp {
		out = append(out, kbID)
	}
	sort.Strings(out)
	return out
}

// ------------------------------------------------------------- version layer

// DataLanded records that versionID's data is now on this node.
//
// It is the entry-creating event: before it there is no entry, after it the
// version is VersionDataPresent. Repeating it is a no-op — several paths
// legitimately reach the same conclusion for one landing (a local write, a
// pull, a push from the coordinator), and a caller should not have to know
// which one won.
func (m *Machine) DataLanded(kbID string, versionID int64) error {
	// A one-way statement — "the data is here" — reported by every path that
	// produced a landing: a push, a pull, and a lag catch-up all report the same
	// one, and they overlap.
	//
	// So a report that arrives when the version is already at or past
	// DATA_PRESENT must change nothing. It must NOT "move" the version there:
	// versionTransitions allows VersionReady -> VersionDataPresent (IndexDropped
	// needs that edge, for the index that really did disappear), so a landing
	// reported against a READY version would rewind it — and the next thing that
	// happens to a version with data and no index is another BUILD of an index
	// that is already on disk. Measured on the 3+3 cluster before this: every
	// version was built twice per replica, the second build landing exactly when
	// lag catch-up reported the landing.
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.versions[versionKey{kbID, versionID}]; ok && entry.state != VersionAbsent {
		return nil
	}
	return m.transitionVersionLocked(kbID, versionID, VersionDataPresent, "")
}

// BuildScheduled records that an index build for versionID is queued or
// running.
//
// LEGAL from VersionDataPresent (the ordinary path) and from
// VersionBuildFailed (a retry). VersionAbsent is refused: without data there
// is nothing to build an index from, and that refusal is the point of having
// the table at all.
func (m *Machine) BuildScheduled(kbID string, versionID int64) error {
	return m.transitionVersion(kbID, versionID, VersionBuilding, "")
}

// BuildSucceeded records that versionID's index is built and on disk.
//
// LEGAL only from VersionBuilding. A caller that reports success without a
// build having been scheduled is describing an artifact this machine never saw
// appear, and the refusal is deliberate rather than an inconvenience: the
// startup path that projects existing artifacts does so through
// RestoreVersion, which is a different statement ("disk already had this").
func (m *Machine) BuildSucceeded(kbID string, versionID int64) error {
	return m.transitionVersion(kbID, versionID, VersionReady, "")
}

// BuildFailed records that versionID's build attempt failed. detail is the
// cause, carried for the operator reading a status response.
func (m *Machine) BuildFailed(kbID string, versionID int64, detail string) error {
	return m.transitionVersion(kbID, versionID, VersionBuildFailed, detail)
}

// VersionDropped removes versionID's entry: the version is gone from this
// node — deleted, reclaimed after a permanent verdict, or its knowledge base
// dropped.
//
// It is removal rather than a transition to VersionAbsent because absence is
// the absence of a fact, not a fact. Afterwards VersionState reports
// VersionAbsent, which is the truth.
func (m *Machine) VersionDropped(kbID string, versionID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.versions, versionKey{kbID, versionID})
}

// DropKnowledgeBase removes every entry for kbID, along with any catch-up
// declaration for it. Used when the knowledge base itself goes away.
func (m *Machine) DropKnowledgeBase(kbID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.versions {
		if key.kbID == kbID {
			delete(m.versions, key)
		}
	}
	delete(m.catchingUp, kbID)
}

// VersionState reports where versionID stands on this node. An unknown version
// reports VersionAbsent, which is correct rather than a default: entries come
// into existence when data lands.
func (m *Machine) VersionState(kbID string, versionID int64) VersionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.versions[versionKey{kbID, versionID}]
	if !ok {
		return VersionAbsent
	}
	return entry.state
}

// RestoreVersion records what DISK FACTS say about versionID, overwriting
// whatever this machine believed.
//
// This is one of the two entries that bypass the transition table — IndexDropped
// is the other, and it only ever corrects an existing entry — and it is not a
// general-purpose setter. It exists for two callers (docs/…plan.md §5.1
// and §5.2): the startup rebuild, which projects the reconciled disk state
// before the node serves, and the runtime correction of a fact that moved
// underneath the machine — the retention policy dropping an artifact, or a
// vecstore data directory being replaced. In both, the disk is authoritative
// and the machine is its explicit projection.
//
// What keeps it from being a back door is its accepted states: only the two
// the disk can express — VersionDataPresent and VersionReady. A caller trying
// to restore VersionBuilding or VersionBuildFailed is describing an activity,
// not an artifact, and is refused. To record "there is nothing here", use
// VersionDropped.
func (m *Machine) RestoreVersion(kbID string, versionID int64, state VersionState, detail string) error {
	switch state {
	case VersionDataPresent, VersionReady:
	default:
		return fmt.Errorf("nodestate: restore accepts only DATA_PRESENT or READY, got %s", state)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	key := versionKey{kbID, versionID}
	prev, had := m.versions[key]
	m.versions[key] = &versionEntry{state: state, since: m.now(), detail: detail}
	if !had || prev.state != state {
		m.logger.Debug("nodestate: version restored from disk facts",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.String("state", state.String()))
	}
	return nil
}

// IndexInstalled records that an index for versionID arrived from another
// replica and is now on this node's disk (§8.4's "build once, distribute N").
//
// It is RestoreVersion(READY) under a name that says where the fact came from:
// the artifact was produced elsewhere and copied here, so this node never saw a
// build and there is no BUILDING to transition out of. That is exactly what
// RestoreVersion is for — projecting a disk fact — whereas BuildSucceeded would
// claim a build this node never ran.
func (m *Machine) IndexInstalled(kbID string, versionID int64) error {
	return m.RestoreVersion(kbID, versionID, VersionReady, "")
}

// IndexDropped records that versionID's artifact was removed from this node's
// disk by the retention policy — §5.2's "the disk is the authority, and the
// machine is its projection".
//
// It only CORRECTS an entry that already exists. A version the machine never
// recorded an artifact for is left alone, because creating a DATA_PRESENT entry
// here would accumulate one per version the policy has ever dropped, for a fact
// the cursor already implies. That is also why it is not RestoreVersion: that is
// the path that projects what the disk HOLDS, and it does create entries.
//
// And it is deliberately not BuildFailed: nothing failed. Going back to
// DATA_PRESENT is the normal consequence of the policy — the version is rebuilt
// when something asks for it.
func (m *Machine) IndexDropped(kbID string, versionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.versions[versionKey{kbID, versionID}]
	if !ok || entry.state != VersionReady {
		return nil
	}
	entry.state = VersionDataPresent
	entry.since = m.now()
	entry.detail = ""
	m.logger.Debug("nodestate: index artifact dropped by the retention policy",
		zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
	return nil
}

// VersionEntry is one version's state as seen from outside.
type VersionEntry struct {
	KBID      string
	VersionID int64
	State     VersionState
	// Since is when the version entered State.
	Since time.Time
	// Detail carries the cause for VersionBuildFailed and is empty otherwise.
	Detail string
}

// NonSteadyVersions returns the versions that are NOT at rest — those being
// built or whose last build failed — sorted by (kbID, versionID) so the answer
// is stable.
//
// VersionDataPresent is at rest and is therefore excluded: data present with
// no index is the designed state of a version nobody has asked for, because
// builds are lazy (§8.6b). Listing every such version would turn a status
// response into a dump of the node's whole dataset. VersionReady is likewise
// at rest — a node holding thousands of them has nothing to report about each.
func (m *Machine) NonSteadyVersions() []VersionEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []VersionEntry
	for key, entry := range m.versions {
		if entry.state != VersionBuilding && entry.state != VersionBuildFailed {
			continue
		}
		out = append(out, VersionEntry{
			KBID:      key.kbID,
			VersionID: key.versionID,
			State:     entry.state,
			Since:     entry.since,
			Detail:    entry.detail,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].KBID != out[j].KBID {
			return out[i].KBID < out[j].KBID
		}
		return out[i].VersionID < out[j].VersionID
	})
	return out
}

// BuildCount reports how many versions are currently building.
func (m *Machine) BuildCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.buildCountLocked()
}

func (m *Machine) buildCountLocked() int {
	n := 0
	for _, entry := range m.versions {
		if entry.state == VersionBuilding {
			n++
		}
	}
	return n
}

// transitionVersion applies an event-driven version transition under the lock.
func (m *Machine) transitionVersion(kbID string, versionID int64, next VersionState, detail string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.transitionVersionLocked(kbID, versionID, next, detail)
}

// transitionVersionLocked is transitionVersion with the lock already held, for
// callers that have to read the current state to decide whether to move it.
func (m *Machine) transitionVersionLocked(kbID string, versionID int64, next VersionState, detail string) error {
	key := versionKey{kbID, versionID}
	entry, ok := m.versions[key]
	from := VersionAbsent
	if ok {
		from = entry.state
	}

	if from == next {
		// Idempotent, and not merely tolerated: a landing is reported by every
		// path that produced it, and a retried event must not look like a bug.
		return nil
	}
	if !from.canTransitionTo(next) {
		return &transitionError{what: "version", from: from, to: next}
	}

	if !ok {
		entry = &versionEntry{}
		m.versions[key] = entry
	}
	entry.state = next
	entry.since = m.now()
	entry.detail = detail
	m.logger.Debug("nodestate: version state",
		zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
		zap.String("from", from.String()), zap.String("to", next.String()),
		zap.String("detail", detail))
	return nil
}

// ------------------------------------------------------------------ snapshot

// NodeSnapshot is the node layer as seen from outside: the phase, the
// redundancy verdict, and a human-readable list of what the node is doing.
//
// Actions is a PROJECTION FOR READING, not an input to any decision — no code
// path may branch on it. The two questions that look like they could ("is it
// busy", "is it behind") have real answers elsewhere: NonSteadyVersions for
// the version layer, and CatchingUp for the knowledge bases. Keeping this
// stringly list out of the decision path is what lets its wording change
// without breaking anything.
type NodeSnapshot struct {
	State            NodeState
	Since            time.Time
	Redundancy       Redundancy
	RedundancyDetail string
	Actions          []string
}

// Snapshot returns the node layer's current state in one consistent read.
func (m *Machine) Snapshot() NodeSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return NodeSnapshot{
		State:            m.state,
		Since:            m.since,
		Redundancy:       m.redundancy,
		RedundancyDetail: m.redundancyDetail,
		Actions:          m.actionsLocked(),
	}
}

// actionsLocked builds the human-readable activity list.
func (m *Machine) actionsLocked() []string {
	var actions []string
	if m.state == NodeReconciling {
		actions = append(actions, "reconciling indexes")
	}
	if n := m.buildCountLocked(); n > 0 {
		actions = append(actions, fmt.Sprintf("building %d index(es)", n))
	}
	if kbs := m.catchingUpLocked(); len(kbs) > 0 {
		actions = append(actions, "catching up "+strings.Join(kbs, ", "))
	}
	return actions
}
