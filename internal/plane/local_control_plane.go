package plane

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// LocalControlPlane is the in-process ControlPlane for stage ① of
// control-data-separation-design.md §7: it translates storage-layer reports
// into the metadata proposals the control layer already understands. It is
// meant to be the only channel through which the storage layer reaches
// replicated state — which is what keeps the boundary honest while both
// layers still live in one process.
// MetadataProposer is the slice of the control layer's metadata API the storage
// layer reports through. Keeping it narrow means the ControlPlane — and its
// tests — depend only on what they actually use; *raft.RaftNode satisfies it.
type MetadataProposer interface {
	MetadataLister
	ProposeUpdateVersionStatus(ctx context.Context, versionID int64, status types.IndexStatus, nodeID int64) error
	ProposeUpdateVersionSummary(ctx context.Context, versionID int64, docIDSetHash string) error
	ProposeMarkVersionDataDurable(ctx context.Context, versionID int64) error
	ProposeMarkVersionFailedPermanent(ctx context.Context, kbID string, versionID int64, side types.FailureSide, reason string, count int32) error
}

// DefaultFailureBudget is how many failed attempts a version tolerates before
// the control layer declares it permanently failed
// (Stratum_设计文档v13.md §10.1). Like the other §10.4 numbers it is a starting
// point still to be calibrated against real workloads.
const DefaultFailureBudget = 5

type LocalControlPlane struct {
	rn MetadataProposer

	// nodeID is the node this plane speaks for, set by WithNodeID. It is what
	// makes an index-readiness report say WHO is ready rather than "someone is"
	// (§8.6(d)). Zero means unwired: reports then record no replica, which
	// under-states the serving count and is therefore the safe direction.
	nodeID int64

	// logger is optional; a nil logger silently drops the advisory log lines.
	logger *zap.Logger

	// failures counts failed attempts per (kbID, versionID): the control
	// layer is the arbiter of the terminal verdict, so it has to keep the
	// count itself — the storage layer only reports that an attempt failed.
	mu            sync.Mutex
	failures      map[string]int32
	failureBudget int32

	// kbBudgets overrides the budget per knowledge base, set from that KB's
	// DurabilityPolicy (SetFailureBudget). Absent means "use failureBudget".
	kbBudgets map[string]int32

	// dataVersions is the leader's §7.13.4 aggregate of the storage layer's
	// cursor reports, and leaderGate answers whether this node is the one whose
	// aggregate is authoritative. Both nil on a node that does not lead: the
	// view is simply unavailable there, which callers must be able to tell apart
	// from "nobody holds it".
	dataVersions *DataVersionRegistry
	leaderGate   *LeaderGate

	// leaderWatermarks are the watermarks the control leader carried back on a
	// report's response — the fallback for a node that writes data but does not lead
	// (see ReclaimableChangesThrough). Guarded by mu.
	leaderWatermarks map[string]int64

	// requiredReplicas lists the node IDs that must hold a version before the WAL
	// records behind it may be discarded (§7.5). It comes from the cluster
	// topology, never from the aggregate: deriving "who should have it" from
	// "who has reported" would let a node that is merely silent drop out of the
	// requirement, and the reclaim verdict would then be derived from the very
	// reports it is supposed to be checked against.
	requiredReplicas func() ([]int64, error)
}

// ControlPlaneOption configures a LocalControlPlane.
type ControlPlaneOption func(*LocalControlPlane)

// WithDataVersionView wires the leader's §7.13.4 cursor aggregate, so the control
// plane can answer "which nodes hold version V". The gate is consulted on every
// query rather than at construction: leadership moves, and a node that has just
// taken over has an empty aggregate (LeaderGate clears it on takeover) — answering
// from it is correct, whereas answering from a predecessor's would not be.
func WithDataVersionView(reg *DataVersionRegistry, gate *LeaderGate) ControlPlaneOption {
	return func(c *LocalControlPlane) {
		c.dataVersions = reg
		c.leaderGate = gate
	}
}

// WithRequiredReplicas wires the replica set a version must reach before the WAL
// changes behind it become reclaimable (§7.5). Optional: without it the reclaimable
// watermark stays unavailable, which is the conservative answer.
func WithRequiredReplicas(fn func() ([]int64, error)) ControlPlaneOption {
	return func(c *LocalControlPlane) {
		c.requiredReplicas = fn
	}
}

// WithFailureBudget overrides how many failures precede the terminal verdict.
// Non-positive values keep DefaultFailureBudget.
func WithFailureBudget(n int) ControlPlaneOption {
	return func(c *LocalControlPlane) {
		if n > 0 {
			c.failureBudget = int32(n)
		}
	}
}

// WithNodeID tells the control plane which node it speaks for.
//
// It is needed because a replica's index-readiness report has to name the
// REPORTER, not just assert that something is ready (§8.6(d)): the count of
// serving replicas is what decides whether one of them may step out for
// maintenance, and a node asking that question has to exclude itself. A report
// that cannot say who it came from cannot be subtracted from.
func WithNodeID(nodeID int64) ControlPlaneOption {
	return func(c *LocalControlPlane) { c.nodeID = nodeID }
}

// WithControlLogger wires a logger for the control plane's advisory reports
// (dropped late confirmations, for instance).
func WithControlLogger(l *zap.Logger) ControlPlaneOption {
	return func(c *LocalControlPlane) {
		if l != nil {
			c.logger = l
		}
	}
}

// NewLocalControlPlane returns a ControlPlane backed by rn.
func NewLocalControlPlane(rn MetadataProposer, opts ...ControlPlaneOption) *LocalControlPlane {
	c := &LocalControlPlane{
		rn:            rn,
		failures:      make(map[string]int32),
		failureBudget: DefaultFailureBudget,
		kbBudgets:     make(map[string]int32),
		logger:        zap.NewNop(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// ReportVersionFailure records a failed attempt and, once the budget is spent —
// or immediately, for a globally fatal failure — records the terminal verdict
// (Stratum_设计文档v13.md §10.1).
//
// The counter is kept per (version, side): the data side and the index side
// fail independently, so an index build that keeps failing must not spend the
// budget the data write needs, and neither verdict may stand for the other
// (§10.1b). The budget itself stays per KB.
//
// The count is per-process state on purpose: a restart forgets it and the
// budget starts over. That errs toward retrying a version more often than the
// budget allows rather than declaring it dead too early — retrying is the
// recoverable direction, and the budget itself is still a §10.4 number to be
// calibrated.
func (c *LocalControlPlane) ReportVersionFailure(ctx context.Context, kbID string, versionID int64, side types.FailureSide, class types.FailureClass, detail string) (bool, error) {
	c.mu.Lock()
	key := sideFailureKey(kbID, versionID, side)
	c.failures[key]++
	count := c.failures[key]
	budget := c.failureBudget
	if perKB, ok := c.kbBudgets[kbID]; ok {
		budget = perKB
	}
	c.mu.Unlock()

	// A globally fatal failure does not wait for the budget: retrying cannot
	// help, so spending the remaining retries would only delay the terminal
	// verdict and keep the version PENDING meanwhile
	// (Stratum_设计文档v13.md §10.1).
	if class != types.FailureFatalGlobal && count < budget {
		return false, nil // still inside the budget: the next attempt may succeed
	}
	if err := c.rn.ProposeMarkVersionFailedPermanent(ctx, kbID, versionID, side, detail, count); err != nil {
		return false, fmt.Errorf("plane: ReportVersionFailure: mark version %d permanently failed: %w", versionID, err)
	}
	return true, nil
}

// SetFailureBudget records the retry budget declared for kbID. It is how a
// KB's DurabilityPolicy reaches the layer that owns the terminal verdict
// (Stratum_设计文档v13.md §10.1). A non-positive value clears any override,
// falling back to the process default.
func (c *LocalControlPlane) SetFailureBudget(_ context.Context, kbID string, maxFailures int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if maxFailures <= 0 {
		delete(c.kbBudgets, kbID)
		return nil
	}
	c.kbBudgets[kbID] = int32(maxFailures)
	return nil
}

// failureKey namespaces a (kbID, versionID) pair. Version IDs are only unique
// within one knowledge base.
//
// It is shared by the failure counters, the §7.3 takeover timers, and the §10.6
// cleanup queue — all three key off the same pair — which is why it takes no
// side: the sides are a failure-counter concern only (see sideFailureKey).
func failureKey(kbID string, versionID int64) string {
	return kbID + "\x00" + strconv.FormatInt(versionID, 10)
}

// sideFailureKey namespaces a FAILURE COUNTER per knowledge base, version, and
// side. The data side and the index side fail independently, so their budgets
// must not share a counter (Stratum_设计文档v13.md §10.1b).
func sideFailureKey(kbID string, versionID int64, side types.FailureSide) string {
	return failureKey(kbID, versionID) + "\x00" + side.String()
}

// clearFailures drops one side's counter once that side succeeds, so a later,
// unrelated failure starts from a fresh budget. The other side's counter is left
// alone: it is a different question with a different answer.
func (c *LocalControlPlane) clearFailures(kbID string, versionID int64, side types.FailureSide) {
	c.mu.Lock()
	delete(c.failures, sideFailureKey(kbID, versionID, side))
	c.mu.Unlock()
}

var _ ControlPlane = (*LocalControlPlane)(nil)

// ReportDataDurable records the version's document-set digest — the same
// proposal followers already rely on to verify a completed data pull
// (VersionMeta.DocIDSetHash).
//
// Whether a late confirmation still applies is decided by the state machine
// rather than here: it holds the version's status deterministically, needs no
// extra read on the write path, and is the same on every replica. A report for
// an already settled version is dropped there (Stratum_设计文档v13.md §10.6).
func (c *LocalControlPlane) ReportDataDurable(ctx context.Context, kbID string, versionID int64, digest string) error {
	if err := c.rn.ProposeUpdateVersionSummary(ctx, versionID, digest); err != nil {
		return err
	}
	c.clearFailures(kbID, versionID, types.FailureSideData)
	return nil
}

// ReportIndexReady marks the version's index built and serviceable — on THIS
// node.
//
// The report carries the reporter's identity, because "serviceable" is a fact
// about this replica rather than about the version in the abstract, and the
// control layer aggregates the identities into the serving count §8.6(d)'s
// rolling cleanup has to consult. An unwired nodeID (0) records nothing, which
// under-states that count — the safe direction: it can only make cleanup more
// cautious.
//
// A successful build also clears the INDEX side's failure counter: the side
// that just succeeded is the one whose budget has been reset, and clearing only
// that side is what keeps an index build's history from spending the data
// side's retries (Stratum_设计文档v13.md §10.1b).
func (c *LocalControlPlane) ReportIndexReady(ctx context.Context, kbID string, versionID int64) error {
	if err := c.rn.ProposeUpdateVersionStatus(ctx, versionID, types.IndexStatusReady, c.nodeID); err != nil {
		return err
	}
	c.clearFailures(kbID, versionID, types.FailureSideIndex)
	return nil
}

// IndexReadyReplicaCount reports how many nodes OTHER than `except` have reported
// this version's index serviceable (§8.6(d)).
//
// It answers exactly one question, out of the control layer's own authoritative
// state: may this node step out of service for a moment to reclaim its artifact?
// The asker is excluded because it is about to stop serving — counting it would
// let a node vouch for its own safety, which is the one thing the check exists to
// prevent.
//
// A version that is not in the metadata at all is an ERROR rather than a count of
// zero: "this version is gone" and "nobody is serving it" call for opposite
// actions, and only the caller knows which it meant to ask about.
func (c *LocalControlPlane) IndexReadyReplicaCount(ctx context.Context, kbID string, versionID int64, except int64) (int, error) {
	versions, err := c.rn.ListVersions(ctx, kbID)
	if err != nil {
		return 0, fmt.Errorf("plane: IndexReadyReplicaCount: list versions of %s: %w", kbID, err)
	}
	for _, v := range versions {
		if v.VersionID != versionID {
			continue
		}
		count := 0
		for _, node := range v.IndexReadyNodes {
			if node != except {
				count++
			}
		}
		return count, nil
	}
	return 0, fmt.Errorf("plane: IndexReadyReplicaCount: version %d of %s not found: %w",
		versionID, kbID, stratumerrors.ErrVersionNotFound)
}

// ReportAvailability maps the abstract storage-layer availability onto the
// version status the control layer keeps (v1 §4.3: AVAILABLE → READY,
// UNAVAILABLE → FAILED).
//
// DEGRADED deliberately maps to no status change: it describes the read
// service capacity distribution rather than the version's own state
// (Stratum_设计文档v13.md §8.2), so the version stays READY and the
// degradation is an alerting concern upstream.
//
// nodeID 0: these are verdicts ABOUT the version, not a replica claiming it is
// serving (§8.6(d) counts the latter). Recording a node here would let one
// replica's availability verdict stand in for another's readiness.
func (c *LocalControlPlane) ReportAvailability(ctx context.Context, _ string, versionID int64, state Availability) error {
	switch state {
	case AvailabilityDegraded:
		return nil
	case AvailabilityAvailable:
		return c.rn.ProposeUpdateVersionStatus(ctx, versionID, types.IndexStatusReady, 0)
	case AvailabilityUnavailable:
		return c.rn.ProposeUpdateVersionStatus(ctx, versionID, types.IndexStatusFailed, 0)
	default:
		return fmt.Errorf("plane: ReportAvailability: unknown availability %d", int(state))
	}
}

// ReportEpoch reconciles control-layer state with storage-layer facts after a
// restart (v1 §5.3): the storage layer reports which versions it holds durably,
// and every reported version whose metadata still says PENDING is promoted to
// READY — the state the storage-layer facts justify.
//
// The other half of §5.3 (demoting versions the storage layer did *not*
// report) is deliberately left to the storage layer: a version whose index is
// missing inside the retention window is rebuilt rather than demoted, and
// demoting on absence would fight that rebuild — the pre-contract reconcile
// behaved the same way.
func (c *LocalControlPlane) ReportEpoch(ctx context.Context, _ uint64, dataVersions map[string]int64, indexReadyVersions map[string][]int64) error {
	// Stage ① runs in-process, so there is no stale-report window to close yet:
	// the epoch itself becomes meaningful once the storage cluster keeps its own
	// manifest (stage ④, v1 §5.3), and is accepted-and-ignored here.
	//
	// Each half promotes its OWN side and nothing else (§7.9, §10.1b):
	//
	//   - the index half promotes an explicit version SET to READY, because
	//     readiness is not monotonic in version order (§8.1) and a scalar could
	//     not express it;
	//   - the data half turns a reported cursor into DURABLE — see
	//     promoteDurableData below. Keeping that in the data side is the point of
	//     the split payload: a cursor alone must never make a version queryable,
	//     so it may not touch IndexStatus.
	for kbID, cursor := range dataVersions {
		if err := c.promoteDurableData(ctx, kbID, cursor); err != nil {
			return err
		}
	}

	for kbID, readyIDs := range indexReadyVersions {
		if len(readyIDs) == 0 {
			continue
		}
		ready := make(map[int64]bool, len(readyIDs))
		for _, id := range readyIDs {
			ready[id] = true
		}
		versions, err := c.rn.ListVersions(ctx, kbID)
		if err != nil {
			return fmt.Errorf("plane: ReportEpoch: list versions of %s: %w", kbID, err)
		}
		for _, v := range versions {
			if !ready[v.VersionID] || v.IndexStatus != types.IndexStatusPending {
				continue
			}
			// nodeID 0: the version's readiness is established by the storage
			// layer's own reconcile, not by a replica claiming it serves — §8.6(d)
			// counts the latter, and this promotion must not add a phantom server.
			if err := c.rn.ProposeUpdateVersionStatus(ctx, v.VersionID, types.IndexStatusReady, 0); err != nil {
				return fmt.Errorf("plane: ReportEpoch: promote version %d to READY: %w", v.VersionID, err)
			}
		}
	}
	return nil
}

// promoteDurableData records the data side as durable for every version at or
// below the reported cursor that the control layer still holds as PENDING
// (Stratum_设计文档v13.md §10.1b; §7.9 owns the payload).
//
// The cursor is a scalar because the chain is linear: version numbering is dense
// over the versions that still exist, so "at or below the cursor" is exactly
// "durable" (§7.9). It is also conservative in the safe direction — the storage
// layer reports the quorum MINIMUM of its replicas' cursors (§7.8), which can
// only under-report, so the worst case is a version that stays PENDING until a
// writer records it.
//
// Two filters carry real weight:
//
//   - the version must still EXIST: it comes from ListVersions, which is the
//     authority on existence, while the cursor only answers "is the data here";
//   - it must not be DELETING. A delete flow reclaims the data before it removes
//     the metadata (§10.6), so inside that window a cursor can sit above a
//     version whose bytes are already gone; calling that DURABLE would tell the
//     cleanup path a version is alive when it is not.
//
// Only PENDING is promoted; the state machine enforces that too, so a replayed or
// late report cannot undo a verdict.
func (c *LocalControlPlane) promoteDurableData(ctx context.Context, kbID string, cursor int64) error {
	versions, err := c.rn.ListVersions(ctx, kbID)
	if err != nil {
		return fmt.Errorf("plane: ReportEpoch: list versions of %s: %w", kbID, err)
	}
	for _, v := range versions {
		if v.VersionID > cursor || v.Deleting || v.DataStatus != types.DataStatusPending {
			continue
		}
		if err := c.rn.ProposeMarkVersionDataDurable(ctx, v.VersionID); err != nil {
			return fmt.Errorf("plane: ReportEpoch: mark version %d data durable: %w", v.VersionID, err)
		}
	}
	return nil
}

// DataVersionHolders answers "which nodes hold version V of kbID", from the
// leader's §7.13.4 aggregate of the storage layer's cursor reports. ok=false means
// the answer is UNAVAILABLE — this node is not the leader, or no aggregate is
// wired — and is emphatically not "nobody has it": the two lead to opposite
// actions, since the second would justify deleting data (§10.6) that a healthy
// node in fact holds.
//
// A non-empty result is still only "these nodes reported a cursor reaching V". It
// is a routing hint: fetch from one of them rather than from a peer that never
// reached V. It is never evidence for a destructive decision — that needs each
// node's own confirmation.
func (c *LocalControlPlane) DataVersionHolders(kbID string, versionID int64) ([]Holder, bool) {
	if c.dataVersions == nil || c.leaderGate == nil || !c.leaderGate.IsLeader() {
		return nil, false
	}
	return c.dataVersions.Holders(kbID, versionID), true
}

// ReclaimableChangesThrough reports the highest version V such that the WAL changes
// for every version up to V may be discarded (§7.5): each required replica has
// reported a contiguous cursor reaching V, so the delta has served every peer that
// was supposed to need it.
//
// ok=false means the watermark is NOT KNOWN — this node is not the leader, no
// aggregate or replica set is wired, the topology cannot be read, or at least one
// required replica has not reported. Every one of those resolves to "do not
// reclaim", which is why they share one answer: a CRASH is the failure mode, and
// the only safe response to "I cannot prove it is safe" is to keep the data.
//
// Note that this is a policy input, not an instruction: reclaiming is irreversible
// (a node that later needs the gap must fall back to a full state transfer), so the
// caller decides, and the caller is expected to be conservative.
func (c *LocalControlPlane) ReclaimableChangesThrough(kbID string) (int64, bool) {
	if watermark, ok := c.localReclaimable(kbID); ok {
		return watermark, true
	}
	// Not the leader (or the judgement cannot be made here), so fall back to what the
	// leader carried back on the report's response. Handing the watermark to the node
	// that WROTE the data is the whole point: that is the WAL that grows, and under
	// §7.13.2 it need not be the leader's.
	c.mu.Lock()
	defer c.mu.Unlock()
	watermark, ok := c.leaderWatermarks[kbID]
	return watermark, ok
}

// localReclaimable computes the watermark from this node's own authoritative view.
// It answers false whenever this node is not the leader or some replica's cursor is
// unknown — every such answer means "keep the data".
func (c *LocalControlPlane) localReclaimable(kbID string) (int64, bool) {
	if c.dataVersions == nil || c.leaderGate == nil || c.requiredReplicas == nil || !c.leaderGate.IsLeader() {
		return 0, false
	}
	required, err := c.requiredReplicas()
	if err != nil {
		return 0, false
	}
	if len(required) == 0 {
		return 0, false
	}

	// The watermark is the *minimum* across replicas: the slowest replica decides,
	// because a version is only safe to forget once every peer that needs the delta
	// has it. A replica that has never reported makes the whole answer unknown
	// rather than excepting it — its silence is not evidence that it does not need
	// the changes.
	watermark := int64(-1)
	for _, nodeID := range required {
		cursor, ok := c.dataVersions.Cursor(nodeID, kbID)
		if !ok {
			return 0, false
		}
		if watermark < 0 || cursor < watermark {
			watermark = cursor
		}
	}
	if watermark < 0 {
		return 0, false
	}
	return watermark, true
}

// SetLeaderWatermarks records the watermarks the control leader carried back on a
// report's response. It is only consulted when this node cannot judge locally (see
// ReclaimableChangesThrough), i.e. on a node that writes data but does not lead.
//
// Staleness is safe in the only direction that matters here: a reported watermark was
// true when the leader computed it, and cursors never move backwards, so a stale value
// is never HIGHER than the current truth — it reclaims less than it could, never more.
// (A leader change empties the new leader's aggregate, so the value can also go down;
// discarding changes the previous leader had justified remains correct, and a replica
// that later turns out to need one falls back to a full-state transfer by §6.4.)
func (c *LocalControlPlane) SetLeaderWatermarks(watermarks map[string]int64) {
	next := make(map[string]int64, len(watermarks))
	for kbID, version := range watermarks {
		next[kbID] = version
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leaderWatermarks = next
}

// VersionExists reports whether a version is still present in the replicated
// metadata. The backfill asks this before advancing its cursor over a version whose
// pull returned no records: "empty" and "deleted" look identical at the storage
// layer, and only the metadata can tell them apart (§7.5).
//
// It reads the local replica of the metadata, not a Raft round trip: the caller
// runs on the apply path, where any blocking call would stall every later entry.
func (c *LocalControlPlane) ExistingVersions(ctx context.Context, kbID string) (map[int64]bool, error) {
	versions, err := c.rn.ListVersions(ctx, kbID)
	if err != nil {
		return nil, fmt.Errorf("plane: list versions of %s: %w", kbID, err)
	}
	existing := make(map[int64]bool, len(versions))
	for _, v := range versions {
		existing[v.VersionID] = true
	}
	return existing, nil
}
