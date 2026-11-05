package plane

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

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

	// kbBudgets overrides the budget per knowledge base, set through
	// SetFailureBudget. Absent means "use failureBudget".
	kbBudgets map[string]int32

	// liveness is the leader's record of the storage nodes' heartbeats, and leaderGate
	// answers whether this node is the one whose record is authoritative. Both nil on a
	// node that does not lead: the verdict is simply unavailable there.
	liveness   *LivenessRegistry
	leaderGate *LeaderGate

	// requiredReplicas lists the node IDs a write must reach. It comes from the cluster
	// topology, never from who has sent a heartbeat: deriving "who should have it" from
	// "who has spoken" would let a node that is merely silent drop out of the requirement.
	requiredReplicas func() ([]int64, error)

	// silenceWindow is how long a required replica may go without a heartbeat
	// before the redundancy verdict stops counting it as live
	// (docs/storage-degradation-signal-plan.md §3.2). It is the verdict's
	// hysteresis: see Degrade. Zero means DefaultStorageSilenceWindow.
	silenceWindow time.Duration
}

// ControlPlaneOption configures a LocalControlPlane.
type ControlPlaneOption func(*LocalControlPlane)

// WithLivenessView wires the leader's record of the storage nodes' heartbeats, so the
// control plane can judge storage redundancy. Passing the gate alongside it is what makes
// the record per-term: taking over clears it.
func WithLivenessView(reg *LivenessRegistry, gate *LeaderGate) ControlPlaneOption {
	return func(c *LocalControlPlane) {
		c.liveness = reg
		c.leaderGate = gate
	}
}

// WithRequiredReplicas wires the replica set a write must reach.
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
		// The version settled while this report was travelling: the data side reached
		// DURABLE — usually because a replica that caught up announced it — so the
		// failure this report describes has been resolved by events. Answered as
		// "already settled" instead of as a report failure, and with terminal=false
		// rather than true: the caller must stop retrying, but must NOT reclaim data
		// a child version may already depend on (docs/known-gaps.md §K).
		if errors.Is(err, stratumerrors.ErrVersionDataSettled) {
			c.clearFailures(kbID, versionID, side)
			c.logger.Debug("plane: the version is already durable; the failure report is discarded",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.String("side", side.String()))
			return false, nil
		}
		return false, fmt.Errorf("plane: ReportVersionFailure: mark version %d permanently failed: %w", versionID, err)
	}
	return true, nil
}

// SetFailureBudget records the retry budget declared for kbID. It is how a
// KB's declared failure budget would reach the layer that owns the terminal
// verdict (Stratum_设计文档v13.md §10.1); nothing in the node assembly calls it
// today, so the process default governs. A non-positive value clears any
// override, falling back to the process default.
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
	if versionID <= 0 {
		// Version ids start at 1; a non-positive ask would make the lower bound a
		// no-op for the range filter and pull the whole chain back.
		return 0, fmt.Errorf("plane: IndexReadyReplicaCount: version %d of %s not found: %w",
			versionID, kbID, stratumerrors.ErrVersionNotFound)
	}
	// §F: the one version the question is about — (versionID-1, versionID] — rather
	// than the whole chain filtered here. The index GC asks this per candidate.
	from, to := versionID-1, versionID
	versions, err := c.rn.ListVersionsInRange(ctx, kbID, &from, &to)
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

// WithStorageSilenceWindow sets how long a required replica may go without
// reporting before the redundancy verdict stops counting it live
// (docs/storage-degradation-signal-plan.md §3.2). Non-positive values keep
// DefaultStorageSilenceWindow, which is three heartbeat intervals.
func WithStorageSilenceWindow(d time.Duration) ControlPlaneOption {
	return func(c *LocalControlPlane) {
		if d > 0 {
			c.silenceWindow = d
		}
	}
}

// StorageDegradation reports whether the storage layer can still meet a write's
// durability contract for kbID, and why not when it cannot.
//
// ok=false means the judgement cannot be made — this node is not the control
// leader, no replica topology is wired, the topology cannot be read, or the
// record holds no heartbeat yet. Every one of those means the same thing to
// a caller: ALLOW. This is an expression-layer signal rather than a correctness
// gate, so "cannot tell" must not become a refusal — see Degradation.Known and
// the plan's §3.3 (fail-open).
//
// kbID is deliberately not part of the judgement. The question a write gate has
// to answer is "can a quorum of the replicas that must hold this version be
// reached", and with a single cluster-wide replica topology that answer is the
// same for every knowledge base (docs/storage-degradation-signal-plan.md §3.1).
// Per-KB placement (§10.2) is what would separate the two scopes; until then kbID
// is carried only so the diagnosis names the knowledge base it was asked about.
//
// The verdict is soft state and can be stale in either direction, so a refusal
// must be retryable: the record moves with the heartbeats, and the caller's
// next attempt is the one that gets the authoritative answer (§4.3).
func (c *LocalControlPlane) StorageDegradation(kbID string) (StorageState, string, bool) {
	if c.liveness == nil || c.leaderGate == nil || !c.leaderGate.IsLeader() {
		return StorageHealthy, "storage redundancy unknown: not the control leader", false
	}
	if c.requiredReplicas == nil {
		return StorageHealthy, "storage redundancy unknown: replica topology not wired", false
	}
	required, err := c.requiredReplicas()
	if err != nil {
		// An unreadable topology is "unknown", never "unavailable": §3.3 lists it
		// beside the empty record for exactly that reason.
		return StorageHealthy, fmt.Sprintf(
			"storage redundancy unknown: read replica topology: %v", err), false
	}
	d := c.liveness.Degrade(required, time.Now(), c.effectiveSilenceWindow())
	if !d.Known {
		return StorageHealthy, d.Detail, false
	}
	detail := d.Detail
	if kbID != "" {
		detail = kbID + ": " + d.Detail
	}
	return d.State, detail, true
}

// StorageDegraded is StorageDegradation reduced to the single bit a write gate
// needs, in the shape the service layer consumes — it declares its own narrow
// interface rather than importing this package, so the state enum never crosses
// that boundary.
//
// STRICTLY "short of a quorum with someone still answering": a caller asking
// whether a write can land must ask StorageUnavailable as well, and the two
// answers together decide which sentinel the refusal carries (§4.1). Keeping them
// apart here is what lets "the storage layer is gone" read differently from "it is
// one replica short".
//
// ok=false keeps its meaning from StorageDegradation: unknown, and therefore
// ALLOW. degraded is false whenever ok is false, so a caller that checks the flag
// alone still fails open.
func (c *LocalControlPlane) StorageDegraded(kbID string) (degraded bool, detail string, ok bool) {
	state, detail, ok := c.StorageDegradation(kbID)
	return ok && state == StorageDegraded, detail, ok
}

// StorageUnavailable is the same reduction for the extreme: NOT ONE required
// replica is live.
//
// It exists so a refusal can say which of the two happened without the service
// layer importing this package's state enum. Callers that want "can a write land
// at all" must consult this AND StorageDegraded; callers that want the diagnosis
// take detail from either.
//
// ok=false is unknown, as everywhere else, and therefore ALLOW.
func (c *LocalControlPlane) StorageUnavailable(kbID string) (unavailable bool, detail string, ok bool) {
	state, detail, ok := c.StorageDegradation(kbID)
	return ok && state == StorageUnavailable, detail, ok
}

// chainGenerationSource is the part of a Raft node that can name the state of the chain
// tails. Only the shape that holds the log has it, which is the only shape that leads.
type chainGenerationSource interface {
	ChainGeneration() (term, seq uint64)
}

// ChainGeneration names the current state of the chain tails, so a node can tell whether
// it has to pull them again. ok=false means this node offers no signal.
//
// It is read BEFORE the tails it describes wherever the two travel together, so tails
// are always at least as new as the generation they came with: at worst a node pulls
// once more than it had to, and it can never miss a change.
func (c *LocalControlPlane) ChainGeneration() (term, seq uint64, ok bool) {
	src, isSource := c.rn.(chainGenerationSource)
	if !isSource {
		return 0, 0, false
	}
	term, seq = src.ChainGeneration()
	return term, seq, true
}

// ChainTails answers "what is the newest version the control layer has accepted?" for
// every knowledge base — the tails a node compares its own cursors against to decide
// whether it has fallen behind (docs/active-lag-detection-design.md).
//
// It reads the replicated metadata directly, so this is the leader's answer and nobody
// else's: a follower's metadata view can lag, and a lagging tail would tell a node it is
// fine when it is not. The list comes from the metadata rather than from any node's
// report because a node that missed a whole chain has nothing to report about it, and this
// is how it learns there is something to be behind on.
//
// A knowledge base whose tail cannot be read, or which has no versions, is left out:
// absence is "no signal", never "nothing to catch up".
func (c *LocalControlPlane) ChainTails(ctx context.Context) (map[string]int64, error) {
	if c.rn == nil {
		return nil, errors.New("plane: chain tails: no metadata source wired")
	}
	kbs, err := c.rn.ListKnowledgeBases(ctx)
	if err != nil {
		return nil, fmt.Errorf("plane: chain tails: list knowledge bases: %w", err)
	}
	tails := make(map[string]int64, len(kbs))
	for _, kb := range kbs {
		// The extreme value, not the chain: the control node answers in O(1) (see
		// RaftNode.LastVersionID).
		tail, err := c.rn.LastVersionID(ctx, kb.KBID)
		if err != nil || tail <= 0 {
			c.logger.Debug("plane: chain tail unavailable",
				zap.String("kb_id", kb.KBID), zap.Int64("tail", tail), zap.Error(err))
			continue
		}
		tails[kb.KBID] = tail
	}
	return tails, nil
}

// DataStatusOf answers "is this version's DATA side still PENDING?" from the
// local metadata replica, without leaving the node (plane.VersionDataStatusQuerier).
//
// Why the DATA side and not the version as a whole: the §7.3 takeover announces
// data durability, so IndexStatus is irrelevant here. An index reaches READY on
// a node that holds no data, which is exactly the confusion the two-sided state
// exists to prevent (v13 §10.1b).
//
// ok=false means "cannot answer" — no metadata replica wired, or the version is
// not in it. The caller then announces anyway: "not found" is not "settled", and
// an announcement nobody needed is cheaper than a version stranded in PENDING.
func (c *LocalControlPlane) DataStatusOf(ctx context.Context, kbID string, versionID int64) (types.DataStatus, bool) {
	if c.rn == nil || versionID <= 0 {
		return types.DataStatusPending, false
	}
	// ListVersionsInRange is (fromExclusive, toInclusive]: the narrowest read
	// that answers "just this version", so the takeover costs one bounded
	// metadata read instead of a whole-chain scan.
	from, to := versionID-1, versionID
	metas, err := c.rn.ListVersionsInRange(ctx, kbID, &from, &to)
	if err != nil {
		return types.DataStatusPending, false
	}
	for _, m := range metas {
		if m.VersionID == versionID {
			return m.DataStatus, true
		}
	}
	return types.DataStatusPending, false
}

// effectiveSilenceWindow resolves the configured silence window, defaulting it exactly
// the way LivenessRegistry.Degrade does.
func (c *LocalControlPlane) effectiveSilenceWindow() time.Duration {
	if c.silenceWindow <= 0 {
		return DefaultStorageSilenceWindow
	}
	return c.silenceWindow
}
