package coordinator

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/embed"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/index"
	"stratum/internal/kvraft"
	"stratum/internal/plane"
	"stratum/internal/raft"
	"stratum/internal/splitter"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"
)

// DefaultDispatchTimeout bounds one background dispatch when the config does not
// say otherwise.
//
// It must stay LARGER than plane's per-candidate budget (15 s floor +
// 50 ms/document, capped at 10 min — see plane.CoordinatorDispatcher): this is
// the OUTER bound, and two budgets of the same size would let the inner one
// expire first, leaving the outer one nothing to report and the caller with a
// write that looks dispatched but is not.
const DefaultDispatchTimeout = 5 * time.Minute

// WriteCoordinatorConfig bundles all dependencies and configuration for
// WriteCoordinatorImpl, following the constructor-injection convention.
type WriteCoordinatorConfig struct {
	MaxRetries          int
	RetryBaseIntervalMS int

	WAL      wal.WAL
	RaftNode raft.RaftNode

	Splitter    splitter.ChunkSplitter
	EmbedClient embed.EmbedClient

	// ChunkBloom is the per-KB chunk-existence bloom filter. Used on the
	// write path to skip ChunkStore.Exists round-trips for new chunks.
	ChunkBloom bloom.BloomFilter

	// VersionBloom persists each version's document bloom filter (one per
	// version). May be nil in tests that do not exercise filter
	// persistence; the write path then skips step 5 and the read path
	// rebuilds filters lazily from VersionDocList.
	VersionBloom *bloom.VersionBloomStore

	ChunkStore     chunkstore.ChunkStore
	ChunkDocMapper chunkdoc.ChunkDocMapper
	DocStore       docstore.DocStore
	VersionDocList versiondoc.VersionDocList
	IndexManager   index.IndexManager

	// DataPlane runs the storage layer's write transaction (BEGIN → storage
	// writes → COMMIT) and reports its outcome back up
	// (control-data-separation-design.md §5.1; the split write path of
	// Stratum_设计文档v13.md §7.12). When nil, the constructor assembles the
	// in-process LocalDataPlane over the pieces above — which is what the
	// tests and the single-node deployment use.
	DataPlane plane.DataPlane

	// Dispatch hands a committed version's write to the coordinator the control
	// layer picks (§7.13.2), instead of running that write here. When nil, this
	// node coordinates its own writes — the pre-§7.13.2 behaviour, kept so a node
	// (or a test stack) that has not adopted the dispatch path keeps working.
	Dispatch func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error

	// Logger receives background dispatch failures. Optional.
	Logger *zap.Logger

	// ControlPlane records a version's failure so §10.1's retry budget can decide
	// its fate (transient → retried; budget spent → FAILED_PERMANENT). Optional:
	// without it a give-up can only be logged, which is exactly the hole this
	// field closes — a version whose dispatch failed stayed PENDING forever while
	// Query answered "version is PENDING", i.e. "try again later", when nothing
	// was ever going to try again.
	ControlPlane plane.ControlPlane

	// DispatchTimeout bounds ONE background dispatch, from the moment the version
	// is committed to the moment its write has been handed to a coordinator.
	// Zero means DefaultDispatchTimeout.
	//
	// Why it must exist: the background dispatch starts from a fresh context
	// (the client's is long gone), and a context with no deadline hands every
	// layer below an unbounded wait. The dispatcher does bound each candidate
	// (see plane.CoordinatorDispatcher.candidateBudget), but that bound covers
	// the candidate RPC only — candidate resolution, connection handling and the
	// retry loop over candidates are not bounded by it, and a dispatch that never
	// returns leaves no log line and no state: silence is the symptom. This is
	// the outer bound, so it must be LARGER than the per-candidate budget.
	DispatchTimeout time.Duration

	// Locks hands out the per-knowledge-base write lock, held for a whole
	// CreateVersion transaction (BEGIN through COMMIT). It is shared with the
	// orphan-chunk garbage collector so the GC's reclaim phase (current-version
	// re-check + mapping/vector deletion) is mutually exclusive with a write to the
	// SAME knowledge base — closing the stale-snapshot race where a sweep erases data
	// committed by a newer version — while leaving unrelated knowledge bases free to
	// write in parallel (M7 of docs/code-review-2026-09-24.md). When nil, the
	// coordinator allocates a private set; callers that also run a
	// ChunkGarbageCollector MUST inject the same instance into both.
	Locks *KBLockSet
}

// WriteCoordinatorImpl is the real WriteCoordinator implementation,
// orchestrating the full CreateVersion write path (steps 1-7) as documented
// in Stratum_接口设计v9.md "CreateVersion" and Stratum_设计文档v10.md "写路径".
type WriteCoordinatorImpl struct {
	cfg WriteCoordinatorConfig

	// locks serializes CreateVersion transactions per knowledge base, end to end
	// (BEGIN through COMMIT), so the WAL record order within one KB is
	// BEGIN -> VERSION_ID with no interleaved BEGIN of the same KB — the property
	// FileWAL relies on to bind each VERSION_ID to the right transaction's replay
	// input (see internal/wal/file.go). It is cfg.Locks (or a private fallback when
	// nil), shared with ChunkGarbageCollectorImpl's reclaim phase.
	//
	// Per KB rather than global: the lock spans a Raft round trip plus, on the
	// inline path, the storage fan-out, so a single lock made every KB wait behind
	// every other KB's network hop (M7 of docs/code-review-2026-09-24.md).
	locks *KBLockSet

	// pendingDispatch holds the changes of writes this node has proposed but not
	// yet dispatched (§7.13.2). The dispatch runs on the apply of the entry, and
	// the Raft command itself carries no changes — they deliberately stay out of
	// the log (§7.7) — so they have to be handed over out of band. Keyed by
	// (kbID, clientRequestID): that is what Execute knows before the version ID
	// exists, and what the apply hook can read back from the command.
	dispatchMu      sync.Mutex
	pendingDispatch map[dispatchKey]pendingDispatchEntry

	// claimedDispatch records the registrations Execute's own background dispatch
	// took, keyed the same way and timed, so the apply hook can tell the two
	// causes of a miss apart.
	//
	// Why it exists: a miss used to carry one message for both causes, and the
	// message named the rarer one. "Nobody holds these changes" (the proposing
	// process is gone) is the case worth warning about; "someone already took
	// them" is the DESIGNED outcome of racing Execute's dispatch against the
	// apply hook — Execute dispatches the moment the version is committed, and
	// whichever side arrives first wins, the other becoming a no-op. Measured:
	// every single write logged the warning, so the one occurrence that mattered
	// was indistinguishable from the noise. The claim is timed because the two
	// sides are milliseconds apart in the case this exists to recognise; a claim
	// older than dispatchClaimWindow can no longer explain a miss, and dropping
	// stale ones also keeps the map from growing without bound.
	claimedDispatch map[dispatchKey]time.Time

	// dispatchWG tracks in-flight background dispatches, so a shutdown (or a
	// test) can wait for the ones already handed off.
	dispatchWG sync.WaitGroup
}

// dispatchClaimWindow is how long a background dispatch's claim explains a later
// miss by the apply hook.
//
// Generous next to the gap it measures (Execute hands off right after the
// proposal returns; the apply loop is a goroutine away) because the cost of
// being too large is only that a genuine "nobody holds these changes" gets
// reported as noise — while the cost of being too small is a false warning on
// every write, which is the state this replaced.
const dispatchClaimWindow = 2 * time.Minute

// dispatchKey identifies one in-flight write's pending dispatch.
type dispatchKey struct {
	kbID            string
	clientRequestID string
}

// pendingDispatchEntry is what the dispatcher needs but the Raft command cannot
// carry: the changes to apply.
type pendingDispatchEntry struct {
	parentVersionID int64
	changes         []types.DocChange
}

// RegisterPendingDispatch remembers a write's changes so the apply-time
// dispatcher (§7.13.2) can hand them to the coordinator it picks. Registered
// before the proposal, because the apply may run before Execute has its version
// ID back.
func (c *WriteCoordinatorImpl) RegisterPendingDispatch(kbID, clientRequestID string, parentVersionID int64, changes []types.DocChange) {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	if c.pendingDispatch == nil {
		c.pendingDispatch = make(map[dispatchKey]pendingDispatchEntry)
	}
	c.pendingDispatch[dispatchKey{kbID: kbID, clientRequestID: clientRequestID}] = pendingDispatchEntry{
		parentVersionID: parentVersionID,
		changes:         changes,
	}
}

// TakePendingDispatch removes and returns the changes registered for
// (kbID, clientRequestID). Taking rather than reading makes a repeated dispatch
// attempt a no-op instead of a second write of the same version.
func (c *WriteCoordinatorImpl) TakePendingDispatch(kbID, clientRequestID string) (int64, []types.DocChange, bool) {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	key := dispatchKey{kbID: kbID, clientRequestID: clientRequestID}
	entry, ok := c.pendingDispatch[key]
	if !ok {
		return 0, nil, false
	}
	delete(c.pendingDispatch, key)
	return entry.parentVersionID, entry.changes, true
}

// claimDispatch records that the proposer's own background dispatch took this
// registration, so a later miss by the apply hook can be attributed to the race
// instead of to the version having no owner. See claimedDispatch.
func (c *WriteCoordinatorImpl) claimDispatch(kbID, clientRequestID string) {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	if c.claimedDispatch == nil {
		c.claimedDispatch = make(map[dispatchKey]time.Time)
	}
	now := time.Now()
	// Sweep as we go: claims only matter for dispatchClaimWindow, and this keeps
	// the map bounded without a background goroutine.
	for k, at := range c.claimedDispatch {
		if now.Sub(at) > dispatchClaimWindow {
			delete(c.claimedDispatch, k)
		}
	}
	c.claimedDispatch[dispatchKey{kbID: kbID, clientRequestID: clientRequestID}] = now
}

// DispatchClaimed reports whether the proposer's background dispatch took this
// registration recently — i.e. the write is being handled, not orphaned.
//
// False means no claim is on record, which is what the apply hook's miss needs
// to know before it calls the version ownerless.
func (c *WriteCoordinatorImpl) DispatchClaimed(kbID, clientRequestID string) bool {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	at, ok := c.claimedDispatch[dispatchKey{kbID: kbID, clientRequestID: clientRequestID}]
	return ok && time.Since(at) <= dispatchClaimWindow
}

// ForgetPendingDispatch drops a registration whose proposal never landed, so it
// cannot be dispatched later against a version that does not exist.
func (c *WriteCoordinatorImpl) ForgetPendingDispatch(kbID, clientRequestID string) {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	delete(c.pendingDispatch, dispatchKey{kbID: kbID, clientRequestID: clientRequestID})
}

// NewWriteCoordinatorImpl constructs a WriteCoordinatorImpl.
func NewWriteCoordinatorImpl(cfg WriteCoordinatorConfig) *WriteCoordinatorImpl {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBaseIntervalMS <= 0 {
		cfg.RetryBaseIntervalMS = 100
	}
	locks := cfg.Locks
	if locks == nil {
		locks = NewKBLockSet()
	}
	c := &WriteCoordinatorImpl{cfg: cfg, locks: locks}
	if c.cfg.DataPlane == nil {
		c.cfg.DataPlane = plane.NewLocalDataPlane(plane.LocalDataPlaneConfig{
			IndexManager: cfg.IndexManager,
			WAL:          cfg.WAL,
			Executor:     c,
			Control:      plane.NewLocalControlPlane(cfg.RaftNode),
		})
	}
	return c
}

// SetDataPlane replaces the write data plane. The node assembly needs it: the
// DataPlane is built after the coordinator (it needs the coordinator as its
// version-write executor), and then injected here so one instance serves both
// the write path and the read/sync path.
func (c *WriteCoordinatorImpl) SetDataPlane(dp plane.DataPlane) {
	c.cfg.DataPlane = dp
}

// NewDispatchID mints an idempotency key for a write that arrived without one.
//
// Exported because the service layer needs the same generator: it stamps the key
// onto CreateVersionResponse, so the format a caller is handed is the format the
// coordinator's own log lines show (docs/await-version-plan.md §7 Step 4).
// Execute runs under the knowledge base's write lock, so the timestamp alone is
// unambiguous.
func NewDispatchID() string {
	return fmt.Sprintf("auto-%d", time.Now().UnixNano())
}

// Execute implements WriteCoordinator.
func (c *WriteCoordinatorImpl) Execute(ctx context.Context, kbID string, parentVersionID int64, changes []types.DocChange, clientRequestID string) (int64, error) {
	// Per-stage timings of one write's control-plane half, at debug level.
	//
	// Why they are here: this is the only stage the CLIENT waits for. The
	// response carries the version id; the storage transaction (stage 5, on
	// whichever node coordinates it) and the index build (stage 6) both happen
	// after it. So this line answers "why is CreateVersion slow" on its own, with
	// the two parts that can be slow for reasons the caller cannot see:
	//
	//   - txn_wait_us is the serialization of §7.7 (the KB write lock): writes to one
	//     knowledge base run one at a time, so a burst of concurrent versions
	//     shows up here and nowhere else.
	//   - propose_us is the Raft round trip plus the apply phase that allocates
	//     the version id — the only replicated step in the path.
	//
	// storage_us is filled on the un-dispatched path only (Dispatch == nil),
	// where the storage transaction runs inline and this call really does wait
	// for it; on the dispatched path it stays 0 because the work has been handed
	// off, and the node that takes it reports its own stages after this returns.
	execStart := time.Now()
	var txnWait, tRegister, tPropose, tHandoff, tInline time.Duration
	var versionID int64
	var dispatched bool
	defer func() {
		c.logger().Debug("coordinator: write execute timings",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.Int("changes", len(changes)), zap.Bool("dispatched", dispatched),
			zap.Int64("txn_wait_us", txnWait.Microseconds()),
			zap.Int64("register_us", tRegister.Microseconds()),
			zap.Int64("propose_us", tPropose.Microseconds()),
			zap.Int64("storage_us", tInline.Microseconds()),
			zap.Int64("handoff_us", tHandoff.Microseconds()),
			zap.Int64("total_us", time.Since(execStart).Microseconds()))
	}()

	// Serialize the whole transaction (BEGIN through COMMIT) so the WAL's
	// BEGIN -> VERSION_ID binding per version stays unambiguous — per knowledge base,
	// which is the granularity the binding actually needs (see locks).
	txnStart := time.Now()
	unlockTxn := c.locks.Lock(kbID)
	txnWait = time.Since(txnStart)
	defer unlockTxn()

	// §7.13.2: only the leader accepts a write. It is the node that can put the
	// entry in the log AND the node that holds the changes, so a client that
	// reached a follower directly is answered with NotLeader and re-resolves the
	// leader (the router does that for it) rather than being served through a
	// second, client-side forwarding hop.
	if !c.cfg.RaftNode.IsLeader() {
		return 0, kvraft.ErrNotLeader
	}

	// An empty changes list is refused, not accepted as "a version with no
	// documents" (docs/cursor-persistence-plan.md §5): a new version's document set
	// is its PARENT's set plus these changes, so an empty list means "unchanged" —
	// and the empty set is merely one thing that can be unchanged, at the root of a
	// chain. Treating the two as the same code path made a 0-change version
	// indistinguishable from an empty one everywhere downstream: a version marked
	// as an empty document set whose real content was its parent's.
	//
	// The check is here, before the version id is allocated, because a version's
	// document set has to be statable from the moment it exists.
	//
	// Idempotent replays are unaffected: the same (kbID, ClientRequestID) still
	// returns the version the first attempt allocated.
	if len(changes) == 0 {
		return 0, fmt.Errorf("coordinator: CreateVersion(%s): %w", kbID, stratumerrors.ErrEmptyChanges)
	}

	// Step 1-2: the control layer allocates the version ID (its apply phase
	// writes the WAL's VERSION_ID record). Everything the version needs to
	// exist is now in replicated metadata; the data itself is the storage
	// layer's business.
	//
	// The changes never enter the log (§7.7), yet the §7.13.2 dispatch needs them
	// at apply time. They are registered under a key the apply hook can read back
	// out of the command, so a write that arrived without an idempotency key gets
	// a generated one — without it there would be nothing to register against.
	dispatchID := clientRequestID
	if dispatchID == "" {
		dispatchID = NewDispatchID()
	}
	stepStart := time.Now()
	c.RegisterPendingDispatch(kbID, dispatchID, parentVersionID, changes)
	tRegister = time.Since(stepStart)

	opts := []raft.ProposeOption{raft.WithClientRequestID(dispatchID)}
	stepStart = time.Now()
	versionID, err := c.cfg.RaftNode.ProposeCreateVersion(ctx, kbID, parentVersionID, opts...)
	tPropose = time.Since(stepStart)
	if err != nil {
		// The entry never landed: drop the registration, or it would later be
		// dispatched against a version that does not exist.
		c.ForgetPendingDispatch(kbID, dispatchID)
		return 0, fmt.Errorf("coordinator: Raft propose: %w", err)
	}

	// With no dispatcher wired, this node coordinates the write itself — the
	// pre-§7.13.2 behaviour, kept so a node (or a test stack) that has not
	// adopted the dispatch path keeps working.
	if c.cfg.Dispatch == nil {
		// Steps 3-7: the storage layer runs its own transaction (BEGIN → storage
		// writes → COMMIT), reports the document-set digest up and schedules the
		// index build (control-data-separation-design.md §5.1; the split write
		// path of Stratum_设计文档v13.md §7.12).
		stepStart = time.Now()
		err := c.cfg.DataPlane.WriteVersionData(ctx, kbID, versionID, parentVersionID, changes)
		tInline = time.Since(stepStart)
		if err != nil {
			return 0, err
		}
		return versionID, nil
	}

	// Dispatched (§2.5): the version exists as soon as it is committed, and the
	// data lands right after, on whichever candidate takes the write.
	//
	// Dispatching here as well as from the apply hook is deliberate: the
	// registration is TAKEN, not read, so exactly one of the two wins and the
	// other is a no-op; and racing it here is what lets a client retry — same
	// idempotency key, so no new apply happens — still get its write dispatched.
	stepStart = time.Now()
	c.dispatchInBackground(kbID, versionID, dispatchID)
	tHandoff = time.Since(stepStart)
	dispatched = true
	return versionID, nil
}

// dispatchInBackground hands a committed version's write to the coordinator the
// control layer picks (§7.13.2). It runs off the request path: the caller's
// version already exists, and the write's progress is visible through the
// version's status and digest.
func (c *WriteCoordinatorImpl) dispatchInBackground(kbID string, versionID int64, dispatchID string) {
	parentVersionID, changes, ok := c.TakePendingDispatch(kbID, dispatchID)
	if !ok {
		return // the apply hook got there first
	}
	// Claim it so the apply hook, if it is still on its way, can see that this
	// dispatch is the one handling the write — which is the normal outcome here,
	// not an orphaned version.
	c.claimDispatch(kbID, dispatchID)
	c.dispatchWG.Add(1)
	go func() {
		defer c.dispatchWG.Done()

		// Bounded, and it has to be: this context starts here (the client's is
		// long gone), so without a deadline every layer below inherits an
		// unbounded wait — and a stuck dispatch then produces neither a log line
		// nor a state change. Silence is the symptom. The dispatcher bounds each
		// candidate's RPC (plane.CoordinatorDispatcher.candidateBudget), but not
		// candidate resolution, not the retry loop over candidates, and not this
		// hand-off; this is the outer bound, so it is deliberately larger than
		// the per-candidate budget.
		ctx, cancel := context.WithTimeout(context.Background(), c.dispatchTimeout())
		defer cancel()

		if err := c.cfg.Dispatch(ctx, kbID, versionID, parentVersionID, changes); err != nil {
			// Not just a log line: hand it to the same give-up path the apply
			// hook uses, so §10.1's retry budget owns the verdict instead of the
			// version sitting PENDING forever.
			c.AbandonDispatch(ctx, kbID, versionID, types.FailureTransient,
				fmt.Sprintf("dispatch failed: %v", err))
		}
	}()
}

// dispatchTimeout is the outer bound on one background dispatch.
func (c *WriteCoordinatorImpl) dispatchTimeout() time.Duration {
	if c.cfg.DispatchTimeout > 0 {
		return c.cfg.DispatchTimeout
	}
	return DefaultDispatchTimeout
}

// AbandonDispatch is the single give-up path of the write path: the ONE place a
// version whose data never landed is handed to §10.1's failure accounting.
//
// What it replaces: two independent code paths (the apply hook finding no
// pending dispatch, and a background dispatch that failed or timed out) each
// logged a warning and returned. Both left the version PENDING for good while
// Query answered FailedPrecondition "version is PENDING" — a message that means
// "try again shortly" about a version nothing was ever going to touch again.
// Whether §10.1 retries it or declares FAILED_PERMANENT is that layer's
// decision; all this does is make sure the decision gets made.
//
// Safe to call repeatedly: the control plane owns the budget.
func (c *WriteCoordinatorImpl) AbandonDispatch(ctx context.Context, kbID string, versionID int64, class types.FailureClass, detail string) {
	log := c.logger()
	if versionID == 0 {
		// Nothing to attribute the failure to: no version was ever allocated
		// (the proposal did not land), so there is no version state to settle.
		log.Warn("coordinator: abandoning a write that never allocated a version",
			zap.String("kb_id", kbID), zap.String("detail", detail))
		return
	}
	log.Warn("coordinator: abandoning a write's data path",
		zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
		zap.String("class", class.String()), zap.String("detail", detail))

	if c.cfg.ControlPlane == nil {
		// No accounting wired (a test stack, or a node without a control plane):
		// the warning above is all there is. Say so rather than pretending.
		log.Warn("coordinator: no control plane wired; this failure is recorded only in the log",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
		return
	}
	terminal, err := c.cfg.ControlPlane.ReportVersionFailure(ctx, kbID, versionID, types.FailureSideData, class, detail)
	if err != nil {
		log.Warn("coordinator: reporting a version's failure",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}
	if !terminal {
		return // still inside the retry budget: §10.1 will try again
	}
	// The terminal verdict just landed: reclaim whatever physical data made it to disk
	// HERE.
	//
	// No broadcast. §10.6 broadcast because the control layer never knew which replicas
	// had received the version; every replica now learns the verdict from its own apply
	// and reclaims locally (plane.LocalDataPlane.NoteTerminalVersion), so broadcasting
	// again would mainly re-reach replicas already doing it. What this path adds is
	// SPEED on the node that detected the failure.
	if c.cfg.DataPlane == nil {
		return
	}
	if err := c.cfg.DataPlane.ReclaimVersionDataLocally(ctx, kbID, versionID); err != nil {
		log.Warn("coordinator: cleanup after a permanent failure",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

// logger returns the configured logger, or a no-op one.
func (c *WriteCoordinatorImpl) logger() *zap.Logger {
	if c.cfg.Logger != nil {
		return c.cfg.Logger
	}
	return zap.NewNop()
}

// ReplayVersionStorageWrites implements WriteCoordinator: replays steps
// 3-6 (plus summary + async build) for an already-committed version after
// a crash, without writing a BEGIN record or proposing a new version.
func (c *WriteCoordinatorImpl) ReplayVersionStorageWrites(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) error {
	unlock := c.locks.Lock(kbID)
	defer unlock()

	// The WAL already framed this transaction (its BEGIN record is what the
	// recovery path read the replay input from), so the storage layer resumes
	// it rather than starting a second one.
	return c.cfg.DataPlane.ResumeVersionWrite(ctx, kbID, versionID, parentVersionID, changes)
}

// DropVersionStorage removes one version's physical storage writes: its MVCC
// records in the document store and its document-ID list. It is the cleanup of
// Stratum_设计文档v13.md §10.6 — a version the control layer declared
// FAILED_PERMANENT may still have landed on a replica whose acknowledgement was
// lost, and nobody else would reclaim it.
//
// The reclaim is a plain prefix delete, as §10.6 specifies. It deliberately does
// NOT apply the visibility-anchor rule the delete-version flow uses: that rule
// protects a SURVIVING version's reads, and a permanently failed version has
// none — visibility follows the control layer's state, not what is physically
// present, so its records are unreachable by construction. Applying the anchor
// here would also bolt a metadata lookup onto a cleanup path that has to work
// precisely when the control layer is unreachable.
//
// Idempotent: both deletes are prefix scans, so a version that never arrived
// here is a no-op rather than an error.
func (c *WriteCoordinatorImpl) DropVersionStorage(ctx context.Context, kbID string, versionID int64) error {
	// Each store is optional because this runs on nodes that hold none of the
	// data. A control-role node has no local docstore and no version-doc list:
	// its job is to ASK the nodes that do (§7.13.2, "asking rather than
	// doing"), and it reaches them through the data plane — not through these
	// fields. IndexManager was already guarded below for the same reason.
	//
	// The guards are not cosmetic. Without them a plain knowledge-base delete on
	// a control node panics with a nil dereference deep inside Pebble
	// (docstore.(*PebbleDocStore).DeleteByVersion on a nil receiver) and takes
	// the whole process down: measured on the 2-tier container cluster, all three
	// control nodes died mid-cleanup, each left its KB stuck in
	// KB_STATUS_DELETING, and the API had already answered success.
	if c.cfg.DocStore != nil {
		if err := c.cfg.DocStore.DeleteByVersion(ctx, kbID, versionID); err != nil {
			return fmt.Errorf("coordinator: DropVersionStorage: docstore %s v%d: %w", kbID, versionID, err)
		}
	}
	if c.cfg.VersionDocList != nil {
		if err := c.cfg.VersionDocList.DeleteByVersion(ctx, kbID, versionID); err != nil {
			return fmt.Errorf("coordinator: DropVersionStorage: versiondoc %s v%d: %w", kbID, versionID, err)
		}
	}
	// §10.6(4): reclaim the version's index artifacts too — the vecstore-side
	// index object and its persisted files (.index, .ids, .index.mem). A version
	// that reached FAILED_PERMANENT is never queryable and never rebuilt, so a
	// surviving artifact would only linger and skew the disk retention window.
	// Discard is idempotent, and server-side it is a no-op for an index that was
	// never built — so a replica that never received this version can run the
	// same cleanup harmlessly.
	if c.cfg.IndexManager != nil {
		if err := c.cfg.IndexManager.Discard(ctx, kbID, versionID); err != nil {
			return fmt.Errorf("coordinator: DropVersionStorage: index %s v%d: %w", kbID, versionID, err)
		}
	}
	// And the version's document filter. The DeleteVersion flow has always removed it
	// (LocalVersionDropper), but THIS path did not — so a version retired by a terminal
	// verdict left one behind, for good. Nothing else would collect it: the version was
	// never queryable (visibility is the control layer's judgement, not a disk fact) and
	// the reverse reconciliation looks for versions the node holds whose metadata is
	// GONE, while this one is still in the metadata — marked terminal. Idempotent like
	// the layers above, and a replica that never built a filter for the version removes
	// nothing.
	if c.cfg.VersionBloom != nil {
		if err := c.cfg.VersionBloom.DeleteByVersion(kbID, versionID); err != nil {
			return fmt.Errorf("coordinator: DropVersionStorage: bloom %s v%d: %w", kbID, versionID, err)
		}
	}
	return nil
}

// WriteVersionStorage is the storage layer's data-write step for one version
// (steps 3-5): it resolves the KB metadata itself and performs the per-change
// split/embed/writes plus the version document set and bloom filter. It does
// NOT frame the WAL transaction (BEGIN/COMMIT) — the caller owns that framing,
// which is what lets the storage layer's DataPlane run the transaction as its
// own (Stratum_设计文档v13.md §7.12).
// docStages accumulates the per-phase cost of the per-document step across its
// concurrent workers.
//
// The values are SUMS, not wall clock, and that is deliberate: with
// maxConcurrentDocumentWrites in flight they can exceed documents_us, and a sum is
// what answers "where did the work go" — a question a wall-clock number per
// document cannot answer when eight of them overlap.
//
// Why it matters: this step is where an embedder round trip sits, and the write
// path's own notes measure it at ~10 ms of the ~14.6 ms it takes to write one
// document. embed_us is what makes that visible from the outside, and
// embed_calls is its denominator.
type docStages struct {
	splitUs    atomic.Int64
	embedUs    atomic.Int64
	storeUs    atomic.Int64
	embedCalls atomic.Int64

	// The inside of store_us, which the write path had as one number covering
	// three stores with very different costs: the vecstore gRPC round trip per
	// NEW chunk (writeChunkUs), the chunk-existence checks per chunk
	// (chunkCheckUs — a bloom hit costs nothing, a miss costs a gRPC Exists), and
	// the document body (docWriteUs). Which of them dominates decides what a
	// batch API would have to batch; guessing at it would mean rebuilding the C++
	// vecstore for nothing.
	writeChunkUs atomic.Int64
	writeChunkN  atomic.Int64
	chunkCheckUs atomic.Int64
	docWriteUs   atomic.Int64
}

// writeStorageStages is where one version's storage writes went, split out of
// plane's `write: stage timings` storage_us.
//
// Why: storage_us is the largest piece of a write transaction's local half and
// was one opaque number covering four unrelated steps — a control-plane metadata
// read, the per-document split/embed/store, the version's full document-ID set,
// and its bloom filter. Which dominates decides what to optimise.
type writeStorageStages struct {
	getKB     time.Duration
	documents time.Duration
	docList   time.Duration
	bloom     time.Duration
	docs      docStages
}

// WriteVersionStorage implements plane.VersionWriteExecutor: the storage-layer
// steps of one write (steps 3-5): it resolves the KB metadata itself and
// performs the per-change split/embed/writes plus the version document set and
// bloom filter. It does
// NOT frame the WAL transaction (BEGIN/COMMIT) — the caller owns that framing,
// which is what lets the storage layer's DataPlane run the transaction as its
// own (Stratum_设计文档v13.md §7.12).
func (c *WriteCoordinatorImpl) WriteVersionStorage(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) ([]string, error) {
	// Per-phase timings of one version's storage writes, at debug level.
	//
	// Why: this call IS plane's `write: stage timings` storage_us, the largest
	// sub-stage of local_us — itself the second largest stage of a write
	// transaction (p50 1.34 s for a 2,000-document batch, 2.68 s for 8,000). The
	// numbers here say which of its four steps that time belongs to, and, for the
	// per-document step, which phase of a document dominated it.
	storageStart := time.Now()
	var st writeStorageStages
	defer func() {
		c.logger().Debug("write: storage timings",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.Int("changes", len(changes)),
			zap.Int64("getkb_us", st.getKB.Microseconds()),
			zap.Int64("documents_us", st.documents.Microseconds()),
			zap.Int64("doclist_us", st.docList.Microseconds()),
			zap.Int64("bloom_us", st.bloom.Microseconds()),
			zap.Int64("split_us", st.docs.splitUs.Load()),
			zap.Int64("embed_us", st.docs.embedUs.Load()),
			zap.Int64("embed_calls", st.docs.embedCalls.Load()),
			zap.Int64("chunkcheck_us", st.docs.chunkCheckUs.Load()),
			zap.Int64("writechunk_us", st.docs.writeChunkUs.Load()),
			zap.Int64("writechunk_calls", st.docs.writeChunkN.Load()),
			zap.Int64("docwrite_us", st.docs.docWriteUs.Load()),
			zap.Int64("store_us", st.docs.storeUs.Load()),
			zap.Int64("total_us", time.Since(storageStart).Microseconds()))
	}()

	stepStart := time.Now()
	kbMeta, err := c.cfg.RaftNode.GetKB(ctx, kbID)
	st.getKB = time.Since(stepStart)
	if err != nil {
		return nil, fmt.Errorf("coordinator: GetKB: %w", err)
	}
	return c.writeVersionStorage(ctx, kbID, parentVersionID, versionID, changes, kbMeta, &st)
}

// writeVersionStorage executes the synchronous storage-layer steps of the
// write path (3-5) for (kbID, versionID): per-change split/embed/write,
// the version's full document-ID set, the version-document bloom filter,
// and the WAL COMMIT. Shared by Execute and the crash-recovery replay;
// every write is idempotent, so re-running it for an already-partially-
// written version is always safe. Returns the version's sorted docIDs.
func (c *WriteCoordinatorImpl) writeVersionStorage(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange, kbMeta types.KnowledgeBaseMeta, st *writeStorageStages) ([]string, error) {
	// Step 3: Per changed document: split -> embed -> per chunk: bloom
	// test -> exists confirm -> write -> chunk-doc map -> doc store.
	stepStart := time.Now()
	err := c.writeDocumentsConcurrently(ctx, kbID, versionID, changes, kbMeta, &st.docs)
	st.documents = time.Since(stepStart)
	if err != nil {
		return nil, err
	}

	// Step 4: VersionDocList.Write — compute the full document set for
	// the new version from the parent's set + this version's changes.
	stepStart = time.Now()
	docIDs, err := c.writeVersionDocList(ctx, kbID, parentVersionID, versionID, changes)
	st.docList = time.Since(stepStart)
	if err != nil {
		return nil, err
	}

	// Step 5: version-document bloom filter, built from the full docID
	// set and persisted to disk. Non-fatal: a failed persist leaves the
	// filter absent, and the read path rebuilds it lazily from
	// VersionDocList on demand.
	if c.cfg.VersionBloom != nil {
		stepStart = time.Now()
		if _, err := c.cfg.VersionBloom.BuildAndPersist(kbID, versionID, docIDs); err != nil {
			_ = err // non-fatal; read path rebuilds lazily
		}
		st.bloom = time.Since(stepStart)
	}

	return docIDs, nil
}

// maxConcurrentDocumentWrites bounds how many documents step 3 processes at once.
//
// The per-document work is independent and dominated by an embed round trip, so
// the parallelism is real; the bound exists because the embedder is a shared
// external service, and unbounded fan-out here would turn one large write into
// everyone else's outage. Eight is deliberately modest: it already takes a
// 1000-document batch from ~14.6 s of serialised embedding to a couple of
// seconds, and the point is to stop queueing, not to saturate the embedder.
const maxConcurrentDocumentWrites = 8

// writeDocumentsConcurrently runs step 3 for every change with bounded
// concurrency and returns the first failure.
//
// Why: each change touches only its own content, chunks, chunk-document mapping
// and doc-store entry, and each one waits on an embed call — measured at ~14.6 ms
// per document for a 1000-document batch, 10 ms of which was the embedder round
// trip. Serially that is 14.6 s inside a single candidate's dispatch budget
// (15 s floor + 50 ms/document), so a large batch was spending its budget
// queueing rather than working.
//
// The ordering that matters is preserved: the version's document-ID set and
// bloom filter are still written by the caller AFTER every change has settled,
// so a partly-written version is never published as complete. Every individual
// write is idempotent (see writeVersionStorage), so a failure that cancels the
// rest leaves a version a later attempt can finish — not a corrupt one.
func (c *WriteCoordinatorImpl) writeDocumentsConcurrently(ctx context.Context, kbID string, versionID int64, changes []types.DocChange, kbMeta types.KnowledgeBaseMeta, st *docStages) error {
	if st == nil {
		st = &docStages{}
	}
	// One presence cache for the whole version. Its lifetime is exactly this call
	// — one version's documents — which is the window in which a chunk's presence
	// cannot change except through this write. See chunkPresenceCache.
	presence := &chunkPresenceCache{}
	if len(changes) <= 1 {
		// Nothing to overlap. Keeping the single-document case free of the
		// coordination below matters because it is the interactive-write common
		// case, and it is what every existing order-sensitive test exercises.
		for _, change := range changes {
			if err := c.writeOneChange(ctx, kbID, versionID, change, kbMeta, st, presence); err != nil {
				return err
			}
		}
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, maxConcurrentDocumentWrites)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for _, change := range changes {
		if ctx.Err() != nil {
			break // an earlier failure already stopped this write
		}
		sem <- struct{}{} // bounds how many embeddings are in flight
		wg.Add(1)
		go func(change types.DocChange) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := c.writeOneChange(ctx, kbID, versionID, change, kbMeta, st, presence); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel() // stop the rest: this version is not going to publish
				}
				mu.Unlock()
			}
		}(change)
	}
	wg.Wait()
	return firstErr
}

// writeOneChange is step 3 for a single change: an add/update goes through the
// split -> embed -> write path, a delete writes a tombstone.
func (c *WriteCoordinatorImpl) writeOneChange(ctx context.Context, kbID string, versionID int64, change types.DocChange, kbMeta types.KnowledgeBaseMeta, st *docStages, presence *chunkPresenceCache) error {
	switch change.Op {
	case types.ChangeOpAdd, types.ChangeOpUpdate:
		return c.writeDocument(ctx, kbID, versionID, change, kbMeta, st, presence)
	case types.ChangeOpDelete:
		// Write a tombstone for the deleted document.
		if err := c.retry(ctx, func() error {
			return c.cfg.DocStore.Write(ctx, kbID, change.DocID, versionID, nil)
		}); err != nil {
			return fmt.Errorf("coordinator: write tombstone for %s: %w", change.DocID, err)
		}
	}
	return nil
}

// writeDocument handles a single ADD or UPDATE document change: split, embed
// what is new, write chunks + mappings + doc content. st, when non-nil,
// accumulates this document's split/embed/store cost; see docStages for why
// those are sums rather than wall clock.
func (c *WriteCoordinatorImpl) writeDocument(ctx context.Context, kbID string, versionID int64, change types.DocChange, kbMeta types.KnowledgeBaseMeta, st *docStages, presence *chunkPresenceCache) error {
	// Split, with the algorithm this knowledge base was created with: the
	// splitter is shared by every KB on the node, so the mode travels with the
	// call (docs/content-defined-chunking-plan.md §3.8).
	params := kbMeta.Chunking()
	splitStart := time.Now()
	chunks := c.cfg.Splitter.Split(change.Content, params, kbMeta.EmbedConfig.ModelID)
	if st != nil {
		st.splitUs.Add(time.Since(splitStart).Microseconds())
	}

	if len(chunks) == 0 {
		// No chunks produced (e.g. empty content): just write the document
		// content (or tombstone for empty content).
		storeStart := time.Now()
		err := c.retry(ctx, func() error {
			return c.cfg.DocStore.Write(ctx, kbID, change.DocID, versionID, []byte(change.Content))
		})
		if st != nil {
			st.storeUs.Add(time.Since(storeStart).Microseconds())
		}
		return err
	}

	// Embed, but only the chunks this KB does not already hold.
	//
	// The chunk store is content-addressed, so a chunk the KB already has needs
	// neither a fresh embedding nor a fresh vector — only its chunk-document
	// mapping. Asking the embed service for it anyway is what used to make a
	// higher reuse rate worth nothing in compute; filtering here is what turns
	// the reuse into skipped embed calls (docs/content-defined-chunking-plan.md
	// §4). The filter asks exactly the question writeChunk asks (chunkPresent),
	// so the two cannot disagree.
	//
	// This is a per-node optimisation, not global dedup: the bloom filter that
	// answers it is local, so a chunk another node stored is still embedded
	// here (§4.3).
	pending := make([]types.Chunk, 0, len(chunks))
	alreadyPresent := 0
	for _, chunk := range chunks {
		chunkCheckStart := time.Now()
		present, err := c.chunkPresent(ctx, kbID, chunk.ChunkID, presence)
		if st != nil {
			st.chunkCheckUs.Add(time.Since(chunkCheckStart).Microseconds())
		}
		if err != nil {
			return err
		}
		if present {
			alreadyPresent++
			continue
		}
		pending = append(pending, chunk)
	}

	var vectors map[string][]float32
	if len(pending) > 0 {
		embedStart := time.Now()
		err := c.retry(ctx, func() error {
			var inner error
			vectors, inner = c.cfg.EmbedClient.Embed(ctx, pending)
			return inner
		})
		if st != nil {
			st.embedUs.Add(time.Since(embedStart).Microseconds())
			st.embedCalls.Add(1)
		}
		if err != nil {
			return fmt.Errorf("coordinator: embed chunks for doc %s: %w", change.DocID, err)
		}
	}

	// Store the new vectors. writeChunk re-checks existence: a concurrent write
	// of the same chunk (another document, another version) can store it
	// between the filter above and here, and that check is what keeps the two
	// paths from storing one chunk twice.
	storeStart := time.Now()
	lateHits := 0
	for _, chunk := range pending {
		vector, ok := vectors[chunk.ChunkID]
		if !ok {
			return fmt.Errorf("coordinator: embed did not return vector for chunk %s", chunk.ChunkID)
		}
		writeChunkStart := time.Now()
		existed, err := c.writeChunk(ctx, kbID, chunk, vector, presence)
		if st != nil {
			st.writeChunkUs.Add(time.Since(writeChunkStart).Microseconds())
			st.writeChunkN.Add(1)
		}
		if err != nil {
			return err
		}
		if existed {
			lateHits++
		}
	}

	// Map every chunk — the reused ones included — to this document, in ONE
	// durable commit rather than one per chunk (idempotent either way).
	chunkIDs := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		chunkIDs = append(chunkIDs, chunk.ChunkID)
	}
	if err := c.retry(ctx, func() error {
		return c.cfg.ChunkDocMapper.WriteMany(ctx, kbID, change.DocID, chunkIDs)
	}); err != nil {
		return fmt.Errorf("coordinator: chunk-doc map write: %w", err)
	}

	// Write document content.
	docWriteStart := time.Now()
	docWriteErr := c.retry(ctx, func() error {
		return c.cfg.DocStore.Write(ctx, kbID, change.DocID, versionID, []byte(change.Content))
	})
	if st != nil {
		st.docWriteUs.Add(time.Since(docWriteStart).Microseconds())
	}
	if docWriteErr != nil {
		return fmt.Errorf("coordinator: doc store write: %w", docWriteErr)
	}
	if st != nil {
		st.storeUs.Add(time.Since(storeStart).Microseconds())
	}

	// Reuse observation (docs/content-defined-chunking-plan.md §5). The two
	// counts differ exactly when a concurrent writer stored a chunk after the
	// pre-embed filter: those were reused, but their embedding was paid for
	// already.
	c.logger().Info("write: document stored",
		zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
		zap.String("doc_id", change.DocID),
		zap.String("chunk_mode", params.Mode.String()),
		zap.Int("chunks", len(chunks)),
		zap.Int("chunks_already_present", alreadyPresent+lateHits),
		zap.Int("embed_skipped", alreadyPresent))

	return nil
}

// chunkPresent reports whether kbID's chunk store already holds chunkID.
//
// It is the write path's single answer to that question — whether the caller is
// filtering before an embed call or confirming just before a write. The bloom
// filter only hints: it can answer "maybe" for a chunk that is not there, and it
// is per-node state that starts empty after a restart. So a hint is always
// confirmed against the authoritative store.
func (c *WriteCoordinatorImpl) chunkPresent(ctx context.Context, kbID, chunkID string, presence *chunkPresenceCache) (bool, error) {
	if presence != nil && presence.isPresent(chunkID) {
		return true, nil
	}
	if !c.cfg.ChunkBloom.Test(chunkID) {
		return false, nil
	}
	exists, err := c.cfg.ChunkStore.Exists(ctx, kbID, chunkID)
	if err != nil {
		return false, fmt.Errorf("coordinator: ChunkStore.Exists for %s: %w", chunkID, err)
	}
	if exists && presence != nil {
		presence.notePresent(chunkID)
	}
	return exists, nil
}

// chunkPresenceCache remembers, for the life of ONE version's write, which chunks
// a single authoritative check has already confirmed present.
//
// Why it exists: a version's documents share chunks — that is what content
// addressing means, and the more repetitive the corpus the more they share — yet
// the write path asked the vecstore about every (document, chunk) pair
// separately. Measured on the 3+3 cluster with a 1,000-document batch,
// chunkPresent was 51.4% of the per-document step while the chunk WRITES in that
// same step were 38 calls: the bulk of it was ChunkStore.Exists asking the same
// question about the same chunk hundreds of times.
//
// Only PRESENT is remembered, deliberately. A bloom miss already answers "not
// here" locally and for free, so caching it buys nothing — and it could go stale
// inside the same write: a chunk another document just stored has become
// present, and a remembered "absent" would make the next document embed it again.
//
// Concurrent workers can each miss on the same chunk and both ask the store; that
// costs a few duplicate round trips at the start of a batch and keeps the cache
// lock-free, which is the right trade at this scale.
type chunkPresenceCache struct {
	present sync.Map // chunkID -> struct{}
}

func (c *chunkPresenceCache) isPresent(chunkID string) bool {
	_, ok := c.present.Load(chunkID)
	return ok
}

func (c *chunkPresenceCache) notePresent(chunkID string) {
	c.present.Store(chunkID, struct{}{})
}

// writeChunk stores one chunk's vector unless the store already holds it, and
// reports whether it was already there (writeDocument counts those).
func (c *WriteCoordinatorImpl) writeChunk(ctx context.Context, kbID string, chunk types.Chunk, vector []float32, presence *chunkPresenceCache) (bool, error) {
	present, err := c.chunkPresent(ctx, kbID, chunk.ChunkID, presence)
	if err != nil {
		return false, err
	}
	if present {
		return true, nil // already stored; nothing to do
	}

	// Write chunk to vecstore.
	if err := c.retry(ctx, func() error {
		return c.cfg.ChunkStore.Write(ctx, kbID, chunk.ChunkID, vector)
	}); err != nil {
		return false, fmt.Errorf("coordinator: ChunkStore.Write for %s: %w", chunk.ChunkID, err)
	}

	// Add to bloom filter, and to the in-flight cache: the chunk is present now,
	// and the documents that share it should not ask the store again.
	c.cfg.ChunkBloom.Add(chunk.ChunkID)
	if presence != nil {
		presence.notePresent(chunk.ChunkID)
	}
	return false, nil
}

// writeVersionDocList computes the new version's full document ID set by
// taking the parent version's set and applying this version's changes.
// It writes the set into VersionDocList and returns the sorted docIDs so
// the caller can compute the version's document-ID set digest.
func (c *WriteCoordinatorImpl) writeVersionDocList(ctx context.Context, kbID string, parentVersionID, newVersionID int64, changes []types.DocChange) ([]string, error) {
	// Get parent version's full doc set.
	parentDocs := make(map[string]bool)
	if parentVersionID != 0 {
		docIDs, err := c.cfg.VersionDocList.ListDocIDs(ctx, kbID, parentVersionID)
		if err != nil {
			return nil, fmt.Errorf("coordinator: list parent version %d docs: %w", parentVersionID, err)
		}
		for _, id := range docIDs {
			parentDocs[id] = true
		}
	}

	// Apply changes.
	for _, ch := range changes {
		switch ch.Op {
		case types.ChangeOpAdd, types.ChangeOpUpdate:
			parentDocs[ch.DocID] = true
		case types.ChangeOpDelete:
			delete(parentDocs, ch.DocID)
		}
	}

	// Write the new version's full doc-ID set in ONE durable commit.
	//
	// It used to go one Write per docID — one durable commit each — measured at
	// 0.42 ms per document and 29% of a version's whole storage write (423 ms of
	// 1.44 s for a 1,000-document batch), paid on every replica. The set is
	// written whole and the writes are idempotent, so a single batch is both
	// faster and no less safe: the retry wraps the batch, and re-running it
	// rewrites identical keys.
	docIDs := make([]string, 0, len(parentDocs))
	for docID := range parentDocs {
		docIDs = append(docIDs, docID)
	}
	sort.Strings(docIDs)
	if err := c.retry(ctx, func() error {
		return c.cfg.VersionDocList.WriteMany(ctx, kbID, newVersionID, docIDs)
	}); err != nil {
		return nil, fmt.Errorf("coordinator: version doc list write: %w", err)
	}

	return docIDs, nil
}

// retry executes fn with exponential backoff up to MaxRetries times.
func (c *WriteCoordinatorImpl) retry(ctx context.Context, fn func() error) error {
	base := time.Duration(c.cfg.RetryBaseIntervalMS) * time.Millisecond

	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt < c.cfg.MaxRetries {
			backoff := base * time.Duration(int64(math.Pow(2, float64(attempt))))
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return fmt.Errorf("retry exhausted after %d attempts: %w", c.cfg.MaxRetries+1, lastErr)
}

var _ WriteCoordinator = (*WriteCoordinatorImpl)(nil)
