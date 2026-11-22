package coordinator

import (
	"context"
	"fmt"
	"sort"
	"sync"
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

// WriteRaft is the Raft subset WriteCoordinatorImpl and its LocalControlPlane
// use: cluster leadership reads, metadata reads, and version proposals.
type WriteRaft interface {
	raft.ClusterReader
	raft.MetaReader
	raft.VersionWriter
}

// WriteCoordinatorConfig bundles all dependencies and configuration for
// WriteCoordinatorImpl, following the constructor-injection convention.
type WriteCoordinatorConfig struct {
	MaxRetries          int
	RetryBaseIntervalMS int

	WAL      wal.WAL
	RaftNode WriteRaft

	Splitter    splitter.ChunkSplitter
	EmbedClient embed.EmbedClient

	// ChunkBloom is the per-KB chunk-existence bloom filter. Used on the
	// write path to skip ChunkStore.Exists round-trips for new chunks.
	ChunkBloom bloom.BloomFilter

	// VersionViews reclaims a version's cached read view (L0 + L1 .vview file)
	// when DropVersionStorage drops a failed version. Optional.
	VersionViews VersionViewCleaner

	ChunkStore     chunkstore.ChunkStore
	ChunkDocMapper chunkdoc.ChunkDocMapper
	DocStore       docstore.DocStore
	VersionDocList versiondoc.VersionDocList
	IndexManager   index.IndexManager

	// ChunkReclaimer reclaims the chunk vectors a deleted version was the last
	// live referent of (the event-driven half of the orphan-chunk GC). Optional:
	// DropVersionStorage runs it BEFORE dropping the version's document list, which
	// is what names the candidate chunks, and reports its failure like any other
	// layer's — the version's tombstone stays PENDING on the receiving node, so its
	// retry loop runs this again.
	ChunkReclaimer VersionChunkReclaimer

	// DataPlane runs the storage layer's write transaction (BEGIN → storage
	// writes → COMMIT) and reports its outcome back up
	// (docs/已实施/control-data-separation-design.md §5.1; the split write path of
	// docs/参考/历史/Stratum_设计文档v13.md §7.12). When nil, the constructor assembles the
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

	// DispatchClaimWindow is how long a background dispatch's registration claim
	// keeps explaining a later miss by the apply hook (zero takes
	// dispatchClaimWindow).
	DispatchClaimWindow time.Duration

	// AbandonReportTimeout bounds the Raft round trip that records a give-up
	// (zero takes abandonReportTimeout).
	AbandonReportTimeout time.Duration

	// MaxConcurrentDocumentWrites bounds how many documents one version's write
	// processes at once (zero takes maxConcurrentDocumentWrites).
	MaxConcurrentDocumentWrites int

	// Locks hands out the per-knowledge-base write lock, held for a whole
	// CreateVersion transaction (BEGIN through COMMIT). It is shared with the
	// orphan-chunk garbage collector so the GC's reclaim phase (current-version
	// re-check + mapping/vector deletion) is mutually exclusive with a write to the
	// SAME knowledge base — closing the stale-snapshot race where a sweep erases data
	// committed by a newer version — while leaving unrelated knowledge bases free to
	// write in parallel (M7 of docs/参考/code-review-2026-09-24.md). When nil, the
	// coordinator allocates a private set; callers that also run a
	// ChunkGarbageCollector MUST inject the same instance into both.
	Locks *KBLockSet
}

// WriteCoordinatorImpl is the real WriteCoordinator implementation,
// orchestrating the full CreateVersion write path (steps 1-7) as documented
// in docs/参考/历史/Stratum_接口设计v9.md "CreateVersion" and docs/参考/历史/Stratum_设计文档v10.md "写路径".
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
	// every other KB's network hop (M7 of docs/参考/code-review-2026-09-24.md).
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

// NewWriteCoordinatorImpl constructs a WriteCoordinatorImpl.
func NewWriteCoordinatorImpl(cfg WriteCoordinatorConfig) *WriteCoordinatorImpl {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBaseIntervalMS <= 0 {
		cfg.RetryBaseIntervalMS = 100
	}
	if cfg.DispatchClaimWindow <= 0 {
		cfg.DispatchClaimWindow = dispatchClaimWindow
	}
	if cfg.AbandonReportTimeout <= 0 {
		cfg.AbandonReportTimeout = abandonReportTimeout
	}
	if cfg.MaxConcurrentDocumentWrites <= 0 {
		cfg.MaxConcurrentDocumentWrites = maxConcurrentDocumentWrites
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

// NewDispatchID mints an idempotency key for a write that arrived without one.
//
// Exported because the service layer needs the same generator: it stamps the key
// onto CreateVersionResponse, so the format a caller is handed is the format the
// coordinator's own log lines show (docs/已实施/await-version-plan.md §7 Step 4).
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
	// documents" (docs/已实施/cursor-persistence-plan.md §5): a new version's document set
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
		// index build (docs/已实施/control-data-separation-design.md §5.1; the split write
		// path of docs/参考/历史/Stratum_设计文档v13.md §7.12).
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

// logger returns the configured logger, or a no-op one.
func (c *WriteCoordinatorImpl) logger() *zap.Logger {
	if c.cfg.Logger != nil {
		return c.cfg.Logger
	}
	return zap.NewNop()
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
		// "This node holds no data for the parent" and "the parent has no
		// documents" both come back from ListDocIDs as an empty slice, and the
		// difference decides whether the child version is correct (H1 of
		// docs/参考/code-review-2026-10-18.md). It matters under the dispatch topology:
		// the candidate set is "every replica of the KB", with no check that the
		// candidate actually holds the parent version, so a replica whose fan-out
		// failed — or that restarted, or just joined — would derive the child's
		// document set from nothing. Every inherited document would then vanish from
		// the new version, and because the bogus set is what gets hashed into
		// DocIDSetHash and fanned out, the whole cluster would agree on it.
		if err := c.verifyParentDocSet(ctx, kbID, parentVersionID, docIDs); err != nil {
			return nil, err
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
	return retryWithBackoff(ctx, c.cfg.MaxRetries, c.cfg.RetryBaseIntervalMS, fn)
}

var _ WriteCoordinator = (*WriteCoordinatorImpl)(nil)
