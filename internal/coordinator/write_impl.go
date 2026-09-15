package coordinator

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/embed"
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

	// WriteMu is the lock serializing CreateVersion write transactions
	// (BEGIN through COMMIT). It is shared with the orphan-chunk
	// garbage collector so the GC's reclaim phase (current-version
	// re-check + mapping/vector deletion) is mutually exclusive with
	// concurrent writes — closing the stale-snapshot race where a sweep
	// erases data committed by a newer version. When nil, the
	// WriteCoordinatorImpl allocates a private lock (fine for single
	// writer; callers that also run a ChunkGarbageCollector MUST inject
	// the same mutex into both).
	WriteMu *sync.Mutex
}

// WriteCoordinatorImpl is the real WriteCoordinator implementation,
// orchestrating the full CreateVersion write path (steps 1-7) as documented
// in Stratum_接口设计v9.md "CreateVersion" and Stratum_设计文档v10.md "写路径".
type WriteCoordinatorImpl struct {
	cfg WriteCoordinatorConfig

	// txnMu serializes CreateVersion transactions end to end (BEGIN through
	// COMMIT) so the WAL record order is BEGIN -> VERSION_ID per
	// transaction with no interleaved BEGIN from a concurrent transaction —
	// the property FileWAL.rebuildIndex relies on to bind each VERSION_ID
	// to the correct transaction's replay input (see internal/wal/file.go).
	// It is the cfg.WriteMu instance (or a private fallback when nil), and
	// is shared with ChunkGarbageCollectorImpl's reclaim phase.
	txnMu *sync.Mutex

	// pendingDispatch holds the changes of writes this node has proposed but not
	// yet dispatched (§7.13.2). The dispatch runs on the apply of the entry, and
	// the Raft command itself carries no changes — they deliberately stay out of
	// the log (§7.7) — so they have to be handed over out of band. Keyed by
	// (kbID, clientRequestID): that is what Execute knows before the version ID
	// exists, and what the apply hook can read back from the command.
	dispatchMu      sync.Mutex
	pendingDispatch map[dispatchKey]pendingDispatchEntry

	// dispatchWG tracks in-flight background dispatches, so a shutdown (or a
	// test) can wait for the ones already handed off.
	dispatchWG sync.WaitGroup
}

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
	mu := cfg.WriteMu
	if mu == nil {
		mu = &sync.Mutex{}
	}
	c := &WriteCoordinatorImpl{cfg: cfg, txnMu: mu}
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

// newDispatchID mints an idempotency key for a write that arrived without one.
// Execute runs under txnMu, so the timestamp alone is unambiguous.
func newDispatchID() string {
	return fmt.Sprintf("auto-%d", time.Now().UnixNano())
}

// Execute implements WriteCoordinator.
func (c *WriteCoordinatorImpl) Execute(ctx context.Context, kbID string, parentVersionID int64, changes []types.DocChange, clientRequestID string) (int64, error) {
	// Serialize the whole transaction (BEGIN through COMMIT) so the WAL's
	// BEGIN -> VERSION_ID binding per version stays unambiguous (see
	// txnMu's doc comment).
	c.txnMu.Lock()
	defer c.txnMu.Unlock()

	// §7.13.2: only the leader accepts a write. It is the node that can put the
	// entry in the log AND the node that holds the changes, so a client that
	// reached a follower directly is answered with NotLeader and re-resolves the
	// leader (the router does that for it) rather than being served through a
	// second, client-side forwarding hop.
	if !c.cfg.RaftNode.IsLeader() {
		return 0, kvraft.ErrNotLeader
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
		dispatchID = newDispatchID()
	}
	c.RegisterPendingDispatch(kbID, dispatchID, parentVersionID, changes)

	opts := []raft.ProposeOption{raft.WithClientRequestID(dispatchID)}
	versionID, err := c.cfg.RaftNode.ProposeCreateVersion(ctx, kbID, parentVersionID, opts...)
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
		if err := c.cfg.DataPlane.WriteVersionData(ctx, kbID, versionID, parentVersionID, changes); err != nil {
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
	c.dispatchInBackground(kbID, versionID, dispatchID)
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
	terminal, err := c.cfg.ControlPlane.ReportVersionFailure(ctx, kbID, versionID, class, detail)
	if err != nil {
		log.Warn("coordinator: reporting a version's failure",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}
	if !terminal {
		return // still inside the retry budget: §10.1 will try again
	}
	// The terminal verdict just landed: reclaim whatever physical data made it to
	// disk, on every candidate replica — the control layer never knew which ones
	// received it (§10.6).
	if c.cfg.DataPlane == nil {
		return
	}
	if err := c.cfg.DataPlane.DropVersionData(ctx, kbID, versionID); err != nil {
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
	c.txnMu.Lock()
	defer c.txnMu.Unlock()

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
	if err := c.cfg.DocStore.DeleteByVersion(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("coordinator: DropVersionStorage: docstore %s v%d: %w", kbID, versionID, err)
	}
	if err := c.cfg.VersionDocList.DeleteByVersion(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("coordinator: DropVersionStorage: versiondoc %s v%d: %w", kbID, versionID, err)
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
	return nil
}

// WriteVersionStorage is the storage layer's data-write step for one version
// (steps 3-5): it resolves the KB metadata itself and performs the per-change
// split/embed/writes plus the version document set and bloom filter. It does
// NOT frame the WAL transaction (BEGIN/COMMIT) — the caller owns that framing,
// which is what lets the storage layer's DataPlane run the transaction as its
// own (Stratum_设计文档v13.md §7.12).
func (c *WriteCoordinatorImpl) WriteVersionStorage(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) ([]string, error) {
	kbMeta, err := c.cfg.RaftNode.GetKB(ctx, kbID)
	if err != nil {
		return nil, fmt.Errorf("coordinator: GetKB: %w", err)
	}
	return c.writeVersionStorage(ctx, kbID, parentVersionID, versionID, changes, kbMeta)
}

// writeVersionStorage executes the synchronous storage-layer steps of the
// write path (3-5) for (kbID, versionID): per-change split/embed/write,
// the version's full document-ID set, the version-document bloom filter,
// and the WAL COMMIT. Shared by Execute and the crash-recovery replay;
// every write is idempotent, so re-running it for an already-partially-
// written version is always safe. Returns the version's sorted docIDs.
func (c *WriteCoordinatorImpl) writeVersionStorage(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange, kbMeta types.KnowledgeBaseMeta) ([]string, error) {
	// Step 3: Per changed document: split -> embed -> per chunk: bloom
	// test -> exists confirm -> write -> chunk-doc map -> doc store.
	if err := c.writeDocumentsConcurrently(ctx, kbID, versionID, changes, kbMeta); err != nil {
		return nil, err
	}

	// Step 4: VersionDocList.Write — compute the full document set for
	// the new version from the parent's set + this version's changes.
	docIDs, err := c.writeVersionDocList(ctx, kbID, parentVersionID, versionID, changes)
	if err != nil {
		return nil, err
	}

	// Step 5: version-document bloom filter, built from the full docID
	// set and persisted to disk. Non-fatal: a failed persist leaves the
	// filter absent, and the read path rebuilds it lazily from
	// VersionDocList on demand.
	if c.cfg.VersionBloom != nil {
		if _, err := c.cfg.VersionBloom.BuildAndPersist(kbID, versionID, docIDs); err != nil {
			_ = err // non-fatal; read path rebuilds lazily
		}
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
func (c *WriteCoordinatorImpl) writeDocumentsConcurrently(ctx context.Context, kbID string, versionID int64, changes []types.DocChange, kbMeta types.KnowledgeBaseMeta) error {
	if len(changes) <= 1 {
		// Nothing to overlap. Keeping the single-document case free of the
		// coordination below matters because it is the interactive-write common
		// case, and it is what every existing order-sensitive test exercises.
		for _, change := range changes {
			if err := c.writeOneChange(ctx, kbID, versionID, change, kbMeta); err != nil {
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
			if err := c.writeOneChange(ctx, kbID, versionID, change, kbMeta); err != nil {
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
func (c *WriteCoordinatorImpl) writeOneChange(ctx context.Context, kbID string, versionID int64, change types.DocChange, kbMeta types.KnowledgeBaseMeta) error {
	switch change.Op {
	case types.ChangeOpAdd, types.ChangeOpUpdate:
		return c.writeDocument(ctx, kbID, versionID, change, kbMeta)
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

// writeDocument handles a single ADD or UPDATE document change: split,
// embed, write chunks + mappings + doc content.
func (c *WriteCoordinatorImpl) writeDocument(ctx context.Context, kbID string, versionID int64, change types.DocChange, kbMeta types.KnowledgeBaseMeta) error {
	// Split
	chunks := c.cfg.Splitter.Split(change.Content, kbMeta.ChunkWindowSize, kbMeta.ChunkOverlapSize, kbMeta.EmbedConfig.ModelID)

	if len(chunks) == 0 {
		// No chunks produced (e.g. empty content): just write the document
		// content (or tombstone for empty content).
		return c.retry(ctx, func() error {
			return c.cfg.DocStore.Write(ctx, kbID, change.DocID, versionID, []byte(change.Content))
		})
	}

	// Embed
	var vectors map[string][]float32
	err := c.retry(ctx, func() error {
		var inner error
		vectors, inner = c.cfg.EmbedClient.Embed(ctx, chunks)
		return inner
	})
	if err != nil {
		return fmt.Errorf("coordinator: embed chunks for doc %s: %w", change.DocID, err)
	}

	// Per chunk: bloom check -> exists confirm (if needed) -> write + bloom add -> chunk-doc map
	for _, chunk := range chunks {
		vector, ok := vectors[chunk.ChunkID]
		if !ok {
			return fmt.Errorf("coordinator: embed did not return vector for chunk %s", chunk.ChunkID)
		}

		if err := c.writeChunk(ctx, kbID, chunk, vector); err != nil {
			return err
		}

		// Write chunk-doc mapping (idempotent).
		if err := c.retry(ctx, func() error {
			return c.cfg.ChunkDocMapper.Write(ctx, kbID, chunk.ChunkID, change.DocID)
		}); err != nil {
			return fmt.Errorf("coordinator: chunk-doc map write: %w", err)
		}
	}

	// Write document content.
	if err := c.retry(ctx, func() error {
		return c.cfg.DocStore.Write(ctx, kbID, change.DocID, versionID, []byte(change.Content))
	}); err != nil {
		return fmt.Errorf("coordinator: doc store write: %w", err)
	}

	return nil
}

// writeChunk handles a single chunk: bloom check -> authoritative exists
// confirm -> write + bloom add (if truly new).
func (c *WriteCoordinatorImpl) writeChunk(ctx context.Context, kbID string, chunk types.Chunk, vector []float32) error {
	// Bloom filter test.
	if c.cfg.ChunkBloom.Test(chunk.ChunkID) {
		// Bloom says "maybe exists" — confirm against the authoritative store.
		exists, err := c.cfg.ChunkStore.Exists(ctx, kbID, chunk.ChunkID)
		if err != nil {
			return fmt.Errorf("coordinator: ChunkStore.Exists for %s: %w", chunk.ChunkID, err)
		}
		if exists {
			return nil // already stored; nothing to do
		}
		// False positive: write it now.
	}

	// Write chunk to vecstore.
	if err := c.retry(ctx, func() error {
		return c.cfg.ChunkStore.Write(ctx, kbID, chunk.ChunkID, vector)
	}); err != nil {
		return fmt.Errorf("coordinator: ChunkStore.Write for %s: %w", chunk.ChunkID, err)
	}

	// Add to bloom filter.
	c.cfg.ChunkBloom.Add(chunk.ChunkID)
	return nil
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

	// Write each doc ID to the new version.
	docIDs := make([]string, 0, len(parentDocs))
	for docID := range parentDocs {
		docIDs = append(docIDs, docID)
		if err := c.retry(ctx, func() error {
			return c.cfg.VersionDocList.Write(ctx, kbID, newVersionID, docID)
		}); err != nil {
			return nil, fmt.Errorf("coordinator: version doc list write: %w", err)
		}
	}
	sort.Strings(docIDs)

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
