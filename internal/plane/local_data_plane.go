package plane

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	stratinternalsync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// VersionPuller is the storage layer's data-transfer primitive: it copies one
// version's storage-layer records (documents, chunk↔doc mapping, version doc
// list, chunks) onto this node. *stratumsync.Follower implements it; keeping
// it as an interface here lets the DataPlane be exercised without a live peer.
type VersionPuller interface {
	PullVersion(ctx context.Context, sourceAddr string, kbID string, versionID int64) error
	// PullVersionData fetches the version's data without scheduling an index
	// build (see FetchVersionData).
	PullVersionData(ctx context.Context, sourceAddr string, kbID string, versionID int64) error
}

// VersionChangesFetcher fetches the changes a peer recorded over a version range —
// the client side of §7.5's delta backfill. *sync.VersionChangesPuller implements
// it; declared here so this package does not depend on the transport.
type VersionChangesFetcher interface {
	ChangesInRange(ctx context.Context, peerAddr, kbID string, fromExclusive, toInclusive int64) (map[int64]wal.VersionDelta, error)
}

// DataVerifier reports whether this node's local stores already hold the
// complete data for (kbID, versionID). The write path commits a document-set
// digest for the version, and this is how the storage layer checks a pull
// against it.
type DataVerifier func(ctx context.Context, kbID string, versionID int64) bool

// SourceResolver answers "which peer holds this version's data right now?".
// ok=false means there is nothing to fetch: either this node is the writer
// (the data landed through the write path) or no source is known yet.
//
// It is deliberately a cheap, non-probing lookup: this runs on the Raft apply
// path (via onVersionCreated during log replay), so any blocking RPC here would
// stall every later log entry. §8.5's "any node can coordinate a write" is
// built on top of that constraint rather than around it: a writer announces
// itself, and this lookup reads the announcement (see DataSourceRegistry)
// before falling back to the leader.
type SourceResolver func(ctx context.Context, kbID string, versionID int64) (addr string, ok bool, err error)

// VersionPusher replicates one version's records to a peer replica and returns
// that replica's node ID (its acknowledgement). *stratumsync.Pusher satisfies
// it through a small adapter in the node assembly; keeping the interface this
// narrow keeps the DataPlane independent of the transport.
type VersionPusher interface {
	PushVersion(ctx context.Context, targetAddr, kbID string, versionID int64) (acceptorID int64, err error)
}

// IndexShipper ships one version's built index to a peer
// (DataSyncService.PushIndexData). *sync.IndexPusher implements it.
type IndexShipper interface {
	PushIndex(ctx context.Context, targetAddr, kbID string, versionID int64, indexData, sidecarData []byte) error
	// ProbeIndex asks a peer whether it already holds (kbID, versionID)'s
	// artifact, WITHOUT shipping it, so the caller can drop that peer before
	// reading the file it would have shipped (§8.4(a)). A peer that cannot
	// answer is reported as an error and the caller ships to it anyway.
	ProbeIndex(ctx context.Context, targetAddr, kbID string, versionID int64) (bool, error)
}

// IndexReader reads a version's persisted index files from this node's disk.
// *index.IndexManagerImpl implements it.
type IndexReader interface {
	ReadIndexFiles(kbID string, versionID int64) (indexData, sidecarData []byte, err error)
}

// WriteConfirmer tells a replica that a version it received did reach quorum,
// so its §7.3 takeover timer can stand down. The same message carries the
// writer's own DataSyncService address (sourceAddr) — §8.5's announcement: the
// replica records it, so a later reader knows where the data is instead of
// having to ask the leader. *sync.ConfirmBroadcaster implements it.
type WriteConfirmer interface {
	ConfirmVersionWrite(ctx context.Context, peerAddr, kbID string, versionID int64, sourceAddr string) error
}

// VersionPresenceQuerier asks a peer whether it holds a version's data
// (DataSyncService.VersionPresence). *sync.PresenceChecker implements it.
type VersionPresenceQuerier interface {
	HasVersion(ctx context.Context, peerAddr, kbID string, versionID int64) (bool, error)
}

// VersionDigest computes a version's document-set digest from this node's own
// stores. The §7.3 takeover needs it: a replica announcing "V is durable" must
// announce the same digest the coordinator would have, or followers verifying
// a pulled version against it would reject data that is perfectly fine.
type VersionDigest interface {
	DigestOf(ctx context.Context, kbID string, versionID int64) (string, error)
}

// VersionDataDropper removes one version's physical data on this node. The
// write path reaches the stores through VersionWriteExecutor; dropping gets its
// own narrow seam so the cleanup of §10.6 does not depend on the whole write
// surface.
type VersionDataDropper interface {
	DropVersionStorage(ctx context.Context, kbID string, versionID int64) error
}

// VersionDataCleaner asks one peer to reclaim a version's physical data
// (DataSyncService.DeleteVersionData). *sync.VersionDataCleaner implements it.
type VersionDataCleaner interface {
	DeleteVersionData(ctx context.Context, peerAddr, kbID string, versionID int64, reason string) error
}

// CursorQuerier asks a peer how far its contiguous history reaches, so a node
// that is backfilling can pick a source that actually holds the gap instead of
// always asking the leader — which fails when the leader is the one that is
// behind (Stratum_设计文档v13.md §7.6). *sync.LocalVersionQuerier implements it;
// keeping it an interface keeps the DataPlane independent of the transport.
type CursorQuerier interface {
	LocalVersionOf(ctx context.Context, peerAddr, kbID string) (int64, error)
}

// ReplicaResolver answers "which other replicas should hold this version?".
// Nil means replication is not configured (a single-node deployment, or a
// test), in which case the local write is the whole quorum.
//
// The target list is storage-layer topology, so it is resolved here rather
// than sent down by the control layer (Stratum_设计文档v13.md §7.1).
type ReplicaResolver func(ctx context.Context) ([]string, error)

// IndexStore is the slice of the storage layer's index manager the DataPlane
// uses: search, schedule a build, probe disk, trim by retention. The full
// index.IndexManager satisfies it; keeping it narrow keeps the DataPlane (and
// its tests) independent of the rest of the manager surface.
type IndexStore interface {
	Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error)
	TriggerBuild(ctx context.Context, kbID string, versionID int64) error
	// TriggerBuildBackfill is TriggerBuild at BACKFILL priority: a head start for a
	// version nobody is waiting for. Reconcile uses this one so a head start can
	// never outrank the build a live write is waiting on. It currently schedules
	// only PENDING versions and the active one (see ReconcileIndexes), but the
	// priority is what keeps any future head start from getting in front of live
	// work.
	TriggerBuildBackfill(ctx context.Context, kbID string, versionID int64) error
	IndexExists(ctx context.Context, kbID string, versionID int64) (bool, error)
	EnforceDiskRetention(ctx context.Context, kbID string, protectedIDs []int64) error
}

// LocalDataPlane is the in-process DataPlane (control-data-separation-design.md
// §4.1, stage ① of §7). It owns everything the control layer must not know:
// whether this node needs a version's data at all, which peer to fetch it
// from, how long to retry a pull, how many replicas must acknowledge a write
// before it counts as durable, and how a build gets scheduled.
type LocalDataPlane struct {
	indexMgr        IndexStore
	puller          VersionPuller
	changesFetcher  VersionChangesFetcher
	localVersions   LocalVersionLister
	liveness        VersionLivenessLister
	verify          DataVerifier
	resolve         SourceResolver
	wal             TransactionWAL
	cursorWAL       CursorStore
	executor        VersionWriteExecutor
	control         ControlPlane
	pusher          VersionPusher
	resolveReplicas ReplicaResolver
	// replicaCount is how many replicas should hold a written version, this node
	// included (1 = no replication). fanOut needs it to tell an expected empty
	// target list from a broken one; see the field of the same name on
	// LocalDataPlaneConfig.
	replicaCount  int
	cursorQuerier CursorQuerier
	dropper       VersionDataDropper
	cleaner       VersionDataCleaner
	presence      VersionPresenceQuerier
	digest        VersionDigest
	confirmer     WriteConfirmer
	indexReader   IndexReader
	indexShipper  IndexShipper
	// pullIdleTimeout / pullMaxDuration bound the EnsureIndex pull loop by
	// PROGRESS and by an absolute ceiling respectively (see the loop's comment).
	// Zero means the Default* constants; the config carries them so a test can
	// exercise the loop's bounds without waiting out 30 s.
	pullIdleTimeout time.Duration
	pullMaxDuration time.Duration
	// indexPushSem bounds how many PushIndexToReplicas runs may be in flight at
	// once. One run reads a whole index file into memory and ships it to N
	// replicas, so this is simultaneously the cap on distribution's memory peak
	// and on the builder's outbound bandwidth (v13 §8.4(a)). A nil semaphore
	// means "unconfigured, no limit" — the historical behaviour.
	indexPushSem chan struct{}

	// selfDataSyncAddr is what this node announces as the holder of the
	// versions it writes (§8.5). See LocalDataPlaneConfig.SelfDataSyncAddr.
	selfDataSyncAddr string

	// limiter queues writes per knowledge base so a stalled version cannot let
	// an unbounded backlog form behind it (Stratum_设计文档v13.md §7.7).
	limiter *writeLimiter

	logger *zap.Logger

	// localVersion is this node's data cursor per knowledge base: the highest
	// version it holds contiguously. 0 means "unknown yet" (a node that has
	// not applied anything for that KB). Guarded by versionMu; it only ever
	// moves forward.
	//
	// "Contiguously" is load-bearing, and handledAbove/announcedAbove exist to
	// keep it true. The cursor may only step over a version that either this
	// node holds or that does not exist; a plain maximum would let one failed
	// push followed by a successful later push claim an unbroken history the
	// node does not have (§7.5's invariant, §9.3(2)'s freshness check).
	versionMu    sync.RWMutex
	localVersion map[string]int64

	// deletedReconcileDirty records that what this node HOLDS has changed since the
	// last reverse reconciliation — a version landed, or the cursor moved. The
	// periodic pass skips its work while it is clear, so an IDLE node pays nothing for
	// a scan that could only rediscover what the previous pass already found. A node
	// being written to sets it on every landing, which is the conservative direction
	// on purpose: marking only the landings that can actually leave a leftover (a
	// version arriving below the cursor — §B's late push) would miss the ones left by
	// a reclaim that did not finish, and "did that cleanup land" is a much harder fact
	// to observe here than "something landed". Guarded by versionMu: every change
	// comes through advanceLocalVersion, which already holds it.
	deletedReconcileDirty bool

	// handledAbove records versions ABOVE the cursor whose data this node has
	// accounted for: written here, pulled from a peer, pushed by the
	// coordinator, dropped as deleted, or covered by a full-state transfer.
	//
	// announcedAbove records versions above the cursor that the control layer
	// says exist, announced at apply time. A version in announcedAbove but not
	// in handledAbove is a KNOWN GAP: it exists and this node does not have it,
	// so the cursor must stop below it rather than step over it.
	//
	// With nothing announced the two collapse into "the highest version seen",
	// i.e. the pre-fix behaviour; every storage path announces, so in practice
	// a gap is always known.
	//
	// Both are bounded by the versions of the KB above the cursor: that stays
	// tiny while the chain is healthy (the cursor advances and the entries are
	// dropped) and grows only while a gap is genuinely missing.
	handledAbove   map[string]map[int64]struct{}
	announcedAbove map[string]map[int64]struct{}

	// cursorMu guards cursorPending and cursorFlushing: the queue of cursor
	// values waiting to be persisted, and the flag saying a flusher goroutine
	// is already draining it.
	//
	// The queue exists because versionMu must never be held across IO. Every
	// read path (EnsureIndex, announce, prune, a peer's cursor query) takes that
	// lock, so an fsync inside it would stall all of them behind the disk. The
	// value handed over here is read UNDER versionMu and queued outside it,
	// which is what keeps the lock's critical section free of IO.
	//
	// Only the HIGHEST pending value per knowledge base is kept: the cursor is a
	// scalar, so an intermediate value carries nothing the newer one does not.
	cursorMu       sync.Mutex
	cursorPending  map[string]int64
	cursorFlushing bool

	// takeoverMu guards pendingTakeovers: the §7.3 timers this node started
	// for versions it received via fan-out, keyed like the failure counters.
	takeoverMu       sync.Mutex
	pendingTakeovers map[string]*takeoverWatch

	// terminalReclaim holds the apply-driven local reclaims that did not finish: one
	// task per version, retried on its own cadence (terminalReclaimInterval). See
	// NoteTerminalVersion for why it is not §10.6's broadcast queue — that queue is
	// gone, because every node learns a terminal verdict from its own apply instead.
	terminalReclaimMu sync.Mutex
	terminalReclaim   map[string]*cleanupTask
}

// LocalDataPlaneConfig wires a LocalDataPlane.
type LocalDataPlaneConfig struct {
	// IndexManager schedules and serves version indexes.
	IndexManager IndexStore
	// Puller copies a version's storage-layer data from a peer.
	Puller VersionPuller
	// Verify checks a completed pull against the committed document-set
	// digest.
	Verify DataVerifier
	// Resolve locates the peer to pull from (see SourceResolver).
	Resolve SourceResolver
	// SelfDataSyncAddr is this node's own DataSyncService address. It is what
	// the writer announces with the §7.3 confirmation, so replicas learn where
	// this version's data lives (§8.5). Empty means "do not announce": the
	// confirmation keeps its pre-§8.5 shape and receivers learn nothing.
	SelfDataSyncAddr string
	// PullIdleTimeout bounds how long the pull loop may go WITHOUT progress before
	// it gives up. <= 0 means DefaultPullIdleTimeout. Progress is a completed
	// transfer, not a clock tick — see EnsureIndex's pull loop.
	PullIdleTimeout time.Duration
	// PullMaxDuration is the pull loop's absolute ceiling, so a loop that keeps
	// making a little progress forever still ends. <= 0 means
	// DefaultPullMaxDuration.
	PullMaxDuration time.Duration
	// WAL frames the storage layer's write transaction (WriteVersionData).
	WAL TransactionWAL
	// CursorWAL persists this node's contiguous data cursor, so a restart reads
	// it back instead of inferring it from disk facts (docs/cursor-persistence-plan.md
	// §3). Optional: without it the cursor stays in memory and startup falls back
	// to the inference below, exactly as before.
	CursorWAL CursorStore
	// Executor performs the per-version storage writes inside that
	// transaction.
	Executor VersionWriteExecutor
	// Control receives the reports a completed write produces (the
	// document-set digest). Optional: without it the digest is simply not
	// reported, exactly as a missed propose was before.
	Control ControlPlane
	// Pusher replicates a written version to a peer replica.
	Pusher VersionPusher
	// ChangesFetcher fetches the changes a peer recorded over a version range, so
	// a backfill can replay the delta instead of pulling every version in full
	// (v13 §7.5). Optional: without it backfill always transfers full records.
	ChangesFetcher VersionChangesFetcher
	// LocalVersions enumerates the versions this node holds documents for, and Liveness
	// answers which of them are still alive (plus how far allocation got). Both are what
	// the deleted-version reconciliation needs (§10.6/§B), and the liveness read is also
	// the judgement the backfill asks for above.
	//
	// Liveness is nil only where no state machine is reachable (and no control tier to ask
	// it questions); the backfill and the reconciler then report that they cannot judge,
	// rather than guessing.
	LocalVersions LocalVersionLister
	Liveness      VersionLivenessLister
	// ResolveReplicas lists the *other* replicas that should hold a written
	// version. Nil (or an empty list) means "no replication": the local write
	// is the whole quorum — the single-node and test default. It doubles as
	// the candidate set for backfill sources.
	ResolveReplicas ReplicaResolver
	// ReplicaCount is how many replicas should hold a written version, this node
	// included; 1 (or, for a caller that does not set it, the zero value) means
	// "no replication".
	//
	// fanOut needs it because an empty ResolveReplicas result has two meanings
	// and only one is a fault: on a single-replica deployment the target list is
	// empty by construction — the local write IS the quorum — while on a
	// multi-replica one it means the peer list could not be built and the write
	// will never reach the other replicas. Without the count, fanOut warned on
	// both, and since the first case happens on every write the warning was
	// noise that hid the second.
	ReplicaCount int
	// CursorQuerier asks peers how far their history reaches, so a backfill
	// can pick a source that holds the gap (v13 §7.6). Optional: without it
	// the source stays whatever SourceResolver returned.
	CursorQuerier CursorQuerier

	// DataDropper removes a version's physical data on this node, and
	// CleanupBroadcaster asks the candidate replicas to do the same. Both are
	// used by the §10.6 cleanup after a permanent failure; without them
	// DropVersionData reports that it is not wired.
	DataDropper        VersionDataDropper
	CleanupBroadcaster VersionDataCleaner

	// Presence and Digest are what the §7.3 takeover needs to stand up for a
	// version the coordinator never announced: Presence asks peers whether a
	// quorum holds it, Digest produces the same document-set digest the
	// coordinator would have reported. Without both, a replica can receive
	// data but never vouch for it.
	Presence  VersionPresenceQuerier
	Digest    VersionDigest
	Confirmer WriteConfirmer

	// IndexReader and IndexShipper carry a built index to the other replicas
	// (v13 §8.4). Both are optional: without them a build stays local, and
	// every replica builds for itself exactly as before.
	IndexReader  IndexReader
	IndexShipper IndexShipper

	// MaxConcurrentIndexPush caps concurrent PushIndexToReplicas runs (v13
	// §8.4(a)). Zero means DefaultMaxConcurrentIndexPush. One distribution reads
	// a whole index file into memory and ships it to every replica, so the cap
	// bounds the builder's memory peak and outbound bandwidth at once.
	MaxConcurrentIndexPush int

	// MaxInFlightWrites caps concurrent in-flight writes per knowledge base
	// (§7.7). Zero means DefaultMaxInFlightWrites. Per-KB values arrive later
	// through SetDurabilityPolicy.
	MaxInFlightWrites int

	// Logger receives progress/retry diagnostics. Optional.
	Logger *zap.Logger
}

// NewLocalDataPlane returns a DataPlane over the given storage-layer pieces.
func NewLocalDataPlane(cfg LocalDataPlaneConfig) *LocalDataPlane {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &LocalDataPlane{
		indexMgr:         cfg.IndexManager,
		puller:           cfg.Puller,
		changesFetcher:   cfg.ChangesFetcher,
		localVersions:    cfg.LocalVersions,
		liveness:         cfg.Liveness,
		verify:           cfg.Verify,
		resolve:          cfg.Resolve,
		wal:              cfg.WAL,
		cursorWAL:        cfg.CursorWAL,
		executor:         cfg.Executor,
		control:          cfg.Control,
		pusher:           cfg.Pusher,
		resolveReplicas:  cfg.ResolveReplicas,
		replicaCount:     cfg.ReplicaCount,
		cursorQuerier:    cfg.CursorQuerier,
		dropper:          cfg.DataDropper,
		cleaner:          cfg.CleanupBroadcaster,
		presence:         cfg.Presence,
		digest:           cfg.Digest,
		confirmer:        cfg.Confirmer,
		indexReader:      cfg.IndexReader,
		indexShipper:     cfg.IndexShipper,
		indexPushSem:     newIndexPushSem(cfg.MaxConcurrentIndexPush),
		selfDataSyncAddr: cfg.SelfDataSyncAddr,
		pullIdleTimeout:  cfg.PullIdleTimeout,
		pullMaxDuration:  cfg.PullMaxDuration,
		limiter:          newWriteLimiter(cfg.MaxInFlightWrites),
		logger:           logger,
		localVersion:     make(map[string]int64),
		handledAbove:     make(map[string]map[int64]struct{}),
		announcedAbove:   make(map[string]map[int64]struct{}),
		pendingTakeovers: make(map[string]*takeoverWatch),
		cursorPending:    make(map[string]int64),
	}
}

var _ DataPlane = (*LocalDataPlane)(nil)

// DefaultPullIdleTimeout is how long a pull loop may go without a completed
// transfer before it gives up (LocalDataPlaneConfig.PullIdleTimeout).
//
// PROGRESS is what this bounds, not elapsed time: a transfer that is merely large
// is not a failure, and a 20,000-document version (~56 MB) does not fit in any
// fixed window that a small one also fits in.
const DefaultPullIdleTimeout = 30 * time.Second

// DefaultPullMaxDuration is the pull loop's absolute ceiling, so a loop that makes
// a little progress forever still ends (LocalDataPlaneConfig.PullMaxDuration).
const DefaultPullMaxDuration = 10 * time.Minute

// EnsureIndex brings (kbID, versionID) to a queryable state on this node:
// the version's data is fetched if this node does not hold it yet — the
// "replication lives inside the storage layer" move of
// control-data-separation-design.md §7 — and the index build is then scheduled
// by the puller itself. Idempotent, so re-running it after a crash is safe.
func (d *LocalDataPlane) EnsureIndex(ctx context.Context, kbID string, versionID int64) error {
	if d.puller == nil {
		// Nowhere to fetch from: this node has no storage-layer connection to a peer
		// (a control node, or a plane assembled without one). Failing loudly beats
		// dereferencing a nil puller, and it is also the truth the caller needs — this
		// node cannot bring the version here.
		return fmt.Errorf("plane: EnsureIndex(%s, %d): this node has no puller", kbID, versionID)
	}
	// Holding the version's data already is the authoritative "nothing to
	// fetch" fact, and after §8.5 it is the *only* one: the old code read
	// resolve()'s ok=false as "I am the writer", which held only while the
	// writer was also the leader. The contiguous cursor says it directly — it
	// advances exactly when a version's data has landed (write path, pull, or
	// backfill) and never moves backwards.
	if d.localVersionOf(kbID) >= versionID {
		return nil
	}
	addr, ok, err := d.resolve(ctx, kbID, versionID)
	if err != nil {
		return fmt.Errorf("plane: EnsureIndex(%s, %d): resolve data source: %w", kbID, versionID, err)
	}
	if !ok {
		// No source is known yet: nobody has announced this version, and there is
		// no leader to fall back on. NOTHING WAS DONE here — so this must not read
		// as success.
		//
		// Returning nil is exactly the trap data_source_registry.go:107 already
		// names ("an ok=false here makes EnsureIndex give up silently, which is
		// worse than an address that fails"), and it misleads every caller that
		// acts on the result:
		//   - plane/lag_catchup.go logs "caught up with the chain tail" for a node
		//     that caught up nothing. Measured on the 3+3 cluster: a storage replica
		//     that was away for 55 versions came back with its pre-restart cursor
		//     (365 against a chain tail of 420) and its artifact count unchanged
		//     (2 → 2), while the log claimed a catch-up;
		//   - service/query.go's bounded retry reads nil as "the pull finished" and
		//     stops retrying — though its own comment notes the announcement that
		//     would ask for the pull is best-effort and "if this attempt fails
		//     nothing else will come along";
		//   - cmd/stratum's apply hook logs an error only for a non-nil result, so
		//     this outcome was invisible there too.
		//
		// ErrIndexNotReady is the right sentinel rather than a new one: its wire
		// name is already part of the node-to-node protocol (a mixed-version
		// cluster keeps agreeing on it) and router.retryableReasons already maps it
		// to "another candidate may have it". The message carries the diagnosis,
		// since the sentinel alone cannot say which of the two reasons applies.
		return fmt.Errorf("%w: EnsureIndex(%s, %d): no data source known yet "+
			"(no replica has announced this version, and no leader is a usable data source in this topology)",
			stratumerrors.ErrIndexNotReady, kbID, versionID)
	}

	// A node must never hold version V without holding V-1: fill any known gap
	// before pulling versionID itself (Stratum_设计文档v13.md §7.5). Without
	// this a node that missed a version would apply the next one onto an
	// incomplete document set, and "version number comparison implies
	// completeness" would stop holding.
	if err := d.backfillTo(ctx, addr, kbID, versionID); err != nil {
		return err
	}

	// Pull with digest verification, retrying until the data is complete. The
	// writer commits the version's document-set digest only after its storage
	// writes finish, so this node recomputes the digest from its local store
	// after each pull and retries until it matches — closing the race where a
	// pull arrives before the writer's writes land. If the digest never
	// arrives (a missed propose on the writer), a pull that produced data is
	// accepted (the verifier's fallback).
	// The loop's bound is about PROGRESS, not about wall clock.
	//
	// It used to be a flat 30 s over the whole loop. That works for a small version
	// and is impossible for a large one: a 20,000-document version is ~56 MB, which
	// does not cross the wire, land, and get applied inside 30 s — so every attempt
	// timed out, and the retry used the same 30 s, which means the loop could not
	// converge at any number of attempts. Measured on the 3+3 cluster: 64 rounds of
	// `sync: recv SyncEntry: ... DeadlineExceeded` followed by "version data did not
	// converge within 30s", while the same transfer completed perfectly well once
	// given room.
	//
	// What failure actually looks like here — an unreachable peer, a source that no
	// longer holds the version, a stream that keeps being refused — is attempts that
	// make NO progress. So idle time is what gets bounded, with an absolute ceiling
	// so that a pathological loop still ends.
	pullIdleTimeout := d.pullIdleTimeout
	if pullIdleTimeout <= 0 {
		pullIdleTimeout = DefaultPullIdleTimeout
	}
	pullMaxDuration := d.pullMaxDuration
	if pullMaxDuration <= 0 {
		pullMaxDuration = DefaultPullMaxDuration
	}
	start := time.Now()
	lastProgress := start
	attempts := 0
	var lastErr error
	backoff := 200 * time.Millisecond
	for {
		// Re-checked every attempt: when this node is the coordinator, its own
		// apply can run ahead of its storage writes, so the callback reaches
		// here before the data lands locally (§8.5). The write path advances
		// the cursor as it finishes, and this sees it — instead of pulling its
		// own data back from a peer until the deadline expires.
		if d.localVersionOf(kbID) >= versionID {
			return nil
		}
		// Re-resolved every attempt as well: a coordinator that is not the
		// leader announces itself only once its own writes finish, so the first
		// resolution legitimately answers "the leader", which holds no such
		// data. Holding on to that answer would mean pulling from a pointless
		// source for the whole timeout while the announcement sits unread.
		if fresh, freshOK, freshErr := d.resolve(ctx, kbID, versionID); freshErr != nil {
			d.logger.Warn("plane: resolve data source failed, will retry",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(freshErr))
		} else if freshOK && fresh != addr {
			addr = fresh
		}
		attempts++
		err := d.puller.PullVersion(ctx, addr, kbID, versionID)
		if err != nil {
			lastErr = err
			d.logger.Warn("plane: data pull failed, will retry",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Int("attempt", attempts),
				zap.Duration("no_progress_for", time.Since(lastProgress)),
				zap.Error(err))
		} else {
			// A COMPLETED transfer is progress, and it is recorded before the
			// verification: the writer commits the version's digest only after its own
			// storage writes finish, so "the data is here and the digest is not yet" is
			// a reason to keep going, not to give up. Counting it as progress is what
			// lets a large version finish across several attempts instead of timing out
			// on a clock that was never about the data.
			lastProgress = time.Now()
			lastErr = nil
			if d.verify(ctx, kbID, versionID) {
				d.advanceLocalVersion(kbID, versionID)
				return nil
			}
			if empty, emptyErr := d.versionHasNoDocuments(ctx, kbID, versionID); emptyErr == nil && empty {
				// The pull succeeded and the version holds no documents — so the
				// empty set IS its content. Waiting for a digest here means waiting
				// forever: a writer never commits one for a version with no document
				// set. The cursor would stay below a version this node in fact
				// holds, which is what made a freshly created knowledge base answer
				// "local history reaches version 0" to the station's freshness
				// check (§9.3(2)) and refuse the query.
				//
				// Gated on the pull having SUCCEEDED just above: a pull that failed
				// never reaches here, so a broken transfer can never be mistaken for
				// an empty version.
				d.advanceLocalVersion(kbID, versionID)
				return nil
			}
		}
		// Both ceilings carry the diagnosis the old message could not: how far this
		// node got, how many attempts were spent, and what the last attempt said. A
		// bare "did not converge within 30s" left an operator to guess whether the
		// peer was gone, the transfer too big, or the digest missing.
		if idle := time.Since(lastProgress); idle > pullIdleTimeout {
			return fmt.Errorf(
				"plane: EnsureIndex(%s, %d): version data did not converge — %s without progress "+
					"(attempts=%d, local cursor=%d, target=%d, elapsed=%s, last error=%v)",
				kbID, versionID, idle.Round(time.Second), attempts,
				d.localVersionOf(kbID), versionID, time.Since(start).Round(time.Second), lastErr)
		}
		if elapsed := time.Since(start); elapsed > pullMaxDuration {
			return fmt.Errorf(
				"plane: EnsureIndex(%s, %d): version data did not converge within %s "+
					"(attempts=%d, local cursor=%d, target=%d, last error=%v)",
				kbID, versionID, pullMaxDuration, attempts,
				d.localVersionOf(kbID), versionID, lastErr)
		}
		time.Sleep(backoff)
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// versionHasNoDocuments reports whether this node holds no documents for the
// version — so the empty set IS the version's content.
//
// Why it needs saying out loud: §7.5's pull loop waits for the writer's
// document-set digest to match, and a version created with no changes never gets
// one (there is no document set to hash). Left alone, the loop spins out its
// whole timeout while the node's contiguous cursor stays below a version it in
// fact holds. A freshly created knowledge base is exactly that state, and the
// station's freshness check (§9.3(2)) reads the cursor — so its first query was
// refused with "local history reaches version 0" depending on whether the route
// table had refreshed yet.
//
// Conservative by construction: with no digest source it answers "no", so a gap
// here degrades to the previous behaviour (wait, then fail) instead of advancing
// the cursor on evidence it does not have. Callers must have a successful pull
// behind them before trusting the answer.
func (d *LocalDataPlane) versionHasNoDocuments(ctx context.Context, kbID string, versionID int64) (bool, error) {
	if d.digest == nil {
		return false, nil
	}
	got, err := d.digest.DigestOf(ctx, kbID, versionID)
	if err != nil {
		return false, err
	}
	return got == stratinternalsync.ComputeDocIDSetHash(nil), nil
}

// FetchVersionData brings the version's data here without building its index.
//
// It is what a non-active version needs (§8.6b): the data must be present for
// the version to count as durable, but building an index nobody may ever query
// is wasted work. A later query builds it lazily.
func (d *LocalDataPlane) FetchVersionData(ctx context.Context, kbID string, versionID int64) error {
	// Same fact as in EnsureIndex: this node holding the version already means
	// there is nothing to fetch — which after §8.5 can no longer be inferred
	// from "I am the leader/writer".
	if d.localVersionOf(kbID) >= versionID {
		return nil
	}
	addr, ok, err := d.resolve(ctx, kbID, versionID)
	if err != nil {
		return fmt.Errorf("plane: FetchVersionData(%s, %d): resolve data source: %w", kbID, versionID, err)
	}
	if !ok {
		// No source is known yet: nothing to fetch. A later apply or reconcile
		// pass retries.
		return nil
	}
	if err := d.backfillTo(ctx, addr, kbID, versionID); err != nil {
		return err
	}
	if err := d.puller.PullVersionData(ctx, addr, kbID, versionID); err != nil {
		return err
	}
	d.advanceLocalVersion(kbID, versionID)
	return nil
}

// advanceLocalVersion records that this node has accounted for versionID: it
// holds the version's data (written here, pulled, pushed, or covered by a
// full-state transfer) or the version no longer exists (dropped as deleted).
//
// The cursor advances only over an UNBROKEN run of accounted versions. A version
// known to exist but not yet accounted for stops the cursor below itself, which
// is what keeps "the cursor is where my history is unbroken" true when a push to
// this node is lost and a later version's push still lands (§7.5, §9.3(2)).
//
// The cursor never moves back: re-applying an older version (idempotent retries,
// backfill replay) must not move it down, and a version announced only after the
// cursor has already passed it does not pull the cursor back either. The fix is
// for the future, not a retroactive correction.
func (d *LocalDataPlane) advanceLocalVersion(kbID string, versionID int64) {
	d.versionMu.Lock()
	// Whatever brought us here changed what this node holds — including the early
	// return right below, which is exactly the late-push case (a version landing under
	// a cursor that has already moved past it, §B's no-failure-needed leftover). The
	// reverse reconciliation only has to run again after such a change, so mark it
	// before deciding anything.
	d.deletedReconcileDirty = true
	if versionID <= d.localVersion[kbID] {
		d.versionMu.Unlock()
		return
	}
	handled := d.handledAbove[kbID]
	if handled == nil {
		handled = make(map[int64]struct{})
		d.handledAbove[kbID] = handled
	}
	handled[versionID] = struct{}{}
	d.advanceCursorLocked(kbID)
	cursor := d.localVersion[kbID]
	d.versionMu.Unlock()

	// Data first, cursor second — the invariant the whole record depends on. It
	// is enforced HERE, on the single path every advance goes through, rather
	// than at the call sites: there are a dozen of them (write, pull, push,
	// backfill, drop, full-state transfer), and one forgetting the order would
	// let a restarted node claim history it does not hold.
	//
	// The value queued is the cursor AFTER the step, not versionID: the cursor
	// only moves over a contiguous run, so after an advance it may have stepped
	// over several versions at once — and that boundary is the thing worth
	// recording.
	d.persistCursor(kbID, cursor)
}

// AnnounceVersion records that the control layer says versionID exists. It is the
// "known gap" half of the §7.5 invariant: a version this node has been told about
// but does not hold must block the cursor from stepping over it.
//
// Announcing is idempotent and cheap, and it deliberately does NOT move the
// cursor: existence is not possession.
func (d *LocalDataPlane) AnnounceVersion(kbID string, versionID int64) {
	d.versionMu.Lock()
	defer d.versionMu.Unlock()
	if versionID <= d.localVersion[kbID] {
		return
	}
	announced := d.announcedAbove[kbID]
	if announced == nil {
		announced = make(map[int64]struct{})
		d.announcedAbove[kbID] = announced
	}
	announced[versionID] = struct{}{}
}

// advanceCursorLocked walks the cursor up over every accounted-for version,
// stopping at the first KNOWN version that is not accounted for (or when nothing
// above the cursor is known).
//
// The work is proportional to the number of versions above the cursor still being
// tracked, which is one in the healthy case — an advance immediately consumes its
// own entry — and grows only while a gap is genuinely missing.
func (d *LocalDataPlane) advanceCursorLocked(kbID string) {
	handled := d.handledAbove[kbID]
	announced := d.announcedAbove[kbID]
	for {
		next, ok := d.lowestAboveLocked(kbID)
		if !ok {
			break
		}
		if _, done := handled[next]; !done {
			// A version the control layer says exists and this node does not
			// hold: the cursor stops below it rather than claiming a history
			// with a hole in it.
			break
		}
		delete(handled, next)
		delete(announced, next)
		d.localVersion[kbID] = next
	}
	if len(handled) == 0 {
		delete(d.handledAbove, kbID)
	}
	if len(announced) == 0 {
		delete(d.announcedAbove, kbID)
	}
}

// lowestAboveLocked returns the smallest version above the cursor that this node
// knows of — accounted for or merely announced. The second return value is false
// when nothing above the cursor is known, i.e. there is nothing left to step over.
func (d *LocalDataPlane) lowestAboveLocked(kbID string) (int64, bool) {
	cursor := d.localVersion[kbID]
	found := false
	var lowest int64
	for _, set := range []map[int64]struct{}{d.handledAbove[kbID], d.announcedAbove[kbID]} {
		for v := range set {
			if v <= cursor {
				continue
			}
			if !found || v < lowest {
				lowest, found = v, true
			}
		}
	}
	return lowest, found
}

// markVersionsHandled accounts for every version in [from, to] in one step. It is
// for the paths that obtain a whole VERSION'S state at once — §6.4's full-state
// transfer is the only one — where advancing version by version would re-scan the
// pending sets once per version.
func (d *LocalDataPlane) markVersionsHandled(kbID string, from, to int64) {
	d.versionMu.Lock()
	handled := d.handledAbove[kbID]
	if handled == nil {
		handled = make(map[int64]struct{})
		d.handledAbove[kbID] = handled
	}
	for v := from; v <= to; v++ {
		if v > d.localVersion[kbID] {
			handled[v] = struct{}{}
		}
	}
	d.advanceCursorLocked(kbID)
	cursor := d.localVersion[kbID]
	d.versionMu.Unlock()
	d.persistCursor(kbID, cursor)
}

// MarkVersionContiguous implements sync.LocalVersionAdvancer: a version whose
// records this node has received — pushed by the coordinator or pulled from a
// peer — is one it holds, and the cursor is how it says so.
//
// The two halves are wired separately on purpose: this plane owns the cursor,
// while the sync handler is what knows the records landed. Without the wiring a
// replica with every record on disk still answered "version 0" to the station's
// freshness check (§9.3(2)) and was refused as stale.
func (d *LocalDataPlane) MarkVersionContiguous(kbID string, versionID int64) {
	d.advanceLocalVersion(kbID, versionID)
}

// LocalVersionOf reports the highest version this node holds contiguously for
// kbID (0 = nothing known yet). It is the storage layer's answer to peers
// looking for a backfill source (Stratum_设计文档v13.md §7.6).
func (d *LocalDataPlane) LocalVersionOf(kbID string) int64 {
	return d.localVersionOf(kbID)
}

// localVersionOf reports the highest version this node holds contiguously for
// kbID (0 = nothing known yet).
func (d *LocalDataPlane) localVersionOf(kbID string) int64 {
	d.versionMu.RLock()
	defer d.versionMu.RUnlock()
	return d.localVersion[kbID]
}

// cursorWriteTimeout bounds ONE cursor record's write. The write is a local
// append plus fsync, so this is generous; it exists so a stuck disk can never
// pin the flusher goroutine forever — the value stays queued either way.
const cursorWriteTimeout = 5 * time.Second

// cursorRetryDelay is how long flushCursors waits before retrying a batch it
// could not write. It only ever applies to a FAILED batch — the happy path never
// reaches it — and it exists for the same reason the failed value is re-queued at
// all: without a delay, re-queueing spins the flusher at disk-error speed, one
// repeated warning per fsync attempt, for as long as the disk misbehaves.
const cursorRetryDelay = time.Second

// persistCursor queues a cursor value for persistence. It never blocks and never
// touches the disk itself: the callers are the write path (§7.7) and the Raft
// apply path (via onVersionCreated), and IO there would stall everything behind
// it.
//
// Merging is per knowledge base and keeps only the highest pending value,
// because the cursor is a scalar: an intermediate value says nothing the newer
// one does not, so a burst of advances costs ONE record.
func (d *LocalDataPlane) persistCursor(kbID string, versionID int64) {
	if d.cursorWAL == nil || versionID <= 0 {
		return
	}
	d.cursorMu.Lock()
	if versionID > d.cursorPending[kbID] {
		d.cursorPending[kbID] = versionID
	}
	if d.cursorFlushing {
		d.cursorMu.Unlock()
		return
	}
	d.cursorFlushing = true
	d.cursorMu.Unlock()
	go d.flushCursors()
}

// flushCursors drains the queue, one knowledge base at a time, on its own
// goroutine. Exactly one flusher runs at a time (cursorFlushing), so the records
// for one knowledge base are written in the order they were queued.
//
// A failed write is logged and its value goes back in the queue, AFTER a delay
// (cursorRetryDelay). Both halves matter, and they pull in opposite directions:
// dropping the value would leave the cursor unpersisted until some later advance
// happened to come along, while re-queueing it with no delay turns a disk outage
// into a hot loop — the same warning at fsync speed for as long as the outage
// lasts.
//
// The in-memory cursor is deliberately NOT rolled back. Rolling it back would
// make this node re-fetch data it has already served — and the lagging record is
// safe by construction: it can only understate the cursor, so a restart backfills
// a little more than it strictly must. Only the CRASHED state is low, never the
// running one.
func (d *LocalDataPlane) flushCursors() {
	for {
		d.cursorMu.Lock()
		pending := d.cursorPending
		d.cursorPending = make(map[string]int64)
		if len(pending) == 0 {
			d.cursorFlushing = false
			d.cursorMu.Unlock()
			return
		}
		d.cursorMu.Unlock()

		var failed bool
		for kbID, versionID := range pending {
			ctx, cancel := context.WithTimeout(context.Background(), cursorWriteTimeout)
			err := d.cursorWAL.WriteCursor(ctx, kbID, versionID)
			cancel()
			if err == nil {
				continue
			}
			failed = true
			d.logger.Warn("plane: persisting the data cursor failed; it will be retried, and the record lags the data meanwhile",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			d.cursorMu.Lock()
			if versionID > d.cursorPending[kbID] {
				// Nothing newer superseded this value while the write was in
				// flight, so it goes back in the queue — it must not be dropped,
				// because another advance is not guaranteed to come and the
				// record is the only thing a restart reads.
				d.cursorPending[kbID] = versionID
			}
			d.cursorMu.Unlock()
		}
		if failed {
			// Back off rather than spin, once per batch rather than per record.
			// An advance arriving during the sleep is not lost: it lands in
			// cursorPending (cursorFlushing still reports a flusher running) and
			// the next pass of this loop picks it up.
			time.Sleep(cursorRetryDelay)
		}
	}
}

// persistedCursors reads back the cursors this node recorded. A plane assembled
// without a cursor store answers "nothing persisted", which is exactly what the
// inference fallback needs to see.
func (d *LocalDataPlane) persistedCursors(ctx context.Context) (map[string]int64, error) {
	if d.cursorWAL == nil {
		return nil, nil
	}
	return d.cursorWAL.RecoverCursors(ctx)
}

// installPersistedCursor installs a cursor read back from the WAL.
//
// It deliberately does NOT go through advanceLocalVersion: that path exists to
// PERSIST the value, and this value came from the log — writing it again would
// be a redundant fsync on every startup. Monotone for the same reason the cursor
// is: memory may already have moved past the record (a write that landed before
// this ran), and moving back would forget versions this node holds.
func (d *LocalDataPlane) installPersistedCursor(kbID string, versionID int64) {
	d.versionMu.Lock()
	defer d.versionMu.Unlock()
	if versionID > d.localVersion[kbID] {
		d.localVersion[kbID] = versionID
	}
}

// DataVersionsSnapshot returns a copy of every knowledge base's contiguous
// cursor — the whole view a node reports to the control leader (§7.13.4).
//
// A copy, not the live map: the caller may encode it, retry it, or hold it across
// a tick, and a concurrent write must not mutate what was already reported.
// Knowledge bases with no local data are absent rather than zero, so the receiver
// can tell "reported 0" from "never reported" — a distinction §7.13.4's aggregate
// depends on.
func (d *LocalDataPlane) DataVersionsSnapshot() map[string]int64 {
	d.versionMu.RLock()
	defer d.versionMu.RUnlock()
	snapshot := make(map[string]int64, len(d.localVersion))
	for kbID, versionID := range d.localVersion {
		snapshot[kbID] = versionID
	}
	return snapshot
}

// backfillTo closes a known gap before versionID is applied, by pulling
// (local cursor, versionID) from sourceAddr in order. It is deliberately
// conservative: with no cursor yet (a node that has applied nothing for this
// KB) there is no *known* gap to close, and the versions it learns about are
// pulled whole. The versions it fills are then part of the node's local
// history, so the cursor advances as each one lands.
func (d *LocalDataPlane) backfillTo(ctx context.Context, sourceAddr, kbID string, versionID int64) error {
	local := d.localVersionOf(kbID)
	if local == 0 || local >= versionID-1 {
		return nil
	}
	// Pick a peer that actually holds the gap: the default source (the leader)
	// is precisely what a lagging follower cannot rely on
	// (Stratum_设计文档v13.md §7.6).
	source := d.pickBackfillSource(ctx, kbID, versionID-1, sourceAddr)

	// The replicated metadata is read ONCE here and BOTH paths share it: the delta
	// path needs it to know whether the gap holds a version that no longer exists, and
	// the full-record path needs it to know whether the gap holds a version CONFIRMED
	// deleted — which is the case transferFullState exists for. One read rather than
	// one per path, and not one per version either: the gap may be long, and asking per
	// version would re-read the whole version list at every step.
	//
	// A failed read is carried rather than resolved here, because the two paths answer
	// it differently: the delta path turns its check OFF and replays as it did before
	// the check existed, while the full-record path STOPS (it cannot decide what to
	// pull without knowing what exists). The warning is emitted here so that a failure
	// is logged once instead of once per path.
	var deleted map[int64]bool
	var deletedErr error
	if d.liveness != nil {
		// The judgement is "which of these versions is GONE" — asked of the CURRENT state:
		// alive-in-range plus how far allocation got. An id at or below lastAllocated that
		// is not alive was handed out and is gone, and THAT answer cannot be pruned away
		// (docs/known-gaps.md §B). The older shape asked for a removal record instead, and
		// a removal record has a lifetime: once pruning drops it, the gap reads as "nothing
		// was removed" and the deleted version is replayed.
		//
		// The gap is (local, versionID), and that is exactly what is asked for: the bounds
		// travel to the control layer and narrow both the answer and the cost. A failed read
		// is carried rather than resolved here, because the two paths answer it differently
		// (see below).
		from, to := local, versionID-1
		var alive []int64
		var lastAllocated int64
		alive, lastAllocated, deletedErr = d.liveness.VersionLiveness(ctx, kbID, &from, &to)
		if deletedErr == nil {
			aliveSet := make(map[int64]bool, len(alive))
			for _, id := range alive {
				aliveSet[id] = true
			}
			deleted = make(map[int64]bool)
			for v := local + 1; v < versionID; v++ {
				if v <= lastAllocated && !aliveSet[v] {
					deleted[v] = true
				}
			}
		} else {
			d.logger.Warn("plane: backfill cannot read the liveness of the versions in the gap",
				zap.String("kb_id", kbID), zap.Int64("from_version", local),
				zap.Int64("to_version", versionID-1), zap.Error(deletedErr))
		}
	}

	// §7.5: try the delta path first — it transfers what changed instead of every
	// version's full record set. It only applies when the source has a record for
	// EVERY version in the gap: a missing delta cannot be told apart from a version
	// that changed nothing, and replaying the rest would leave a hole behind the
	// cursor.
	if d.changesFetcher != nil {
		err := d.backfillByChanges(ctx, source, kbID, local, versionID, deleted)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errBackfillGapIncomplete) {
			// A transport failure says nothing about the data: fall through to full
			// records, which is the path that always works.
			d.logger.Warn("plane: delta backfill failed; using full records",
				zap.String("kb_id", kbID), zap.Int64("from_version", local),
				zap.Int64("to_version", versionID), zap.Error(err))
		}
	}

	if deletedErr != nil {
		// "I could not find out" is not "it is gone" — but unlike the delta path, this
		// path has no conservative default to fall back on: it has to know which
		// versions were removed before it decides what to pull, so an unreadable answer
		// stops the backfill here.
		return fmt.Errorf("plane: backfill %s: read deleted versions: %w", kbID, deletedErr)
	}

	for v := local + 1; v < versionID; v++ {
		// Check BEFORE pulling. A version can be genuinely EMPTY (nothing to send)
		// or GONE (deleted, its records no longer on any node), and the storage layer
		// cannot tell them apart — absent rows mean both. The replicated metadata can,
		// so it is asked. Two things follow from asking first: a confirmed-deleted
		// version is never fetched at all (no pointless transfer), and "the gap was
		// handed to the full-state path without pulling the deleted version" becomes
		// something a caller can observe — which is how the end-to-end case proves the
		// fallback actually ran. Advancing the cursor over a vanished version would
		// claim history this node never received, and the cursor's whole meaning is
		// "contiguous up to here" (§7.5).
		if deleted[v] {
			// §6.4: the version is confirmed gone (a middle version may be
			// deleted), so its records exist nowhere and the gap cannot be filled
			// version by version. Fall back to a full-state transfer.
			//
			// Its source is chosen for versionID and NOT for the gap. The transfer
			// hands over the version being APPLIED, and a replica whose cursor stops
			// at versionID-1 — exactly what the gap's own need admits — holds no data
			// for it. Such a replica answers with an empty stream, which the sync
			// layer reports as success and cannot tell apart from a version that is
			// genuinely empty (Follower.PullVersionWith advances the cursor on any
			// completed receive, including one that carried nothing), so transferring
			// from it would move this node's cursor onto a version it does not hold —
			// and "the cursor says the data landed" is what every later fetch is
			// short-circuited by. Failing here instead keeps the retry available.
			holder, ok := d.pickTransferSource(ctx, kbID, versionID, source)
			if !ok {
				return fmt.Errorf("plane: backfill %s: v%d was deleted and no replica holds v%d to transfer its state from: %w",
					kbID, v, versionID, stratumerrors.ErrIndexNotReady)
			}
			return d.transferFullState(ctx, holder, kbID, local, versionID, v)
		}
		if err := d.puller.PullVersion(ctx, source, kbID, v); err != nil {
			return fmt.Errorf("plane: backfill %s v%d from %s (local cursor %d): %w", kbID, v, source, local, err)
		}
		d.advanceLocalVersion(kbID, v)
	}
	return nil
}

// transferFullState is §6.4's last resort — the analogue of Raft's InstallSnapshot.
// It runs only when a version in the gap is CONFIRMED deleted by the metadata, never
// on a transport failure or an unreadable topology: those are "I could not find
// out", and the answer to them stays "abort the apply" rather than "silently skip".
// That distinction is the whole reason the gap's versions are judged from the
// replicated state instead of from the storage layer's absent rows.
//
// Its source must HOLD the snapshot version — the caller picks it with pickTransferSource
// for exactly that reason. An empty answer is reported as a successful transfer and looks
// identical to a version that is genuinely empty, so a source that merely reaches the gap
// would move this node's cursor onto a version it does not have.
//
// The node transfers ONE whole version's state and moves its cursor straight to it.
// The version it transfers is versionID — the version being applied — and NOT
// versionID-1, which is what "the newest one the gap needs" would suggest. The
// reason is the whole point of this path: the gap is unfillable BECAUSE one of its
// versions was deleted, and versionID-1 may well be that version. Naming a version
// we know is gone as the snapshot would leave this path failing exactly when it is
// needed. versionID, by contrast, is committed and therefore present everywhere the
// apply reached. It can be transferred without dragging the chain behind it because
// a version's docID list is stored whole, not as a delta from its parent.
//
// What this costs, stated plainly: the versions between the old cursor and the
// snapshot are NOT held afterwards, so the cursor now means "contiguous up to here,
// with everything below this point replaced by the snapshot". Reads of one of those
// versions fail — which is where they already stood, since a deleted middle version
// is unreadable to every node.
func (d *LocalDataPlane) transferFullState(ctx context.Context, sourceAddr, kbID string, local, versionID, deletedVersion int64) error {
	snapshot := versionID
	if err := d.puller.PullVersionData(ctx, sourceAddr, kbID, snapshot); err != nil {
		return fmt.Errorf("plane: backfill %s from %s: v%d was deleted (local cursor %d) and the full-state transfer of v%d failed: %w",
			kbID, sourceAddr, deletedVersion, local, snapshot, err)
	}
	// §7.5: the snapshot IS this version's whole state, so the cursor may move up
	// to it — but it has to move as a run of ACCOUNTED versions, not as a bare
	// jump (which the cursor no longer performs for anyone). Every version the
	// transfer skipped is accounted for by it: they are not individually held (the
	// log line below says exactly which), but the gap they belonged to is
	// precisely what the snapshot replaced. Registering them in ONE step keeps
	// this O(versions skipped) instead of one cursor scan per version.
	d.markVersionsHandled(kbID, local+1, snapshot)
	d.logger.Warn("plane: backfilled via full-state transfer after a deleted version; the skipped versions are not readable locally",
		zap.String("kb_id", kbID), zap.Int64("from_version", local),
		zap.Int64("deleted_version", deletedVersion), zap.Int64("snapshot_version", snapshot))
	return nil
}

// errBackfillGapIncomplete reports that the delta path cannot serve this gap at
// all. It has two causes, and the caller deliberately does not have to tell them
// apart: the source has no recorded changes for some version in the gap, or the
// replicated metadata no longer has that version. Both mean the same thing —
// "replay the source's deltas" is not an option for this range — and both have the
// same remedy, which already exists: the full-state transfer, the path written
// precisely for a gap that holds a deleted version (§6.4).
var errBackfillGapIncomplete = errors.New("backfill: the gap cannot be replayed from the source's recorded changes")

// backfillByChanges replays the source's recorded deltas for (local, versionID-1],
// in ascending order. It refuses the WHOLE range when any version cannot be
// replayed: a partial replay would leave a hole in this node's history while
// advancing its cursor over it, and the cursor is what makes "version number
// comparison implies completeness" true (§7.5).
//
// A version cannot be replayed for either of two reasons, and BOTH are checked
// here rather than only the first:
//
//   - the source has no delta for it. The source is honest about this by
//     construction: ChangesInRange reports a version it holds no BEGIN record for
//     as ABSENT rather than as "nothing changed" (internal/wal/file.go), so the gap
//     is visible instead of silent.
//
//   - the metadata records the version as REMOVED. This is the case the delta
//     path alone cannot see, and it is not hypothetical: a version's BEGIN record
//     leaves the source's WAL only once every replica that should hold it has
//     reported a cursor past it (ReclaimableChangesThrough), so for as long as some
//     replica is behind — the very situation a backfill runs in — a DELETED
//     version's delta is still served. Replaying it would write that version's data
//     on a node whose metadata says the version is gone, and nothing would later
//     detect it: the judgement that names such data is exactly the metadata row
//     that is missing. transferFullState exists for a gap holding a deleted
//     version; the delta path has to hand the gap over instead of reconstructing it.
//
// "I could not read the metadata" is NOT "the version is gone", so a nil set — which
// is what the caller passes when the read failed, or when no checker is wired at all
// — leaves this check OFF and the gap is replayed exactly as it was before the check
// existed. That is the conservative direction available here: treating an unreadable
// answer as a verdict would turn a transient metadata read failure on this node into
// a full-state transfer, a transfer decision coupled to a local read it does not
// belong to.
//
// deleted is the CALLER's read (see backfillTo), not one of this function's own: the
// full-record path needs the same answer, and reading it per path paid for it twice on
// every gap that fell back.
func (d *LocalDataPlane) backfillByChanges(ctx context.Context, sourceAddr, kbID string, local, versionID int64, deleted map[int64]bool) error {
	deltas, err := d.changesFetcher.ChangesInRange(ctx, sourceAddr, kbID, local, versionID-1)
	if err != nil {
		return fmt.Errorf("plane: delta backfill %s (%d,%d] from %s: %w", kbID, local, versionID-1, sourceAddr, err)
	}

	for v := local + 1; v < versionID; v++ {
		if _, ok := deltas[v]; !ok {
			return fmt.Errorf("plane: delta backfill %s (%d,%d] from %s: no record for v%d: %w",
				kbID, local, versionID-1, sourceAddr, v, errBackfillGapIncomplete)
		}
		if deleted[v] {
			return fmt.Errorf("plane: delta backfill %s (%d,%d] from %s: v%d no longer exists: %w",
				kbID, local, versionID-1, sourceAddr, v, errBackfillGapIncomplete)
		}
	}

	for v := local + 1; v < versionID; v++ {
		delta := deltas[v]
		if err := d.ApplyBackfillChanges(ctx, kbID, v, delta.ParentVersionID, delta.Changes); err != nil {
			return err
		}
	}
	return nil
}

// pickBackfillSource returns the first candidate whose contiguous history
// already reaches need, falling back to fallback when none does — or when no
// cursor querier or candidate list is configured. A peer that cannot be
// reached is skipped: it cannot serve as a source, but its silence says
// nothing about the others.
func (d *LocalDataPlane) pickBackfillSource(ctx context.Context, kbID string, need int64, fallback string) string {
	if d.cursorQuerier == nil || d.resolveReplicas == nil {
		return fallback
	}
	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		return fallback
	}
	for _, peer := range peers {
		if peer == fallback {
			continue // already the default source
		}
		// Bounded for the same reason as SafeDurableVersion's peer queries: this can
		// run while a peer is not serving yet, and an unbounded wait would turn a
		// backfill choice into a hang.
		peerCtx, cancel := context.WithTimeout(ctx, peerCursorTimeout)
		cursor, err := d.cursorQuerier.LocalVersionOf(peerCtx, peer, kbID)
		cancel()
		if err != nil {
			continue
		}
		if cursor >= need {
			d.logger.Info("plane: backfill source selected",
				zap.String("peer", peer), zap.String("kb_id", kbID), zap.Int64("peer_cursor", cursor))
			return peer
		}
	}
	return fallback
}

// pickTransferSource returns a replica whose contiguous cursor already reaches need, and
// whether one exists. Candidates are the resolved replicas plus the caller's own default
// source, which may be a replica resolveReplicas does not list.
//
// It deliberately has NO fallback, unlike pickBackfillSource: a full-state transfer asks
// for the version being applied, and "the default source" is not evidence that anyone
// holds it. When nothing does, the caller must fail rather than transfer from a replica
// that will answer with an empty stream — on the wire, "this source has no data for that
// version" and "that version has no data" are the same thing.
//
// An unwired cursorQuerier leaves the caller's source as the only one there is. That is
// the pre-existing behaviour for a plane assembled without one, and it keeps those
// deployments and tests working unchanged.
func (d *LocalDataPlane) pickTransferSource(ctx context.Context, kbID string, need int64, also string) (string, bool) {
	if d.cursorQuerier == nil {
		if also == "" {
			return "", false
		}
		return also, true
	}
	candidates := make([]string, 0, 4)
	if d.resolveReplicas != nil {
		if peers, err := d.resolveReplicas(ctx); err == nil {
			candidates = append(candidates, peers...)
		}
	}
	if also != "" {
		candidates = append(candidates, also)
	}
	seen := make(map[string]bool, len(candidates))
	for _, peer := range candidates {
		if peer == "" || seen[peer] {
			continue
		}
		seen[peer] = true
		// Bounded for the same reason as pickBackfillSource's query: this can run
		// while a peer is not serving yet, and an unbounded wait would turn a source
		// choice into a hang.
		peerCtx, cancel := context.WithTimeout(ctx, peerCursorTimeout)
		cursor, err := d.cursorQuerier.LocalVersionOf(peerCtx, peer, kbID)
		cancel()
		if err != nil {
			continue
		}
		if cursor >= need {
			d.logger.Info("plane: full-state transfer source selected",
				zap.String("peer", peer), zap.String("kb_id", kbID),
				zap.Int64("need_version", need), zap.Int64("peer_cursor", cursor))
			return peer, true
		}
	}
	return "", false
}

// Search runs a vector query against versionID's index.
func (d *LocalDataPlane) Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) {
	return d.indexMgr.Search(ctx, kbID, versionID, vector, topK)
}

// WriteVersionData runs the storage layer's write transaction for an
// already-allocated version: BEGIN (persisting the replay input) → the
// per-change storage writes → COMMIT. The control layer only allocated the
// version ID; the transaction that makes the data durable is entirely the
// storage layer's (Stratum_设计文档v13.md §7.12).
func (d *LocalDataPlane) WriteVersionData(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
	// Per-stage timings of one write transaction, at debug level.
	//
	// Why they are here: a dispatched write is bounded by the candidate budget
	// (plane.CoordinatorDispatcher.candidateBudget, floor 15 s) and, before this,
	// had no way to say where that budget went. The first measurement paid for
	// itself — fan-out spent 15.0 s here, exactly the budget, because ONE replica
	// was unreachable and every push inherited the whole budget (see
	// replicaPushTimeout).
	writeStart := time.Now()
	var tAcquire, tLocal time.Duration
	// The inside of local_us and fanout_us, reported alongside them. Both are
	// large enough to dominate a batch write's wall clock — together they were
	// 98% of a 2,000-document transaction (local p50 1.34 s + fanout p50 1.75 s of
	// a 3.15 s total) — and both were single opaque numbers. See localStages and
	// fanoutStages for what each sub-stage is.
	var local localStages
	var fin finishStages
	defer func() {
		d.logger.Debug("write: stage timings",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Int("changes", len(changes)),
			zap.Int64("acquire_us", tAcquire.Microseconds()),
			zap.Int64("local_us", tLocal.Microseconds()),
			zap.Int64("wal_begin_us", local.walBegin.Microseconds()),
			zap.Int64("storage_us", local.storage.Microseconds()),
			zap.Int64("wal_commit_us", local.walCommit.Microseconds()),
			zap.Int64("fanout_us", fin.fanTotal.Microseconds()),
			zap.Int64("resolve_us", fin.fan.resolve.Microseconds()),
			zap.Int64("push_us", fin.fan.push.Microseconds()),
			zap.Int("fanout_targets", fin.fan.targets),
			zap.Int64("report_us", fin.report.Microseconds()),
			zap.Int64("confirm_us", fin.confirm.Microseconds()),
			zap.Int64("total_us", time.Since(writeStart).Microseconds()))
	}()

	// One version is one Saga. Queue behind the writes already in flight for
	// this KB instead of letting them pile up on the storage nodes
	// (Stratum_设计文档v13.md §7.7). The wait honours ctx, so a caller that gives
	// up stops waiting rather than holding a place it no longer wants.
	stepStart := time.Now()
	if err := d.limiter.Acquire(ctx, kbID); err != nil {
		return err
	}
	tAcquire = time.Since(stepStart)
	defer d.limiter.Release(kbID)

	stepStart = time.Now()
	docIDs, err := d.writeLocalTransaction(ctx, kbID, versionID, parentVersionID, changes, &local)
	tLocal = time.Since(stepStart)
	if err != nil {
		// The storage layer reports that an attempt failed; the control layer
		// owns the terminal verdict (Stratum_设计文档v13.md §10.1).
		d.reportFailure(ctx, kbID, versionID, classifyLocalWriteFailure(err), fmt.Sprintf("local write failed: %v", err))
		return err
	}
	// Replicate to the other replicas and require a quorum before reporting the
	// version durable (v13 §7.1/§7.2). finishVersionWrite owns that tail — and the
	// crash-recovery path shares it, which is the point: a version resumed after a
	// crash must clear the same quorum bar as one that never crashed.
	return d.finishVersionWrite(ctx, kbID, versionID, len(changes), docIDs, &fin)
}

// PushIndexToReplicas ships a locally built index to the candidate replicas, so
// they load the artifact instead of building their own — "建一次、分发 N 份"
// (Stratum_设计文档v13.md §8.4).
//
// Best effort per replica, and it never fails the build. A replica that cannot
// be reached falls back to building for itself, which is exactly the behaviour
// the cluster had before distribution existed: distribution is an optimisation
// over that baseline, not a new way for a version to become unservable.
//
// §8.4(a): the call is bounded by indexPushSem. Distribution is the third of the
// storage layer's resource-hungry paths — after replayed writes and local index
// builds — and was the only one without a gate, so a batch of version switches
// or a set of replicas catching up at once would have every build's callback open
// a distribution at the same moment, each reading a whole index file into memory.
// The gate counts calls, not replicas: one call already covers N replicas, and N
// is small.
func (d *LocalDataPlane) PushIndexToReplicas(ctx context.Context, kbID string, versionID int64) error {
	if d.indexReader == nil || d.indexShipper == nil || d.resolveReplicas == nil {
		return nil // distribution not wired; each replica builds its own
	}
	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		return fmt.Errorf("plane: PushIndexToReplicas: resolve replicas: %w", err)
	}

	// §8.4(a) probe before the read. The probe needs only the ids, while the read
	// is where the memory peaks (tens of megabytes held in the builder for as long
	// as the fan-out runs), so asking first turns "everyone already has it" — the
	// multi-builder round-robin this exists for — into N small RTTs: no read, no
	// distribution slot, no outbound bytes.
	//
	// The probes deliberately sit OUTSIDE indexPushSem. That gate means "one
	// whole-file read plus its fan-out"; a probe queued behind one would lose
	// exactly the head start it is here to win. A peer that cannot answer counts
	// as "ship it": not knowing is not the same as knowing it is absent.
	needed := make([]string, 0, len(peers))
	var skipped int
	for _, peer := range peers {
		probeCtx, cancel := context.WithTimeout(ctx, peerCursorTimeout)
		held, probeErr := d.indexShipper.ProbeIndex(probeCtx, peer, kbID, versionID)
		cancel()
		switch {
		case probeErr != nil:
			d.logger.Debug("plane: index presence probe failed; shipping anyway",
				zap.String("peer", peer), zap.String("kb_id", kbID),
				zap.Int64("version_id", versionID), zap.Error(probeErr))
			needed = append(needed, peer)
		case held:
			skipped++
		default:
			needed = append(needed, peer)
		}
	}

	// The ship that follows probes too — PushIndex opens with the same frame — so
	// a peer can still answer "already held" between the two checks. Both feed the
	// same tally: the line below is about how much distribution the probe saved,
	// not about which of the two noticed. It is a deferred report because the
	// count is only final once the fan-out has finished.
	defer func() {
		if skipped > 0 {
			d.logger.Info("plane: index distribution skipped replicas that already hold the artifact",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Int("skipped", skipped))
		}
	}()

	if len(needed) == 0 {
		// Every candidate already holds the artifact: the read and the fan-out are
		// both unnecessary, so neither happens.
		return nil
	}

	// Acquire before the read, not before the ship: the read is what makes the
	// memory peak, so it has to be inside the gate. Honouring ctx matters as much
	// as the bound — the callers are background goroutines (the build callback in
	// cmd/stratum and reconcile), and one that cannot be cancelled would sit here
	// for as long as the storm lasts.
	if d.indexPushSem != nil {
		select {
		case d.indexPushSem <- struct{}{}:
			defer func() { <-d.indexPushSem }()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	indexData, sidecarData, err := d.indexReader.ReadIndexFiles(kbID, versionID)
	if err != nil {
		return fmt.Errorf("plane: PushIndexToReplicas(%s v%d): read local index: %w", kbID, versionID, err)
	}
	for _, peer := range needed {
		err := d.indexShipper.PushIndex(ctx, peer, kbID, versionID, indexData, sidecarData)
		switch {
		case err == nil:
		case errors.Is(err, stratinternalsync.ErrIndexAlreadyPresent):
			// The peer answered the in-stream probe with "already held". No bytes
			// moved for it, and the read above could not know that in advance
			// because the other peers did need the ship.
			skipped++
		default:
			d.logger.Warn("plane: index distribution failed; that replica will build its own",
				zap.String("peer", peer), zap.String("kb_id", kbID),
				zap.Int64("version_id", versionID), zap.Error(err))
		}
	}
	return nil
}

// DefaultMaxConcurrentIndexPush is how many PushIndexToReplicas runs may be in
// flight at once when nothing is configured (v13 §8.4(a)).
//
// One distribution reads a whole index file into memory and streams it to every
// replica, so this number bounds two resources at once: the builder's memory peak
// and its outbound bandwidth. Four is deliberately modest — the point is to turn
// a distribution storm (a batch of version switches, or replicas catching up at
// once) into a queue instead of a spike, not to saturate the network. Like the
// other budget numbers it is a §10.4 placeholder, to be revised once measured.
const DefaultMaxConcurrentIndexPush = 4

// newIndexPushSem builds the distribution semaphore. A non-positive limit takes
// the default, following LocalDataPlaneConfig.MaxInFlightWrites' convention.
func newIndexPushSem(limit int) chan struct{} {
	if limit <= 0 {
		limit = DefaultMaxConcurrentIndexPush
	}
	return make(chan struct{}, limit)
}

// The confirmation broadcast's budget. All placeholder values in the §10.4 sense:
// what matters is the shape — a per-attempt deadline, a bounded number of
// attempts, and a whole-broadcast ceiling with room for every peer to spend its
// own retries.
const (
	// confirmAttempts is how many times one candidate is told the version reached
	// quorum. A confirmation is idempotent and the receiver only stands down a
	// timer, so a repeat costs nothing.
	confirmAttempts = 3
)

// The timing values are variables rather than constants so a test can shrink them
// (the takeoverTimeout precedent): the case worth pinning down is a peer that
// hangs for its whole attempt deadline, and no test should spend seconds per
// attempt to produce it. Unexported, and written nowhere outside tests.
var (
	// confirmAttemptTimeout bounds ONE attempt. Same order as peerCursorTimeout:
	// this is a small RPC between peers, and one unreachable peer must not hold
	// the broadcast.
	confirmAttemptTimeout = 2 * time.Second
	// confirmRetryBackoff is the pause between attempts — short enough that a
	// peer's retries fit the broadcast budget, long enough to let a peer that is
	// still coming up become reachable.
	confirmRetryBackoff = 500 * time.Millisecond
	// confirmBroadcastBudget bounds the whole broadcast, resolving the peers
	// included: confirmAttempts × (confirmAttemptTimeout + confirmRetryBackoff)
	// per peer, for a replica set of a few peers, with room to spare.
	confirmBroadcastBudget = 60 * time.Second
)

// broadcastConfirmation tells the candidate replicas that the version reached
// quorum, so any §7.3 takeover timer they started can stand down.
//
// Best effort and asynchronous: the write is already durable and reported, and
// a replica that misses this message only checks for itself later — an extra
// announcement, never a missing one. That asymmetry is why the caller is not
// made to wait on it, and why everything below runs on its own budget.
//
// It goes to *every* candidate rather than only the replicas that acknowledged,
// for the same reason the cleanup broadcast does (§10.6): the coordinator does
// not track which push succeeded, and a stray confirmation to a replica that
// never received the data is harmless.
//
// Each peer gets its OWN context and its own bounded retry budget. Sharing one
// context across the loop was a real defect, and its failure mode is invisible in
// the logs it produces: the first peer to hang — an offline replica is the normal
// case, since the write that triggered this confirmation has just failed to push
// to it — consumed the whole window, and every peer after it then failed instantly
// with "context deadline exceeded" without ever being dialled. Measured with one
// storage node down: the confirmation reached NEITHER of the other two, so no
// quorum ever formed, the version's digest was never committed, and a returning
// node could not tell that version from an empty one — it advances over an empty
// version without fetching anything, so the miss reads as a successful catch-up.
func (d *LocalDataPlane) broadcastConfirmation(kbID string, versionID int64) {
	if d.confirmer == nil || d.resolveReplicas == nil {
		return
	}
	go func() {
		// A fresh context: the request that ran the write is over by now. This
		// bounds the whole broadcast — including resolving the peers — so a slow
		// peer cannot starve the ones behind it.
		ctx, cancel := context.WithTimeout(context.Background(), confirmBroadcastBudget)
		defer cancel()
		peers, err := d.resolveReplicas(ctx)
		if err != nil {
			return
		}
		for _, peer := range peers {
			d.confirmPeer(ctx, peer, kbID, versionID)
		}
	}()
}

// confirmPeer tells one peer that the version reached quorum, retrying a bounded
// number of times with a deadline of its own for each attempt.
//
// A context per attempt (rather than one for the whole peer) is what makes the
// retry mean anything: a single unreachable address should cost one attempt's
// deadline, not the peer's entire budget.
func (d *LocalDataPlane) confirmPeer(ctx context.Context, peer, kbID string, versionID int64) {
	var lastErr error
	for attempt := 1; attempt <= confirmAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			lastErr = err
			break
		}
		attemptCtx, cancel := context.WithTimeout(ctx, confirmAttemptTimeout)
		// The confirmation doubles as the §8.5 announcement: it carries this
		// node's own address so the peer records where the version's data is
		// (see DataSourceRegistry). An empty address means "announce nothing".
		err := d.confirmer.ConfirmVersionWrite(attemptCtx, peer, kbID, versionID, d.selfDataSyncAddr)
		cancel()
		if err == nil {
			if attempt > 1 {
				// Worth a line: it says the earlier attempt was lost to something
				// transient, which is the whole reason the retry exists.
				d.logger.Info("plane: confirm version write landed on a retry",
					zap.String("peer", peer), zap.String("kb_id", kbID),
					zap.Int64("version_id", versionID), zap.Int("attempt", attempt))
			}
			return
		}
		lastErr = err
		if attempt < confirmAttempts {
			select {
			case <-ctx.Done():
			case <-time.After(confirmRetryBackoff):
			}
		}
	}
	// Still best effort, so this stays a warning: failing the write because a
	// confirmation did not land would turn lost gossip into data loss. The
	// attempts count is what separates "the peer was unreachable" from "we only
	// tried once".
	d.logger.Warn("plane: confirm version write",
		zap.String("peer", peer), zap.String("kb_id", kbID),
		zap.Int64("version_id", versionID), zap.Int("attempts", confirmAttempts), zap.Error(lastErr))
}

// classifyLocalWriteFailure decides how the control layer should treat a failed
// local write (Stratum_设计文档v13.md §10.1).
//
// Only the cases that no amount of retrying can fix are fatal: a knowledge base
// that is gone, or input the state machine will keep rejecting. Everything
// else — a full disk, an unavailable dependency, a timeout — is worth another
// attempt, possibly on another node.
func classifyLocalWriteFailure(err error) types.FailureClass {
	switch {
	case errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound),
		errors.Is(err, stratumerrors.ErrKnowledgeBaseDeleted),
		errors.Is(err, stratumerrors.ErrInvalidArgument),
		errors.Is(err, stratumerrors.ErrInvalidParentVersion),
		errors.Is(err, stratumerrors.ErrVersionNotFound):
		return types.FailureFatalGlobal
	default:
		return types.FailureTransient
	}
}

// reportFailure tells the control layer that an attempt at making the version
// durable failed. A report error is logged rather than returned: the caller is
// already handling a failure, and losing a report costs retries — not
// correctness, since the verdict is only ever "stop retrying automatically".
func (d *LocalDataPlane) reportFailure(ctx context.Context, kbID string, versionID int64, class types.FailureClass, detail string) {
	if d.control == nil {
		return
	}
	terminal, err := d.control.ReportVersionFailure(ctx, kbID, versionID, types.FailureSideData, class, detail)
	if err != nil {
		d.logger.Warn("plane: report version failure",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}
	if !terminal {
		return
	}
	// The verdict just landed: reclaim whatever physical data made it to disk HERE —
	// no broadcast.
	//
	// §10.6 broadcast because the control layer never knew which replicas had received
	// the version. That is no longer the situation: every replica is told by its own
	// apply of the verdict (see NoteTerminalVersion), so broadcasting again would turn
	// N nodes into N×N calls and mostly re-reach replicas already doing it. What this
	// path adds is SPEED — the node that detected the failure cleans up now, instead of
	// when its apply loop reaches the entry.
	if err := d.reclaimTerminalVersionLocally(kbID, versionID); err != nil {
		d.logger.Warn("plane: cleanup after a permanent failure",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

// localStages is where one local write transaction's time went, split out of
// plane's `write: stage timings` local_us.
//
// Why it is split: local_us is the second largest stage of a write transaction
// (p50 1.34 s for a 2,000-document batch, 2.68 s for 8,000) and was a single
// opaque number covering three unrelated things — write-ahead-log framing on both
// sides of the storage write, and the storage write itself. Which of them
// dominates decides what is worth optimising, so they are reported apart.
type localStages struct {
	walBegin  time.Duration
	storage   time.Duration
	walCommit time.Duration
}

// fanoutStages is where one fan-out's time went, split out of `write: stage
// timings` fanout_us: resolving the replica set, then waiting for quorum.
//
// targets is the number of replicas asked (this node's own copy excluded), which
// is what turns "fan-out was slow" into "fan-out waited on N replicas".
type fanoutStages struct {
	resolve time.Duration
	push    time.Duration
	targets int
}

// writeLocalTransaction frames and runs this node's half of the write:
// BEGIN (persisting the replay input) → the per-change storage writes →
// COMMIT. Returns the version's document-ID set. st, when non-nil, receives the
// per-stage split; callers that are not measuring pass nil.
func (d *LocalDataPlane) writeLocalTransaction(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange, st *localStages) ([]string, error) {
	if d.wal == nil || d.executor == nil {
		return nil, fmt.Errorf("plane: WriteVersionData: the storage write transaction is not configured")
	}
	beginStart := time.Now()
	err := d.wal.WriteBegin(ctx, kbID, parentVersionID, changes)
	if st != nil {
		st.walBegin = time.Since(beginStart)
	}
	if err != nil {
		return nil, fmt.Errorf("plane: WriteVersionData: WAL.WriteBegin: %w", err)
	}
	storageStart := time.Now()
	docIDs, err := d.executor.WriteVersionStorage(ctx, kbID, parentVersionID, versionID, changes)
	if st != nil {
		st.storage = time.Since(storageStart)
	}
	if err != nil {
		return nil, err
	}
	commitStart := time.Now()
	err = d.wal.WriteCommit(ctx, versionID)
	if st != nil {
		st.walCommit = time.Since(commitStart)
	}
	if err != nil {
		return nil, fmt.Errorf("plane: WriteVersionData: WAL.WriteCommit: %w", err)
	}
	d.advanceLocalVersion(kbID, versionID)
	return docIDs, nil
}

// fanOut hands the version to the other replicas concurrently and fails unless
// a majority acknowledged. Counting this node's own completed write as the
// first acknowledgement, quorum is a majority of (1 + targets) — see
// QuorumSize.
//
// A failed or missing replica does not fail the write on its own: only falling
// short of quorum does, and the error names the shortfall so the caller's Saga
// can decide (retry, or hand the version to §10.1's permanent-failure path).
// ApplyBackfillChanges writes one version's data on a node that is catching up
// (§7.5): the changes are replayed from a peer's WAL, so this node ends up with
// the same records the original writer produced.
//
// It deliberately stops at the local transaction, skipping everything
// WriteVersionData layers on top:
//
//   - no fan-out: the replicas it would push to are the very peers this node is
//     learning from, and they already hold the version. A node catching up does
//     not notify the nodes it is copying from.
//   - no durable report (digest): this version's durability was settled long ago —
//     that is precisely why the peer's WAL range was still readable. Reporting it
//     again confirms a fact nobody is waiting on.
//   - no index scheduling: historical versions build lazily or never (§8.6b), so
//     an unconditional build attempt spends work on something no query asks for.
//     If the version happens to be the active one, the very apply that triggered
//     this backfill schedules its build right after.
//   - no failure report: reportFailure is the *coordinator's* channel and can end
//     in §10.1's terminal verdict. A node that merely cannot fetch someone else's
//     history must not be able to declare that version permanently failed.
func (d *LocalDataPlane) ApplyBackfillChanges(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
	// writeLocalTransaction advances the cursor itself, so there is nothing else to
	// do here: the version counts as held once its data is in the stores.
	if _, err := d.writeLocalTransaction(ctx, kbID, versionID, parentVersionID, changes, nil); err != nil {
		return fmt.Errorf("plane: backfill apply %s v%d: %w", kbID, versionID, err)
	}
	return nil
}

// replicaPushTimeout bounds ONE replica's push during fan-out.
//
// Why not the caller's budget: a push inherits the candidate's dispatch budget
// (plane.CoordinatorDispatcher.candidateBudget = 15 s + 50 ms per document,
// capped at 10 min) when nothing bounds it here, and fan-out waits for every
// target — so ONE unreachable replica costs the whole budget. Measured on the
// 3+3 cluster with one storage replica killed: fanout_us = 15026509 (exactly the
// 15 s budget) inside a dispatch whose own budget is the same 15 s, which made a
// write that HAD reached quorum look like a candidate failure.
//
// It is a floor, not the whole story: PushVersion sends the version's identity,
// and the replica it reaches then pulls and writes the data itself (§7.5), so
// the time a push legitimately needs scales with the batch. A 1000-document
// batch measured pushBudget == the timeout exactly on all three replicas —
// every push timed out while the peers were busy committing the very version
// being pushed — which is what pushTimeoutPerDoc pays for.
const replicaPushTimeout = 3 * time.Second

// pushTimeoutPerDoc is what one document's worth of remote work adds to a push's
// budget: the peer pulls the documents and runs its own local transaction
// (measured at ~14.6 ms/document for a 1000-document batch, split/embed/write).
// Without it the budget is a constant while the work is not, and a large batch
// times out on a replica that is doing exactly what it was asked to.
const pushTimeoutPerDoc = 20 * time.Millisecond

func (d *LocalDataPlane) fanOut(ctx context.Context, kbID string, versionID int64, docs int, st *fanoutStages) error {
	if d.pusher == nil || d.resolveReplicas == nil {
		return nil // replication not configured: the local write is the quorum
	}
	resolveStart := time.Now()
	targets, err := d.resolveReplicas(ctx)
	if st != nil {
		st.resolve = time.Since(resolveStart)
		st.targets = len(targets)
	}
	if err != nil {
		return fmt.Errorf("plane: fan-out for version %d: resolve replicas: %w", versionID, err)
	}
	if len(targets) == 0 {
		// 解析不出任何副本。"没有副本"在 quorum 判定上与"副本就是我自己"是同一
		// 件事——于是这一版被当成已复制完成,控制层据此推进。这里从前静默返回成
		// 功,而沉默有代价:多副本部署里它意味着其余副本永远拿不到这一版。
		//
		// 但空列表有两种来源,只有一种是故障。单副本部署里它**结构上就是空的**
		// (ResolveReplicas 列的是"除我之外的副本",而我之外没有副本),那种情况
		// 下本地写就是整个 quorum,是正常结果。ReplicaCount 用来区分二者:不区分
		// 的话每次写都会告警,而真正的故障会淹在里头。
		fields := []zap.Field{
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.Int("replica_count", d.replicaCount),
		}
		if d.replicaCount <= 1 {
			d.logger.Debug("plane: fan-out has no replica targets; this node's own copy is the whole quorum",
				fields...)
			return nil
		}
		d.logger.Warn("plane: fan-out found no replica targets on a multi-replica deployment; "+
			"treating this node's own copy as the quorum, and the other replicas will not receive this version",
			fields...)
		return nil
	}

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		acked  = 1 // this node's own local write
		needed = QuorumSize(1 + len(targets))
		total  = 1 + len(targets)

		// reached fires the moment quorum is satisfied; allDone fires when every
		// push has settled. Waiting only for the latter is what made a dead
		// replica cost the entire budget: quorum is the contract, and the
		// replicas beyond it are free to be slow.
		reached = make(chan struct{})
		once    sync.Once
		allDone = make(chan struct{})
	)
	// Bound every push by its own budget, and never by more than the caller has
	// left: a push that inherits the dispatch budget would spend it in full on a
	// replica that is simply gone.
	//
	// The budget scales with the batch because the peer's work does: PushVersion
	// hands over the version's identity and the peer pulls and writes those
	// documents itself (§7.5). A constant budget against scaling work times out on
	// a peer doing exactly what it was asked to — measured on a 1000-document
	// batch, where pushBudget was the timeout to the microsecond on all three
	// replicas while each was busy committing the version being pushed.
	pushBudget := replicaPushTimeout + time.Duration(docs)*pushTimeoutPerDoc
	if deadline, ok := ctx.Deadline(); ok {
		if left := time.Until(deadline); left < pushBudget {
			pushBudget = left
		}
	}
	pushStart := time.Now()
	for _, target := range targets {
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			pushCtx, cancel := context.WithTimeout(ctx, pushBudget)
			defer cancel()
			if _, err := d.pusher.PushVersion(pushCtx, target, kbID, versionID); err != nil {
				d.logger.Warn("plane: replica push failed",
					zap.String("replica", target), zap.String("kb_id", kbID),
					zap.Int64("version_id", versionID), zap.Error(err))
				return
			}
			mu.Lock()
			acked++
			satisfied := acked >= needed
			mu.Unlock()
			if satisfied {
				once.Do(func() { close(reached) })
			}
		}(target)
	}
	go func() {
		wg.Wait()
		close(allDone)
	}()

	select {
	case <-reached:
		// Quorum is in hand; the remaining pushes keep running in the background
		// with their own budget.
	case <-allDone:
	case <-ctx.Done():
		// The caller's own bound expired: report what we have below.
	}
	if st != nil {
		st.push = time.Since(pushStart)
	}

	mu.Lock()
	final := acked
	mu.Unlock()
	if final < needed {
		return fmt.Errorf(
			"plane: version %d of %s reached %d/%d acknowledgements, below the quorum of %d",
			versionID, kbID, final, total, needed)
	}
	return nil
}

// peerCursorTimeout bounds ONE peer's cursor query.
//
// It exists because the callers run during STARTUP: ReconcileIndexes asks every peer
// what it holds, and every storage node does that concurrently, reaching
// grpcServer.Serve only after its own reconcile returns. With no bound here the
// cluster deadlocks at boot — each node waits for the others while none of them is
// serving. Measured on a node holding historical data: it never logged "Stratum gRPC
// server listening", and the control layer got "connection refused" from every
// storage address.
//
// Short on purpose. A peer that cannot answer within this window while starting is
// not going to answer sooner if we wait longer, and the quorum check is what decides
// whether the answers that DID arrive suffice. Like the other budget numbers this is
// a placeholder: raise it if startup on slow disks shows peers being skipped that
// would have answered.
const peerCursorTimeout = 2 * time.Second

// QuorumSize is the durable threshold for n replicas (Stratum_设计文档v13.md
// §7.1): ⌈(n+1)/2⌉, a bare majority, so any two quorums intersect.
func QuorumSize(n int) int {
	if n <= 0 {
		return 0
	}
	return (n+1)/2 + (n+1)%2
}

// finishStages is where a completed write's tail went: the fan-out (itself split
// into resolve/push by fanoutStages), the report, and the confirmation broadcast.
type finishStages struct {
	fanTotal time.Duration
	fan      fanoutStages
	report   time.Duration
	confirm  time.Duration
}

// finishVersionWrite is the tail of every write whose local transaction has
// already COMMITted: replicate to the candidate replicas, require a quorum, and
// only then report the version durable.
//
// Both the normal path (WriteVersionData) and the crash-recovery path
// (ResumeVersionWrite) end here. They used to implement this tail separately, and
// the recovery copy skipped the fan-out — going straight to the durable report for
// replicas that were never written to. That is the exact claim the failure branch
// below refuses to make without a quorum ("making it without quorum would be a lie
// the control layer acts on"), so the two paths had ended up disagreeing about what
// durability means. Sharing one function is what keeps them from drifting apart
// again: a step added here reaches both.
func (d *LocalDataPlane) finishVersionWrite(ctx context.Context, kbID string, versionID int64, docs int, docIDs []string, st *finishStages) error {
	fanStart := time.Now()
	var fan fanoutStages
	fanErr := d.fanOut(ctx, kbID, versionID, docs, &fan)
	if st != nil {
		st.fanTotal = time.Since(fanStart)
		st.fan = fan
	}
	if fanErr != nil {
		// The local transaction above already committed: this node holds the
		// version's documents, its document-ID set and its WAL commit record. What
		// fan-out adds is durability on OTHER replicas — which §7.5 restores by
		// backfill — not the version's existence, and not its ability to be served
		// from here.
		//
		// So the build is scheduled on this path too. It used to be reachable only
		// through reportAndSchedule on the success path, which left a version whose
		// replication fell short with no index and no report at all: the control
		// layer kept it PENDING, every query answered "index not ready", and the
		// documents sat on disk with nobody able to finish or serve them. Measured
		// on a 1000-document batch: report_us = 0 on all three replicas, each of
		// which had already spent 14.6 s committing that very version.
		reportStart := time.Now()
		d.scheduleIndexBuild(ctx, kbID, versionID)
		if st != nil {
			st.report = time.Since(reportStart)
		}

		// Replication failed, and it is still transient (peers come back), so it is
		// reported and returned — the caller's Saga owns the retry decision (§10.1).
		// Note what is deliberately NOT done: no durable report. Durability is
		// precisely the claim quorum was supposed to establish, and making it
		// without quorum would be a lie the control layer acts on.
		d.reportFailure(ctx, kbID, versionID, types.FailureTransient, fmt.Sprintf("replication failed: %v", fanErr))
		return fanErr
	}

	reportStart := time.Now()
	d.reportAndSchedule(ctx, kbID, versionID, docIDs)
	if st != nil {
		st.report = time.Since(reportStart)
	}

	// len(docIDs) == 0 is the whole reason the announcement carries a flag: a
	// version with no documents is never fanned out (fanOut above sends nothing),
	// so replicas that did NOT coordinate it have no other way to learn it exists —
	// and their cursors would stay behind it, which the station reads as "stale"
	// (§9.3(2)).
	confirmStart := time.Now()
	d.broadcastConfirmation(kbID, versionID)
	if st != nil {
		st.confirm = time.Since(confirmStart)
	}
	return nil
}

// ResumeVersionWrite re-runs the storage writes and commits without writing a
// second BEGIN: the transaction's BEGIN record is already durable (crash
// recovery drove this path).
func (d *LocalDataPlane) ResumeVersionWrite(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
	if d.wal == nil || d.executor == nil {
		return fmt.Errorf("plane: ResumeVersionWrite: the storage write transaction is not configured")
	}
	docIDs, err := d.executor.WriteVersionStorage(ctx, kbID, parentVersionID, versionID, changes)
	if err != nil {
		return err
	}
	if err := d.wal.WriteCommit(ctx, versionID); err != nil {
		return fmt.Errorf("plane: ResumeVersionWrite: WAL.WriteCommit: %w", err)
	}
	d.advanceLocalVersion(kbID, versionID)
	// The same tail the normal path runs, fan-out included. Recovery used to skip
	// straight to the durable report, so a crash during the local write produced a
	// version the control layer called durable while no other replica held it.
	return d.finishVersionWrite(ctx, kbID, versionID, len(docIDs), docIDs, nil)
}

// reportAndSchedule finishes a completed write transaction: it reports the
// version's document-set digest up (v1 §4.2 ReportDataDurable — followers use
// it to verify their data pulls) and schedules the index build. Both stay
// best-effort, exactly as they were inside the coordinator: a missed digest
// only costs followers a best-effort pull, and a failed build can be retried.
func (d *LocalDataPlane) reportAndSchedule(ctx context.Context, kbID string, versionID int64, docIDs []string) {
	if d.control != nil {
		digest := stratinternalsync.ComputeDocIDSetHash(docIDs)
		if err := d.control.ReportDataDurable(ctx, kbID, versionID, digest); err != nil {
			if errors.Is(err, stratumerrors.ErrVersionNotFound) {
				// The version is gone — discarded by its caller, or deleted — while
				// its write path was still finishing. Discarding means "act as if
				// this never existed", so a later digest report for it has nowhere
				// to land: expected, not a failure. The index build's callback
				// reaches the same conclusion the same way (see internal/index's
				// invokeCallback).
				//
				// Telling this apart matters because the warning below is the one
				// worth reading — a version with no durable digest never leaves
				// PENDING — and firing it on every discard is what buries that.
				d.logger.Debug("plane: the version is gone (discarded or deleted); no digest to report",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
			} else {
				// This used to be discarded (`_ =`), and the silence is expensive: with no
				// digest the version carries no evidence of having been durably
				// replicated, so cursor recovery reads it as "not held here"
				// (holdsVersionLocally) and the data side never leaves PENDING.
				// Measured on a 3-node cluster: not one version reached DATA_DURABLE, while
				// fan-out itself never failed — so whatever goes wrong, it goes wrong
				// here.
				d.logger.Warn("plane: reporting the version's digest failed; it keeps no durable digest",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			}
		} else {
			d.logger.Debug("plane: reported the version's digest",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Int("docs", len(docIDs)))
		}
	} else {
		d.logger.Debug("plane: no control plane wired; the version's digest is not reported",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
	}
	d.scheduleIndexBuild(ctx, kbID, versionID)
}

// scheduleIndexBuild asks for this version's index to be built on this node.
//
// It is separate from reportAndSchedule because the two have different
// preconditions. Reporting durable is a claim about *quorum*: only the caller
// that got enough replica acknowledgements may make it (§7.1). Scheduling the
// build is a claim about *this node*, and it is warranted as soon as the local
// transaction has committed — the documents, the document-ID set and the WAL
// commit record are all here, so the index can be built and a query answered
// from this node whenever the version becomes servable.
func (d *LocalDataPlane) scheduleIndexBuild(ctx context.Context, kbID string, versionID int64) {
	if d.indexMgr != nil {
		_ = d.indexMgr.TriggerBuild(ctx, kbID, versionID)
	}
}

// NoteTerminalVersion records that the control layer settled versionID's DATA side
// as terminal — learned from THIS node's own apply of the Raft entry, not from
// §10.6's broadcast.
//
// That difference is the point. The broadcast goes to the candidate replicas and is
// best-effort: a replica that is partitioned, or that is restarting while it fires,
// never hears it, and its copy of the version stays behind with nothing left to
// notice it (the metadata that would have named it is gone). Every node applies the
// same Raft entry, so this path reaches exactly the nodes that have to reclaim.
//
// It reclaims LOCALLY, and that is not an optimisation — it is what the apply path
// changes. §10.6's DropVersionData broadcasts because the node issuing it does not
// know which replicas hold the version; here every replica is told by its own apply,
// so broadcasting again would turn N nodes into N×N calls (the same reasoning §B's
// reconciliation uses). A node that never held the version runs a prefix delete over
// nothing and moves on.
//
// Called from the apply loop, so it does the cheap part inline (one local delete, no
// network) and hands the retry to its own queue — see terminalReclaimInterval for why
// that cadence is deliberately not §10.6's.
func (d *LocalDataPlane) NoteTerminalVersion(kbID string, versionID int64) {
	if err := d.reclaimTerminalVersionLocally(kbID, versionID); err != nil {
		d.logger.Warn("plane: local reclaim of a terminal version failed; retrying on its own cadence",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

// ReclaimTerminalVersions rebuilds the apply-driven reclaim list at startup.
//
// The list is memory (see NoteTerminalVersion), and a terminal verdict does NOT come
// back on a restart: a node that was down when the entry was applied only replays it if
// the entry is still in the log — after compaction the state machine is restored from
// the snapshot and the entry is never applied individually, so the hook that would have
// queued the reclaim never fires. Measured on a single node (verdict applied, log
// compacted, restart): the hook does not fire again, and that version's leftover data
// is nobody's business again.
//
// What DOES survive is the version's metadata: a data-side verdict lives in
// DataStatusFailedPermanent, which the snapshot carries. So this costs no persistence
// at all — sweep the state machine once and queue exactly what the apply hook would
// have queued.
//
// The INDEX side is deliberately excluded (§10.1b): that verdict says a build failed,
// not that the data is gone, so reclaiming storage on its behalf would throw away good
// data. A version that was DELETED meanwhile is not in the metadata at all; those go
// through DeleteVersion's own cleanup (which persists its intent and can be
// re-triggered), so a sweep cannot see them and does not need to.
func (d *LocalDataPlane) ReclaimTerminalVersions(ctx context.Context, meta MetadataLister) (int, error) {
	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		return 0, fmt.Errorf("plane: reclaim terminal versions: list knowledge bases: %w", err)
	}
	queued := 0
	for _, kb := range kbs {
		versions, err := meta.ListVersions(ctx, kb.KBID)
		if err != nil {
			// One unreadable knowledge base must not stop the rest: the sweep is
			// idempotent, and the next start tries again.
			d.logger.Warn("plane: reclaim terminal versions: list versions failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
			continue
		}
		for _, v := range versions {
			if v.DataStatus != types.DataStatusFailedPermanent {
				continue
			}
			d.NoteTerminalVersion(kb.KBID, v.VersionID)
			queued++
		}
	}
	return queued, nil
}

// ReclaimVersionDataLocally implements DataPlane: the local half of DropVersionData,
// with no broadcast. See the interface comment for who is allowed to call it.
func (d *LocalDataPlane) ReclaimVersionDataLocally(_ context.Context, kbID string, versionID int64) error {
	return d.reclaimTerminalVersionLocally(kbID, versionID)
}

// cleanupTask is one version whose cleanup did not finish — an entry in the
// apply-driven reclaim queue (see NoteTerminalVersion). The broadcast queue it used to
// belong to is gone: its only producer was a failed broadcast, and that case is now
// covered by the apply path and by the deletion flow's own persistence.
type cleanupTask struct {
	kbID      string
	versionID int64
	attempts  int
}

// terminalReclaimInterval / terminalReclaimAttempts pace the reclaim queue, which is
// now the ONLY cleanup-retry channel: §10.6's broadcast queue is gone, because every
// node learns a terminal verdict from its own apply and reclaims locally
// (NoteTerminalVersion).
//
// The cadence follows from what this queue touches — only THIS node's storage, for a
// version whose data will never arrive. A failed local prefix delete is disk hygiene
// that a later pass fixes, so the interval can be generous (a minute) and the patience
// long (ten attempts): nothing is waiting on it, and nothing else will do it.
var terminalReclaimInterval = time.Minute

const terminalReclaimAttempts = 10

// reclaimTerminalVersionLocally removes whatever this node still holds for a version
// the control layer retired, and steps the cursor past it.
//
// No broadcast (see NoteTerminalVersion) and no existence probe: DropVersionStorage is
// a prefix delete, so "I do not have it" is a no-op rather than a case to detect.
func (d *LocalDataPlane) reclaimTerminalVersionLocally(kbID string, versionID int64) error {
	if d.dropper != nil {
		if err := d.dropper.DropVersionStorage(context.Background(), kbID, versionID); err != nil {
			d.scheduleTerminalReclaim(kbID, versionID)
			return err
		}
	}
	// The cursor steps over it only once the bytes are gone: a version whose data will
	// never arrive must not hold the contiguous cursor back (that would read as "this
	// node is behind" and start a catch-up that can never finish), and the order is the
	// same invariant every other advance follows — data first, cursor second.
	d.advanceLocalVersion(kbID, versionID)
	d.clearTerminalReclaim(kbID, versionID)
	return nil
}

// scheduleTerminalReclaim remembers a local reclaim that did not finish. Keyed per
// version (repeats collapse) and bounded by terminalReclaimAttempts, so it cannot
// become a leak of its own.
func (d *LocalDataPlane) scheduleTerminalReclaim(kbID string, versionID int64) {
	key := failureKey(kbID, versionID)
	d.terminalReclaimMu.Lock()
	if d.terminalReclaim == nil {
		d.terminalReclaim = make(map[string]*cleanupTask)
	}
	if _, ok := d.terminalReclaim[key]; !ok {
		d.terminalReclaim[key] = &cleanupTask{kbID: kbID, versionID: versionID}
	}
	d.terminalReclaimMu.Unlock()
}

func (d *LocalDataPlane) clearTerminalReclaim(kbID string, versionID int64) {
	key := failureKey(kbID, versionID)
	d.terminalReclaimMu.Lock()
	delete(d.terminalReclaim, key)
	d.terminalReclaimMu.Unlock()
}

// StartTerminalReclaims runs the reclaim retry loop until ctx ends.
func (d *LocalDataPlane) StartTerminalReclaims(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(terminalReclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.retryTerminalReclaims(ctx)
			}
		}
	}()
}

// retryTerminalReclaims makes one pass: each unfinished reclaim is retried, and the
// ones that have run out of attempts are dropped with an error.
func (d *LocalDataPlane) retryTerminalReclaims(ctx context.Context) {
	d.terminalReclaimMu.Lock()
	pending := make([]*cleanupTask, 0, len(d.terminalReclaim))
	for _, task := range d.terminalReclaim {
		pending = append(pending, task)
	}
	d.terminalReclaimMu.Unlock()

	for _, task := range pending {
		if d.dropper != nil {
			if err := d.dropper.DropVersionStorage(ctx, task.kbID, task.versionID); err != nil {
				task.attempts++
				if task.attempts >= terminalReclaimAttempts {
					d.clearTerminalReclaim(task.kbID, task.versionID)
					d.logger.Error("plane: giving up on a local terminal-version reclaim; leftover bytes need an operator",
						zap.String("kb_id", task.kbID), zap.Int64("version_id", task.versionID),
						zap.Int("attempts", task.attempts), zap.Error(err))
					continue
				}
				continue
			}
		}
		d.advanceLocalVersion(task.kbID, task.versionID)
		d.clearTerminalReclaim(task.kbID, task.versionID)
	}
}

// PendingTerminalReclaims reports how many local reclaims are waiting to be retried,
// for diagnostics and tests.
func (d *LocalDataPlane) PendingTerminalReclaims() int {
	d.terminalReclaimMu.Lock()
	defer d.terminalReclaimMu.Unlock()
	return len(d.terminalReclaim)
}

// DropVersionData: see WriteVersionData.
// DropVersionData removes versionID's physical data — on this node and on
// every candidate replica.
//
// It deliberately covers *all* candidates rather than only the replicas that
// acknowledged the write: when the control layer declares a version permanently
// failed it does not know which replicas actually landed the data, so a no-op
// on a replica that never received it is far cheaper than leaking the data on
// the one that did (Stratum_设计文档v13.md §10.6).
//
// The cursor is advanced past the version: a permanently failed version will
// never have data, so treating it as "known" is what keeps the gap from
// looking like something a later backfill should fetch.
func (d *LocalDataPlane) DropVersionData(ctx context.Context, kbID string, versionID int64) error {
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if d.cleaner != nil && d.resolveReplicas != nil {
		peers, err := d.resolveReplicas(ctx)
		if err != nil {
			record(fmt.Errorf("plane: DropVersionData: resolve replicas: %w", err))
		} else {
			for _, peer := range peers {
				if err := d.cleaner.DeleteVersionData(ctx, peer, kbID, versionID, "permanently failed version"); err != nil {
					record(fmt.Errorf("plane: DropVersionData: broadcast to %s: %w", peer, err))
				}
			}
		}
	}

	if d.dropper != nil {
		if err := d.dropper.DropVersionStorage(ctx, kbID, versionID); err != nil {
			record(fmt.Errorf("plane: DropVersionData: local drop: %w", err))
		}
	} else if d.cleaner == nil {
		return fmt.Errorf("plane: DropVersionData: not wired (no local dropper and no broadcaster)")
	}
	d.advanceLocalVersion(kbID, versionID)
	// A broadcast that did not reach everyone is no longer retried here. It used to be:
	// §10.6's only way to reclaim a version on a replica that missed the message was to
	// tell it again. Every replica now learns a terminal verdict from its own apply and
	// reclaims locally (NoteTerminalVersion), and the DELETION path keeps its own
	// persisted intent (the Deleting marker plus its coordinator's retries) — so this
	// channel had no work left, only a queue, a background loop and a set of parameters.
	if firstErr != nil {
		d.logger.Warn("plane: version cleanup broadcast did not reach every candidate",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(firstErr))
	}
	return firstErr
}

// SetDurabilityPolicy records the declarative target for kbID.
//
// The failure budget it carries is passed straight on to the control layer,
// which owns the terminal verdict (Stratum_设计文档v13.md §10.1): the policy is
// declared at the storage-layer boundary but enforced where the decision lives.
// The replication target itself stays advisory for now — the replica set comes
// from the cluster's topology (§7.1/§7.2), not from this value.
func (d *LocalDataPlane) SetDurabilityPolicy(ctx context.Context, kbID string, policy DurabilityPolicy) error {
	// §7.7: the in-flight cap is enforced here, where the backlog would form.
	d.limiter.SetLimit(kbID, policy.MaxInFlightWrites)

	if d.control == nil || policy.MaxFailures <= 0 {
		return nil
	}
	if err := d.control.SetFailureBudget(ctx, kbID, policy.MaxFailures); err != nil {
		return fmt.Errorf("plane: SetDurabilityPolicy(%s): %w", kbID, err)
	}
	return nil
}

// TransactionWAL frames the storage layer's write transaction. *wal.FileWAL
// satisfies it; keeping it narrow keeps the DataPlane independent of the rest
// of the WAL surface (deletes, recovery, replay counters).
type TransactionWAL interface {
	WriteBegin(ctx context.Context, kbID string, parentVersionID int64, changes []types.DocChange) error
	WriteCommit(ctx context.Context, versionID int64) error
}

// CursorStore persists this node's contiguous data cursor per knowledge base,
// so a restart reads the local fact back instead of inferring it from disk
// (Stratum_设计文档v13.md §7.8; docs/cursor-persistence-plan.md §3). *wal.FileWAL
// satisfies it. Deliberately narrow — two calls, not the whole log surface —
// so the plane stays independent of the WAL's transaction and recovery halves.
type CursorStore interface {
	// WriteCursor records that this node's contiguous history for kbID has
	// reached versionID. Monotone; a lower value is a no-op.
	WriteCursor(ctx context.Context, kbID string, versionID int64) error
	// RecoverCursors returns every cursor this node recorded. A knowledge base
	// absent from the map has no record, which is not the same as cursor 0.
	RecoverCursors(ctx context.Context) (map[string]int64, error)
}

// VersionWriteExecutor performs one version's storage-layer writes: split,
// embed, chunk/docstore/versiondoc writes and the version document bloom. It
// deliberately does NOT frame the WAL transaction (BEGIN/COMMIT) — that
// framing is what makes the transaction the storage layer's own, and it lives
// in WriteVersionData.
type VersionWriteExecutor interface {
	WriteVersionStorage(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) ([]string, error)
}

// MetadataLister is the read-only view of replicated metadata the storage
// layer needs in order to reconcile itself against the control layer.
// *raft.RaftNode satisfies it; the storage layer never proposes through it.
type MetadataLister interface {
	ListKnowledgeBases(ctx context.Context) ([]types.KnowledgeBaseMeta, error)
	ListVersions(ctx context.Context, kbID string) ([]types.VersionMeta, error)
	// ListVersionsInRange is the same read narrowed to (fromExclusive, toInclusive]
	// (nil = no bound on that side). The plane's readers ask about ranges — a backfill
	// about one gap — and the range is what keeps a whole-chain answer off the wire.
	ListVersionsInRange(ctx context.Context, kbID string, fromExclusive, toInclusive *int64) ([]types.VersionMeta, error)
	// LastVersionID is the extreme-value read: the highest version id still in
	// kbID's metadata (0 when it has none). ChainTail needs exactly this, once per
	// knowledge base on every cursor report, so it must not be a whole-chain read on
	// the shape that serves it most (see RaftNode.LastVersionID).
	LastVersionID(ctx context.Context, kbID string) (int64, error)
}

// EnforceRetention applies the disk retention policy once at startup: for
// every knowledge base, drop on-disk index files older than the newest
// IndexRetentionCount versions, shielding the active version. A no-op when
// retention is unconfigured (the manager decides that). The same policy also
// runs after every successful build; this is the startup pass, and it must
// precede ReconcileIndexes so the reconcile sees post-retention disk facts.
//
// ONLY the index layer is bounded here, and that is a contract rather than an
// omission: a version is kept forever. `RollbackVersion` accepts any surviving
// version, so "nobody will read this one again" is not a fact the control layer can
// ever establish — retiring an old version would turn a legal rollback into an empty
// knowledge base. What IS rebuildable is the index: drop the artifact and the next
// query rebuilds it (`ErrIndexNotReady`, then `RebuildIndex`), which is exactly why
// that is the layer with a window. A version's doc-id list and its documents have no
// such window, so a node's footprint for a knowledge base grows with its version
// count. That is the price of "you may roll back to any version", and it is the
// reason the maintenance paths that enumerate a knowledge base's versions
// (DeletedVersionLeftovers, reconcile) are written to cost what the node HOLDS
// rather than what the knowledge base ever had.
func (d *LocalDataPlane) EnforceRetention(ctx context.Context, meta MetadataLister) error {
	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		return fmt.Errorf("plane: EnforceRetention: list knowledge bases: %w", err)
	}
	for _, kb := range kbs {
		if err := d.indexMgr.EnforceDiskRetention(ctx, kb.KBID, []int64{kb.ActiveVersionID}); err != nil {
			d.logger.Warn("plane: retention: EnforceDiskRetention failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
		}
	}
	return nil
}

// takeoverTimeout is how long a replica waits for the coordinator's "quorum
// reached" signal before checking for itself (Stratum_设计文档v13.md §7.3).
// Short by design: replicas sit on the same LAN as the coordinator, and the
// cost of waiting too long is a version stranded in PENDING. Like the other
// §10.4 numbers it is a placeholder.
//
// A var so tests can shorten it.
var takeoverTimeout = 200 * time.Millisecond

// takeoverCheckTimeout bounds the whole quorum check attemptTakeover runs once
// the timer fires.
//
// It is deliberately NOT derived from takeoverTimeout. That one is a test-tunable
// knob, and this bound is read from the timer's own goroutine — deriving one from
// the other would make a test that shortens the knob race with a previous test's
// still-running check on the same variable (measured with -race). It is also the
// honest number: the check is a handful of small RPCs, not something whose budget
// should shrink to tens of milliseconds because a test wanted a prompt timer.
const takeoverCheckTimeout = 2 * time.Second

// takeoverWatch is one pending §7.3 timer.
type takeoverWatch struct {
	timer *time.Timer
}

// WatchVersionWrite starts the §7.3 takeover timer for a version this node has
// just received via fan-out.
//
// This is the replica's half of coordinator-crash recovery: the data is already
// here, so the only thing that can go missing is the announcement that a quorum
// holds it. If that announcement never arrives, the replica checks for itself
// rather than letting the version rot in PENDING.
func (d *LocalDataPlane) WatchVersionWrite(kbID string, versionID int64) {
	if d.control == nil || d.resolveReplicas == nil || d.presence == nil || d.digest == nil {
		return // takeover not wired; the client-retry path still covers this
	}
	key := failureKey(kbID, versionID)
	d.takeoverMu.Lock()
	if old, ok := d.pendingTakeovers[key]; ok {
		old.timer.Stop()
	}
	d.pendingTakeovers[key] = &takeoverWatch{
		timer: time.AfterFunc(takeoverTimeout, func() { d.attemptTakeover(kbID, versionID) }),
	}
	d.takeoverMu.Unlock()
}

// ConfirmVersionWrite cancels the takeover timer: the coordinator reported that
// the version reached quorum, so there is nothing left for a replica to
// announce (Stratum_设计文档v13.md §7.3).
func (d *LocalDataPlane) ConfirmVersionWrite(kbID string, versionID int64) {
	key := failureKey(kbID, versionID)
	d.takeoverMu.Lock()
	if w, ok := d.pendingTakeovers[key]; ok {
		w.timer.Stop()
		delete(d.pendingTakeovers, key)
	}
	d.takeoverMu.Unlock()
}

// attemptTakeover is §7.3's "no coordinator signal arrived" path: count how
// many replicas hold the version and, if that is a quorum, announce it.
//
// It replaces only the *announcement*, never the transfer: the data is already
// on disk here and on whichever peers answer. Announcing is safe to repeat
// across replicas — the proposal is idempotent and the state machine drops
// reports for versions that are already settled (§10.6) — which is exactly why
// no replica has to be elected to do it.
func (d *LocalDataPlane) attemptTakeover(kbID string, versionID int64) {
	key := failureKey(kbID, versionID)
	d.takeoverMu.Lock()
	_, stillPending := d.pendingTakeovers[key]
	delete(d.pendingTakeovers, key)
	d.takeoverMu.Unlock()
	if !stillPending {
		return // confirmed in the meantime
	}

	// A fresh context: the request that delivered the data is long gone by
	// now, and the announcement is this node's own business.
	ctx, cancel := context.WithTimeout(context.Background(), takeoverCheckTimeout)
	defer cancel()

	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		d.logger.Warn("plane: takeover: resolve replicas",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}

	// This node received the push, so its own copy counts — the same "self is
	// one acknowledgement" rule fan-out uses.
	acks := 1
	for _, peer := range peers {
		holds, err := d.presence.HasVersion(ctx, peer, kbID, versionID)
		if err != nil || !holds {
			// Unreachable and not-holding both go uncounted: an unreachable
			// peer is not evidence either way, and the quorum test below is
			// what decides whether the remaining answers suffice.
			continue
		}
		acks++
	}
	quorum := QuorumSize(len(peers) + 1)
	if acks < quorum {
		// Not enough evidence to announce anything. Better to let the version
		// run out its retry budget and reach §10.1's terminal state than to
		// claim durability on a minority.
		d.logger.Info("plane: takeover: quorum not reached, leaving the version to its retry budget",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.Int("acks", acks), zap.Int("quorum", quorum))
		return
	}

	digest, err := d.digest.DigestOf(ctx, kbID, versionID)
	if err != nil {
		d.logger.Warn("plane: takeover: compute digest",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}
	if err := d.control.ReportDataDurable(ctx, kbID, versionID, digest); err != nil {
		d.logger.Warn("plane: takeover: report durable",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}
	d.logger.Info("plane: takeover: announced a version the coordinator never announced",
		zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Int("acks", acks))
}

// SafeDurableVersion reports the highest version this node may claim durable
// for kbID after a restart: the largest version a quorum's worth of cursors
// still reaches (Stratum_设计文档v13.md §7.8).
//
// The contiguous cursor lives in memory, so a restarted node starts at 0 and
// cannot tell how far behind it is. Asking the peers is the only way to find
// out, and requiring a quorum's worth of answers is what keeps the claim safe:
// the answer can never overstate progress, because a version only counts if
// enough replicas still report holding it.
//
// ok=false means "there is no replica set to ask" (single-node deployment, or
// nothing wired), in which case the caller falls back to its local view —
// there is nobody to disagree with. An error means a quorum could not be
// assembled: "no answer" must never be mistaken for "no data".
func (d *LocalDataPlane) SafeDurableVersion(ctx context.Context, kbID string) (int64, bool, error) {
	if d.cursorQuerier == nil || d.resolveReplicas == nil {
		return 0, false, nil
	}
	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("plane: SafeDurableVersion: resolve replicas: %w", err)
	}
	if len(peers) == 0 {
		return 0, false, nil
	}

	// This node always counts, even at cursor 0: its own (possibly lost)
	// progress is part of the picture, not an absence of one.
	reports := []int64{d.localVersionOf(kbID)}
	for _, peer := range peers {
		// A SHORT deadline per peer, not the caller's. This runs during STARTUP, and
		// the peers it asks may be inside this very call: every storage node reconciles
		// concurrently, and none of them reaches grpcServer.Serve until its own
		// reconcile has returned. Unbounded here, the cluster deadlocks at boot — each
		// node waits for the others while none of them is serving. (Measured: a storage
		// node holding historical data never logged "Stratum gRPC server listening",
		// and the control layer got "connection refused" from every storage address.)
		//
		// An unreachable peer is not evidence of anything and is skipped either way, so
		// the deadline only decides how long establishing "unreachable" takes — which
		// during startup is the difference between a node that boots and one that hangs.
		peerCtx, cancel := context.WithTimeout(ctx, peerCursorTimeout)
		cursor, err := d.cursorQuerier.LocalVersionOf(peerCtx, peer, kbID)
		cancel()
		if err != nil {
			// An unreachable peer is not evidence of anything. It is skipped,
			// and the quorum requirement below decides whether what is left
			// suffices.
			continue
		}
		reports = append(reports, cursor)
	}

	quorum := QuorumSize(len(peers) + 1)
	if len(reports) < quorum {
		return 0, false, fmt.Errorf("plane: SafeDurableVersion: %s: only %d of a required %d cursors arrived",
			kbID, len(reports), quorum)
	}
	// The largest version a quorum's worth of reports still reaches: sorted
	// descending, that is the quorum-th report.
	sort.Slice(reports, func(i, j int) bool { return reports[i] > reports[j] })
	return reports[quorum-1], true, nil
}

// emptyDocIDSetHash is the digest of a version whose document set is empty — the
// version every knowledge base is created with, and any version created with no
// document changes. It is a value, not an absence, and it is defined once in
// internal/types because BOTH layers need it: the control layer records it when it
// creates such a version, and this one recognises it.
var emptyDocIDSetHash = types.EmptyDocIDSetHash

// RecoverLocalCursors rebuilds this node's contiguous data cursor for every
// knowledge base from LOCAL facts, once at startup.
//
// Why it is needed: the cursor lives in memory (§7.8), so a restarted node
// starts at 0 for every knowledge base while its disk still holds the data. The
// station's freshness check (§9.3(2)) reads that cursor, so this replica — data
// complete, index present — is refused with "local history reaches version 0,
// below the required N" until some later write happens to advance it. Measured
// on the 3+3 cluster: after restarting one storage node, its sixth query failed
// with exactly that error (TestT4_QueryLatency).
//
// The evidence is local, not the peers': what a node holds is a question about
// its own disk. §7.8's quorum bound answers a different one ("what may I claim
// durable?"), and it cannot answer this — a restarted node's own report is 0,
// so the quorum minimum of {0, N, N} is 0 and the cursor would stay where it
// was.
//
// Three facts say "this version is here": an artifact on this node (built here
// or received via §8.4); a committed empty document set, because the empty set
// IS that version's content; and a READY version whose digest was never
// committed, which is how a version created with no changes looks. The third is
// what keeps an EMPTY knowledge base servable after a restart — it has no
// artifact anywhere, so the first two never fire, and the cursor would stay at 0
// for good. holdsVersionLocally owns the order they are consulted in.
//
// Only a CONTIGUOUS prefix of the version chain is recovered: a node holding v3
// and v5 but not v4 must not claim 5, because the cursor is what "my history is
// unbroken to here" means.
func (d *LocalDataPlane) RecoverLocalCursors(ctx context.Context, meta MetadataLister) error {
	if meta == nil {
		return nil
	}
	// The persisted record comes FIRST (docs/cursor-persistence-plan.md §3.5).
	// It is a direct statement of the local fact, so a knowledge base that has
	// one needs no inference — and the inference below is exactly what the record
	// exists to replace: it reads an index artifact off this node's disk, and an
	// artifact is a CACHE the retention policy may have deleted. Judging "what I
	// hold" from a cache is what let a node holding v1..v10 report a cursor of 0
	// after a restart.
	persisted, err := d.persistedCursors(ctx)
	if err != nil {
		d.logger.Warn("plane: cursor recovery: reading the persisted cursors failed; inferring instead",
			zap.Error(err))
	}

	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		return fmt.Errorf("plane: RecoverLocalCursors: list knowledge bases: %w", err)
	}

	for _, kb := range kbs {
		if versionID, ok := persisted[kb.KBID]; ok {
			d.installPersistedCursor(kb.KBID, versionID)
			// Logged even when it changed nothing, and worded so it cannot be
			// mistaken for the inference below: "read the record back" and
			// "reconstructed from what the disk still held" are different facts,
			// and an operator looking at a low cursor needs to know which one.
			d.logger.Info("plane: cursor recovery: read the persisted data cursor back",
				zap.String("kb_id", kb.KBID), zap.Int64("version_id", versionID))
			continue
		}

		versions, err := meta.ListVersions(ctx, kb.KBID)
		if err != nil {
			d.logger.Warn("plane: cursor recovery: ListVersions failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
			continue
		}
		sort.Slice(versions, func(i, j int) bool { return versions[i].VersionID < versions[j].VersionID })

		var recovered int64
		for _, v := range versions {
			reason, holds := d.holdsVersionLocally(ctx, kb.KBID, v)
			if !holds {
				d.logger.Info("plane: cursor recovery: stopped at a version this node does not hold",
					zap.String("kb_id", kb.KBID),
					zap.Int64("version_id", v.VersionID),
					zap.String("reason", reason),
					zap.String("doc_id_set_hash", v.DocIDSetHash),
					zap.String("index_status", v.IndexStatus.String()),
					zap.Int64("recovered_to", recovered))
				break
			}
			recovered = v.VersionID
		}
		if recovered <= d.localVersionOf(kb.KBID) {
			continue
		}
		d.advanceLocalVersion(kb.KBID, recovered)
		d.logger.Info("plane: cursor recovery: recovered the local data cursor from disk",
			zap.String("kb_id", kb.KBID),
			zap.Int64("version_id", recovered),
			zap.Int("versions_considered", len(versions)))
	}
	return nil
}

// holdsVersionLocally reports whether this node holds the version's data —
// judged only by facts about this node and the version's own metadata — and
// says why, so the startup log shows an operator which version broke the chain.
//
// Deprecated: this is the FALLBACK, reached only for a knowledge base that has
// no persisted cursor record — an older WAL, or one this node has not advanced
// since the record type existed (docs/cursor-persistence-plan.md §3.5). Every
// knowledge base with a record is answered by that record instead, which is the
// only evidence here that is not a cache: both branches below bottom out in an
// index artifact, which the retention policy is free to delete.
//
// The order matters, and the artifact is consulted first: of the three facts it
// is the only one backed by a completed build, so no inference is involved. The
// two document-set facts follow, because a version with no document set has no
// artifact either — the index manager answers "version has no chunks; nothing to
// build" for it — and a disk-only order would report a gap where there is none
// and stop the recovery at the very first version of the knowledge base.
//
// An absent artifact is NOT evidence of absence for a version that HAS a
// document set: §8.6(b) builds lazily, so such a version may legitimately have
// no local artifact while its records sit in the stores. That case is reported
// as "not held" — conservative on purpose, since claiming a version whose
// records did not land would make this node serve an incomplete result instead
// of refusing. The cost is a cursor that stays low (and a replica the station
// keeps off) rather than a wrong answer.
func (d *LocalDataPlane) holdsVersionLocally(ctx context.Context, kbID string, v types.VersionMeta) (string, bool) {
	if d.hasLocalArtifact(ctx, kbID, v.VersionID) {
		return "local artifact", true
	}
	if v.DocIDSetHash == emptyDocIDSetHash {
		// The empty set IS this version's content: there is nothing on disk to look for
		// and nothing to fetch from anyone. This is what keeps an empty knowledge base
		// servable, and it is the ONLY thing that may claim a version this node holds no
		// artifact for.
		//
		// It used to be a second branch as well — "durable DATA with no committed
		// digest" — and that branch is gone because it could not tell two states apart.
		// A version carrying real documents whose digest was never committed has
		// exactly that pair, since a cursor promotion (§7.9) settles the data side
		// without a digest. Reading it as "I hold this" is what let a replica that had
		// missed ONE version report the whole chain as its cursor and then skip the fetch
		// it needed (measured: cursor recovery returned v6 for a node holding only v5, so
		// EnsureIndex concluded there was nothing to do — and the node stayed without
		// the data while claiming to have it).
		//
		// The control layer now records the empty set's digest when it creates a version
		// with no document changes, so "no documents" travels as a value instead of
		// being inferred — see types.EmptyDocIDSetHash and the doc_id_set_hash field.
		return "empty document set", true
	}
	return "no local artifact", false
}

// hasLocalArtifact reports whether this node holds the version's artifact on
// disk. A missing index store means "no".
//
// Deprecated: only the holdsVersionLocally fallback calls this. An artifact is a
// cache, not a source of truth about what this node holds — the persisted cursor
// record is (docs/cursor-persistence-plan.md §4.1).
func (d *LocalDataPlane) hasLocalArtifact(ctx context.Context, kbID string, versionID int64) bool {
	if d.indexMgr == nil {
		return false
	}
	exists, err := d.indexMgr.IndexExists(ctx, kbID, versionID)
	if err != nil {
		d.logger.Warn("plane: cursor recovery: IndexExists failed",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return false
	}
	return exists
}

// ReconcileIndexes is the storage layer's half of the startup reconcile — the
// "block report" of control-data-separation-design.md §5.3. It walks the
// versions the control layer knows about, compares them against the indexes
// actually present on this node's disk, (re)schedules builds for the ones
// missing inside the retention window, and returns the versions whose index is
// durable. The caller hands that set to ControlPlane.ReportEpoch, which is
// what promotes a lost READY proposal.
//
// Storage-layer policy (the retention window, rebuild-on-demand) stays in
// here; every control-layer state change stays on the other side of the
// contract.
func (d *LocalDataPlane) ReconcileIndexes(ctx context.Context, meta MetadataLister, retentionCount int) ([]VersionRef, error) {
	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		return nil, fmt.Errorf("plane: ReconcileIndexes: list knowledge bases: %w", err)
	}

	var durable []VersionRef
	for _, kb := range kbs {
		versions, err := meta.ListVersions(ctx, kb.KBID)
		if err != nil {
			d.logger.Warn("plane: reconcile: ListVersions failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
			continue
		}

		// Which versions actually have an artifact on disk? The retention window is
		// defined over on-disk artifacts — EnforceDiskRetention counts files —
		// while the control layer's version set also holds plenty that never
		// produced one here (PENDING, FAILED, versions built elsewhere). Deriving
		// the window from the version set instead would call versions
		// "retention-dropped" that the policy never even saw.
		present := make(map[int64]bool, len(versions))
		unknown := make(map[int64]bool)
		var onDisk []int64
		for _, v := range versions {
			// FAILED is retryable — RebuildIndex may trigger another attempt — but
			// FAILED_PERMANENT is the control layer's terminal verdict (§10.1):
			// nothing re-triggers it, and its data may already have been reclaimed
			// (§10.6). Rebuilding here would contradict the recorded verdict and,
			// with the data gone, fail on every sweep.
			//
			// The DATA side's verdict counts too: data that will never arrive
			// cannot produce an index, so scheduling a build for it would fail on
			// every sweep for the same reason (§10.1b).
			if v.IndexStatus.IsFailed() ||
				v.DataStatus == types.DataStatusFailedPermanent {
				continue
			}
			exists, err := d.indexMgr.IndexExists(ctx, kb.KBID, v.VersionID)
			if err != nil {
				d.logger.Warn("plane: reconcile: IndexExists failed",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID), zap.Error(err))
				unknown[v.VersionID] = true
				continue
			}
			if exists {
				present[v.VersionID] = true
				onDisk = append(onDisk, v.VersionID)
			}
		}

		// retentionCutoff is the smallest version ID the on-disk retention policy
		// would keep. Below it, an absent artifact was dropped by the policy rather
		// than lost — the very set EnforceDiskRetention works with.
		retentionCutoff := retentionCutoffOf(onDisk, retentionCount)

		for _, v := range versions {
			if v.IndexStatus.IsFailed() ||
				v.DataStatus == types.DataStatusFailedPermanent {
				continue
			}
			if unknown[v.VersionID] {
				// IndexExists failed for this version; anything said about it
				// would be a guess.
				continue
			}
			switch {
			case present[v.VersionID]:
				// The index is on disk: the version is durable, and the
				// control layer promotes a lost READY proposal from this
				// report.
				durable = append(durable, VersionRef{KBID: kb.KBID, VersionID: v.VersionID})
			case retentionCount > 0 && v.VersionID < retentionCutoff:
				// Intentionally dropped by the disk retention policy
				// (PENDING or READY, outside the newest retentionCount
				// versions): leave it absent and rebuild on demand.
				d.logger.Info("plane: reconcile: skipping rebuild of retention-dropped index",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID))
			case v.IndexStatus == types.IndexStatusPending || v.VersionID == kb.ActiveVersionID:
				// Worth a head start, and only these two:
				//
				//   PENDING — the writer is waiting for this very build; without it
				//   the write stalls instead of merely being slower.
				//
				//   the active version — every query for this knowledge base lands on
				//   it, so its build is needed immediately.
				//
				// TriggerBuild is idempotent, and the build re-persists and
				// re-reports the status.
				d.logger.Info("plane: reconcile: (re)building missing index",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID),
					zap.String("status", v.IndexStatus.String()))
				if err := d.indexMgr.TriggerBuildBackfill(ctx, kb.KBID, v.VersionID); err != nil {
					d.logger.Warn("plane: reconcile: TriggerBuildBackfill failed",
						zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID), zap.Error(err))
				}
			default:
				// READY, artifact missing, inside the window, and NOT the active
				// version: leave it absent.
				//
				// The retention branch above gives the same answer for the same
				// reason — EnsureIndex builds on demand, so an absent artifact costs a
				// slow first query and nothing else. Rebuilding it here instead is how
				// a restart over a populated volume turns into a rebuild storm: one
				// eager build per historical version, every one of them competing with
				// the writes that actually need the CPU, disk and vecstore. Measured: a
				// fresh write waited 601s for READY while 41 historical artifacts were
				// rebuilt ahead of it.
				d.logger.Info("plane: reconcile: leaving a non-active missing index absent",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID),
					zap.String("status", v.IndexStatus.String()))
			}
		}
	}
	return durable, nil
}

// retentionCutoffOf returns the oldest version ID the on-disk retention policy
// would keep, given the version IDs whose artifacts are actually on disk: the
// smallest of the newest retentionCount. It returns -1 when the policy is off or
// when nothing would be dropped, which reads as "no absence can be explained by
// retention".
//
// It counts files, not versions, deliberately: EnforceDiskRetention works on the
// `.index` files it finds, so a version that never produced one here is not part
// of its window either.
func retentionCutoffOf(onDisk []int64, retentionCount int) int64 {
	if retentionCount <= 0 || len(onDisk) <= retentionCount {
		return -1
	}
	ids := append([]int64(nil), onDisk...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids[len(ids)-retentionCount]
}
