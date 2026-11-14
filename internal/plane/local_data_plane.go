package plane

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"stratum/internal/nodestate"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// VersionPuller is the storage layer's data-transfer primitive: it copies one
// version's storage-layer records (documents, chunk↔doc mapping, version doc
// list, chunks) onto this node. *stratumsync.Follower implements it; keeping
// it as an interface here lets the DataPlane be exercised without a live peer.
type VersionPuller interface {
	// PullVersionData fetches the version's data WITHOUT building its index.
	//
	// There is deliberately no "pull and build" flavour. §8.6(b) builds a version
	// when something asks for it, and a data transfer is not an ask: a pull runs
	// on every replica a version's data reaches, so building on arrival meant
	// every replica built every version. That flavour existed as PullVersion and
	// was removed (internal/sync/follower.go carries the longer note).
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

// IndexFetcher pulls one version's built index FROM a peer that holds it
// (DataSyncService.FetchIndexData). *sync.IndexFetcher implements it.
//
// This is §8.4's "建一次、分发 N 份" in its only direction: the push half was
// removed, because a fire-and-forget push cannot tell a replica that its artifact
// was meant for it and never arrived, while a fetch cannot miss. A peer that does
// not hold the artifact answers with the caller-recognisable "not exported"
// error, so the caller moves on rather than retrying — and if no peer has it, the
// caller builds it (the documented fallback, see EnsureIndex).
type IndexFetcher interface {
	FetchIndex(ctx context.Context, sourceAddr, kbID string, versionID int64) (indexData, sidecarData []byte, err error)
}

// IndexInstaller writes an artifact that CAME FROM a peer onto this node's disk.
// *index.IndexManagerImpl implements it; installation is the other half of a
// fetch, since bytes that arrive and are not installed leave the version exactly
// as cold as before.
type IndexInstaller interface {
	InstallIndex(ctx context.Context, kbID string, versionID int64, indexData, sidecarData []byte) error
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

// VersionDataStatusQuerier answers whether a version's DATA side is still PENDING.
//
// It exists so the §7.3 takeover can tell "nobody has announced this version"
// from "it was announced long ago". The difference matters: announcing again is
// harmless (the state machine accepts a repeated digest for a DURABLE version
// and drops one for a retired version), but doing it on every query would turn
// a no-op into a Raft proposal plus three presence RPCs.
//
// ok=false means "this node cannot answer" — an unwired plane, or metadata that
// no longer has the version. The caller then reports anyway, which is the safe
// direction: forgetting an announcement strands the version in PENDING.
type VersionDataStatusQuerier interface {
	DataStatusOf(ctx context.Context, kbID string, versionID int64) (types.DataStatus, bool)
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

	// reclaimDiag is what the last WAL reclaim pass found blocking each knowledge base.
	reclaimDiag reclaimDiagnosis
	dropper     VersionDataDropper
	cleaner     VersionDataCleaner
	// tombstones is this node's record of deletions it was told about (see
	// SetVersionTombstones). Read on every landing path, drained by
	// StartDeletedVersionRetry, and the home of this node's watermark into the
	// control layer's deletion log.
	tombstones VersionTombstones
	// kbDropper reclaims a whole knowledge base locally. Needed by the retry loop
	// because a KB deletion is ONE log entry (version 0), not one per version.
	kbDropper KBStorageDropper
	// deletions is the control layer's bounded deletion log, read incrementally. Nil
	// means no log is reachable and ReconcileDeletions always takes the live-chain path.
	deletions       DeletionLogSource
	deletionLogPage int
	// chainReconcileEnabled gates the periodic FULL pass; see the config field.
	chainReconcileEnabled bool
	// lastChainReconcile is when the last FULL live-chain pass ran, and
	// chainReconcileMaxAge bounds how long the incremental path may keep it away.
	// In-memory: a restart forces a full pass, which is the safe direction.
	lastChainReconcile    time.Time
	chainReconcileMaxAge  time.Duration
	chainReconcileMinAge  time.Duration
	deletionPagesPerSweep int
	presence              VersionPresenceQuerier
	digest                VersionDigest
	// dataStatus reads this node's local metadata to tell a still-PENDING
	// version from one already announced (§7.3 takeover). Optional.
	dataStatus     VersionDataStatusQuerier
	confirmer      WriteConfirmer
	indexFetcher   IndexFetcher
	indexInstaller IndexInstaller
	// pullIdleTimeout / pullMaxDuration bound the EnsureIndex pull loop by
	// PROGRESS and by an absolute ceiling respectively (see the loop's comment).
	// Zero means the Default* constants; the config carries them so a test can
	// exercise the loop's bounds without waiting out 30 s.
	pullIdleTimeout time.Duration
	pullMaxDuration time.Duration

	// The peer-facing timing knobs. Every one follows the same convention: a
	// positive configured value wins, zero falls back to the package-level
	// default — a var where a test shortens it, a const otherwise. The defaults
	// are read ONCE, in NewLocalDataPlane, so a test must shrink its var before
	// building the plane (every existing test does).
	cursorWriteTimeout    time.Duration
	cursorRetryDelay      time.Duration
	confirmAttemptTimeout time.Duration
	confirmRetryBackoff   time.Duration
	confirmBudget         time.Duration
	replicaPushTimeout    time.Duration
	pushTimeoutPerDoc     time.Duration
	peerCursorTimeout     time.Duration
	takeoverTimeout       time.Duration
	takeoverCheckTimeout  time.Duration
	takeoverMaxAttempts   int
	takeoverMaxRetryDelay time.Duration
	terminalReclaimEvery  time.Duration
	terminalReclaimTries  int

	// selfDataSyncAddr is what this node announces as the holder of the
	// versions it writes (§8.5). See LocalDataPlaneConfig.SelfDataSyncAddr.
	selfDataSyncAddr string

	// limiter queues writes per knowledge base so a stalled version cannot let
	// an unbounded backlog form behind it (Stratum_设计文档v13.md §7.7).
	limiter *writeLimiter

	logger *zap.Logger

	// versionMu guards deletedReconcileDirty and nothing else. The cursor and
	// the two accounts it advances over used to live behind it; they are the
	// state machine's now (docs/storage-node-state-machine-plan.md §7, item 3),
	// which is what makes one component answerable for the whole data layer's
	// local position.
	versionMu sync.RWMutex

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
	// takeoverGen hands out watch identities (guarded by takeoverMu). See
	// armTakeover for why the timer's identity is a value and not a pointer.
	takeoverGen uint64

	// terminalReclaim holds the apply-driven local reclaims that did not finish: one
	// task per version, retried on its own cadence (terminalReclaimInterval). See
	// NoteTerminalVersion for why it is not §10.6's broadcast queue — that queue is
	// gone, because every node learns a terminal verdict from its own apply instead.
	terminalReclaimMu sync.Mutex
	terminalReclaim   map[string]*cleanupTask

	// state is the node state machine: the version layer's entries, the data
	// cursor, and the two accounts the cursor advances over
	// (docs/storage-node-state-machine-plan.md §7). Never nil — see
	// NewLocalDataPlane for why an unwired plane still gets one.
	state *nodestate.Machine

	// indexFetchMu guards indexFetchInflight: the artifact fetches currently
	// running, one per (kb, version).
	//
	// The deduplication is not an optimisation (H9 of
	// docs/code-review-2026-10-18.md). Two callers ask about the same version as a
	// matter of course — the query path on every PENDING query, the catch-up loop on
	// every round — so N concurrent asks used to mean N downloads of the same
	// artifact, and InstallIndex's per-version shard lock then serialised them into N
	// disk rewrites of the same file.
	indexFetchMu       sync.Mutex
	indexFetchInflight map[fetchInflightKey]*indexFetchCall
}

// fetchInflightKey identifies one artifact fetch.
type fetchInflightKey struct {
	kbID      string
	versionID int64
}

// indexFetchCall is one in-flight fetch, shared by every caller that asked while it
// ran: they wait on done and read the same answer.
type indexFetchCall struct {
	done      chan struct{}
	installed bool
	err       error
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
	// KBDropper reclaims a whole knowledge base's physical data on this node. Used by
	// the deletion retry loop for KB-level entries (version 0).
	KBDropper KBStorageDropper
	// Deletions is the control layer's bounded deletion log, read incrementally.
	// Optional: without it every deletion sweep takes the live-chain path.
	Deletions DeletionLogSource
	// DeletionLogPage caps one incremental read (0 = DefaultDeletionLogPage).
	DeletionLogPage int
	// ChainReconcileMaxAge bounds how long the incremental path may keep the full
	// live-chain pass away (0 = 24h).
	ChainReconcileMaxAge time.Duration
	// ChainReconcileMinAge is the floor between two FULL passes triggered by local
	// activity, so a busy node does not turn "something landed here" into a range
	// scan every tick (0 = 5min).
	ChainReconcileMinAge time.Duration
	// DeletionPagesPerSweep caps one pass's pagination; a bulk deletion larger than
	// this is finished by the following ticks (0 = 8 pages).
	DeletionPagesPerSweep int
	// DisableChainReconcile switches off the ACTIVITY-DRIVEN full pass: the
	// reconciliation that finds leftovers nobody told this node about, on a local landing
	// or once ChainReconcileMaxAge has gone by. The start-up sweep and this node's own
	// tombstone retries still run, and so does the rollback the incremental path needs
	// when the log cannot serve this node (no watermark, truncated, epoch mismatch) —
	// that is correctness, not cadence, and honouring the switch there would leave such a
	// node permanently unable to converge.
	DisableChainReconcile bool
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
	//
	// DataStatus is the optional third piece: it answers from the local
	// metadata replica whether the version's data side is still PENDING, so a
	// takeover on an already-announced version costs nothing. Without it the
	// takeover reports whenever a quorum holds the version — correct, but it
	// pays a Raft proposal and three presence RPCs to say something nobody
	// needs (the state machine accepts a repeated digest for a DURABLE version
	// and drops one for a retired version).
	Presence   VersionPresenceQuerier
	Digest     VersionDigest
	DataStatus VersionDataStatusQuerier
	Confirmer  WriteConfirmer

	// IndexFetcher carries a built index to the other replicas (v13 §8.4).
	// Optional: without it a build stays local, and every replica builds for
	// itself exactly as before. The push direction reads the artifact through
	// sync's own NodeIndexReader, not through this config.
	IndexFetcher IndexFetcher
	// IndexInstaller lands an artifact fetched from a peer. Optional: without it
	// (or without IndexFetcher) a replica that does not build for itself has no
	// way to obtain the artifact, and FetchIndexFromPeers reports "not wired".
	IndexInstaller IndexInstaller

	// MaxInFlightWrites caps concurrent in-flight writes per knowledge base
	// (§7.7). Zero means DefaultMaxInFlightWrites. The cap is process-wide:
	// per-KB values used to arrive through the (now removed) durability-policy
	// declaration and are not wired yet.
	MaxInFlightWrites int

	// The peer-facing timing knobs below are all optional: zero takes the
	// package default (the same number as before they were configurable), so
	// an unconfigured node behaves exactly as it always did.

	// CursorWriteTimeout bounds ONE cursor record's write to the WAL.
	CursorWriteTimeout time.Duration
	// CursorRetryDelay is the pause before a failed cursor batch is retried.
	CursorRetryDelay time.Duration
	// ConfirmAttemptTimeout / ConfirmRetryBackoff / ConfirmBudget pace the §7.3
	// confirmation broadcast: one attempt, the pause between attempts, and the
	// whole broadcast.
	ConfirmAttemptTimeout time.Duration
	ConfirmRetryBackoff   time.Duration
	ConfirmBudget         time.Duration
	// ReplicaPushTimeout / PushTimeoutPerDoc bound one replica's push during
	// fan-out: a floor plus a per-document increment, because the peer's work
	// scales with the batch (v13 §7.2).
	ReplicaPushTimeout time.Duration
	PushTimeoutPerDoc  time.Duration
	// PeerCursorTimeout bounds one peer's cursor query on the startup /
	// reclaim paths.
	PeerCursorTimeout time.Duration
	// TakeoverTimeout is the §7.3 "did the coordinator announce this?" wait;
	// TakeoverCheckTimeout bounds one quorum check, TakeoverMaxAttempts caps
	// how many checks one watch spends and TakeoverMaxRetryDelay caps the
	// backoff between them.
	TakeoverTimeout       time.Duration
	TakeoverCheckTimeout  time.Duration
	TakeoverMaxAttempts   int
	TakeoverMaxRetryDelay time.Duration
	// TerminalReclaimInterval / TerminalReclaimAttempts pace the local reclaim
	// queue for versions the control layer retired.
	TerminalReclaimInterval time.Duration
	TerminalReclaimAttempts int

	// State is the node state machine this plane maintains
	// (docs/storage-node-state-machine-plan.md §7). Optional: a plane built
	// without one makes its own, which is what every test gets — and what a
	// single-node deployment would want anyway. The machine has no dependencies
	// of its own, so requiring the wiring would only add a failure mode.
	//
	// Note what is deliberately NOT here: reconciliation's projection of disk
	// facts. That is the startup path's job (main runs it once, before the node
	// serves), because it needs the reconciled artifact set — which this plane
	// does not produce on its own.
	State *nodestate.Machine

	// Logger receives progress/retry diagnostics. Optional.
	Logger *zap.Logger
}

// NewLocalDataPlane returns a DataPlane over the given storage-layer pieces.
func NewLocalDataPlane(cfg LocalDataPlaneConfig) *LocalDataPlane {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	// An unwired plane still gets a machine. The alternative — a nil one — would
	// put an "is it wired" branch at every call site for a component that has no
	// dependencies and no failure mode, and would mean the plane's data cursor
	// (the one thing this layer is authoritative about) had nowhere to live.
	state := cfg.State
	if state == nil {
		state = nodestate.New(nodestate.Options{Logger: logger.Named("nodestate")})
	}
	return &LocalDataPlane{
		indexMgr:              cfg.IndexManager,
		puller:                cfg.Puller,
		changesFetcher:        cfg.ChangesFetcher,
		localVersions:         cfg.LocalVersions,
		liveness:              cfg.Liveness,
		kbDropper:             cfg.KBDropper,
		deletions:             cfg.Deletions,
		deletionLogPage:       cfg.DeletionLogPage,
		chainReconcileEnabled: !cfg.DisableChainReconcile,
		chainReconcileMaxAge:  cfg.ChainReconcileMaxAge,
		chainReconcileMinAge:  durationOr(cfg.ChainReconcileMinAge, defaultChainReconcileMinAge),
		deletionPagesPerSweep: intOr(cfg.DeletionPagesPerSweep, maxDeletionPagesPerSweep),
		verify:                cfg.Verify,
		resolve:               cfg.Resolve,
		wal:                   cfg.WAL,
		cursorWAL:             cfg.CursorWAL,
		executor:              cfg.Executor,
		control:               cfg.Control,
		pusher:                cfg.Pusher,
		resolveReplicas:       cfg.ResolveReplicas,
		replicaCount:          cfg.ReplicaCount,
		cursorQuerier:         cfg.CursorQuerier,
		dropper:               cfg.DataDropper,
		cleaner:               cfg.CleanupBroadcaster,
		presence:              cfg.Presence,
		digest:                cfg.Digest,
		dataStatus:            cfg.DataStatus,
		confirmer:             cfg.Confirmer,
		indexFetcher:          cfg.IndexFetcher,
		indexInstaller:        cfg.IndexInstaller,
		selfDataSyncAddr:      cfg.SelfDataSyncAddr,
		pullIdleTimeout:       cfg.PullIdleTimeout,
		pullMaxDuration:       cfg.PullMaxDuration,
		cursorWriteTimeout:    durationOr(cfg.CursorWriteTimeout, cursorWriteTimeout),
		cursorRetryDelay:      durationOr(cfg.CursorRetryDelay, cursorRetryDelay),
		confirmAttemptTimeout: durationOr(cfg.ConfirmAttemptTimeout, confirmAttemptTimeout),
		confirmRetryBackoff:   durationOr(cfg.ConfirmRetryBackoff, confirmRetryBackoff),
		confirmBudget:         durationOr(cfg.ConfirmBudget, confirmBroadcastBudget),
		replicaPushTimeout:    durationOr(cfg.ReplicaPushTimeout, replicaPushTimeout),
		pushTimeoutPerDoc:     durationOr(cfg.PushTimeoutPerDoc, pushTimeoutPerDoc),
		peerCursorTimeout:     durationOr(cfg.PeerCursorTimeout, peerCursorTimeout),
		takeoverTimeout:       durationOr(cfg.TakeoverTimeout, takeoverTimeout),
		takeoverCheckTimeout:  durationOr(cfg.TakeoverCheckTimeout, takeoverCheckTimeout),
		takeoverMaxAttempts:   intOr(cfg.TakeoverMaxAttempts, maxTakeoverAttempts),
		takeoverMaxRetryDelay: durationOr(cfg.TakeoverMaxRetryDelay, maxTakeoverRetryDelay),
		terminalReclaimEvery:  durationOr(cfg.TerminalReclaimInterval, terminalReclaimInterval),
		terminalReclaimTries:  intOr(cfg.TerminalReclaimAttempts, terminalReclaimAttempts),
		limiter:               newWriteLimiter(cfg.MaxInFlightWrites),
		state:                 state,
		logger:                logger,
		pendingTakeovers:      make(map[string]*takeoverWatch),
		cursorPending:         make(map[string]int64),
	}
}

var _ DataPlane = (*LocalDataPlane)(nil)

// durationOr and intOr apply the "configured value wins, else the package
// default" rule the timing knobs share. Written once so a knob added later
// cannot invent a third convention (and so "unset" can never be read as "zero",
// which for a duration is a bound that fires immediately).
func durationOr(v, fallback time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return fallback
}

func intOr(v, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}

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

// effectivePullIdleTimeout is the no-progress bound with its default applied.
//
// It exists because "unset" and "zero" are the same value in the config, and a
// zero bound is not a small bound — it is one that fires immediately, which
// turned every on-demand fetch into an instant timeout. Both callers must go
// through this rather than reading the field.
func (d *LocalDataPlane) effectivePullIdleTimeout() time.Duration {
	if d.pullIdleTimeout > 0 {
		return d.pullIdleTimeout
	}
	return DefaultPullIdleTimeout
}

// effectivePullMaxDuration is the absolute ceiling with its default applied.
func (d *LocalDataPlane) effectivePullMaxDuration() time.Duration {
	if d.pullMaxDuration > 0 {
		return d.pullMaxDuration
	}
	return DefaultPullMaxDuration
}

// Search runs a vector query against versionID's index.
func (d *LocalDataPlane) Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) {
	return d.indexMgr.Search(ctx, kbID, versionID, vector, topK)
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
//   - no durable report (digest), and no §7.3 quorum check either: everything
//     replayed here has a successor, and CreateVersion refuses a parent that is
//     not durable, so these versions are settled already. Only the catch-up target
//     can still be PENDING, and that one is pulled (see EnsureIndex), which is where
//     the check is armed. Arming here would also send one metadata RPC per version
//     in a burst, on a gap that can be thousands of versions long.
//   - no index scheduling: historical versions build lazily or never (§8.6b), so
//     an unconditional build attempt spends work on something no query asks for.
//     If the version happens to be the active one, the very apply that triggered
//     this backfill schedules its build right after.
//   - no failure report: reportFailure is the *coordinator's* channel and can end
//     in §10.1's terminal verdict. A node that merely cannot fetch someone else's
//     history must not be able to declare that version permanently failed.
func (d *LocalDataPlane) ApplyBackfillChanges(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
	// A version this node knows was deleted is not replayed: the delta path can still
	// be served a deleted version's BEGIN record (the source's WAL keeps it until
	// every replica reports a cursor past it), and writing it here would resurrect
	// data whose metadata is gone. Skipping is not advancing — the cursor stays where
	// it is, and the caller's own deleted-version check already refuses such a gap.
	if d.skipIfDeleted(ctx, kbID, versionID, "backfill delta") {
		return nil
	}
	// writeLocalTransaction advances the cursor itself, so there is nothing else to
	// do here: the version counts as held once its data is in the stores.
	if _, err := d.writeLocalTransaction(ctx, kbID, versionID, parentVersionID, changes, nil); err != nil {
		return fmt.Errorf("plane: backfill apply %s v%d: %w", kbID, versionID, err)
	}
	return nil
}

// QuorumSize is the durable threshold for n replicas (Stratum_设计文档v13.md
// §7.1): ⌈(n+1)/2⌉, a bare majority, so any two quorums intersect.
func QuorumSize(n int) int {
	if n <= 0 {
		return 0
	}
	return (n+1)/2 + (n+1)%2
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
//
// It DOES own the durability barrier between the two: when it returns success,
// every storage write it made (chunk vectors included) is durable on stable
// storage, so the COMMIT the caller writes afterwards is telling the truth.
// The vecstore's writes are sync=false and the COMMIT is fsynced, so an
// executor that skipped the flush would let a crash keep the COMMIT and drop
// what it vouches for — and recovery never replays a version whose COMMIT is
// present.
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
	// knowledge base on every pull of the chain tails, so it must not be a whole-chain
	// read on the shape that serves it most (see RaftNode.LastVersionID).
	LastVersionID(ctx context.Context, kbID string) (int64, error)
}
