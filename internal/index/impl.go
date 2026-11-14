package index

import (
	"context"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	vecstorepb "stratum/api/proto/vecstore"
	"stratum/internal/types"
)

// IndexManagerConfig holds the configuration knobs for IndexManagerImpl,
// corresponding directly to the index_manager section of the node config.
type IndexManagerConfig struct {
	// LRUCapacity is the maximum number of indexes to keep in memory.
	LRUCapacity int

	// BruteForceMaxChunks is the version size up to which a query for an
	// unbuilt version is answered by scanning instead of building (§8.6b).
	// Zero means DefaultBruteForceMaxChunks.
	BruteForceMaxChunks int

	// LoadWaitTimeout bounds how long a Search call blocks waiting for a
	// concurrent load of the same version to finish.
	LoadWaitTimeout time.Duration

	// CallbackMaxRetries is how many times a BuildCompleteCallback is
	// retried (with exponential backoff) before giving up.
	CallbackMaxRetries int

	// CallbackRetryBaseMS is the base interval (ms) for exponential backoff
	// on callback retries.
	CallbackRetryBaseMS int

	// VecstoreAddr is the vecstore gRPC address for the VectorIndexService.
	// If empty, the implementation must set vectorIndexClient before use.
	VecstoreAddr string

	// IndexDataDir is the directory under which each version's persisted
	// index lives at dataDir/index/<kbID>/<versionID>.index. Shared with
	// the local vecstore process (same filesystem). Used to derive READY
	// status from disk facts and to restore indexes after a restart.
	IndexDataDir string

	// IndexRetentionCount bounds how many on-disk index files are kept
	// per knowledge base (Stratum_设计文档v10.md "磁盘保留策略": the most
	// recent N versions stay persisted; older ones are deleted and
	// rebuilt on demand via RebuildIndex). Applied after every successful
	// build and at startup. <= 0 disables the policy (keep everything).
	IndexRetentionCount int

	// RetentionProtectWindow shields recently-queried versions from the
	// retention policy above: a version queried here within this window is not
	// dropped even though it is older than the newest IndexRetentionCount. The
	// version someone is still reading — comparing a historical one, or a
	// client pinned to an older version — looks exactly like a dead one to a
	// number-only policy, and dropping it means rebuilding it on the next
	// query, for a version that was never cold.
	//
	// The evidence is the <versionID>.index.used sidecar, not the in-memory
	// access table: the pass that drops the most runs at startup, when memory
	// holds nothing. This protection is EXTRA retention, capped by
	// RetentionProtectMax, so at most IndexRetentionCount+RetentionProtectMax
	// index files are kept.
	//
	// Zero means DefaultRetentionProtectWindow; negative disables the
	// protection entirely, which is the historical "newest N only" behaviour.
	RetentionProtectWindow time.Duration

	// RetentionProtectMax caps how many versions RetentionProtectWindow may
	// shield at once, so a knowledge base whose versions are all read regularly
	// cannot grow the on-disk set without bound. <= 0 means IndexRetentionCount.
	RetentionProtectMax int

	// MemoryThresholdMB bounds the estimated in-memory footprint of all
	// loaded indexes (vector payload bytes, summed and tracked per loaded
	// index). When the estimate exceeds the threshold, new loads/builds
	// evict least-recently-used, ref-count-zero indexes first
	// (Stratum_设计文档v10.md "内存换入换出"). <= 0 disables the byte
	// threshold; LRUCapacity still applies.
	MemoryThresholdMB int64

	// CandidateN is the coarse-pass budget every search asks the vector store
	// to use (Stratum_设计文档v12.md §2.2). It is the one knob that trades
	// quantized recall for latency: the candidate set is what gets re-ranked
	// against full-precision vectors, so a wider set covers more of the true
	// neighbours and costs more disk reads.
	//
	// <= 0 leaves the field unset on the request, and the vector store applies
	// its own default — clamp(top_k × 8, 16, 4096). Setting it here overrides
	// that for every query on this node, which is coarser than deciding per
	// query but is what the config file can express today.
	CandidateN int

	// ColdThreshold is how long a version may go without a Search before it is
	// built in the graph-free form (§8.6a). The HNSW graph dominates both build
	// time and resident memory once vectors are quantized, and a version nobody
	// queries does not need it; the answers stay equivalent because the
	// graph-free variants keep the quantizer and its rerank semantics.
	// The policy is off when <= 0 (the default), which keeps the historical
	// "every version carries a full graph" behaviour.
	//
	// There is no sweep interval and no background evaluator: the shape is
	// decided INSIDE a build (coldShapeFor) and re-checked on the read path
	// (needsReshape). A timer would decide outside every request — building
	// artifacts nobody asked for, and unable to see §8.4's owner sets (an
	// artifact built outside the owner set is one no query ever reaches). Both
	// directions are covered: a version queried again within half the threshold
	// gets its graph back, and the gap between the two bars is hysteresis so a
	// version sitting on the boundary is not rebuilt every time.
	ColdThreshold time.Duration

	// BuildAbandonTimeout bounds how long a half-built index artifact may sit on
	// disk before the sweeper deletes it
	// (coordinator-selection-and-node-liveness-design.md §6, "索引构建失败产物的回收").
	//
	// The gap it closes: a build that dies or times out leaves an artifact that
	// no one will ever look at again — the control layer simply moves on to the
	// next candidate and that candidate succeeds, so the version never reaches
	// FAILED_PERMANENT and §1.4's cleanup broadcast never fires. Without this, the
	// remains sit there for good.
	//
	// <= 0 means DefaultBuildAbandonTimeout; negative disables the sweeper.
	BuildAbandonTimeout time.Duration

	// BuildAbandonSweepInterval is how often that sweeper looks (<= 0 means
	// DefaultBuildAbandonSweepInterval).
	BuildAbandonSweepInterval time.Duration

	// BuildRetryTimeout and BuildRetryInterval bound the in-build retry loop that
	// waits for a racing data sync to land (<= 0 means buildRetryTimeout /
	// buildRetryInterval). Raising the timeout is what lets a build win against a
	// slow pull instead of failing and being retried from the control layer.
	BuildRetryTimeout  time.Duration
	BuildRetryInterval time.Duration

	// AccessPersistInterval throttles how often a version's access time is
	// written to its sidecar (<= 0 means accessPersistInterval).
	AccessPersistInterval time.Duration

	// KeepaliveInterval / KeepaliveTimeout are the gRPC keepalive parameters for
	// the vecstore connection (<= 0 means 60s / 3s). They are deliberately only
	// for ACTIVE streams: pinging without one gets this client GoAway'd.
	KeepaliveInterval time.Duration
	KeepaliveTimeout  time.Duration

	// AppendMaxDeadRatio bounds how much dead weight a §8.6(c) pure-append
	// reuse may carry: the share of the base artifact's vectors that this
	// version no longer needs (documents deleted here, or by an ancestor and
	// carried along by an earlier reuse). HNSW cannot remove vectors, so those
	// dead ones stay resident and compete for candidate slots — the read path
	// filters their results out, so they cost memory and recall, not
	// correctness. Above the ratio the build falls back to a full rebuild,
	// which drops them. <= 0 means DefaultAppendMaxDeadRatio; 1.0 disables the
	// check.
	AppendMaxDeadRatio float64

	// MaxCodebookDriftRatio and MaxCodebookAppends are the two triggers that
	// retire a stale quantizer codebook (docs/codebook-refresh-plan.md §3):
	// either one fires and the build abandons append reuse, rebuilding from
	// scratch — which is the only path that trains a NEW codebook.
	//
	// 缺口：码本只在全量重建时训练一次，而"只 append、不删除"的库几乎不会全量
	// 重建，于是新向量一直用旧码本编码 —— 量化误差随分布漂移上升，粗筛的候选覆盖
	// 变差。排序仍由全精度 rerank 决定，所以查询不报错、不超时，只是"本该进候选的
	// 近邻在 stage-1 就漏掉了"：功能与集成测试天然测不到这个维度。
	//
	// 收益不只在"防漂移"：基准里即便 delta 与 base 同分布，重训码本仍换来
	// +8%~+16% 的候选覆盖 —— 因为新码本用上了增长后的语料（训练样本量）。语料越大
	// 这份收益越大。
	//
	// 只对需要训练的量化类型生效（SQ8 学 per-dim range、PQ 学 k-means centroids）；
	// SQ_FP16 / SQ_BF16 是纯位截断（免训练），OFF 根本没有码本 —— 见
	// quantizerNeedsTraining，那三类实测的漂移代价恰为 0，对它们重建是纯开销。
	//
	// MaxCodebookDriftRatio <= 0 取 DefaultMaxCodebookDriftRatio；>= 1.0 实际上
	// 不会触发（等价于关闭该判据）。MaxCodebookAppends <= 0 取
	// DefaultMaxCodebookAppends。
	MaxCodebookDriftRatio float64
	MaxCodebookAppends    int64
	// MinCodebookBaselineVectors is the baseline size below which the cumulative
	// ratio is ignored (only the append-count fallback applies). It exists
	// because the ratio is scale-free: without a floor, a tiny KB rebuilds on
	// nearly every version for no benefit. <= 0 takes
	// DefaultMinCodebookBaselineVectors; NEGATIVE removes the floor.
	MinCodebookBaselineVectors int64

	// GCRatioThreshold is the dead-vector share above which an ACTIVE version
	// becomes a §8.6(d) cleanup candidate. It is a separate knob from
	// AppendMaxDeadRatio on purpose: that one decides whether a BUILD may start
	// from an artifact, while this one decides whether a SEALED artifact is
	// worth reopening — a different trade, because reopening takes the version
	// out of service on this node for a moment. <= 0 means
	// DefaultGCRatioThreshold.
	GCRatioThreshold float64

	// GCSweepInterval is how often the §8.6(d) scanner re-estimates the dead
	// share of the active versions. Zero means DefaultGCSweepInterval;
	// negative disables the scanner.
	GCSweepInterval time.Duration

	// GCEnabled turns the §8.6(d) scanner from a REPORT into an ACTOR: the
	// candidates it finds are then actually collected (reopen the sealed
	// artifact, drop the dead vectors, reseal).
	//
	// ON since 2026-10 (it was opt-in before that). The collection does rewrite a
	// live artifact, but it is gated twice before it does: serving_replica_min
	// requires other replicas to be serving the version (so it stays answerable
	// while this node steps out), and gc_ratio_threshold requires enough dead
	// weight to make the rewrite worth it. Deployments that want the pre-2026-10
	// behaviour set `gc_enabled: false` in the config file.
	GCEnabled bool

	// IndexServingReplicaMin is how many OTHER replicas must be serving the
	// version's index before this node may take itself out of service to collect
	// (§8.6(d)). <= 0 means DefaultIndexServingReplicaMin.
	//
	// It is independent of the declared replication target on purpose (§8.2):
	// that one protects durability — the data must survive — while this one
	// protects READ SERVICE CAPACITY. A deployment can happily run 3 replicas for
	// durability and require only 2 to be serving before rotating one out for
	// maintenance.
	IndexServingReplicaMin int

	// NodeID is the node this manager runs on, used to exclude itself when
	// asking how many other replicas are serving (see replicaCounter). Zero means
	// unwired, and collection then refuses to run — the conservative direction:
	// without knowing who "I" am, this node cannot tell whether stepping out
	// would leave anyone behind.
	NodeID int64

	// GCGraphRebuildRatio is the dead-vector share at which a GRAPHED active
	// version is worth a full rebuild (§8.6(d)).
	//
	// Higher than GCRatioThreshold on purpose, because the two operations are not
	// comparable in cost: a graph-free artifact is reopened and edited, while a
	// graphed one has to be built from scratch — faiss cannot remove from HNSW, so
	// the entire graph is thrown away and rebuilt from the current document set.
	//
	// The threshold is also the only honest proxy available for §8.6(d)'s "how much
	// longer will this version be served?". A version whose artifact is mostly dead
	// weight has been edited (and served) for a while, and the dead share is
	// unlikely to shrink; a version that just got a heavy deletion is better left
	// alone, because the next version will replace it and start clean. <= 0 means
	// DefaultGCGraphRebuildRatio.
	GCGraphRebuildRatio float64

	// BuildConcurrency is how many index builds may run at once. <= 0 means
	// defaultBuildConcurrency() (the CPU count).
	//
	// It exists because "one goroutine per request" had no ceiling: after a restart
	// over a populated volume, every historical version's missing artifact used to be
	// rebuilt at once, and a live write waited 601s for READY behind them. The bound
	// caps the contention; the pool's interactive priority is what keeps a live write
	// from queueing behind the sweep at all.
	BuildConcurrency int

	// BuildState reports this manager's build transitions to the node state
	// machine (docs/storage-node-state-machine-plan.md §7, item 4). Optional:
	// nil means they are simply not reported, which is the unit-test default.
	//
	// It belongs here rather than at the call sites because the manager is the
	// single entry point every build passes through — including the one that
	// never starts.
	BuildState BuildStateRecorder
}

// DefaultIndexServingReplicaMin is the default for IndexServingReplicaMin (§8.6(d)):
// how many OTHER replicas must be serving a version before this node may step out of
// the pool to collect it.
//
// It is 1, not the replication factor minus one. The original 2 was written for a
// world where every replica holds every version's index; with §8.5's on-demand builds
// and owner sets that is no longer true — a replica nothing routes to never builds
// the artifact, so "2 others serving" actually demanded that ALL THREE replicas hold
// it, and collection could never start. Measured on the 3+3 cluster:
// `others_serving=1, minimum_required=2` (docs/stress-test-report.md §E). What the
// gate is FOR is "some other replica still answers after I leave", and 1 delivers
// exactly that; raise it for a deployment that wants more slack while rotating.
const DefaultIndexServingReplicaMin = 1

// DefaultGCGraphRebuildRatio is the placeholder for GCGraphRebuildRatio (§8.6(d)):
// half the artifact's vectors are dead before it is worth rebuilding a graph from
// scratch. Deliberately much higher than DefaultGCRatioThreshold — a rebuild costs
// the whole graph, so it should only be spent on a version that is clearly not
// about to be replaced.
const DefaultGCGraphRebuildRatio = 0.5

// DefaultBuildAbandonTimeout is how long a half-built artifact may sit before
// the sweeper reclaims it. The placeholder comes from §6.3: the known target is
// "a 100k-chunk index builds within 5 minutes", and 6x headroom keeps the
// sweeper away from a legitimately slow build (large batch, cold page cache).
const DefaultBuildAbandonTimeout = 30 * time.Minute

// DefaultBuildAbandonSweepInterval is how often the sweeper looks. It is
// deliberately coarse — the thing it reclaims has been dead for half an hour,
// so finding it a minute later costs nothing.
const DefaultBuildAbandonSweepInterval = 5 * time.Minute

// DefaultAppendMaxDeadRatio is the dead-vector share above which §8.6(c)'s
// pure-append reuse is abandoned for a full rebuild. At 20% a fifth of the
// resident vectors are pure overhead: they occupy memory and, worse, take
// candidate slots in every search (the read path discards their results
// afterwards, so they can only push real hits out of the top-K).
const DefaultAppendMaxDeadRatio = 0.2

// DefaultMaxCodebookDriftRatio is the cumulative-new-vector share at which a
// version stops appending to its parent's artifact and rebuilds from scratch to
// retrain the quantizer's codebook (docs/codebook-refresh-plan.md §3). The
// share is (totalChunks - trainedNtotal) / trainedNtotal.
//
// 0.25 is a starting point, not a measured optimum: large enough that a KB whose
// distribution is stable pays for a rebuild rarely, small enough that a
// fast-moving one does not drift far. vecstore/test/recall_drift_bench_test.cpp
// (§6) is the harness that would pin it down.
const DefaultMaxCodebookDriftRatio = 0.25

// DefaultMaxCodebookAppends is the fallback trigger: how many append-reuses may
// pass before a rebuild retrains the codebook, however few vectors each one
// added. It covers the KB that writes many small versions, where the cumulative
// share climbs too slowly to trip the ratio.
const DefaultMaxCodebookAppends = 50

// DefaultMinCodebookBaselineVectors is the baseline size below which the
// cumulative ratio is not consulted at all.
//
// The ratio is scale-free, so a 4-vector KB trips it by adding a single vector:
// a full rebuild whose benefit is nil (quantization error cannot matter across
// four vectors) and whose only observable effect is churn, plus a log line per
// rebuild claiming the codebook was refreshed. Below this floor only the
// append-count fallback applies.
const DefaultMinCodebookBaselineVectors = 1000

// IndexManagerImpl is the real IndexManager implementation, backed by the
// C++ vecstore's VectorIndexService gRPC. It manages per-version HNSW
// indexes with LRU eviction, reference-counted eviction protection, and
// asynchronous builds via the standard data flow:
//
//	VersionDocList.ListDocIDs -> ChunkDocMapper.ListChunkIDsByDocs ->
//	ChunkStore.Read (batched per chunk) -> VectorIndexService.Build.
//
// Build-callback retries: when a BuildCompleteCallback returns an error,
// it is retried with exponential backoff up to CallbackMaxRetries; once
// retries are exhausted the failure is logged and the version's status
// stays as-is (PENDING), to be resumed by an explicit rebuild or a node
// restart.
type IndexManagerImpl struct {
	cfg IndexManagerConfig

	// Build data-source callbacks, wired to real PebbleDB-backed modules
	// (VersionDocList, ChunkDocMapper, ChunkStore) by the caller.
	listDocIDs         func(ctx context.Context, kbID string, versionID int64) ([]string, error)
	listChunkIDsByDocs func(ctx context.Context, kbID string, docIDs []string) ([]string, error)
	readChunkVector    func(ctx context.Context, kbID, chunkID string) ([]float32, error)

	// kbMetaGetter, when set, supplies the KB metadata (from the Raft
	// state machine via cmd/stratum/main.go wiring) so async builds can
	// forward the KB-level quantizer config to the vecstore. Nil means the
	// default (full precision / OFF) is used for every build.
	kbMetaGetter func(ctx context.Context, kbID string) (types.KnowledgeBaseMeta, error)

	// versionParent, when set, returns versionID's parent version (0 when it
	// has none). §8.6(c)'s pure-append reuse needs it to find the artifact a
	// build can start from. Nil disables the reuse entirely, which is the
	// safe default: every build then proceeds from scratch.
	versionParent func(ctx context.Context, kbID string, versionID int64) (int64, error)

	// vecstore gRPC client for Build/Search/Save/LoadForAppend/Load/Reset.
	vectorIndexClient vecstorepb.VectorIndexServiceClient
	vecstoreConn      *grpc.ClientConn // owned; closed on shutdown

	mu      sync.Mutex
	cond    *sync.Cond
	loaded  map[indexKey]*loadedIndex
	loading map[indexKey]bool // builds or loads currently in progress

	// installShards serialise InstallIndex per version (see install.go). They
	// exist because one version can be shipped more than once, and two installs
	// interleaving their renames leaves an index file beside the wrong sidecar.
	installShards [installShardCount]sync.Mutex

	// readInflight merges concurrent ReadIndexFiles calls for the SAME version:
	// the first caller reads the pair off disk, the rest wait for it and share
	// the bytes (§5.8's 读盘合并). Guarded by readMu, which is held only long
	// enough to look up or publish the entry — never across the disk read.
	readInflight map[indexKey]*artifactRead
	readMu       sync.Mutex

	// readFile is os.ReadFile. Injectable so a test can count and gate the reads
	// (the same shape as listDocIDs / readChunkVector above).
	readFile func(string) ([]byte, error)

	// sizeByKey tracks each loaded index's estimated in-memory footprint
	// (vector payload bytes from the last build; 0 when unknown, e.g. a
	// pre-policy index loaded without a size sidecar). loadedBytes is
	// their sum, consulted by makeRoomLocked when MemoryThresholdMB is
	// set. Both are guarded by mu.
	sizeByKey   map[indexKey]int64
	loadedBytes int64

	// lastPinWarn rate-limits the "over capacity, nothing evictable" warning that
	// makeRoomLocked emits (M8 of docs/code-review-2026-09-24.md). The condition
	// lasts as long as a burst of concurrent searches pins every resident index,
	// and every one of them calls makeRoomLocked — one line per minute is enough
	// to make the overage visible without turning it into the loudest thing in the
	// log. Guarded by mu.
	lastPinWarn time.Time

	// lastSearch records, per version, when a Search request last asked
	// for it (§8.6a). It is deliberately NOT the same thing as
	// loadedIndex.lastAccess: that one is an LRU hint that is lost when
	// the index is evicted from memory, while "is this version cold?" is
	// a question about query traffic, so it must survive eviction and
	// cover versions that were never loaded at all (a small version
	// answered by brute force, or one whose index was dropped by the
	// on-disk retention policy). A successful build seeds the entry with
	// its own timestamp, so a version that is built but never queried
	// still ages into cold; a version with no entry at all has been
	// neither searched nor built by this process, so the cold policy does
	// not know it exists. Guarded by mu.
	lastSearch map[indexKey]time.Time

	// builtGraphFree records, per version, whether the index currently
	// built for it is the graph-free variant (§8.6a). The read path reads
	// it (needsReshape) to leave a version whose shape already matches its
	// traffic alone. Dropped with the version
	// or KB, like the access record. Guarded by mu.
	builtGraphFree map[indexKey]bool

	// startedAt is when this process constructed the manager. A version this
	// process has neither queried nor built has no access record at all, so it
	// is aged from here: after a restart every version is in that state, and
	// stamping them "now" both avoids reshaping a whole knowledge base at boot
	// and avoids the cold policy standing still forever. Guarded by mu.
	startedAt time.Time

	// lastPersist records, per version, when its access time was last written
	// to disk (the .index.used sidecar). It is only a throttle — the value that
	// matters is on disk. Guarded by mu.
	lastPersist map[indexKey]time.Time
	// abandonCancel/abandonWG govern the background sweeper that reclaims
	// half-built index artifacts (§6). abandonCancel is nil while it is off.
	abandonCancel context.CancelFunc
	abandonWG     sync.WaitGroup

	// activeVersions reports, per knowledge base, the version it is currently
	// serving. §8.6(d)'s scanner covers only those: a version WITH a successor
	// is reclaimed by the append path's dead-ratio rebuild, and a historical
	// version is queried too rarely to be worth the scan. Nil disables the
	// scanner entirely.
	activeVersions func(ctx context.Context) (map[string]int64, error)

	// chainTailVersions reports, per knowledge base, the version at the TAIL of the
	// replicated chain — the second source of §8.6(d) targets (see
	// SetChainTailVersionsProvider for why the active version alone leaves a hole).
	// Optional: unwired means "active versions only", the pre-existing behaviour.
	chainTailVersions func(ctx context.Context) (map[string]int64, error)

	// gcCancel/gcWG govern the §8.6(d) background scanner. gcCancel is nil
	// while it is off.
	gcCancel context.CancelFunc
	gcWG     sync.WaitGroup

	// buildPool bounds how many index builds run at once and decides which one runs
	// next (interactive before backfill). It replaces the previous
	// "one goroutine per request, no ceiling" behaviour, which let a post-restart
	// reconcile sweep of every historical version starve a fresh write of its build
	// (measured: 601s without READY).
	//
	// Started lazily on the first Submit, so a manager that never builds anything —
	// most unit tests — starts no workers and leaks none. buildPoolOnce guards it.
	buildPool *buildPool
	// buildPoolMu guards buildPool for Close. buildPoolOnce already orders the
	// creation for submitBuild (sync.Once is a happens-before edge), but Close may run
	// on a different goroutine at any point, so it needs a real lock to read the field.
	buildPoolMu   sync.Mutex
	buildPoolOnce sync.Once

	// replicaCounter answers "how many OTHER replicas are serving this version"
	// (§8.6(d) collection needs it before it may take this node out of service).
	// Nil, or a zero cfg.NodeID, disables collection: without both, this node
	// cannot tell whether stepping out would leave anyone behind. Guarded by mu.
	replicaCounter ReplicaCounter

	// maintenance holds the versions this node has taken out of service for
	// §8.6(d) collection. A search that asks for one gets
	// stratumerrors.ErrIndexMaintenance instead of a stale answer or a hang: the
	// station recognizes that sentinel and moves to another replica, which is the
	// whole point of the rolling scheme. Guarded by mu.
	maintenance map[indexKey]bool

	// gcBlocked holds the versions whose §8.6(d) collection is stuck behind the
	// service-capacity check: the artifact carries too much dead weight, and too
	// few other replicas are serving it for this node to step out. It exists to
	// be REPORTED (BlockedCollections → GetSystemStatus) rather than acted on —
	// the condition is a configuration problem, and §8.6(d) is explicit that it
	// must not be endured in silence. Guarded by mu.
	gcBlocked map[indexKey]*gcBlockedState

	// deletedKBs / deletedVersions are tombstones set by knowledge-base
	// deletion (DeleteFilesByKB) and version deletion (Discard). They
	// close the "resurrection" race where a Search-triggered Load RPC
	// started before the deletion but finished after it: loadFromDisk
	// checks the tombstones (under mu) and refuses to re-insert the
	// index. KB IDs are generated (UUIDs) and never reused, and deleted
	// versions are removed from the Raft state machine, so tombstones
	// never block a legitimate later load.
	deletedKBs      map[string]bool
	deletedVersions map[indexKey]bool

	callbacks []BuildCompleteCallback

	logger *zap.Logger
}

type loadedIndex struct {
	refCount   int
	lastAccess time.Time
}

var _ IndexManager = (*IndexManagerImpl)(nil)

// defaultKeepaliveInterval / defaultKeepaliveTimeout are the vecstore channel's
// keepalive defaults when the config leaves them unset (vecstore.keepalive_*).
// The same numbers chunkstore uses; kept here rather than imported so this
// package does not take a dependency for two literals.
const (
	defaultKeepaliveInterval = 60 * time.Second
	defaultKeepaliveTimeout  = 3 * time.Second
)

// NewIndexManager constructs an IndexManagerImpl. The caller must either
// set cfg.VecstoreAddr (and the constructor will dial it), or inject
// vectorIndexClient and the build data-source callbacks directly before
// use (used by tests).
func NewIndexManager(cfg IndexManagerConfig) *IndexManagerImpl {
	// Fill the timing defaults once, so every reader below can use the config
	// value directly (the same shape the other components use).
	if cfg.BuildAbandonSweepInterval <= 0 {
		cfg.BuildAbandonSweepInterval = DefaultBuildAbandonSweepInterval
	}
	if cfg.BuildRetryTimeout <= 0 {
		cfg.BuildRetryTimeout = buildRetryTimeout
	}
	if cfg.BuildRetryInterval <= 0 {
		cfg.BuildRetryInterval = buildRetryInterval
	}
	if cfg.AccessPersistInterval <= 0 {
		cfg.AccessPersistInterval = accessPersistInterval
	}
	if cfg.KeepaliveInterval <= 0 {
		cfg.KeepaliveInterval = defaultKeepaliveInterval
	}
	if cfg.KeepaliveTimeout <= 0 {
		cfg.KeepaliveTimeout = defaultKeepaliveTimeout
	}
	im := &IndexManagerImpl{
		cfg:             cfg,
		loaded:          make(map[indexKey]*loadedIndex),
		loading:         make(map[indexKey]bool),
		sizeByKey:       make(map[indexKey]int64),
		lastSearch:      make(map[indexKey]time.Time),
		builtGraphFree:  make(map[indexKey]bool),
		readInflight:    make(map[indexKey]*artifactRead),
		readFile:        os.ReadFile,
		lastPersist:     make(map[indexKey]time.Time),
		startedAt:       time.Now(),
		deletedKBs:      make(map[string]bool),
		deletedVersions: make(map[indexKey]bool),
		maintenance:     make(map[indexKey]bool),
		gcBlocked:       make(map[indexKey]*gcBlockedState),
		logger:          zap.NewNop(),
	}
	im.cond = sync.NewCond(&im.mu)

	if cfg.VecstoreAddr != "" {
		conn, err := grpc.NewClient(cfg.VecstoreAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				// 10s 无流 ping + PermitWithoutStream 会被 vecstore 的
				// C++ 服务端以 too_many_pings GoAway 踢掉连接（压测中
				// 导致 build RPC 挂起、版本永久 PENDING）。改为仅在
				// 活动流上以 60s 间隔探测：既保留死连接检测，又不再
				// 触发服务端 keepalive 强制策略。
				Time:                cfg.KeepaliveInterval,
				Timeout:             cfg.KeepaliveTimeout,
				PermitWithoutStream: false,
			}),
		)
		if err != nil {
			im.logger.Error("failed to dial vecstore for IndexManager", zap.Error(err))
		} else {
			im.vecstoreConn = conn
			im.vectorIndexClient = vecstorepb.NewVectorIndexServiceClient(conn)
		}
	}
	return im
}

// SetLogger binds a logger for lifecycle and error messages.
func (im *IndexManagerImpl) SetLogger(l *zap.Logger) {
	im.logger = l
}

// loggerOrNop returns the configured logger, or a no-op one.
//
// Same contract as buildLogger below, for the same reason: a manager built
// directly (tests do) has no logger, and a nil *zap.Logger panics instead of
// staying quiet.
func (im *IndexManagerImpl) loggerOrNop() *zap.Logger {
	if im.logger == nil {
		return zap.NewNop()
	}
	return im.logger
}

// buildLogger returns the configured logger, or a no-op one.
//
// The same accessor router.Router has, for the same reason: NewIndexManager
// installs a no-op logger, but a manager built directly (tests do) has none, and
// a nil *zap.Logger panics instead of staying quiet. The per-stage build timings
// are emitted on EVERY build, success or failure, so they must not be the thing
// that turns a logger-less manager into a crash.
func (im *IndexManagerImpl) buildLogger() *zap.Logger {
	return im.loggerOrNop()
}

// SetBuildDataSources wires the three data-source callbacks to real
// implementations (VersionDocList, ChunkDocMapper, ChunkStore). Callers
// must call this once before TriggerBuild is used.
func (im *IndexManagerImpl) SetBuildDataSources(
	listDocIDs func(ctx context.Context, kbID string, versionID int64) ([]string, error),
	listChunkIDsByDocs func(ctx context.Context, kbID string, docIDs []string) ([]string, error),
	readChunkVector func(ctx context.Context, kbID, chunkID string) ([]float32, error),
) {
	im.listDocIDs = listDocIDs
	im.listChunkIDsByDocs = listChunkIDsByDocs
	im.readChunkVector = readChunkVector
}

// Close releases the vecstore gRPC connection, if one was created. It
// also stops the background sweeps this node runs.
func (im *IndexManagerImpl) Close() error {
	im.StopAbandonSweeper()
	im.StopGCScanner()
	// The build pool FIRST, and before the connection: a build holds the vecstore
	// client for its whole Build → AddChunks → Save sequence, so closing the
	// connection under it turns an orderly shutdown into a burst of failures — and
	// leaving the pool running leaves its workers parked in cond.Wait() forever
	// (M7 of docs/code-review-2026-10-18.md). A manager that never built anything has
	// no pool, which is the common case in tests.
	im.buildPoolMu.Lock()
	pool := im.buildPool
	im.buildPoolMu.Unlock()
	if pool != nil {
		pool.Close()
	}
	if im.vecstoreConn != nil {
		return im.vecstoreConn.Close()
	}
	return nil
}

// SetKBMetaGetter wires a function returning a KB's metadata (e.g.
// RaftNode.GetKB). Used by async builds to read the KB-level quantizer
// configuration and forward it in the Build RPC.
func (im *IndexManagerImpl) SetKBMetaGetter(
	getter func(ctx context.Context, kbID string) (types.KnowledgeBaseMeta, error)) {
	im.kbMetaGetter = getter
}

// SetVersionParentGetter wires a lookup from a version to its parent version
// (0 when it has none, e.g. a knowledge base's first version). §8.6(c)'s
// pure-append reuse reads it to find the artifact a build may start from;
// without it every build rebuilds from scratch.
func (im *IndexManagerImpl) SetVersionParentGetter(
	getter func(ctx context.Context, kbID string, versionID int64) (int64, error)) {
	im.versionParent = getter
}

const (
	// DefaultRetentionProtectWindow is the window the retention policy shields
	// recently-queried versions for, used when RetentionProtectWindow is zero.
	DefaultRetentionProtectWindow = 24 * time.Hour

	// accessPersistInterval throttles how often a version's access time is
	// written to its .used sidecar.
	accessPersistInterval = time.Minute
)
