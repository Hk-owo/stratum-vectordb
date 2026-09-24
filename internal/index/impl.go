package index

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	vecstorepb "stratum/api/proto/vecstore"
	stratumerrors "stratum/internal/errors"
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

	// ColdThreshold is how long a version may go without a Search before
	// the background evaluator rebuilds it in the graph-free form
	// (§8.6a). The HNSW graph dominates both build time and resident
	// memory once vectors are quantized, and a version nobody queries
	// does not need it; the answers stay equivalent because the
	// graph-free variants keep the quantizer and its rerank semantics.
	// The policy is off when <= 0 (the default), which keeps the
	// historical "every version carries a full graph" behaviour.
	//
	// The evaluator applies it only to versions this node holds that are
	// neither active nor the end of the version chain, and it reshapes in both
	// directions: a graph-free version queried again within half the threshold
	// gets its graph back. The gap is hysteresis, so a version near the
	// boundary is not rebuilt on every sweep.
	ColdThreshold time.Duration

	// ColdSweepInterval is how often the evaluator re-reads the access
	// table. Zero means DefaultColdSweepInterval.
	ColdSweepInterval time.Duration

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
	// Off by default, deliberately. The scan is free and its output is a log
	// line; the collection rewrites a live artifact — atomically, and the
	// version stays queryable elsewhere, but it is still a change to data an
	// operator did not ask for. Everything that touches stored bytes should be
	// opt-in, and the log line is what tells an operator they want to opt in.
	GCEnabled bool

	// IndexServingReplicaMin is how many OTHER replicas must be serving the
	// version's index before this node may take itself out of service to collect
	// (§8.6(d)). <= 0 means DefaultIndexServingReplicaMin.
	//
	// It is independent of DurabilityPolicy.Replicas on purpose (§8.2): that one
	// protects durability — the data must survive — while this one protects READ
	// SERVICE CAPACITY. A deployment can happily run 3 replicas for durability and
	// require only 2 to be serving before rotating one out for maintenance.
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
}

// DefaultIndexServingReplicaMin is the placeholder for IndexServingReplicaMin
// (§8.6(d)): with the typical DurabilityPolicy.Replicas = 3 it leaves exactly one
// replica free to rotate through maintenance. Like the other §10.4 numbers it is
// a starting point for a deployment to adjust, not a derived value.
const DefaultIndexServingReplicaMin = 2

// DefaultGCGraphRebuildRatio is the placeholder for GCGraphRebuildRatio (§8.6(d)):
// half the artifact's vectors are dead before it is worth rebuilding a graph from
// scratch. Deliberately much higher than DefaultGCRatioThreshold — a rebuild costs
// the whole graph, so it should only be spent on a version that is clearly not
// about to be replaced.
const DefaultGCGraphRebuildRatio = 0.5

// DefaultColdSweepInterval is how often the §8.6a evaluator re-reads the
// access table when ColdSweepInterval is unset. The sweep only reads an
// in-memory map, so the cost is negligible; the delay it adds is the
// worst-case lag between a version going cold and being reshaped.
const DefaultColdSweepInterval = time.Minute

// DefaultBuildAbandonTimeout is how long a half-built artifact may sit before
// the sweeper reclaims it. The placeholder comes from §6.3: the known target is
// "a 100k-chunk index builds within 5 minutes", and 6x headroom keeps the
// sweeper away from a legitimately slow build (large batch, cold page cache).
const DefaultBuildAbandonTimeout = 30 * time.Minute

// DefaultBuildAbandonSweepInterval is how often the sweeper looks. It is
// deliberately slower than the cold evaluator: the thing it reclaims has been
// dead for half an hour, so finding it a minute later costs nothing.
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
	// neither searched nor built by this process, so the evaluator does
	// not know it exists. Guarded by mu.
	lastSearch map[indexKey]time.Time

	// builtGraphFree records, per version, whether the index currently
	// built for it is the graph-free variant (§8.6a). The evaluator uses
	// it to leave an already-cold-shaped version alone instead of
	// re-triggering a rebuild on every sweep. Dropped with the version
	// or KB, like the access record. Guarded by mu.
	builtGraphFree map[indexKey]bool

	// startedAt is when this process constructed the manager. A version this
	// process has neither queried nor built has no access record at all, so it
	// is aged from here: after a restart every version is in that state, and
	// stamping them "now" both avoids reshaping a whole knowledge base at boot
	// and avoids the evaluator standing still forever. Guarded by mu.
	startedAt time.Time

	// coldVersions enumerates the authoritative version set (§8.6a): per
	// knowledge base, which versions exist. The cold evaluator walks THIS, not
	// the local access table — the table only answers "how long since this was
	// queried here", and using it as the enumeration source makes the policy
	// forget every version on each restart. Nil disables the evaluator.
	coldVersions func(ctx context.Context) (map[string][]int64, error)

	// coldFailedAt records, per version, when a build last failed. The
	// evaluator consults it to back off: without it, a build that keeps failing
	// is re-queued on every sweep. Guarded by mu.
	coldFailedAt map[indexKey]time.Time

	// lastPersist records, per version, when its access time was last written
	// to disk (the .index.used sidecar). It is only a throttle — the value that
	// matters is on disk. Guarded by mu.
	lastPersist map[indexKey]time.Time
	// abandonCancel/abandonWG govern the background sweeper that reclaims
	// half-built index artifacts (§6). abandonCancel is nil while it is off.
	abandonCancel context.CancelFunc
	abandonWG     sync.WaitGroup

	// coldCancel/coldWG govern the background cold-version evaluator
	// (§8.6a). coldCancel is nil while the policy is off or stopped.
	coldCancel context.CancelFunc
	coldWG     sync.WaitGroup

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
	buildPool     *buildPool
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

// NewIndexManager constructs an IndexManagerImpl. The caller must either
// set cfg.VecstoreAddr (and the constructor will dial it), or inject
// vectorIndexClient and the build data-source callbacks directly before
// use (used by tests).
func NewIndexManager(cfg IndexManagerConfig) *IndexManagerImpl {
	im := &IndexManagerImpl{
		cfg:             cfg,
		loaded:          make(map[indexKey]*loadedIndex),
		loading:         make(map[indexKey]bool),
		sizeByKey:       make(map[indexKey]int64),
		lastSearch:      make(map[indexKey]time.Time),
		builtGraphFree:  make(map[indexKey]bool),
		coldFailedAt:    make(map[indexKey]time.Time),
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
				Time:                60 * time.Second,
				Timeout:             3 * time.Second,
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

// buildPriorityName is what the build-timings line reports for a request's
// priority. Spelled out rather than a boolean, because the reader's question is
// "was this build one somebody was waiting for" and the answer is the queue it
// went into.
func buildPriorityName(p BuildPriority) string {
	if p == BuildPriorityBackfill {
		return "backfill"
	}
	return "interactive"
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
// also stops the §8.6a cold-version evaluator.
func (im *IndexManagerImpl) Close() error {
	im.StopColdPolicy()
	im.StopAbandonSweeper()
	im.StopGCScanner()
	if im.vecstoreConn != nil {
		return im.vecstoreConn.Close()
	}
	return nil
}

// searchRequest builds the vector-store search request, applying the node's
// coarse-pass budget when one is configured (Stratum_设计文档v12.md §2.2).
//
// CandidateN stays unset at zero on purpose: the vector store already reads 0
// as "use my own default" (clamp(top_k × 8, 16, 4096)), and leaving the field
// out keeps a node that does not configure it identical on the wire to one
// built before this knob existed.
func (im *IndexManagerImpl) searchRequest(kbID string, versionID int64, vector []float32, topK int) *vecstorepb.SearchIndexRequest {
	req := &vecstorepb.SearchIndexRequest{
		KbId:      kbID,
		VersionId: versionID,
		Vector:    vector,
		TopK:      int32(topK),
	}
	if im.cfg.CandidateN > 0 {
		req.CandidateN = int32(im.cfg.CandidateN)
	}
	return req
}

// Search implements IndexManager.
func (im *IndexManagerImpl) Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	key := indexKey{kbID, versionID}

	// Per-stage timings of one search, at debug level — the read-path counterpart
	// of "index: build timings". The service layer's search_us wraps this whole
	// call; these fields say what it was made of, and where the process boundary
	// to vecstore sits:
	//
	//	load_us        — reopening the artifact from disk (only when not loaded)
	//	bruteforce_us  — the graph-free scan that answers a small unloaded version
	//	vecstore_us    — the gRPC round trip to the C++ HNSW search
	//	empty_check_us — the "documents are here but their chunks are not" probe,
	//	                 which runs ONLY on an empty result and costs two local reads
	searchStart := time.Now()
	var loadDur, bruteforceDur, vecstoreDur, emptyCheckDur time.Duration
	defer func() {
		im.loggerOrNop().Debug("index: search timings",
			zap.String("kb_id", kbID),
			zap.Int64("version_id", versionID),
			zap.Int("top_k", topK),
			zap.Int64("load_us", loadDur.Microseconds()),
			zap.Int64("bruteforce_us", bruteforceDur.Microseconds()),
			zap.Int64("vecstore_us", vecstoreDur.Microseconds()),
			zap.Int64("empty_check_us", emptyCheckDur.Microseconds()),
			zap.Int64("total_us", time.Since(searchStart).Microseconds()))
	}()

	// §8.6(d): while this node is collecting the version's artifact — it is
	// reopened, so it is BUILDING, so the vecstore cannot answer from it — say so
	// explicitly instead of letting the call fall through to a load that would
	// fail for a reason the caller cannot act on. The named sentinel is what lets
	// the station tell "this replica is briefly out" apart from "this version is
	// gone", and retry elsewhere.
	if im.inMaintenance(key) {
		return nil, stratumerrors.ErrIndexMaintenance
	}

	im.recordSearch(key)

	// Restore path: if the version's index is not already loaded (e.g.
	// this Go process restarted, or the index was LRU-evicted and the
	// vecstore side lost it), try loading it from the persisted file on
	// disk before falling back to ErrIndexNotReady. The load is
	// idempotent and concurrency-safe; failure here is non-fatal — a
	// truly unbuilt version still reports ErrIndexNotReady below.
	im.mu.Lock()
	_, loaded := im.loaded[key]
	_, loading := im.loading[key]
	im.mu.Unlock()
	if !loaded && !loading {
		loadStart := time.Now()
		loadErr := im.loadFromDisk(ctx, kbID, versionID)
		loadDur = time.Since(loadStart)
		if loadErr != nil {
			// §8.6b: indexes are built lazily now, so "no index on disk" is a
			// normal state rather than a failure. A small version is answered
			// by scanning; a large one falls through to the build below, which
			// acquire() then waits for.
			bruteStart := time.Now()
			answered, results, terr := im.tryBruteForce(ctx, kbID, versionID, vector, topK)
			bruteforceDur = time.Since(bruteStart)
			if terr != nil {
				return nil, terr
			}
			if answered {
				return results, nil
			}
			if err := im.TriggerBuild(ctx, kbID, versionID); err != nil {
				im.logger.Warn("index: could not schedule a lazy build",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			}
		}
	}

	// Acquire the index: load if needed, increment ref count.
	if err := im.acquire(ctx, key); err != nil {
		return nil, err
	}
	defer im.release(key)

	vecstoreStart := time.Now()
	resp, err := im.vectorIndexClient.Search(ctx, im.searchRequest(kbID, versionID, vector, topK))
	vecstoreDur = time.Since(vecstoreStart)
	if err != nil {
		// Translate the vector store's own classification rather than re-inventing it.
		// The C++ side already answers a search on a still-building index with
		// FAILED_PRECONDITION (hnsw_index.cpp → grpc_service.cpp maps absl's
		// FailedPrecondition straight through), and grpc-go hands us that as a
		// *status.Error — so the code is available here and is the right thing to read.
		//
		// Reading the code, not the message, is deliberate: the text ("index is still
		// building") is a wording detail that may change, while the status code is the
		// contract both sides already agreed on.
		//
		// Why this matters beyond tidiness: this error used to travel up wrapped in %w
		// only, and ToGRPCStatus' fallback then re-labelled a correctly-classified
		// error as Internal — the classification was not missing, it was overwritten.
		// Attaching the local sentinel keeps the meaning, and ToGRPCStatus maps it back
		// to FailedPrecondition.
		// Two codes mean the same thing ON THIS PATH: FAILED_PRECONDITION ("still
		// building") and NOT_FOUND ("no index built or loaded"). Both answer "the
		// version's index is not usable on this node yet" — whether a version exists is
		// the control layer's business, not the store's, so a NOT_FOUND here never means
		// "this version does not exist".
		switch status.Code(err) {
		case codes.FailedPrecondition, codes.NotFound:
			return nil, fmt.Errorf("index: vector search (%s/%d): %w", kbID, versionID, stratumerrors.ErrIndexNotReady)
		}
		return nil, fmt.Errorf("index: vector search (%s/%d): %w", kbID, versionID, err)
	}

	results := make([]types.SearchResult, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = types.SearchResult{ChunkID: r.ChunkId, Score: r.Score}
	}
	if len(results) == 0 {
		emptyCheckStart := time.Now()
		emptyErr := im.emptySearchMeansTheDataIsStillLanding(ctx, kbID, versionID)
		emptyCheckDur = time.Since(emptyCheckStart)
		if emptyErr != nil {
			return nil, emptyErr
		}
	}
	return results, nil
}

// emptySearchMeansTheDataIsStillLanding reports an error when a search that found
// nothing is explained by this replica not having the version's chunks yet.
//
// Three ways a replica can answer a version's query with an EMPTY result while being
// unable to serve it at all are now all named: the document set is missing
// (service/query.go), every chunk vector is unreadable (§8.6b's scan), and — this one
// — the documents are here but the chunks that carry them are not, so the index this
// replica holds is empty and a search over it SUCCEEDS with nothing.
//
// Measured on the 3+3 cluster (2026-09-21, debug level), one replica 5 s before it
// finished catching up:
//
//	query: stage timings {candidates: 0, matched_docs: 0, chunkmap_calls: 0,
//	                      filter_us: 0, meta_us: 490, bloom_us: 2472, search_us: 493}
//	index: built from scratch … version 195, total_chunks: 11, status: READY   (t+5 s)
//
// The document set read fine AND non-empty, so the service layer's "document set is
// missing" test did not fire; the search returned successfully, so nothing upstream
// saw a retryable error; and the caller got `results=0, err=nil` — the one answer a
// caller cannot act on, because the replicas that DID hold the version were never
// asked (the station reads an empty result as an answer, by design).
//
// Only "documents here, chunks not" is reported. A version whose chunks are all
// present and simply do not match is a legitimate empty answer, and stays one.
func (im *IndexManagerImpl) emptySearchMeansTheDataIsStillLanding(ctx context.Context, kbID string, versionID int64) error {
	if im.listDocIDs == nil || im.listChunkIDsByDocs == nil {
		return nil
	}
	docIDs, err := im.listDocIDs(ctx, kbID, versionID)
	if err != nil || len(docIDs) == 0 {
		// Not this function's to name: "this replica has no document set" is already
		// its own retryable refusal in the service layer, and a version that really has
		// no documents is a legitimate empty answer.
		return nil
	}
	chunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, docIDs)
	if err != nil || len(chunkIDs) > 0 {
		return nil
	}
	return fmt.Errorf("index: vector search (%s/%d): %d documents are here but none of their chunks are, so this replica's index for the version is empty: %w",
		kbID, versionID, len(docIDs), stratumerrors.ErrIndexNotReady)
}

// TriggerBuild implements IndexManager.
//
// Interactive priority: this is what EnsureIndex calls when someone is waiting for
// the index to answer a query or a confirmation. Reconcile used to call it too, which
// is why a post-restart sweep could outrank live work; reconcile now uses
// TriggerBuildBackfill below.
func (im *IndexManagerImpl) TriggerBuild(ctx context.Context, kbID string, versionID int64) error {
	return im.triggerBuild(kbID, versionID, false, BuildPriorityInteractive)
}

// TriggerBuildBackfill schedules a build nobody is waiting for yet — a head start,
// typically reconcile restoring what it can after a restart.
//
// It yields to every interactive build, so no backlog of these can starve a live
// write of its build.
func (im *IndexManagerImpl) TriggerBuildBackfill(ctx context.Context, kbID string, versionID int64) error {
	return im.triggerBuild(kbID, versionID, false, BuildPriorityBackfill)
}

// TriggerBuildGraphFree builds the version without an HNSW graph (§8.6a), for a
// cold version whose index is unlikely to be queried: the graph is what makes
// it expensive to build and to keep resident, and a scan of quantized codes is
// an acceptable price for a version nobody is asking about.
func (im *IndexManagerImpl) TriggerBuildGraphFree(ctx context.Context, kbID string, versionID int64) error {
	// Backfill priority: §8.6a reshapes a COLD version — by definition one nobody is
	// querying — so this is a head start, not something anyone is waiting for.
	return im.triggerBuild(kbID, versionID, true, BuildPriorityBackfill)
}

// StartAbandonSweeper reclaims index artifacts whose build was abandoned
// (coordinator-selection-and-node-liveness-design.md §6).
//
// The gap it closes: a build that dies or times out leaves a half-written
// artifact behind, and nothing will ever come back for it. The control layer
// moves on to the next candidate, that candidate succeeds, the version reaches
// READY — so it never reaches FAILED_PERMANENT either, and §1.4's cleanup
// broadcast never fires. The remains sit there for good. This is the everyday
// case (an ordinary timeout or transient failure), far more frequent than a
// version being condemned outright.
//
// Two deliberate properties:
//
//   - The verdict is read from the ARTIFACT's mtime, not from an in-process
//     timer. A candidate may itself restart, and an in-memory timer would go
//     with it, losing all knowledge of how long the remains have been there.
//   - It waits for no external signal. §1.4's broadcast only covers the case
//     where the version is condemned; here nobody will ever send "you were
//     abandoned" — the control layer has already chosen someone else. Like the
//     local health view (§5) and the leader's soft state (§4.3), the node
//     cleans up on its own rather than depending on a message that may never
//     arrive.
//
// A negative BuildAbandonTimeout disables it; <= 0 takes the default. It is
// idempotent, and it does nothing when persistence is unconfigured (there is no
// on-disk artifact to reclaim).
func (im *IndexManagerImpl) StartAbandonSweeper() {
	if im.cfg.BuildAbandonTimeout < 0 || im.cfg.IndexDataDir == "" {
		return
	}
	im.mu.Lock()
	if im.abandonCancel != nil {
		im.mu.Unlock()
		return // already running
	}
	ctx, cancel := context.WithCancel(context.Background())
	im.abandonCancel = cancel
	im.mu.Unlock()

	im.abandonWG.Add(1)
	go func() {
		defer im.abandonWG.Done()
		ticker := time.NewTicker(DefaultBuildAbandonSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				im.sweepAbandonedArtifacts(now)
			}
		}
	}()
}

// StopAbandonSweeper stops the sweeper and waits for the running sweep to
// return. Safe to call when it was never started.
func (im *IndexManagerImpl) StopAbandonSweeper() {
	im.mu.Lock()
	cancel := im.abandonCancel
	im.abandonCancel = nil
	im.mu.Unlock()
	if cancel != nil {
		cancel()
		im.abandonWG.Wait()
	}
}

// sweepAbandonedArtifacts reclaims what a dead Save left behind.
//
// Two kinds of remains, and only two — v13 §8.8 named both:
//
//   - An <v>.index with no <v>.index.ids. Save renames the Faiss file into
//     place FIRST and its .ids sidecar SECOND (§8.3's atomic-write ordering),
//     so a process that dies between the two renames leaves exactly this: an
//     index nothing can load (Load rejects an unpaired file) and nothing will
//     ever rewrite.
//   - <v>.index.tmp / <v>.index.ids.tmp, the temporaries Save writes before
//     renaming. A stale one is a Save that died mid-write; the next Save
//     overwrites it and EnforceDiskRetention ignores it, but until then it
//     costs its full size.
//
// A SEALED pair is never touched here, however old — a READY version's artifact
// staying on disk is EnforceDiskRetention's business, not this one's.
//
// The mtime test exists for the two-rename window, not for "a build in flight":
// a build runs entirely in memory and touches no file (§8.8 finding 1 and 3), so
// a stale mtime cannot mean "still building". What it means is that the remains
// have been there for a while, and waiting one timeout keeps the sweeper away
// from a Save that is merely slow to finish. Nothing is reported to the control
// layer — the version's state there is authoritative, and this node's local
// remains are not its concern.
func (im *IndexManagerImpl) sweepAbandonedArtifacts(now time.Time) {
	timeout := im.cfg.BuildAbandonTimeout
	if timeout <= 0 {
		timeout = DefaultBuildAbandonTimeout
	}
	root := filepath.Join(im.cfg.IndexDataDir, "index")
	kbs, err := os.ReadDir(root)
	if err != nil {
		if !os.IsNotExist(err) {
			im.logger.Warn("index: abandoned-artifact sweep could not read the index directory", zap.Error(err))
		}
		return
	}
	for _, kb := range kbs {
		if !kb.IsDir() {
			continue
		}
		dir := filepath.Join(root, kb.Name())
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			switch {
			case strings.HasSuffix(name, ".index.tmp"), strings.HasSuffix(name, ".index.ids.tmp"):
				im.removeIfStale(dir, kb.Name(), e, now, timeout,
					"an abandoned Save temporary")
			case strings.HasSuffix(name, ".index"):
				version := strings.TrimSuffix(name, ".index")
				// Sealed by a Save: a completed artifact, not a remainder.
				if fileExists(filepath.Join(dir, version+".index.ids")) {
					continue
				}
				im.removeIfStale(dir, kb.Name(), e, now, timeout,
					"an index artifact that no Save ever sealed")
			}
		}
	}
}

// removeIfStale deletes one entry whose mtime is older than timeout, and says
// so. A sweep is best-effort by design: a file that cannot be stat'ed or
// removed is left for the next pass rather than failing the whole sweep.
func (im *IndexManagerImpl) removeIfStale(dir, kbID string, e os.DirEntry, now time.Time, timeout time.Duration, what string) {
	info, err := e.Info()
	if err != nil {
		return
	}
	age := now.Sub(info.ModTime())
	if age < timeout {
		return
	}
	if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !os.IsNotExist(err) {
		im.logger.Warn("index: could not remove an abandoned artifact",
			zap.String("kb_id", kbID), zap.String("file", e.Name()), zap.Error(err))
		return
	}
	im.logger.Warn("index: removed "+what+" — nothing else would have reclaimed it",
		zap.String("kb_id", kbID), zap.String("file", e.Name()),
		zap.Duration("age", age), zap.Duration("timeout", timeout))
}

// StartColdPolicy starts the background evaluator that reshapes versions
// which have gone cold (§8.6a). It is a no-op when ColdThreshold <= 0, so
// a deployment that does not configure a threshold keeps the historical
// "every version carries a full graph" behaviour, and it is idempotent:
// calling it twice leaves one evaluator running.
//
// The policy is intentionally local and passive, per the design's "自动、
// 存储层自己判": it reads only this node's own access table, takes no
// part in consensus, and never talks to other nodes. Two replicas may
// therefore reshape at slightly different times; that is harmless, since
// the graph-free variant answers the same queries (its quantizer and
// rerank semantics are unchanged) and each node's build goes through the
// same Save + §8.4 distribution path as any other build.
func (im *IndexManagerImpl) StartColdPolicy() {
	if im.cfg.ColdThreshold <= 0 {
		return
	}
	im.mu.Lock()
	if im.coldCancel != nil {
		im.mu.Unlock()
		return // already running
	}
	ctx, cancel := context.WithCancel(context.Background())
	im.coldCancel = cancel
	im.mu.Unlock()

	interval := im.cfg.ColdSweepInterval
	if interval <= 0 {
		interval = DefaultColdSweepInterval
	}
	im.coldWG.Add(1)
	go func() {
		defer im.coldWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				im.sweepCold(ctx, now)
			}
		}
	}()
}

// StopColdPolicy stops the evaluator and waits for the running sweep to
// return. Safe to call when the policy was never started.
func (im *IndexManagerImpl) StopColdPolicy() {
	im.mu.Lock()
	cancel := im.coldCancel
	im.coldCancel = nil
	im.mu.Unlock()
	if cancel != nil {
		cancel()
		im.coldWG.Wait()
	}
}

// sweepCold reshapes every version whose shape no longer matches its traffic:
// a version that went cold is rebuilt graph-free, and one that came back is
// rebuilt with its graph. Scheduling is asynchronous (triggerBuild spawns the
// build), so a sweep never blocks on a vecstore build; a version already
// building is skipped by triggerBuild itself.
func (im *IndexManagerImpl) sweepCold(ctx context.Context, now time.Time) {
	for _, c := range im.coldCandidates(ctx, now) {
		if ctx.Err() != nil {
			return
		}
		key := c.key
		var err error
		if c.reheat {
			im.logger.Info("index: version is queried again; restoring its graph (§8.6a)",
				zap.String("kb_id", key.kbID), zap.Int64("version_id", key.versionID))
			err = im.TriggerBuildBackfill(ctx, key.kbID, key.versionID)
		} else {
			im.logger.Info("index: version is cold; reshaping graph-free (§8.6a)",
				zap.String("kb_id", key.kbID), zap.Int64("version_id", key.versionID))
			err = im.TriggerBuildGraphFree(ctx, key.kbID, key.versionID)
		}
		if err != nil {
			im.logger.Warn("index: could not schedule the reshape",
				zap.String("kb_id", key.kbID), zap.Int64("version_id", key.versionID), zap.Error(err))
		}
	}
}

// coldCandidate is one version the §8.6a evaluator wants to reshape, with the
// shape it wants.
type coldCandidate struct {
	key indexKey
	// reheat is true when the version is being queried again and needs its HNSW
	// graph back; false when it went cold and should be rebuilt graph-free.
	reheat bool
}

// coldCandidates returns this sweep's reshapes, in a deterministic
// (kbID, versionID) order.
//
// It walks the AUTHORITATIVE version set (coldVersions, from the control
// layer's metadata), not the local access table: the table only supplies "how
// long since this version was queried here", and using it as the enumeration
// source makes the policy forget every version on each restart (§8.6a). Three
// filters decide what is left:
//
//   - An ACTIVE version is never reshaped. The design says "a cold version —
//     non-active and long unqueried", and dropping the first half turns the
//     knowledge base's serving version into an O(n) scan. When the control
//     layer has no active pointer (the common case: CreateVersion does not move
//     it, only RollbackVersion does), the end of the version chain — the
//     largest versionID — is what actually serves, so it is excluded too.
//   - A version THIS NODE DOES NOT HOLD is never reshaped. A version that was
//     merely queried once, whose index never landed here, must not be built by
//     the evaluator out of nowhere: that bypasses §8.6b's build-on-demand and
//     pairs with the disk retention policy into a build-then-delete loop.
//   - A FAILED build backs off. Re-enumerating every sweep must not mean
//     re-queuing a build that keeps failing.
//
// The shape is read from the artifact's sidecar (artifactGraphFree), not from
// the in-memory record: the latter is lost on restart, which would make the
// evaluator rebuild an already graph-free version on every sweep.
func (im *IndexManagerImpl) coldCandidates(ctx context.Context, now time.Time) []coldCandidate {
	threshold := im.cfg.ColdThreshold
	im.mu.Lock()
	versionsProvider := im.coldVersions
	activeProvider := im.activeVersions
	im.mu.Unlock()

	if versionsProvider == nil {
		return nil
	}
	authority, err := versionsProvider(ctx)
	if err != nil {
		im.logger.Warn("index: cold policy could not read the version set", zap.Error(err))
		return nil
	}

	active := map[string]int64{}
	if activeProvider != nil {
		if a, err := activeProvider(ctx); err != nil {
			im.logger.Warn("index: cold policy could not read the active versions", zap.Error(err))
		} else {
			active = a
		}
	}

	im.mu.Lock()
	defer im.mu.Unlock()

	var out []coldCandidate
	for kbID, versionIDs := range authority {
		if im.deletedKBs[kbID] {
			continue
		}
		newest := newestVersion(versionIDs)
		for _, versionID := range versionIDs {
			key := indexKey{kbID, versionID}
			if im.deletedVersions[key] || im.loading[key] {
				continue
			}
			// The version serving queries is never reshaped: the active one, and
			// the end of the chain (what serves when the control layer has no
			// active pointer). They can be different versions, so both are
			// checked rather than one standing in for the other.
			if act, ok := active[kbID]; ok && versionID == act {
				continue
			}
			if versionID == newest {
				continue
			}
			if !im.holdsArtifactLocked(key) {
				continue
			}
			last, ok := im.lastSearch[key]
			if !ok {
				// Never seen by this process: age it from startup rather than
				// treating it as permanently hot (the policy would stall) or as
				// immediately cold (a restart would reshape the whole node).
				last = im.startedAt
			}
			if failedAt, ok := im.coldFailedAt[key]; ok && !last.After(failedAt) {
				continue // nothing queried it since the failure: back off
			}
			age := now.Sub(last)
			graphFree, known := im.shapeGraphFreeLocked(key)
			switch {
			case !known || !graphFree:
				if age >= threshold {
					out = append(out, coldCandidate{key: key})
				}
			case age <= threshold/2:
				// Queried again within half the cold window: restore the graph.
				// The gap between the two thresholds is hysteresis — without it
				// a version sitting near the boundary is rebuilt, and
				// redistributed, on every sweep, in both directions.
				out = append(out, coldCandidate{key: key, reheat: true})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].key.kbID != out[j].key.kbID {
			return out[i].key.kbID < out[j].key.kbID
		}
		return out[i].key.versionID < out[j].key.versionID
	})
	return out
}

// newestVersion returns the largest version id in ids (0 for an empty set).
func newestVersion(ids []int64) int64 {
	var newest int64
	for _, id := range ids {
		if id > newest {
			newest = id
		}
	}
	return newest
}

// holdsArtifactLocked reports whether this node holds the version's index
// artifact — in memory, or as a file on disk. The question is "do we have it",
// not "was it ever queried": a query puts a version in the access table without
// implying this node has its index, and §8.6a reshapes only what is here.
// The stat happens under the lock on purpose: the caller is a low-frequency
// background sweep.
func (im *IndexManagerImpl) holdsArtifactLocked(key indexKey) bool {
	if _, ok := im.loaded[key]; ok {
		return true
	}
	if _, ok := im.builtGraphFree[key]; ok {
		return true
	}
	if im.cfg.IndexDataDir == "" {
		return false
	}
	return fileExists(im.indexPath(key.kbID, key.versionID))
}

// shapeGraphFreeLocked reports the version's current index shape and whether it
// is known. The in-memory record is consulted first (a version this process
// just built needs no disk read); the sidecar is the fallback, and the only
// source that survives a restart or describes an artifact this node merely
// received from a peer.
func (im *IndexManagerImpl) shapeGraphFreeLocked(key indexKey) (graphFree bool, known bool) {
	if graphFree, ok := im.builtGraphFree[key]; ok {
		return graphFree, true
	}
	if im.cfg.IndexDataDir == "" {
		return false, false
	}
	return im.artifactGraphFree(key.kbID, key.versionID)
}

// shapeGraphFree is shapeGraphFreeLocked for callers that do not hold im.mu.
//
// It exists because §8.6(c)'s reuse and §8.6(d)'s two collections used to read
// the in-memory record ALONE, while §8.6(a)'s cold/hot decision read the sidecar
// as well (shapeGraphFreeLocked above). Those two answers differ exactly where it
// matters: `builtGraphFree` is written only by this node's own successful build
// (doBuild), so a version §8.4 handed this node has no entry — and "no entry" was
// being read as "shape unknown", which sent such a version down the silent
// full-rebuild path in appendBase and off both collection paths entirely. Measured
// on the 3+3 cluster: 2 of 3 replicas rebuilt every child from scratch while
// holding the parent's artifact, and their tombstones were never collected.
//
// The disk read is the point, not a fallback of convenience (see artifactGraphFree):
// the shape line is written by vecstore's Save right next to the artifact, so it
// survives a restart, travels with a §8.4 handoff, and is cross-checked against
// the file's actual shape when the artifact is loaded — a mismatched pair refuses
// to load rather than being served as the other shape. A sidecar predating the
// line (§8.6a) reports known=false, which every caller must keep treating as the
// conservative answer.
func (im *IndexManagerImpl) shapeGraphFree(kbID string, versionID int64) (graphFree bool, known bool) {
	key := indexKey{kbID, versionID}
	im.mu.Lock()
	graphFree, known = im.builtGraphFree[key]
	im.mu.Unlock()
	if known {
		return graphFree, true
	}
	if im.cfg.IndexDataDir == "" {
		return false, false
	}
	return im.artifactGraphFree(kbID, versionID)
}

func (im *IndexManagerImpl) triggerBuild(kbID string, versionID int64, graphFree bool, priority BuildPriority) error {
	key := indexKey{kbID, versionID}

	im.mu.Lock()
	if im.loading[key] {
		im.mu.Unlock()
		return nil // build already in progress
	}
	im.loading[key] = true
	im.mu.Unlock()

	// Queued, not spawned. The pool bounds how many builds run at once, and lets a
	// build somebody is waiting for jump ahead of a reconcile sweep's backfill.
	if !im.submitBuild(buildRequest{
		kbID:       kbID,
		versionID:  versionID,
		graphFree:  graphFree,
		priority:   priority,
		enqueuedAt: time.Now(),
	}) {
		// The pool is shut down and will never run this. Clear the in-progress marker
		// so the state stays truthful — a version that is NOT being built must not
		// look like one that is, or EnsureIndex would wait for a build that will never
		// arrive.
		im.mu.Lock()
		delete(im.loading, key)
		im.mu.Unlock()
		return errors.New("index: the build pool is closed")
	}
	return nil
}

// submitBuild hands a request to the pool, starting it on first use.
//
// Lazy on purpose: a manager that never builds anything — most unit tests — should
// start no workers at all, and deferring the startup to the first request puts the
// cost where the work is.
func (im *IndexManagerImpl) submitBuild(req buildRequest) bool {
	im.buildPoolOnce.Do(func() {
		im.buildPool = newBuildPool(im.cfg.BuildConcurrency, func(r buildRequest) {
			im.doBuild(r)
		}, im.logger)
	})
	return im.buildPool.Submit(req)
}

func (im *IndexManagerImpl) doBuild(req buildRequest) {
	kbID, versionID, graphFree := req.kbID, req.versionID, req.graphFree
	key := indexKey{kbID, versionID}

	status := types.IndexStatusReady
	var sizeBytes int64
	// evicted collects what makeRoomLocked had to drop to fit this index; it is
	// handed to the vecstore after the lock is released (dropEvicted).
	var evicted []indexKey

	// Per-stage timings of one build, at debug level.
	//
	// Why they are here: this is stage 6 of the write path — the version is
	// PENDING until this returns and the control layer records READY — and until
	// now the only thing a caller could observe was the wait itself. Queueing and
	// building are the two things that can make that wait long, and they call for
	// opposite responses (more build workers vs. a look at vecstore and the
	// batch), so the line separates them. build_us measures buildWithRetry, and
	// therefore includes any retries it needed.
	buildStart := time.Now()
	var buildUs time.Duration

	// Deferred cleanup guarantees that loading is ALWAYS cleared and
	// waiters are ALWAYS woken, even if build()/makeRoomLocked/panics
	// below blow up. Without this a wedged build goroutine leaves
	// loading[key] true forever and every acquire() on that version spins
	// in cond.Wait until its context dies — the pressure-test Query hang.
	defer func() {
		if r := recover(); r != nil {
			im.logger.Error("index build panicked; marking version FAILED",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Any("panic", r))
			status = types.IndexStatusFailed
		}

		// Emitted HERE, and not from a defer of its own, for two reasons.
		//
		// Ordering: this line must be in the log before anything downstream of
		// the build can act on its outcome — the waiters woken by the broadcast
		// below and the completion callbacks that carry the status to the
		// control layer. A separate defer registered above this one would run
		// after them (defers are LIFO), so a reader woken by the outcome would
		// race the line's arrival and could not rely on it to explain what it
		// just saw.
		//
		// Completeness: status is only final once the panic recovery above has
		// run, so a panicking build gets its line too, with the FAILED status
		// this build actually reported.
		if !req.enqueuedAt.IsZero() {
			im.buildLogger().Debug("index: build timings",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Bool("graph_free", graphFree), zap.String("priority", buildPriorityName(req.priority)),
				zap.Int64("queue_us", buildStart.Sub(req.enqueuedAt).Microseconds()),
				zap.Int64("build_us", buildUs.Microseconds()),
				zap.Int64("total_us", time.Since(req.enqueuedAt).Microseconds()),
				zap.String("status", status.String()),
				zap.Int64("size_bytes", sizeBytes))
		}

		im.mu.Lock()
		delete(im.loading, key)
		if status != types.IndexStatusReady {
			// Back off the cold evaluator: it re-enumerates on every sweep, so
			// without a record of this failure a build that cannot succeed is
			// re-queued forever (both the graph-free reshape and the reheat go
			// through here).
			im.coldFailedAt[key] = time.Now()
		}
		if status == types.IndexStatusReady {
			// Make room before inserting the new index, then release whatever it
			// evicted on the vecstore side once the lock is gone (see
			// dropEvicted).
			evicted = im.makeRoomLocked()
			im.loaded[key] = &loadedIndex{lastAccess: time.Now()}
			im.sizeByKey[key] = sizeBytes
			im.loadedBytes += sizeBytes
			// Remember which shape is now on disk, so the cold
			// evaluator does not rebuild an already graph-free version
			// on every sweep (§8.6a).
			im.builtGraphFree[key] = graphFree
			// A version nobody has searched yet has no access record;
			// seed one so the cold policy has a baseline to age from
			// instead of having to guess when an unqueried version was
			// last relevant.
			im.seedAccessLocked(key)
		}
		callbacks := append([]BuildCompleteCallback(nil), im.callbacks...)
		im.cond.Broadcast()
		im.mu.Unlock()
		im.dropEvicted(evicted)

		// Persist the size sidecar next to the index file so a later
		// restart (loadFromDisk) can account for this version's memory
		// footprint. Best-effort: a failed sidecar write only degrades
		// the estimate to 0.
		if status == types.IndexStatusReady {
			im.persistSizeSidecar(kbID, versionID, sizeBytes)
			// §8.4(a): put this version's retention shield on disk too, not
			// only in seedAccessLocked's in-memory table. The callback below
			// ships the artifact to the replicas, and the retention pass that
			// could drop it runs on the *next* build — possibly one that fires
			// while this distribution is still queued behind the push semaphore.
			im.recordInterestNow(kbID, versionID)
		}

		// Invoke callbacks with retry. The on-disk retention policy is
		// NOT enforced here: EnforceDiskRetention needs to know the KB's
		// active version (to avoid dropping a rolled-back active
		// version's index), which requires the Raft layer — the
		// registered BuildCompleteCallback in cmd/stratum/main.go
		// applies the policy instead.
		for _, cb := range callbacks {
			im.invokeCallback(cb, kbID, versionID, status)
		}
	}()

	var err error
	sizeBytes, err = im.buildWithRetry(kbID, versionID, graphFree)
	buildUs = time.Since(buildStart)
	if err != nil {
		im.logger.Error("index build failed",
			zap.String("kb_id", kbID),
			zap.Int64("version_id", versionID),
			zap.Error(err))
		status = types.IndexStatusFailed
		if !isTransientBuildErr(err) {
			// 确定性失败（vecstore 拒绝了这批数据、参数不合法……）不会因为再试一次
			// 而变好：buildWithRetry 已经在 isTransientBuildErr 上认出了这一点并提前
			// 返回。上报时必须把同一判断带上——否则控制层收到的是「暂时失败」，把它
			// 记进重试预算，版本就一直停在 PENDING 等一个永远不会来的成功
			// （§10.1、cmd/stratum/main.go 的 reportIndexStatus）。
			status = types.IndexStatusFailedPermanent
		}
	}
}

// buildWithRetry runs build() inside a bounded window, retrying on
// failure so transient conditions self-heal instead of leaving the
// version FAILED/PENDING. Observed transient failures:
//   - vecstore "Save: no index has been built or loaded": a concurrent
//     build of the same (kb, version) can reset the shared HNSW index
//     between this node's Build and Save RPCs; a retry typically
//     succeeds once the other build finishes.
//   - follower builds racing the data sync: a pull may not yet have
//     landed all chunk-doc/version-doc entries when the build reads them,
//     so the chunk set comes back empty; a retry after the sync
//     converges succeeds.
//
// build() is idempotent (Build/AddChunks/Save re-write the same keys), so
// retrying is safe. The whole retry loop is bounded by buildRetryTimeout
// so a permanently-failing build still surfaces as FAILED instead of
// wedging the version forever.
const buildRetryTimeout = 5 * time.Minute
const buildRetryInterval = 2 * time.Second

// isTransientBuildErr reports whether a build failure is worth retrying:
// vecstore RPC failures of the "data not ready / index reset by a
// concurrent build / connection teardown" kind (FailedPrecondition,
// Unavailable, DeadlineExceeded, Internal). Deterministic errors (e.g. a
// caller-level error string) fail immediately so callers see a FAILED
// status promptly instead of a silent retry loop.
func isTransientBuildErr(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.FailedPrecondition, codes.Internal:
		return true
	}
	return false
}

// chunkVectorReadErr 给"读 chunk 向量失败"一个可判别的名字：vecstore 对尚未落盘的
// 向量答 NOT FOUND，而那是**数据侧的一个过渡态**，不是关于这次构建的判据。
//
// 写入路径先让文档与 chunk 映射可见、向量随后才落盘，所以构建恰好在这两者之间起跑
// 时，"要一个还在路上的向量"是合法的。isTransientBuildErr 把裸的 NOT FOUND 读成
// 确定性失败，一次竞态就退掉整个版本 —— 3+3 集群实测：一个副本的数据还在落盘，它用
// 暴力扫描回答了查询（§7.4 有意保留的容忍），同一次查询触发了 lazy build，其中一个
// chunk 的向量读回 NOT FOUND，于是版本被标 FAILED_PERMANENT：包括两个已经建好索引的
// 副本在内，**任何副本上都不再可查**，直到运维重建。
//
// 换成 FailedPrecondition（并保留原文）就把这一例交给 buildWithRetry —— 它在
// buildRetryTimeout 内等数据落齐，构造函数内注释里"抢在同步前面的构建，重试即可收敛"
// 说的正是这件事。其余错误码原样保留，确定性失败照旧立刻上报。
func chunkVectorReadErr(chunkID string, err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("index: read chunk vector %s: not stored yet (the data is still landing): %w",
			chunkID, status.Errorf(codes.FailedPrecondition, "%v", err))
	}
	return fmt.Errorf("index: read chunk vector %s: %w", chunkID, err)
}

func (im *IndexManagerImpl) buildWithRetry(kbID string, versionID int64, graphFree bool) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), buildRetryTimeout)
	defer cancel()

	var lastErr error
	for {
		// skipReuse false: a normal build, where starting from the parent's
		// artifact is the cheaper path (§8.6(c)).
		sizeBytes, err := im.build(ctx, kbID, versionID, graphFree, false)
		if err == nil {
			return sizeBytes, nil
		}
		lastErr = err
		if !isTransientBuildErr(err) {
			return 0, err
		}
		select {
		case <-ctx.Done():
			return 0, lastErr
		case <-time.After(buildRetryInterval):
		}
	}
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

// quantizerForKB maps a knowledge base's quantizer metadata to the
// vecstore Build RPC fields. OFF (or an unknown value) maps to no
// quantization, keeping the historical full-precision behavior.
func quantizerForKB(kb types.KnowledgeBaseMeta, graphFree bool) (vecstorepb.QuantizerTypeProto, int32, int32) {
	var q vecstorepb.QuantizerTypeProto
	switch kb.QuantizerType {
	case "SQ8":
		q = vecstorepb.QuantizerTypeProto_QUANTIZER_SQ8
	case "SQ_BF16":
		q = vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_BF16
	case "SQ_FP16":
		q = vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_FP16
	case "PQ":
		q = vecstorepb.QuantizerTypeProto_QUANTIZER_PQ
	default:
		q = vecstorepb.QuantizerTypeProto_QUANTIZER_OFF
	}
	pqM, pqNBits := int32(0), int32(0)
	if kb.QuantizerType == "PQ" {
		pqM = int32(kb.QuantizerPQM)
		pqNBits = int32(kb.QuantizerPQNBits)
	}
	return graphFreeVariant(q, graphFree), pqM, pqNBits
}

// graphFreeVariant maps a quantizer to its graph-free twin (§8.6a). The
// quantizer itself is unchanged; only the HNSW graph is dropped.
func graphFreeVariant(q vecstorepb.QuantizerTypeProto, graphFree bool) vecstorepb.QuantizerTypeProto {
	if !graphFree {
		return q
	}
	switch q {
	case vecstorepb.QuantizerTypeProto_QUANTIZER_OFF:
		return vecstorepb.QuantizerTypeProto_QUANTIZER_OFF_FLAT
	case vecstorepb.QuantizerTypeProto_QUANTIZER_SQ8:
		return vecstorepb.QuantizerTypeProto_QUANTIZER_SQ8_FLAT
	case vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_BF16:
		return vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_BF16_FLAT
	case vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_FP16:
		return vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_FP16_FLAT
	case vecstorepb.QuantizerTypeProto_QUANTIZER_PQ:
		return vecstorepb.QuantizerTypeProto_QUANTIZER_PQ_FLAT
	default:
		return q
	}
}

// buildQuantizerFromKB returns the vecstore Build RPC quantizer fields
// for kbID, consulting kbMetaGetter when set (default OFF otherwise).
func (im *IndexManagerImpl) buildQuantizerFromKB(ctx context.Context, kbID string, graphFree bool) (vecstorepb.QuantizerTypeProto, int32, int32) {
	if im.kbMetaGetter == nil {
		return graphFreeVariant(vecstorepb.QuantizerTypeProto_QUANTIZER_OFF, graphFree), 0, 0
	}
	kb, err := im.kbMetaGetter(ctx, kbID)
	if err != nil {
		return graphFreeVariant(vecstorepb.QuantizerTypeProto_QUANTIZER_OFF, graphFree), 0, 0
	}
	return quantizerForKB(kb, graphFree)
}

// build executes the full build data flow and returns the estimated
// in-memory footprint of the built index (sum of vector payload bytes;
// 0 for an empty version). It reports success only if the index was also
// persisted to disk (see saveToDisk), so a failed save surfaces as a
// build failure.
func (im *IndexManagerImpl) build(ctx context.Context, kbID string, versionID int64, graphFree bool, skipReuse bool) (int64, error) {
	// Forward the KB-level quantizer config with every Build RPC; OFF
	// (default) keeps the historical full-precision index type.
	quantizerType, pqM, pqNBits := im.buildQuantizerFromKB(ctx, kbID, graphFree)
	// Last reported in-memory estimate from the vecstore (0 = unreported).
	reportedMemBytes := int64(0)

	docIDs, err := im.listDocIDs(ctx, kbID, versionID)
	if err != nil {
		return 0, fmt.Errorf("index: ListDocIDs: %w", err)
	}

	chunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, docIDs)
	if err != nil {
		return 0, fmt.Errorf("index: ListChunkIDsByDocs: %w", err)
	}

	// 码本刷新判定（docs/codebook-refresh-plan.md §3）：只在"追加复用本来会发生"
	// 时才需要 —— 判定成立就把它关掉，让下面走全量重建，而全量重建正是重训码本
	// 的唯一途径。放在 appendBase 之前，是为了不先白付一次 LoadForAppend。
	if !skipReuse {
		if needed, reason, ratio := im.codebookRefreshNeeded(ctx, kbID, versionID, quantizerType, len(chunkIDs)); needed {
			skipReuse = true
			im.logger.Info("index: full rebuild to refresh the quantizer codebook (§3)",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.String("reason", reason),
				zap.Float64("cumulative_drift_ratio", ratio),
				zap.Int("total_chunks", len(chunkIDs)),
				zap.String("quantizer", quantizerType.String()))
		}
	}

	// §8.6(c): a pure-append version can start from its parent's artifact and
	// add only the new chunks, instead of rebuilding the whole graph. Purely
	// an optimisation — a failure here falls back to the full build below,
	// and nothing downstream (callback, distribution, retention) can tell
	// which path produced the artifact.
	//
	// skipReuse turns it off, and two callers need that: §8.6(d)'s graphed
	// collection, and §3's codebook refresh just above. The reasoning is the
	// same shape for both — the rebuild exists to discard something the parent's
	// artifact would otherwise carry in: tombstones in the first case, a stale
	// codebook in the second (a quantized version's existing codes cannot be
	// re-encoded under a new codebook in place, so a full rebuild is the only
	// way to refresh it).
	if !skipReuse {
		if parentID, delta, dead, ok := im.appendBase(ctx, kbID, versionID, graphFree, chunkIDs); ok {
			size, appendErr := im.buildFromBase(ctx, kbID, versionID, parentID, delta, dead, len(chunkIDs), graphFree)
			if appendErr == nil {
				im.logger.Info("index: built by appending to the parent version's artifact (§8.6c)",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
					zap.Int64("parent_version_id", parentID),
					zap.Int("total_chunks", len(chunkIDs)), zap.Int("delta_chunks", len(delta)),
					zap.Float64("append_delta_ratio", float64(len(delta))/float64(max(len(chunkIDs), 1))),
					zap.Int("deleted_chunks", len(dead)), zap.Bool("graph_free", graphFree))
				return size, nil
			}
			if errors.Is(appendErr, errAppendTooManyTombstones) {
				// Not a failure: the reuse was legal, but the base carried too much
				// dead weight, so rebuilding (which drops it) is the better trade.
				// The ratio is reported here too: this path still pays for a full
				// rebuild, and without the number beside it there is no way to see
				// which versions keep drifting past the limit
				// (docs/content-defined-chunking-plan.md §5).
				im.logger.Info("index: append reuse not worth it; rebuilding from scratch",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
					zap.Int64("parent_version_id", parentID),
					zap.Int("total_chunks", len(chunkIDs)), zap.Int("delta_chunks", len(delta)),
					zap.Float64("append_delta_ratio", float64(len(delta))/float64(max(len(chunkIDs), 1))),
					zap.Error(appendErr))
			} else {
				im.logger.Warn("index: append reuse failed; rebuilding from scratch",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
					zap.Int64("parent_version_id", parentID), zap.Error(appendErr))
			}
		} else {
			// No reuse AND no line about it: the silent full rebuild. A version this
			// node received from a peer had no in-memory shape to check, so appendBase
			// declined and this path left no trace at all — which is why "only the
			// parent's builder ever reuses" was invisible in production logs. One line
			// per version, the same volume as "built by appending" below, so the two
			// can be counted against each other (§8.6c reuse rate).
			im.logger.Info("index: built from scratch — no reusable parent artifact on this node (§8.6c)",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Int("total_chunks", len(chunkIDs)), zap.Bool("graph_free", graphFree))
		}
	}

	// 分批读取并发送：单条 Build/AddChunks RPC 的载荷必须小于 gRPC 默认
	// 4 MiB 上限。第一批用 Build 全量建索引，后续批用 AddChunks 增量追加。
	batches, sizeBytes, err := im.collectChunkBatches(ctx, kbID, chunkIDs)
	if err != nil {
		return 0, err
	}

	// 空版本（没有 chunk）：vecstore 侧不建索引。
	//
	// 这里曾经调一次 Build(empty) 再 Save，期望在 vecstore 侧"建立该
	// (kb,version) 的索引条目"。实测不成立：vecstore 的
	// AddChunksLocked 对空 batch 直接返回 OkStatus，不创建 Faiss 索引，
	// 于是紧随其后的 Save 永远报
	// "Save: no index has been built or loaded"（FailedPrecondition）。
	// 该错误被 isTransientBuildErr 判为可重试，构建便进入 5 分钟重试窗
	// 口，loading[key] 一直为真 —— 期间对该版本的任何查询都以
	// "index load timeout" 失败（integration 的 TwoNodeReplication /
	// FaultTolerance 就是这么被拖垮的：KB 初始版本 v1 是空版本）。
	//
	// 空版本没有可检索内容，构建到此即完成：查询由 tryBruteForce 直接
	// 回答空结果（chunk 数 0 远低于 BruteForceMaxChunks），不需要、也
	// 无法在 vecstore 侧留下一个空索引文件。
	if len(batches) == 0 {
		// "没有 chunk" 有两种来源，而它们完全不同，不该共用一句话。
		//
		// 一种是版本本身没有文档：正常，上面的推理到此为止。
		//
		// 另一种是版本有文档、却一个 chunk 都没产出——切分没吐出内容，或者每
		// 一次嵌入都失败了却没有让调用方失败。这一种从前也走这里，记一行 Info
		// 就算完，于是集群少一个依赖（实测：mock-embed 没起来）时，写入是半成
		// 功的：版本提交了、数据没落地、索引永远 PENDING，查询被 FailedPrecondition
		// 一直拒，而日志里只有一行看起来无害的 "nothing to build"。把异常那一
		// 种以 Error 记出来，让静默半成功至少不再静默。
		if im.listDocIDs != nil {
			if ids, err := im.listDocIDs(ctx, kbID, versionID); err == nil && len(ids) > 0 {
				im.logger.Error("index: version has documents but produced no chunks; its data never landed",
					zap.String("kb_id", kbID),
					zap.Int64("version_id", versionID),
					zap.Int("documents", len(ids)))
				return 0, nil
			}
		}
		im.logger.Info("index: version has no chunks; nothing to build",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
		return 0, nil
	}

	for i, batch := range batches {
		chunks := make([]*vecstorepb.ChunkVectorProto, 0, len(batch))
		for _, cv := range batch {
			chunks = append(chunks, &vecstorepb.ChunkVectorProto{ChunkId: cv.id, Vector: cv.vec})
		}
		if i == 0 {
			resp, buildErr := im.vectorIndexClient.Build(ctx, &vecstorepb.BuildIndexRequest{
				KbId:      kbID,
				VersionId: versionID,
				Chunks:    chunks,
				Metric:    vecstorepb.MetricTypeProto_COSINE,
				Quantizer: quantizerType,
				PqM:       pqM,
				PqNbits:   pqNBits,
			})
			if buildErr != nil {
				return 0, fmt.Errorf("index: Build RPC: %w", buildErr)
			}
			reportedMemBytes = resp.GetMemBytes()
		} else {
			resp, addErr := im.vectorIndexClient.AddChunks(ctx, &vecstorepb.AddChunksRequest{
				KbId:      kbID,
				VersionId: versionID,
				Chunks:    chunks,
			})
			if addErr != nil {
				return 0, fmt.Errorf("index: AddChunks RPC: %w", addErr)
			}
			reportedMemBytes = resp.GetMemBytes()
		}
	}

	// Memory accounting (Stratum_设计文档v12.md 3.3): for quantized KBs the
	// vecstore reports the index's resident estimate on the coarse-
	// retriever basis (HNSW graph edges + quantized codes); prefer that
	// over the 4×d×n vector-payload estimate (which is only right for
	// full-precision OFF indexes). OFF KBs and unreported quantized
	// builds keep the historical sizeBytes estimate.
	if quantizerType != vecstorepb.QuantizerTypeProto_QUANTIZER_OFF && reportedMemBytes > 0 {
		sizeBytes = reportedMemBytes
	}

	// Persist the finished index to disk — the durable fact that READY
	// status is derived from. A failed save means the build is not
	// durable, so it is reported as a build failure (the caller marks the
	// version FAILED instead of READY).
	return sizeBytes, im.saveToDisk(ctx, kbID, versionID)
}

// saveToDisk persists the just-built index for (kbID, versionID) to
// <IndexDataDir>/index/<kbID>/<versionID>.index (plus the .ids sidecar
// written by the vecstore side). The directory is created here because
// this node and the vecstore process share the filesystem; the vecstore
// side's Save writes both files. Idempotent: a repeat save overwrites.
// With an empty IndexDataDir (unconfigured, as in in-process tests) the
// save is skipped and the index stays in-memory only.
func (im *IndexManagerImpl) saveToDisk(ctx context.Context, kbID string, versionID int64) error {
	if im.cfg.IndexDataDir == "" {
		return nil // persistence not configured
	}
	// The same shard lock InstallIndex and ReadIndexFiles take (installShards).
	// A build and an install of one version are two writers of the same pair of
	// files, and letting them interleave is what leaves an index beside the wrong
	// sidecar. Held across the vecstore Save and the sidecar write, so no reader
	// can observe a half-updated pair. (Neither caller of this function holds
	// im.mu, so the order shard → im.mu stays consistent with InstallIndex.)
	shard := &im.installShards[installShardOf(indexKey{kbID, versionID})]
	shard.Lock()
	defer shard.Unlock()

	path := im.indexPath(kbID, versionID)
	// 0777: this node and the vecstore process share the filesystem but may
	// run as different users (docker 集群形态：节点容器内 root、宿主机
	// vecstore 普通用户)。放宽目录权限让共享的 vecstore 能写入索引文件。
	// MkdirAll 的权限位会被进程 umask(022) 收窄，故创建后显式 Chmod。
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return fmt.Errorf("index: save mkdir: %w", err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		return fmt.Errorf("index: save chmod: %w", err)
	}
	// The INTERMEDIATE level is shared as well. MkdirAll applies its mode only to
	// the leaves it creates, and umask narrows it further, so <IndexDataDir>/index
	// ends up 0755 root — and a vecstore running as another user then cannot create
	// anything under it. Harmless while this node creates every version directory
	// first (which it does), but it contradicts what the mode above promises, and
	// it silently breaks any path where the vecstore side makes a directory itself.
	if err := os.Chmod(filepath.Dir(dir), 0o777); err != nil {
		return fmt.Errorf("index: save chmod parent: %w", err)
	}
	if _, err := im.vectorIndexClient.Save(ctx, &vecstorepb.SaveIndexRequest{
		KbId: kbID, VersionId: versionID, Path: path,
	}); err != nil {
		return fmt.Errorf("index: Save RPC: %w", err)
	}
	return nil
}

// indexPath returns the on-disk path of (kbID, versionID)'s persisted
// index. kbID is generated by the system (a UUID) and never contains path
// separators.
func (im *IndexManagerImpl) indexPath(kbID string, versionID int64) string {
	return filepath.Join(im.cfg.IndexDataDir, "index", kbID, fmt.Sprintf("%d.index", versionID))
}

// IndexExists implements IndexManager: asks the vecstore side whether the
// persisted index files for (kbID, versionID) exist on disk. Stateless on
// the vecstore side, so the answer reflects disk facts even right after a
// vecstore restart. With an empty IndexDataDir (persistence unconfigured)
// it reports false — there is no on-disk index.
func (im *IndexManagerImpl) IndexExists(ctx context.Context, kbID string, versionID int64) (bool, error) {
	if im.cfg.IndexDataDir == "" {
		return false, nil
	}
	resp, err := im.vectorIndexClient.ExistsIndex(ctx, &vecstorepb.ExistsIndexRequest{
		KbId: kbID, VersionId: versionID, Path: im.indexPath(kbID, versionID),
	})
	if err != nil {
		return false, fmt.Errorf("index: ExistsIndex RPC: %w", err)
	}
	return resp.GetExists(), nil
}

// loadFromDisk restores (kbID, versionID)'s index from its persisted file
// via the vecstore Load RPC and marks it loaded (respecting the LRU
// capacity). Safe to call concurrently: if a load/build finished while the
// RPC was in flight, the existing entry wins. A missing file (never
// built, or file deleted) or an empty IndexDataDir (persistence
// unconfigured) returns an error that callers map to ErrIndexNotReady.
func (im *IndexManagerImpl) loadFromDisk(ctx context.Context, kbID string, versionID int64) error {
	if im.cfg.IndexDataDir == "" {
		return fmt.Errorf("index: persistence not configured")
	}
	key := indexKey{kbID, versionID}
	// Refuse to load an index whose KB or version is being deleted (see
	// the tombstone fields on IndexManagerImpl). Checked before the RPC
	// AND after it: a Load that started before the deletion but finished
	// after it must not resurrect the index.
	im.mu.Lock()
	if im.deletedKBs[kbID] || im.deletedVersions[key] {
		im.mu.Unlock()
		return fmt.Errorf("index: %s/%d is deleted", kbID, versionID)
	}
	im.mu.Unlock()

	if _, err := im.vectorIndexClient.Load(ctx, &vecstorepb.LoadIndexRequest{
		KbId: kbID, VersionId: versionID, Path: im.indexPath(kbID, versionID),
	}); err != nil {
		return fmt.Errorf("index: Load RPC: %w", err)
	}

	im.mu.Lock()
	var dropped []indexKey
	// Registered before the unlock defer so it runs after it (defers are LIFO):
	// telling the vecstore is a network call and must not run under im.mu.
	defer func() { im.dropEvicted(dropped) }()
	defer im.mu.Unlock()
	if im.deletedKBs[kbID] || im.deletedVersions[key] {
		return fmt.Errorf("index: %s/%d was deleted while loading", kbID, versionID)
	}
	if _, ok := im.loaded[key]; ok {
		return nil // a concurrent load/build already brought it in
	}
	dropped = im.makeRoomLocked()
	im.loaded[key] = &loadedIndex{lastAccess: time.Now()}
	// The file we just read decides the shape; the sidecar beside it is the
	// authority (§8.6a). A remembered entry could be stale — the artifact may
	// have been replaced since this process recorded one — so it is dropped and
	// re-derived on demand.
	delete(im.builtGraphFree, key)
	size := im.readSizeSidecar(kbID, versionID)
	im.sizeByKey[key] = size
	im.loadedBytes += size
	return nil
}

// maxBuildMessageBytes 是单次 Build/AddChunks RPC 载荷的字节预算上限。
// gRPC 默认最大消息 4 MiB（4194304 字节）；预算取一半，给 chunk_id 与
// protobuf 序列化开销留出余量。预算按 chunk 粒度切分，若单个 chunk 的向量
// 本身已超过预算，该批会如实超限（现实中 embedding 维度远达不到该量级）。
const maxBuildMessageBytes = 2 * 1024 * 1024

// chunkVec 是单个 chunk 的向量载荷（chunk_id + 向量）。
type chunkVec struct {
	id  string
	vec []float32
}

// appendBase reports whether versionID's index can be built by extending its
// parent version's artifact rather than rebuilding from scratch (§8.6c "pure
// append"), and if so returns the parent's id and the chunks that are new.
//
// The reuse needs all of:
//   - a parent version (the version chain is linear, §6);
//   - that parent's artifact still present on this node's disk;
//   - this node knowing the parent's shape, and it being the shape this build
//     wants — extending a graph-free artifact into a graphed index would have
//     to rebuild anyway (and vice versa), and right after a restart nothing is
//     known, so the conservative answer there is "no";
//   - at least one chunk to append (with none, reuse is a copy, not a saving).
//
// Deletions do NOT disqualify the reuse: HNSW cannot remove vectors, so a
// version that deletes documents leaves dead vectors in the base — but they
// are filtered out of results by the read path (the chunk→doc mapping is
// cleared for a deleted document, and the version's doc list confirms it), so
// they cost memory and candidate slots rather than correctness. How much of
// the base is dead is only known after the base is loaded (its `ntotal`),
// which is why buildFromBase checks the ratio and may still fall back.
func (im *IndexManagerImpl) appendBase(
	ctx context.Context, kbID string, versionID int64, graphFree bool, chunkIDs []string,
) (parentID int64, delta []string, dead []string, ok bool) {
	if im.versionParent == nil || im.cfg.IndexDataDir == "" || len(chunkIDs) == 0 {
		return 0, nil, nil, false
	}
	parent, err := im.versionParent(ctx, kbID, versionID)
	if err != nil || parent <= 0 || parent == versionID {
		return 0, nil, nil, false
	}
	// The artifact must be on this node. The files are the fact; the
	// ExistsIndex RPC is not used as the criterion (some vecstore builds
	// answer Unimplemented for it).
	if !fileExists(im.indexPath(kbID, parent)) || !fileExists(im.sidecarPath(kbID, parent)) {
		return 0, nil, nil, false
	}
	// The base's shape must be known here and match what this build wants.
	//
	// Through shapeGraphFree, not the in-memory record alone: a parent this node
	// received by §8.4 distribution has no builtGraphFree entry, and reading that
	// as "unknown shape" is what made every replica but the parent's builder
	// rebuild each child from scratch — the reuse existed on paper (the shape line
	// travels in the sidecar) but never triggered there. A shape that still cannot
	// be read (a sidecar written before §8.6a) stays unknown, and this returns
	// false: the old conservative rebuild.
	parentGraphFree, known := im.shapeGraphFree(kbID, parent)
	if !known || parentGraphFree != graphFree {
		im.buildLogger().Debug("index: §8.6c reuse skipped — the parent's shape is not usable here",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.Int64("parent_version_id", parent),
			zap.Bool("parent_graph_free", parentGraphFree), zap.Bool("parent_shape_known", known),
			zap.Bool("wanted_graph_free", graphFree))
		return 0, nil, nil, false
	}

	parentDocIDs, err := im.listDocIDs(ctx, kbID, parent)
	if err != nil {
		return 0, nil, nil, false
	}
	parentChunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, parentDocIDs)
	if err != nil || len(parentChunkIDs) == 0 {
		return 0, nil, nil, false
	}
	present := make(map[string]bool, len(chunkIDs))
	for _, id := range chunkIDs {
		present[id] = true
	}
	parentSet := make(map[string]bool, len(parentChunkIDs))
	for _, id := range parentChunkIDs {
		parentSet[id] = true
	}
	delta = make([]string, 0, len(chunkIDs))
	for _, id := range chunkIDs {
		if !parentSet[id] {
			delta = append(delta, id)
		}
	}
	if len(delta) == 0 {
		// Nothing new: the parent's artifact already describes this version's
		// chunk set (or is a superset of it), so reusing it would be a copy at
		// best — and if this version deleted anything, the dead vectors would
		// come along for nothing. Rebuild from scratch.
		return 0, nil, nil, false
	}
	// Chunks the parent holds and this version no longer does: the deletions of
	// this step. A graph-free base can drop their vectors outright (§8.6c's
	// RemoveChunks); a graphed one has to carry them as tombstones until the
	// dead-vector ratio forces a rebuild.
	dead = make([]string, 0)
	for _, id := range parentChunkIDs {
		if !present[id] {
			dead = append(dead, id)
		}
	}
	return parent, delta, dead, true
}

// buildFromBase extends the parent's artifact with delta and seals it for
// (kbID, versionID) (§8.6c). The outcome is an ordinary artifact of this
// version: Load restores the retrieval mode from the stored type, so the
// base's shape carries over unchanged — which is exactly why appendBase only
// reuses a base whose shape matches the build's target.
func (im *IndexManagerImpl) buildFromBase(
	ctx context.Context, kbID string, versionID, parentID int64, delta, dead []string,
	totalChunks int, graphFree bool,
) (int64, error) {
	resp, err := im.vectorIndexClient.LoadForAppend(ctx, &vecstorepb.LoadIndexForAppendRequest{
		KbId: kbID, VersionId: versionID, Path: im.indexPath(kbID, parentID),
	})
	if err != nil {
		return 0, fmt.Errorf("index: LoadForAppend RPC: %w", err)
	}
	baseNtotal := resp.GetBaseNtotal()

	// §8.6(c) deletions, cheap path: a graph-free base can drop the vectors
	// this version no longer needs, so they never become tombstones at all
	// (faiss compacts IndexFlatCodes; a graphed index cannot remove). The
	// build is still open here — Save is what seals it — which is exactly the
	// window RemoveChunks requires.
	if graphFree && len(dead) > 0 {
		removed, removeErr := im.removeDeadChunks(ctx, kbID, versionID, dead)
		if removeErr != nil {
			return 0, fmt.Errorf("index: RemoveChunks RPC (append reuse): %w", removeErr)
		}
		baseNtotal -= removed
	}

	// How much of the base this version still no longer needs, now that the
	// deletions we can identify are dropped. The base holds baseNtotal vectors
	// and this version needs len(delta) of them to be new; everything else is
	// either still in use or dead (a document deleted by an ancestor and
	// carried along by an earlier reuse). |base ∩ thisVersion| <=
	// totalChunks-len(delta), so this is an upper bound on the dead weight —
	// erring towards rebuilding, the safe direction: a rebuild is slower, a
	// bloated index is wrong for longer.
	deadWeight := baseNtotal - int64(totalChunks-len(delta))
	if deadWeight < 0 {
		deadWeight = 0
	}
	if limit := im.appendMaxDeadRatio(); limit < 1.0 {
		total := baseNtotal + int64(len(delta))
		if total > 0 && float64(deadWeight)/float64(total) > limit {
			return 0, fmt.Errorf("%w: %d of %d vectors are dead, ratio limit is %.2f",
				errAppendTooManyTombstones, deadWeight, total, limit)
		}
	}

	batches, deltaBytes, err := im.collectChunkBatches(ctx, kbID, delta)
	if err != nil {
		return 0, err
	}
	reportedMemBytes := int64(0)
	for _, batch := range batches {
		chunks := make([]*vecstorepb.ChunkVectorProto, 0, len(batch))
		for _, cv := range batch {
			chunks = append(chunks, &vecstorepb.ChunkVectorProto{ChunkId: cv.id, Vector: cv.vec})
		}
		addResp, addErr := im.vectorIndexClient.AddChunks(ctx, &vecstorepb.AddChunksRequest{
			KbId: kbID, VersionId: versionID, Chunks: chunks,
		})
		if addErr != nil {
			return 0, fmt.Errorf("index: AddChunks RPC (append reuse): %w", addErr)
		}
		reportedMemBytes = addResp.GetMemBytes()
	}

	if err := im.saveToDisk(ctx, kbID, versionID); err != nil {
		return 0, err
	}
	// Memory accounting: prefer what the vecstore reports for the whole
	// resident structure (base + delta); otherwise the parent's recorded
	// footprint plus the delta payload.
	if reportedMemBytes > 0 {
		return reportedMemBytes, nil
	}
	return im.readSizeSidecar(kbID, parentID) + deltaBytes, nil
}

// errAppendTooManyTombstones reports that pure-append reuse is legal but not
// worth it: too much of the base artifact is dead weight for the version being
// built, so a full rebuild (which drops the dead vectors) is the better trade.
var errAppendTooManyTombstones = errors.New("index: append reuse skipped, too many tombstoned vectors")

// appendMaxDeadRatio is the dead-vector share above which appendBase's reuse is
// abandoned in favour of a full rebuild. <= 0 means DefaultAppendMaxDeadRatio;
// 1.0 disables the check (always reuse when the reuse is otherwise legal).
func (im *IndexManagerImpl) appendMaxDeadRatio() float64 {
	if im.cfg.AppendMaxDeadRatio <= 0 {
		return DefaultAppendMaxDeadRatio
	}
	return im.cfg.AppendMaxDeadRatio
}

// codebookDriftTrigger / codebookAppendsTrigger resolve the two §3 triggers that
// retire a stale codebook (docs/codebook-refresh-plan.md). A NEGATIVE config
// value disables that trigger; 0 takes the default.
//
// 1.0 does NOT disable the ratio trigger, unlike AppendMaxDeadRatio's 1.0: a
// dead-vector share cannot exceed 1, but a growth ratio can (a codebook trained
// on N vectors can face 2N), so 1.0 merely means "the corpus must double". Use a
// negative value to switch a trigger off.
func (im *IndexManagerImpl) codebookDriftTrigger() (limit float64, enabled bool) {
	switch {
	case im.cfg.MaxCodebookDriftRatio < 0:
		return 0, false
	case im.cfg.MaxCodebookDriftRatio == 0:
		return DefaultMaxCodebookDriftRatio, true
	default:
		return im.cfg.MaxCodebookDriftRatio, true
	}
}

func (im *IndexManagerImpl) codebookAppendsTrigger() (limit int64, enabled bool) {
	switch {
	case im.cfg.MaxCodebookAppends < 0:
		return 0, false
	case im.cfg.MaxCodebookAppends == 0:
		return DefaultMaxCodebookAppends, true
	default:
		return im.cfg.MaxCodebookAppends, true
	}
}

// minCodebookBaselineVectors is the baseline size below which the cumulative
// ratio is not consulted (see DefaultMinCodebookBaselineVectors). <= 0 takes the
// default; a negative value removes the floor.
func (im *IndexManagerImpl) minCodebookBaselineVectors() int64 {
	if im.cfg.MinCodebookBaselineVectors < 0 {
		return 0
	}
	if im.cfg.MinCodebookBaselineVectors == 0 {
		return DefaultMinCodebookBaselineVectors
	}
	return im.cfg.MinCodebookBaselineVectors
}

// quantizerNeedsTraining reports whether this shape keeps a TRAINED codebook.
// Only those can drift: SQ8 learns a per-dimension range and PQ learns k-means
// centroids, while SQ_FP16 / SQ_BF16 are pure bit truncation (already trained at
// construction) and OFF has no codebook at all.
//
// This gate is what keeps the mechanism off every default deployment: OFF is the
// default quantizer, so an ungated trigger would buy a periodic full rebuild for
// exactly zero benefit. The drift benchmark measures the untrained shapes'
// codebook-drift cost as zero, which is why excluding them costs nothing.
func quantizerNeedsTraining(q vecstorepb.QuantizerTypeProto) bool {
	switch q {
	case vecstorepb.QuantizerTypeProto_QUANTIZER_SQ8,
		vecstorepb.QuantizerTypeProto_QUANTIZER_SQ8_FLAT,
		vecstorepb.QuantizerTypeProto_QUANTIZER_PQ,
		vecstorepb.QuantizerTypeProto_QUANTIZER_PQ_FLAT:
		return true
	}
	return false
}

// codebookRefreshNeeded decides whether this build should give up append reuse
// and rebuild from scratch — the only path that retrains the quantizer
// (docs/codebook-refresh-plan.md §3).
//
// Two triggers, either suffices:
//   - cumulative new-vector share: (totalChunks - trainedNtotal) / trainedNtotal
//   - append-reuses since training: the fallback, which also covers a KB that
//     adds a few vectors per version (the share never reaches the threshold but
//     the codebook still ages)
//
// The baseline is read from the PARENT version's sealed sidecar, where it rides
// along with the artifact — so it survives a restart and a §8.4 handoff with no
// Go-side state. Three cases return true with an explicit reason, and all three
// resolve after a single rebuild (which writes a baseline):
//   - the parent's sidecar has no baseline line (artifact from before §3)
//   - the parent's sidecar cannot be read
//   - the baseline is zero, which is meaningless as a denominator
//
// cumulativeRatio is returned even when no refresh is needed, so the caller can
// log how close a build is to the threshold.
func (im *IndexManagerImpl) codebookRefreshNeeded(
	ctx context.Context, kbID string, versionID int64,
	quantizerType vecstorepb.QuantizerTypeProto, totalChunks int,
) (needed bool, reason string, cumulativeRatio float64) {
	if !quantizerNeedsTraining(quantizerType) || im.versionParent == nil {
		return false, "", 0
	}
	parent, err := im.versionParent(ctx, kbID, versionID)
	if err != nil || parent <= 0 || parent == versionID {
		// No parent (or none can be looked up): this build trains a fresh
		// codebook anyway, so there is nothing stale to refresh.
		return false, "", 0
	}
	// The parent's artifact must be on THIS node. When it is not — ordinary in a
	// multi-node deployment, where a node may never have built the parent — that
	// is not a lost baseline but the case appendBase already handles by giving up
	// the reuse. Reporting "baseline unknown" here would send an operator looking
	// for a damaged artifact when the real reason is just "the artifact is not
	// here", so let appendBase do the attributing.
	if !fileExists(im.indexPath(kbID, parent)) || !fileExists(im.sidecarPath(kbID, parent)) {
		return false, "", 0
	}
	trainedNtotal, appends, known := im.artifactTrainedBaseline(kbID, parent)
	if !known || trainedNtotal <= 0 {
		return true, "baseline unknown", 0
	}
	if limit, enabled := im.codebookAppendsTrigger(); enabled && appends >= limit {
		return true, "appends since training", 0
	}
	ratio := (float64(totalChunks) - float64(trainedNtotal)) / float64(trainedNtotal)
	if floor := im.minCodebookBaselineVectors(); trainedNtotal < floor {
		// Baseline too small for the ratio to mean anything (see
		// minCodebookBaselineVectors): report the ratio, decide nothing.
		return false, "", ratio
	}
	if limit, enabled := im.codebookDriftTrigger(); enabled && ratio >= limit {
		return true, "cumulative drift ratio", ratio
	}
	return false, "", ratio
}

// removeDeadChunks asks the vecstore to drop these chunks' vectors and reports
// how many it actually removed. Only a graph-free index supports it (faiss
// compacts IndexFlatCodes but cannot repair an HNSW graph) and only while the
// build is open — the vecstore answers FailedPrecondition otherwise, which the
// caller turns into a fallback to a full rebuild.
func (im *IndexManagerImpl) removeDeadChunks(
	ctx context.Context, kbID string, versionID int64, dead []string,
) (int64, error) {
	resp, err := im.vectorIndexClient.RemoveChunks(ctx, &vecstorepb.RemoveChunksRequest{
		KbId: kbID, VersionId: versionID, ChunkIds: dead,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetRemoved(), nil
}

// fileExists reports whether path names an existing regular file.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

// collectChunkBatches 逐个读取 chunk 向量，并按估算字节数切分成多个批次，
// 使得每批序列化后都不会超过 maxBuildMessageBytes。同时返回所有 chunk 向量
// 载荷的总字节数（4 × 维度 × chunk 数），作为该版本索引内存占用的估算。
func (im *IndexManagerImpl) collectChunkBatches(ctx context.Context, kbID string, chunkIDs []string) ([][]chunkVec, int64, error) {
	var batches [][]chunkVec
	var cur []chunkVec
	curBytes := 0
	var sizeBytes int64
	for _, chunkID := range chunkIDs {
		v, err := im.readChunkVector(ctx, kbID, chunkID)
		if err != nil {
			return nil, 0, chunkVectorReadErr(chunkID, err)
		}
		// 估算该 chunk 在请求中的字节开销：向量(float32) + chunk_id + 字段头。
		est := 4*len(v) + len(chunkID) + 64
		if len(cur) > 0 && curBytes+est > maxBuildMessageBytes {
			batches = append(batches, cur)
			cur = nil
			curBytes = 0
		}
		cur = append(cur, chunkVec{id: chunkID, vec: v})
		curBytes += est
		sizeBytes += int64(4 * len(v))
	}
	if len(cur) > 0 {
		batches = append(batches, cur)
	}
	return batches, sizeBytes, nil
}

// acquire loads the index for key if not already in memory, blocking
// if a concurrent load is in progress, bounded by loadWaitTimeout and
// ctx.Done(). Returns nil once the index is loaded and ref-counted.
func (im *IndexManagerImpl) acquire(ctx context.Context, key indexKey) error {
	deadline := time.Now().Add(im.cfg.LoadWaitTimeout)

	im.mu.Lock()
	defer im.mu.Unlock()

	for {
		if idx, ok := im.loaded[key]; ok {
			idx.refCount++
			idx.lastAccess = time.Now()
			return nil
		}
		if im.loading[key] {
			// A build/load is in progress for this key; wait.
			if !im.waitLocked(ctx, deadline) {
				return im.waitTimeoutErr(ctx)
			}
			continue
		}
		// Not loaded and not being built.
		return fmt.Errorf("index: no built index for kbID=%s versionID=%d: %w", key.kbID, key.versionID, stratumerrors.ErrIndexNotReady)
	}
}

// waitLocked blocks until the loading condition is re-checkable, ctx is
// done, or deadline passes, returning false in the latter two cases. Must
// be called with im.mu held.
//
// Deliberately NOT implemented with cond.Wait: a lost wakeup (e.g. the
// build goroutine dies between setting loading and broadcasting) would
// park the caller forever — exactly the pressure-test Query hang where a
// goroutine sat in sync.Cond.Wait for minutes. Polling with a bounded
// deadline is slightly less efficient but cannot wedge.
func (im *IndexManagerImpl) waitLocked(ctx context.Context, deadline time.Time) bool {
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return false
		}
		// Drop the lock briefly so the build goroutine (which needs
		// im.mu to clear loading) can make progress, then re-check.
		im.mu.Unlock()
		select {
		case <-ctx.Done():
			im.mu.Lock()
			return false
		case <-time.After(50 * time.Millisecond):
		}
		im.mu.Lock()
	}
	return ctx.Err() == nil && time.Now().Before(deadline)
}

func (im *IndexManagerImpl) waitTimeoutErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return stratumerrors.ErrIndexLoadTimeout
}

func (im *IndexManagerImpl) release(key indexKey) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if idx, ok := im.loaded[key]; ok && idx.refCount > 0 {
		idx.refCount--
		im.cond.Broadcast()
	}
}

// recordSearch stamps key's most recent query time (§8.6a). Called at
// the top of Search, before any load/build/brute-force decision, so a
// version counts as "asked for" even when the request is answered by a
// scan or fails afterwards. Keys deleted by Discard/DeleteFilesByKB are
// revived only by a later search, which is the intended semantic.
func (im *IndexManagerImpl) recordSearch(key indexKey) {
	im.recordAccess(key)
}

// RecordInterest implements IndexManager. An operator asked for this version's
// index explicitly (RebuildIndex / WarmupVersion), which is the same evidence a
// query leaves — "this version is wanted here, now" — so it is recorded the
// same way and the retention policy shields it for the same window.
//
// Without it, the artifact such a request builds is protected only for the
// instant it completes: the post-build retention pass is handed the id being
// built, but the next build, or the next restart, has no reason to keep it — and
// an operator's explicit request then quietly expires.
func (im *IndexManagerImpl) RecordInterest(kbID string, versionID int64) {
	im.recordAccess(indexKey{kbID, versionID})
}

// recordAccess is where "something asked for this version" lands: the in-memory
// access table the cold evaluator reads, and — throttled — the on-disk .used
// sidecar the retention policy reads.
func (im *IndexManagerImpl) recordAccess(key indexKey) {
	now := time.Now()
	im.mu.Lock()
	im.lastSearch[key] = now
	// Persist the access time, throttled. This on-disk record is what lets the
	// retention policy shield a recently-queried version across a restart; a
	// write on every query would put a small file write on the hot path, while
	// the protection window is measured in hours, so a minute of granularity
	// costs nothing.
	persist := false
	if im.retentionProtectWindow() > 0 && im.cfg.IndexDataDir != "" {
		if last, ok := im.lastPersist[key]; !ok || now.Sub(last) >= accessPersistInterval {
			im.lastPersist[key] = now
			persist = true
		}
	}
	im.mu.Unlock()
	if persist {
		// Outside the lock: it is a file write, and Search must not wait on it.
		im.persistAccessTime(key.kbID, key.versionID, now)
	}
}

// recordInterestNow records "this version is wanted here" and writes the on-disk
// .used shield immediately, bypassing recordAccess's throttle.
//
// The build-complete and install paths use it. The artifact they just produced
// or received is exactly the one a lagging replica is about to fetch (§8.4), and
// the retention pass that would drop it runs on every later build — so the shield
// has to be on disk before the next one, not up to a minute later. It maintains
// only lastPersist (the throttle's own bookkeeping); lastSearch is the cold
// evaluator's baseline and seedAccessLocked already set it, where a real query
// outranks a build as evidence.
func (im *IndexManagerImpl) recordInterestNow(kbID string, versionID int64) {
	if im.retentionProtectWindow() <= 0 || im.cfg.IndexDataDir == "" {
		return
	}
	now := time.Now()
	key := indexKey{kbID, versionID}
	im.mu.Lock()
	im.lastPersist[key] = now
	im.mu.Unlock()
	// Outside the lock: it is a file write, and a build or install must not wait
	// on it.
	im.persistAccessTime(kbID, versionID, now)
}

// LastAccess reports when (kbID, versionID) was last searched (or, if it
// was never searched here, when its index was last built), and whether
// this process knows the version at all. It is the fact the cold-version
// policy reads; false means "neither searched nor built here", in which
// case the evaluator never sees the version and leaves it alone.
func (im *IndexManagerImpl) LastAccess(kbID string, versionID int64) (time.Time, bool) {
	im.mu.Lock()
	defer im.mu.Unlock()
	t, ok := im.lastSearch[indexKey{kbID, versionID}]
	return t, ok
}

// seedAccessLocked gives the cold policy a baseline for key when it has
// none. A version this node just built or just received counts as "known
// here from now on": without a record the version would be invisible to
// the evaluator (§8.6a enumerates the access table, not the Raft state
// machine), so a version nobody ever queries could never age into cold.
// An existing record is left alone — a real query is better evidence than
// a build, and re-seeding on every build would keep a version that is
// being rebuilt for other reasons permanently hot. Must be called with
// im.mu held.
func (im *IndexManagerImpl) seedAccessLocked(key indexKey) {
	if _, ok := im.lastSearch[key]; !ok {
		im.lastSearch[key] = time.Now()
	}
}

// forgetSearch drops key's access record. Must be called with im.mu held.
func (im *IndexManagerImpl) forgetSearchLocked(key indexKey) {
	delete(im.lastSearch, key)
}

// forgetVersionLocked drops everything the §8.6a policy remembers about
// key: its access record and the shape of its index. Used when the
// version itself goes away (Discard); eviction is not a reason to forget
// either fact. Must be called with im.mu held.
func (im *IndexManagerImpl) forgetVersionLocked(key indexKey) {
	im.forgetSearchLocked(key)
	delete(im.builtGraphFree, key)
	delete(im.lastPersist, key)
}

// makeRoomLocked evicts least-recently-used, ref-count-zero indexes until
// there is room for one more entry: len(loaded) < LRUCapacity AND (if
// MemoryThresholdMB is set) loadedBytes <= threshold. If no evictable
// index remains (everything is pinned), it stops and returns what it has. Must
// be called with im.mu held; called BEFORE inserting the new entry so the
// brand-new index (refCount 0, nothing has acquired it yet) is never
// itself chosen as the eviction candidate.
//
// It RETURNS the keys it dropped instead of reclaiming them itself: the vecstore
// holds its own copy of every resident index, and the call that releases it is a
// network round trip, so it belongs outside im.mu (see dropEvicted).
func (im *IndexManagerImpl) makeRoomLocked() []indexKey {
	var threshold int64
	if im.cfg.MemoryThresholdMB > 0 {
		threshold = im.cfg.MemoryThresholdMB << 20 // MiB → bytes
	}
	var evicted []indexKey
	for (im.cfg.LRUCapacity > 0 && len(im.loaded) >= im.cfg.LRUCapacity) ||
		(threshold > 0 && im.loadedBytes > threshold) {
		var oldestKey indexKey
		var oldestTime time.Time
		found := false
		for k, idx := range im.loaded {
			if idx.refCount != 0 {
				continue
			}
			if !found || idx.lastAccess.Before(oldestTime) {
				oldestKey = k
				oldestTime = idx.lastAccess
				found = true
			}
		}
		if !found {
			// Everything resident is pinned by a caller in flight, so the overage
			// cannot be corrected right now. Say so, once in a while: the state is
			// the one M8 of docs/code-review-2026-09-24.md describes — the bound is
			// exceeded and looks enforced — and the fix for "silent" is a line, not
			// a refusal (the overage is transient by construction: the pins belong
			// to searches that are about to finish).
			im.warnOnPinnedOverflowLocked(threshold)
			return evicted
		}
		delete(im.loaded, oldestKey)
		evicted = append(evicted, oldestKey)
		if size, ok := im.sizeByKey[oldestKey]; ok {
			im.loadedBytes -= size
			delete(im.sizeByKey, oldestKey)
		}
	}
	return evicted
}

// warnOnPinnedOverflowLocked records "over capacity and nothing is evictable",
// rate-limited to one line per minute.
//
// Must be called with im.mu held. The numbers are the diagnosis: how far over the
// bound the manager is (loaded vs LRUCapacity, loadedBytes vs threshold) and how
// many of the resident indexes are the reason nothing could go (pinned). Without
// them an operator sees memory climb past a configured ceiling with nothing in the
// log to say the ceiling was reached rather than misconfigured.
func (im *IndexManagerImpl) warnOnPinnedOverflowLocked(threshold int64) {
	now := time.Now()
	if !im.lastPinWarn.IsZero() && now.Sub(im.lastPinWarn) < pinnedOverflowWarnInterval {
		return
	}
	im.lastPinWarn = now

	pinned := 0
	for _, idx := range im.loaded {
		if idx.refCount != 0 {
			pinned++
		}
	}
	im.logger.Warn("index: over capacity and nothing is evictable; the overage lasts until the in-flight searches release",
		zap.Int("loaded", len(im.loaded)),
		zap.Int("lru_capacity", im.cfg.LRUCapacity),
		zap.Int("pinned", pinned),
		zap.Int64("loaded_bytes", im.loadedBytes),
		zap.Int64("memory_threshold_bytes", threshold))
}

// pinnedOverflowWarnInterval is how often the pinned-overflow state may be logged.
// Long enough that a burst of concurrent searches produces one line rather than
// hundreds, short enough that a manager stuck over its bound keeps saying so.
const pinnedOverflowWarnInterval = time.Minute

// dropEvicted tells the vecstore to release the indexes this node has stopped
// tracking.
//
// H5 of docs/code-review-2026-09-24.md: the vecstore's index map only ever grew,
// because the Go side's view (loaded / sizeByKey) was the only one being pruned
// and a pruned version's object stayed resident for the life of the process — so
// RSS tracked how many versions had been touched, not how many were held.
//
// Best-effort by construction. What a failure costs is memory, not correctness:
// the version is already gone from this node's view, its artifact is still on
// disk, and a later Load rebuilds the object. So a failure is logged and never
// propagated — failing the caller's build/eviction over a leak would trade a
// slow leak for an outage. The RPC is idempotent, so the next eviction of the
// same key retries harmlessly.
//
// Eviction and dropping are not atomic with respect to a concurrent load, and
// that gap is the whole reason for the two checks below. A version evicted here
// can be loaded again — by a query, or by the catch-up that follows a snapshot
// install — before this call runs, and dropping it then leaves this node
// believing it holds an index the vecstore no longer has. The symptom is not an
// error at the point of the bug: the node's own state says READY and the query
// answers "index not ready", which is exactly what
// TestRealStack_ThreeNodeCluster_SnapshotPipeline caught.
func (im *IndexManagerImpl) dropEvicted(keys []indexKey) {
	if len(keys) == 0 || im.vectorIndexClient == nil {
		return
	}
	// Its own bounded context: this runs off the back of a build or an eviction,
	// whose own deadlines may already be gone, and it must not be able to hang
	// that worker.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, key := range keys {
		if im.IsLoaded(key.kbID, key.versionID) {
			// Loaded again since the eviction: that object is the one this node is
			// using, so it is not garbage.
			continue
		}
		if _, err := im.vectorIndexClient.Drop(ctx, &vecstorepb.DropIndexRequest{
			KbId:      key.kbID,
			VersionId: key.versionID,
		}); err != nil {
			im.logger.Warn("index: vecstore Drop failed; its memory for this version stays resident",
				zap.String("kb_id", key.kbID), zap.Int64("version_id", key.versionID), zap.Error(err))
			continue
		}
		// The load may also have raced the other way: its Load RPC reached the
		// vecstore BEFORE this Drop, so the object it created is the one just
		// deleted. Load is idempotent and the artifact is on disk, so asking again
		// is what makes the two sides agree.
		if im.IsLoaded(key.kbID, key.versionID) {
			im.reloadAfterDrop(ctx, key)
		}
	}
}

// reloadAfterDrop re-materialises the vecstore-side object for a version that was
// loaded again while its Drop was in flight (see dropEvicted). Best-effort: the
// next query's load path retries it anyway, and the alternative — leaving the
// node serving from an object the vecstore has dropped — is worse.
func (im *IndexManagerImpl) reloadAfterDrop(ctx context.Context, key indexKey) {
	if im.cfg.IndexDataDir == "" {
		return
	}
	if _, err := im.vectorIndexClient.Load(ctx, &vecstorepb.LoadIndexRequest{
		KbId:      key.kbID,
		VersionId: key.versionID,
		Path:      im.indexPath(key.kbID, key.versionID),
	}); err != nil {
		im.logger.Warn("index: could not reload an index a concurrent load re-added during a Drop",
			zap.String("kb_id", key.kbID), zap.Int64("version_id", key.versionID), zap.Error(err))
	}
}

// invokeCallback calls cb with exponential backoff retry.
func (im *IndexManagerImpl) invokeCallback(cb BuildCompleteCallback, kbID string, versionID int64, status types.IndexStatus) {
	base := time.Duration(im.cfg.CallbackRetryBaseMS) * time.Millisecond
	if base <= 0 {
		base = 200 * time.Millisecond
	}
	maxRetries := im.cfg.CallbackMaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	// 记下最后一次的 err 并报出去。原实现把 err 的作用域关在 if 里，于是失败时
	// 只留下"重试耗尽"——而**为什么**耗尽正是唯一的诊断信息。定位这个问题时，
	// 就是因为这一行为空，才只能一路读代码去猜失败落在了哪一环。
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := cb(kbID, versionID, status)
		if err == nil {
			return
		}
		// 版本已经不在了——被 `DiscardVersion` 放弃，或被 `DeleteVersion` 删掉，
		// 而索引构建这时可能才刚跑完。它的上报撞上 "version not found" 是**预期
		// 结果**，不是失败：放弃一个版本的语义本来就是"当它没存在过"，那么之后
		// 为它上报索引就绪自然无处可报。
		//
		// 不识别这一条，每次 discard 都会留下 4 次退避重试（200+400+800ms）和一
		// 条 error 级日志——把一次正常的放弃渲染成看起来像故障的东西，而真正的
		// 故障会淹在这片噪音里。
		if errors.Is(err, stratumerrors.ErrVersionNotFound) {
			im.logger.Debug("build callback: version is gone (discarded or deleted); nothing to report",
				zap.String("kb_id", kbID),
				zap.Int64("version_id", versionID),
				zap.String("status", status.String()),
			)
			return
		}
		lastErr = err
		if attempt < maxRetries {
			backoff := base * time.Duration(int64(math.Pow(2, float64(attempt))))
			time.Sleep(backoff)
		}
	}
	im.logger.Error("build callback retries exhausted",
		zap.String("kb_id", kbID),
		zap.Int64("version_id", versionID),
		zap.String("status", status.String()),
		zap.Int("attempts", maxRetries+1),
		zap.Error(lastErr),
	)
}

// RegisterBuildCallback implements IndexManager.
func (im *IndexManagerImpl) RegisterBuildCallback(cb BuildCompleteCallback) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.callbacks = append(im.callbacks, cb)
}

// Evict implements IndexManager.
func (im *IndexManagerImpl) Evict(_ context.Context, kbID string, versionID int64) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	key := indexKey{kbID, versionID}
	delete(im.loaded, key)
	if size, ok := im.sizeByKey[key]; ok {
		im.loadedBytes -= size
		delete(im.sizeByKey, key)
	}
	return nil
}

// EvictByKB implements IndexManager.
func (im *IndexManagerImpl) EvictByKB(_ context.Context, kbID string) error {
	im.mu.Lock()
	var dropped []indexKey
	for k := range im.loaded {
		if k.kbID == kbID {
			delete(im.loaded, k)
			dropped = append(dropped, k)
			if size, ok := im.sizeByKey[k]; ok {
				im.loadedBytes -= size
				delete(im.sizeByKey, k)
			}
		}
	}
	im.mu.Unlock()
	// The vecstore keeps its own object per resident index, so pruning only this
	// node's view left the whole knowledge base resident in the vecstore process
	// for the rest of its life (H5 of docs/code-review-2026-09-24.md). Done after
	// the unlock, because it is a network call.
	im.dropEvicted(dropped)
	return nil
}

// checkKBDirName refuses a knowledge base id that is not a single, ordinary
// path component. Callers use it before turning kbID into a directory that is
// then created, walked or REMOVED.
//
// The control layer mints these ids — a random, opaque handle (see
// service.generateKBID), never the knowledge base's display name — so an id
// carrying a separator or ".." should be impossible. This is the second line of
// defence at the point where the consequence is irreversible: filepath.Join
// cleans "..", so `filepath.Join(IndexDataDir, "index", kbID)` with an
// unvalidated id names a directory OUTSIDE IndexDataDir, and the RemoveAll
// built from it deletes whatever the caller chose.
func checkKBDirName(kbID string) error {
	if kbID == "" || kbID == "." || kbID == ".." || filepath.Base(kbID) != kbID {
		return fmt.Errorf("index: refused kb_id %q: must be a single path component", kbID)
	}
	return nil
}

// DeleteFilesByKB implements IndexManager: removes kbID's on-disk index
// directory (<IndexDataDir>/index/<kbID>/) entirely — the Faiss file, the
// .ids sidecar, and any size sidecars. A missing directory is not an
// error (idempotent re-run after a crash). It also drops every in-memory
// entry for the KB (idempotent with EvictByKB) and sets a KB tombstone so
// an in-flight Search-triggered Load RPC cannot resurrect the index after
// the deletion. No-op when disk persistence is unconfigured.
func (im *IndexManagerImpl) DeleteFilesByKB(_ context.Context, kbID string) error {
	if err := checkKBDirName(kbID); err != nil {
		return err
	}
	im.mu.Lock()
	im.deletedKBs[kbID] = true
	for k := range im.loaded {
		if k.kbID == kbID {
			delete(im.loaded, k)
			if size, ok := im.sizeByKey[k]; ok {
				im.loadedBytes -= size
				delete(im.sizeByKey, k)
			}
		}
	}
	for k := range im.lastSearch {
		if k.kbID == kbID {
			im.forgetSearchLocked(k)
		}
	}
	for k := range im.builtGraphFree {
		if k.kbID == kbID {
			delete(im.builtGraphFree, k)
		}
	}
	im.mu.Unlock()

	if im.cfg.IndexDataDir == "" {
		return nil
	}
	dir := filepath.Join(im.cfg.IndexDataDir, "index", kbID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("index: DeleteFilesByKB(%s): %w", kbID, err)
	}
	return nil
}

// EnforceDiskRetention implements the per-KB on-disk retention policy
// (Stratum_设计文档v10.md "磁盘保留策略"): keeps the most recent
// IndexRetentionCount index files per knowledge base and deletes older
// ones (plus their .ids / size sidecars). protectedIDs are never
// deleted — used to shield the active version at startup. Missing files
// and missing directories are ignored (idempotent). No-op when retention
// is unconfigured (IndexRetentionCount <= 0) or disk persistence is off.
//
// Deleting an index file does not affect a loaded in-memory index; a
// later query against an evicted, retention-dropped version reports
// ErrIndexNotReady and can be recovered via RebuildIndex ("需要时重建").
func (im *IndexManagerImpl) EnforceDiskRetention(_ context.Context, kbID string, protectedIDs []int64) error {
	if im.cfg.IndexRetentionCount <= 0 || im.cfg.IndexDataDir == "" {
		return nil
	}
	if err := checkKBDirName(kbID); err != nil {
		return err
	}
	dir := filepath.Join(im.cfg.IndexDataDir, "index", kbID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("index: EnforceDiskRetention(%s): read dir: %w", kbID, err)
	}

	// Orphaned sidecars first: a `<v>.index.used` or `<v>.index.mem` whose
	// `<v>.index` is gone. The drop loop below walks artifacts, so it can never
	// reach them — a version whose artifact was lost (an interrupted install, a
	// manual delete, a botched partial write) leaves its sidecars behind for
	// good, and nothing else in the retention path ever looks at them.
	//
	// Only these two suffixes. saveToDisk and recordInterestNow write them AFTER
	// the index is in place, whereas InstallIndex writes the `.ids` pair BEFORE
	// the index — so a `.ids` without its index is a normal in-flight state, not
	// garbage, and must be left alone. (That one is §8.8's business, not
	// retention's.)
	withIndex := make(map[string]bool, len(entries))
	for _, e := range entries {
		if name := e.Name(); strings.HasSuffix(name, ".index") {
			withIndex[strings.TrimSuffix(name, ".index")] = true
		}
	}
	for _, e := range entries {
		name := e.Name()
		var base string
		switch {
		case strings.HasSuffix(name, ".index.used"):
			base = strings.TrimSuffix(name, ".index.used")
		case strings.HasSuffix(name, ".index.mem"):
			base = strings.TrimSuffix(name, ".index.mem")
		default:
			continue
		}
		if withIndex[base] {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("index: EnforceDiskRetention(%s): remove orphan sidecar %s: %w", kbID, name, err)
		}
	}

	protected := make(map[int64]bool, len(protectedIDs))
	for _, id := range protectedIDs {
		protected[id] = true
	}

	type idxFile struct {
		versionID int64
		base      string // file base name without the ".index" suffix
	}
	var files []idxFile
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".index") {
			continue
		}
		base := strings.TrimSuffix(name, ".index")
		id, perr := strconv.ParseInt(base, 10, 64)
		if perr != nil || protected[id] {
			continue
		}
		files = append(files, idxFile{versionID: id, base: base})
	}

	if len(files) <= im.cfg.IndexRetentionCount {
		return nil
	}

	// Shield the versions queried here recently, on top of the newest
	// IndexRetentionCount. Read from the .used sidecars rather than from the
	// in-memory access table, because this pass also runs at startup — when
	// memory holds nothing and the drop is at its largest.
	if window := im.retentionProtectWindow(); window > 0 {
		candidates := make([]int64, len(files))
		for i, f := range files {
			candidates[i] = f.versionID
		}
		if keep := im.accessProtectedIDs(kbID, candidates, window, im.retentionProtectMax()); len(keep) > 0 {
			rest := files[:0]
			for _, f := range files {
				if !keep[f.versionID] {
					rest = append(rest, f)
				}
			}
			files = rest
			if len(files) <= im.cfg.IndexRetentionCount {
				return nil
			}
		}
	}

	sort.Slice(files, func(i, j int) bool { return files[i].versionID < files[j].versionID })

	// Drop the oldest (len(files) - retentionCount) versions' files.
	// Sidecar names mirror the vecstore's Save layout: the Faiss file is
	// <versionID>.index, its chunk-ID sidecar is <versionID>.index.ids, the
	// size sidecar is <versionID>.index.mem, and the access-time sidecar (the
	// retention shield above) is <versionID>.index.used.
	for _, f := range files[:len(files)-im.cfg.IndexRetentionCount] {
		for _, suffix := range []string{".index", ".index.ids", ".index.mem", ".index.used"} {
			path := filepath.Join(dir, f.base+suffix)
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("index: EnforceDiskRetention(%s): remove %s: %w", kbID, f.base+suffix, err)
			}
		}
	}
	return nil
}

// persistSizeSidecar writes the version's estimated index footprint (in
// bytes) next to its on-disk index file, so loadFromDisk after a restart
// can account for it against MemoryThresholdMB. Best-effort: failures are
// ignored and the estimate simply degrades to 0.
func (im *IndexManagerImpl) persistSizeSidecar(kbID string, versionID int64, sizeBytes int64) {
	if im.cfg.IndexDataDir == "" {
		return
	}
	path := im.sizeSidecarPath(kbID, versionID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	_ = os.WriteFile(path, []byte(strconv.FormatInt(sizeBytes, 10)), 0o644)
}

// readSizeSidecar loads the persisted size estimate for (kbID, versionID),
// returning 0 when the sidecar is absent (pre-policy indexes) or corrupt.
func (im *IndexManagerImpl) readSizeSidecar(kbID string, versionID int64) int64 {
	data, err := os.ReadFile(im.sizeSidecarPath(kbID, versionID))
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// sizeSidecarPath returns the on-disk path of (kbID, versionID)'s size
// sidecar: <IndexDataDir>/index/<kbID>/<versionID>.index.mem.
func (im *IndexManagerImpl) sizeSidecarPath(kbID string, versionID int64) string {
	return filepath.Join(im.cfg.IndexDataDir, "index", kbID, fmt.Sprintf("%d.index.mem", versionID))
}

const (
	// DefaultRetentionProtectWindow is the window the retention policy shields
	// recently-queried versions for, used when RetentionProtectWindow is zero.
	DefaultRetentionProtectWindow = 24 * time.Hour

	// accessPersistInterval throttles how often a version's access time is
	// written to its .used sidecar.
	accessPersistInterval = time.Minute
)

// retentionProtectWindow resolves the configured shield window: negative
// disables the protection, zero means the default.
func (im *IndexManagerImpl) retentionProtectWindow() time.Duration {
	if im.cfg.RetentionProtectWindow < 0 {
		return 0
	}
	if im.cfg.RetentionProtectWindow > 0 {
		return im.cfg.RetentionProtectWindow
	}
	return DefaultRetentionProtectWindow
}

// retentionProtectMax resolves the shield's size cap; zero or negative means
// IndexRetentionCount (and at least one, so an enabled protection always
// protects something).
func (im *IndexManagerImpl) retentionProtectMax() int {
	if im.cfg.RetentionProtectMax > 0 {
		return im.cfg.RetentionProtectMax
	}
	if im.cfg.IndexRetentionCount > 0 {
		return im.cfg.IndexRetentionCount
	}
	return 1
}

// accessProtectedIDs returns the version ids among candidates that were queried
// here within window, keeping at most max of them — the most recently queried
// win.
//
// The evidence is each version's <versionID>.index.used sidecar, which is what
// makes the shield survive a restart. A version without one (never queried
// here, or written before this policy existed) is simply not protected.
func (im *IndexManagerImpl) accessProtectedIDs(kbID string, candidates []int64, window time.Duration, max int) map[int64]bool {
	type entry struct {
		id int64
		at time.Time
	}
	cutoff := time.Now().Add(-window)
	var recent []entry
	for _, id := range candidates {
		at, ok := im.readAccessTime(kbID, id)
		if !ok || at.Before(cutoff) {
			continue
		}
		recent = append(recent, entry{id: id, at: at})
	}
	if len(recent) > max {
		sort.Slice(recent, func(i, j int) bool { return recent[i].at.After(recent[j].at) })
		recent = recent[:max]
	}
	out := make(map[int64]bool, len(recent))
	for _, e := range recent {
		out[e.id] = true
	}
	return out
}

// persistAccessTime writes the version's last-query time next to its index, so
// the retention policy can shield it after a restart. Best-effort, like the
// size sidecar: a failed write only means the version loses its shield.
func (im *IndexManagerImpl) persistAccessTime(kbID string, versionID int64, t time.Time) {
	if im.cfg.IndexDataDir == "" {
		return
	}
	path := im.usedPath(kbID, versionID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	_ = os.WriteFile(path, []byte(strconv.FormatInt(t.UnixNano(), 10)), 0o644)
}

// readAccessTime loads the persisted last-query time for (kbID, versionID);
// ok is false when the sidecar is absent or corrupt.
func (im *IndexManagerImpl) readAccessTime(kbID string, versionID int64) (time.Time, bool) {
	data, err := os.ReadFile(im.usedPath(kbID, versionID))
	if err != nil {
		return time.Time{}, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, n), true
}

// usedPath returns the on-disk path of (kbID, versionID)'s access-time
// sidecar: <IndexDataDir>/index/<kbID>/<versionID>.index.used.
func (im *IndexManagerImpl) usedPath(kbID string, versionID int64) string {
	return filepath.Join(im.cfg.IndexDataDir, "index", kbID, fmt.Sprintf("%d.index.used", versionID))
}

// Discard implements IndexManager: evicts the in-memory entry, sets a
// version tombstone (closing the Load-RPC resurrection race), drops the
// vecstore-side index object, and removes the version's on-disk index files.
// Dropping a never-built index is a no-op server-side; the local evict,
// the tombstone, and the file deletions are all idempotent.
//
// Drop rather than Reset: Reset empties an index but keeps the object, and the
// object (Faiss index + id table) is the memory a discarded version must not go
// on holding (H5 of docs/code-review-2026-09-24.md).
func (im *IndexManagerImpl) Discard(ctx context.Context, kbID string, versionID int64) error {
	im.mu.Lock()
	key := indexKey{kbID, versionID}
	delete(im.loaded, key)
	if size, ok := im.sizeByKey[key]; ok {
		im.loadedBytes -= size
		delete(im.sizeByKey, key)
	}
	im.deletedVersions[key] = true
	im.forgetVersionLocked(key)
	im.mu.Unlock()

	if im.vectorIndexClient == nil {
		return fmt.Errorf("index: Discard(%s,%d): vectorIndexClient not set", kbID, versionID)
	}
	if _, err := im.vectorIndexClient.Drop(ctx, &vecstorepb.DropIndexRequest{
		KbId:      kbID,
		VersionId: versionID,
	}); err != nil {
		return fmt.Errorf("index: Discard(%s,%d): Drop RPC: %w", kbID, versionID, err)
	}

	// Remove the version's on-disk index files (Faiss file + its .ids
	// sidecar + the size sidecar + the access-time sidecar; indexPath already
	// ends in ".index").
	// Missing files are ignored; a no-op when persistence is unconfigured.
	// Without this, a deleted version's files would linger and skew the
	// disk retention window (see EnforceDiskRetention).
	if im.cfg.IndexDataDir != "" {
		for _, suffix := range []string{"", ".ids", ".mem", ".used"} {
			path := im.indexPath(kbID, versionID) + suffix
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("index: Discard(%s,%d): remove %q: %w", kbID, versionID, filepath.Base(path), err)
			}
		}
	}
	return nil
}

// Ping implements IndexManager.
func (im *IndexManagerImpl) Ping(_ context.Context) error {
	return nil
}

// --- Test helpers (not part of the IndexManager interface) ---

// LoadedCount implements IndexManager: returns how many indexes are
// currently in memory.
func (im *IndexManagerImpl) LoadedCount() int {
	im.mu.Lock()
	defer im.mu.Unlock()
	return len(im.loaded)
}

// IsLoaded reports whether (kbID, versionID)'s index is currently in memory.
func (im *IndexManagerImpl) IsLoaded(kbID string, versionID int64) bool {
	im.mu.Lock()
	defer im.mu.Unlock()
	_, ok := im.loaded[indexKey{kbID, versionID}]
	return ok
}

// RefCount returns the current reference count for (kbID, versionID)'s
// index, or 0 if it is not loaded.
func (im *IndexManagerImpl) RefCount(kbID string, versionID int64) int {
	im.mu.Lock()
	defer im.mu.Unlock()
	if idx, ok := im.loaded[indexKey{kbID, versionID}]; ok {
		return idx.refCount
	}
	return 0
}
