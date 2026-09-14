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

	// MemoryThresholdMB bounds the estimated in-memory footprint of all
	// loaded indexes (vector payload bytes, summed and tracked per loaded
	// index). When the estimate exceeds the threshold, new loads/builds
	// evict least-recently-used, ref-count-zero indexes first
	// (Stratum_设计文档v10.md "内存换入换出"). <= 0 disables the byte
	// threshold; LRUCapacity still applies.
	MemoryThresholdMB int64

	// ColdThreshold is how long a version may go without a Search before
	// the background evaluator rebuilds it in the graph-free form
	// (§8.6a). The HNSW graph dominates both build time and resident
	// memory once vectors are quantized, and a version nobody queries
	// does not need it; the answers stay equivalent because the
	// graph-free variants keep the quantizer and its rerank semantics.
	// The policy is off when <= 0 (the default), which keeps the
	// historical "every version carries a full graph" behaviour.
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
}

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

	// sizeByKey tracks each loaded index's estimated in-memory footprint
	// (vector payload bytes from the last build; 0 when unknown, e.g. a
	// pre-policy index loaded without a size sidecar). loadedBytes is
	// their sum, consulted by makeRoomLocked when MemoryThresholdMB is
	// set. Both are guarded by mu.
	sizeByKey   map[indexKey]int64
	loadedBytes int64

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

	// gcCancel/gcWG govern the §8.6(d) background scanner. gcCancel is nil
	// while it is off.
	gcCancel context.CancelFunc
	gcWG     sync.WaitGroup

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
		deletedKBs:      make(map[string]bool),
		deletedVersions: make(map[indexKey]bool),
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

// Search implements IndexManager.
func (im *IndexManagerImpl) Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	key := indexKey{kbID, versionID}
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
		if err := im.loadFromDisk(ctx, kbID, versionID); err != nil {
			// §8.6b: indexes are built lazily now, so "no index on disk" is a
			// normal state rather than a failure. A small version is answered
			// by scanning; a large one falls through to the build below, which
			// acquire() then waits for.
			answered, results, terr := im.tryBruteForce(ctx, kbID, versionID, vector, topK)
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

	resp, err := im.vectorIndexClient.Search(ctx, &vecstorepb.SearchIndexRequest{
		KbId:      kbID,
		VersionId: versionID,
		Vector:    vector,
		TopK:      int32(topK),
	})
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
	return results, nil
}

// TriggerBuild implements IndexManager.
func (im *IndexManagerImpl) TriggerBuild(ctx context.Context, kbID string, versionID int64) error {
	return im.triggerBuild(kbID, versionID, false)
}

// TriggerBuildGraphFree builds the version without an HNSW graph (§8.6a), for a
// cold version whose index is unlikely to be queried: the graph is what makes
// it expensive to build and to keep resident, and a scan of quantized codes is
// an acceptable price for a version nobody is asking about.
func (im *IndexManagerImpl) TriggerBuildGraphFree(ctx context.Context, kbID string, versionID int64) error {
	return im.triggerBuild(kbID, versionID, true)
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

// sweepCold reshapes every version that has gone cold. Scheduling is
// asynchronous (triggerBuild spawns the build), so a sweep never blocks
// on a vecstore build; a version already building is skipped by
// triggerBuild itself.
func (im *IndexManagerImpl) sweepCold(ctx context.Context, now time.Time) {
	for _, key := range im.coldCandidates(now) {
		if ctx.Err() != nil {
			return
		}
		im.logger.Info("index: version is cold; reshaping graph-free (§8.6a)",
			zap.String("kb_id", key.kbID), zap.Int64("version_id", key.versionID))
		if err := im.TriggerBuildGraphFree(ctx, key.kbID, key.versionID); err != nil {
			im.logger.Warn("index: could not schedule the graph-free rebuild",
				zap.String("kb_id", key.kbID), zap.Int64("version_id", key.versionID), zap.Error(err))
		}
	}
}

// coldCandidates returns the versions whose last access is at least
// ColdThreshold old and whose current index is not already graph-free,
// in a deterministic (kbID, versionID) order. A version mid-build is
// left out: the build in flight decides its own shape, and the next
// sweep re-evaluates it if it stayed hot.
func (im *IndexManagerImpl) coldCandidates(now time.Time) []indexKey {
	im.mu.Lock()
	defer im.mu.Unlock()
	var out []indexKey
	for k, last := range im.lastSearch {
		if im.deletedKBs[k.kbID] || im.deletedVersions[k] {
			continue
		}
		if im.builtGraphFree[k] || im.loading[k] {
			continue
		}
		if now.Sub(last) >= im.cfg.ColdThreshold {
			out = append(out, k)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].kbID != out[j].kbID {
			return out[i].kbID < out[j].kbID
		}
		return out[i].versionID < out[j].versionID
	})
	return out
}

func (im *IndexManagerImpl) triggerBuild(kbID string, versionID int64, graphFree bool) error {
	key := indexKey{kbID, versionID}

	im.mu.Lock()
	if im.loading[key] {
		im.mu.Unlock()
		return nil // build already in progress
	}
	im.loading[key] = true
	im.mu.Unlock()

	go im.doBuild(kbID, versionID, graphFree)
	return nil
}

func (im *IndexManagerImpl) doBuild(kbID string, versionID int64, graphFree bool) {
	key := indexKey{kbID, versionID}

	status := types.IndexStatusReady
	var sizeBytes int64

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
		im.mu.Lock()
		delete(im.loading, key)
		if status == types.IndexStatusReady {
			// Make room before inserting the new index.
			im.makeRoomLocked()
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

		// Persist the size sidecar next to the index file so a later
		// restart (loadFromDisk) can account for this version's memory
		// footprint. Best-effort: a failed sidecar write only degrades
		// the estimate to 0.
		if status == types.IndexStatusReady {
			im.persistSizeSidecar(kbID, versionID, sizeBytes)
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
	if err != nil {
		im.logger.Error("index build failed",
			zap.String("kb_id", kbID),
			zap.Int64("version_id", versionID),
			zap.Error(err))
		status = types.IndexStatusFailed
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

func (im *IndexManagerImpl) buildWithRetry(kbID string, versionID int64, graphFree bool) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), buildRetryTimeout)
	defer cancel()

	var lastErr error
	for {
		sizeBytes, err := im.build(ctx, kbID, versionID, graphFree)
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
func (im *IndexManagerImpl) build(ctx context.Context, kbID string, versionID int64, graphFree bool) (int64, error) {
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

	// §8.6(c): a pure-append version can start from its parent's artifact and
	// add only the new chunks, instead of rebuilding the whole graph. Purely
	// an optimisation — a failure here falls back to the full build below,
	// and nothing downstream (callback, distribution, retention) can tell
	// which path produced the artifact.
	if parentID, delta, dead, ok := im.appendBase(ctx, kbID, versionID, graphFree, chunkIDs); ok {
		size, appendErr := im.buildFromBase(ctx, kbID, versionID, parentID, delta, dead, len(chunkIDs), graphFree)
		if appendErr == nil {
			im.logger.Info("index: built by appending to the parent version's artifact (§8.6c)",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Int64("parent_version_id", parentID),
				zap.Int("total_chunks", len(chunkIDs)), zap.Int("delta_chunks", len(delta)),
				zap.Int("deleted_chunks", len(dead)), zap.Bool("graph_free", graphFree))
			return size, nil
		}
		if errors.Is(appendErr, errAppendTooManyTombstones) {
			// Not a failure: the reuse was legal, but the base carried too much
			// dead weight, so rebuilding (which drops it) is the better trade.
			im.logger.Info("index: append reuse not worth it; rebuilding from scratch",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Int64("parent_version_id", parentID), zap.Error(appendErr))
		} else {
			im.logger.Warn("index: append reuse failed; rebuilding from scratch",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Int64("parent_version_id", parentID), zap.Error(appendErr))
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
	defer im.mu.Unlock()
	if im.deletedKBs[kbID] || im.deletedVersions[key] {
		return fmt.Errorf("index: %s/%d was deleted while loading", kbID, versionID)
	}
	if _, ok := im.loaded[key]; ok {
		return nil // a concurrent load/build already brought it in
	}
	im.makeRoomLocked()
	im.loaded[key] = &loadedIndex{lastAccess: time.Now()}
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
	im.mu.Lock()
	parentGraphFree, known := im.builtGraphFree[indexKey{kbID, parent}]
	im.mu.Unlock()
	if !known || parentGraphFree != graphFree {
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
			return nil, 0, fmt.Errorf("index: read chunk vector %s: %w", chunkID, err)
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
	im.mu.Lock()
	defer im.mu.Unlock()
	im.lastSearch[key] = time.Now()
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
}

// makeRoomLocked evicts least-recently-used, ref-count-zero indexes until
// there is room for one more entry: len(loaded) < LRUCapacity AND (if
// MemoryThresholdMB is set) loadedBytes <= threshold. If no evictable
// index remains (everything is pinned), it stops and returns. Must be
// called with im.mu held; called BEFORE inserting the new entry so the
// brand-new index (refCount 0, nothing has acquired it yet) is never
// itself chosen as the eviction candidate.
func (im *IndexManagerImpl) makeRoomLocked() {
	var threshold int64
	if im.cfg.MemoryThresholdMB > 0 {
		threshold = im.cfg.MemoryThresholdMB << 20 // MiB → bytes
	}
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
			return // everything is pinned
		}
		delete(im.loaded, oldestKey)
		if size, ok := im.sizeByKey[oldestKey]; ok {
			im.loadedBytes -= size
			delete(im.sizeByKey, oldestKey)
		}
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

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := cb(kbID, versionID, status); err == nil {
			return
		}
		if attempt < maxRetries {
			backoff := base * time.Duration(int64(math.Pow(2, float64(attempt))))
			time.Sleep(backoff)
		}
	}
	im.logger.Error("build callback retries exhausted",
		zap.String("kb_id", kbID),
		zap.Int64("version_id", versionID),
		zap.String("status", status.String()),
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
	defer im.mu.Unlock()
	for k := range im.loaded {
		if k.kbID == kbID {
			delete(im.loaded, k)
			if size, ok := im.sizeByKey[k]; ok {
				im.loadedBytes -= size
				delete(im.sizeByKey, k)
			}
		}
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
	dir := filepath.Join(im.cfg.IndexDataDir, "index", kbID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("index: EnforceDiskRetention(%s): read dir: %w", kbID, err)
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
	sort.Slice(files, func(i, j int) bool { return files[i].versionID < files[j].versionID })

	// Drop the oldest (len(files) - retentionCount) versions' files.
	// Sidecar names mirror the vecstore's Save layout: the Faiss file is
	// <versionID>.index, its chunk-ID sidecar is <versionID>.index.ids,
	// and the size sidecar is <versionID>.index.mem.
	for _, f := range files[:len(files)-im.cfg.IndexRetentionCount] {
		for _, suffix := range []string{".index", ".index.ids", ".index.mem"} {
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

// Discard implements IndexManager: evicts the in-memory entry, sets a
// version tombstone (closing the Load-RPC resurrection race), resets the
// vecstore-side index, and removes the version's on-disk index files.
// Resetting a never-built index is a no-op server-side; the local evict,
// the tombstone, and the file deletions are all idempotent.
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
	if _, err := im.vectorIndexClient.Reset(ctx, &vecstorepb.ResetIndexRequest{
		KbId:      kbID,
		VersionId: versionID,
	}); err != nil {
		return fmt.Errorf("index: Discard(%s,%d): Reset RPC: %w", kbID, versionID, err)
	}

	// Remove the version's on-disk index files (Faiss file + its .ids
	// sidecar + the size sidecar; indexPath already ends in ".index").
	// Missing files are ignored; a no-op when persistence is unconfigured.
	// Without this, a deleted version's files would linger and skew the
	// disk retention window (see EnforceDiskRetention).
	if im.cfg.IndexDataDir != "" {
		for _, suffix := range []string{"", ".ids", ".mem"} {
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
