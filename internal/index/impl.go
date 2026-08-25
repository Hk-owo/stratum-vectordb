package index

import (
	"context"
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
}

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

	// vecstore gRPC client for Build/Search/Save/Load/Reset.
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

// Close releases the vecstore gRPC connection, if one was created.
func (im *IndexManagerImpl) Close() error {
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
		_ = im.loadFromDisk(ctx, kbID, versionID)
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
	key := indexKey{kbID, versionID}

	im.mu.Lock()
	if im.loading[key] {
		im.mu.Unlock()
		return nil // build already in progress
	}
	im.loading[key] = true
	im.mu.Unlock()

	go im.doBuild(kbID, versionID)
	return nil
}

func (im *IndexManagerImpl) doBuild(kbID string, versionID int64) {
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
	sizeBytes, err = im.buildWithRetry(kbID, versionID)
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

func (im *IndexManagerImpl) buildWithRetry(kbID string, versionID int64) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), buildRetryTimeout)
	defer cancel()

	var lastErr error
	for {
		sizeBytes, err := im.build(ctx, kbID, versionID)
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

// build executes the full build data flow and returns the estimated
// in-memory footprint of the built index (sum of vector payload bytes;
// 0 for an empty version). It reports success only if the index was also
// persisted to disk (see saveToDisk), so a failed save surfaces as a
// build failure.
func (im *IndexManagerImpl) build(ctx context.Context, kbID string, versionID int64) (int64, error) {
	docIDs, err := im.listDocIDs(ctx, kbID, versionID)
	if err != nil {
		return 0, fmt.Errorf("index: ListDocIDs: %w", err)
	}

	chunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, docIDs)
	if err != nil {
		return 0, fmt.Errorf("index: ListChunkIDsByDocs: %w", err)
	}

	// 分批读取并发送：单条 Build/AddChunks RPC 的载荷必须小于 gRPC 默认
	// 4 MiB 上限。第一批用 Build 全量建索引，后续批用 AddChunks 增量追加。
	batches, sizeBytes, err := im.collectChunkBatches(ctx, kbID, chunkIDs)
	if err != nil {
		return 0, err
	}

	// 空版本（没有 chunk）也必须调一次 Build，在 vecstore 侧建立该
	// (kb, version) 的索引条目：否则后续 Search 会因 "no index built or
	// loaded" 而失败（与分批前 Build(empty) 的语义保持一致）。
	if len(batches) == 0 {
		_, err = im.vectorIndexClient.Build(ctx, &vecstorepb.BuildIndexRequest{
			KbId:      kbID,
			VersionId: versionID,
			Metric:    vecstorepb.MetricTypeProto_COSINE,
		})
		if err != nil {
			return 0, fmt.Errorf("index: Build RPC: %w", err)
		}
		return 0, im.saveToDisk(ctx, kbID, versionID)
	}

	for i, batch := range batches {
		chunks := make([]*vecstorepb.ChunkVectorProto, 0, len(batch))
		for _, cv := range batch {
			chunks = append(chunks, &vecstorepb.ChunkVectorProto{ChunkId: cv.id, Vector: cv.vec})
		}
		if i == 0 {
			_, err = im.vectorIndexClient.Build(ctx, &vecstorepb.BuildIndexRequest{
				KbId:      kbID,
				VersionId: versionID,
				Chunks:    chunks,
				Metric:    vecstorepb.MetricTypeProto_COSINE,
			})
			if err != nil {
				return 0, fmt.Errorf("index: Build RPC: %w", err)
			}
		} else {
			_, err = im.vectorIndexClient.AddChunks(ctx, &vecstorepb.AddChunksRequest{
				KbId:      kbID,
				VersionId: versionID,
				Chunks:    chunks,
			})
			if err != nil {
				return 0, fmt.Errorf("index: AddChunks RPC: %w", err)
			}
		}
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
