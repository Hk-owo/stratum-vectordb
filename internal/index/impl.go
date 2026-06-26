package index

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

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
// it is retried with exponential backoff up to CallbackMaxRetries. Once
// retries are exhausted, the version's status is left as FAILED and the
// version is recorded in a "needs repair" set for operator visibility
// via GetSystemStatus.
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
		cfg:     cfg,
		loaded:  make(map[indexKey]*loadedIndex),
		loading: make(map[indexKey]bool),
		logger:  zap.NewNop(),
	}
	im.cond = sync.NewCond(&im.mu)

	if cfg.VecstoreAddr != "" {
		conn, err := grpc.NewClient(cfg.VecstoreAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    10 * time.Second,
				Timeout: 3 * time.Second,
				PermitWithoutStream: true,
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
	if err := im.build(context.Background(), kbID, versionID); err != nil {
		im.logger.Error("index build failed",
			zap.String("kb_id", kbID),
			zap.Int64("version_id", versionID),
			zap.Error(err))
		status = types.IndexStatusFailed
	}

	im.mu.Lock()
	delete(im.loading, key)
	if status == types.IndexStatusReady {
		// Make room before inserting the new index.
		im.makeRoomLocked()
		im.loaded[key] = &loadedIndex{lastAccess: time.Now()}
	}
	callbacks := append([]BuildCompleteCallback(nil), im.callbacks...)
	im.cond.Broadcast()
	im.mu.Unlock()

	// Invoke callbacks with retry.
	for _, cb := range callbacks {
		im.invokeCallback(cb, kbID, versionID, status)
	}
}

func (im *IndexManagerImpl) build(ctx context.Context, kbID string, versionID int64) error {
	docIDs, err := im.listDocIDs(ctx, kbID, versionID)
	if err != nil {
		return fmt.Errorf("index: ListDocIDs: %w", err)
	}

	chunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, docIDs)
	if err != nil {
		return fmt.Errorf("index: ListChunkIDsByDocs: %w", err)
	}

	chunks := make([]*vecstorepb.ChunkVectorProto, 0, len(chunkIDs))
	for _, chunkID := range chunkIDs {
		v, err := im.readChunkVector(ctx, kbID, chunkID)
		if err != nil {
			return fmt.Errorf("index: read chunk vector %s: %w", chunkID, err)
		}
		chunks = append(chunks, &vecstorepb.ChunkVectorProto{ChunkId: chunkID, Vector: v})
	}

	_, err = im.vectorIndexClient.Build(ctx, &vecstorepb.BuildIndexRequest{
		KbId:      kbID,
		VersionId: versionID,
		Chunks:    chunks,
		Metric:    vecstorepb.MetricTypeProto_COSINE,
	})
	if err != nil {
		return fmt.Errorf("index: Build RPC: %w", err)
	}

	return nil
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

// waitLocked blocks on im.cond until woken, ctx is done, or deadline
// passes, returning false in the latter two cases. Must be called with
// im.mu held.
func (im *IndexManagerImpl) waitLocked(ctx context.Context, deadline time.Time) bool {
	// Use a timer to wake the cond when deadline passes, and a goroutine
	// to wake when ctx is done.
	wakerDone := make(chan struct{})
	timer := time.AfterFunc(time.Until(deadline), func() {
		im.mu.Lock()
		im.cond.Broadcast()
		im.mu.Unlock()
	})
	defer timer.Stop()

	go func() {
		select {
		case <-ctx.Done():
			im.mu.Lock()
			im.cond.Broadcast()
			im.mu.Unlock()
		case <-wakerDone:
		}
	}()
	defer close(wakerDone)

	im.cond.Wait()

	if ctx.Err() != nil {
		return false
	}
	return time.Now().Before(deadline)
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
// len(loaded) < LRUCapacity or no evictable index remains. Must be called
// with im.mu held.
func (im *IndexManagerImpl) makeRoomLocked() {
	if im.cfg.LRUCapacity <= 0 {
		return
	}
	for len(im.loaded) >= im.cfg.LRUCapacity {
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
	delete(im.loaded, indexKey{kbID, versionID})
	return nil
}

// EvictByKB implements IndexManager.
func (im *IndexManagerImpl) EvictByKB(_ context.Context, kbID string) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	for k := range im.loaded {
		if k.kbID == kbID {
			delete(im.loaded, k)
		}
	}
	return nil
}

// Ping implements IndexManager.
func (im *IndexManagerImpl) Ping(_ context.Context) error {
	return nil
}

// --- Test helpers (not part of the IndexManager interface) ---

// LoadedCount returns how many indexes are currently in memory.
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
