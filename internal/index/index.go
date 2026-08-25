// Package index defines the IndexManager interface — the vector index
// manager responsible for loading, evicting, and querying per-version HNSW
// indexes, and for orchestrating asynchronous index builds.
//
// See Stratum_接口设计v9.md "IndexManager" and Stratum_设计文档v10.md "索引
// 管理器" for the authoritative design. This file contains only the
// interface definition; the real implementation (IndexManagerImpl, with
// LRU eviction, reference counting, and a real vecstore gRPC client) is
// built in Phase 4 (4-B).
package index

import (
	"context"

	"stratum/internal/types"
)

// BuildCompleteCallback is invoked once an asynchronous index build for
// (kbID, versionID) finishes, with status set to types.IndexStatusReady on
// success or types.IndexStatusFailed on failure. Callback implementations
// are expected to propose the corresponding status update via
// RaftNode.ProposeUpdateVersionStatus; IndexManager itself does not depend
// on RaftNode directly (kept decoupled so IndexManager's own tests don't
// need a real or mock RaftNode).
type BuildCompleteCallback func(kbID string, versionID int64, status types.IndexStatus) error

// IndexManager manages the lifecycle of per-version HNSW indexes:
// asynchronous building, in-memory LRU caching with reference-counted
// eviction protection, and search.
//
// Build data flow (executed asynchronously by TriggerBuild):
//
//	VersionDocList.ListDocIDs(kbID, versionID)        -> full doc ID set for the version
//	  -> ChunkDocMapper.ListChunkIDsByDocs(kbID, docIDs) -> full chunk ID set (deduped)
//	  -> batched ChunkStorage.Read (via ChunkStore, internal gRPC)        -> chunk vectors
//	  -> VectorIndex.Build(chunks, metric)
//
// Concurrent load wait: if every index currently in memory has a non-zero
// reference count, a new Search-triggered load waits until one becomes
// evictable. The wait is bounded by load_wait_timeout_ms; on timeout,
// Search returns errors.ErrIndexLoadTimeout (mapped to gRPC
// DEADLINE_EXCEEDED). The wait also obeys ctx.Done(), per the project's
// context-handling convention: whichever fires first wins.
//
// Ping is a lightweight liveness probe for HealthCheck: it must not
// trigger any index load and must not touch the reference-count
// bookkeeping.
//
// Build-callback retries: when a BuildCompleteCallback returns an error
// (e.g. the underlying RaftNode.ProposeUpdateVersionStatus call failed),
// the implementation retries with exponential backoff up to
// CallbackMaxRetries, then logs the failure.
type IndexManager interface {
	// Search loads versionID's index (LRU semantics; a cold version is
	// loaded on demand), increments its reference count for the duration
	// of the call, runs the vector search, and returns the top results
	// before topK x N filtering/aggregation happens upstream in the Query
	// read path.
	Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error)

	// TriggerBuild asynchronously builds the index for (kbID, versionID).
	// It returns once the build has been scheduled, not once it
	// completes; completion is reported via the registered
	// BuildCompleteCallback.
	TriggerBuild(ctx context.Context, kbID string, versionID int64) error

	// RegisterBuildCallback registers cb to be invoked when an
	// asynchronous build started by TriggerBuild completes (successfully
	// or not). Implementations may support multiple registered callbacks
	// or a single one; callers should not depend on a specific arity
	// beyond "registered callbacks are invoked."
	RegisterBuildCallback(cb BuildCompleteCallback)

	// IndexExists reports whether a persisted index for (kbID, versionID)
	// exists on disk at this node's index directory (both the Faiss file
	// and its sidecar). Stateless on the vecstore side — it inspects the
	// filesystem, so it answers correctly even right after a vecstore
	// restart. This is the authoritative "is this version's index built
	// and durable" fact that the startup reconcile derives READY status
	// from.
	IndexExists(ctx context.Context, kbID string, versionID int64) (bool, error)

	// Evict removes a single version's index from memory (if loaded; a
	// no-op otherwise).
	Evict(ctx context.Context, kbID string, versionID int64) error

	// Discard removes a single version's index entirely: evicts the
	// in-memory entry and resets the (kbID, versionID) index on the
	// vecstore side. Used by the DeleteVersion cleanup. Resetting a
	// never-built index is a no-op server-side; the local evict is
	// idempotent too.
	Discard(ctx context.Context, kbID string, versionID int64) error

	// EvictByKB removes every loaded index belonging to kbID. Used by
	// knowledge base deletion.
	EvictByKB(ctx context.Context, kbID string) error

	// DeleteFilesByKB deletes kbID's on-disk persisted index files
	// (<IndexDataDir>/index/<kbID>/), ignoring a missing directory
	// (ErrNotExist) so re-running the delete flow after a crash is safe.
	// In-memory entries are NOT touched — callers pair this with
	// EvictByKB. Used by knowledge base deletion (Stratum_设计文档v10.md
	// "删除知识库" 第 4 步). No-op when disk persistence is unconfigured.
	DeleteFilesByKB(ctx context.Context, kbID string) error

	// EnforceDiskRetention applies the per-KB on-disk retention policy:
	// keeps the most recent cfg.IndexRetentionCount index files per
	// knowledge base and deletes older ones (plus sidecars). protectedIDs
	// are never deleted (used to shield the active version). Missing
	// files/directories are ignored; no-op when retention is unconfigured.
	EnforceDiskRetention(ctx context.Context, kbID string, protectedIDs []int64) error

	// Ping is a lightweight health probe: it reports whether IndexManager
	// itself is operating normally, without loading any index or touching
	// reference counts.
	Ping(ctx context.Context) error

	// LoadedCount returns how many version indexes are currently held in
	// memory. Used by GetSystemStatus's resource-usage snapshot.
	LoadedCount() int
}
