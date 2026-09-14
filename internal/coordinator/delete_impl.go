package coordinator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"

	stratumerrors "stratum/internal/errors"
)

// DeleteCoordinatorConfig bundles all dependencies for DeleteCoordinatorImpl.
type DeleteCoordinatorConfig struct {
	MaxRetries          int
	RetryBaseIntervalMS int

	WAL            wal.WAL
	RaftNode       raft.RaftNode
	IndexManager   index.IndexManager
	DocStore       docstore.DocStore
	ChunkStore     chunkstore.ChunkStore
	ChunkDocMapper chunkdoc.ChunkDocMapper
	VersionDocList versiondoc.VersionDocList

	// VersionBloom persists each version's on-disk document bloom filter.
	// May be nil when the filter store is not wired; the cleanup then
	// skips its on-disk file deletion.
	VersionBloom *bloom.VersionBloomStore

	// Dropper reclaims the knowledge base's physical data.
	//
	// When set, it is what the flow calls instead of reaching into the stores
	// above: the reclaim then happens wherever the data actually is, which is
	// what lets a control node with no storage of its own run this flow at all
	// (Stratum_设计文档v13.md §7.0、§11 阶段 ④).
	//
	// nil falls back to a LocalKBDropper built from the fields above — the same
	// layer-by-layer deletes, so the fallback is not a second code path but the
	// same one reached without wiring.
	Dropper KBStorageDropper
}

// DeleteCoordinatorImpl is the real DeleteCoordinator implementation,
// orchestrating the asynchronous cleanup flow for DeleteKnowledgeBase.
//
// The flow is documented in Stratum_接口设计v9.md "DeleteKnowledgeBase"
// and Stratum_设计文档v10.md "删除知识库":
//
//  0. WAL.WriteDeleteMark (idempotent; the durable "deletion started"
//     marker that crash recovery resumes from)
//  1. IndexManager.EvictByKB
//  2. IndexManager.DeleteFilesByKB (on-disk index files) + VersionBloom
//     store's on-disk bloom files — missing files/dirs ignored
//  3. DocStore.DeleteByKB
//  4. ChunkStore.DeleteByKB
//  5. ChunkDocMapper.DeleteByKB
//  6. VersionDocList.DeleteByKB
//  7. RaftNode.ProposeRemoveKBMeta (ErrKnowledgeBaseNotFound treated as success)
//  8. WAL.WriteDeleteComplete
//
// Every step is retried with exponential backoff. If retries are exhausted,
// Execute calls RaftNode.ProposeMarkKBDeleteFailed and returns an error.
type DeleteCoordinatorImpl struct {
	cfg DeleteCoordinatorConfig
}

// NewDeleteCoordinatorImpl constructs a DeleteCoordinatorImpl.
func NewDeleteCoordinatorImpl(cfg DeleteCoordinatorConfig) *DeleteCoordinatorImpl {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.RetryBaseIntervalMS <= 0 {
		cfg.RetryBaseIntervalMS = 500
	}
	return &DeleteCoordinatorImpl{cfg: cfg}
}

// Execute implements DeleteCoordinator.
func (c *DeleteCoordinatorImpl) Execute(ctx context.Context, kbID string) error {
	// Step 0: WAL delete mark (Stratum_设计文档v10.md "删除知识库" 第 1 步).
	// Written before any cleanup so a crash at any later point leaves a
	// durable marker that startup recovery (PendingRecordTypeDeleteMark)
	// resumes this flow from. FileWAL.WriteDeleteMark is idempotent per
	// kbID, so re-running Execute after a crash — or after the caller
	// re-invoked DeleteKnowledgeBase — never duplicates the record.
	if err := c.cfg.WAL.WriteDeleteMark(ctx, kbID); err != nil {
		return c.abort(ctx, kbID, fmt.Errorf("WAL.WriteDeleteMark: %w", err))
	}

	// Steps 1-6: reclaim the knowledge base's physical data — its in-memory
	// indexes, on-disk index and bloom files, documents, chunk vectors,
	// chunk→doc mappings and version→doc lists.
	//
	// Who performs these depends on where the data is, and the storage layer is
	// what owns it (§7.0). So the flow asks for the reclaim rather than doing
	// it: a node that holds the data reclaims locally, and a control node with
	// no storage of its own hands each version to the nodes that do. A nil
	// dropper keeps the historical behaviour by building the local one, so the
	// all-in-one deployment is unchanged.
	dropper := c.cfg.Dropper
	if dropper == nil {
		dropper = LocalKBDropper{
			IndexManager:   c.cfg.IndexManager,
			DocStore:       c.cfg.DocStore,
			ChunkStore:     c.cfg.ChunkStore,
			ChunkDocMapper: c.cfg.ChunkDocMapper,
			VersionDocList: c.cfg.VersionDocList,
			VersionBloom:   c.cfg.VersionBloom,
		}
	}
	if err := c.retry(ctx, func() error {
		return dropper.DropKnowledgeBase(ctx, kbID)
	}); err != nil {
		return c.abort(ctx, kbID, fmt.Errorf("DropKnowledgeBase: %w", err))
	}

	// Step 7: RaftNode.ProposeRemoveKBMeta.
	// Idempotent: ErrKnowledgeBaseNotFound is treated as success.
	if err := c.retry(ctx, func() error {
		err := c.cfg.RaftNode.ProposeRemoveKBMeta(ctx, kbID)
		if err != nil && errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
			return nil
		}
		return err
	}); err != nil {
		return c.abort(ctx, kbID, fmt.Errorf("ProposeRemoveKBMeta: %w", err))
	}

	// Step 8: WAL.WriteDeleteComplete.
	if err := c.cfg.WAL.WriteDeleteComplete(ctx, kbID); err != nil {
		return fmt.Errorf("WAL.WriteDeleteComplete: %w", err)
	}

	return nil
}

// abort marks the knowledge base as DeleteFailed and returns the cause.
func (c *DeleteCoordinatorImpl) abort(ctx context.Context, kbID string, cause error) error {
	// Best-effort: don't hide the original error if marking fails.
	_ = c.cfg.RaftNode.ProposeMarkKBDeleteFailed(ctx, kbID)
	return cause
}

// retry executes fn with exponential backoff up to MaxRetries times.
func (c *DeleteCoordinatorImpl) retry(ctx context.Context, fn func() error) error {
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

var _ DeleteCoordinator = (*DeleteCoordinatorImpl)(nil)
