package coordinator

import (
	"context"
	"fmt"

	"stratum/internal/bloom"
	"stratum/internal/docstore"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"
)

// DeleteVersionCoordinatorConfig bundles all dependencies for
// DeleteVersionCoordinatorImpl.
type DeleteVersionCoordinatorConfig struct {
	MaxRetries          int
	RetryBaseIntervalMS int

	WAL            wal.WAL
	RaftNode       raft.RaftNode
	IndexManager   index.IndexManager
	DocStore       docstore.DocStore
	VersionDocList versiondoc.VersionDocList
	// VersionBloom reclaims the version's document bloom filter. Optional
	// (may be nil when the on-disk bloom store is not wired), mirroring
	// DeleteCoordinatorConfig.VersionBloom.
	VersionBloom *bloom.VersionBloomStore

	// Dropper reclaims the version's physical data. When set it is what the
	// flow calls instead of reaching into the stores above, so the reclaim
	// happens wherever the bytes are (Stratum_设计文档v13.md §7.0).
	// nil falls back to a LocalVersionDropper built from the fields above.
	Dropper VersionStorageDropper
}

// DeleteVersionCoordinatorImpl is the real DeleteVersionCoordinator
// implementation, orchestrating the asynchronous cleanup flow for
// DeleteVersion (see the interface doc comment for the step list).
type DeleteVersionCoordinatorImpl struct {
	cfg DeleteVersionCoordinatorConfig
}

// NewDeleteVersionCoordinatorImpl constructs a DeleteVersionCoordinatorImpl.
func NewDeleteVersionCoordinatorImpl(cfg DeleteVersionCoordinatorConfig) *DeleteVersionCoordinatorImpl {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.RetryBaseIntervalMS <= 0 {
		cfg.RetryBaseIntervalMS = 500
	}
	return &DeleteVersionCoordinatorImpl{cfg: cfg}
}

// Execute implements DeleteVersionCoordinator: discovers every version of
// kbID currently marked Deleting and cleans each one up.
func (c *DeleteVersionCoordinatorImpl) Execute(ctx context.Context, kbID string) error {
	versions, err := c.cfg.RaftNode.ListVersions(ctx, kbID)
	if err != nil {
		return fmt.Errorf("delete-version: ListVersions(%s): %w", kbID, err)
	}
	for _, v := range versions {
		if !v.Deleting {
			continue
		}
		if err := c.deleteOne(ctx, kbID, v.VersionID); err != nil {
			return err
		}
	}
	return nil
}

// deleteOne cleans up a single version marked Deleting.
func (c *DeleteVersionCoordinatorImpl) deleteOne(ctx context.Context, kbID string, versionID int64) error {
	// Step 1: WAL delete mark (idempotent).
	if err := c.cfg.WAL.WriteVersionDeleteMark(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("delete-version: WAL.WriteVersionDeleteMark(%s,%d): %w", kbID, versionID, err)
	}

	// Steps 2-4 (and 6): the storage layer reclaims the version's physical data
	// — its index, its document-ID list, the MVCC records no surviving version
	// still reads, and its bloom filter.
	//
	// Asking rather than doing is what lets this flow run on a node that holds
	// no records of its own: the metadata says the version is gone, and the
	// nodes holding its bytes are the ones that reclaim them
	// (Stratum_设计文档v13.md §7.0、§11 阶段 ④). The visibility anchor is
	// applied by that side, since it is the one holding the records.
	dropper := c.cfg.Dropper
	if dropper == nil {
		dropper = LocalVersionDropper{
			IndexManager:   c.cfg.IndexManager,
			DocStore:       c.cfg.DocStore,
			VersionDocList: c.cfg.VersionDocList,
			VersionBloom:   c.cfg.VersionBloom,
			Metadata:       c.cfg.RaftNode,
		}
	}
	// A failed reclaim no longer BLOCKS the deletion, because removing the metadata is
	// now what tells every storage node the version is gone (the apply appends one
	// entry to the bounded deletion log, and each node's tombstone retry finishes the
	// reclaim on its own).
	//
	// It used to block, and that was the only safe answer while the metadata was the
	// last thing naming the bytes: a replica that could not be reached left bytes with
	// nothing left to find them. The cost of that safety was visible — one permanently
	// unreachable candidate kept the version in DELETING forever, re-broadcast every
	// minute by the delete sweeper.
	dropErr := c.retry(ctx, func() error {
		return dropper.DropVersion(ctx, kbID, versionID)
	})

	// Step 5: remove the version's metadata from the Raft state machine.
	// Idempotent — re-proposing after a crash is safe.
	if err := c.retry(ctx, func() error {
		return c.cfg.RaftNode.ProposeRemoveVersionMeta(ctx, kbID, versionID)
	}); err != nil {
		return fmt.Errorf("delete-version: ProposeRemoveVersionMeta(%s,%d): %w", kbID, versionID, err)
	}

	// Step 7: WAL delete complete (idempotent).
	if err := c.cfg.WAL.WriteVersionDeleteComplete(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("delete-version: WAL.WriteVersionDeleteComplete(%s,%d): %w", kbID, versionID, err)
	}

	// The debt is reported, not retried from here: the version is gone from the
	// metadata (so the delete sweeper will not find it again), and the node that still
	// holds the bytes has the tombstone entry that names the work.
	if dropErr != nil {
		return fmt.Errorf("delete-version: %s v%d is deleted, but its data was not reclaimed everywhere (%w); each node finishes it from the deletion log",
			kbID, versionID, dropErr)
	}
	return nil
}

// retry executes fn with exponential backoff up to MaxRetries times.
func (c *DeleteVersionCoordinatorImpl) retry(ctx context.Context, fn func() error) error {
	return retryWithBackoff(ctx, c.cfg.MaxRetries, c.cfg.RetryBaseIntervalMS, fn)
}

var _ DeleteVersionCoordinator = (*DeleteVersionCoordinatorImpl)(nil)
