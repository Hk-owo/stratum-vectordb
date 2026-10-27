package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

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

	// ChunkReclaimer reclaims the chunk vectors this version was the last live
	// referent of. It is handed to the fallback LocalVersionDropper; a configured
	// Dropper receives it through its own wiring (for a split deployment that is
	// the receiving node's DropVersionStorage). Optional.
	ChunkReclaimer VersionChunkReclaimer

	// Tombstones is this node's record of deletions it has been TOLD about
	// (internal/tombstone). Optional; without it a resumed deletion still reclaims the
	// bytes but leaves no local note that the version is gone.
	//
	// Only ResumeVersionDelete uses it, and the asymmetry is deliberate: the live path
	// learns about a deletion from the control layer (the notification carries the
	// tombstone write on the receive side), while a crash-recovery replay is the one path
	// where this node concludes the version is gone with no message to answer.
	Tombstones VersionTombstoneRecorder
}

// VersionTombstoneRecorder is the slice of internal/tombstone this package needs: two
// calls, declared here so the store's full contract (watermarks, paging, pruning) stays
// the plane's business.
type VersionTombstoneRecorder interface {
	Note(ctx context.Context, kbID string, versionID int64, at time.Time) error
	MarkDone(ctx context.Context, kbID string, versionID int64, at time.Time) error
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
	dropper := c.dropperFor()
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

// dropperFor resolves where a reclaim has to happen. The configured dropper wins; the
// local one is built from the fields above when nothing was wired, so the all-in-one
// deployment is unchanged (see Dropper's comment).
func (c *DeleteVersionCoordinatorImpl) dropperFor() VersionStorageDropper {
	if c.cfg.Dropper != nil {
		return c.cfg.Dropper
	}
	return LocalVersionDropper{
		IndexManager:   c.cfg.IndexManager,
		DocStore:       c.cfg.DocStore,
		VersionDocList: c.cfg.VersionDocList,
		VersionBloom:   c.cfg.VersionBloom,
		Metadata:       c.cfg.RaftNode,
		ChunkReclaimer: c.cfg.ChunkReclaimer,
	}
}

// ResumeVersionDelete implements DeleteVersionCoordinator: the crash-recovery path for
// ONE version, named by a WAL record. See the interface comment for why it exists and
// how it differs from Execute.
//
// The steps are deleteOne's, with the tombstone FIRST — nothing else would refuse a late
// push once the metadata is gone — and with the same failure policy: a reclaim that
// reaches nobody does not keep the record owed, because removing the metadata is what
// tells every storage node the version is gone.
func (c *DeleteVersionCoordinatorImpl) ResumeVersionDelete(ctx context.Context, kbID string, versionID int64) error {
	if versionID <= 0 {
		return fmt.Errorf("delete-version: resume %s: invalid version id %d", kbID, versionID)
	}

	// Step 1: WAL delete mark (idempotent) — what keeps the record recoverable if this
	// pass itself dies before the completion below.
	if err := c.cfg.WAL.WriteVersionDeleteMark(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("delete-version: WAL.WriteVersionDeleteMark(%s,%d): %w", kbID, versionID, err)
	}

	// Step 2: the tombstone, before any byte is touched. A failure is reported but does
	// not stop the deletion: refusing to reclaim because the local table cannot be
	// written would leave bytes for a version nobody can name afterwards, which is the
	// outcome this table exists to prevent.
	var tombstoneErr error
	if c.cfg.Tombstones != nil {
		if err := c.cfg.Tombstones.Note(ctx, kbID, versionID, time.Now()); err != nil {
			tombstoneErr = fmt.Errorf("delete-version: record the tombstone for %s v%d: %w", kbID, versionID, err)
		}
	}

	// Steps 3-6: reclaim wherever the bytes are. Not fatal — see deleteOne.
	dropper := c.dropperFor()
	dropErr := c.retry(ctx, func() error {
		return dropper.DropVersion(ctx, kbID, versionID)
	})

	// Step 7: remove the metadata. Idempotent by contract (an absent version succeeds),
	// which is exactly what lets this record complete when the crash happened after this
	// step — the case Execute cannot reach.
	if err := c.retry(ctx, func() error {
		return c.cfg.RaftNode.ProposeRemoveVersionMeta(ctx, kbID, versionID)
	}); err != nil {
		return fmt.Errorf("delete-version: ProposeRemoveVersionMeta(%s,%d): %w", kbID, versionID, err)
	}

	// Step 8: WAL delete complete (idempotent). This is what stops Recover from handing
	// the record back on the next restart.
	if err := c.cfg.WAL.WriteVersionDeleteComplete(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("delete-version: WAL.WriteVersionDeleteComplete(%s,%d): %w", kbID, versionID, err)
	}

	// A succeeded reclaim leaves nothing owed, so the entry stops being PENDING and the
	// 30 s retry loop never sees it. MarkDone on an entry that was never noted is a
	// no-op, so a failed Note above does not make this wrong.
	if dropErr == nil && c.cfg.Tombstones != nil {
		if err := c.cfg.Tombstones.MarkDone(ctx, kbID, versionID, time.Now()); err != nil {
			tombstoneErr = fmt.Errorf("delete-version: mark the tombstone done for %s v%d: %w", kbID, versionID, err)
		}
	}

	var cause error
	if dropErr != nil {
		cause = fmt.Errorf("delete-version: %s v%d is deleted, but its data was not reclaimed everywhere (%w); each node finishes it from the deletion log",
			kbID, versionID, dropErr)
	}
	return errors.Join(cause, tombstoneErr)
}

// retry executes fn with exponential backoff up to MaxRetries times.
func (c *DeleteVersionCoordinatorImpl) retry(ctx context.Context, fn func() error) error {
	return retryWithBackoff(ctx, c.cfg.MaxRetries, c.cfg.RetryBaseIntervalMS, fn)
}

var _ DeleteVersionCoordinator = (*DeleteVersionCoordinatorImpl)(nil)
