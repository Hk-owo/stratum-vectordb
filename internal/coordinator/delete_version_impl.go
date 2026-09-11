package coordinator

import (
	"context"
	"fmt"
	"math"
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

	// Step 2: drop the version's index (memory + vecstore side).
	if err := c.retry(ctx, func() error {
		return c.cfg.IndexManager.Discard(ctx, kbID, versionID)
	}); err != nil {
		return fmt.Errorf("delete-version: IndexManager.Discard(%s,%d): %w", kbID, versionID, err)
	}

	// Step 3: remove the version's document-ID list.
	if err := c.retry(ctx, func() error {
		return c.cfg.VersionDocList.DeleteByVersion(ctx, kbID, versionID)
	}); err != nil {
		return fmt.Errorf("delete-version: VersionDocList.DeleteByVersion(%s,%d): %w", kbID, versionID, err)
	}

	// Step 4: reclaim the version's MVCC document records — but only those
	// no surviving version still reads. A version's records are the read
	// source for every later version that never rewrote the same document
	// (WriteCoordinator writes only changed documents and ReadAt falls back
	// through versions), so reclaiming them while a survivor is alive would
	// silently make that survivor drop the document or read a stale value.
	// Records still visible at the smallest surviving later version are kept;
	// reclaiming the rest keeps the common case (removing the newest
	// versions) a full cleanup.
	anchor, err := c.visibleAnchor(ctx, kbID, versionID)
	if err != nil {
		return err
	}
	if err := c.retry(ctx, func() error {
		return c.cfg.DocStore.DeleteByVersionExceptVisibleFrom(ctx, kbID, versionID, anchor)
	}); err != nil {
		return fmt.Errorf("delete-version: DocStore.DeleteByVersionExceptVisibleFrom(%s,%d): %w", kbID, versionID, err)
	}

	// Step 5: remove the version's metadata from the Raft state machine.
	// Idempotent — re-proposing after a crash is safe.
	if err := c.retry(ctx, func() error {
		return c.cfg.RaftNode.ProposeRemoveVersionMeta(ctx, kbID, versionID)
	}); err != nil {
		return fmt.Errorf("delete-version: ProposeRemoveVersionMeta(%s,%d): %w", kbID, versionID, err)
	}

	// Step 6: drop the version's document bloom filter (cached entry and
	// on-disk file). The filter is a pure accelerator rebuilt from the
	// version's VersionDocList, so dropping it cannot change results — but
	// without this step one file per deleted version leaks forever, since
	// only whole-KB deletion reclaimed them.
	//
	// Ordering note: this runs after the metadata removal so no *new* request
	// can reach the version — QueryService resolves the version via
	// ListVersions first and rejects an unknown one. An in-flight query that
	// already passed that check can still reach VersionBloomStore.Get in the
	// meantime; its rebuild path then reads the (already deleted, Step 3)
	// VersionDocList, produces an empty filter, and persists it, leaving a
	// stray .bloom file for a version nobody can query. That is harmless (the
	// version is unreachable) and removed by the next whole-KB delete.
	//
	// Optional dependency: skipped when the bloom store is not wired.
	if c.cfg.VersionBloom != nil {
		if err := c.retry(ctx, func() error {
			return c.cfg.VersionBloom.DeleteByVersion(kbID, versionID)
		}); err != nil {
			return fmt.Errorf("delete-version: VersionBloom.DeleteByVersion(%s,%d): %w", kbID, versionID, err)
		}
	}

	// Step 7: WAL delete complete (idempotent).
	if err := c.cfg.WAL.WriteVersionDeleteComplete(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("delete-version: WAL.WriteVersionDeleteComplete(%s,%d): %w", kbID, versionID, err)
	}
	return nil
}

// visibleAnchor returns the smallest surviving version (not itself marked
// Deleting) with an ID greater than versionID, or 0 when none exists. It is
// the version whose reads decide which of versionID's MVCC records must be
// kept: an entry older versions no longer serve is exactly what that anchor
// reads, and any later survivor reads either that same entry or a newer one.
func (c *DeleteVersionCoordinatorImpl) visibleAnchor(ctx context.Context, kbID string, versionID int64) (int64, error) {
	versions, err := c.cfg.RaftNode.ListVersions(ctx, kbID)
	if err != nil {
		return 0, fmt.Errorf("delete-version: ListVersions(%s): %w", kbID, err)
	}
	anchor := int64(0)
	for _, v := range versions {
		if v.Deleting || v.VersionID <= versionID {
			continue
		}
		if anchor == 0 || v.VersionID < anchor {
			anchor = v.VersionID
		}
	}
	return anchor, nil
}

// retry executes fn with exponential backoff up to MaxRetries times.
func (c *DeleteVersionCoordinatorImpl) retry(ctx context.Context, fn func() error) error {
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

var _ DeleteVersionCoordinator = (*DeleteVersionCoordinatorImpl)(nil)
