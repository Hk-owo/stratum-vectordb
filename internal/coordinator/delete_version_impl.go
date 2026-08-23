package coordinator

import (
	"context"
	"fmt"
	"math"
	"time"

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

	// Step 4: physically remove the version's MVCC document records.
	if err := c.retry(ctx, func() error {
		return c.cfg.DocStore.DeleteByVersion(ctx, kbID, versionID)
	}); err != nil {
		return fmt.Errorf("delete-version: DocStore.DeleteByVersion(%s,%d): %w", kbID, versionID, err)
	}

	// Step 5: remove the version's metadata from the Raft state machine.
	// Idempotent — re-proposing after a crash is safe.
	if err := c.retry(ctx, func() error {
		return c.cfg.RaftNode.ProposeRemoveVersionMeta(ctx, kbID, versionID)
	}); err != nil {
		return fmt.Errorf("delete-version: ProposeRemoveVersionMeta(%s,%d): %w", kbID, versionID, err)
	}

	// Step 6: WAL delete complete (idempotent).
	if err := c.cfg.WAL.WriteVersionDeleteComplete(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("delete-version: WAL.WriteVersionDeleteComplete(%s,%d): %w", kbID, versionID, err)
	}
	return nil
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
