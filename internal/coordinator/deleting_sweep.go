package coordinator

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"stratum/internal/types"
)

// DefaultDeletingVersionSweepInterval is how often a node with a state machine looks
// for versions that are marked Deleting but whose cleanup never finished.
//
// A minute, following the terminal-reclaim queue: this pass walks the state machine
// the node already holds — no I/O, no RPC — so its cost is a rounding error, and what
// it repairs is the one state a delete can reach and never leave.
const DefaultDeletingVersionSweepInterval = time.Minute

// VersionLister is the metadata read the sweep needs: which knowledge bases exist, and
// which of their versions are still marked Deleting. Declared here rather than taken
// as a raft.RaftNode so the sweep depends on nothing but these two calls.
type VersionLister interface {
	ListKnowledgeBases(ctx context.Context) ([]types.KnowledgeBaseMeta, error)
	ListVersions(ctx context.Context, kbID string) ([]types.VersionMeta, error)
}

// DeletingVersionSweeper finishes deletions that were marked but never completed.
//
// The hole it closes: DeleteVersion marks the version set in the state machine (a Raft
// command — durable, replicated, and the moment the API answers the caller) and only
// THEN hands the cleanup to a background goroutine. A process that dies in between
// leaves the version marked Deleting with nothing that would ever finish it:
//
//   - the WAL has no record to resume from — the delete mark is written inside that
//     goroutine, after the crash point;
//   - the reverse reconciliation cannot see it either — that pass looks for versions
//     the node holds and the metadata does NOT have, and this one is still in the
//     metadata, marked Deleting.
//
// Until this pass existed, the only remedy was an operator reading deleting_versions
// out of GetSystemStatus and calling DeleteVersion again.
//
// It runs where the metadata lives (a node with its own state machine) and performs the
// same idempotent cleanup the original call did, so a half-finished deletion is
// RESUMED rather than restarted, and a version that finished in the meantime makes the
// pass a no-op. That is also why one Execute per knowledge base is enough: it re-scans
// all of that knowledge base's Deleting versions itself.
type DeletingVersionSweeper struct {
	meta     VersionLister
	cleanup  DeleteVersionCoordinator
	interval time.Duration
	logger   *zap.Logger
}

// NewDeletingVersionSweeper constructs a sweeper. A non-positive interval takes the
// default; a nil logger runs silently.
func NewDeletingVersionSweeper(meta VersionLister, cleanup DeleteVersionCoordinator, interval time.Duration, logger *zap.Logger) *DeletingVersionSweeper {
	if interval <= 0 {
		interval = DefaultDeletingVersionSweepInterval
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &DeletingVersionSweeper{meta: meta, cleanup: cleanup, interval: interval, logger: logger}
}

// Run sweeps until ctx is done. A failed pass never stops the loop: it says nothing
// about the next one, and the next tick tries again.
func (s *DeletingVersionSweeper) Run(ctx context.Context) {
	if s.meta == nil || s.cleanup == nil {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resumed, err := s.SweepOnce(ctx)
			if err != nil {
				s.logger.Warn("delete sweep: pass did not finish; the next tick retries it", zap.Error(err))
			}
			if resumed > 0 {
				s.logger.Info("delete sweep: resumed unfinished version deletions",
					zap.Int("knowledge_bases", resumed))
			}
		}
	}
}

// SweepOnce runs one pass and returns how many knowledge bases it resumed.
//
// A knowledge base whose version list cannot be read is skipped and named in the
// returned error rather than treated as "nothing to do": "I cannot enumerate" is not
// "there is nothing there". One failure does not stop the rest — the pass is
// idempotent, and the next tick retries what was missed.
func (s *DeletingVersionSweeper) SweepOnce(ctx context.Context) (int, error) {
	kbs, err := s.meta.ListKnowledgeBases(ctx)
	if err != nil {
		return 0, fmt.Errorf("delete sweep: list knowledge bases: %w", err)
	}
	resumed := 0
	var firstErr error
	note := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}
	for _, kb := range kbs {
		if err := ctx.Err(); err != nil {
			return resumed, err
		}
		versions, err := s.meta.ListVersions(ctx, kb.KBID)
		if err != nil {
			note(fmt.Errorf("delete sweep: list versions of %s: %w", kb.KBID, err))
			continue
		}
		deleting := 0
		for _, v := range versions {
			if v.Deleting {
				deleting++
			}
		}
		if deleting == 0 {
			continue
		}
		if err := s.cleanup.Execute(ctx, kb.KBID); err != nil {
			note(fmt.Errorf("delete sweep: resume %s (%d versions marked Deleting): %w", kb.KBID, deleting, err))
			continue
		}
		resumed++
	}
	return resumed, firstErr
}
