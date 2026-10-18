package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
)

// DefaultDeletingKBSweepInterval is how often a node with a state machine looks for
// knowledge bases marked Deleting whose cleanup never finished.
//
// A minute, matching the version sweep: the pass reads the state machine the node
// already holds — ids only, no I/O and no RPC — so its cost is one RLock over the
// KB table.
const DefaultDeletingKBSweepInterval = time.Minute

// DeletingKBReader answers the sweep's whole question: which knowledge bases are
// still marked Deleting, ids only.
type DeletingKBReader interface {
	DeletingKBIDs(ctx context.Context) ([]string, error)
}

// DeletingKBSweeper finishes knowledge-base deletions that were marked but never
// completed.
//
// The hole it closes is the KB-level twin of the one DeletingVersionSweeper
// closes (M3 of docs/code-review-2026-10-18.md): DeleteKnowledgeBase proposes the
// Deleting status in the state machine — a Raft command, so it is durable,
// replicated, and already answered to the caller — and only THEN starts the
// goroutine that runs the cleanup, which is what writes the WAL's delete mark. A
// process that dies in between leaves the KB in DELETING and nothing that would
// finish it:
//
//   - the WAL has no record to resume from — that mark is written inside the
//     goroutine, after the crash point;
//   - nothing else scans for it: the KB is neither gone from the metadata (which
//     is what reconciliation looks for) nor reported by GetSystemStatus, which
//     collects only KBStatusDeleteFailed.
//
// Until this pass existed the only remedy was an operator re-calling
// DeleteKnowledgeBase with the same arguments, having first guessed that the KB
// was stuck.
//
// Unlike the version sweep it needs no dirty set: a KB deletion is a rare event
// and the KB table is small, so the pass asks for the ids directly instead of
// paying for a change-tracking path in the proposal code. It performs the same
// idempotent cleanup the original call did, so a half-finished deletion is
// RESUMED rather than restarted.
//
// Every node with its own state machine runs it, so two nodes can attempt the same
// knowledge base at once. That is safe for the same reason the cleanup is idempotent
// per step, and it is the same trade the version sweep makes.
type DeletingKBSweeper struct {
	kbs      DeletingKBReader
	cleanup  DeleteCoordinator
	interval time.Duration
	logger   *zap.Logger
}

// NewDeletingKBSweeper constructs a sweeper. A non-positive interval takes the
// default; a nil logger runs silently.
func NewDeletingKBSweeper(
	kbs DeletingKBReader,
	cleanup DeleteCoordinator,
	interval time.Duration,
	logger *zap.Logger,
) *DeletingKBSweeper {
	if interval <= 0 {
		interval = DefaultDeletingKBSweepInterval
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &DeletingKBSweeper{kbs: kbs, cleanup: cleanup, interval: interval, logger: logger}
}

// Run sweeps until ctx is done. A failed pass never stops the loop: the next tick
// asks the same question again.
func (s *DeletingKBSweeper) Run(ctx context.Context) {
	if s.kbs == nil || s.cleanup == nil {
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
				s.logger.Warn("kb delete sweep: pass did not finish; the next tick retries it", zap.Error(err))
			}
			if resumed > 0 {
				s.logger.Info("kb delete sweep: resumed unfinished knowledge-base deletions",
					zap.Int("knowledge_bases", resumed))
			}
		}
	}
}

// SweepOnce runs one pass and returns how many knowledge bases it resumed.
//
// One failure does not stop the rest, and it is reported rather than swallowed:
// "I could not enumerate" is not "there is nothing to do".
func (s *DeletingKBSweeper) SweepOnce(ctx context.Context) (int, error) {
	if s.kbs == nil {
		return 0, nil
	}
	kbIDs, err := s.kbs.DeletingKBIDs(ctx)
	if err != nil {
		return 0, fmt.Errorf("kb delete sweep: list knowledge bases marked deleting: %w", err)
	}

	resumed := 0
	var firstErr error
	note := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}
	for _, kbID := range kbIDs {
		if err := ctx.Err(); err != nil {
			// Shutting down: the next start runs this pass again, so nothing is
			// lost by leaving the rest for it.
			return resumed, err
		}
		if err := s.cleanup.Execute(ctx, kbID); err != nil {
			if errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
				// The deletion finished between the read and this call: a normal
				// end, not a failure to report.
				continue
			}
			note(fmt.Errorf("kb delete sweep: resume %s: %w", kbID, err))
			continue
		}
		resumed++
	}
	return resumed, firstErr
}
