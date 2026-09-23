package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
)

// DefaultDeletingVersionSweepInterval is how often a node with a state machine looks
// for versions that are marked Deleting but whose cleanup never finished.
//
// A minute, following the terminal-reclaim queue: this pass walks the state machine the
// node already holds — no I/O, no RPC — so its cost is a rounding error, and what it
// repairs is the one state a delete can reach and never leave.
//
// "A rounding error" is a claim about cost, not about scale: the pass reads ids only (see
// DeletingVersionReader) and only for the knowledge bases something has happened to (see
// DirtyDeletingTracker), so a node nobody is deleting on pays nothing.
const DefaultDeletingVersionSweepInterval = time.Minute

// DeletingVersionReader answers the sweep's question for ONE knowledge base: which of its
// versions are still marked Deleting, ids only.
//
// Ids only, because that is all the caller needs — whether there is anything to resume.
// The wider read (ListVersions, which copies a VersionMeta per version) costs about twice
// as much per version for information this pass never looks at.
type DeletingVersionReader interface {
	DeletingVersionIDs(ctx context.Context, kbID string) ([]int64, error)
}

// DirtyDeletingTracker carries "which knowledge bases have had a deletion marked since
// the last pass", and takes one back when a pass could not finish with it.
//
// A set of knowledge bases rather than a version-id watermark, and that is the point: a
// deletion's marks can be anywhere in the chain (SINGLE marks a middle version, ANCESTORS
// a prefix), so "everything at or below V is clean" is not a fact a version id can
// express — a middle version marked later would sit below the watermark for good. The set
// only ever has to answer "ask again about this one", which any mark can.
type DirtyDeletingTracker interface {
	// TakeDirtyDeletingKBs returns the knowledge bases that have changed since the last
	// call, and clears the set so two passes cannot both act on one change.
	TakeDirtyDeletingKBs() []string
	// MarkDeletingDirty puts one back, for a pass that did not finish with it.
	MarkDeletingDirty(kbID string)
}

// DeletingVersionSweeper finishes deletions that were marked but never completed.
//
// The hole it closes: DeleteVersion marks the version set in the state machine (a Raft
// command — durable, replicated, and the moment the API answers the caller) and only
// THEN hands the cleanup to a background goroutine. A process that dies in between leaves
// the version marked Deleting with nothing that would ever finish it:
//
//   - the WAL has no record to resume from — the delete mark is written inside that
//     goroutine, after the crash point;
//   - the reverse reconciliation cannot see it either — that pass looks for versions the
//     node holds and the metadata does NOT have, and this one is still in the metadata,
//     marked Deleting.
//
// Until this pass existed, the only remedy was an operator reading deleting_versions out
// of GetSystemStatus and calling DeleteVersion again.
//
// It runs where the metadata lives (a node with its own state machine) and performs the
// same idempotent cleanup the original call did, so a half-finished deletion is RESUMED
// rather than restarted, and a version that finished in the meantime makes the pass a
// no-op. One Execute per knowledge base is enough: it re-scans all of that knowledge
// base's Deleting versions itself.
type DeletingVersionSweeper struct {
	versions DeletingVersionReader
	dirty    DirtyDeletingTracker
	cleanup  DeleteVersionCoordinator
	interval time.Duration
	logger   *zap.Logger
}

// NewDeletingVersionSweeper constructs a sweeper. A non-positive interval takes the
// default; a nil logger runs silently.
func NewDeletingVersionSweeper(
	versions DeletingVersionReader,
	dirty DirtyDeletingTracker,
	cleanup DeleteVersionCoordinator,
	interval time.Duration,
	logger *zap.Logger,
) *DeletingVersionSweeper {
	if interval <= 0 {
		interval = DefaultDeletingVersionSweepInterval
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &DeletingVersionSweeper{
		versions: versions,
		dirty:    dirty,
		cleanup:  cleanup,
		interval: interval,
		logger:   logger,
	}
}

// Run sweeps until ctx is done. A failed pass never stops the loop: it says nothing about
// the next one, and the next tick tries again — the knowledge base a failed pass could not
// finish with goes back into the dirty set for exactly that reason.
func (s *DeletingVersionSweeper) Run(ctx context.Context) {
	if s.versions == nil || s.dirty == nil || s.cleanup == nil {
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

// SweepOnce runs one pass over the knowledge bases that have changed since the last pass,
// and returns how many it resumed.
//
// The dirty set is what selects them, so a node where nothing has been deleted scans
// nothing at all. The caller is responsible for starting that set off conservatively —
// RaftNodeImpl.MarkAllDeletingDirty at start-up and after a snapshot install — because a
// node that has just started (or just been handed a snapshot) may have MISSED marks
// rather than received them.
//
// A knowledge base whose versions cannot be read is skipped and named in the returned
// error rather than treated as "nothing to do": "I cannot enumerate" is not "there is
// nothing there". One failure does not stop the rest, and a cleanup that fails goes back
// into the dirty set — that pass did not finish with it.
func (s *DeletingVersionSweeper) SweepOnce(ctx context.Context) (int, error) {
	if s.dirty == nil {
		return 0, nil
	}
	kbs := s.dirty.TakeDirtyDeletingKBs()
	if len(kbs) == 0 {
		return 0, nil
	}
	// Sorted so the order — and any log line or test asserting on it — does not depend on
	// Go's map iteration order.
	sort.Strings(kbs)

	resumed := 0
	var firstErr error
	note := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}
	for _, kbID := range kbs {
		if err := ctx.Err(); err != nil {
			// Whatever is left is deliberately NOT put back: the process is shutting
			// down, and the next start treats every knowledge base as dirty
			// (MarkAllDeletingDirty), so nothing is lost by leaving it out here.
			return resumed, err
		}
		ids, err := s.versions.DeletingVersionIDs(ctx, kbID)
		if err != nil {
			if errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
				// The knowledge base is gone (a whole-KB delete ends by removing it),
				// which is a normal end rather than a failure to report.
				continue
			}
			note(fmt.Errorf("delete sweep: read the deleting versions of %s: %w", kbID, err))
			s.dirty.MarkDeletingDirty(kbID)
			continue
		}
		if len(ids) == 0 {
			// Nothing left to resume: the cleanup finished between the mark and this
			// pass, or the version was discarded rather than deleted.
			continue
		}
		if err := s.cleanup.Execute(ctx, kbID); err != nil {
			note(fmt.Errorf("delete sweep: resume %s (%d versions marked Deleting): %w", kbID, len(ids), err))
			s.dirty.MarkDeletingDirty(kbID)
			continue
		}
		resumed++
	}
	return resumed, firstErr
}
