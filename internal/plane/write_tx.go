// The write transaction itself (§3.3, §7.12): BEGIN through COMMIT, plus the
// resume path that re-runs a transaction whose BEGIN record is already durable.
//
// What lives here is the local half — the staged writes that are idempotent by
// construction. Fan-out to the other replicas is in fanout.go, and the catch-up
// replay that feeds a backfill is in backfill.go.

package plane

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// WriteVersionData runs the storage layer's write transaction for an
// already-allocated version: BEGIN (persisting the replay input) → the
// per-change storage writes → COMMIT. The control layer only allocated the
// version ID; the transaction that makes the data durable is entirely the
// storage layer's (Stratum_设计文档v13.md §7.12).
func (d *LocalDataPlane) WriteVersionData(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
	// Per-stage timings of one write transaction, at debug level.
	//
	// Why they are here: a dispatched write is bounded by the candidate budget
	// (plane.CoordinatorDispatcher.candidateBudget, floor 15 s) and, before this,
	// had no way to say where that budget went. The first measurement paid for
	// itself — fan-out spent 15.0 s here, exactly the budget, because ONE replica
	// was unreachable and every push inherited the whole budget (see
	// replicaPushTimeout).
	writeStart := time.Now()
	var tAcquire, tLocal time.Duration
	// The inside of local_us and fanout_us, reported alongside them. Both are
	// large enough to dominate a batch write's wall clock — together they were
	// 98% of a 2,000-document transaction (local p50 1.34 s + fanout p50 1.75 s of
	// a 3.15 s total) — and both were single opaque numbers. See localStages and
	// fanoutStages for what each sub-stage is.
	var local localStages
	var fin finishStages
	defer func() {
		d.logger.Debug("write: stage timings",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Int("changes", len(changes)),
			zap.Int64("acquire_us", tAcquire.Microseconds()),
			zap.Int64("local_us", tLocal.Microseconds()),
			zap.Int64("wal_begin_us", local.walBegin.Microseconds()),
			zap.Int64("storage_us", local.storage.Microseconds()),
			zap.Int64("wal_commit_us", local.walCommit.Microseconds()),
			zap.Int64("fanout_us", fin.fanTotal.Microseconds()),
			zap.Int64("resolve_us", fin.fan.resolve.Microseconds()),
			zap.Int64("push_us", fin.fan.push.Microseconds()),
			zap.Int("fanout_targets", fin.fan.targets),
			zap.Int64("report_us", fin.report.Microseconds()),
			zap.Int64("confirm_us", fin.confirm.Microseconds()),
			zap.Int64("total_us", time.Since(writeStart).Microseconds()))
	}()

	// One version is one Saga. Queue behind the writes already in flight for
	// this KB instead of letting them pile up on the storage nodes
	// (Stratum_设计文档v13.md §7.7). The wait honours ctx, so a caller that gives
	// up stops waiting rather than holding a place it no longer wants.
	stepStart := time.Now()
	if err := d.limiter.Acquire(ctx, kbID); err != nil {
		return err
	}
	tAcquire = time.Since(stepStart)
	defer d.limiter.Release(kbID)

	stepStart = time.Now()
	docIDs, err := d.writeLocalTransaction(ctx, kbID, versionID, parentVersionID, changes, &local)
	tLocal = time.Since(stepStart)
	if err != nil {
		// The storage layer reports that an attempt failed; the control layer
		// owns the terminal verdict (Stratum_设计文档v13.md §10.1).
		d.reportFailure(ctx, kbID, versionID, classifyLocalWriteFailure(err), fmt.Sprintf("local write failed: %v", err))
		return err
	}
	// Replicate to the other replicas and require a quorum before reporting the
	// version durable (v13 §7.1/§7.2). finishVersionWrite owns that tail — and the
	// crash-recovery path shares it, which is the point: a version resumed after a
	// crash must clear the same quorum bar as one that never crashed.
	return d.finishVersionWrite(ctx, kbID, versionID, len(changes), docIDs, &fin)
}

// classifyLocalWriteFailure decides how the control layer should treat a failed
// local write (Stratum_设计文档v13.md §10.1).
//
// Only the cases that no amount of retrying can fix are fatal: a knowledge base
// that is gone, or input the state machine will keep rejecting. Everything
// else — a full disk, an unavailable dependency, a timeout — is worth another
// attempt, possibly on another node.
func classifyLocalWriteFailure(err error) types.FailureClass {
	switch {
	case errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound),
		errors.Is(err, stratumerrors.ErrKnowledgeBaseDeleted),
		errors.Is(err, stratumerrors.ErrInvalidArgument),
		errors.Is(err, stratumerrors.ErrInvalidParentVersion),
		errors.Is(err, stratumerrors.ErrVersionNotFound):
		return types.FailureFatalGlobal
	default:
		return types.FailureTransient
	}
}

// localStages is where one local write transaction's time went, split out of
// plane's `write: stage timings` local_us.
//
// Why it is split: local_us is the second largest stage of a write transaction
// (p50 1.34 s for a 2,000-document batch, 2.68 s for 8,000) and was a single
// opaque number covering three unrelated things — write-ahead-log framing on both
// sides of the storage write, and the storage write itself. Which of them
// dominates decides what is worth optimising, so they are reported apart.
type localStages struct {
	walBegin  time.Duration
	storage   time.Duration
	walCommit time.Duration
}

// writeLocalTransaction frames and runs this node's half of the write:
// BEGIN (persisting the replay input) → the per-change storage writes →
// COMMIT. Returns the version's document-ID set. st, when non-nil, receives the
// per-stage split; callers that are not measuring pass nil.
//
// The storage step is also the durability barrier: by the time it returns, the
// chunk vectors it wrote are flushed to the vecstore's stable storage (see
// VersionWriteExecutor), so the COMMIT below — which is fsynced — cannot outlive
// the data it claims. Writing the commit without that ordering is the silent
// version of this bug: recovery trusts a present COMMIT and never replays.
func (d *LocalDataPlane) writeLocalTransaction(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange, st *localStages) ([]string, error) {
	if d.wal == nil || d.executor == nil {
		return nil, fmt.Errorf("plane: WriteVersionData: the storage write transaction is not configured")
	}
	// Refused BEFORE the WAL BEGIN, and that ordering is the point: a coordinator that
	// is still writing a version the control layer has already deleted would otherwise
	// resurrect it locally (the coordinator is a storage node too, so its own local write
	// goes through here), and it would then report the version durable — writing a row
	// for metadata that no longer exists.
	//
	// NOTE the small window this leaves: the deletion can land between this check and the
	// write, and the two do not share a lock. That is accepted — the window is
	// microseconds wide, and the tombstone table's retry loop plus the reverse
	// reconciliation both reclaim what slips through.
	if d.skipIfDeleted(ctx, kbID, versionID, "WriteVersionData") {
		return nil, fmt.Errorf("%w: %s v%d is known deleted on this node, so the write is abandoned",
			stratumerrors.ErrVersionDeleting, kbID, versionID)
	}
	beginStart := time.Now()
	err := d.wal.WriteBegin(ctx, kbID, parentVersionID, changes)
	if st != nil {
		st.walBegin = time.Since(beginStart)
	}
	if err != nil {
		return nil, fmt.Errorf("plane: WriteVersionData: WAL.WriteBegin: %w", err)
	}
	storageStart := time.Now()
	docIDs, err := d.executor.WriteVersionStorage(ctx, kbID, parentVersionID, versionID, changes)
	if st != nil {
		st.storage = time.Since(storageStart)
	}
	if err != nil {
		return nil, err
	}
	commitStart := time.Now()
	err = d.wal.WriteCommit(ctx, versionID)
	if st != nil {
		st.walCommit = time.Since(commitStart)
	}
	if err != nil {
		return nil, fmt.Errorf("plane: WriteVersionData: WAL.WriteCommit: %w", err)
	}
	d.dataLanded(kbID, versionID)
	return docIDs, nil
}

// ResumeVersionWrite re-runs the storage writes and commits without writing a
// second BEGIN: the transaction's BEGIN record is already durable (crash
// recovery drove this path). Same durability barrier as writeLocalTransaction —
// the storage step flushes the chunk vectors before the COMMIT vouches for them.
func (d *LocalDataPlane) ResumeVersionWrite(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
	if d.wal == nil || d.executor == nil {
		return fmt.Errorf("plane: ResumeVersionWrite: the storage write transaction is not configured")
	}
	// Crash recovery is the other way a deleted version comes back: the WAL holds a
	// BEGIN (or a version id) for a write that never reached COMMIT, and if that version
	// was deleted while this node was down, replaying it would write the whole batch
	// back under metadata that no longer exists. Refused here for the same reason as the
	// live path.
	if d.skipIfDeleted(ctx, kbID, versionID, "ResumeVersionWrite") {
		return fmt.Errorf("%w: %s v%d is known deleted on this node, so its replay is abandoned",
			stratumerrors.ErrVersionDeleting, kbID, versionID)
	}
	docIDs, err := d.executor.WriteVersionStorage(ctx, kbID, parentVersionID, versionID, changes)
	if err != nil {
		return err
	}
	if err := d.wal.WriteCommit(ctx, versionID); err != nil {
		return fmt.Errorf("plane: ResumeVersionWrite: WAL.WriteCommit: %w", err)
	}
	d.dataLanded(kbID, versionID)
	// The same tail the normal path runs, fan-out included. Recovery used to skip
	// straight to the durable report, so a crash during the local write produced a
	// version the control layer called durable while no other replica held it.
	return d.finishVersionWrite(ctx, kbID, versionID, len(docIDs), docIDs, nil)
}
