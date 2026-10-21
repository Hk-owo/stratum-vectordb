package plane

import (
	"context"
	"time"

	"go.uber.org/zap"

	"stratum/internal/tombstone"
	"stratum/internal/types"
)

// VersionTombstones is this node's own record of deletions it has been TOLD about
// (internal/tombstone). The plane reads it on every landing path, drains its
// unfinished entries, and keeps the watermark that makes the control layer's deletion
// log usable incrementally.
//
// It is a LOCAL fact, and the division of labour with the reverse reconciliation is
// the point rather than an overlap:
//
//   - the reconciliation asks the control layer "which of these versions are still
//     alive?" — no retention, no gaps, and the only thing that can find a node nobody
//     ever told;
//   - the tombstone table says "I was told, and I still owe this deletion" — it works
//     with no control tier reachable, on the landing path, which is where a late push
//     can still be refused after the version's metadata is gone.
type VersionTombstones interface {
	// Contains reports whether the version is known deleted here, in either state.
	Contains(ctx context.Context, kbID string, versionID int64) (bool, error)
	// ContainsKB reports whether the whole knowledge base is known deleted.
	ContainsKB(ctx context.Context, kbID string) (bool, error)
	// Pending lists the entries whose local reclaim has not finished.
	Pending(ctx context.Context) ([]tombstone.Entry, error)
	// MarkDone records that this node finished reclaiming the version's bytes.
	MarkDone(ctx context.Context, kbID string, versionID int64, at time.Time) error
	// Prune drops finished entries older than grace.
	Prune(ctx context.Context, now time.Time, grace time.Duration) (int, error)
	// SyncSeq is this node's watermark into the control layer's deletion log.
	SyncSeq(ctx context.Context) (tombstone.SyncState, bool, error)
	// NoteBatch records a page of deletions AND advances the watermark atomically.
	NoteBatch(ctx context.Context, state tombstone.SyncState, entries []tombstone.DeletedVersion) error
}

// DeletionLogSource is the control layer's BOUNDED deletion log, read incrementally
// (raft.RaftNodeImpl answers locally; *raft.RemoteRaftNode asks the control tier).
//
// The log is an accelerator, never a judgement: a consumer that cannot be served
// incrementally — watermark older than the retained window, epoch mismatch, no
// watermark at all — falls back to the live-chain read and loses nothing. See
// ReconcileDeletions for the whole rule.
type DeletionLogSource interface {
	DeletionsSince(ctx context.Context, afterSeq int64, limit int) (entries []types.DeletedVersion, latest, oldest int64, epoch string, err error)
}

// KBStorageDropper reclaims a whole knowledge base's physical data on this node.
// Needed because a KB deletion is ONE log entry (VersionID 0), not one per version.
type KBStorageDropper interface {
	DropKnowledgeBase(ctx context.Context, kbID string) error
}

// DefaultDeletedVersionRetryInterval is how often a node retries the local reclaim
// of deletions it was told about.
//
// Half a minute, and deliberately much shorter than the reconciliation's cadence:
// this pass has no network in it (it reads a local table and does a local prefix
// delete), so it is cheap enough to be the FAST path for "I was told, my cleanup
// failed, nobody is going to tell me again".
const DefaultDeletedVersionRetryInterval = 30 * time.Second

const (
	// DefaultDeletionLogPage caps one incremental read.
	DefaultDeletionLogPage = 512
	// defaultChainReconcileMinAge is the floor between two FULL passes triggered by
	// local activity. The incremental tick runs every minute, and "something landed
	// here" would otherwise mean a range scan every minute on a busy node — the cost
	// this whole mechanism exists to remove.
	defaultChainReconcileMinAge = 5 * time.Minute
	// deletionLogEpochMismatch is the reason string a re-judge reports when the
	// control layer's epoch differs from the watermark's.
	deletionLogEpochMismatch = "epoch-mismatch"
	// chainDisabled is the reason string for "a re-judge was needed but this node is
	// not allowed to run one".
	chainDisabled = "chain-disabled"
	// maxDeletionPagesPerSweep bounds one pass's pagination. A bulk deletion larger than
	// this is finished by the following ticks; the bound is what keeps one pass from
	// growing without limit while a busy cluster keeps deleting.
	maxDeletionPagesPerSweep = 8
	// noEntryAfter asks for the log's WINDOW without any entries: every real sequence
	// number is <= it.
	noEntryAfter = int64(1<<62 - 1)
)

// page returns the configured page size.
func (d *LocalDataPlane) deletionPage() int {
	if d.deletionLogPage > 0 {
		return d.deletionLogPage
	}
	return DefaultDeletionLogPage
}

// chainAvailable reports whether a FULL live-chain pass may run right now: the
// operator has not switched it off, and enough time has passed since the last one for
// THIS trigger's patience.
func (d *LocalDataPlane) chainAvailable(minAge time.Duration) bool {
	return d.chainAvailableTo(d.chainReconcileEnabled, minAge)
}

// chainAvailableTo is chainAvailable with the operator's switch supplied by the caller,
// so a pass authorised differently — the start-up pass, which belongs to
// reconcile.deleted_versions rather than to the periodic switch — can ask the same
// question. The pacing is identical; only who said "yes" changes.
func (d *LocalDataPlane) chainAvailableTo(allowChain bool, minAge time.Duration) bool {
	if !allowChain {
		return false
	}
	if d.lastChainReconcile.IsZero() {
		return true
	}
	return time.Since(d.lastChainReconcile) >= minAge
}

// DeletionSweepResult is what one sweep did, for the debug line that is the only way
// to tell whether the incremental path is earning its keep.
type DeletionSweepResult struct {
	// Source is "incremental", "chain" (the live-chain fallback) or "skipped".
	Source string
	// Fallback names why the chain was used, when it was.
	Fallback       string
	LocalSeq       int64
	LatestSeq      int64
	Truncated      bool
	Entries        int
	KnowledgeBases int
	Reclaimed      int
	// BehindLeader is set when this node's watermark is AHEAD of the replica it asked
	// — a lagging reader, not a rollback. Distinguished from a rollback by the epoch.
	BehindLeader bool
}

// SetVersionTombstones wires this node's deletion table. Optional: without it
// everything here is a no-op and the reverse reconciliation is the only path, which
// is the behaviour before this table existed.
func (d *LocalDataPlane) SetVersionTombstones(t VersionTombstones) {
	d.tombstones = t
}

// skipIfDeleted reports whether the version must not be written on this node, because
// this node already knows it (or its whole knowledge base) is deleted.
//
// "I could not read the table" answers FALSE and says so: refusing data on an
// unreadable local answer would turn a broken table into a stalled replication path,
// while accepting it is the pre-table behaviour and still covered by the
// reconciliation.
func (d *LocalDataPlane) skipIfDeleted(ctx context.Context, kbID string, versionID int64, path string) bool {
	if d.tombstones == nil {
		return false
	}
	// The KB-level check comes first: a deleted knowledge base refuses every version
	// of it, and it has no per-version entry to find.
	if gone, err := d.tombstones.ContainsKB(ctx, kbID); err == nil && gone {
		d.logger.Info("plane: skipping a version of a knowledge base this node knows was deleted",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.String("path", path))
		return true
	}
	gone, err := d.tombstones.Contains(ctx, kbID, versionID)
	if err != nil {
		d.logger.Warn("plane: could not read the tombstone table; treating the version as alive",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.String("path", path), zap.Error(err))
		return false
	}
	if gone {
		d.logger.Info("plane: skipping a version this node knows was deleted",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.String("path", path))
	}
	return gone
}

// StartDeletedVersionRetry runs the local retry of unfinished deletions until ctx
// ends, then prunes the finished entries past their grace window on each pass.
func (d *LocalDataPlane) StartDeletedVersionRetry(ctx context.Context, interval, grace time.Duration) {
	if d.tombstones == nil || d.dropper == nil {
		return
	}
	if interval <= 0 {
		interval = DefaultDeletedVersionRetryInterval
	}
	if grace <= 0 {
		grace = tombstone.DefaultGrace
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := d.RetryPendingDeletions(ctx, grace); err != nil {
					d.logger.Warn("plane: retrying unfinished deletions did not finish", zap.Error(err))
				}
			}
		}
	}()
}

// RetryPendingDeletions runs one pass: reclaim every entry whose tombstone is still
// PENDING, mark each one done, then drop finished entries older than grace. It
// returns how many reclaims SUCCEEDED.
//
// One failure does not stop the rest — the entries are independent and the reclaim is
// idempotent — and a failure leaves its entry PENDING for the next pass, which is
// exactly the state that says "still owed".
func (d *LocalDataPlane) RetryPendingDeletions(ctx context.Context, grace time.Duration) (int, error) {
	if d.tombstones == nil || d.dropper == nil {
		return 0, nil
	}
	pending, err := d.tombstones.Pending(ctx)
	if err != nil {
		return 0, err
	}

	reclaimed := 0
	var firstErr error
	note := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, entry := range pending {
		if err := ctx.Err(); err != nil {
			return reclaimed, err
		}
		var dropErr error
		if entry.VersionID == 0 {
			// The whole knowledge base: one entry, one KB-wide prefix delete.
			if d.kbDropper == nil {
				// Not wired means this node cannot do it; leaving the entry PENDING
				// keeps the debt visible instead of pretending it is paid.
				note(errKBDropperMissing)
				continue
			}
			dropErr = d.kbDropper.DropKnowledgeBase(ctx, entry.KBID)
		} else {
			dropErr = d.dropper.DropVersionStorage(ctx, entry.KBID, entry.VersionID)
		}
		if dropErr != nil {
			d.logger.Warn("plane: unfinished deletion failed again; the next pass retries it",
				zap.String("kb_id", entry.KBID), zap.Int64("version_id", entry.VersionID), zap.Error(dropErr))
			note(dropErr)
			continue
		}
		if err := d.tombstones.MarkDone(ctx, entry.KBID, entry.VersionID, time.Now()); err != nil {
			// The bytes are gone, so this is bookkeeping: leaving it PENDING costs one
			// more idempotent prefix delete on the next pass.
			note(err)
			continue
		}
		reclaimed++
	}

	if _, err := d.tombstones.Prune(ctx, time.Now(), grace); err != nil {
		note(err)
	}
	return reclaimed, firstErr
}

var errKBDropperMissing = errorString("plane: a whole-KB deletion is owed here but no KB dropper is wired")

type errorString string

func (e errorString) Error() string { return string(e) }

// ReconcileDeletions is one pass of the deletion reconciliation, and the place the
// whole rule lives:
//
//  0. A full live-chain pass is FORCED when the last one is older than
//     maxChainReconcileAge. The incremental path may not starve it: "nothing new in
//     the log" proves nothing about leftovers the log cannot describe (a late push
//     that arrived after the grace window, a table rebuilt from nothing).
//  1. No watermark → chain. A node that has never synced cannot be served
//     incrementally, and must not be told "you are up to date" either.
//  2. Watermark present → one incremental read.
//     - epoch differs      → chain (another cluster's sequence numbers)
//     - truncated          → chain (the entries it needs are gone)
//     - local > latest     → skip: this replica is behind, not the cluster
//     - local == latest    → skip: nothing was deleted
//     - entries            → NoteBatch per page (entries + watermark atomically),
//     then drain them locally
//
// The chain path always finishes by aligning the watermark, because the live-chain
// judgement has just covered exactly what the log had pruned away.
func (d *LocalDataPlane) ReconcileDeletions(ctx context.Context, meta MetadataLister) (DeletionSweepResult, error) {
	return d.reconcileDeletions(ctx, meta, d.chainReconcileEnabled)
}

// ReconcileDeletionsAtStartup is the START-UP pass: the same rule, with the live-chain
// fallback unconditionally available.
//
// Why it is authorised differently from the periodic pass:
//
//   - DisableChainReconcile (reconcile.deleted_versions_periodic) is a decision about the
//     PERIODIC full pass — "do not re-scan a whole range every tick". The start-up pass is
//     a separate decision, made by reconcile.deleted_versions, and callers only take this
//     path when that one is on.
//   - It is also the only pass that can ESTABLISH a watermark on this node. Without one
//     every read falls back to the chain (rule 1 of ReconcileDeletions), so honouring the
//     periodic switch here would leave a node that wants only the cheap incremental path
//     with no way to ever reach it.
//
// Running it at boot is what makes the tombstone table's cross-restart bookkeeping whole:
// the pass drains entries a crash left PENDING and aligns the watermark, so the first
// periodic tick reads one page instead of repeating the full live-chain scan. The old
// start-up call did neither — it reclaimed, but left the watermark untouched, so the next
// tick scanned the same range again.
func (d *LocalDataPlane) ReconcileDeletionsAtStartup(ctx context.Context, meta MetadataLister) (DeletionSweepResult, error) {
	if d.tombstones == nil {
		// No table wired: the sweep still runs, it just cannot record what it finds or
		// retry it later. Falling back to the live-chain judgement keeps
		// `reconcile.deleted_versions` meaning what it says — the table is an
		// accelerator and a cross-restart ledger, never the authorisation. Returning an
		// empty result instead would make the setting silently inert on a node whose
		// table failed to open.
		reclaimed, err := d.ReconcileDeletedVersions(ctx, meta)
		return DeletionSweepResult{Source: "chain", Fallback: "no-tombstone-table", Reclaimed: reclaimed}, err
	}
	return d.reconcileDeletions(ctx, meta, true)
}

// reconcileDeletions is the shared body. allowChain is the ONLY thing the two entry
// points disagree about: the periodic pass takes the operator's switch
// (chainReconcileEnabled), the start-up pass is authorised by reconcile.deleted_versions
// and therefore always allows the fallback.
func (d *LocalDataPlane) reconcileDeletions(ctx context.Context, meta MetadataLister, allowChain bool) (DeletionSweepResult, error) {
	var res DeletionSweepResult
	if d.tombstones == nil {
		return res, nil
	}

	state, hasState, err := d.tombstones.SyncSeq(ctx)
	if err != nil {
		return res, err
	}
	res.LocalSeq = state.Seq

	// 0. Three things force the full live-chain pass, and all three come first so no
	// amount of incremental traffic can push them out:
	//   - the local set changed since the last pass (a landing, which is the only way a
	//     late push or an unfinished reclaim appears — the log cannot describe those);
	//   - there is no watermark, so this node cannot be served incrementally at all and
	//     must not be told "you are up to date";
	//   - the last full pass is older than the bound.
	if d.peekDeletedReconcileDirty() && d.chainAvailableTo(allowChain, defaultChainReconcileMinAge) {
		return d.reconcileFromChain(ctx, meta, res, "local-holdings-changed")
	}
	if !hasState || d.deletions == nil {
		reason := "no-watermark"
		if d.deletions == nil {
			reason = "no-deletion-log"
		}
		if !allowChain {
			res.Source, res.Fallback = "skipped", chainDisabled
			return res, nil
		}
		return d.reconcileFromChain(ctx, meta, res, reason)
	}
	if d.chainAvailableTo(allowChain, d.maxChainReconcileAge()) {
		return d.reconcileFromChain(ctx, meta, res, "chain-reconcile-due")
	}

	// 2. Page through the log until it is drained (bounded — see
	// maxDeletionPagesPerSweep). One page per tick would let a bulk deletion take many
	// minutes to become visible; draining a bounded number per tick keeps each pass
	// short while making the backlog converge in one.
	page := d.deletionPage()
	for pages := 0; pages < maxDeletionPagesPerSweep; pages++ {
		entries, latest, oldest, epoch, err := d.deletions.DeletionsSince(ctx, state.Seq, page)
		if err != nil {
			// A control tier that cannot be read now says nothing about the next pass;
			// the chain path would need the same tier.
			return res, err
		}
		res.LatestSeq = latest

		truncated := latest > state.Seq && (oldest == 0 || oldest > state.Seq+1)
		if truncated || epoch != state.Epoch {
			res.Truncated = truncated
			reason := deletionLogEpochMismatch
			if truncated {
				reason = "log-truncated"
			}
			if !allowChain {
				res.Source, res.Fallback = "skipped", chainDisabled
				return res, nil
			}
			return d.reconcileFromChain(ctx, meta, res, reason)
		}

		if state.Seq > latest {
			// This replica has applied fewer entries than this node already consumed. Not
			// a rollback (the epoch matched) — just a lagging reader. Skip and keep the
			// watermark; the forced full pass bounds how long this can go on.
			if res.Entries == 0 {
				res.Source = "skipped"
				res.BehindLeader = true
			}
			break
		}
		if len(entries) == 0 {
			if res.Entries == 0 {
				res.Source = "skipped"
			}
			break
		}

		res.Source = "incremental"
		res.Entries += len(entries)
		batch := make([]tombstone.DeletedVersion, 0, len(entries))
		for _, entry := range entries {
			batch = append(batch, tombstone.DeletedVersion{Seq: entry.Seq, KBID: entry.KBID, VersionID: entry.VersionID})
		}
		next := tombstone.SyncState{Epoch: epoch, Seq: entries[len(entries)-1].Seq}
		if err := d.tombstones.NoteBatch(ctx, next, batch); err != nil {
			// Nothing of this page was recorded (the batch is atomic), so the watermark
			// did not move either: the next pass re-reads the same page.
			return res, err
		}
		state, res.LocalSeq = next, next.Seq

		if len(entries) < page {
			break // the log is drained
		}
	}

	reclaimed, err := d.RetryPendingDeletions(ctx, tombstone.DefaultGrace)
	res.Reclaimed = reclaimed
	return res, err
}

// reconcileFromChain runs the live-chain judgement and aligns the watermark to what
// the control layer currently reports.
func (d *LocalDataPlane) reconcileFromChain(ctx context.Context, meta MetadataLister, res DeletionSweepResult, reason string) (DeletionSweepResult, error) {
	res.Source = "chain"
	res.Fallback = reason
	// Consume the trigger now that the pass it exists for is running.
	d.takeDeletedReconcileDirty()

	// Snapshot the log's position BEFORE judging, and align to THIS value afterwards.
	//
	// The order is the whole point (the same reason a watch starts at the resource
	// version a list returned): a deletion that completes WHILE this pass runs is not in
	// its judgement — the version was still alive when the pass read the chain — so
	// aligning to a position read AFTERWARDS would skip it. Snapshotting first leaves
	// that deletion above the watermark, where the next incremental read finds it.
	var aligned tombstone.SyncState
	var haveAligned bool
	if d.deletions != nil {
		if _, latest, _, epoch, err := d.deletions.DeletionsSince(ctx, noEntryAfter, 1); err == nil && epoch != "" {
			aligned, haveAligned = tombstone.SyncState{Epoch: epoch, Seq: latest}, true
		}
	}

	leftovers, leftoversErr := d.DeletedVersionLeftovers(ctx, meta)
	res.KnowledgeBases = len(leftovers)
	reclaimed := 0
	var reclaimErr error
	if leftovers != nil {
		reclaimed, reclaimErr = d.ReconcileDeletedVersions(ctx, meta)
	}
	res.Reclaimed = reclaimed

	// Align to the position snapshotted BEFORE the pass. Aligning even when some
	// reclaim failed is deliberate: the judgement COVERED those versions (it named
	// them), and a failed reclaim is already remembered as PENDING in the table, which
	// is what the retry loop drains. Not aligning would re-read a range whose answer is
	// already known.
	if haveAligned {
		state, hasState, serr := d.tombstones.SyncSeq(ctx)
		if serr == nil && (!hasState || aligned.Epoch != state.Epoch || aligned.Seq > state.Seq) {
			// No entries to record: the watermark alone moves.
			if err := d.tombstones.NoteBatch(ctx, aligned, nil); err != nil {
				d.logger.Warn("plane: could not align the deletion watermark after a full reconcile", zap.Error(err))
			} else {
				res.LocalSeq = aligned.Seq
			}
		}
	}

	d.lastChainReconcile = time.Now()
	if leftoversErr != nil {
		return res, leftoversErr
	}
	return res, reclaimErr
}

// logDeletionSweep emits the reading this whole mechanism exists to justify: how big
// one pass was, where it got its answer, and whether the log could serve it at all.
//
// Without it there is no way to tell an incremental read that saved a full range scan
// from one that fell back every single time, and "the log earns its keep" would stay an
// assumption.
func (d *LocalDataPlane) logDeletionSweep(res DeletionSweepResult, took time.Duration) {
	d.logger.Debug("plane: deletion sweep",
		zap.String("source", res.Source),
		zap.String("fallback", res.Fallback),
		zap.Int64("local_seq", res.LocalSeq),
		zap.Int64("latest_seq", res.LatestSeq),
		zap.Bool("truncated", res.Truncated),
		zap.Bool("behind_leader", res.BehindLeader),
		zap.Int("entries", res.Entries),
		zap.Int("knowledge_bases", res.KnowledgeBases),
		zap.Int("reclaimed", res.Reclaimed),
		zap.Int64("took_us", took.Microseconds()))
}

// maxChainReconcileAge bounds how long the incremental path may keep the live-chain
// pass away. A day: a leftover the log cannot describe (a late push past the grace
// window, a table rebuilt from nothing) is then found within a day instead of never.
func (d *LocalDataPlane) maxChainReconcileAge() time.Duration {
	if d.chainReconcileMaxAge > 0 {
		return d.chainReconcileMaxAge
	}
	return defaultChainReconcileMaxAge
}

const defaultChainReconcileMaxAge = 24 * time.Hour
