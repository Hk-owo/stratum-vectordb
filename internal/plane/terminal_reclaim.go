// Reclaiming the versions the control layer has retired (§3.8), and the disk
// retention window (§2.5).
//
// Two different questions share this file because they share the same answer
// ("this node no longer needs these bytes"): the terminal-failure reclaim, which
// completes a decision the control layer already made, and the retention policy,
// which keeps the newest N versions' index artifacts per knowledge base.

package plane

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"stratum/internal/types"
)

// NoteTerminalVersion records that the control layer settled versionID's DATA side
// as terminal — learned from THIS node's own apply of the Raft entry, not from
// §10.6's broadcast.
//
// That difference is the point. The broadcast goes to the candidate replicas and is
// best-effort: a replica that is partitioned, or that is restarting while it fires,
// never hears it, and its copy of the version stays behind with nothing left to
// notice it (the metadata that would have named it is gone). Every node applies the
// same Raft entry, so this path reaches exactly the nodes that have to reclaim.
//
// It reclaims LOCALLY, and that is not an optimisation — it is what the apply path
// changes. §10.6's DropVersionData broadcasts because the node issuing it does not
// know which replicas hold the version; here every replica is told by its own apply,
// so broadcasting again would turn N nodes into N×N calls (the same reasoning §B's
// reconciliation uses). A node that never held the version runs a prefix delete over
// nothing and moves on.
//
// Called from the apply loop, so it does the cheap part inline (one local delete, no
// network) and hands the retry to its own queue — see terminalReclaimInterval for why
// that cadence is deliberately not §10.6's.
func (d *LocalDataPlane) NoteTerminalVersion(kbID string, versionID int64) {
	if err := d.reclaimTerminalVersionLocally(kbID, versionID); err != nil {
		d.logger.Warn("plane: local reclaim of a terminal version failed; retrying on its own cadence",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

// ReclaimTerminalVersions rebuilds the apply-driven reclaim list at startup.
//
// The list is memory (see NoteTerminalVersion), and a terminal verdict does NOT come
// back on a restart: a node that was down when the entry was applied only replays it if
// the entry is still in the log — after compaction the state machine is restored from
// the snapshot and the entry is never applied individually, so the hook that would have
// queued the reclaim never fires. Measured on a single node (verdict applied, log
// compacted, restart): the hook does not fire again, and that version's leftover data
// is nobody's business again.
//
// What DOES survive is the version's metadata: a data-side verdict lives in
// DataStatusFailedPermanent, which the snapshot carries. So this costs no persistence
// at all — sweep the state machine once and queue exactly what the apply hook would
// have queued.
//
// The INDEX side is deliberately excluded (§10.1b): that verdict says a build failed,
// not that the data is gone, so reclaiming storage on its behalf would throw away good
// data. A version that was DELETED meanwhile is not in the metadata at all; those go
// through DeleteVersion's own cleanup (which persists its intent and can be
// re-triggered), so a sweep cannot see them and does not need to.
func (d *LocalDataPlane) ReclaimTerminalVersions(ctx context.Context, meta MetadataLister) (int, error) {
	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		return 0, fmt.Errorf("plane: reclaim terminal versions: list knowledge bases: %w", err)
	}
	queued := 0
	for _, kb := range kbs {
		versions, err := meta.ListVersions(ctx, kb.KBID)
		if err != nil {
			// One unreadable knowledge base must not stop the rest: the sweep is
			// idempotent, and the next start tries again.
			d.logger.Warn("plane: reclaim terminal versions: list versions failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
			continue
		}
		for _, v := range versions {
			if v.DataStatus != types.DataStatusFailedPermanent {
				continue
			}
			d.NoteTerminalVersion(kb.KBID, v.VersionID)
			queued++
		}
	}
	return queued, nil
}

// ReclaimVersionDataLocally implements DataPlane: the local half of DropVersionData,
// with no broadcast. See the interface comment for who is allowed to call it.
func (d *LocalDataPlane) ReclaimVersionDataLocally(_ context.Context, kbID string, versionID int64) error {
	return d.reclaimTerminalVersionLocally(kbID, versionID)
}

// cleanupTask is one version whose cleanup did not finish — an entry in the
// apply-driven reclaim queue (see NoteTerminalVersion). The broadcast queue it used to
// belong to is gone: its only producer was a failed broadcast, and that case is now
// covered by the apply path and by the deletion flow's own persistence.
type cleanupTask struct {
	kbID      string
	versionID int64
	attempts  int
}

// terminalReclaimInterval / terminalReclaimAttempts pace the reclaim queue, which is
// now the ONLY cleanup-retry channel: §10.6's broadcast queue is gone, because every
// node learns a terminal verdict from its own apply and reclaims locally
// (NoteTerminalVersion).
//
// The cadence follows from what this queue touches — only THIS node's storage, for a
// version whose data will never arrive. A failed local prefix delete is disk hygiene
// that a later pass fixes, so the interval can be generous (a minute) and the patience
// long (ten attempts): nothing is waiting on it, and nothing else will do it.
// Their defaults are LocalDataPlaneConfig.TerminalReclaimInterval / Attempts.
var terminalReclaimInterval = time.Minute

const terminalReclaimAttempts = 10

// reclaimTerminalVersionLocally removes whatever this node still holds for a version
// the control layer retired, and steps the cursor past it.
//
// No broadcast (see NoteTerminalVersion) and no existence probe: DropVersionStorage is
// a prefix delete, so "I do not have it" is a no-op rather than a case to detect.
func (d *LocalDataPlane) reclaimTerminalVersionLocally(kbID string, versionID int64) error {
	if d.dropper != nil {
		if err := d.dropper.DropVersionStorage(context.Background(), kbID, versionID); err != nil {
			d.scheduleTerminalReclaim(kbID, versionID)
			return err
		}
	}
	// The cursor steps over it only once the bytes are gone: a version whose data will
	// never arrive must not hold the contiguous cursor back (that would read as "this
	// node is behind" and start a catch-up that can never finish), and the order is the
	// same invariant every other advance follows — data first, cursor second.
	d.cursorStepsPast(kbID, versionID)
	d.clearTerminalReclaim(kbID, versionID)
	return nil
}

// scheduleTerminalReclaim remembers a local reclaim that did not finish. Keyed per
// version (repeats collapse) and bounded by terminalReclaimAttempts, so it cannot
// become a leak of its own.
func (d *LocalDataPlane) scheduleTerminalReclaim(kbID string, versionID int64) {
	key := failureKey(kbID, versionID)
	d.terminalReclaimMu.Lock()
	if d.terminalReclaim == nil {
		d.terminalReclaim = make(map[string]*cleanupTask)
	}
	if _, ok := d.terminalReclaim[key]; !ok {
		d.terminalReclaim[key] = &cleanupTask{kbID: kbID, versionID: versionID}
	}
	d.terminalReclaimMu.Unlock()
}

func (d *LocalDataPlane) clearTerminalReclaim(kbID string, versionID int64) {
	key := failureKey(kbID, versionID)
	d.terminalReclaimMu.Lock()
	delete(d.terminalReclaim, key)
	d.terminalReclaimMu.Unlock()
}

// StartTerminalReclaims runs the reclaim retry loop until ctx ends.
func (d *LocalDataPlane) StartTerminalReclaims(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(d.terminalReclaimEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.retryTerminalReclaims(ctx)
			}
		}
	}()
}

// retryTerminalReclaims makes one pass: each unfinished reclaim is retried, and the
// ones that have run out of attempts are dropped with an error.
func (d *LocalDataPlane) retryTerminalReclaims(ctx context.Context) {
	d.terminalReclaimMu.Lock()
	pending := make([]*cleanupTask, 0, len(d.terminalReclaim))
	for _, task := range d.terminalReclaim {
		pending = append(pending, task)
	}
	d.terminalReclaimMu.Unlock()

	for _, task := range pending {
		if d.dropper != nil {
			if err := d.dropper.DropVersionStorage(ctx, task.kbID, task.versionID); err != nil {
				task.attempts++
				if task.attempts >= d.terminalReclaimTries {
					d.clearTerminalReclaim(task.kbID, task.versionID)
					d.logger.Error("plane: giving up on a local terminal-version reclaim; leftover bytes need an operator",
						zap.String("kb_id", task.kbID), zap.Int64("version_id", task.versionID),
						zap.Int("attempts", task.attempts), zap.Error(err))
					continue
				}
				continue
			}
		}
		d.cursorStepsPast(task.kbID, task.versionID)
		d.clearTerminalReclaim(task.kbID, task.versionID)
	}
}

// PendingTerminalReclaims reports how many local reclaims are waiting to be retried,
// for diagnostics and tests.
func (d *LocalDataPlane) PendingTerminalReclaims() int {
	d.terminalReclaimMu.Lock()
	defer d.terminalReclaimMu.Unlock()
	return len(d.terminalReclaim)
}

// DropVersionData: see WriteVersionData.
// DropVersionData removes versionID's physical data — on this node and on
// every candidate replica.
//
// It deliberately covers *all* candidates rather than only the replicas that
// acknowledged the write: when the control layer declares a version permanently
// failed it does not know which replicas actually landed the data, so a no-op
// on a replica that never received it is far cheaper than leaking the data on
// the one that did (Stratum_设计文档v13.md §10.6).
//
// The cursor is advanced past the version: a permanently failed version will
// never have data, so treating it as "known" is what keeps the gap from
// looking like something a later backfill should fetch.
func (d *LocalDataPlane) DropVersionData(ctx context.Context, kbID string, versionID int64) error {
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if d.cleaner != nil && d.resolveReplicas != nil {
		peers, err := d.resolveReplicas(ctx)
		if err != nil {
			record(fmt.Errorf("plane: DropVersionData: resolve replicas: %w", err))
		} else {
			for _, peer := range peers {
				if err := d.cleaner.DeleteVersionData(ctx, peer, kbID, versionID, "permanently failed version"); err != nil {
					record(fmt.Errorf("plane: DropVersionData: broadcast to %s: %w", peer, err))
				}
			}
		}
	}

	if d.dropper != nil {
		if err := d.dropper.DropVersionStorage(ctx, kbID, versionID); err != nil {
			record(fmt.Errorf("plane: DropVersionData: local drop: %w", err))
		}
	} else if d.cleaner == nil {
		return fmt.Errorf("plane: DropVersionData: not wired (no local dropper and no broadcaster)")
	}
	d.cursorStepsPast(kbID, versionID)
	// A broadcast that did not reach everyone is no longer retried here. It used to be:
	// §10.6's only way to reclaim a version on a replica that missed the message was to
	// tell it again. Every replica now learns a terminal verdict from its own apply and
	// reclaims locally (NoteTerminalVersion), and the DELETION path keeps its own
	// persisted intent (the Deleting marker plus its coordinator's retries) — so this
	// channel had no work left, only a queue, a background loop and a set of parameters.
	if firstErr != nil {
		d.logger.Warn("plane: version cleanup broadcast did not reach every candidate",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(firstErr))
	}
	return firstErr
}

// EnforceRetention applies the disk retention policy once at startup: for
// every knowledge base, drop on-disk index files older than the newest
// IndexRetentionCount versions, shielding the active version. A no-op when
// retention is unconfigured (the manager decides that). The same policy also
// runs after every successful build; this is the startup pass, and it must
// precede ReconcileIndexes so the reconcile sees post-retention disk facts.
//
// ONLY the index layer is bounded here, and that is a contract rather than an
// omission: a version is kept forever. `RollbackVersion` accepts any surviving
// version, so "nobody will read this one again" is not a fact the control layer can
// ever establish — retiring an old version would turn a legal rollback into an empty
// knowledge base. What IS rebuildable is the index: drop the artifact and the next
// query rebuilds it (`ErrIndexNotReady`, then `RebuildIndex`), which is exactly why
// that is the layer with a window. A version's doc-id list and its documents have no
// such window, so a node's footprint for a knowledge base grows with its version
// count. That is the price of "you may roll back to any version", and it is the
// reason the maintenance paths that enumerate a knowledge base's versions
// (DeletedVersionLeftovers, reconcile) are written to cost what the node HOLDS
// rather than what the knowledge base ever had.
func (d *LocalDataPlane) EnforceRetention(ctx context.Context, meta MetadataLister) error {
	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		return fmt.Errorf("plane: EnforceRetention: list knowledge bases: %w", err)
	}
	for _, kb := range kbs {
		if err := d.indexMgr.EnforceDiskRetention(ctx, kb.KBID, []int64{kb.ActiveVersionID}); err != nil {
			d.logger.Warn("plane: retention: EnforceDiskRetention failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
		}
	}
	return nil
}
