package plane

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	"go.uber.org/zap"
)

// VersionLivenessLister answers the two facts a "is this version gone?" judgement needs,
// from ONE view of the replicated metadata: which versions in a range are still alive, and
// the highest id ever allocated.
//
// It replaces the removal record (§B) for this judgement, because a removal record is
// HISTORY — pruning can drop it, and once dropped the judgement loses its evidence — while
// these two are properties of the CURRENT state: nothing to keep, nothing to prune, and no
// window in which they can go missing.
//
// One call rather than two, because the two must agree: "id <= lastAllocated" is what
// proves this view has already applied that id's allocation, so only then does "and it is
// not alive" mean removed — rather than "not seen yet". Reading them separately could
// combine a newer live list with an older bound and call a LIVE version gone, and this
// judgement deletes data.
//
// A node without a state machine cannot answer it locally, but it can ask one:
// *raft.RaftNodeImpl reads its own state machine and *raft.RemoteRaftNode asks the control
// tier, so a storage node judges its own leftovers the same way a voter does.
type VersionLivenessLister interface {
	// VersionLiveness returns kbID's live version ids in (fromExclusive, toInclusive],
	// ascending (nil bounds mean the whole chain), plus the highest version id ever
	// allocated cluster-wide (0 when nothing has been allocated yet).
	VersionLiveness(ctx context.Context, kbID string, fromExclusive, toInclusive *int64) ([]int64, int64, error)
}

// LocalVersionLister enumerates the versions this node holds documents for.
// *versiondoc.PebbleVersionDocList satisfies it.
type LocalVersionLister interface {
	ListVersions(ctx context.Context, kbID string) ([]int64, error)
}

// DeletedVersionLeftovers reports, per knowledge base, the version ids this node still
// holds documents for while the control layer no longer has them — the "local leftovers"
// of docs/known-gaps.md §B/§C.
//
// "No longer has them" is judged from CURRENT state (alive-in-range plus the allocation
// bound), never from a removal record: a removal record is history that pruning can drop,
// and a leftover whose evidence was pruned is invisible forever.
//
// It is the READ half of the reconciliation and touches no storage, so a caller
// that only wants to know (or to report) can use it as it is. A knowledge base
// whose local list or whose liveness cannot be read is skipped and named in the
// returned error: "I cannot enumerate" is not "there is nothing here" (which would
// silently skip the work) and it is certainly not "everything here is a leftover".
//
// The question is narrowed to the range this node covers,
// so the answer costs what the node holds rather than what the knowledge base ever
// had. Version ids absent from this node are harmless either way: the reclaim is
// an idempotent prefix delete.
func (d *LocalDataPlane) DeletedVersionLeftovers(ctx context.Context, meta MetadataLister) (map[string][]int64, error) {
	if d.localVersions == nil || d.liveness == nil {
		return nil, fmt.Errorf("plane: deleted version leftovers: not wired (needs the local version list and the liveness read)")
	}
	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		return nil, fmt.Errorf("plane: deleted version leftovers: list knowledge bases: %w", err)
	}

	leftovers := make(map[string][]int64)
	var firstErr error
	note := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}
	for _, kb := range kbs {
		local, err := d.localVersions.ListVersions(ctx, kb.KBID)
		if err != nil {
			d.logger.Warn("plane: deleted version leftovers: cannot list local versions; skipping the knowledge base",
				zap.String("kb_id", kb.KBID), zap.Error(err))
			note(fmt.Errorf("list local versions of %s: %w", kb.KBID, err))
			continue
		}
		if len(local) == 0 {
			continue
		}
		// The judgement is "which of the versions I hold is GONE", asked of the CURRENT
		// state: alive-in-range plus how far allocation got. An id at or below
		// lastAllocated that is not alive was handed out and is gone — the same answer a
		// removal record gives, and one that pruning cannot take away.
		//
		// The range is the one this node covers, so the answer costs what the node holds
		// rather than what the knowledge base ever had. Version ids absent from this node
		// are harmless either way: the reclaim is an idempotent prefix delete.
		// The read is exclusive on the low side, like every other version read here, so the
		// bound is one below the first local id: local[0] itself must be included.
		from, to := local[0]-1, local[len(local)-1]
		alive, lastAllocated, err := d.liveness.VersionLiveness(ctx, kb.KBID, &from, &to)
		if err != nil {
			d.logger.Warn("plane: deleted version leftovers: cannot read the liveness of the local versions; skipping the knowledge base",
				zap.String("kb_id", kb.KBID), zap.Error(err))
			note(fmt.Errorf("read version liveness of %s: %w", kb.KBID, err))
			continue
		}
		aliveSet := make(map[int64]bool, len(alive))
		for _, id := range alive {
			aliveSet[id] = true
		}
		var gone []int64
		for _, id := range local {
			// Above the allocation bound the id was never handed out, so the control layer
			// is not saying "removed" — it is saying nothing. Leave it alone rather than
			// guess.
			if id > lastAllocated || aliveSet[id] {
				continue
			}
			gone = append(gone, id)
		}
		if len(gone) > 0 {
			leftovers[kb.KBID] = gone
		}
	}
	return leftovers, firstErr
}

// ReconcileDeletedVersions reclaims the leftovers DeletedVersionLeftovers reports
// (the "confirmed deleted" judgement is documented there).
//
// A reclaim that fails is reported and the rest continue: the delete is idempotent,
// the next pass retries it, and one stuck version must not stop the others. The
// returned error is the aggregate — read failures from the scan plus reclaim
// failures — so a caller logging it sees everything that went wrong in one place.
//
// It deliberately does not touch the data cursor. The versions it reclaims were
// deleted, so nothing is waiting on them: the cursor had already stepped past them,
// or never reached them (§7.5).
func (d *LocalDataPlane) ReconcileDeletedVersions(ctx context.Context, meta MetadataLister) (int, error) {
	if d.dropper == nil {
		return 0, fmt.Errorf("plane: reconcile deleted versions: not wired (needs a dropper)")
	}
	leftovers, firstErr := d.DeletedVersionLeftovers(ctx, meta)
	if leftovers == nil {
		return 0, firstErr
	}

	// Sorted so the order is deterministic: the log lines and any test asserting on
	// them must not depend on Go's map iteration order.
	kbIDs := make([]string, 0, len(leftovers))
	for kbID := range leftovers {
		kbIDs = append(kbIDs, kbID)
	}
	sort.Strings(kbIDs)

	reclaimed := 0
	for _, kbID := range kbIDs {
		for _, versionID := range leftovers[kbID] {
			if err := d.dropper.DropVersionStorage(ctx, kbID, versionID); err != nil {
				d.logger.Warn("plane: reconcile deleted versions: reclaim failed; the next pass retries it",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
				if firstErr == nil {
					firstErr = fmt.Errorf("reclaim deleted version %d of %s: %w", versionID, kbID, err)
				}
				continue
			}
			reclaimed++
		}
	}
	return reclaimed, firstErr
}

// DefaultDeletionSweepInterval paces the periodic deletion sweep.
//
// A minute, and the number follows the WORK one tick does, not the deletion's
// urgency: a tick is one small read of the control layer's bounded deletion log
// (ReconcileDeletions rule 2), and the live-chain pass — the expensive half — runs only
// when something forces it (a local landing, a truncated log, no watermark, or
// ChainReconcileMaxAge). It used to be an hour, from when a tick WAS a per-knowledge-base
// range scan; leaving it there made the incremental path a decoration, because a
// deletion the log could have served waited an hour to be read.
//
// This is the interval `reconcile.deleted_versions_incremental_interval_s` documents
// (<= 0 takes it), and the sweep's own re-judge half is bounded separately by
// ChainReconcileMaxAge (24 h).
const DefaultDeletionSweepInterval = time.Minute

// StartDeletedVersionReconcile runs the deletion sweep on a cadence until ctx ends.
// Each tick is ReconcileDeletions: one incremental read, plus whatever the rule decides
// is due (a forced live-chain pass, and the local drain of deleted bytes still owed).
//
// Why a cadence: the startup pass only sees leftovers that exist at boot, and the ones
// that matter are created at RUNTIME — the late push that outlived the delete meant to
// remove it (§B's source ③) needs no failure at all, so "restart the node and see" is
// not a discovery mechanism.
//
// The two halves of a tick have different switches on purpose:
//   - the INCREMENTAL read is `reconcile.deleted_versions_incremental`, and it is cheap
//     enough to run on every tick — it is what makes a deletion known without re-reading
//     a whole range;
//   - the FULL live-chain re-judge is `reconcile.deleted_versions_periodic` (through
//     DisableChainReconcile), and it is the half that costs a range scan per knowledge
//     base. It compares what this node HOLDS against the control layer's live chain,
//     which is the one judgement with no retention and no gaps — the only thing that can
//     name a node nobody ever told.
//
// NOTE the blind spot the split leaves: a deletion performed while this node is idle
// waits for the next landing or the next restart. That window is what the tombstone table
// covers (the notification is recorded locally when it arrives and retried every 30 s),
// so this pass is free to be the slow, authoritative answer instead of the fast one.
//
// The reclaim itself is idempotent end to end, but the opt-in is separate by design:
// `reconcile.deleted_versions` governs the start-up sweep and
// `reconcile.deleted_versions_periodic` governs this one, because "clean up what is lying
// around at boot" and "keep recovering what appears while running" are different
// decisions about the same irreversible action.
//
// A non-positive interval takes DefaultDeletionSweepInterval.
func (d *LocalDataPlane) StartDeletedVersionReconcile(ctx context.Context, meta MetadataLister, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultDeletionSweepInterval
	}
	go func() {
		// Phase first, ticker second — see reconcileJitter. The start-up sweep has
		// already run as a separate call, so delaying the FIRST periodic pass by up to
		// one interval costs nothing.
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconcileJitter(interval)):
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				start := time.Now()
				// The INCREMENTAL path runs every pass: it is one small read, and it is
				// what makes a deletion known without re-reading a whole range. Whether
				// the live-chain judgement also runs is ReconcileDeletions' decision —
				// on a dirty local set, on a forced full pass, or whenever the log
				// cannot serve this node.
				res, err := d.ReconcileDeletions(ctx, meta)
				if err != nil {
					d.logger.Warn("plane: periodic deletion sweep did not finish; the next pass retries it",
						zap.Error(err))
				}
				d.logDeletionSweep(res, time.Since(start))
				if res.Reclaimed > 0 {
					d.logger.Info("plane: periodic deletion sweep reclaimed local leftovers",
						zap.Int("versions", res.Reclaimed), zap.String("source", res.Source))
				}
			}
		}
	}()
}

// reconcileJitter is the periodic pass's start-up delay: a draw from [0, interval).
//
// Why it exists: every node's ticker is otherwise aligned to its own boot time, so a
// cluster restarted as one batch — or simply a set of nodes all being written to —
// asks the control layer for its per-knowledge-base liveness reads in the same
// instant. That is N nodes × KBs of concurrent reads: harmless in kind (read-only, an
// in-memory range read, serial within each node) but free to avoid.
//
// The phase only has to be scattered ONCE. Every ticker keeps its own offset
// afterwards, so this shifts each node's first pass rather than wobbling every pass.
func reconcileJitter(interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(interval)))
}

// takeDeletedReconcileDirty consumes the dirty flag. Read-and-clear in one hold: two
// passes must not both act on the same change — harmless, since the action is
// idempotent, but it would double the work for nothing.
// peekDeletedReconcileDirty reports whether the local holdings changed since the last
// reverse reconciliation, WITHOUT consuming the flag.
//
// The flag's consumer is the live-chain pass, and only it: a local landing is a reason
// to compare this node's set against the live chain, not a reason to read an
// incremental log that cannot describe a late push. Peeking first is what lets the
// sweep choose a path without stealing the trigger from the path that needs it.
func (d *LocalDataPlane) peekDeletedReconcileDirty() bool {
	d.versionMu.RLock()
	defer d.versionMu.RUnlock()
	return d.deletedReconcileDirty
}

func (d *LocalDataPlane) takeDeletedReconcileDirty() bool {
	d.versionMu.Lock()
	defer d.versionMu.Unlock()
	dirty := d.deletedReconcileDirty
	d.deletedReconcileDirty = false
	return dirty
}
