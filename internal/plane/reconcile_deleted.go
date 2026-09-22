package plane

import (
	"context"
	"fmt"
	"sort"

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
