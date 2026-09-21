package plane

import (
	"context"
	"fmt"
	"sort"

	"go.uber.org/zap"
)

// DeletionLister answers "which versions has the control layer recorded as
// REMOVED?" — the tombstones described in docs/known-gaps.md §B.
//
// It is the authoritative "confirmed deleted" fact that version numbers cannot
// express: at the storage layer an absent row means BOTH "this version is empty"
// and "this version is gone" (§7.5), and once the metadata row is removed there
// is nothing left to distinguish them. A tombstone is that distinction, recorded
// where the decision was made.
//
// A node without a state machine cannot answer it. The wiring leaves it nil there
// and ReconcileDeletedVersions then reports that it cannot run, rather than
// guessing — a guessed verdict is exactly what kept §B unbuilt.
type DeletionLister interface {
	// DeletionsInRange returns kbID's version ids in (fromExclusive, toInclusive]
	// whose metadata has been removed.
	DeletionsInRange(ctx context.Context, kbID string, fromExclusive, toInclusive int64) ([]int64, error)
}

// LocalVersionLister enumerates the versions this node holds documents for.
// *versiondoc.PebbleVersionDocList satisfies it.
type LocalVersionLister interface {
	ListVersions(ctx context.Context, kbID string) ([]int64, error)
}

// DeletedVersionLeftovers reports, per knowledge base, the version ids this node
// still holds documents for while the control layer has recorded them as deleted
// — the "local leftovers" of docs/known-gaps.md §B/§C.
//
// It is the READ half of the reconciliation and touches no storage, so a caller
// that only wants to know (or to report) can use it as it is. A knowledge base
// whose local list or whose tombstones cannot be read is skipped and named in the
// returned error: "I cannot enumerate" is not "there is nothing here" (which would
// silently skip the work) and it is certainly not "everything here is a leftover".
//
// The question put to the tombstones is narrowed to the range this node covers,
// so the answer costs what the node holds rather than what the knowledge base ever
// had. Version ids absent from this node are harmless either way: the reclaim is
// an idempotent prefix delete.
func (d *LocalDataPlane) DeletedVersionLeftovers(ctx context.Context, meta MetadataLister) (map[string][]int64, error) {
	if d.localVersions == nil || d.deletions == nil {
		return nil, fmt.Errorf("plane: deleted version leftovers: not wired (needs the local version list and the tombstones)")
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
		deleted, err := d.deletions.DeletionsInRange(ctx, kb.KBID, local[0]-1, local[len(local)-1])
		if err != nil {
			d.logger.Warn("plane: deleted version leftovers: cannot read the tombstones; skipping the knowledge base",
				zap.String("kb_id", kb.KBID), zap.Error(err))
			note(fmt.Errorf("list deleted versions of %s: %w", kb.KBID, err))
			continue
		}
		if len(deleted) > 0 {
			leftovers[kb.KBID] = deleted
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
