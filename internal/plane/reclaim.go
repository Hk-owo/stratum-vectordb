package plane

import (
	"context"
	"fmt"
)

// WALCompactor is the reclaim capability a WAL may expose: rewrite the log,
// dropping the recorded changes of versions at or below each knowledge base's
// watermark. It is a separate interface rather than part of TransactionWAL because
// a WAL that cannot compact (the in-memory mock, for instance) is perfectly usable —
// it just grows.
type WALCompactor interface {
	Compact(ctx context.Context, keepThrough map[string]int64) error
}

// WALKnowledgeBases is the WAL's own answer to "which knowledge bases do I hold
// recorded changes for?". It is what a reclaim pass should be scoped to: the question
// is about the LOG's contents, not about which cursors this node happens to track. A
// WAL that does not implement it falls back to the node's own knowledge bases.
type WALKnowledgeBases interface {
	KnowledgeBases() []string
}

// ReclaimChanges compacts the WAL, discarding the recorded changes of versions that
// every replica already holds (Stratum_设计文档v13.md §7.5).
//
// It is the storage layer's half of the split the design calls for: the JUDGEMENT is
// global and therefore the control layer's (only the leader holds the reports), while
// the PHYSICAL reclaim is the storage layer's, because the WAL is its own file. This
// method is where the two meet — it asks the control plane for each watermark and
// hands it to the WAL.
//
// The scope is the WAL's own knowledge bases when it can report them, and this node's
// tracked knowledge bases otherwise. Scoping by the log is the accurate one: what is
// being reclaimed is log content, so a knowledge base whose data was dropped still has
// records to drop, while one this node never wrote has none to begin with.
//
// It reclaims nothing when it cannot prove safety:
//   - the WAL exposes no compaction (in-memory mock): nothing to do;
//   - no control layer is wired: no judgement is available;
//   - a knowledge base's watermark is unknown (not leader, a replica has not
//     reported): that knowledge base keeps everything.
//
// A partial answer is used as far as it goes: knowledge bases that DO have a known
// watermark are reclaimed even if others do not, since the two are independent.
//
// The returned map is what was actually used, which is what a test or an operator
// wants to see; an empty map means nothing was reclaimed.
func (d *LocalDataPlane) ReclaimChanges(ctx context.Context) (map[string]int64, error) {
	compactor, ok := d.wal.(WALCompactor)
	if !ok || d.control == nil {
		return nil, nil
	}

	scope := d.localKnowledgeBases()
	if byLog, ok := d.wal.(WALKnowledgeBases); ok {
		scope = byLog.KnowledgeBases()
	}

	keep := make(map[string]int64)
	for _, kbID := range scope {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if watermark, ok := d.control.ReclaimableChangesThrough(kbID); ok {
			keep[kbID] = watermark
		}
	}
	if len(keep) == 0 {
		return nil, nil
	}
	if err := compactor.Compact(ctx, keep); err != nil {
		return nil, fmt.Errorf("plane: reclaim changes: %w", err)
	}
	return keep, nil
}

// localKnowledgeBases returns the knowledge bases this node holds data for — the
// ones whose changes could be sitting in its WAL. A node that never wrote or
// received a knowledge base has nothing to reclaim there.
func (d *LocalDataPlane) localKnowledgeBases() []string {
	d.versionMu.RLock()
	defer d.versionMu.RUnlock()
	kbs := make([]string, 0, len(d.localVersion))
	for kbID := range d.localVersion {
		kbs = append(kbs, kbID)
	}
	return kbs
}
