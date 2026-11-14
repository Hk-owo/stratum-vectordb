package plane

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
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
// The judgement is made here, from the replicas' own answers: this node asks each one
// how far its contiguous history reaches and takes the minimum, its own cursor included.
// Reclaiming is rare (DefaultReclaimInterval), so asking is cheap, and a replica's own
// answer is the freshest evidence there is.
//
// The scope is the WAL's own knowledge bases when it can report them, and this node's
// tracked knowledge bases otherwise. Scoping by the log is the accurate one: what is
// being reclaimed is log content, so a knowledge base whose data was dropped still has
// records to drop, while one this node never wrote has none to begin with.
//
// It reclaims nothing when it cannot prove safety:
//   - the WAL exposes no compaction (in-memory mock): nothing to do;
//   - no replica topology or cursor query is wired: no judgement is available;
//   - a replica cannot be asked, or holds nothing for the knowledge base: that knowledge
//     base keeps everything.
//
// A partial answer is used as far as it goes: knowledge bases that DO have a known
// watermark are reclaimed even if others do not, since the two are independent.
//
// The returned map is what was actually used, which is what a test or an operator
// wants to see; an empty map means nothing was reclaimed.
func (d *LocalDataPlane) ReclaimChanges(ctx context.Context) (map[string]int64, error) {
	compactor, ok := d.wal.(WALCompactor)
	if !ok || d.cursorQuerier == nil || d.resolveReplicas == nil {
		return nil, nil
	}
	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		// An unreadable topology is "cannot prove safety", not a failed pass.
		d.logger.Warn("plane: reclaim changes: resolve replicas", zap.Error(err))
		return nil, nil
	}

	scope := d.localKnowledgeBases()
	if byLog, ok := d.wal.(WALKnowledgeBases); ok {
		scope = byLog.KnowledgeBases()
	}

	keep := make(map[string]int64)
	blocked := make(map[string][]ReclaimBlocker)
	// A peer that failed once this pass is not asked again until the next one, so a
	// dead peer costs one timeout per pass rather than one per knowledge base.
	unreachable := make(map[string]bool)
	for _, kbID := range scope {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		watermark, blockers := d.reclaimWatermark(ctx, kbID, peers, unreachable)
		if len(blockers) > 0 {
			blocked[kbID] = blockers
			continue
		}
		keep[kbID] = watermark
	}

	d.reclaimDiag.mu.Lock()
	d.reclaimDiag.judged = true
	d.reclaimDiag.blocked = blocked
	d.reclaimDiag.mu.Unlock()

	if len(keep) == 0 {
		return nil, nil
	}
	if err := compactor.Compact(ctx, keep); err != nil {
		return nil, fmt.Errorf("plane: reclaim changes: %w", err)
	}
	return keep, nil
}

// reclaimWatermark is the minimum contiguous cursor across this node and peers, plus
// whatever keeps that minimum from meaning anything.
func (d *LocalDataPlane) reclaimWatermark(ctx context.Context, kbID string, peers []string, unreachable map[string]bool) (int64, []ReclaimBlocker) {
	var blockers []ReclaimBlocker
	watermark := d.localVersionOf(kbID)
	if watermark <= 0 {
		blockers = append(blockers, ReclaimBlocker{Address: ReclaimBlockerLocalNode, Reason: ReclaimBlockerNoCursor})
	}
	for _, peer := range peers {
		if unreachable[peer] {
			blockers = append(blockers, ReclaimBlocker{Address: peer, Reason: ReclaimBlockerUnreachable})
			continue
		}
		peerCtx, cancel := context.WithTimeout(ctx, d.peerCursorTimeout)
		cursor, err := d.cursorQuerier.LocalVersionOf(peerCtx, peer, kbID)
		cancel()
		switch {
		case err != nil:
			unreachable[peer] = true
			blockers = append(blockers, ReclaimBlocker{Address: peer, Reason: ReclaimBlockerUnreachable})
		case cursor <= 0:
			// It answers, but holds nothing here. Its delta may still be needed, so
			// this is not a reclaimable zero.
			blockers = append(blockers, ReclaimBlocker{Address: peer, Reason: ReclaimBlockerNoCursor})
		case cursor < watermark:
			watermark = cursor
		}
	}
	return watermark, blockers
}

// ReclaimBlocker names one replica that kept a knowledge base's reclaim watermark
// unavailable on the last pass, and why. It is the diagnosis the bare "nothing was
// reclaimed" cannot carry: the question an operator asks is always "which node".
type ReclaimBlocker struct {
	Address string
	// Reason is one of the ReclaimBlocker* constants below.
	Reason string
}

// The reasons a replica blocks the watermark. Stable strings, because they travel to
// an operator-facing status response.
const (
	// ReclaimBlockerUnreachable: the replica could not be asked.
	ReclaimBlockerUnreachable = "unreachable"
	// ReclaimBlockerNoCursor: it answers, but holds nothing for this knowledge base.
	ReclaimBlockerNoCursor = "no_cursor_for_kb"
	// ReclaimBlockerLocalNode is the Address of a blocker that is this node itself.
	ReclaimBlockerLocalNode = "local"
)

// reclaimDiagnosis keeps the last pass's blockers, so a status call can answer without
// asking every replica about every knowledge base.
type reclaimDiagnosis struct {
	mu      sync.RWMutex
	judged  bool
	blocked map[string][]ReclaimBlocker
}

// ReclaimBlockers reports which replicas kept kbID's watermark unavailable on the last
// reclaim pass.
//
// ok=false means no pass has judged anything yet, which is NOT the same answer as an
// empty list: empty means the last pass found nothing blocking this knowledge base, while
// ok=false means this node cannot tell yet.
func (d *LocalDataPlane) ReclaimBlockers(kbID string) ([]ReclaimBlocker, bool) {
	d.reclaimDiag.mu.RLock()
	defer d.reclaimDiag.mu.RUnlock()
	if !d.reclaimDiag.judged {
		return nil, false
	}
	return append([]ReclaimBlocker(nil), d.reclaimDiag.blocked[kbID]...), true
}

// localKnowledgeBases returns the knowledge bases this node holds data for — the
// ones whose changes could be sitting in its WAL. A node that never wrote or
// received a knowledge base has nothing to reclaim there.
func (d *LocalDataPlane) localKnowledgeBases() []string {
	return d.state.KnowledgeBases()
}
