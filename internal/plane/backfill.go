// Catching this node up (§4.4): pulling a version's data it is missing, and the
// two ways a gap is closed — replay the recorded changes, or transfer the full
// state when the gap is too old for the change log.
//
// The source is chosen here too (the four-layer lookup of §4.4): the announced
// data sources first, then the holders this node learned while catching up, then
// the leader as the pre-split fallback.

package plane

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
)

// FetchVersionData brings the version's data here without building its index.
//
// It is what a non-active version needs (§8.6b): the data must be present for
// the version to count as durable, but building an index nobody may ever query
// is wasted work. A later query builds it lazily.
func (d *LocalDataPlane) FetchVersionData(ctx context.Context, kbID string, versionID int64) error {
	// Same fact as in EnsureIndex: this node holding the version already means
	// there is nothing to fetch — which after §8.5 can no longer be inferred
	// from "I am the leader/writer".
	// "Data only" is the whole contract. This is the §8.6(b) INACTIVE path: no
	// build, on any node. The version is built when a query asks for it
	// (service/query.go) or an operator warms it, and the router's owner set is
	// what decides which nodes those requests reach (internal/router, §5 of
	// docs/index-owner-sharding-plan.md).
	//
	// It is tempting to build here "because the data just arrived anyway". That is
	// exactly the eager behaviour §8.6(b) removed, and on a bulk import it is the
	// whole cost: measured on the 3+3 cluster, a run of N
	// versions built all N, most of them for versions nothing had ever queried.
	if d.localVersionOf(kbID) >= versionID {
		return nil
	}
	// Checked before resolving: for a version this node knows was deleted there is
	// nothing to fetch and no source worth asking about.
	if d.skipIfDeleted(ctx, kbID, versionID, "FetchVersionData") {
		return nil
	}
	addr, ok, err := d.resolve(ctx, kbID, versionID)
	if err != nil {
		return fmt.Errorf("plane: FetchVersionData(%s, %d): resolve data source: %w", kbID, versionID, err)
	}
	if !ok {
		// No source is known yet: nothing to fetch. A later apply or reconcile
		// pass retries.
		return nil
	}
	if err := d.backfillTo(ctx, addr, kbID, versionID); err != nil {
		return err
	}
	if err := d.puller.PullVersionData(ctx, addr, kbID, versionID); err != nil {
		return err
	}
	d.dataLanded(kbID, versionID)
	// Nobody confirms a pull, so arm the §7.3 quorum check (see EnsureIndex).
	d.watchForDurability(ctx, kbID, versionID)
	return nil
}

// backfillTo closes a known gap before versionID is applied, by pulling
// (local cursor, versionID) from sourceAddr in order. It is deliberately
// conservative: with no cursor yet (a node that has applied nothing for this
// KB) there is no *known* gap to close, and the versions it learns about are
// pulled whole. The versions it fills are then part of the node's local
// history, so the cursor advances as each one lands.
func (d *LocalDataPlane) backfillTo(ctx context.Context, sourceAddr, kbID string, versionID int64) error {
	local := d.localVersionOf(kbID)
	if local == 0 || local >= versionID-1 {
		return nil
	}
	// Pick a peer that actually holds the gap: the default source (the leader)
	// is precisely what a lagging follower cannot rely on
	// (Stratum_设计文档v13.md §7.6).
	source := d.pickBackfillSource(ctx, kbID, versionID-1, sourceAddr)

	// The replicated metadata is read ONCE here and BOTH paths share it: the delta
	// path needs it to know whether the gap holds a version that no longer exists, and
	// the full-record path needs it to know whether the gap holds a version CONFIRMED
	// deleted — which is the case transferFullState exists for. One read rather than
	// one per path, and not one per version either: the gap may be long, and asking per
	// version would re-read the whole version list at every step.
	//
	// A failed read is carried rather than resolved here, because the two paths answer
	// it differently: the delta path turns its check OFF and replays as it did before
	// the check existed, while the full-record path STOPS (it cannot decide what to
	// pull without knowing what exists). The warning is emitted here so that a failure
	// is logged once instead of once per path.
	var deleted map[int64]bool
	var deletedErr error
	if d.liveness != nil {
		// The judgement is "which of these versions is GONE" — asked of the CURRENT state:
		// alive-in-range plus how far allocation got. An id at or below lastAllocated that
		// is not alive was handed out and is gone, and THAT answer cannot be pruned away
		// (docs/known-gaps.md §B). The older shape asked for a removal record instead, and
		// a removal record has a lifetime: once pruning drops it, the gap reads as "nothing
		// was removed" and the deleted version is replayed.
		//
		// The gap is (local, versionID), and that is exactly what is asked for: the bounds
		// travel to the control layer and narrow both the answer and the cost. A failed read
		// is carried rather than resolved here, because the two paths answer it differently
		// (see below).
		from, to := local, versionID-1
		var alive []int64
		var lastAllocated int64
		alive, lastAllocated, deletedErr = d.liveness.VersionLiveness(ctx, kbID, &from, &to)
		if deletedErr == nil {
			aliveSet := make(map[int64]bool, len(alive))
			for _, id := range alive {
				aliveSet[id] = true
			}
			deleted = make(map[int64]bool)
			for v := local + 1; v < versionID; v++ {
				if v <= lastAllocated && !aliveSet[v] {
					deleted[v] = true
				}
			}
		} else {
			d.logger.Warn("plane: backfill cannot read the liveness of the versions in the gap",
				zap.String("kb_id", kbID), zap.Int64("from_version", local),
				zap.Int64("to_version", versionID-1), zap.Error(deletedErr))
		}
	}

	// §7.5: try the delta path first — it transfers what changed instead of every
	// version's full record set. It only applies when the source has a record for
	// EVERY version in the gap: a missing delta cannot be told apart from a version
	// that changed nothing, and replaying the rest would leave a hole behind the
	// cursor.
	if d.changesFetcher != nil {
		err := d.backfillByChanges(ctx, source, kbID, local, versionID, deleted)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errBackfillGapIncomplete) {
			// A transport failure says nothing about the data: fall through to full
			// records, which is the path that always works.
			d.logger.Warn("plane: delta backfill failed; using full records",
				zap.String("kb_id", kbID), zap.Int64("from_version", local),
				zap.Int64("to_version", versionID), zap.Error(err))
		}
	}

	if deletedErr != nil {
		// "I could not find out" is not "it is gone" — but unlike the delta path, this
		// path has no conservative default to fall back on: it has to know which
		// versions were removed before it decides what to pull, so an unreadable answer
		// stops the backfill here.
		return fmt.Errorf("plane: backfill %s: read deleted versions: %w", kbID, deletedErr)
	}

	for v := local + 1; v < versionID; v++ {
		// Check BEFORE pulling. A version can be genuinely EMPTY (nothing to send)
		// or GONE (deleted, its records no longer on any node), and the storage layer
		// cannot tell them apart — absent rows mean both. The replicated metadata can,
		// so it is asked. Two things follow from asking first: a confirmed-deleted
		// version is never fetched at all (no pointless transfer), and "the gap was
		// handed to the full-state path without pulling the deleted version" becomes
		// something a caller can observe — which is how the end-to-end case proves the
		// fallback actually ran. Advancing the cursor over a vanished version would
		// claim history this node never received, and the cursor's whole meaning is
		// "contiguous up to here" (§7.5).
		if deleted[v] {
			// §6.4: the version is confirmed gone (a middle version may be
			// deleted), so its records exist nowhere and the gap cannot be filled
			// version by version. Fall back to a full-state transfer.
			//
			// Its source is chosen for versionID and NOT for the gap. The transfer
			// hands over the version being APPLIED, and a replica whose cursor stops
			// at versionID-1 — exactly what the gap's own need admits — holds no data
			// for it. Such a replica answers with an empty stream, which the sync
			// layer reports as success and cannot tell apart from a version that is
			// genuinely empty (Follower.PullVersionData advances the cursor on any
			// completed receive, including one that carried nothing), so transferring
			// from it would move this node's cursor onto a version it does not hold —
			// and "the cursor says the data landed" is what every later fetch is
			// short-circuited by. Failing here instead keeps the retry available.
			holder, ok := d.pickTransferSource(ctx, kbID, versionID, source)
			if !ok {
				return fmt.Errorf("plane: backfill %s: v%d was deleted and no replica holds v%d to transfer its state from: %w",
					kbID, v, versionID, stratumerrors.ErrIndexNotReady)
			}
			return d.transferFullState(ctx, holder, kbID, local, versionID, v)
		}
		// Data only. Backfill fills in a GAP — the versions between this node's
		// cursor and the one being applied — and nothing asked for those versions:
		// they are being filled so the chain is unbroken, not so they can be
		// served. Building each of them would be the eager behaviour §8.6(b)
		// removed, multiplied by the size of the gap.
		if d.skipIfDeleted(ctx, kbID, v, "backfill") {
			// The gap's version is one this node knows was deleted: it must not be
			// replayed, and the cursor must not step over a version whose records are
			// not here. Leaving the cursor below it is what keeps "the cursor is where
			// my history is unbroken" true; the reconciliation is what later confirms
			// the deletion from the control layer's side.
			continue
		}
		if err := d.puller.PullVersionData(ctx, source, kbID, v); err != nil {
			return fmt.Errorf("plane: backfill %s v%d from %s (local cursor %d): %w", kbID, v, source, local, err)
		}
		d.dataLanded(kbID, v)
	}
	return nil
}

// transferFullState is §6.4's last resort — the analogue of Raft's InstallSnapshot.
// It runs only when a version in the gap is CONFIRMED deleted by the metadata, never
// on a transport failure or an unreadable topology: those are "I could not find
// out", and the answer to them stays "abort the apply" rather than "silently skip".
// That distinction is the whole reason the gap's versions are judged from the
// replicated state instead of from the storage layer's absent rows.
//
// Its source must HOLD the snapshot version — the caller picks it with pickTransferSource
// for exactly that reason. An empty answer is reported as a successful transfer and looks
// identical to a version that is genuinely empty, so a source that merely reaches the gap
// would move this node's cursor onto a version it does not have.
//
// The node transfers ONE whole version's state and moves its cursor straight to it.
// The version it transfers is versionID — the version being applied — and NOT
// versionID-1, which is what "the newest one the gap needs" would suggest. The
// reason is the whole point of this path: the gap is unfillable BECAUSE one of its
// versions was deleted, and versionID-1 may well be that version. Naming a version
// we know is gone as the snapshot would leave this path failing exactly when it is
// needed. versionID, by contrast, is committed and therefore present everywhere the
// apply reached. It can be transferred without dragging the chain behind it because
// a version's docID list is stored whole, not as a delta from its parent.
//
// What this costs, stated plainly: the versions between the old cursor and the
// snapshot are NOT held afterwards, so the cursor now means "contiguous up to here,
// with everything below this point replaced by the snapshot". Reads of one of those
// versions fail — which is where they already stood, since a deleted middle version
// is unreadable to every node.
func (d *LocalDataPlane) transferFullState(ctx context.Context, sourceAddr, kbID string, local, versionID, deletedVersion int64) error {
	snapshot := versionID
	if d.skipIfDeleted(ctx, kbID, snapshot, "backfill full-state") {
		// The snapshot version itself is one this node knows was deleted, so there is
		// no state to transfer: a removal happened after the deletion this transfer was
		// begun for, and the caller has to re-judge the gap (the next pass does).
		return fmt.Errorf("plane: backfill %s from %s: the snapshot version v%d is itself known deleted here",
			kbID, sourceAddr, snapshot)
	}
	if err := d.puller.PullVersionData(ctx, sourceAddr, kbID, snapshot); err != nil {
		return fmt.Errorf("plane: backfill %s from %s: v%d was deleted (local cursor %d) and the full-state transfer of v%d failed: %w",
			kbID, sourceAddr, deletedVersion, local, snapshot, err)
	}
	// §7.5: the snapshot IS this version's whole state, so the cursor may move up
	// to it — but it has to move as a run of ACCOUNTED versions, not as a bare
	// jump (which the cursor no longer performs for anyone). Every version the
	// transfer skipped is accounted for by it: they are not individually held (the
	// log line below says exactly which), but the gap they belonged to is
	// precisely what the snapshot replaced. Registering them in ONE step keeps
	// this O(versions skipped) instead of one cursor scan per version.
	d.markVersionsHandled(kbID, local+1, snapshot)
	d.logger.Warn("plane: backfilled via full-state transfer after a deleted version; the skipped versions are not readable locally",
		zap.String("kb_id", kbID), zap.Int64("from_version", local),
		zap.Int64("deleted_version", deletedVersion), zap.Int64("snapshot_version", snapshot))
	return nil
}

// errBackfillGapIncomplete reports that the delta path cannot serve this gap at
// all. It has two causes, and the caller deliberately does not have to tell them
// apart: the source has no recorded changes for some version in the gap, or the
// replicated metadata no longer has that version. Both mean the same thing —
// "replay the source's deltas" is not an option for this range — and both have the
// same remedy, which already exists: the full-state transfer, the path written
// precisely for a gap that holds a deleted version (§6.4).
var errBackfillGapIncomplete = errors.New("backfill: the gap cannot be replayed from the source's recorded changes")

// backfillByChanges replays the source's recorded deltas for (local, versionID-1],
// in ascending order. It refuses the WHOLE range when any version cannot be
// replayed: a partial replay would leave a hole in this node's history while
// advancing its cursor over it, and the cursor is what makes "version number
// comparison implies completeness" true (§7.5).
//
// A version cannot be replayed for either of two reasons, and BOTH are checked
// here rather than only the first:
//
//   - the source has no delta for it. The source is honest about this by
//     construction: ChangesInRange reports a version it holds no BEGIN record for
//     as ABSENT rather than as "nothing changed" (internal/wal/file.go), so the gap
//     is visible instead of silent.
//
//   - the metadata records the version as REMOVED. This is the case the delta
//     path alone cannot see, and it is not hypothetical: a version's BEGIN record
//     leaves the source's WAL only once every replica that should hold it has a
//     cursor past it (ReclaimChanges), so for as long as some
//     replica is behind — the very situation a backfill runs in — a DELETED
//     version's delta is still served. Replaying it would write that version's data
//     on a node whose metadata says the version is gone, and nothing would later
//     detect it: the judgement that names such data is exactly the metadata row
//     that is missing. transferFullState exists for a gap holding a deleted
//     version; the delta path has to hand the gap over instead of reconstructing it.
//
// "I could not read the metadata" is NOT "the version is gone", so a nil set — which
// is what the caller passes when the read failed, or when no checker is wired at all
// — leaves this check OFF and the gap is replayed exactly as it was before the check
// existed. That is the conservative direction available here: treating an unreadable
// answer as a verdict would turn a transient metadata read failure on this node into
// a full-state transfer, a transfer decision coupled to a local read it does not
// belong to.
//
// deleted is the CALLER's read (see backfillTo), not one of this function's own: the
// full-record path needs the same answer, and reading it per path paid for it twice on
// every gap that fell back.
func (d *LocalDataPlane) backfillByChanges(ctx context.Context, sourceAddr, kbID string, local, versionID int64, deleted map[int64]bool) error {
	deltas, err := d.changesFetcher.ChangesInRange(ctx, sourceAddr, kbID, local, versionID-1)
	if err != nil {
		return fmt.Errorf("plane: delta backfill %s (%d,%d] from %s: %w", kbID, local, versionID-1, sourceAddr, err)
	}

	for v := local + 1; v < versionID; v++ {
		if _, ok := deltas[v]; !ok {
			return fmt.Errorf("plane: delta backfill %s (%d,%d] from %s: no record for v%d: %w",
				kbID, local, versionID-1, sourceAddr, v, errBackfillGapIncomplete)
		}
		if deleted[v] {
			return fmt.Errorf("plane: delta backfill %s (%d,%d] from %s: v%d no longer exists: %w",
				kbID, local, versionID-1, sourceAddr, v, errBackfillGapIncomplete)
		}
	}

	for v := local + 1; v < versionID; v++ {
		delta := deltas[v]
		if err := d.ApplyBackfillChanges(ctx, kbID, v, delta.ParentVersionID, delta.Changes); err != nil {
			return err
		}
	}
	return nil
}

// pickBackfillSource returns the first candidate whose contiguous history
// already reaches need, falling back to fallback when none does — or when no
// cursor querier or candidate list is configured. A peer that cannot be
// reached is skipped: it cannot serve as a source, but its silence says
// nothing about the others.
func (d *LocalDataPlane) pickBackfillSource(ctx context.Context, kbID string, need int64, fallback string) string {
	if d.cursorQuerier == nil || d.resolveReplicas == nil {
		return fallback
	}
	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		return fallback
	}
	for _, peer := range peers {
		if peer == fallback {
			continue // already the default source
		}
		// Bounded for the same reason as SafeDurableVersion's peer queries: this can
		// run while a peer is not serving yet, and an unbounded wait would turn a
		// backfill choice into a hang.
		peerCtx, cancel := context.WithTimeout(ctx, d.peerCursorTimeout)
		cursor, err := d.cursorQuerier.LocalVersionOf(peerCtx, peer, kbID)
		cancel()
		if err != nil {
			continue
		}
		if cursor >= need {
			d.logger.Info("plane: backfill source selected",
				zap.String("peer", peer), zap.String("kb_id", kbID), zap.Int64("peer_cursor", cursor))
			return peer
		}
	}
	return fallback
}

// pickTransferSource returns a replica whose contiguous cursor already reaches need, and
// whether one exists. Candidates are the resolved replicas plus the caller's own default
// source, which may be a replica resolveReplicas does not list.
//
// It deliberately has NO fallback, unlike pickBackfillSource: a full-state transfer asks
// for the version being applied, and "the default source" is not evidence that anyone
// holds it. When nothing does, the caller must fail rather than transfer from a replica
// that will answer with an empty stream — on the wire, "this source has no data for that
// version" and "that version has no data" are the same thing.
//
// An unwired cursorQuerier leaves the caller's source as the only one there is. That is
// the pre-existing behaviour for a plane assembled without one, and it keeps those
// deployments and tests working unchanged.
func (d *LocalDataPlane) pickTransferSource(ctx context.Context, kbID string, need int64, also string) (string, bool) {
	if d.cursorQuerier == nil {
		if also == "" {
			return "", false
		}
		return also, true
	}
	candidates := make([]string, 0, 4)
	if d.resolveReplicas != nil {
		if peers, err := d.resolveReplicas(ctx); err == nil {
			candidates = append(candidates, peers...)
		}
	}
	if also != "" {
		candidates = append(candidates, also)
	}
	seen := make(map[string]bool, len(candidates))
	for _, peer := range candidates {
		if peer == "" || seen[peer] {
			continue
		}
		seen[peer] = true
		// Bounded for the same reason as pickBackfillSource's query: this can run
		// while a peer is not serving yet, and an unbounded wait would turn a source
		// choice into a hang.
		peerCtx, cancel := context.WithTimeout(ctx, d.peerCursorTimeout)
		cursor, err := d.cursorQuerier.LocalVersionOf(peerCtx, peer, kbID)
		cancel()
		if err != nil {
			continue
		}
		if cursor >= need {
			d.logger.Info("plane: full-state transfer source selected",
				zap.String("peer", peer), zap.String("kb_id", kbID),
				zap.Int64("need_version", need), zap.Int64("peer_cursor", cursor))
			return peer, true
		}
	}
	return "", false
}

// PeerHolders asks the replicas how far they have reached for kbID, and returns the
// furthest version anyone reached with the peers that reached it. Those peers hold every
// version below it too, since a cursor is a contiguous prefix.
//
// ok=false means no peer holds anything it could say so about: a silent peer is not
// evidence of anything, so it is skipped rather than held against the answer.
func (d *LocalDataPlane) PeerHolders(ctx context.Context, kbID string) (version int64, addrs []string, ok bool) {
	if d.cursorQuerier == nil || d.resolveReplicas == nil {
		return 0, nil, false
	}
	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		return 0, nil, false
	}
	for _, peer := range peers {
		peerCtx, cancel := context.WithTimeout(ctx, d.peerCursorTimeout)
		cursor, err := d.cursorQuerier.LocalVersionOf(peerCtx, peer, kbID)
		cancel()
		if err != nil || cursor <= 0 || cursor < version {
			continue
		}
		if cursor > version {
			version, addrs = cursor, addrs[:0]
		}
		addrs = append(addrs, peer)
	}
	if len(addrs) == 0 {
		return 0, nil, false
	}
	sort.Strings(addrs)
	return version, addrs, true
}

// peerCursorTimeout bounds ONE peer's cursor query.
//
// It exists because the callers run during STARTUP: ReconcileIndexes asks every peer
// what it holds, and every storage node does that concurrently, reaching
// grpcServer.Serve only after its own reconcile returns. With no bound here the
// cluster deadlocks at boot — each node waits for the others while none of them is
// serving. Measured on a node holding historical data: it never logged "Stratum gRPC
// server listening", and the control layer got "connection refused" from every
// storage address.
//
// Short on purpose. A peer that cannot answer within this window while starting is
// not going to answer sooner if we wait longer, and the quorum check is what decides
// whether the answers that DID arrive suffice. Like the other budget numbers this is
// a placeholder: raise it if startup on slow disks shows peers being skipped that
// would have answered.
//
// The default for LocalDataPlaneConfig.PeerCursorTimeout.
const peerCursorTimeout = 2 * time.Second
