// Data cursor (§4.1) and its persistence (§4.2).
//
// The cursor is a SCALAR per knowledge base — "the highest version this node
// holds completely" — and it is the node's own fact, not the Raft applied index.
// Everything that advances it lives here, together with the WAL record that
// survives a restart and the in-memory queue that lets the value lead the
// persisted one (writes must never block on an fsync).

package plane

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"stratum/internal/types"
)

// advanceLocalVersion records that this node has accounted for versionID: it
// holds the version's data (written here, pulled, pushed, or covered by a
// full-state transfer) or the version no longer exists (dropped as deleted).
//
// The cursor advances only over an UNBROKEN run of accounted versions. A version
// known to exist but not yet accounted for stops the cursor below itself, which
// is what keeps "the cursor is where my history is unbroken" true when a push to
// this node is lost and a later version's push still lands (§7.5, §9.3(2)).
//
// The cursor never moves back: re-applying an older version (idempotent retries,
// backfill replay) must not move it down, and a version announced only after the
// cursor has already passed it does not pull the cursor back either. The fix is
// for the future, not a retroactive correction.
func (d *LocalDataPlane) advanceLocalVersion(kbID string, versionID int64) {
	// Whatever brought us here changed what this node holds — including a late
	// push (a version landing under a cursor that has already moved past it,
	// §B's no-failure-needed leftover), which the machine's advance treats as a
	// no-op on the cursor but not on what this node holds. The reverse
	// reconciliation only has to run again after such a change, so mark it
	// before asking.
	d.noteHoldingsChanged()

	// Data first, cursor second — the invariant the whole record depends on. It
	// is enforced HERE, on the single path every advance goes through, rather
	// than at the call sites: there are a dozen of them (write, pull, push,
	// backfill, drop, full-state transfer), and one forgetting the order would
	// let a restarted node claim history it does not hold.
	//
	// The value queued is the cursor AFTER the step, not versionID: the cursor
	// only moves over a contiguous run, so after an advance it may have stepped
	// over several versions at once — and that boundary is the thing worth
	// recording.
	d.persistCursor(kbID, d.state.Advance(kbID, versionID))
}

// noteHoldingsChanged records that what this node HOLDS has changed — a version
// landed, was reclaimed, or the cursor moved — so the next reverse
// reconciliation has to run.
//
// It is set on every advance rather than only on the ones that can actually
// leave a leftover, and that is the conservative direction on purpose: "did
// that cleanup land" is a much harder fact to observe here than "something
// changed", and a missed mark means a leftover nothing will ever collect.
func (d *LocalDataPlane) noteHoldingsChanged() {
	d.versionMu.Lock()
	d.deletedReconcileDirty = true
	d.versionMu.Unlock()
}

// dataLanded records a version landing on this node: the fact first (the state
// machine), then the cursor (how far the unbroken history reaches).
//
// The order is the invariant every other advance follows — data first, cursor
// second — and it is why this is one named pair rather than the cursor call
// alone at each of the half-dozen sites that produce a landing (a local write,
// a pull, a push, a backfill, a resumed transaction).
func (d *LocalDataPlane) dataLanded(kbID string, versionID int64) {
	d.noteDataLanded(kbID, versionID)
	d.advanceLocalVersion(kbID, versionID)
}

// cursorStepsPast records the OPPOSITE fact from dataLanded: this node will
// never hold versionID, so the cursor has to be able to move over it (a version
// the control layer retired and this node reclaimed — see
// reclaimTerminalVersionLocally). Telling the state machine is what keeps "the
// version is not here" from being recorded as "it landed and then vanished".
func (d *LocalDataPlane) cursorStepsPast(kbID string, versionID int64) {
	d.noteVersionDropped(kbID, versionID)
	d.advanceLocalVersion(kbID, versionID)
}

// noteDataLanded reports a landing to the state machine, if one is wired.
//
// A refused transition is LOGGED, not returned: it means this layer produced an
// event the machine's table says cannot happen, which is worth seeing — but the
// storage write has already committed by the time this runs, so failing the
// write over a bookkeeping disagreement would turn a reporting bug into data
// loss. The machine keeps its previous value and the log is the evidence.
func (d *LocalDataPlane) noteDataLanded(kbID string, versionID int64) {
	if err := d.state.DataLanded(kbID, versionID); err != nil {
		d.logger.Warn("plane: node state: landing not recorded",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

// noteVersionDropped reports a removal to the state machine, if one is wired.
// Nothing can refuse it, so there is nothing to log.
func (d *LocalDataPlane) noteVersionDropped(kbID string, versionID int64) {
	d.state.VersionDropped(kbID, versionID)
}

// AnnounceVersion records that the control layer says versionID exists. It is the
// "known gap" half of the §7.5 invariant: a version this node has been told about
// but does not hold must block the cursor from stepping over it.
//
// Announcing is idempotent and cheap, and it deliberately does NOT move the
// cursor: existence is not possession.
func (d *LocalDataPlane) AnnounceVersion(kbID string, versionID int64) {
	d.state.Announce(kbID, versionID)
}

// markVersionsHandled accounts for every version in [from, to] in one step. It is
// for the paths that obtain a whole VERSION'S state at once — §6.4's full-state
// transfer is the only one — where advancing version by version would re-scan the
// pending sets once per version.
func (d *LocalDataPlane) markVersionsHandled(kbID string, from, to int64) {
	d.persistCursor(kbID, d.state.MarkHandled(kbID, from, to))
}

// MarkVersionContiguous implements sync.LocalVersionAdvancer: a version whose
// records this node has received — pushed by the coordinator or pulled from a
// peer — is one it holds, and the cursor is how it says so.
//
// The two halves are wired separately on purpose: this plane owns the cursor,
// while the sync handler is what knows the records landed. Without the wiring a
// replica with every record on disk still answered "version 0" to the station's
// freshness check (§9.3(2)) and was refused as stale.
func (d *LocalDataPlane) MarkVersionContiguous(kbID string, versionID int64) {
	d.dataLanded(kbID, versionID)
}

// LocalVersionOf reports the highest version this node holds contiguously for
// kbID (0 = nothing known yet). It is the storage layer's answer to peers
// looking for a backfill source (Stratum_设计文档v13.md §7.6).
func (d *LocalDataPlane) LocalVersionOf(kbID string) int64 {
	return d.localVersionOf(kbID)
}

// localVersionOf reports the highest version this node holds contiguously for
// kbID (0 = nothing known yet).
func (d *LocalDataPlane) localVersionOf(kbID string) int64 {
	return d.state.Cursor(kbID)
}

// cursorWriteTimeout bounds ONE cursor record's write. The write is a local
// append plus fsync, so this is generous; it exists so a stuck disk can never
// pin the flusher goroutine forever — the value stays queued either way.
//
// The default for LocalDataPlaneConfig.CursorWriteTimeout.
const cursorWriteTimeout = 5 * time.Second

// cursorRetryDelay is how long flushCursors waits before retrying a batch it
// could not write. It only ever applies to a FAILED batch — the happy path never
// reaches it — and it exists for the same reason the failed value is re-queued at
// all: without a delay, re-queueing spins the flusher at disk-error speed, one
// repeated warning per fsync attempt, for as long as the disk misbehaves.
//
// The default for LocalDataPlaneConfig.CursorRetryDelay.
const cursorRetryDelay = time.Second

// persistCursor queues a cursor value for persistence. It never blocks and never
// touches the disk itself: the callers are the write path (§7.7) and the Raft
// apply path (via onVersionCreated), and IO there would stall everything behind
// it.
//
// Merging is per knowledge base and keeps only the highest pending value,
// because the cursor is a scalar: an intermediate value says nothing the newer
// one does not, so a burst of advances costs ONE record.
func (d *LocalDataPlane) persistCursor(kbID string, versionID int64) {
	if d.cursorWAL == nil || versionID <= 0 {
		return
	}
	d.cursorMu.Lock()
	if versionID > d.cursorPending[kbID] {
		d.cursorPending[kbID] = versionID
	}
	if d.cursorFlushing {
		d.cursorMu.Unlock()
		return
	}
	d.cursorFlushing = true
	d.cursorMu.Unlock()
	go d.flushCursors()
}

// flushCursors drains the queue, one knowledge base at a time, on its own
// goroutine. Exactly one flusher runs at a time (cursorFlushing), so the records
// for one knowledge base are written in the order they were queued.
//
// A failed write is logged and its value goes back in the queue, AFTER a delay
// (cursorRetryDelay). Both halves matter, and they pull in opposite directions:
// dropping the value would leave the cursor unpersisted until some later advance
// happened to come along, while re-queueing it with no delay turns a disk outage
// into a hot loop — the same warning at fsync speed for as long as the outage
// lasts.
//
// The in-memory cursor is deliberately NOT rolled back. Rolling it back would
// make this node re-fetch data it has already served — and the lagging record is
// safe by construction: it can only understate the cursor, so a restart backfills
// a little more than it strictly must. Only the CRASHED state is low, never the
// running one.
func (d *LocalDataPlane) flushCursors() {
	for {
		d.cursorMu.Lock()
		pending := d.cursorPending
		d.cursorPending = make(map[string]int64)
		if len(pending) == 0 {
			d.cursorFlushing = false
			d.cursorMu.Unlock()
			return
		}
		d.cursorMu.Unlock()

		var failed bool
		for kbID, versionID := range pending {
			ctx, cancel := context.WithTimeout(context.Background(), d.cursorWriteTimeout)
			err := d.cursorWAL.WriteCursor(ctx, kbID, versionID)
			cancel()
			if err == nil {
				continue
			}
			failed = true
			d.logger.Warn("plane: persisting the data cursor failed; it will be retried, and the record lags the data meanwhile",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			d.cursorMu.Lock()
			if versionID > d.cursorPending[kbID] {
				// Nothing newer superseded this value while the write was in
				// flight, so it goes back in the queue — it must not be dropped,
				// because another advance is not guaranteed to come and the
				// record is the only thing a restart reads.
				d.cursorPending[kbID] = versionID
			}
			d.cursorMu.Unlock()
		}
		if failed {
			// Back off rather than spin, once per batch rather than per record.
			// An advance arriving during the sleep is not lost: it lands in
			// cursorPending (cursorFlushing still reports a flusher running) and
			// the next pass of this loop picks it up.
			time.Sleep(d.cursorRetryDelay)
		}
	}
}

// persistedCursors reads back the cursors this node recorded. A plane assembled
// without a cursor store answers "nothing persisted", which is exactly what the
// inference fallback needs to see.
func (d *LocalDataPlane) persistedCursors(ctx context.Context) (map[string]int64, error) {
	if d.cursorWAL == nil {
		return nil, nil
	}
	return d.cursorWAL.RecoverCursors(ctx)
}

// installPersistedCursor installs a cursor read back from the WAL.
//
// It deliberately does NOT go through advanceLocalVersion: that path exists to
// PERSIST the value, and this value came from the log — writing it again would
// be a redundant fsync on every startup. Monotone for the same reason the cursor
// is: memory may already have moved past the record (a write that landed before
// this ran), and moving back would forget versions this node holds.
func (d *LocalDataPlane) installPersistedCursor(kbID string, versionID int64) {
	d.state.InstallCursor(kbID, versionID)
}

// DataVersionsSnapshot returns a copy of every knowledge base's contiguous cursor, which
// the lag catch-up compares against the chain tails.
//
// A copy, not the live map: the caller may hold it across a tick, and a concurrent write
// must not mutate it. Knowledge bases with no local data are absent rather than zero.
func (d *LocalDataPlane) DataVersionsSnapshot() map[string]int64 {
	return d.state.Cursors()
}

// emptyDocIDSetHash is the digest of a version whose document set is empty — the
// version every knowledge base is created with, and any version created with no
// document changes. It is a value, not an absence, and it is defined once in
// internal/types because BOTH layers need it: the control layer records it when it
// creates such a version, and this one recognises it.
var emptyDocIDSetHash = types.EmptyDocIDSetHash

// RecoverLocalCursors rebuilds this node's contiguous data cursor for every
// knowledge base from LOCAL facts, once at startup.
//
// Why it is needed: the cursor lives in memory (§7.8), so a restarted node
// starts at 0 for every knowledge base while its disk still holds the data. The
// station's freshness check (§9.3(2)) reads that cursor, so this replica — data
// complete, index present — is refused with "local history reaches version 0,
// below the required N" until some later write happens to advance it. Measured
// on the 3+3 cluster: after restarting one storage node, its sixth query failed
// with exactly that error (TestT4_QueryLatency).
//
// The evidence is local, not the peers': what a node holds is a question about
// its own disk. §7.8's quorum bound answers a different one ("what may I claim
// durable?"), and it cannot answer this — a restarted node's own report is 0,
// so the quorum minimum of {0, N, N} is 0 and the cursor would stay where it
// was.
//
// Three facts say "this version is here": an artifact on this node (built here
// or received via §8.4); a committed empty document set, because the empty set
// IS that version's content; and a READY version whose digest was never
// committed, which is how a version created with no changes looks. The third is
// what keeps an EMPTY knowledge base servable after a restart — it has no
// artifact anywhere, so the first two never fire, and the cursor would stay at 0
// for good. holdsVersionLocally owns the order they are consulted in.
//
// Only a CONTIGUOUS prefix of the version chain is recovered: a node holding v3
// and v5 but not v4 must not claim 5, because the cursor is what "my history is
// unbroken to here" means.
func (d *LocalDataPlane) RecoverLocalCursors(ctx context.Context, meta MetadataLister) error {
	if meta == nil {
		return nil
	}
	// The persisted record comes FIRST (docs/cursor-persistence-plan.md §3.5).
	// It is a direct statement of the local fact, so a knowledge base that has
	// one needs no inference — and the inference below is exactly what the record
	// exists to replace: it reads an index artifact off this node's disk, and an
	// artifact is a CACHE the retention policy may have deleted. Judging "what I
	// hold" from a cache is what let a node holding v1..v10 report a cursor of 0
	// after a restart.
	persisted, err := d.persistedCursors(ctx)
	if err != nil {
		d.logger.Warn("plane: cursor recovery: reading the persisted cursors failed; inferring instead",
			zap.Error(err))
	}

	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		return fmt.Errorf("plane: RecoverLocalCursors: list knowledge bases: %w", err)
	}

	for _, kb := range kbs {
		if versionID, ok := persisted[kb.KBID]; ok {
			d.installPersistedCursor(kb.KBID, versionID)
			// Logged even when it changed nothing, and worded so it cannot be
			// mistaken for the inference below: "read the record back" and
			// "reconstructed from what the disk still held" are different facts,
			// and an operator looking at a low cursor needs to know which one.
			d.logger.Info("plane: cursor recovery: read the persisted data cursor back",
				zap.String("kb_id", kb.KBID), zap.Int64("version_id", versionID))
			continue
		}

		versions, err := meta.ListVersions(ctx, kb.KBID)
		if err != nil {
			d.logger.Warn("plane: cursor recovery: ListVersions failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
			continue
		}
		sort.Slice(versions, func(i, j int) bool { return versions[i].VersionID < versions[j].VersionID })

		var recovered int64
		for _, v := range versions {
			reason, holds := d.holdsVersionLocally(ctx, kb.KBID, v)
			if !holds {
				d.logger.Info("plane: cursor recovery: stopped at a version this node does not hold",
					zap.String("kb_id", kb.KBID),
					zap.Int64("version_id", v.VersionID),
					zap.String("reason", reason),
					zap.String("doc_id_set_hash", v.DocIDSetHash),
					zap.String("index_status", v.IndexStatus.String()),
					zap.Int64("recovered_to", recovered))
				break
			}
			recovered = v.VersionID
		}
		if recovered <= d.localVersionOf(kb.KBID) {
			continue
		}
		d.advanceLocalVersion(kb.KBID, recovered)
		d.logger.Info("plane: cursor recovery: recovered the local data cursor from disk",
			zap.String("kb_id", kb.KBID),
			zap.Int64("version_id", recovered),
			zap.Int("versions_considered", len(versions)))
	}
	return nil
}
