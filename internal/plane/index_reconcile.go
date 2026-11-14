// The index side of the storage layer (§5): making a version's index
// serviceable on this node, and reconciling what the control layer believes with
// what is actually on disk.
//
// The reconcile is the authority for "built and durable" — a persisted index
// file, not a callback that may never have been delivered (see
// ReconcileIndexes for the four cases it derives).

package plane

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	stratinternalsync "stratum/internal/sync"
	"stratum/internal/types"
)

// EnsureIndex brings (kbID, versionID) to the state THIS NODE is meant to hold
// it in: always the data, and the index only if this node owns the version.
//
// The data half is the "replication lives inside the storage layer" move of
// control-data-separation-design.md §7. The index half is §8.4's owner sharding:
// one node builds and everyone else takes the artifact from it, so a node that
// is not the owner ends up here with data and no index, and that is the intended
// end state rather than a half-done job.
//
// Callers are therefore of two kinds, and the difference matters:
//
//   - Background catch-up (the apply hook's active version, lag catch-up):
//     "make this node able to serve the version if it ever has to". Non-owners
//     stop at the data, because building here is exactly the duplicate work
//     sharding exists to remove, and because the head's artifact is on its way.
//   - A caller that needs to ANSWER FROM THIS NODE NOW: it must ask for the
//     artifact and, failing that, build it. That fallback lives with the query
//     path (service/query.go), not here — a query that cannot get an artifact
//     still has to answer, while a catch-up that cannot get one can simply wait
//     for the next query to ask again.
//
// Idempotent, so re-running it after a crash is safe.
func (d *LocalDataPlane) EnsureIndex(ctx context.Context, kbID string, versionID int64) error {
	if d.puller == nil {
		// Nowhere to fetch from: this node has no storage-layer connection to a peer
		// (a control node, or a plane assembled without one). Failing loudly beats
		// dereferencing a nil puller, and it is also the truth the caller needs — this
		// node cannot bring the version here.
		return fmt.Errorf("plane: EnsureIndex(%s, %d): this node has no puller", kbID, versionID)
	}
	// Holding the version's data already is the authoritative "nothing to
	// FETCH" fact, and after §8.5 it is the *only* one: the old code read
	// resolve()'s ok=false as "I am the writer", which held only while the
	// writer was also the leader. The contiguous cursor says it directly — it
	// advances exactly when a version's data has landed (write path, pull, or
	// backfill) and never moves backwards.
	//
	// It is not, however, "nothing to DO". The cursor is the DATA cursor, and a
	// node can hold every byte of a version and still have no index for it — which
	// for a node that has been ASKED for the version is a job outstanding, not a
	// finished one. Reading it as "done" is what left versions built by nobody:
	// measured on the 3+3 cluster, storage2 held v4 and v7 with no index and built
	// neither, while all three replicas reported the data present. So the index
	// half is checked separately, below.
	// Known deleted here: there is nothing to fetch and nothing to serve. Asked
	// again (the catch-up loop, a query's background pull), it stays a no-op — and
	// crucially it does not fall through to the pull below, which would write the
	// version's records back and move the cursor onto a version this node does not
	// hold (its metadata is gone).
	if d.skipIfDeleted(ctx, kbID, versionID, "EnsureIndex") {
		return nil
	}
	if d.localVersionOf(kbID) >= versionID {
		return d.ensureOwnIndex(ctx, kbID, versionID)
	}
	// The data is what has to be fetched here, and ONLY the data: the index comes
	// after, in ensureOwnIndex, because an index on this node is worth nothing
	// without the records it indexes. See that function for the ordering bug that
	// taught this — asking for the artifact first let a node return "done" holding
	// a peer's index and none of the version's data.
	addr, ok, err := d.resolve(ctx, kbID, versionID)
	if err != nil {
		return fmt.Errorf("plane: EnsureIndex(%s, %d): resolve data source: %w", kbID, versionID, err)
	}
	if !ok {
		// No source is known yet: nobody has announced this version, and there is
		// no leader to fall back on. NOTHING WAS DONE here — so this must not read
		// as success.
		//
		// Returning nil is exactly the trap data_source_registry.go:107 already
		// names ("an ok=false here makes EnsureIndex give up silently, which is
		// worse than an address that fails"), and it misleads every caller that
		// acts on the result:
		//   - plane/lag_catchup.go logs "caught up with the chain tail" for a node
		//     that caught up nothing. Measured on the 3+3 cluster: a storage replica
		//     that was away for 55 versions came back with its pre-restart cursor
		//     (365 against a chain tail of 420) and its artifact count unchanged
		//     (2 → 2), while the log claimed a catch-up;
		//   - service/query.go's bounded retry reads nil as "the pull finished" and
		//     stops retrying — though its own comment notes the announcement that
		//     would ask for the pull is best-effort and "if this attempt fails
		//     nothing else will come along";
		//   - cmd/stratum's apply hook logs an error only for a non-nil result, so
		//     this outcome was invisible there too.
		//
		// ErrIndexNotReady is the right sentinel rather than a new one: its wire
		// name is already part of the node-to-node protocol (a mixed-version
		// cluster keeps agreeing on it) and router.retryableReasons already maps it
		// to "another candidate may have it". The message carries the diagnosis,
		// since the sentinel alone cannot say which of the two reasons applies.
		return fmt.Errorf("%w: EnsureIndex(%s, %d): no data source known yet "+
			"(no replica has announced this version, and no leader is a usable data source in this topology)",
			stratumerrors.ErrIndexNotReady, kbID, versionID)
	}

	// A node must never hold version V without holding V-1: fill any known gap
	// before pulling versionID itself (Stratum_设计文档v13.md §7.5). Without
	// this a node that missed a version would apply the next one onto an
	// incomplete document set, and "version number comparison implies
	// completeness" would stop holding.
	if err := d.backfillTo(ctx, addr, kbID, versionID); err != nil {
		return err
	}

	// Pull with digest verification, retrying until the data is complete. The
	// writer commits the version's document-set digest only after its storage
	// writes finish, so this node recomputes the digest from its local store
	// after each pull and retries until it matches — closing the race where a
	// pull arrives before the writer's writes land. If the digest never
	// arrives (a missed propose on the writer), a pull that produced data is
	// accepted (the verifier's fallback).
	// The loop's bound is about PROGRESS, not about wall clock.
	//
	// It used to be a flat 30 s over the whole loop. That works for a small version
	// and is impossible for a large one: a 20,000-document version is ~56 MB, which
	// does not cross the wire, land, and get applied inside 30 s — so every attempt
	// timed out, and the retry used the same 30 s, which means the loop could not
	// converge at any number of attempts. Measured on the 3+3 cluster: 64 rounds of
	// `sync: recv SyncEntry: ... DeadlineExceeded` followed by "version data did not
	// converge within 30s", while the same transfer completed perfectly well once
	// given room.
	//
	// What failure actually looks like here — an unreachable peer, a source that no
	// longer holds the version, a stream that keeps being refused — is attempts that
	// make NO progress. So idle time is what gets bounded, with an absolute ceiling
	// so that a pathological loop still ends.
	pullIdleTimeout := d.effectivePullIdleTimeout()
	pullMaxDuration := d.effectivePullMaxDuration()
	start := time.Now()
	lastProgress := start
	attempts := 0
	var lastErr error
	backoff := 200 * time.Millisecond
	for {
		// M10 of docs/code-review-2026-09-24.md: the loop has to notice the
		// caller's context. Nothing else here does — every failure below is
		// treated as "try again" — so a cancelled pull kept running to its idle
		// timeout (30 s) or its ceiling (10 min) while the goroutine that asked
		// for it had already gone, and each retry sent RPCs nobody was waiting
		// for. Cancellation is an answer, not a failure to retry.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("plane: EnsureIndex(%s, %d): pull cancelled after %s (attempts=%d, local cursor=%d): %w",
				kbID, versionID, time.Since(start).Round(time.Millisecond), attempts,
				d.localVersionOf(kbID), err)
		}
		// Re-checked every attempt: when this node is the coordinator, its own
		// apply can run ahead of its storage writes, so the callback reaches
		// here before the data lands locally (§8.5). The write path advances
		// the cursor as it finishes, and this sees it — instead of pulling its
		// own data back from a peer until the deadline expires.
		if d.localVersionOf(kbID) >= versionID {
			// The data arrived some other way while this loop ran — this node's own
			// write, or another path's pull. Arm the §7.3 quorum check anyway: nothing
			// says a confirmation will follow, and a version every replica holds must
			// not stay PENDING (docs/known-gaps.md §K). watchForDurability skips the
			// arming when the version is already settled.
			d.watchForDurability(ctx, kbID, versionID)
			return nil
		}
		// Re-resolved every attempt as well: a coordinator that is not the
		// leader announces itself only once its own writes finish, so the first
		// resolution legitimately answers "the leader", which holds no such
		// data. Holding on to that answer would mean pulling from a pointless
		// source for the whole timeout while the announcement sits unread.
		if fresh, freshOK, freshErr := d.resolve(ctx, kbID, versionID); freshErr != nil {
			d.logger.Warn("plane: resolve data source failed, will retry",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(freshErr))
		} else if freshOK && fresh != addr {
			addr = fresh
		}
		attempts++
		// DATA ONLY. This method does not build, and §8.6(b) is the reason: the
		// only version worth building eagerly is the active one, and catch-up is
		// not a reason to build anything. Building here would mean every lagging
		// node rebuilds every version the chain advances through.
		//
		// The fetch above is tried first and can succeed — a replayed log, a
		// version whose head finished long ago, a node that was simply behind. It
		// usually cannot, because background catch-up runs the moment the chain
		// tail moves, which is before the head has finished building. That is fine:
		// reaching here without an artifact means "nobody has published one yet",
		// and the answer is to wait. A query for the version will ask again and, if
		// that still fails, build it (service/query.go) — the fallback belongs to
		// the caller that has to answer, not here.
		//
		// A node left holding data and no index is not stranded, and the fallback
		// deliberately lives with the caller that needs it rather than here. A
		// query that reaches such a version asks for the artifact and, failing
		// that, builds it (service/query.go) — that path has to answer, so it
		// cannot wait. Background catch-up can, and does: the next query asks
		// again, and the head's artifact usually arrives in the meantime.
		err := d.puller.PullVersionData(ctx, addr, kbID, versionID)
		if err != nil {
			// A cancelled context surfaces here too (the transfer aborts mid-flight),
			// and it must not be logged as "will retry" — there is nobody left to
			// retry for. The loop-top check would catch it next round; returning now
			// skips a log line that would read like a peer problem.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return fmt.Errorf("plane: EnsureIndex(%s, %d): pull cancelled after %s (attempts=%d, local cursor=%d): %w",
					kbID, versionID, time.Since(start).Round(time.Millisecond), attempts,
					d.localVersionOf(kbID), ctxErr)
			}
			lastErr = err
			d.logger.Warn("plane: data pull failed, will retry",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Int("attempt", attempts),
				zap.Duration("no_progress_for", time.Since(lastProgress)),
				zap.Error(err))
		} else {
			// A COMPLETED transfer is progress, and it is recorded before the
			// verification: the writer commits the version's digest only after its own
			// storage writes finish, so "the data is here and the digest is not yet" is
			// a reason to keep going, not to give up. Counting it as progress is what
			// lets a large version finish across several attempts instead of timing out
			// on a clock that was never about the data.
			lastProgress = time.Now()
			lastErr = nil
			if d.verify(ctx, kbID, versionID) {
				d.dataLanded(kbID, versionID)
				// A pulled version gets no coordinator confirmation. This is the path a
				// lagging replica's catch-up target takes, and a PENDING version is
				// always the chain's tail, so this is where the check matters most.
				d.watchForDurability(ctx, kbID, versionID)
				// The data is here; the index is a separate question, and this node
				// was ASKED for the version — so it takes a peer's artifact or builds
				// one. Returning straight after the data left a requested version
				// with no index on this node at all, which is what the empty-version
				// branch below had already got right.
				return d.ensureOwnIndex(ctx, kbID, versionID)
			}
			if empty, emptyErr := d.versionHasNoDocuments(ctx, kbID, versionID); emptyErr == nil && empty {
				// The pull succeeded and the version holds no documents — so the
				// empty set IS its content. Waiting for a digest here means waiting
				// forever: a writer never commits one for a version with no document
				// set. The cursor would stay below a version this node in fact
				// holds, which is what made a freshly created knowledge base answer
				// "local history reaches version 0" to the station's freshness
				// check (§9.3(2)) and refuse the query.
				//
				// Gated on the pull having SUCCEEDED just above: a pull that failed
				// never reaches here, so a broken transfer can never be mistaken for
				// an empty version.
				d.dataLanded(kbID, versionID)
				d.watchForDurability(ctx, kbID, versionID)
				// The data is here; the index is a separate question, and a node
				// that was ASKED for this version answers it by building.
				return d.ensureOwnIndex(ctx, kbID, versionID)
			}
		}
		// Both ceilings carry the diagnosis the old message could not: how far this
		// node got, how many attempts were spent, and what the last attempt said. A
		// bare "did not converge within 30s" left an operator to guess whether the
		// peer was gone, the transfer too big, or the digest missing.
		if idle := time.Since(lastProgress); idle > pullIdleTimeout {
			return fmt.Errorf(
				"plane: EnsureIndex(%s, %d): version data did not converge — %s without progress "+
					"(attempts=%d, local cursor=%d, target=%d, elapsed=%s, last error=%v)",
				kbID, versionID, idle.Round(time.Second), attempts,
				d.localVersionOf(kbID), versionID, time.Since(start).Round(time.Second), lastErr)
		}
		if elapsed := time.Since(start); elapsed > pullMaxDuration {
			return fmt.Errorf(
				"plane: EnsureIndex(%s, %d): version data did not converge within %s "+
					"(attempts=%d, local cursor=%d, target=%d, last error=%v)",
				kbID, versionID, pullMaxDuration, attempts,
				d.localVersionOf(kbID), versionID, lastErr)
		}
		// The backoff is interruptible for the same reason the loop checks ctx: a
		// cancelled pull must not sit here for up to 5 s (the cap) before noticing.
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return fmt.Errorf("plane: EnsureIndex(%s, %d): pull cancelled after %s (attempts=%d, local cursor=%d): %w",
				kbID, versionID, time.Since(start).Round(time.Millisecond), attempts,
				d.localVersionOf(kbID), ctx.Err())
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// versionHasNoDocuments reports whether this node holds no documents for the
// version — so the empty set IS the version's content.
//
// Why it needs saying out loud: §7.5's pull loop waits for the writer's
// document-set digest to match, and a version created with no changes never gets
// one (there is no document set to hash). Left alone, the loop spins out its
// whole timeout while the node's contiguous cursor stays below a version it in
// fact holds. A freshly created knowledge base is exactly that state, and the
// station's freshness check (§9.3(2)) reads the cursor — so its first query was
// refused with "local history reaches version 0" depending on whether the route
// table had refreshed yet.
//
// Conservative by construction: with no digest source it answers "no", so a gap
// here degrades to the previous behaviour (wait, then fail) instead of advancing
// the cursor on evidence it does not have. Callers must have a successful pull
// behind them before trusting the answer.
func (d *LocalDataPlane) versionHasNoDocuments(ctx context.Context, kbID string, versionID int64) (bool, error) {
	if d.digest == nil {
		return false, nil
	}
	got, err := d.digest.DigestOf(ctx, kbID, versionID)
	if err != nil {
		return false, err
	}
	return got == stratinternalsync.ComputeDocIDSetHash(nil), nil
}

// ensureOwnIndex makes this node able to serve versionID: it takes an existing
// artifact from a peer if one has it, and builds the index itself if none does.
//
// This is the whole of a storage node's rule, and it needs no knowledge of who
// owns what. WHO builds is decided in the station (§8.4 owner sets,
// internal/router): queries are routed to a version's owners, so a node that is
// never asked never builds anything. What happens here is the other half — a node
// that HAS been asked becomes able to answer.
//
// ORDER MATTERS, and it is the reason this is called only once the data has
// landed. Asking for the artifact is cheap and is tried first, but an artifact
// existing somewhere says nothing about THIS node's data: a replica can hold a
// peer's index and none of the version's records. Taking the index and returning
// early on that basis left the version's data unfetched — measured on the 3+3
// cluster, a catch-up logged "caught up with the chain tail" while the node's
// cursor stayed 55 versions behind, which is exactly the misleading-success trap
// the resolve() comment above describes.
func (d *LocalDataPlane) ensureOwnIndex(ctx context.Context, kbID string, versionID int64) error {
	if d.indexMgr == nil {
		return nil
	}
	// The artifact on THIS node's disk is the cheaper and more authoritative answer,
	// so it is asked FIRST (H9 of docs/code-review-2026-10-18.md). The order used to
	// be the other way round, which meant a version that already had a READY artifact
	// here still asked every replica for a copy on every EnsureIndex — every catch-up
	// round, every query — and installed the first one that answered, re-downloading
	// tens of megabytes and rewriting the file each time. IndexExists is what makes
	// that unnecessary; it just has to run before the question goes out.
	//
	// It deliberately does not call TriggerBuild unconditionally: a build for a
	// version whose artifact is already on disk is a FULL REBUILD — triggerBuild
	// de-duplicates in-flight builds, not finished ones — so every catch-up round
	// would rebuild.
	exists, err := d.indexMgr.IndexExists(ctx, kbID, versionID)
	if err != nil {
		// Unknown is not "absent": building on an unanswered question risks a
		// redundant rebuild, so the failure is reported and the caller retries.
		return fmt.Errorf("plane: ensureOwnIndex(%s, %d): check for an existing artifact: %w", kbID, versionID, err)
	}
	if exists {
		return nil
	}
	// Nothing here: somebody else's artifact costs a round trip to install, against
	// minutes to build.
	if installed, err := d.FetchIndexFromPeers(ctx, kbID, versionID); err == nil && installed {
		return nil
	}
	if err := d.indexMgr.TriggerBuild(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("plane: ensureOwnIndex(%s, %d): schedule the build: %w", kbID, versionID, err)
	}
	return nil
}

// holdsVersionLocally reports whether this node holds the version's data —
// judged only by facts about this node and the version's own metadata — and
// says why, so the startup log shows an operator which version broke the chain.
//
// Deprecated: this is the FALLBACK, reached only for a knowledge base that has
// no persisted cursor record — an older WAL, or one this node has not advanced
// since the record type existed (docs/cursor-persistence-plan.md §3.5). Every
// knowledge base with a record is answered by that record instead, which is the
// only evidence here that is not a cache: both branches below bottom out in an
// index artifact, which the retention policy is free to delete.
//
// The order matters, and the artifact is consulted first: of the three facts it
// is the only one backed by a completed build, so no inference is involved. The
// two document-set facts follow, because a version with no document set has no
// artifact either — the index manager answers "version has no chunks; nothing to
// build" for it — and a disk-only order would report a gap where there is none
// and stop the recovery at the very first version of the knowledge base.
//
// An absent artifact is NOT evidence of absence for a version that HAS a
// document set: §8.6(b) builds lazily, so such a version may legitimately have
// no local artifact while its records sit in the stores. That case is reported
// as "not held" — conservative on purpose, since claiming a version whose
// records did not land would make this node serve an incomplete result instead
// of refusing. The cost is a cursor that stays low (and a replica the station
// keeps off) rather than a wrong answer.
func (d *LocalDataPlane) holdsVersionLocally(ctx context.Context, kbID string, v types.VersionMeta) (string, bool) {
	if d.hasLocalArtifact(ctx, kbID, v.VersionID) {
		return "local artifact", true
	}
	if v.DocIDSetHash == emptyDocIDSetHash {
		// The empty set IS this version's content: there is nothing on disk to look for
		// and nothing to fetch from anyone. This is what keeps an empty knowledge base
		// servable, and it is the ONLY thing that may claim a version this node holds no
		// artifact for.
		//
		// It used to be a second branch as well — "durable DATA with no committed
		// digest" — and that branch is gone because it could not tell two states apart.
		// A version carrying real documents whose digest was never committed has
		// exactly that pair, since a cursor promotion (§7.9) settles the data side
		// without a digest. Reading it as "I hold this" is what let a replica that had
		// missed ONE version report the whole chain as its cursor and then skip the fetch
		// it needed (measured: cursor recovery returned v6 for a node holding only v5, so
		// EnsureIndex concluded there was nothing to do — and the node stayed without
		// the data while claiming to have it).
		//
		// The control layer now records the empty set's digest when it creates a version
		// with no document changes, so "no documents" travels as a value instead of
		// being inferred — see types.EmptyDocIDSetHash and the doc_id_set_hash field.
		return "empty document set", true
	}
	return "no local artifact", false
}

// hasLocalArtifact reports whether this node holds the version's artifact on
// disk. A missing index store means "no".
//
// Deprecated: only the holdsVersionLocally fallback calls this. An artifact is a
// cache, not a source of truth about what this node holds — the persisted cursor
// record is (docs/cursor-persistence-plan.md §4.1).
func (d *LocalDataPlane) hasLocalArtifact(ctx context.Context, kbID string, versionID int64) bool {
	if d.indexMgr == nil {
		return false
	}
	exists, err := d.indexMgr.IndexExists(ctx, kbID, versionID)
	if err != nil {
		d.logger.Warn("plane: cursor recovery: IndexExists failed",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return false
	}
	return exists
}

// ReconcileIndexes is the storage layer's half of the startup reconcile — the
// "block report" of control-data-separation-design.md §5.3. It walks the
// versions the control layer knows about, compares them against the indexes
// actually present on this node's disk, (re)schedules builds for the ones
// missing inside the retention window, and returns the versions whose index is
// durable. The caller hands that set to ControlPlane.ReportEpoch, which is
// what promotes a lost READY proposal.
//
// Storage-layer policy (the retention window, rebuild-on-demand) stays in
// here; every control-layer state change stays on the other side of the
// contract.
func (d *LocalDataPlane) ReconcileIndexes(ctx context.Context, meta MetadataLister, retentionCount int) ([]VersionRef, error) {
	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		return nil, fmt.Errorf("plane: ReconcileIndexes: list knowledge bases: %w", err)
	}

	var durable []VersionRef
	for _, kb := range kbs {
		versions, err := meta.ListVersions(ctx, kb.KBID)
		if err != nil {
			d.logger.Warn("plane: reconcile: ListVersions failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
			continue
		}

		// Which versions actually have an artifact on disk? The retention window is
		// defined over on-disk artifacts — EnforceDiskRetention counts files —
		// while the control layer's version set also holds plenty that never
		// produced one here (PENDING, FAILED, versions built elsewhere). Deriving
		// the window from the version set instead would call versions
		// "retention-dropped" that the policy never even saw.
		present := make(map[int64]bool, len(versions))
		unknown := make(map[int64]bool)
		var onDisk []int64
		for _, v := range versions {
			// A version under deletion is not a candidate for anything here: the deletion
			// flow owns its bytes right now (usually having dropped the artifact already),
			// so counting it as durable would report READY for a version that is about to
			// stop existing, and scheduling a build below would race the reclaim for the
			// same files. The metadata is what names the work, so leaving it out costs
			// nothing: the deletion removes the metadata.
			//
			// Skipping it is only safe because the version is unqueryable while Deleting
			// (visibility is the control layer's judgement), so no reader is waiting on an
			// artifact this reconcile declined to build.
			if v.Deleting {
				continue
			}
			// FAILED is retryable — RebuildIndex may trigger another attempt — but
			// FAILED_PERMANENT is the control layer's terminal verdict (§10.1):
			// nothing re-triggers it, and its data may already have been reclaimed
			// (§10.6). Rebuilding here would contradict the recorded verdict and,
			// with the data gone, fail on every sweep.
			//
			// The DATA side's verdict counts too: data that will never arrive
			// cannot produce an index, so scheduling a build for it would fail on
			// every sweep for the same reason (§10.1b).
			if v.IndexStatus.IsFailed() ||
				v.DataStatus == types.DataStatusFailedPermanent {
				continue
			}
			exists, err := d.indexMgr.IndexExists(ctx, kb.KBID, v.VersionID)
			if err != nil {
				d.logger.Warn("plane: reconcile: IndexExists failed",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID), zap.Error(err))
				unknown[v.VersionID] = true
				continue
			}
			if exists {
				present[v.VersionID] = true
				onDisk = append(onDisk, v.VersionID)
			}
		}

		// retentionCutoff is the smallest version ID the on-disk retention policy
		// would keep. Below it, an absent artifact was dropped by the policy rather
		// than lost — the very set EnforceDiskRetention works with.
		retentionCutoff := retentionCutoffOf(onDisk, retentionCount)

		for _, v := range versions {
			// Same skip as the presence scan above: a version under deletion is not a
			// rebuild candidate, and its metadata (which the deletion removes) is what names
			// the work. Without this, a restart during a deletion gives the version a fresh
			// build that competes with the reclaim for the same artifact.
			if v.Deleting {
				continue
			}
			if v.IndexStatus.IsFailed() ||
				v.DataStatus == types.DataStatusFailedPermanent {
				continue
			}
			if unknown[v.VersionID] {
				// IndexExists failed for this version; anything said about it
				// would be a guess.
				continue
			}
			switch {
			case present[v.VersionID]:
				// The index is on disk: the version is durable, and the
				// control layer promotes a lost READY proposal from this
				// report.
				durable = append(durable, VersionRef{KBID: kb.KBID, VersionID: v.VersionID})
			case retentionCount > 0 && v.VersionID < retentionCutoff:
				// Intentionally dropped by the disk retention policy
				// (PENDING or READY, outside the newest retentionCount
				// versions): leave it absent and rebuild on demand.
				d.logger.Info("plane: reconcile: skipping rebuild of retention-dropped index",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID))
			case v.IndexStatus == types.IndexStatusPending || v.VersionID == kb.ActiveVersionID:
				// Worth a head start, and only these two:
				//
				//   PENDING — the writer is waiting for this very build; without it
				//   the write stalls instead of merely being slower.
				//
				//   the active version — every query for this knowledge base lands on
				//   it, so its build is needed immediately.
				//
				// TriggerBuild is idempotent, and the build re-persists and
				// re-reports the status.
				d.logger.Info("plane: reconcile: (re)building missing index",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID),
					zap.String("status", v.IndexStatus.String()))
				if err := d.indexMgr.TriggerBuildBackfill(ctx, kb.KBID, v.VersionID); err != nil {
					d.logger.Warn("plane: reconcile: TriggerBuildBackfill failed",
						zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID), zap.Error(err))
				}
			default:
				// READY, artifact missing, inside the window, and NOT the active
				// version: leave it absent.
				//
				// The retention branch above gives the same answer for the same
				// reason — EnsureIndex builds on demand, so an absent artifact costs a
				// slow first query and nothing else. Rebuilding it here instead is how
				// a restart over a populated volume turns into a rebuild storm: one
				// eager build per historical version, every one of them competing with
				// the writes that actually need the CPU, disk and vecstore. Measured: a
				// fresh write waited 601s for READY while 41 historical artifacts were
				// rebuilt ahead of it.
				d.logger.Info("plane: reconcile: leaving a non-active missing index absent",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID),
					zap.String("status", v.IndexStatus.String()))
			}
		}
	}
	return durable, nil
}

// retentionCutoffOf returns the oldest version ID the on-disk retention policy
// would keep, given the version IDs whose artifacts are actually on disk: the
// smallest of the newest retentionCount. It returns -1 when the policy is off or
// when nothing would be dropped, which reads as "no absence can be explained by
// retention".
//
// It counts files, not versions, deliberately: EnforceDiskRetention works on the
// `.index` files it finds, so a version that never produced one here is not part
// of its window either.
func retentionCutoffOf(onDisk []int64, retentionCount int) int64 {
	if retentionCount <= 0 || len(onDisk) <= retentionCount {
		return -1
	}
	ids := append([]int64(nil), onDisk...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids[len(ids)-retentionCount]
}

// FetchIndexFromPeers asks the other replicas whether any of them holds
// versionID's artifact, and installs the first one that answers.
//
// Under §8.4 owner sharding a version's index lives on ONE node, so every other
// replica that needs it has to get it from there — and asking is the only way,
// because distribution is pull-only (the push half was removed; see
// sync.IndexFetcher for why a fire-and-forget ship was the wrong mechanism).
// This is the ask.
//
// Returns installed=false (with a nil error) when nothing needed doing or nobody
// had it — "no peer holds this" is an ordinary answer, not a failure, and the
// caller falls through to building. Errors are reserved for transport trouble
// worth a log line, and even then the caller may still build.
func (d *LocalDataPlane) FetchIndexFromPeers(ctx context.Context, kbID string, versionID int64) (bool, error) {
	if d.indexFetcher == nil || d.indexInstaller == nil || d.resolveReplicas == nil {
		return false, nil // nothing to fetch with, or nobody to fetch from
	}

	// One fetch per version at a time, shared with everyone who asks meanwhile (H9 of
	// docs/code-review-2026-10-18.md). Without this, N concurrent asks produced N
	// downloads of the same artifact and N serialised rewrites of the same file.
	key := fetchInflightKey{kbID: kbID, versionID: versionID}
	d.indexFetchMu.Lock()
	if call, ok := d.indexFetchInflight[key]; ok {
		d.indexFetchMu.Unlock()
		select {
		case <-call.done:
			return call.installed, call.err
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	if d.indexFetchInflight == nil {
		d.indexFetchInflight = make(map[fetchInflightKey]*indexFetchCall)
	}
	call := &indexFetchCall{done: make(chan struct{})}
	d.indexFetchInflight[key] = call
	d.indexFetchMu.Unlock()

	call.installed, call.err = d.fetchIndexFromPeers(ctx, kbID, versionID)

	d.indexFetchMu.Lock()
	delete(d.indexFetchInflight, key)
	d.indexFetchMu.Unlock()
	close(call.done)
	return call.installed, call.err
}

// fetchIndexFromPeers is the unshared body of FetchIndexFromPeers.
func (d *LocalDataPlane) fetchIndexFromPeers(ctx context.Context, kbID string, versionID int64) (bool, error) {
	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		return false, fmt.Errorf("plane: FetchIndexFromPeers: resolve replicas: %w", err)
	}

	var lastErr error
	for _, peer := range peers {
		if peer == "" {
			continue
		}
		fetchCtx, cancel := context.WithTimeout(ctx, d.effectivePullIdleTimeout())
		indexData, sidecarData, fetchErr := d.indexFetcher.FetchIndex(fetchCtx, peer, kbID, versionID)
		cancel()
		if fetchErr != nil {
			if errors.Is(fetchErr, stratinternalsync.ErrIndexNotExported) {
				// This peer does not have it. Ordinary; try the next.
				continue
			}
			lastErr = fetchErr
			d.logger.Warn("plane: fetching an index artifact from a peer failed",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.String("peer", peer), zap.Error(fetchErr))
			continue
		}
		if err := d.indexInstaller.InstallIndex(ctx, kbID, versionID, indexData, sidecarData); err != nil {
			lastErr = err
			d.logger.Warn("plane: installing a fetched index artifact failed",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.String("peer", peer), zap.Error(err))
			continue
		}
		d.logger.Info("plane: fetched an index artifact from a peer instead of building",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.String("peer", peer))
		return true, nil
	}
	return false, lastErr
}
