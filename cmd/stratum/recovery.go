package main

import (
	"context"
	"time"

	"go.uber.org/zap"

	"stratum/internal/coordinator"
	"stratum/internal/nodestate"
	"stratum/internal/plane"
	"stratum/internal/raft"
	stratumsync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"
)

// maxConcurrentVersionFetches bounds the on-apply version transfers started by
// SetOnVersionCreated. See the semaphore's comment in the assembly below.
const maxConcurrentVersionFetches = 4

// Crash-recovery replay policy. A PendingRecord that cannot be replayed
// must not abort startup: the failure modes it covers (dependency down,
// KB/version not yet raft-committed on a restarting follower, KB deleted
// meanwhile) are all authoritatively restored by Raft log replay +
// DataSync, and a fatal error here wedges the node in a crash loop —
// it can never get far enough to sync. Each record therefore gets a
// bounded in-process retry window (covering a transient vecstore/embed
// outage at boot), then is skipped and surfaced via the replay counter
// (GetSystemStatus). The record stays in the WAL and is retried on the
// next restart.
// replayRetry is the crash-replay budget (recovery.crash_replay_*): how many
// in-process attempts per record, and the base of the exponential backoff
// between them.
type replayRetry struct {
	MaxAttempts int
	BaseMS      int
}

const (
	// crashReplayMaxAttempts bounds in-process replay retries per record.
	crashReplayMaxAttempts = 3
	// crashReplayRetryBaseMillis is the initial backoff between attempts
	// (doubled each retry: 100ms, 200ms — the whole window is <1s).
	crashReplayRetryBaseMillis = 100
)

// replayCoordinatorCall runs fn with bounded in-process retries and
// exponential backoff. On the first success it returns nil; if every
// attempt fails it returns the last error (the caller then skips the
// record — see runCrashRecovery).
func replayCoordinatorCall(ctx context.Context, r replayRetry, fn func() error) error {
	var err error
	for attempt := 1; attempt <= r.MaxAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt < r.MaxAttempts {
			select {
			case <-time.After(time.Duration(r.BaseMS<<(attempt-1)) * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return err
}

// runCrashRecovery replays every WAL PendingRecord through the
// coordinator layer after startup. See the WAL package doc comment for
// the record semantics. Records that cannot be replayed — after the
// bounded in-process retry window — are skipped: the replay counter is
// bumped (visible via GetSystemStatus), the record stays in the WAL for
// the next restart, and startup continues. Startup is never aborted by a
// replay failure; data convergence is delegated to Raft log replay +
// DataSync.
func runCrashRecovery(
	ctx context.Context,
	logger *zap.Logger,
	records []types.PendingRecord,
	wc coordinator.WriteCoordinator,
	dc coordinator.DeleteCoordinator,
	dvc coordinator.DeleteVersionCoordinator,
	w wal.WAL,
	retry replayRetry,
) {
	for _, rec := range records {
		switch rec.Type {
		case types.PendingRecordTypeDeleteMark:
			logger.Info("crash recovery: resuming interrupted knowledge-base deletion",
				zap.String("kb_id", rec.KBID))
			if err := replayCoordinatorCall(ctx, retry, func() error { return dc.Execute(ctx, rec.KBID) }); err != nil {
				logger.Warn("crash recovery: skipping KB-deletion resume after retries exhausted",
					zap.String("kb_id", rec.KBID), zap.Error(err))
				w.IncrementReplayCounter(rec)
				continue
			}

		case types.PendingRecordTypeVersionDelete:
			logger.Info("crash recovery: resuming interrupted version deletion",
				zap.String("kb_id", rec.KBID), zap.Int64("version_id", rec.VersionID))
			// Two calls, on purpose, and the order matters:
			//
			//   - ResumeVersionDelete finishes THIS record. It does not discover its work
			//     from the metadata, which is what makes it the only path that can pay a
			//     debt whose version was already removed before the crash (Execute lists
			//     Deleting versions, so for that case it finds nothing and the record is
			//     replayed on every restart, forever). It also writes the tombstone first.
			//   - Execute still covers the SIBLINGS: versions marked Deleting whose own
			//     record was never written (the crash landed between the mark and
			//     deleteOne's WriteVersionDeleteMark). Those are not this record's debt,
			//     so a failure there is reported without counting this record as unpaid.
			if err := replayCoordinatorCall(ctx, retry, func() error {
				return dvc.ResumeVersionDelete(ctx, rec.KBID, rec.VersionID)
			}); err != nil {
				logger.Warn("crash recovery: skipping version-deletion resume after retries exhausted",
					zap.String("kb_id", rec.KBID), zap.Int64("version_id", rec.VersionID), zap.Error(err))
				w.IncrementReplayCounter(rec)
				continue
			}
			if err := replayCoordinatorCall(ctx, retry, func() error { return dvc.Execute(ctx, rec.KBID) }); err != nil {
				logger.Warn("crash recovery: the sibling sweep after a version-deletion resume did not finish",
					zap.String("kb_id", rec.KBID), zap.Error(err))
			}

		case types.PendingRecordTypeVersionWrite:
			if len(rec.Changes) == 0 {
				// No local transaction input: the VERSION_ID was applied
				// by a node that never ran this Execute locally (a
				// follower applying the leader's log), or was written by
				// an older WAL format. It cannot be replayed here — the
				// node's data for it is restored by Raft log replay +
				// DataSync instead. Surface it so operators are aware.
				logger.Warn("crash recovery: skipping version-write replay without local transaction input (follower-applied or legacy WAL record)",
					zap.Int64("version_id", rec.VersionID))
				w.IncrementReplayCounter(rec)
				continue
			}
			logger.Info("crash recovery: replaying interrupted version write",
				zap.String("kb_id", rec.KBID), zap.Int64("version_id", rec.VersionID))
			err := replayCoordinatorCall(ctx, retry, func() error {
				return wc.ReplayVersionStorageWrites(ctx, rec.KBID, rec.ParentVersionID, rec.VersionID, rec.Changes)
			})
			if err != nil {
				logger.Warn("crash recovery: skipping version-write replay after retries exhausted (restored by Raft log replay + DataSync)",
					zap.String("kb_id", rec.KBID), zap.Int64("version_id", rec.VersionID), zap.Error(err))
				w.IncrementReplayCounter(rec)
				continue
			}
		}
	}
}

// reconcileIndexStatus drives the startup reconcile across the contract: the
// storage layer decides what is durable on disk and what must be rebuilt
// (DataPlane.ReconcileIndexes, which owns the retention policy), and that
// durable set is reported up so the control layer promotes versions whose
// READY proposal was lost (ControlPlane.ReportEpoch). Before stage ① this
// function walked local disks and proposed status itself — see
// docs/已实施/control-data-separation-design.md §5.3/§7 and the decision table at the
// call site.
// projectNodeState rebuilds the version layer from disk facts at startup
// (docs/已实施/storage-node-state-machine-plan.md §5.1).
//
// Both inputs are facts this process has already established by the time it is
// called: the reconciled durable set (an artifact is on disk) and the recovered
// data cursors (everything at or below one is held). Nothing here probes the
// disk again, which is the point — the machine is a projection of work already
// done, not a second scan.
//
// Artifacts go in FIRST. An artifact is the stronger fact, and the cursor pass
// covers a superset of versions, so doing it the other way round would leave
// every built version looking like data-with-no-index.
//
// A version with neither is left with no entry at all, rather than recorded as
// absent: absence is the absence of a fact, and the machine reports it as
// ABSENT without needing one. That also keeps the entry count proportional to
// what this node actually holds.
func projectNodeState(
	ctx context.Context,
	logger *zap.Logger,
	m *nodestate.Machine,
	dp *plane.LocalDataPlane,
	meta plane.MetadataLister,
	durable []plane.VersionRef,
) {
	if m == nil {
		return
	}

	// Tracked separately from the machine so the cursor pass can skip these
	// without a read-back and a lock per version.
	ready := make(map[string]map[int64]struct{}, len(durable))
	for _, ref := range durable {
		if ready[ref.KBID] == nil {
			ready[ref.KBID] = make(map[int64]struct{})
		}
		ready[ref.KBID][ref.VersionID] = struct{}{}
		if err := m.RestoreVersion(ref.KBID, ref.VersionID, nodestate.VersionReady, ""); err != nil {
			logger.Warn("nodestate: projecting a durable index failed",
				zap.String("kb_id", ref.KBID), zap.Int64("version_id", ref.VersionID), zap.Error(err))
		}
	}

	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		// Partial is worth saying out loud: the machine now under-reports what
		// this node holds, and a reader would see ABSENT for versions that are
		// on disk.
		logger.Warn("nodestate: version layer projection is partial: listing knowledge bases failed", zap.Error(err))
		return
	}

	var held int
	for _, kb := range kbs {
		cursor := dp.LocalVersionOf(kb.KBID)
		for v := int64(1); v <= cursor; v++ {
			if _, isReady := ready[kb.KBID][v]; isReady {
				continue
			}
			if err := m.RestoreVersion(kb.KBID, v, nodestate.VersionDataPresent, ""); err != nil {
				logger.Warn("nodestate: projecting a held version failed",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v), zap.Error(err))
				continue
			}
			held++
		}
	}

	logger.Info("nodestate: version layer projected from disk facts",
		zap.Int("knowledge_bases", len(kbs)), zap.Int("ready", len(durable)), zap.Int("data_present", held))
}

func reconcileIndexStatus(ctx context.Context, logger *zap.Logger, dp *plane.LocalDataPlane, cp plane.ControlPlane, meta plane.MetadataLister, retentionCount int) []plane.VersionRef {
	// §7.8: the contiguous cursor lives in memory, so a restarted node would
	// answer "0" for every knowledge base — data complete, index on disk, and
	// every query refused by the station's freshness check (§9.3(2)) until a
	// later write happens to advance the cursor. Rebuild it from this node's own
	// facts before anything reads it. (Measured: TestT4_QueryLatency failed with
	// exactly that after restarting one storage replica.)
	if err := dp.RecoverLocalCursors(ctx, meta); err != nil {
		logger.Warn("index reconcile: cursor recovery failed", zap.Error(err))
	}

	durable, err := dp.ReconcileIndexes(ctx, meta, retentionCount)
	if err != nil {
		logger.Warn("index reconcile: storage-layer reconcile failed", zap.Error(err))
		return nil
	}

	// The §7.9 epoch payload is deliberately NOT published here. Its index side is
	// local, but its data side is a quorum claim, and at this point in startup no
	// storage node is serving yet — see reportEpoch. What this function can
	// establish on its own is the durable set, and the caller hands it over once
	// the node is up.
	return durable
}

// reportEpoch publishes the §7.9 payload (one data cursor per knowledge base plus
// the explicit index-ready set) and returns how many knowledge bases it had to
// leave out because no quorum could be established.
//
// The two sides have different needs, which is why the payload goes out after the
// node is serving rather than during reconcile:
//
//   - the INDEX side is local — the durable set ReconcileIndexes just produced.
//   - the DATA side is a quorum claim (SafeDurableVersion). During startup every
//     storage node is still inside its own reconcile, and none of them reaches
//     grpcServer.Serve until that returns, so each one sees only itself, the claim
//     fails for every knowledge base at once, and the whole data side is dropped.
//     Measured before this change: 15 "no quorum for a durable claim" lines over
//     56 seconds, with "Stratum gRPC server listening" landing 0.4 ms after the
//     last one. Every version then sat at DATA_STATUS_PENDING, which in turn made
//     cursor recovery overclaim a node's position — see
//     docs/已实施/active-lag-detection-design.md.
func reportEpoch(ctx context.Context, logger *zap.Logger, dp *plane.LocalDataPlane, cp plane.ControlPlane, durable []plane.VersionRef) int {
	// §7.9: the payload keeps the two sides separate — a scalar cursor per KB for
	// the data side (linearization makes a scalar sufficient), an explicit version
	// set for the index side (readiness cannot be collapsed).
	//
	// One cursor query per knowledge base, not per version.
	cursors := make(map[string]int64)
	indexReady := make(map[string][]int64)
	skipKB := make(map[string]bool)
	queried := make(map[string]bool)
	for _, ref := range durable {
		if !queried[ref.KBID] {
			queried[ref.KBID] = true
			safe, ok, err := dp.SafeDurableVersion(ctx, ref.KBID)
			switch {
			case err != nil:
				// No quorum means no safe claim. Reporting the local view anyway is
				// precisely the "control layer runs ahead of the data" failure the
				// epoch exists to prevent.
				logger.Warn("index reconcile: no quorum for a durable claim; leaving this knowledge base out of the payload",
					zap.String("kb_id", ref.KBID), zap.Error(err))
				skipKB[ref.KBID] = true
			case ok:
				cursors[ref.KBID] = safe
			}
		}
		if skipKB[ref.KBID] {
			continue
		}
		if limit, hasLimit := cursors[ref.KBID]; hasLimit && ref.VersionID > limit {
			continue
		}
		indexReady[ref.KBID] = append(indexReady[ref.KBID], ref.VersionID)
	}

	// epoch 0: stage ① runs in-process, so there is no stale-report window to
	// close yet (the storage cluster keeps its own manifest in stage ④).
	if err := cp.ReportEpoch(ctx, 0, cursors, indexReady); err != nil {
		logger.Warn("index reconcile: ReportEpoch failed", zap.Error(err))
	}
	// Report what went out. Without this line the only thing visible is the
	// absence of a warning, which cannot be told apart from "never ran" — and the
	// data side of this payload is what moves versions to DATA_DURABLE
	// (promoteDurableData), so an empty cursor set is a silent outage.
	logger.Info("index reconcile: epoch payload published",
		zap.Int("knowledge_bases", len(indexReady)),
		zap.Int("cursors", len(cursors)),
		zap.Int("skipped_no_quorum", len(skipKB)))
	return len(skipKB)
}

const (
	// epochReportRetryInterval is the DEFAULT wait between attempts to publish a
	// payload that quorum was not ready for yet (recovery.epoch_report_retry_interval_ms).
	epochReportRetryInterval = 3 * time.Second
	// epochReportRetryWindow is the DEFAULT bound on the retrying
	// (recovery.epoch_report_retry_window_ms). Peers come up within seconds of
	// each other; a peer that is still not serving after this long is a different
	// problem, and the log line below is the evidence for it.
	epochReportRetryWindow = 60 * time.Second
)

// reportEpochWhenPeersAreUp publishes the payload once quorum can actually be
// established, retrying the knowledge bases that had none.
//
// Retrying is the point: "this node is serving" does not imply "its peers are" —
// the nodes still start concurrently, so the first one to get here can find every
// peer inside its own reconcile. Giving up quietly would leave the data side
// unpublished for the whole life of the process, which is exactly the failure this
// replaces.
func reportEpochWhenPeersAreUp(ctx context.Context, logger *zap.Logger, dp *plane.LocalDataPlane, cp plane.ControlPlane, durable []plane.VersionRef, retryInterval, retryWindow time.Duration) {
	deadline := time.Now().Add(retryWindow)
	for {
		skipped := reportEpoch(ctx, logger, dp, cp, durable)
		if skipped == 0 {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if time.Now().After(deadline) {
			logger.Warn("index reconcile: epoch payload stayed incomplete; the data side is unpublished for these knowledge bases",
				zap.Int("knowledge_bases", skipped))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryInterval):
		}
	}
}

// verifyVersionPull reports whether this node's local stores hold the
// complete data for (kbID, versionID): the locally computed document-ID
// set digest must match the digest the leader committed into the version
// metadata (see stratumsync.VerifyDocIDSet). If the leader has not committed a
// digest yet (initial/empty version or a missed propose), a pull that
// produced data is accepted.
func verifyVersionPull(ctx context.Context, rn raft.MetaReader, vd versiondoc.VersionDocList, kbID string, versionID int64) bool {
	var metaHash string
	if versions, err := rn.ListVersions(ctx, kbID); err == nil {
		for _, v := range versions {
			if v.VersionID == versionID {
				metaHash = v.DocIDSetHash
				break
			}
		}
	}

	ok, _, err := stratumsync.VerifyDocIDSet(ctx, vd, kbID, versionID, metaHash)
	if err != nil {
		return false
	}
	if ok {
		return true
	}
	if metaHash == "" {
		docs, err := vd.ListDocIDs(ctx, kbID, versionID)
		return err == nil && len(docs) > 0
	}
	return false
}

// totalVersionIDs counts the version ids across a leftovers map, for a log line
// (the reconciler's maps are per knowledge base).
func totalVersionIDs(leftovers map[string][]int64) int {
	n := 0
	for _, ids := range leftovers {
		n += len(ids)
	}
	return n
}

// recoverStorageStartup runs everything a storage-holding restart has to do
// before the node may answer: replay the interrupted WAL transactions, trim
// and reconcile indexes against disk facts, project the node state machine,
// queue terminal-version reclaims, and run startup/periodic deletion
// reconciliation.
//
// It returns the epoch-durable set: publishing it has to wait until every node
// is serving (see reportEpochWhenPeersAreUp), which is after Serve.
func recoverStorageStartup(
	ctx context.Context,
	cfg appConfig,
	logger *zap.Logger,
	records []types.PendingRecord,
	rn raft.RaftNode,
	controlPlane *plane.LocalControlPlane,
	nodeState *nodestate.Machine,
	walImpl *wal.FileWAL,
	writeCoord *coordinator.WriteCoordinatorImpl,
	deleteCoord *coordinator.DeleteCoordinatorImpl,
	deleteVersionCoord *coordinator.DeleteVersionCoordinatorImpl,
	dataPlane *plane.LocalDataPlane,
	liveness plane.VersionLivenessLister,
) []plane.VersionRef {
	// Replay interrupted WAL transactions. Both halves reach for state wired
	// onto the coordinators/data plane above (WriteCoordinator.SetDataPlane and
	// deferred droppers).
	runCrashRecovery(ctx, logger, records, writeCoord, deleteCoord, deleteVersionCoord, walImpl,
		replayRetry{MaxAttempts: cfg.CrashReplayMaxAttempts, BaseMS: cfg.CrashReplayRetryBaseMS})

	// Enforce the disk retention policy BEFORE reconciling indexes so the
	// reconciler sees post-retention disk facts (active versions are shielded).
	if err := dataPlane.EnforceRetention(ctx, rn); err != nil {
		logger.Warn("index retention: ListKnowledgeBases failed", zap.Error(err))
	}
	epochDurable := reconcileIndexStatus(ctx, logger, dataPlane, controlPlane, rn, cfg.IndexRetentionCount)

	// Project reconciled disk facts and recovered cursors into nodeState once
	// before serving.
	projectNodeState(ctx, logger, nodeState, dataPlane, rn, epochDurable)

	// Rebuild the in-memory reclaim queue for versions that already reached a
	// terminal verdict before restart.
	if n, err := dataPlane.ReclaimTerminalVersions(ctx, rn); err != nil {
		logger.Warn("startup: could not rebuild the terminal-version reclaim list", zap.Error(err))
	} else if n > 0 {
		logger.Info("startup: queued terminal versions for local reclaim", zap.Int("versions", n))
	}

	// Deletion reconciliation (§B/§C), once at boot, plus periodic incremental
	// sweep when configured.
	if liveness != nil {
		if cfg.ReconcileDeletedVersions {
			res, err := dataPlane.ReconcileDeletionsAtStartup(ctx, rn)
			if err != nil {
				logger.Warn("startup: the deletion reconciliation did not finish; the periodic sweep retries it", zap.Error(err))
			}
			if res.Reclaimed > 0 {
				logger.Info("startup: reclaimed local leftovers of deleted versions",
					zap.Int("versions", res.Reclaimed), zap.String("source", res.Source))
			} else {
				logger.Info("startup: deletion reconciliation found nothing to reclaim",
					zap.String("source", res.Source), zap.String("fallback", res.Fallback),
					zap.Int64("watermark", res.LocalSeq))
			}
		} else if leftovers, err := dataPlane.DeletedVersionLeftovers(ctx, rn); err != nil {
			logger.Warn("startup: could not scan for local leftovers of deleted versions", zap.Error(err))
		} else if n := totalVersionIDs(leftovers); n > 0 {
			logger.Info("startup: local leftovers of deleted versions found; set reconcile.deleted_versions to reclaim them",
				zap.Int("versions", n))
		}

		if cfg.ReconcileDeletionsIncremental || cfg.ReconcileDeletedVersionsPeriodic {
			dataPlane.StartDeletedVersionReconcile(ctx, rn,
				time.Duration(cfg.ReconcileDeletionsIncrementalIntervalS)*time.Second)
		}
	}

	dataPlane.StartTerminalReclaims(ctx)
	return epochDurable
}
