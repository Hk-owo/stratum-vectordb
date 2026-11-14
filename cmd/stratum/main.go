// Stratum is a distributed vector-search knowledge base engine: Raft-
// consensus-backed server that manages versioned document collections,
// splits documents into chunks, embeds them into vectors, indexes them
// with HNSW (Faiss), and serves similarity queries.
//
// See doc/Stratum_设计文档v10.md for the full architecture.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"

	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/coordinator"
	"stratum/internal/docstore"
	"stratum/internal/embed"
	"stratum/internal/index"
	"stratum/internal/nodestate"
	"stratum/internal/plane"
	"stratum/internal/raft"
	"stratum/internal/splitter"
	stratumsync "stratum/internal/sync"
	"stratum/internal/tombstone"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"
	"stratum/service"
)

// loggerAtLevel returns base rebuilt at the configured level (zap's names:
// debug/info/warn/error).
//
// Empty means "keep base". An unknown level also keeps base and says so: a typo
// in the config must not take the node down, and this field's whole purpose is
// observability. Before this existed the setting was read by nobody — the node
// built an info-level logger before the config file was even parsed — so
// `logging.level: debug` silently had no effect and no debug line was reachable
// (that is how the per-stage query timings stayed invisible, v13 §5 #18).
func loggerAtLevel(base *zap.Logger, level string) *zap.Logger {
	if level == "" {
		return base
	}
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		base.Warn("unknown logging level; keeping the current one",
			zap.String("level", level), zap.Error(err))
		return base
	}
	pc := zap.NewProductionConfig()
	pc.Level = zap.NewAtomicLevelAt(lvl)
	rebuilt, err := pc.Build()
	if err != nil {
		base.Warn("could not rebuild the logger at the configured level",
			zap.String("level", level), zap.Error(err))
		return base
	}
	return rebuilt
}

// maxConcurrentVersionFetches bounds the on-apply version transfers started by
// SetOnVersionCreated. See the semaphore's comment in the assembly below.
const maxConcurrentVersionFetches = 4

func main() {
	b := bootstrapNode()
	defer b.cleanup()
	defer func() { _ = b.logger.Sync() }()

	cfg, logger := b.cfg, b.logger
	walImpl, records := b.walImpl, b.records
	rn, raftNode, mark := b.rn, b.raftNode, b.mark
	dataDir := b.dataDir

	// --- Storage layer (local) ---
	//
	// A control node keeps no data storage: it owns replicated metadata only
	// (Stratum_设计文档v13.md §7.0), and every path that used to reach into a
	// local store now goes through the storage contract instead. What it keeps
	// is the WAL and data directory above — Raft's own log and snapshots, which
	// are what make this node a member rather than a holder of data.
	//
	// The bindings below are nil on a control node. That is deliberate and not
	// a latent panic: the code paths that would dereference them are the ones
	// the role split removed.
	storageLocal := cfg.Role != NodeRoleControl
	// The data-plane helpers that actually touch stores. A control node builds
	// none of them: what it keeps is the broadcast side of the contract plus the
	// leader-side record of the storage nodes' heartbeats.
	var ds *docstore.PebbleDocStore
	var cdm *chunkdoc.PebbleChunkDocMapper
	var vd *versiondoc.PebbleVersionDocList
	var chunkStore *chunkstore.VecstoreChunkStore
	var chunkBloom *bloom.BitsAndBloomsFilter
	var vBloomStore *bloom.VersionBloomStore
	var chunkSplitter splitter.ChunkSplitter
	var indexMgr *index.IndexManagerImpl
	var embedClient *embed.HTTPEmbedClient
	// versionTombstones is this node's durable record of versions the control layer
	// told it are deleted. Nil on a control node (no storage, nothing to refuse).
	var versionTombstones *tombstone.PebbleStore

	// docs/storage-node-state-machine-plan.md §3: this node's own state, in one
	// place. Created only where there is storage — a control node keeps none, so
	// it has no phases to report, and its admin surface omits node_state rather
	// than claiming a phase it does not have.
	//
	// Created BEFORE the storage stack opens, deliberately: STARTING is a phase
	// a node really passes through (opening Pebble stores, dialling vecstore),
	// and constructing the machine past it would make it unreportable.
	var nodeState *nodestate.Machine
	if storageLocal {
		nodeState = nodestate.New(nodestate.Options{Logger: logger.Named("nodestate")})
	}

	ctx := context.Background()
	if storageLocal {
		// nodeState goes in here because the index manager owns the version layer's
		// build transitions (docs/storage-node-state-machine-plan.md §7, item 4):
		// every build and every installed artifact passes through it, and the one
		// build that never starts is only visible from inside.
		stack, err := buildStorageStack(cfg, dataDir, rn, logger, nodeState)
		if err != nil {
			logger.Fatal("failed to initialize the storage layer", zap.Error(err))
		}
		defer stack.Close()
		ds, cdm, vd, chunkStore = stack.DocStore, stack.ChunkDocMapper, stack.VersionDocList, stack.ChunkStore
		chunkBloom, vBloomStore = stack.ChunkBloom, stack.VersionBloom
		chunkSplitter, indexMgr, embedClient = stack.ChunkSplitter, stack.IndexManager, stack.EmbedClient
		versionTombstones = stack.Tombstones
		logger.Info("storage layer initialized", zap.String("role", string(cfg.Role)))

		// The stack is open, so startup reconciliation is what this node is
		// doing now: recovering cursors, reconciling indexes against disk
		// facts, publishing the epoch payload. The phase lasts until SERVING
		// below, and it is observable for its whole duration — which is the
		// point, since it is the window in which queries are answered from a
		// partially restored node.
		if err := nodeState.Transition(nodestate.NodeReconciling); err != nil {
			logger.Fatal("node state: entering RECONCILING", zap.Error(err))
		}
	} else {
		logger.Info("starting without local storage: metadata only (control role)",
			zap.String("role", string(cfg.Role)))
	}

	// controlPlane is the storage layer's only channel to replicated state
	// (control-data-separation-design.md §4.2); stage ① runs it in-process.
	//
	// The leader's record of the storage nodes' heartbeats, plus the gate that clears it
	// when this node takes over leadership. Clearing on takeover is the point: a new leader
	// must not judge redundancy from heartbeats its predecessor received. Both are created
	// here because the control plane is the record's first reader.
	livenessRegistry := plane.NewLivenessRegistry()
	livenessGate := plane.NewLeaderGate(rn.IsLeader, livenessRegistry.Reset)
	// requiredReplicaIDs is assigned from the topology further down.
	var requiredReplicaIDs func() ([]int64, error)
	controlPlane := plane.NewLocalControlPlane(rn,
		// The control plane's own advisory logging (the chain-tail signal it
		// hands back to nodes, and the reports it drops) goes to the node's
		// logger. Without this it stays on zap.NewNop() and every one of those
		// lines is discarded.
		plane.WithControlLogger(logger),
		// §10.1: how many failed attempts precede the FAILED_PERMANENT verdict.
		// Zero (the default) keeps plane.DefaultFailureBudget.
		plane.WithFailureBudget(cfg.ControlPlaneFailureBudget),
		// §8.6(d): index-readiness reports name the reporter, so the control
		// layer can tell "how many replicas are serving this version" from
		// "someone is" — the rolling cleanup asks that before taking one out of
		// service.
		plane.WithNodeID(cfg.NodeID),
		plane.WithLivenessView(livenessRegistry, livenessGate),
		// The replica set a write must reach, for the redundancy verdict. It comes from
		// the cluster topology — never from who has reported — so a node that is merely
		// silent cannot drop out of the requirement. Assigned further down (once the
		// topology is built) and read through this closure, so the assembly order does
		// not matter.
		plane.WithRequiredReplicas(func() ([]int64, error) {
			if requiredReplicaIDs == nil {
				return nil, errors.New("stratum: replica topology not assembled yet")
			}
			return requiredReplicaIDs()
		}),
		// §3.2: how long a required replica may go quiet before the redundancy
		// verdict stops counting it live. Zero keeps the plane's default, so an
		// unconfigured node behaves like every node did before this existed.
		plane.WithStorageSilenceWindow(b.storageSilence))
	// Build completion is reported through that contract rather than by
	// proposing metadata directly: the storage layer no longer reaches into
	// the Raft state machine itself.
	//
	// §8.6(d): the rolling collection asks the control layer how many OTHER
	// replicas are serving a version before it takes this node out of service.
	// Wired here because it needs both halves — the index manager (built with the
	// storage stack above) and the control plane (built just now). A control node
	// has no index manager, so there is nothing to wire there.
	if indexMgr != nil {
		indexMgr.SetGCReplicaCounter(controlPlane, cfg.NodeID)
	}
	// A control node has no index manager, so no builds to observe.
	if indexMgr != nil {
		indexMgr.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
			// NOTE: the node state machine's build transitions are NOT reported
			// here. They belong to the index manager itself (its BuildState
			// recorder, wired in buildStorageStack), which is the single entry
			// point every build passes through — including the one that never
			// starts because the pool is closed, which this callback would never
			// hear about. Reporting from both would be two sources for one fact,
			// which is what the state machine exists to end.
			//
			// Apply the on-disk retention policy after every successful build:
			// keep the newest cfg.IndexRetentionCount index files per KB, drop
			// older ones (rebuilt on demand via RebuildIndex). The active
			// version is shielded so a rolled-back active version's index is
			// never dropped. Best-effort: a policy failure is logged upstream
			// and does not fail the build.
			if status == types.IndexStatusReady {
				if kb, err := rn.GetKB(context.Background(), kbID); err == nil {
					_ = indexMgr.EnforceDiskRetention(context.Background(), kbID, []int64{kb.ActiveVersionID, versionID})
				}
			}
			return reportIndexStatus(context.Background(), controlPlane, kbID, versionID, status)
		})
	}

	// --- Coordinators ---
	// kbLocks hands out the per-knowledge-base write lock held for a whole
	// CreateVersion transaction (BEGIN through COMMIT). It is shared with the
	// orphan-chunk GC's reclaim phase so a sweep re-validates and deletes candidates
	// under mutual exclusion with writes to the SAME knowledge base, while writes to
	// other knowledge bases proceed (M7 of docs/code-review-2026-09-24.md). Both
	// sides MUST receive the same set.
	// dispatchVersionWrite and resolveReplicaAddrs are late-bound: the coordinator
	// needs a dispatcher, the dispatcher needs the data plane, and the data plane
	// needs the coordinator (§7.13.2). Routing them through these closures keeps
	// the assembly acyclic (the same late-binding trick dispatchVersionWrite and
	// resolveReplicaAddrs already use).
	a := &nodeAssembly{
		cfg: cfg, logger: logger, ctx: ctx,
		walImpl: walImpl, rn: rn, raftNode: raftNode,
		storageLocal: storageLocal, nodeState: nodeState, controlPlane: controlPlane,
		ds: ds, cdm: cdm, vd: vd, chunkStore: chunkStore,
		vBloomStore: vBloomStore, chunkBloom: chunkBloom,
		chunkSplitter: chunkSplitter, indexMgr: indexMgr,
		embedClient: embedClient, versionTombstones: versionTombstones,
	}
	a.wireCoordinators()

	// --- Index status reconcile (derive state from disk facts) ---
	// The authoritative fact for "this version's index is built and
	// durable" is the persisted index file on disk (written by Save after
	// every successful build). Reconcile every version against that fact
	// so IndexStatus converges without depending on a build-completion
	// callback having been delivered:
	//   - PENDING + index on disk → propose READY (the build finished; the
	//     callback was lost — the state is derived, not replayed).
	//   - PENDING + no index      → trigger the build (idempotent).
	//   - READY + no index        → the on-disk index was lost (e.g. the
	//     vecstore data directory was replaced); rebuild it — UNLESS the
	//     index was intentionally dropped by the disk retention policy
	//     (versions outside the newest gc.version_retention_count), in
	//     which case it stays absent until a query/RebuildIndex asks for
	//     it ("需要时重建").
	// FAILED versions are left alone (explicit RebuildIndex / deletion).
	//
	// Enforce the retention policy BEFORE reconciling, so the reconcile
	// sees the post-retention disk facts (the active version is protected
	// from dropping).

	// --- Orphan-chunk garbage collector (periodic safety net) ---
	// Built above; only the loop starts here, and only on a node that holds the
	// mappings. The delete flow's ReclaimVersionChunks does the real work.
	if storageLocal {
		go a.gcImpl.Run(ctx)
	}

	// --- Data sync (leader→follower) ---
	a.wireDataPlane()
	requiredReplicaIDs = a.topology.requiredReplicaIDs

	// --- WAL crash recovery ---
	epochDurable := a.recoverStartup(records)

	grpcServer, lis, err := startGRPCServer(grpcDeps{
		cfg: cfg, logger: logger, mark: mark,
		rn: rn, raftNode: raftNode, storageLocal: storageLocal,
		writeCoord: a.writeCoord, deleteCoord: a.deleteCoord, deleteVersionCoord: a.deleteVersionCoord,
		indexMgr: indexMgr, cdm: cdm, vd: vd, ds: ds, vBloomStore: vBloomStore,
		chunkStore: chunkStore, walImpl: walImpl, dataPlane: a.dataPlane,
		syncLeader: a.syncLeader, syncFollower: a.syncFollower,
		controlPlane: controlPlane, dataSources: a.dataSources,
		livenessRegistry: livenessRegistry, livenessGate: livenessGate,
		versionTombstones: versionTombstones, nodeState: nodeState,
		resolveReplicaAddrs: a.resolveReplicaAddrs,
	})
	if err != nil {
		logger.Fatal("failed to listen", zap.String("addr", cfg.GRPCAddr), zap.Error(err))
	}
	// --- Signal handling ---
	startShutdownWatcher(shutdownDeps{
		cfg: cfg, logger: logger, nodeState: nodeState, grpcServer: grpcServer,
		indexMgr: indexMgr, chunkStore: chunkStore,
	})

	chainTails := newLagCatchup(lagCatchupDeps{
		cfg: cfg, logger: logger, storageLocal: storageLocal,
		dataPlane: a.dataPlane, holdersCache: a.holdersCache,
		nodeState: nodeState, indexMgr: indexMgr,
	})

	// Tell the control leader this node is alive, every interval. The leader it reaches is
	// looked up per interval, never cached: caching would pin the heartbeat to a leadership
	// term that has ended. It is soft state, so a failed one is only logged.
	go stratumsync.NewHeartbeater(stratumsync.HeartbeaterConfig{
		NodeID: cfg.NodeID,
		// sync.heartbeat_interval_ms; zero takes the package default (5 s).
		Interval: cfg.SyncHeartbeatInterval,
		// Re-resolved every interval rather than cached, and shared with the
		// source lookup's holders client: both want the same address, and for the
		// same reason — §7.13.1's re-resolve instead of a cached forwarding path.
		ResolveLeader: a.topology.resolveControlLeader(a.rn),
		// The chain tails are what tell this node it has fallen behind.
		ChainTails: chainTails,
		Logger:     logger,
	}).Run(ctx)

	// §7.5: reclaim the WAL's recorded changes once every replica holds the versions
	// behind them. This is a background loop on purpose — compacting rewrites a file,
	// and nothing on the apply path may block on it. Each pass asks the replicas
	// themselves how far they have reached.
	go plane.NewWALReclaimer(plane.WALReclaimerConfig{
		Reclaimer: a.dataPlane,
		// gc.wal_reclaim_interval_s; zero takes the plane's default (5 min).
		Interval: cfg.GCWALReclaimInterval,
		Logger:   logger,
	}).Run(ctx)

	logger.Info("Stratum gRPC server listening", zap.String("addr", cfg.GRPCAddr))

	// docs/storage-node-state-machine-plan.md §3.1: reconciliation is done and
	// the listener is accepting, so the node is SERVING. It is the last phase
	// transition of startup and the one an operator's status call should see for
	// the life of the process.
	//
	// Note what is NOT a condition here: whether every knowledge base is caught
	// up, or whether a replica is short. Those are the orthogonal dimensions —
	// a node serves while it catches up, and reporting it as not-serving would
	// read as an outage.
	if nodeState != nil {
		if err := nodeState.Transition(nodestate.NodeServing); err != nil {
			logger.Error("node state: entering SERVING", zap.Error(err))
		}
	}

	// §7.9: publish the epoch payload now that this node is serving. It cannot be
	// done during the reconcile above, because the payload's data side is a quorum
	// claim and every storage node is still inside its own reconcile at that
	// point — none of them serves until that returns, so each one sees only itself
	// and the claim fails for every knowledge base at once. The helper retries,
	// since "this node is serving" does not imply "its peers are".
	go reportEpochWhenPeersAreUp(ctx, logger, a.dataPlane, controlPlane, epochDurable,
		cfg.EpochReportRetryInterval, cfg.EpochReportRetryWindow)

	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatal("gRPC server failed", zap.Error(err))
	}

	logger.Info("Stratum stopped")
}

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
// controlAddrsFromPeers is the control cluster's address table as a storage node
// needs it: every Raft peer by ID. The peers list is the control layer's own
// membership, which is exactly who can answer a metadata read or accept a
// forwarded proposal.
func controlAddrsFromPeers(peers []raft.PeerConfig) map[int64]string {
	addrs := make(map[int64]string, len(peers))
	for _, peer := range peers {
		if peer.ServiceAddr != "" {
			addrs[peer.ID] = peer.ServiceAddr
		}
	}
	return addrs
}

// replicaPusher adapts stratumsync.Pusher to plane.VersionPusher: the storage
// layer only needs the acknowledging replica's node ID, not the whole
// response.
type replicaPusher struct {
	pusher *stratumsync.Pusher
}

func (r replicaPusher) PushVersion(ctx context.Context, targetAddr, kbID string, versionID int64) (int64, error) {
	ack, err := r.pusher.PushVersion(ctx, targetAddr, kbID, versionID)
	if err != nil {
		return 0, err
	}
	return ack.GetAcceptorId(), nil
}

// reportIndexStatus maps an index build outcome onto the ControlPlane
// contract: a finished build reports the index ready, a failed one reports a
// failure on the INDEX side, which is where a build's retry budget and its
// terminal verdict live (Stratum_设计文档v13.md §10.1, §10.1b).
//
// It deliberately no longer goes through the storage layer's availability
// channel (plane.ControlPlane had a ReportAvailability method, now removed):
// that channel described the version's abstract availability, a different
// question from "did this build fail". Routing build failures through it set a
// retryable FAILED with no budget behind it — so nothing ever reached a verdict
// for the index side, and a build that kept failing simply kept reporting.
// deletionLogSource narrows a raft node to the incremental deletion-log read, or nil
// when this shape cannot answer it. The sweep treats nil as "no log reachable" and
// stays on the live-chain path, which is the behaviour before the log existed.
func deletionLogSource(rn raft.RaftNode) plane.DeletionLogSource {
	if src, ok := rn.(plane.DeletionLogSource); ok {
		return src
	}
	return nil
}

// tombstoneRecorder returns the delete-version coordinator's tombstone recorder, or nil
// when this node keeps no table.
//
// The nil has to be an untyped interface nil: handing back the nil *PebbleStore would
// give the coordinator a NON-nil interface holding a nil pointer, and the first call
// would dereference it.
func tombstoneRecorder(t *tombstone.PebbleStore) coordinator.VersionTombstoneRecorder {
	if t == nil {
		return nil
	}
	return t
}

// tombstoneOption returns the receive side's tombstone option, or nil when this
// node keeps no table. NewPushHandler skips nil options, which keeps the call site
// free of a branch wrapped around a single argument.
func tombstoneOption(t *tombstone.PebbleStore) stratumsync.PushHandlerOption {
	if t == nil {
		return nil
	}
	return stratumsync.WithVersionTombstones(t)
}

func reportIndexStatus(ctx context.Context, cp plane.ControlPlane, kbID string, versionID int64, status types.IndexStatus) error {
	switch status {
	case types.IndexStatusReady:
		return cp.ReportIndexReady(ctx, kbID, versionID)
	case types.IndexStatusFailed:
		// The terminal flag is deliberately ignored: it means "the DATA side
		// should reclaim its data". An index-side verdict reclaims nothing —
		// the data may be perfectly durable, which is the whole reason the two
		// sides are counted apart.
		_, err := cp.ReportVersionFailure(ctx, kbID, versionID, types.FailureSideIndex,
			types.FailureTransient, "index build failed")
		return err
	case types.IndexStatusFailedPermanent:
		// 确定性失败：再试一次还是同一批数据、同一个参数，所以声明 FailureFatalGlobal
		// ——它短路重试预算，直接把版本推到终态（internal/plane/local_control_plane.go
		// 的 ReportVersionFailure）。少了这一条，这类失败会以「暂时失败」的名义留在
		// PENDING 上，直到预算被耗光，而预算根本不该花在它身上。
		_, err := cp.ReportVersionFailure(ctx, kbID, versionID, types.FailureSideIndex,
			types.FailureFatalGlobal, "index build failed (non-retryable)")
		return err
	default:
		return fmt.Errorf("index build reported unexpected status %v for version %d", status, versionID)
	}
}

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
// control-data-separation-design.md §5.3/§7 and the decision table at the
// call site.
// projectNodeState rebuilds the version layer from disk facts at startup
// (docs/storage-node-state-machine-plan.md §5.1).
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
//     docs/active-lag-detection-design.md.
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
func verifyVersionPull(ctx context.Context, rn raft.RaftNode, vd versiondoc.VersionDocList, kbID string, versionID int64) bool {
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

// NodeRole selects which half of Stratum a process runs
// (Stratum_设计文档v13.md §7.0 的 v1 演进阶段 2、§11 阶段 ④「存储集群独立进程」).
type NodeRole string

const (
	// NodeRoleAll runs the control layer and the storage layer in one process,
	// with the vecstore beside them. It is the pre-split deployment and the
	// default, so a config that says nothing about roles keeps behaving exactly
	// as it did before roles existed.
	NodeRoleAll NodeRole = "all"

	// NodeRoleControl runs only the control layer: the Raft cluster and the
	// replicated metadata. It accepts writes, commits them, and hands each
	// version's data work to a storage node. It holds no documents, no chunks
	// and no indexes of its own.
	NodeRoleControl NodeRole = "control"

	// NodeRoleStorage runs only the storage layer: documents, chunks, indexes
	// and the vecstore they live in. It keeps no Raft log, and reads the
	// replicated metadata it needs through a RemoteRaftNode.
	NodeRoleStorage NodeRole = "storage"
)

// NodeRef names one member of the storage group.
type NodeRef struct {
	ID   int64
	Addr string // the node's gRPC service address
}

// parseNodeRole accepts a configured role name, rejecting an unknown one rather
// than falling back to the default: a typo in "storage" that quietly ran a full
// node would look like a working deployment while exercising the wrong topology.
func parseNodeRole(s string) (NodeRole, error) {
	switch role := NodeRole(s); role {
	case NodeRoleAll, NodeRoleControl, NodeRoleStorage:
		return role, nil
	default:
		return "", fmt.Errorf("unknown node role %q (want %q, %q or %q)",
			s, NodeRoleAll, NodeRoleControl, NodeRoleStorage)
	}
}

// appConfig holds the startup configuration for a Stratum node.
type appConfig struct {
	NodeID   int64
	Role     NodeRole
	DataDir  string
	GRPCAddr string
	RaftAddr string
	Peers    []raft.PeerConfig

	// MetricsAddr is where this node serves Prometheus /metrics
	// (node.metrics_addr). Empty — the default — leaves the endpoint off.
	MetricsAddr string

	// RequireAuthenticated makes this node accept client-facing calls
	// (KnowledgeBaseService / QueryService / AdminService) only when they carry
	// the service station's trust mark (Stratum_设计文档v13.md §9.3(5)).
	//
	// Off by default because a deployment without a station must keep working.
	// It is the setting to turn ON wherever a station sits in front of the
	// cluster: without it, anything that can reach a node's port steps around
	// the station's authentication — and silently, since a station misconfigured
	// to omit the mark looks perfectly normal.
	//
	// A configured StationSecret switches the default to true: a node that has
	// the station's shared key is, by that fact, part of a deployment with a
	// station in front of it, which is exactly the condition the gate exists for
	// (H4 of docs/code-review-2026-09-24.md). Setting `require_authenticated:
	// false` explicitly still wins — some deployments front a node with only
	// proxy-level access control and do not want a second gate.
	//
	// Internal collaboration (DataSyncService / InternalService) is never gated:
	// those calls carry no end user's credential, and requiring one would break
	// fan-out, catch-up and Raft forwarding outright.
	RequireAuthenticated bool

	// RequireAuthenticatedSet records whether the config file said anything about
	// RequireAuthenticated. Without it, "false" cannot be told apart from "unset"
	// — and the default above is derived from StationSecret rather than being a
	// constant, so the distinction decides behaviour.
	RequireAuthenticatedSet bool

	// StationSecret is the shared secret this node checks the service station's
	// trust mark with (node.station_secret). It must be the same value the
	// station stamps with (-station-secret).
	//
	// Empty means "no key": the node verifies no mark, so it must not require one
	// (see above) — and with require_authenticated on it would refuse every
	// client-facing call rather than accept unverified ones, which is the
	// failure an operator with a half-configured deployment should get.
	StationSecret string

	// StorageNodes is the storage group (Stratum_设计文档v13.md §11 阶段 ④).
	//
	// On a control node it is the replica topology a committed write is
	// dispatched to and a built index is distributed across. On a storage node
	// it is the replica set a write fans out to — including the node itself.
	//
	// Empty means "this deployment predates the split": every member of the Raft
	// cluster also holds data, which is how the all-in-one role behaves and why
	// an unmodified config keeps working.
	StorageNodes []NodeRef

	// Raft timing. The kvraft defaults ([150ms, 300ms) election timeout)
	// are tuned for in-process tests; over a real network (e.g. Docker)
	// they cause perpetual split votes, so we widen them here.
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// MaxLogLength bounds the raft log before a local snapshot is
	// requested; 0 = kvraft default (1000).
	MaxLogLength uint64

	// DeletedLogLimit bounds the control layer's deletion log (raft.deleted_log_limit,
	// 0 = 10 000). A bigger log lets a storage node fall further behind before it has
	// to re-judge from the live chain; every entry rides in each snapshot.
	DeletedLogLimit int

	// ControlPlaneFailureBudget is the global default for how many failed
	// attempts a version tolerates before the control layer declares it
	// FAILED_PERMANENT (Stratum_设计文档v13.md §10.1), from
	// control_plane.failure_budget. Zero or negative keeps
	// plane.DefaultFailureBudget.
	//
	// It is only the default: the control plane can also carry a per-KB
	// override (ControlPlane.SetFailureBudget). Nothing in the node assembly
	// sets that one today, so this value is what actually decides the verdict.
	ControlPlaneFailureBudget int

	VecstoreGRPCAddr string
	EmbedServiceAddr string

	// LogLevel is the process log level from the config file's logging.level
	// (debug/info/warn/error, zap's names). Empty or unknown means info. It was
	// ignored entirely before — the node built an info-level logger and never
	// looked at the setting, so `logging.level: debug` silently did nothing and
	// the per-stage query timings (and every other debug line) were unreachable.
	LogLevel string

	IndexLRUCapacity         int
	IndexLoadWaitTimeout     time.Duration
	IndexCallbackMaxRetries  int
	IndexCallbackRetryBaseMS int

	// IndexRetentionCount keeps the most recent N on-disk index files per
	// knowledge base (gc.version_retention_count); <= 0 keeps everything.
	IndexRetentionCount int

	// IndexRetentionProtectWindow shields recently-queried versions from that
	// policy (index_manager.retention_protect_window_ms): a version queried
	// here within the window stays on disk even though it is older than the
	// newest N. Zero takes the IndexManager default (24h); negative disables
	// the protection, keeping the historical "newest N only" behaviour.
	IndexRetentionProtectWindow time.Duration

	// IndexRetentionProtectMax caps how many versions that window may shield
	// (index_manager.retention_protect_max); <= 0 means the retention count.
	IndexRetentionProtectMax int

	// IndexMemoryThresholdMB bounds estimated in-memory footprint of all
	// loaded indexes (index_manager.memory_threshold_mb); <= 0 disables.
	IndexMemoryThresholdMB int64

	// IndexCandidateN is the coarse-pass candidate budget sent to the vector
	// store on every search (index_manager.candidate_n, §2.2); <= 0 leaves the
	// field unset so the vector store applies clamp(top_k × 8, 16, 4096).
	IndexCandidateN int

	// IndexColdThreshold is how long a version may go unqueried before it is
	// built in the graph-free form (index_manager.cold_threshold_ms, §8.6a);
	// <= 0 disables the policy. There is no sweep interval — the shape is decided
	// inside a build (coldShapeFor) and re-checked on the read path.
	IndexColdThreshold time.Duration

	// IndexBuildAbandonTimeout is how long a half-built index artifact may sit
	// on disk before the sweeper reclaims it
	// (index_manager.build_abandon_timeout_ms; §6 of
	// coordinator-selection-and-node-liveness-design.md).
	//
	// Semantics deliberately invert those of the cold threshold: reclaiming an
	// artifact that no one sealed is pure hygiene with no behavioural risk, so it
	// is ON by default (<= 0 takes 30 minutes) and only a NEGATIVE value disables
	// it.
	IndexBuildAbandonTimeout time.Duration

	// IndexAppendMaxDeadRatio bounds the dead weight a §8.6(c) pure-append
	// reuse may carry (index_manager.append_max_dead_ratio); <= 0 means the
	// IndexManager's default, 1.0 disables the check.
	IndexAppendMaxDeadRatio float64

	// IndexGCRatioThreshold is the dead-vector share above which a §8.6(d)
	// SCANNER reports an active version as a collection candidate
	// (index_manager.gc_ratio_threshold); <= 0 means the IndexManager's default.
	//
	// Separate knob from IndexAppendMaxDeadRatio on purpose — one decides whether a
	// BUILD may start from an artifact, the other whether a SEALED artifact is worth
	// reopening (which takes the version out of service here for a moment). Their
	// defaults happen to be equal today, and that is a known wart rather than a
	// decision: with both at the same value §8.6(c) rebuilds exactly when §8.6(d)
	// starts caring, so (d) never observes a candidate. Exposing the knob is what
	// lets a deployment separate them without a code change; the value itself waits
	// for deployment data (docs/known-gaps.md §G).
	IndexGCRatioThreshold float64

	// IndexMaxCodebookDriftRatio / IndexMaxCodebookAppends are §3's two triggers
	// for retiring a stale quantizer codebook
	// (index_manager.max_codebook_drift_ratio / max_codebook_appends): either one
	// fires and the build rebuilds from scratch instead of appending, which is the
	// only way to train a NEW codebook. <= 0 means the IndexManager's default.
	//
	// Both are inert unless the KB's quantizer actually learns a codebook (SQ8 or
	// PQ): SQ_FP16 / SQ_BF16 are trained at construction and OFF has none, so
	// those KBs never trigger a refresh however these knobs are set.
	IndexMaxCodebookDriftRatio float64
	IndexMaxCodebookAppends    int64
	// IndexMinCodebookBaselineVectors is the baseline size below which the drift
	// ratio is ignored (index_manager.min_codebook_baseline_vectors); <= 0 takes
	// the IndexManager's default, NEGATIVE removes the floor.
	IndexMinCodebookBaselineVectors int64

	// IndexGCEnabled turns on §8.6(d) collection (index_manager.gc_enabled). ON by
	// default since 2026-10: the scanner always runs and only reports, and the
	// collection itself is gated by serving_replica_min (other replicas must be
	// serving that version, so the version stays answerable while this node steps
	// out) and by gc_ratio_threshold (how much dead weight is enough). Set
	// `gc_enabled: false` to go back to report-only.
	IndexGCEnabled bool

	// ReconcileDeletedVersions reclaims this node's leftovers of DELETED versions
	// (reconcile.deleted_versions) — the half of docs/known-gaps.md §B that the
	// control layer's current state makes safe to act on. The scan always runs and
	// REPORTS; this decides whether the bytes are reclaimed.
	//
	// ON by default, unlike every other irreversibility switch here (gc_enabled,
	// index retention), because leaving it off makes a storage node's leftovers
	// invisible in practice: a version deleted by a BROADCAST is one that node has no
	// record of — it wrote no WAL mark, it was merely told to drop the data — so the
	// only thing that can name it again is this comparison against the control layer.
	// With the switch off, those bytes sit there until an operator happens to read a
	// startup log line.
	//
	// The trade those other switches make — "it is irreversible, and a mistaken
	// judgement would be replayed on every restart" — is bounded here by the judgement
	// itself: a prefix delete of versions the control layer no longer has, with §B's
	// safety rules enforced inside it (an unreadable answer skips the knowledge base
	// rather than guessing, and "never handed out" is not read as "removed").
	ReconcileDeletedVersions bool

	// ReconcileDeletedVersionsPeriodic keeps looking for those leftovers while the node
	// runs (reconcile.deleted_versions_periodic), instead of only at start-up.
	//
	// A switch of its own rather than a second meaning for the one above: the two
	// answer different questions about the same irreversible action — "clean up what is
	// lying around at boot" versus "keep recovering what appears while running" — so an
	// operator may want either alone.
	//
	// ON by default, for the stronger form of the reason above: the leftovers that
	// actually appear are the RUNTIME ones. A push that outlived the delete meant to
	// remove it needs no failure at all (§B's source ③), so a node that only looks at
	// boot carries that data until its next restart.
	//
	// It gates the ACTIVITY-DRIVEN full pass, not the rollback the incremental path needs
	// when the log cannot serve this node (no watermark, truncated, epoch mismatch): that
	// rollback runs whatever this says. Gating it too would leave a
	// "only the cheap incremental path" deployment permanently at skipped(chain-disabled)
	// — every incremental read falls back to a pass that is switched off.
	ReconcileDeletedVersionsPeriodic bool

	// ReconcileDeletedVersionsIntervalS is how old the last FULL live-chain pass may
	// get before the next sweep is forced to run one (<= 0 takes the plane's default,
	// 24 hours). It used to pace the periodic pass itself; the tick is now
	// ReconcileDeletionsIncrementalIntervalS, because the passes have different costs.
	ReconcileDeletedVersionsIntervalS int

	// ReconcileDeletionsIncremental turns on the incremental fast path: one small read
	// of the control layer's bounded deletion log per tick instead of a per-KB range
	// scan (reconcile.deleted_versions_incremental, ON by default).
	ReconcileDeletionsIncremental bool
	// ReconcileDeletionsIncrementalIntervalS paces that tick (<= 0 takes the plane's
	// default, 60 s). Only read when the incremental path is on.
	ReconcileDeletionsIncrementalIntervalS int
	// ReconcileDeletionsLogPage caps one incremental read (<= 0 takes the plane's
	// default, 512).
	ReconcileDeletionsLogPage int

	// IndexServingReplicaMin is how many OTHER replicas must be serving a version
	// before this node takes itself out of service to collect it
	// (index_manager.serving_replica_min); <= 0 means the IndexManager's default.
	IndexServingReplicaMin int

	// IndexGCGraphRebuildRatio is the dead-share bar for rebuilding a graphed
	// artifact (index_manager.graph_rebuild_ratio); <= 0 means the IndexManager's
	// default. Much higher than IndexAppendMaxDeadRatio on purpose: a rebuild
	// costs the whole graph.
	IndexGCGraphRebuildRatio float64

	// LagCatchup configures the background catch-up that turns the chain tail the
	// control leader reports into an occasional recovery
	// (lag_catchup.*, docs/active-lag-detection-design.md). There is no switch: a
	// replica that is behind and stays behind is one nothing routes to, so catching up
	// is ordinary behaviour rather than an operator's opt-in.
	//
	// LagCatchupJitterMS damps the trigger — each signal is followed by a random
	// delay in [0, that) — and LagCatchupMaxConcurrentKBs bounds how many knowledge
	// bases may catch up at once (<= 0 takes the default). LagCatchupMinLagVersions
	// is how far behind the tail counts as left behind.
	LagCatchupMinLagVersions   int
	LagCatchupJitterMS         int
	LagCatchupMaxConcurrentKBs int

	// IndexGCSweepInterval is how often the §8.6(d) scanner re-estimates the dead
	// share (index_manager.gc_sweep_interval_ms); <= 0 means the IndexManager's
	// default, negative disables the scanner. Shortening it is cheap — a scan only
	// reads local state — but collection candidates are only ever produced by a
	// scan, so disabling the scanner disables collection too.
	IndexGCSweepInterval time.Duration

	// IndexBuildConcurrency is how many index builds may run at once
	// (index_manager.build_concurrency); <= 0 means the IndexManager's default (the
	// CPU count). It bounds how much contention a rebuild sweep can cause; the
	// manager's build pool separately keeps such a sweep from outranking a live write.
	IndexBuildConcurrency int

	// IndexFetchConcurrency is how many index-artifact transfers (§8.4) THIS
	// NODE WILL SERVE AT ONCE (index_manager.fetch_concurrency); <= 0 serves
	// them without a bound.
	//
	// It bounds the serving side, which is where the resource is spent: one
	// transfer reads a whole index file into memory before streaming it, so with
	// distribution now pull-only this is the one place a distribution storm —
	// every replica asking the same node for the same artifact — turns into a
	// queue instead of a memory spike.
	IndexFetchConcurrency int

	WriteMaxRetries   int
	WriteRetryBaseMS  int
	DeleteMaxRetries  int
	DeleteRetryBaseMS int

	// Bloom filters. Both the chunk-existence filter (write path) and the
	// version-document filter (read path) are sized with these parameters.
	BloomExpectedItems     uint
	BloomFalsePositiveRate float64

	// Orphan-chunk garbage collector sweep interval.
	GCSweepIntervalSec int

	// --- Tuning knobs, grouped by the component they belong to. ---
	//
	// Every duration/limit below reads 0 (or, where noted, a negative value) as
	// "leave the component's own default in place". That keeps the defaults in
	// one place — the components that own them — instead of restating them here,
	// and it is what makes an unconfigured node behave exactly as it did before
	// these keys existed.

	// StationMarkTTL is how long the station's trust mark stays acceptable
	// (node.station_mark_ttl_ms); 0 takes authmeta.DefaultMarkTTL.
	StationMarkTTL time.Duration

	// RaftRPCTimeout / RaftProposeApplyTimeout are the Raft log's own bounds
	// (raft.rpc_timeout_ms / propose_apply_timeout_ms); 0 takes kvraft's /
	// raft's defaults.
	RaftRPCTimeout          time.Duration
	RaftProposeApplyTimeout time.Duration

	// VecstoreKeepaliveInterval / VecstoreKeepaliveTimeout configure the gRPC
	// channel to the vecstore process, shared by the chunk store and the index
	// manager (vecstore.keepalive_*_ms); 0 takes 60 s / 3 s.
	VecstoreKeepaliveInterval time.Duration
	VecstoreKeepaliveTimeout  time.Duration

	// EmbedTimeout is the HTTP client timeout for the embed service
	// (embed.timeout_ms). 0 takes 30 s; negative means no client-level timeout.
	EmbedTimeout time.Duration

	// Data plane (data_plane.*): the pull/push/confirm/takeover budgets and the
	// per-KB write cap.
	PullIdleTimeout         time.Duration
	PullMaxDuration         time.Duration
	MaxInFlightWrites       int
	CursorWriteTimeout      time.Duration
	CursorRetryDelay        time.Duration
	ConfirmAttemptTimeout   time.Duration
	ConfirmRetryBackoff     time.Duration
	ConfirmBudget           time.Duration
	ReplicaPushTimeout      time.Duration
	PushTimeoutPerDoc       time.Duration
	PeerCursorTimeout       time.Duration
	TakeoverTimeout         time.Duration
	TakeoverCheckTimeout    time.Duration
	TakeoverMaxAttempts     int
	TakeoverMaxRetryDelay   time.Duration
	TerminalReclaimInterval time.Duration
	TerminalReclaimAttempts int

	// sync.*: heartbeat cadence, pull handshake bound, holders cache.
	SyncHeartbeatInterval time.Duration
	PullDialTimeout       time.Duration
	HoldersCacheTTL       time.Duration
	HoldersCacheLimit     int

	// dispatcher.*: one write candidate's budget.
	CandidateTimeout       time.Duration
	CandidateTimeoutPerDoc time.Duration
	MaxCandidateTimeout    time.Duration

	// health.*: when a peer is demoted and for how long.
	HealthDemoteAfter int
	HealthDemoteFor   time.Duration

	// reconcile.*: the pacing of the deleted-version reconciliation and its
	// local tombstone retry.
	ReconcileChainMinAge        time.Duration
	ReconcileDeletionPages      int
	DeletedVersionRetryInterval time.Duration
	TombstoneGrace              time.Duration

	// index_manager.*: build-side timing and the brute-force fallback size.
	IndexBruteForceMaxChunks       int
	IndexBuildAbandonSweepInterval time.Duration
	IndexBuildRetryTimeout         time.Duration
	IndexBuildRetryInterval        time.Duration
	IndexAccessPersistInterval     time.Duration

	// write_coordinator.*: the background dispatch and document-write bounds.
	WriteDispatchTimeout      time.Duration
	WriteDispatchClaimWindow  time.Duration
	WriteAbandonReportTimeout time.Duration
	WriteMaxConcurrentDocs    int

	// delete_coordinator.*: the two sweeps that finish interrupted deletions.
	DeletingVersionSweepInterval time.Duration
	DeletingKBSweepInterval      time.Duration

	// lag_catchup.*: one catch-up's budget and its holders refresh.
	LagCatchupTimeout        time.Duration
	LagCatchupHoldersRefresh time.Duration

	// GCWALReclaimInterval is how often the WAL reclaimer compacts
	// (gc.wal_reclaim_interval_s); 0 takes plane.DefaultReclaimInterval.
	GCWALReclaimInterval time.Duration

	// recovery.*: the node's own start-up/shutdown pacing. These are the only
	// knobs here whose defaults live in this file rather than in a component,
	// because nothing else uses them.
	CrashReplayMaxAttempts   int
	CrashReplayRetryBaseMS   int
	EpochReportRetryInterval time.Duration
	EpochReportRetryWindow   time.Duration
	ShutdownTimeout          time.Duration
}

// fileConfig mirrors the on-disk YAML schema (configs/config1.yaml and
// integration/docker/config{1,2,3}.yaml). Fields left unset fall back to
// defaultConfig()'s values.
type fileConfig struct {
	// Logging.Level (debug/info/warn/error, zap's names) becomes
	// appConfig.LogLevel. It used to be parsed by nobody at all: the node built an
	// info-level logger before the config file was even read, so setting
	// `logging.level: debug` did nothing.
	Logging struct {
		Level string `yaml:"level"`
	} `yaml:"logging"`

	Node struct {
		NodeID   int64  `yaml:"node_id"`
		Role     string `yaml:"role"`
		GRPCAddr string `yaml:"grpc_addr"`
		RaftAddr string `yaml:"raft_addr"`
		// MetricsAddr is where Prometheus scrapes this node.
		MetricsAddr string `yaml:"metrics_addr"`

		// RequireAuthenticated is a pointer so "unset" and "false" stay
		// distinguishable: leaving it out lets the default follow
		// StationSecret (see appConfig.RequireAuthenticated), while an explicit
		// `require_authenticated: false` is honoured as written.
		RequireAuthenticated *bool `yaml:"require_authenticated"`

		// StationSecret is the shared secret the station's trust mark is
		// signed with (internal/authmeta). Same value as the station's
		// -station-secret.
		StationSecret string `yaml:"station_secret"`

		// StationMarkTTLMS is how long a forwarded call's trust mark stays
		// acceptable; <= 0 takes authmeta.DefaultMarkTTL (30 s).
		StationMarkTTLMS int `yaml:"station_mark_ttl_ms"`
	} `yaml:"node"`

	Raft struct {
		Peers []struct {
			ID          int64  `yaml:"id"`
			Addr        string `yaml:"addr"`
			ServiceAddr string `yaml:"service_addr"`
		} `yaml:"peers"`

		// Raft timing (ms). These are optional in the YAML; unset fields
		// keep defaultConfig()'s values. The ops console generates them so
		// startup timing can be tuned from the web UI.
		HeartbeatIntervalMS  int64 `yaml:"heartbeat_interval_ms"`
		ElectionTimeoutMinMS int64 `yaml:"election_timeout_min_ms"`
		ElectionTimeoutMaxMS int64 `yaml:"election_timeout_max_ms"`

		// MaxLogLength bounds the raft log before a local snapshot is
		// requested (kvraft default 1000). Optional; 0 = kvraft default.
		// Exposed so large-volume deployments (or snapshot tests) can
		// tune compaction frequency without a code change.
		MaxLogLength int64 `yaml:"max_log_length"`
		// DeletedLogLimit bounds the bounded deletion log a storage node consumes
		// incrementally (0 = 10 000 entries). Raising it lets a node fall further
		// behind before it must re-read a whole version range; it also grows each
		// snapshot.
		DeletedLogLimit int `yaml:"deleted_log_limit"`

		// RPCTimeoutMS bounds one peer-to-peer Raft RPC; <= 0 takes kvraft's 1 s.
		RPCTimeoutMS int `yaml:"rpc_timeout_ms"`
		// ProposeApplyTimeoutMS bounds one proposal's wait for its entry to be
		// applied; <= 0 takes raft's 60 s.
		ProposeApplyTimeoutMS int `yaml:"propose_apply_timeout_ms"`
	} `yaml:"raft"`

	// ControlPlane carries the control layer's own knobs. Today that is the
	// failure budget behind the FAILED_PERMANENT verdict (§10.1).
	ControlPlane struct {
		// FailureBudget is how many failed attempts a version tolerates before
		// the control layer declares it FAILED_PERMANENT. 0 keeps the plane's
		// default (5); a negative value also means "keep the default", since the
		// plane only accepts positive overrides.
		FailureBudget int `yaml:"failure_budget"`
	} `yaml:"control_plane"`

	Storage struct {
		DataDir string `yaml:"data_dir"`

		// Nodes is the storage group: the members that hold data and build
		// indexes. Omitted (the pre-split shape) means "every Raft member also
		// stores", which is what the all-in-one role assumes.
		Nodes []struct {
			ID   int64  `yaml:"id"`
			Addr string `yaml:"addr"`
		} `yaml:"nodes"`
	} `yaml:"storage"`

	Vecstore struct {
		GRPCAddr string `yaml:"grpc_addr"`
		// KeepaliveIntervalMS / KeepaliveTimeoutMS tune the gRPC channel both the
		// chunk store and the index manager open to the vecstore. <= 0 takes
		// 60 s / 3 s. Only ACTIVE streams are probed: a stream-less ping gets this
		// client GoAway'd by the vecstore's C++ server.
		KeepaliveIntervalMS int `yaml:"keepalive_interval_ms"`
		KeepaliveTimeoutMS  int `yaml:"keepalive_timeout_ms"`
	} `yaml:"vecstore"`

	Embed struct {
		ServiceAddr string `yaml:"service_addr"`
		// TimeoutMS is the HTTP client timeout; <= 0 takes 30 s.
		TimeoutMS int `yaml:"timeout_ms"`
	} `yaml:"embed"`

	// Reconcile is the reconciliation's opt-ins — the start-up sweep and the runtime
	// pass separately, since they are different decisions about one irreversible action.
	Reconcile struct {
		// DeletedVersions reclaims local leftovers of deleted versions at start-up
		// (docs/known-gaps.md §B). The scan itself always runs and reports; this
		// only decides whether the bytes are reclaimed. On by default — see the
		// appConfig field comment for why this one differs from the other
		// irreversibility switches.
		DeletedVersions bool `yaml:"deleted_versions"`
		// DeletedVersionsPeriodic does the same while the node runs. Independent of
		// the start-up switch; also on by default.
		DeletedVersionsPeriodic bool `yaml:"deleted_versions_periodic"`
		// DeletedVersionsIntervalS is how old the last FULL live-chain pass may get
		// before the sweep forces another one (<= 0 means the plane's 24 hours).
		DeletedVersionsIntervalS int `yaml:"deleted_versions_interval_s"`
		// DeletedVersionsIncremental turns on the incremental fast path — one small
		// read of the control layer's bounded deletion log per tick, instead of a
		// per-KB range scan. On by default, and INDEPENDENT of the periodic switch:
		// the two have different costs, and wanting the cheap one without the
		// expensive one is a reasonable deployment.
		DeletedVersionsIncremental bool `yaml:"deleted_versions_incremental"`
		// DeletedVersionsIncrementalIntervalS paces that tick (<= 0 means 60 s).
		DeletedVersionsIncrementalIntervalS int `yaml:"deleted_versions_incremental_interval_s"`
		// DeletedVersionsLogPage caps one incremental read (<= 0 means 512).
		DeletedVersionsLogPage int `yaml:"deleted_versions_log_page"`
		// ChainReconcileMinAgeMS is the floor between two FULL passes triggered by
		// local activity (<= 0 means 5 minutes), so a busy node does not turn
		// "something landed here" into a range scan every tick.
		ChainReconcileMinAgeMS int `yaml:"chain_reconcile_min_age_ms"`
		// DeletionPagesPerSweep caps one pass's pagination (<= 0 means 8); the
		// following ticks finish a larger bulk deletion.
		DeletionPagesPerSweep int `yaml:"deletion_pages_per_sweep"`
		// DeletedVersionRetryIntervalMS paces the LOCAL retry of deletions this node
		// was told about (<= 0 means 30 s). Much shorter than the reconciliation's
		// cadence on purpose: it has no network in it.
		DeletedVersionRetryIntervalMS int `yaml:"deleted_version_retry_interval_ms"`
		// TombstoneGraceMS is how long a finished deletion's entry is kept so a late
		// arrival is still refused (<= 0 means 1 hour).
		TombstoneGraceMS int `yaml:"tombstone_grace_ms"`
	} `yaml:"reconcile"`

	IndexManager struct {
		LRUCapacity       int `yaml:"lru_capacity"`
		MemoryThresholdMB int `yaml:"memory_threshold_mb"`
		// CandidateN is the coarse-pass budget for quantized search (§2.2).
		// 0 leaves it to the vector store's own clamp(top_k × 8, 16, 4096).
		CandidateN          int `yaml:"candidate_n"`
		LoadWaitTimeoutMS   int `yaml:"load_wait_timeout_ms"`
		CallbackMaxRetries  int `yaml:"callback_max_retries"`
		CallbackRetryBaseMS int `yaml:"callback_retry_base_interval_ms"`
		ColdThresholdMS     int `yaml:"cold_threshold_ms"`
		// RetentionProtectWindowMS shields recently-queried versions from the
		// disk retention policy. 0 takes the default (24h), negative disables.
		RetentionProtectWindowMS int `yaml:"retention_protect_window_ms"`
		// RetentionProtectMax caps how many versions that window may shield;
		// <= 0 means version_retention_count.
		RetentionProtectMax int `yaml:"retention_protect_max"`
		// BuildAbandonTimeoutMS is the §6 abandoned-artifact window. <= 0 takes
		// the default (30 minutes); negative disables the sweeper.
		BuildAbandonTimeoutMS int     `yaml:"build_abandon_timeout_ms"`
		AppendMaxDeadRatio    float64 `yaml:"append_max_dead_ratio"`
		// §8.6(d) scanner threshold. 0 = unset, which leaves the IndexManager's
		// own default in place (the same reading as append_max_dead_ratio above).
		GCRatioThreshold float64 `yaml:"gc_ratio_threshold"`
		// §3 codebook refresh. Either trigger fires and the build rebuilds from
		// scratch instead of appending, which is the only way to retrain the
		// quantizer. Both are inert for KBs whose quantizer does not learn a
		// codebook (OFF / SQ_FP16 / SQ_BF16); <= 0 takes the default.
		MaxCodebookDriftRatio float64 `yaml:"max_codebook_drift_ratio"`
		MaxCodebookAppends    int64   `yaml:"max_codebook_appends"`
		// MinBaselineVectors is the baseline size below which the drift ratio is
		// ignored, so a tiny KB does not rebuild on every version.
		MinBaselineVectors int64 `yaml:"min_codebook_baseline_vectors"`
		// §8.6(d) collection. The scanner always runs (it only reads); GCEnabled
		// additionally lets it rewrite a SERVING artifact.
		//
		// A pointer so "absent" and "false" are distinguishable: absent takes the
		// default, which is ON since 2026-10 (collection is gated by
		// serving_replica_min and only fires when the dead share clears
		// gc_ratio_threshold — see DefaultGCRatioThreshold). Write `false`
		// explicitly for the pre-2026-10 report-only behaviour.
		GCEnabled *bool `yaml:"gc_enabled"`
		// ServingReplicaMin is how many other replicas must be serving before this
		// node steps out to collect (§8.6(d)). <= 0 takes the default (2).
		ServingReplicaMin int `yaml:"serving_replica_min"`
		// GraphRebuildRatio is the dead-share bar for rebuilding a graphed
		// artifact, which costs the whole graph. <= 0 takes the default (0.5),
		// deliberately far above append_max_dead_ratio.
		GraphRebuildRatio float64 `yaml:"graph_rebuild_ratio"`
		// GCSweepIntervalMS is how often the scanner re-estimates. <= 0 takes the
		// default (10 minutes); negative disables the scanner (and with it
		// collection — nothing would ever produce a candidate).
		GCSweepIntervalMS int `yaml:"gc_sweep_interval_ms"`
		// BuildConcurrency is how many index builds may run at once. <= 0 takes the
		// IndexManager's default (the CPU count).
		//
		// Worth having as a knob rather than a constant: the right number trades
		// build throughput against how much a build steals from live queries, and
		// that trade depends on the deployment's disk and vecstore, not on anything
		// this repository can know.
		BuildConcurrency int `yaml:"build_concurrency"`
		// FetchConcurrency is how many artifact transfers (§8.4) this node will
		// serve at once. <= 0 serves them without a bound — the gate is off, which
		// is the shape a single-replica or throwaway deployment wants.
		//
		// It bounds a fetch storm's cost in source-node disk IO and memory — one
		// transfer reads the whole index file into memory before streaming it, and
		// N−1 replicas asking the same node for the same artifact is the ordinary
		// consequence of a version switch. Since distribution became pull-only
		// this is the only gate on it.
		FetchConcurrency int `yaml:"fetch_concurrency"`
		// BruteForceMaxChunks is the version size up to which a query for an
		// unbuilt version is answered by scanning instead of building (§8.6b).
		// <= 0 takes the IndexManager's default (50 000).
		BruteForceMaxChunks int `yaml:"brute_force_max_chunks"`
		// BuildAbandonSweepIntervalMS is how often the abandoned-artifact sweeper
		// looks (<= 0 takes 5 minutes).
		BuildAbandonSweepIntervalMS int `yaml:"build_abandon_sweep_interval_ms"`
		// BuildRetryTimeoutMS / BuildRetryIntervalMS bound the in-build retry loop
		// that waits for a racing data sync to land (<= 0 means 5 minutes / 2 s).
		// Raising the timeout lets a build win against a slow pull instead of
		// failing and being retried from the control layer.
		BuildRetryTimeoutMS  int `yaml:"build_retry_timeout_ms"`
		BuildRetryIntervalMS int `yaml:"build_retry_interval_ms"`
		// AccessPersistIntervalMS throttles how often a version's access time is
		// written to its sidecar — the evidence the retention shield reads
		// (<= 0 means 1 minute).
		AccessPersistIntervalMS int `yaml:"access_persist_interval_ms"`
	} `yaml:"index_manager"`

	WriteCoordinator struct {
		MaxRetries          int `yaml:"max_retries"`
		RetryBaseIntervalMS int `yaml:"retry_base_interval_ms"`
		// DispatchTimeoutMS bounds ONE background dispatch, from the commit to the
		// hand-off (<= 0 means 5 minutes). It must stay ABOVE the dispatcher's
		// per-candidate budget — it is the outer bound.
		DispatchTimeoutMS int `yaml:"dispatch_timeout_ms"`
		// DispatchClaimWindowMS is how long a dispatch's registration claim keeps
		// explaining a later miss (<= 0 means 2 minutes).
		DispatchClaimWindowMS int `yaml:"dispatch_claim_window_ms"`
		// AbandonReportTimeoutMS bounds the Raft round trip that records a give-up
		// (<= 0 means 30 s).
		AbandonReportTimeoutMS int `yaml:"abandon_report_timeout_ms"`
		// MaxConcurrentDocumentWrites bounds how many documents one version's write
		// processes at once (<= 0 means 8).
		MaxConcurrentDocumentWrites int `yaml:"max_concurrent_document_writes"`
	} `yaml:"write_coordinator"`

	DeleteCoordinator struct {
		MaxRetries          int `yaml:"max_retries"`
		RetryBaseIntervalMS int `yaml:"retry_base_interval_ms"`
		// DeletingVersionSweepIntervalMS / DeletingKBSweepIntervalMS pace the passes
		// that finish a deletion whose cleanup never completed (<= 0 means 1 minute
		// each).
		DeletingVersionSweepIntervalMS int `yaml:"deleting_version_sweep_interval_ms"`
		DeletingKBSweepIntervalMS      int `yaml:"deleting_kb_sweep_interval_ms"`
	} `yaml:"delete_coordinator"`

	BloomFilter struct {
		ExpectedItems     uint64  `yaml:"expected_items"`
		FalsePositiveRate float64 `yaml:"false_positive_rate"`
	} `yaml:"bloom_filter"`

	GC struct {
		VersionRetentionCount int `yaml:"version_retention_count"`
		SweepIntervalSec      int `yaml:"sweep_interval_s"`
		// WALReclaimIntervalSec paces WAL compaction (<= 0 means 5 minutes).
		WALReclaimIntervalSec int `yaml:"wal_reclaim_interval_s"`
	} `yaml:"gc"`

	// LagCatchup is the background catch-up described in
	// docs/active-lag-detection-design.md: the node reads the chain tail the control
	// leader carries back on its cursor report and catches up when it is behind.
	LagCatchup struct {
		MinLagVersions   int `yaml:"min_lag_versions"`
		JitterMS         int `yaml:"jitter_ms"`
		MaxConcurrentKBs int `yaml:"max_concurrent_kbs"`
		// TimeoutMS bounds one catch-up; HoldersRefreshIntervalMS paces how often a
		// running catch-up re-asks its peers who can serve it.
		TimeoutMS                int `yaml:"timeout_ms"`
		HoldersRefreshIntervalMS int `yaml:"holders_refresh_interval_ms"`
	} `yaml:"lag_catchup"`

	// DataPlane carries the storage plane's peer-facing budgets: pulls, pushes,
	// confirmations, takeovers and the per-KB write cap. Every field reads 0 as
	// "the component's default", so a config file that predates this section
	// behaves exactly as before.
	DataPlane struct {
		PullIdleTimeoutMS         int `yaml:"pull_idle_timeout_ms"`
		PullMaxDurationMS         int `yaml:"pull_max_duration_ms"`
		MaxInFlightWrites         int `yaml:"max_in_flight_writes"`
		CursorWriteTimeoutMS      int `yaml:"cursor_write_timeout_ms"`
		CursorRetryDelayMS        int `yaml:"cursor_retry_delay_ms"`
		ConfirmAttemptTimeoutMS   int `yaml:"confirm_attempt_timeout_ms"`
		ConfirmRetryBackoffMS     int `yaml:"confirm_retry_backoff_ms"`
		ConfirmBudgetMS           int `yaml:"confirm_budget_ms"`
		ReplicaPushTimeoutMS      int `yaml:"replica_push_timeout_ms"`
		PushTimeoutPerDocMS       int `yaml:"push_timeout_per_doc_ms"`
		PeerCursorTimeoutMS       int `yaml:"peer_cursor_timeout_ms"`
		TakeoverTimeoutMS         int `yaml:"takeover_timeout_ms"`
		TakeoverCheckTimeoutMS    int `yaml:"takeover_check_timeout_ms"`
		TakeoverMaxAttempts       int `yaml:"takeover_max_attempts"`
		TakeoverMaxRetryDelayMS   int `yaml:"takeover_max_retry_delay_ms"`
		TerminalReclaimIntervalMS int `yaml:"terminal_reclaim_interval_ms"`
		TerminalReclaimAttempts   int `yaml:"terminal_reclaim_attempts"`
	} `yaml:"data_plane"`

	// Sync is the node-to-peer control traffic: the liveness heartbeat, the pull
	// handshake, and the holders cache a catch-up fills.
	Sync struct {
		HeartbeatIntervalMS int `yaml:"heartbeat_interval_ms"`
		PullDialTimeoutMS   int `yaml:"pull_dial_timeout_ms"`
		HoldersCacheTTLMS   int `yaml:"holders_cache_ttl_ms"`
		HoldersCacheLimit   int `yaml:"holders_cache_limit"`
	} `yaml:"sync"`

	// Dispatcher is one write candidate's budget (§7.13.2): a floor, a per-document
	// allowance, and the cap on their sum.
	Dispatcher struct {
		CandidateTimeoutMS       int `yaml:"candidate_timeout_ms"`
		CandidateTimeoutPerDocMS int `yaml:"candidate_timeout_per_doc_ms"`
		MaxCandidateTimeoutMS    int `yaml:"max_candidate_timeout_ms"`
	} `yaml:"dispatcher"`

	// Health is the §7.13.5 peer-demotion heuristic.
	Health struct {
		DemoteAfter int `yaml:"demote_after"`
		DemoteForMS int `yaml:"demote_for_ms"`
	} `yaml:"health"`

	// Recovery is the node's own start-up/shutdown pacing: WAL crash replay, the
	// epoch payload's retry, and how long a graceful stop waits.
	Recovery struct {
		CrashReplayMaxAttempts     int `yaml:"crash_replay_max_attempts"`
		CrashReplayRetryBaseMS     int `yaml:"crash_replay_retry_base_ms"`
		EpochReportRetryIntervalMS int `yaml:"epoch_report_retry_interval_ms"`
		EpochReportRetryWindowMS   int `yaml:"epoch_report_retry_window_ms"`
		ShutdownTimeoutMS          int `yaml:"shutdown_timeout_ms"`
	} `yaml:"recovery"`
}

// loadConfig reads a YAML config file and overlays it on the defaults.
// Unset fields keep their defaultConfig() values.
func loadConfig(path string) (appConfig, error) {
	cfg := defaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}

	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return cfg, fmt.Errorf("parse %s: %w", path, err)
	}

	if fc.Node.NodeID != 0 {
		cfg.NodeID = fc.Node.NodeID
	}
	if fc.Node.GRPCAddr != "" {
		cfg.GRPCAddr = fc.Node.GRPCAddr
	}
	if fc.Node.RaftAddr != "" {
		cfg.RaftAddr = fc.Node.RaftAddr
	}
	if fc.Node.MetricsAddr != "" {
		cfg.MetricsAddr = fc.Node.MetricsAddr
	}
	if len(fc.Raft.Peers) > 0 {
		peers := make([]raft.PeerConfig, 0, len(fc.Raft.Peers))
		for _, p := range fc.Raft.Peers {
			peers = append(peers, raft.PeerConfig{
				ID:          p.ID,
				RaftAddr:    p.Addr,
				ServiceAddr: p.ServiceAddr,
			})
		}
		cfg.Peers = peers
	}
	if fc.Raft.HeartbeatIntervalMS != 0 {
		cfg.HeartbeatInterval = time.Duration(fc.Raft.HeartbeatIntervalMS) * time.Millisecond
	}
	if fc.Raft.ElectionTimeoutMinMS != 0 {
		cfg.ElectionTimeoutMin = time.Duration(fc.Raft.ElectionTimeoutMinMS) * time.Millisecond
	}
	if fc.Raft.ElectionTimeoutMaxMS != 0 {
		cfg.ElectionTimeoutMax = time.Duration(fc.Raft.ElectionTimeoutMaxMS) * time.Millisecond
	}
	if fc.Raft.DeletedLogLimit > 0 {
		cfg.DeletedLogLimit = fc.Raft.DeletedLogLimit
	}
	if fc.Raft.MaxLogLength > 0 {
		cfg.MaxLogLength = uint64(fc.Raft.MaxLogLength)
	}
	if fc.Node.RequireAuthenticated != nil {
		cfg.RequireAuthenticated = *fc.Node.RequireAuthenticated
		cfg.RequireAuthenticatedSet = true
	}
	if fc.Node.StationSecret != "" {
		cfg.StationSecret = fc.Node.StationSecret
		// A node that holds the station's key is part of a deployment with a
		// station in front of it, and that is the condition the gate exists
		// for — so the key turns it on. An explicit setting still wins.
		if !cfg.RequireAuthenticatedSet {
			cfg.RequireAuthenticated = true
		}
	}
	if fc.Node.Role != "" {
		role, err := parseNodeRole(fc.Node.Role)
		if err != nil {
			return cfg, fmt.Errorf("parse %s: %w", path, err)
		}
		cfg.Role = role
	}
	if fc.ControlPlane.FailureBudget != 0 {
		cfg.ControlPlaneFailureBudget = fc.ControlPlane.FailureBudget
	}
	if fc.Storage.DataDir != "" {
		cfg.DataDir = fc.Storage.DataDir
	}
	if len(fc.Storage.Nodes) > 0 {
		nodes := make([]NodeRef, 0, len(fc.Storage.Nodes))
		for _, n := range fc.Storage.Nodes {
			nodes = append(nodes, NodeRef{ID: n.ID, Addr: n.Addr})
		}
		cfg.StorageNodes = nodes
	}
	if fc.Vecstore.GRPCAddr != "" {
		cfg.VecstoreGRPCAddr = fc.Vecstore.GRPCAddr
	}
	if fc.Embed.ServiceAddr != "" {
		cfg.EmbedServiceAddr = fc.Embed.ServiceAddr
	}
	if fc.Logging.Level != "" {
		cfg.LogLevel = fc.Logging.Level
	}
	if fc.IndexManager.LRUCapacity != 0 {
		cfg.IndexLRUCapacity = fc.IndexManager.LRUCapacity
	}
	if fc.IndexManager.MemoryThresholdMB != 0 {
		cfg.IndexMemoryThresholdMB = int64(fc.IndexManager.MemoryThresholdMB)
	}
	if fc.IndexManager.CandidateN != 0 {
		cfg.IndexCandidateN = fc.IndexManager.CandidateN
	}
	if fc.IndexManager.LoadWaitTimeoutMS != 0 {
		cfg.IndexLoadWaitTimeout = time.Duration(fc.IndexManager.LoadWaitTimeoutMS) * time.Millisecond
	}
	if fc.IndexManager.CallbackMaxRetries != 0 {
		cfg.IndexCallbackMaxRetries = fc.IndexManager.CallbackMaxRetries
	}
	if fc.IndexManager.CallbackRetryBaseMS != 0 {
		cfg.IndexCallbackRetryBaseMS = fc.IndexManager.CallbackRetryBaseMS
	}
	if fc.IndexManager.ColdThresholdMS != 0 {
		cfg.IndexColdThreshold = time.Duration(fc.IndexManager.ColdThresholdMS) * time.Millisecond
	}
	// The retention shield is independent of the cold policy: it protects what
	// is still being read, not what has stopped being read.
	if fc.IndexManager.RetentionProtectWindowMS != 0 {
		cfg.IndexRetentionProtectWindow = time.Duration(fc.IndexManager.RetentionProtectWindowMS) * time.Millisecond
	}
	if fc.IndexManager.RetentionProtectMax != 0 {
		cfg.IndexRetentionProtectMax = fc.IndexManager.RetentionProtectMax
	}
	// The abandoned-artifact window is independent of the cold policy: a node
	// that never reshapes a version still leaves remains behind when a build
	// dies, and this sweeper is the only thing that reclaims them (§8.8).
	// Parsing it inside the cold-threshold branch — where it used to live —
	// meant that setting only build_abandon_timeout_ms silently did nothing.
	if ms := fc.IndexManager.BuildAbandonTimeoutMS; ms != 0 {
		cfg.IndexBuildAbandonTimeout = time.Duration(ms) * time.Millisecond
	}
	if fc.IndexManager.AppendMaxDeadRatio != 0 {
		cfg.IndexAppendMaxDeadRatio = fc.IndexManager.AppendMaxDeadRatio
	}
	if fc.IndexManager.GCRatioThreshold != 0 {
		cfg.IndexGCRatioThreshold = fc.IndexManager.GCRatioThreshold
	}
	// §3 codebook refresh. 0 means "unset" for both (neither default is 0), so
	// the != 0 guard reads correctly here too.
	if fc.IndexManager.MaxCodebookDriftRatio != 0 {
		cfg.IndexMaxCodebookDriftRatio = fc.IndexManager.MaxCodebookDriftRatio
	}
	if fc.IndexManager.MaxCodebookAppends != 0 {
		cfg.IndexMaxCodebookAppends = fc.IndexManager.MaxCodebookAppends
	}
	if fc.IndexManager.MinBaselineVectors != 0 {
		cfg.IndexMinCodebookBaselineVectors = fc.IndexManager.MinBaselineVectors
	}
	// §8.6(d). gc_enabled is a *bool so an ABSENT key keeps the default (ON since
	// 2026-10) while an explicit `false` still turns collection off. A plain bool
	// cannot tell those two apart, and its zero value would silently disable the
	// default for every config file that predates the change.
	if fc.IndexManager.GCEnabled != nil {
		cfg.IndexGCEnabled = *fc.IndexManager.GCEnabled
	}
	cfg.ReconcileDeletedVersions = fc.Reconcile.DeletedVersions
	cfg.ReconcileDeletedVersionsPeriodic = fc.Reconcile.DeletedVersionsPeriodic
	cfg.ReconcileDeletionsIncremental = fc.Reconcile.DeletedVersionsIncremental
	if fc.Reconcile.DeletedVersionsIncrementalIntervalS > 0 {
		cfg.ReconcileDeletionsIncrementalIntervalS = fc.Reconcile.DeletedVersionsIncrementalIntervalS
	}
	if fc.Reconcile.DeletedVersionsLogPage > 0 {
		cfg.ReconcileDeletionsLogPage = fc.Reconcile.DeletedVersionsLogPage
	}
	cfg.ReconcileDeletedVersionsIntervalS = fc.Reconcile.DeletedVersionsIntervalS
	if fc.IndexManager.ServingReplicaMin != 0 {
		cfg.IndexServingReplicaMin = fc.IndexManager.ServingReplicaMin
	}
	if fc.IndexManager.GraphRebuildRatio != 0 {
		cfg.IndexGCGraphRebuildRatio = fc.IndexManager.GraphRebuildRatio
	}
	// Negative is meaningful here (disable the scanner), so this one is applied
	// whenever it is set rather than guarded by != 0.
	if fc.IndexManager.GCSweepIntervalMS != 0 {
		cfg.IndexGCSweepInterval = time.Duration(fc.IndexManager.GCSweepIntervalMS) * time.Millisecond
	}
	if fc.IndexManager.BuildConcurrency != 0 {
		cfg.IndexBuildConcurrency = fc.IndexManager.BuildConcurrency
	}
	if fc.IndexManager.FetchConcurrency != 0 {
		cfg.IndexFetchConcurrency = fc.IndexManager.FetchConcurrency
	}
	if fc.WriteCoordinator.MaxRetries != 0 {
		cfg.WriteMaxRetries = fc.WriteCoordinator.MaxRetries
	}
	if fc.WriteCoordinator.RetryBaseIntervalMS != 0 {
		cfg.WriteRetryBaseMS = fc.WriteCoordinator.RetryBaseIntervalMS
	}
	if fc.DeleteCoordinator.MaxRetries != 0 {
		cfg.DeleteMaxRetries = fc.DeleteCoordinator.MaxRetries
	}
	if fc.DeleteCoordinator.RetryBaseIntervalMS != 0 {
		cfg.DeleteRetryBaseMS = fc.DeleteCoordinator.RetryBaseIntervalMS
	}
	if fc.BloomFilter.ExpectedItems != 0 {
		cfg.BloomExpectedItems = uint(fc.BloomFilter.ExpectedItems)
	}
	if fc.BloomFilter.FalsePositiveRate != 0 {
		cfg.BloomFalsePositiveRate = fc.BloomFilter.FalsePositiveRate
	}
	if fc.GC.VersionRetentionCount != 0 {
		cfg.IndexRetentionCount = fc.GC.VersionRetentionCount
	}
	if fc.GC.SweepIntervalSec != 0 {
		cfg.GCSweepIntervalSec = fc.GC.SweepIntervalSec
	}

	// lag_catchup.*. Each field keeps its own "unset" meaning (<= 0 for the bounds,
	// 1 for the lag); there is no switch to read.
	if fc.LagCatchup.MinLagVersions != 0 {
		cfg.LagCatchupMinLagVersions = fc.LagCatchup.MinLagVersions
	}
	if fc.LagCatchup.JitterMS != 0 {
		cfg.LagCatchupJitterMS = fc.LagCatchup.JitterMS
	}
	if fc.LagCatchup.MaxConcurrentKBs != 0 {
		cfg.LagCatchupMaxConcurrentKBs = fc.LagCatchup.MaxConcurrentKBs
	}
	applyMS(&cfg.LagCatchupTimeout, fc.LagCatchup.TimeoutMS)
	applyMS(&cfg.LagCatchupHoldersRefresh, fc.LagCatchup.HoldersRefreshIntervalMS)

	// --- Tuning knobs (see appConfig's comment on the group). ---
	//
	// One helper per unit, each applying only a NON-ZERO value: an absent key must
	// keep the component's own default, and 0 in these units is not a sensible
	// bound. Negative values pass through, so the knobs whose "off" is negative
	// (e.g. index_manager.build_abandon_timeout_ms) keep working.
	applyMS(&cfg.StationMarkTTL, fc.Node.StationMarkTTLMS)
	applyMS(&cfg.RaftRPCTimeout, fc.Raft.RPCTimeoutMS)
	applyMS(&cfg.RaftProposeApplyTimeout, fc.Raft.ProposeApplyTimeoutMS)
	applyMS(&cfg.VecstoreKeepaliveInterval, fc.Vecstore.KeepaliveIntervalMS)
	applyMS(&cfg.VecstoreKeepaliveTimeout, fc.Vecstore.KeepaliveTimeoutMS)
	applyMS(&cfg.EmbedTimeout, fc.Embed.TimeoutMS)

	applyMS(&cfg.PullIdleTimeout, fc.DataPlane.PullIdleTimeoutMS)
	applyMS(&cfg.PullMaxDuration, fc.DataPlane.PullMaxDurationMS)
	applyInt(&cfg.MaxInFlightWrites, fc.DataPlane.MaxInFlightWrites)
	applyMS(&cfg.CursorWriteTimeout, fc.DataPlane.CursorWriteTimeoutMS)
	applyMS(&cfg.CursorRetryDelay, fc.DataPlane.CursorRetryDelayMS)
	applyMS(&cfg.ConfirmAttemptTimeout, fc.DataPlane.ConfirmAttemptTimeoutMS)
	applyMS(&cfg.ConfirmRetryBackoff, fc.DataPlane.ConfirmRetryBackoffMS)
	applyMS(&cfg.ConfirmBudget, fc.DataPlane.ConfirmBudgetMS)
	applyMS(&cfg.ReplicaPushTimeout, fc.DataPlane.ReplicaPushTimeoutMS)
	applyMS(&cfg.PushTimeoutPerDoc, fc.DataPlane.PushTimeoutPerDocMS)
	applyMS(&cfg.PeerCursorTimeout, fc.DataPlane.PeerCursorTimeoutMS)
	applyMS(&cfg.TakeoverTimeout, fc.DataPlane.TakeoverTimeoutMS)
	applyMS(&cfg.TakeoverCheckTimeout, fc.DataPlane.TakeoverCheckTimeoutMS)
	applyInt(&cfg.TakeoverMaxAttempts, fc.DataPlane.TakeoverMaxAttempts)
	applyMS(&cfg.TakeoverMaxRetryDelay, fc.DataPlane.TakeoverMaxRetryDelayMS)
	applyMS(&cfg.TerminalReclaimInterval, fc.DataPlane.TerminalReclaimIntervalMS)
	applyInt(&cfg.TerminalReclaimAttempts, fc.DataPlane.TerminalReclaimAttempts)

	applyMS(&cfg.SyncHeartbeatInterval, fc.Sync.HeartbeatIntervalMS)
	applyMS(&cfg.PullDialTimeout, fc.Sync.PullDialTimeoutMS)
	applyMS(&cfg.HoldersCacheTTL, fc.Sync.HoldersCacheTTLMS)
	applyInt(&cfg.HoldersCacheLimit, fc.Sync.HoldersCacheLimit)

	applyMS(&cfg.CandidateTimeout, fc.Dispatcher.CandidateTimeoutMS)
	applyMS(&cfg.CandidateTimeoutPerDoc, fc.Dispatcher.CandidateTimeoutPerDocMS)
	applyMS(&cfg.MaxCandidateTimeout, fc.Dispatcher.MaxCandidateTimeoutMS)

	applyInt(&cfg.HealthDemoteAfter, fc.Health.DemoteAfter)
	applyMS(&cfg.HealthDemoteFor, fc.Health.DemoteForMS)

	applyMS(&cfg.ReconcileChainMinAge, fc.Reconcile.ChainReconcileMinAgeMS)
	applyInt(&cfg.ReconcileDeletionPages, fc.Reconcile.DeletionPagesPerSweep)
	applyMS(&cfg.DeletedVersionRetryInterval, fc.Reconcile.DeletedVersionRetryIntervalMS)
	applyMS(&cfg.TombstoneGrace, fc.Reconcile.TombstoneGraceMS)

	applyInt(&cfg.IndexBruteForceMaxChunks, fc.IndexManager.BruteForceMaxChunks)
	applyMS(&cfg.IndexBuildAbandonSweepInterval, fc.IndexManager.BuildAbandonSweepIntervalMS)
	applyMS(&cfg.IndexBuildRetryTimeout, fc.IndexManager.BuildRetryTimeoutMS)
	applyMS(&cfg.IndexBuildRetryInterval, fc.IndexManager.BuildRetryIntervalMS)
	applyMS(&cfg.IndexAccessPersistInterval, fc.IndexManager.AccessPersistIntervalMS)

	applyMS(&cfg.WriteDispatchTimeout, fc.WriteCoordinator.DispatchTimeoutMS)
	applyMS(&cfg.WriteDispatchClaimWindow, fc.WriteCoordinator.DispatchClaimWindowMS)
	applyMS(&cfg.WriteAbandonReportTimeout, fc.WriteCoordinator.AbandonReportTimeoutMS)
	applyInt(&cfg.WriteMaxConcurrentDocs, fc.WriteCoordinator.MaxConcurrentDocumentWrites)

	applyMS(&cfg.DeletingVersionSweepInterval, fc.DeleteCoordinator.DeletingVersionSweepIntervalMS)
	applyMS(&cfg.DeletingKBSweepInterval, fc.DeleteCoordinator.DeletingKBSweepIntervalMS)

	applySec(&cfg.GCWALReclaimInterval, fc.GC.WALReclaimIntervalSec)

	applyInt(&cfg.CrashReplayMaxAttempts, fc.Recovery.CrashReplayMaxAttempts)
	applyInt(&cfg.CrashReplayRetryBaseMS, fc.Recovery.CrashReplayRetryBaseMS)
	applyMS(&cfg.EpochReportRetryInterval, fc.Recovery.EpochReportRetryIntervalMS)
	applyMS(&cfg.EpochReportRetryWindow, fc.Recovery.EpochReportRetryWindowMS)
	applyMS(&cfg.ShutdownTimeout, fc.Recovery.ShutdownTimeoutMS)

	return cfg, nil
}

// applyMS / applySec / applyInt overlay a config-file value onto a default,
// skipping only the "unset" zero. They exist so the tuning-knob block above stays
// one line per knob — and so no knob can quietly grow its own idea of what an
// unset key means.
func applyMS(dst *time.Duration, ms int) {
	if ms != 0 {
		*dst = time.Duration(ms) * time.Millisecond
	}
}

func applySec(dst *time.Duration, sec int) {
	if sec != 0 {
		*dst = time.Duration(sec) * time.Second
	}
}

func applyInt(dst *int, v int) {
	if v != 0 {
		*dst = v
	}
}

// defaultConfig returns hardcoded defaults matching configs/config1.yaml.
// A YAML config file (loadConfig) overlays these defaults.
func defaultConfig() appConfig {
	return appConfig{
		NodeID: 1,
		// Role defaults to the all-in-one shape: a node that has not been told
		// which half it plays keeps running both, so every existing deployment
		// and test binary is unaffected by the roles existing.
		Role:     NodeRoleAll,
		DataDir:  "/var/lib/stratum/node1",
		GRPCAddr: "0.0.0.0:7000",
		RaftAddr: "0.0.0.0:8000",
		Peers: []raft.PeerConfig{
			{ID: 1, RaftAddr: "127.0.0.1:8000", ServiceAddr: "127.0.0.1:7000"},
		},

		HeartbeatInterval:  200 * time.Millisecond,
		ElectionTimeoutMin: 2000 * time.Millisecond,
		ElectionTimeoutMax: 4000 * time.Millisecond,

		VecstoreGRPCAddr: "127.0.0.1:7100",
		EmbedServiceAddr: "http://localhost:8080",

		IndexLRUCapacity:         16,
		IndexLoadWaitTimeout:     5 * time.Second,
		IndexCallbackMaxRetries:  3,
		IndexCallbackRetryBaseMS: 200,

		LogLevel: "info",

		// Disk retention and memory thresholds default to disabled (0).
		// They activate only when the YAML config sets
		// gc.version_retention_count / index_manager.memory_threshold_mb.
		IndexRetentionCount:    0,
		IndexMemoryThresholdMB: 0,

		// §8.6a cold-version policy: off by default, so an unconfigured node
		// builds a full HNSW graph for every version. Enable with
		// index_manager.cold_threshold_ms. There is no sweep interval to set —
		// the shape is decided inside a build, not by a background evaluator.
		IndexColdThreshold: 0,

		// §8.6(c) pure-append reuse: 0 means the IndexManager's own default
		// ratio (20% dead vectors). Set append_max_dead_ratio to 1.0 to never
		// let dead weight force a rebuild.
		IndexAppendMaxDeadRatio: 0,

		// §3 codebook refresh: 0 means the IndexManager's own defaults (drift
		// ratio 0.25, 50 appends since training). Only a KB whose quantizer
		// learns a codebook (SQ8 / PQ) can trigger it.
		IndexMaxCodebookDriftRatio: 0,
		IndexMaxCodebookAppends:    0,
		// 0 => the IndexManager's floor (1000); negative removes it.
		IndexMinCodebookBaselineVectors: 0,

		// §8.6(d) collection: ON since 2026-10, gated twice — serving_replica_min
		// (2) says how many OTHER replicas must be serving before this node may step
		// out, and gc_ratio_threshold (0.3, deliberately above
		// append_max_dead_ratio's 0.2) says how much dead weight is enough to
		// bother. The scanner always runs and reports regardless, so a node that
		// turns collection off still says when it would have been worth it. Write
		// `gc_enabled: false` for the pre-2026-10 behaviour.
		IndexGCEnabled:           true,
		ReconcileDeletedVersions: true,
		// On by default too — see the field comments for why this pair differs from
		// every other irreversibility switch in this file.
		ReconcileDeletedVersionsPeriodic: true,
		ReconcileDeletionsIncremental:    true,
		// 0 => the plane's default interval (5 minutes).
		ReconcileDeletedVersionsIntervalS: 0,
		IndexServingReplicaMin:            0,
		IndexGCGraphRebuildRatio:          0,
		IndexGCSweepInterval:              0,
		IndexBuildConcurrency:             0,

		WriteMaxRetries:   3,
		WriteRetryBaseMS:  100,
		DeleteMaxRetries:  5,
		DeleteRetryBaseMS: 500,

		// Bloom filters: sized for ~1M keys at 1% false-positive rate,
		// matching the bloom_filter section in configs/config1.yaml.
		BloomExpectedItems:     1_000_000,
		BloomFalsePositiveRate: 0.01,

		GCSweepIntervalSec: 3600,

		// The tuning knobs added with the config cleanup keep 0 here unless the
		// value is used by THIS file rather than by a component that owns its own
		// default. Those few follow; everything else stays 0 = "the component's
		// default", so the numbers live in exactly one place.
		EmbedTimeout:             30 * time.Second,
		CrashReplayMaxAttempts:   crashReplayMaxAttempts,
		CrashReplayRetryBaseMS:   crashReplayRetryBaseMillis,
		EpochReportRetryInterval: epochReportRetryInterval,
		EpochReportRetryWindow:   epochReportRetryWindow,
		ShutdownTimeout:          2 * time.Second,
	}
}

// reclaimDiagnosticsSource adapts the data plane's reclaim diagnosis
// (plane.ReclaimBlocker) to the shape the admin service reports
// (service.ReclaimBlockedReplica). It is bridged here because service does not import
// plane, the same reason storageGateSource exists.
type reclaimDiagnosticsSource struct {
	dp *plane.LocalDataPlane
}

// ReclaimBlockedReplicas reports which replicas kept kbID's reclaim watermark unavailable
// on this node's last pass. ok=false means no pass has judged anything yet: the admin
// service reports that as "judgement unavailable", never as "nothing is blocked".
func (s reclaimDiagnosticsSource) ReclaimBlockedReplicas(kbID string) ([]service.ReclaimBlockedReplica, bool) {
	blockers, ok := s.dp.ReclaimBlockers(kbID)
	if !ok {
		return nil, false
	}
	out := make([]service.ReclaimBlockedReplica, 0, len(blockers))
	for _, b := range blockers {
		out = append(out, service.ReclaimBlockedReplica{Address: b.Address, Reason: b.Reason})
	}
	return out, true
}

// storageGateSource adapts the control plane's redundancy verdict to the shape the
// service layer asks for. It exists to keep the dependency pointing one way: service does
// not import plane's state enum, and both service callers that need the verdict — the
// CreateVersion gate and HealthCheck — take this one adapter.
type storageGateSource struct {
	cp *plane.LocalControlPlane
}

// StorageDegraded reports whether the storage layer is short of a quorum for
// kbID, with a real replica still answering.
//
// ok=false means the verdict is unavailable — this node does not lead, or its
// replica topology is not wired — and every caller treats that as "allow". See
// service.StorageDegradationSource: the verdict is soft state, so it may only
// ever cost a retry, never a refused write that would otherwise have succeeded.
func (s storageGateSource) StorageDegraded(kbID string) (bool, string, bool) {
	return s.cp.StorageDegraded(kbID)
}

// StorageUnavailable is the other tier: not one required replica is live. The two
// are separate methods because service declares its own narrow interface rather
// than importing plane's state enum, and the pair of answers is what decides which
// sentinel a refusal carries (§4.1).
func (s storageGateSource) StorageUnavailable(kbID string) (bool, string, bool) {
	return s.cp.StorageUnavailable(kbID)
}

// pendingDispatchRegistry is the part of the write coordinator the §7.13.2
// hand-off uses: the once-only take of a write's changes, the question that tells
// a lost race from an orphaned version, and the give-up path it deliberately no
// longer calls on a miss.
type pendingDispatchRegistry interface {
	TakePendingDispatch(kbID, clientRequestID string) (int64, []types.DocChange, bool)
	DispatchClaimed(kbID, clientRequestID string) bool
	AbandonDispatch(ctx context.Context, kbID string, versionID int64, class types.FailureClass, detail string)
}

// versionWriteDispatcher is the one call the hand-off makes on the dispatcher,
// declared narrowly so a test can stand in for a live one.
type versionWriteDispatcher interface {
	Dispatch(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) (string, error)
}

// dispatchCommittedVersion is the §7.13.2 hand-off, extracted from the apply
// callback so the case Step 0 is about can be tested without a live cluster
// (docs/await-version-plan.md §4.3, §7 Step 0).
//
// The changes live in THIS process's memory — the Raft command deliberately
// carries none (§7.7) — so a missing TakePendingDispatch means one of two
// things. The registry tells them apart (DispatchClaimed):
//
//   - the background dispatch already took them (Execute dispatches the moment
//     the version is committed; Take is once-only), so the write is on its way.
//     This is a DESIGNED outcome, not a fault: the hook and Execute race, and
//     whichever arrives first wins. Logged at debug.
//   - nobody holds them at all: the process that proposed them is gone (a
//     restarted or deposed leader). Nothing is in flight, and this is the case
//     worth a warning.
//
// Before DispatchClaimed existed the hook could not tell the two apart, so it
// warned on both — and since the first cause happens on every write, the warning
// was noise that hid the second. The distinction is what makes the message mean
// something.
//
// It used to report a transient failure on the miss, and that was wrong twice
// over: an in-memory miss on one node proves neither case, so the retry budget
// could spend a version whose write was in fact in flight; and declaring a
// version dead is the caller's decision, not this node's. What settles such a
// version is the DATA-side existence probe (the same one GetSystemStatus uses),
// which the await path exposes as data_missing, followed by the caller either
// re-sending the changes or discarding the version. Until then it stays
// PENDING — which is the truth for the orphaned case.
func dispatchCommittedVersion(
	registry pendingDispatchRegistry,
	dispatcher versionWriteDispatcher,
	logger *zap.Logger,
	kbID string,
	versionID, parentVersionID int64,
	clientRequestID string,
) {
	regParentID, changes, ok := registry.TakePendingDispatch(kbID, clientRequestID)
	if !ok {
		// A miss has two causes and only one of them is worth a warning. When the
		// proposer's own background dispatch took the registration, the write is
		// already being handled: this hook lost a race it is DESIGNED to lose
		// sometimes, because Execute dispatches as soon as the version is
		// committed and whichever side arrives first wins. Warning there fired on
		// every single write, which buried the cause that matters.
		//
		// That other cause — nobody holds the changes, because the process that
		// proposed them is gone — is what actually leaves a version PENDING with
		// no owner. It is the one worth saying out loud.
		if registry.DispatchClaimed(kbID, clientRequestID) {
			logger.Debug("version committed here; the proposer's dispatch is already in flight",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.String("client_request_id", clientRequestID))
			return
		}
		logger.Warn("version committed here without a pending dispatch; leaving it PENDING",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.String("client_request_id", clientRequestID))
		return
	}
	if regParentID != 0 {
		parentVersionID = regParentID
	}
	// Its own context: this runs from the apply loop's callback, so it must not
	// be cancelled by whatever request happened to carry the write in.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := dispatcher.Dispatch(ctx, kbID, versionID, parentVersionID, changes); err != nil {
		logger.Warn("write dispatch failed; the client's retry or the retry budget takes over",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
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
