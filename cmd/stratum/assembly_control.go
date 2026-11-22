package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"stratum/internal/coordinator"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/nodestate"
	"stratum/internal/plane"
	"stratum/internal/raft"
	"stratum/internal/rpcpool"
	"stratum/internal/storage"
	stratumsync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// nodeAssembly holds the assembled control, coordination, data-plane, and
// optional storage components of a Stratum node in dependency order.
type nodeAssembly struct {
	cfg       appConfig
	logger    *zap.Logger
	ctx       context.Context
	walImpl   *wal.FileWAL
	rn        raft.RaftNode
	raftNode  *raft.RaftNodeImpl
	applyGate *raftApplyGate
	peerPool  *rpcpool.Pool

	stor *storageAssembly

	livenessRegistry *plane.LivenessRegistry
	livenessGate     *plane.LeaderGate
	controlPlane     *plane.LocalControlPlane
	topology         *clusterTopology

	gcImpl               *coordinator.ChunkGarbageCollectorImpl
	writeCoord           *coordinator.WriteCoordinatorImpl
	dataSources          *plane.DataSourceRegistry
	deleteCoord          *coordinator.DeleteCoordinatorImpl
	deleteVersionCoord   *coordinator.DeleteVersionCoordinatorImpl
	holdersCache         *plane.HoldersCache
	dataPlane            *plane.LocalDataPlane
	writeDispatcher      *plane.CoordinatorDispatcher
	dispatchVersionWrite func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error
	resolveReplicaAddrs  func(ctx context.Context) ([]string, error)
	liveness             plane.VersionLivenessLister
}

// controlContext holds the control plane, cluster topology, and liveness gate
// shared across the node assembly. None of these depend on local storage, so
// they are constructed first and handed into openStorageAssembly and assembleNode.
type controlContext struct {
	livenessRegistry *plane.LivenessRegistry
	livenessGate     *plane.LeaderGate
	controlPlane     *plane.LocalControlPlane
	topology         *clusterTopology
}

func buildControlContext(cfg appConfig, logger *zap.Logger, rn raft.RaftNode, topology *clusterTopology, storageSilence time.Duration) *controlContext {
	livenessRegistry := plane.NewLivenessRegistry()
	livenessGate := plane.NewLeaderGate(rn.IsLeader, livenessRegistry.Reset)

	controlPlane := plane.NewLocalControlPlane(rn,
		plane.WithControlLogger(logger),
		plane.WithFailureBudget(cfg.ControlPlaneFailureBudget),
		plane.WithNodeID(cfg.NodeID),
		plane.WithKBPlacement(rn.(storage.Reader).GetKBPlacement),
		plane.WithLivenessView(livenessRegistry, livenessGate),
		plane.WithRequiredReplicas(topology.requiredReplicaIDs),
		plane.WithStorageSilenceWindow(storageSilence),
	)

	return &controlContext{
		livenessRegistry: livenessRegistry,
		livenessGate:     livenessGate,
		controlPlane:     controlPlane,
		topology:         topology,
	}
}

// assembleNode wires coordinators, data plane, and (when stor is non-nil) local
// storage components into a single cohesive nodeAssembly. Role=all naturally
// composes both the storage assembly and the Raft control wiring without
// interleaved role conditionals.
func assembleNode(
	ctx context.Context,
	cfg appConfig,
	logger *zap.Logger,
	walImpl *wal.FileWAL,
	rn raft.RaftNode,
	raftNode *raft.RaftNodeImpl,
	applyGate *raftApplyGate,
	ctrl *controlContext,
	stor *storageAssembly,
	peerPool *rpcpool.Pool,
) *nodeAssembly {
	stor.wireControlCallbacks(ctrl.controlPlane, rn, cfg)

	a := &nodeAssembly{
		cfg:              cfg,
		logger:           logger,
		ctx:              ctx,
		walImpl:          walImpl,
		rn:               rn,
		raftNode:         raftNode,
		applyGate:        applyGate,
		peerPool:         peerPool,
		stor:             stor,
		livenessRegistry: ctrl.livenessRegistry,
		livenessGate:     ctrl.livenessGate,
		controlPlane:     ctrl.controlPlane,
		topology:         ctrl.topology,
	}

	a.wireNodeComponents()

	return a
}

// wireNodeComponents builds the local data plane first (using lateWriteCoordBridge
// for execution/dropper), constructs WriteCoordinatorImpl directly with that
// DataPlane, binds the bridge, builds delete coordinators and dispatcher, and
// starts storage GC/retry loops.
func (a *nodeAssembly) wireNodeComponents() {
	cfg, logger, ctx := a.cfg, a.logger, a.ctx
	rn, walImpl := a.rn, a.walImpl
	controlPlane, topology := a.controlPlane, a.topology
	stor := a.stor
	h := stor.coordinatorHandles()

	kbLocks := coordinator.NewKBLockSet()
	dataSources := plane.NewDataSourceRegistry()
	holdersCache := plane.NewHoldersCache(plane.HoldersCacheConfig{
		TTL:    cfg.HoldersCacheTTL,
		Limit:  cfg.HoldersCacheLimit,
		Logger: logger,
	})

	var liveness plane.VersionLivenessLister
	if lv, ok := rn.(plane.VersionLivenessLister); ok {
		liveness = lv
	}

	stor.initSyncHalves(cfg, rn)

	var (
		executionGate *storage.ExecutionGate
		nodeState     *nodestate.Machine
		syncFollower  *stratumsync.Follower
		syncPusher    *stratumsync.Pusher
	)
	if stor != nil {
		executionGate = stor.executionGate
		nodeState = stor.nodeState
		syncFollower = stor.syncFollower
		syncPusher = stor.syncPusher
	}

	lateBridge := &lateWriteCoordBridge{}
	dataPlane := plane.NewLocalDataPlane(dataPlaneDeps{
		executionGate:     executionGate,
		cfg:               cfg,
		logger:            logger,
		rn:                rn,
		controlPlane:      controlPlane,
		indexMgr:          h.concreteIndexMgr,
		vd:                h.concreteVD,
		ds:                h.concreteDocStore,
		cdm:               h.concreteCDM,
		chunkStore:        h.concreteChunk,
		versionViews:      stor.versionViewsIfNonNil(),
		walImpl:           walImpl,
		executor:          lateBridge,
		dataDropper:       lateBridge,
		versionTombstones: h.versionTombstones,
		syncFollower:      syncFollower,
		syncPusher:        syncPusher,
		dataSources:       dataSources,
		holdersCache:      holdersCache,
		topology:          topology,
		nodeState:         nodeState,
		liveness:          liveness,
		peerPool:          a.peerPool,
	}.config())

	gcImpl := coordinator.NewChunkGarbageCollectorImpl(coordinator.ChunkGarbageCollectorConfig{
		SweepIntervalSec:     cfg.GCSweepIntervalSec,
		Locks:                kbLocks,
		RaftNode:             rn,
		ChunkDocMapper:       h.chunkDocMapper,
		DocStore:             h.docStore,
		ChunkStore:           h.chunkStore,
		VersionDocList:       h.versionDocList,
		ServiceLease:         stor.serviceLeaseIfNonNil(),
		StorageMutationGuard: stor.storageMutationGuard(cfg, rn),
		Logger:               logger.Named("gc"),
	})

	writeCoord := coordinator.NewWriteCoordinatorImpl(coordinator.WriteCoordinatorConfig{
		MaxRetries:                  cfg.WriteMaxRetries,
		RetryBaseIntervalMS:         cfg.WriteRetryBaseMS,
		DispatchTimeout:             cfg.WriteDispatchTimeout,
		DispatchClaimWindow:         cfg.WriteDispatchClaimWindow,
		AbandonReportTimeout:        cfg.WriteAbandonReportTimeout,
		MaxConcurrentDocumentWrites: cfg.WriteMaxConcurrentDocs,
		Locks:                       kbLocks,
		WAL:                         walImpl,
		RaftNode:                    rn,
		DataPlane:                   dataPlane,
		Splitter:                    h.chunkSplitter,
		EmbedClient:                 h.embedClient,
		ChunkBloom:                  h.chunkBloom,
		VersionViews:                h.versionViews,
		ChunkStore:                  h.chunkStore,
		ChunkDocMapper:              h.chunkDocMapper,
		DocStore:                    h.docStore,
		VersionDocList:              h.versionDocList,
		IndexManager:                h.indexMgr,
		ChunkReclaimer:              gcImpl,
		Dispatch: func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
			if a.dispatchVersionWrite == nil {
				return fmt.Errorf("stratum: write dispatch is not wired")
			}
			return a.dispatchVersionWrite(ctx, kbID, versionID, parentVersionID, changes)
		},
		Logger:       logger,
		ControlPlane: controlPlane,
	})
	lateBridge.coord = writeCoord

	stor.startGarbageCollector(ctx, gcImpl)
	stor.bindDataPlane(ctx, cfg, dataPlane)

	kbDropper := coordinator.NewDeferredKBDropper(func() coordinator.KBStorageDropper {
		droppers := coordinator.KBStorageDroppers{
			coordinator.BroadcastKBDropper{Metadata: rn, Dropper: dataPlane},
		}
		droppers = append(droppers, stor.localKBDroppers()...)
		return droppers
	})

	deleteCoord := coordinator.NewDeleteCoordinatorImpl(coordinator.DeleteCoordinatorConfig{
		MaxRetries:          cfg.DeleteMaxRetries,
		RetryBaseIntervalMS: cfg.DeleteRetryBaseMS,
		WAL:                 walImpl,
		RaftNode:            rn,
		IndexManager:        h.concreteIndexMgr,
		DocStore:            h.concreteDocStore,
		ChunkStore:          h.concreteChunk,
		ChunkDocMapper:      h.concreteCDM,
		VersionDocList:      h.concreteVD,
		VersionViews:        h.versionViews,
		Dropper:             kbDropper,
		SourceAnnouncements: dataSources,
	})

	versionDropper := coordinator.NewDeferredVersionDropper(func() coordinator.VersionStorageDropper {
		droppers := coordinator.VersionStorageDroppers{
			coordinator.BroadcastVersionDropper{Dropper: dataPlane},
		}
		droppers = append(droppers, stor.localVersionDroppers(rn, gcImpl)...)
		return droppers
	})

	deleteVersionCoord := coordinator.NewDeleteVersionCoordinatorImpl(coordinator.DeleteVersionCoordinatorConfig{
		MaxRetries:          cfg.DeleteMaxRetries,
		RetryBaseIntervalMS: cfg.DeleteRetryBaseMS,
		WAL:                 walImpl,
		RaftNode:            rn,
		IndexManager:        h.concreteIndexMgr,
		DocStore:            h.concreteDocStore,
		VersionDocList:      h.concreteVD,
		VersionViews:        h.versionViews,
		Dropper:             versionDropper,
		Tombstones:          tombstoneRecorder(h.versionTombstones),
	})

	resolveReplicaAddrs := topology.resolveReplicaAddrs
	dispatcherCfg := plane.CoordinatorDispatcherConfig{
		Placement:              rn.(storage.Reader).GetKBPlacement,
		Replicas:               resolveReplicaAddrs,
		SelfAddr:               topology.localAddr,
		Pool:                   a.peerPool,
		Logger:                 logger,
		CandidateTimeout:       cfg.CandidateTimeout,
		CandidateTimeoutPerDoc: cfg.CandidateTimeoutPerDoc,
		MaxCandidateTimeout:    cfg.MaxCandidateTimeout,
		Health: plane.NewLocalHealthView(plane.HealthViewConfig{
			DemoteAfter: cfg.HealthDemoteAfter,
			DemoteFor:   cfg.HealthDemoteFor,
		}),
	}
	stor.configureDispatcher(&dispatcherCfg, dataPlane)

	writeDispatcher := plane.NewCoordinatorDispatcher(dispatcherCfg)
	dispatchVersionWrite := func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
		_, err := writeDispatcher.Dispatch(ctx, kbID, versionID, parentVersionID, changes)
		return err
	}

	a.gcImpl, a.writeCoord = gcImpl, writeCoord
	a.dataSources, a.deleteCoord = dataSources, deleteCoord
	a.deleteVersionCoord = deleteVersionCoord
	a.holdersCache = holdersCache
	a.dataPlane, a.writeDispatcher = dataPlane, writeDispatcher
	a.dispatchVersionWrite, a.resolveReplicaAddrs = dispatchVersionWrite, resolveReplicaAddrs
	a.liveness = liveness
}

// startupAndHooks runs storage recovery (for storage-capable nodes), activates
// the Raft apply hooks wired into NewRaftNodeImpl, and starts Raft control-plane
// background sweeps (for control-capable nodes).
func (a *nodeAssembly) startupAndHooks(records []types.PendingRecord) []plane.VersionRef {
	var epochDurable []plane.VersionRef
	if a.stor != nil {
		epochDurable = recoverStorageStartup(
			a.ctx, a.cfg, a.logger, records, a.rn, a.controlPlane,
			a.stor.nodeState, a.walImpl, a.writeCoord, a.deleteCoord,
			a.deleteVersionCoord, a.dataPlane, a.liveness,
		)
	}
	var (
		onCreated           func(kbID string, versionID int64)
		onFailed            func(kbID string, versionID int64)
		onCommittedAsLeader func(kbID string, versionID, parentVersionID int64, clientRequestID string)
	)
	if a.stor != nil && a.raftNode != nil {
		onFailed = a.dataPlane.NoteTerminalVersion
		onCreated = newHybridOnVersionCreated(a.ctx, a.logger, a.dataPlane)
	}
	if a.raftNode != nil {
		onCommittedAsLeader = func(kbID string, versionID, parentVersionID int64, clientRequestID string) {
			dispatchCommittedVersion(a.writeCoord, a.writeDispatcher, a.logger, kbID, versionID, parentVersionID, clientRequestID)
		}
	}
	if a.applyGate != nil {
		a.applyGate.activate(onCreated, onFailed, onCommittedAsLeader)
	}
	if a.raftNode != nil {
		startControlRaftLoops(a.ctx, a.cfg, a.logger, a.topology, a.raftNode,
			a.deleteCoord, a.deleteVersionCoord, a.peerPool)
	}
	return epochDurable
}

// newHybridOnVersionCreated returns the Raft apply hook that replicates newly
// created versions when the local process runs both Raft and local storage (role=all).
func newHybridOnVersionCreated(
	ctx context.Context,
	logger *zap.Logger,
	dataPlane *plane.LocalDataPlane,
) func(kbID string, versionID int64) {
	fetchSlots := make(chan struct{}, maxConcurrentVersionFetches)

	return func(kbID string, versionID int64) {
		owned, err := dataPlane.OwnsKnowledgeBase(ctx, kbID)
		if err != nil {
			logger.Debug("placement unavailable; chain-tail catch-up will retry", zap.String("kb_id", kbID), zap.Error(err))
			return
		}
		if !owned {
			return
		}
		select {
		case fetchSlots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		defer func() { <-fetchSlots }()

		dataPlane.AnnounceVersion(kbID, versionID)
		if err := dataPlane.FetchVersionData(context.Background(), kbID, versionID); err != nil {
			logger.Error("sync: version data fetch did not converge",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		}
	}
}

// startControlRaftLoops launches control-plane background loops (migration
// scheduler, directory bootstrapper, and interrupted-deletion sweepers) on
// nodes that participate in Raft.
func startControlRaftLoops(
	ctx context.Context,
	cfg appConfig,
	logger *zap.Logger,
	topology *clusterTopology,
	raftNode *raft.RaftNodeImpl,
	deleteCoord *coordinator.DeleteCoordinatorImpl,
	deleteVersionCoord *coordinator.DeleteVersionCoordinatorImpl,
	peerPool *rpcpool.Pool,
) {

	go func() {
		client := &stratumsync.FenceClient{Pool: peerPool}
		defer client.Close()
		(&storage.FenceScheduler{
			Manager:  raftNode,
			IsLeader: raftNode.IsLeader,
			Versions: raftNode.ListVersions,
			Fence:    client.Fence,
			Stage:    client.Stage,
			Install:  client.Install,
			Serve:    client.PrepareServing,
			Activate: client.Activate,
			Cleanup:  client.Cleanup,
			Revoke: func(c context.Context, nodeID int64, block storage.ServiceBlock) error {
				return raftNode.WaitStorageServiceRevoked(c, storage.ServiceRequest{
					NodeID: nodeID, OperationID: block.OperationID, BlockRevision: block.Revision,
				})
			},
			Logger: logger.Named("migration"),
		}).Run(ctx)
	}()

	go func() {
		nodes, err := topology.directoryNodes()
		if err != nil {
			logger.Error("storage directory bootstrap refused", zap.Error(err))
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			if raftNode.IsLeader() {
				attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
				err := storage.Initialize(attempt, raftNode, nodes)
				cancel()
				if err == nil {
					logger.Info("storage directory initialized")
					return
				}
				if errors.Is(err, stratumerrors.ErrPlacementChangeUnsafe) {
					logger.Error("storage directory differs from deployment; topology migration is required", zap.Error(err))
					return
				}
				logger.Warn("storage directory bootstrap will retry", zap.Error(err))
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	raftNode.MarkAllDeletingDirty()
	go coordinator.NewDeletingVersionSweeper(raftNode, raftNode, deleteVersionCoord, cfg.DeletingVersionSweepInterval, logger).Run(ctx)
	go coordinator.NewDeletingKBSweeper(raftNode, deleteCoord, cfg.DeletingKBSweepInterval, logger).Run(ctx)
}
