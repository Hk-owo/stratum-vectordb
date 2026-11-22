package main

import (
	"context"
	"errors"
	"path/filepath"

	"go.uber.org/zap"

	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/coordinator"
	"stratum/internal/docstore"
	"stratum/internal/embed"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/index"
	"stratum/internal/nodestate"
	"stratum/internal/plane"
	"stratum/internal/raft"
	"stratum/internal/rpcpool"
	"stratum/internal/splitter"
	"stratum/internal/storage"
	stratumsync "stratum/internal/sync"
	"stratum/internal/tombstone"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/versionview"
)

// storageAssembly holds the local storage stack, sync endpoints, state machine,
// and lease owned by storage-capable roles (NodeRoleStorage and NodeRoleAll).
// A nil *storageAssembly represents a metadata-only NodeRoleControl process and
// is safe to query on all helper accessors.
type storageAssembly struct {
	executionGate *storage.ExecutionGate
	nodeState     *nodestate.Machine
	stack         *storageStack

	ds                *docstore.PebbleDocStore
	cdm               *chunkdoc.PebbleChunkDocMapper
	vd                *versiondoc.PebbleVersionDocList
	chunkStore        *chunkstore.VecstoreChunkStore
	versionViews      *versionview.Store
	chunkBloom        *bloom.BitsAndBloomsFilter
	chunkSplitter     splitter.ChunkSplitter
	indexMgr          *index.IndexManagerImpl
	embedClient       *embed.HTTPEmbedClient
	versionTombstones *tombstone.PebbleStore

	syncLeader       *stratumsync.LeaderHandler
	syncFollower     *stratumsync.Follower
	syncPusher       *stratumsync.Pusher
	serviceLease     *storage.ServiceLease
	localAdvancer    *lateVersionAdvancer
	chainTailsSource func() map[string]int64
	peerPool         *rpcpool.Pool
}

type lateVersionAdvancer struct {
	fn func(kbID string, versionID int64)
}

func (a *lateVersionAdvancer) MarkVersionContiguous(kbID string, versionID int64) {
	if a.fn != nil {
		a.fn(kbID, versionID)
	}
}

// coordinatorStorageHandles holds the local storage fields passed to the
// coordinator layer as untyped interfaces so nil stays an untyped nil rather
// than a typed nil pointer wrapper.
type coordinatorStorageHandles struct {
	docStore          docstore.DocStore
	versionDocList    versiondoc.VersionDocList
	chunkStore        chunkstore.ChunkStore
	chunkDocMapper    chunkdoc.ChunkDocMapper
	indexMgr          index.IndexManager
	concreteIndexMgr  *index.IndexManagerImpl
	concreteDocStore  *docstore.PebbleDocStore
	concreteChunk     *chunkstore.VecstoreChunkStore
	concreteCDM       *chunkdoc.PebbleChunkDocMapper
	concreteVD        *versiondoc.PebbleVersionDocList
	versionViews      coordinator.VersionViewCleaner
	chunkBloom        *bloom.BitsAndBloomsFilter
	chunkSplitter     splitter.ChunkSplitter
	embedClient       *embed.HTTPEmbedClient
	versionTombstones *tombstone.PebbleStore
}

// openStorageAssembly opens the storage fences, creates the storage node state
// machine (initially STARTING), creates the node's ServiceLease, opens the local
// storage stack with full IndexManager dependencies, and advances nodeState into
// RECONCILING.
func openStorageAssembly(
	cfg appConfig,
	dataDir string,
	rn raft.RaftNode,
	logger *zap.Logger,
	replicaCounter index.ReplicaCounter,
	topology *clusterTopology,
	peerPool *rpcpool.Pool,
) (*storageAssembly, error) {
	executionGate, err := storage.NewExecutionGate(filepath.Join(dataDir, "storage-fences"))
	if err != nil {
		logger.Fatal("cannot restore storage execution fences", zap.Error(err))
	}

	serviceLease := storage.NewServiceLease(cfg.NodeID, topology.localAddr)

	// Created BEFORE the storage stack opens so STARTING covers Pebble/vecstore init.
	nodeState := nodestate.New(nodestate.Options{Logger: logger.Named("nodestate")})

	stor := &storageAssembly{
		executionGate: executionGate,
		nodeState:     nodeState,
		serviceLease:  serviceLease,
		peerPool:      peerPool,
	}
	chainTailsProvider := func(context.Context) (map[string]int64, error) {
		if stor.chainTailsSource == nil {
			return nil, nil
		}
		return stor.chainTailsSource(), nil
	}

	stack, err := buildStorageStack(cfg, dataDir, rn, logger, nodeState, replicaCounter, serviceLease, chainTailsProvider)
	if err != nil {
		return nil, err
	}
	logger.Info("storage layer initialized", zap.String("role", string(cfg.Role)))

	if err := nodeState.Transition(nodestate.NodeReconciling); err != nil {
		logger.Fatal("node state: entering RECONCILING", zap.Error(err))
	}

	stor.stack = stack
	stor.ds = stack.DocStore
	stor.cdm = stack.ChunkDocMapper
	stor.vd = stack.VersionDocList
	stor.chunkStore = stack.ChunkStore
	stor.versionViews = stack.VersionViews
	stor.chunkBloom = stack.ChunkBloom
	stor.chunkSplitter = stack.ChunkSplitter
	stor.indexMgr = stack.IndexManager
	stor.embedClient = stack.EmbedClient
	stor.versionTombstones = stack.Tombstones
	return stor, nil
}

func (s *storageAssembly) close() {
	if s == nil || s.stack == nil {
		return
	}
	s.stack.Close()
}

func (s *storageAssembly) coordinatorHandles() coordinatorStorageHandles {
	if s == nil {
		return coordinatorStorageHandles{}
	}
	var (
		docStoreIface   docstore.DocStore
		versionDocIface versiondoc.VersionDocList
		chunkStoreIface chunkstore.ChunkStore
		chunkDocIface   chunkdoc.ChunkDocMapper
		indexMgrIface   index.IndexManager
	)
	if s.ds != nil {
		docStoreIface = s.ds
	}
	if s.vd != nil {
		versionDocIface = s.vd
	}
	if s.chunkStore != nil {
		chunkStoreIface = s.chunkStore
	}
	if s.cdm != nil {
		chunkDocIface = s.cdm
	}
	if s.indexMgr != nil {
		indexMgrIface = s.indexMgr
	}
	return coordinatorStorageHandles{
		docStore:          docStoreIface,
		versionDocList:    versionDocIface,
		chunkStore:        chunkStoreIface,
		chunkDocMapper:    chunkDocIface,
		indexMgr:          indexMgrIface,
		concreteIndexMgr:  s.indexMgr,
		concreteDocStore:  s.ds,
		concreteChunk:     s.chunkStore,
		concreteCDM:       s.cdm,
		concreteVD:        s.vd,
		versionViews:      versionViewCleaner(s.versionViews),
		chunkBloom:        s.chunkBloom,
		chunkSplitter:     s.chunkSplitter,
		embedClient:       s.embedClient,
		versionTombstones: s.versionTombstones,
	}
}

func (s *storageAssembly) versionViewsIfNonNil() *versionview.Store {
	if s == nil {
		return nil
	}
	return s.versionViews
}

func (s *storageAssembly) serviceLeaseIfNonNil() *storage.ServiceLease {
	if s == nil {
		return nil
	}
	return s.serviceLease
}

func (s *storageAssembly) storageMutationGuard(cfg appConfig, rn raft.RaftNode) func(context.Context, string) (context.Context, func(), error) {
	if s == nil {
		return nil
	}
	return func(c context.Context, kbID string) (context.Context, func(), error) {
		if snap, ok := storage.Bound(c, kbID); ok {
			return storage.EnterMutation(c, s.executionGate, snap)
		}
		snap, err := rn.(storage.Reader).GetKBPlacement(c, kbID)
		if err != nil {
			if errors.Is(err, stratumerrors.ErrKnowledgeBaseDeleted) {
				return storage.EnterCleanup(c, s.executionGate, rn.(storage.Reader), cfg.NodeID, kbID)
			}
			return c, func() {}, err
		}
		if !snap.Placement.Contains(cfg.NodeID) {
			return c, func() {}, stratumerrors.ErrNotKBReplica
		}
		return storage.EnterMutation(storage.Bind(c, snap), s.executionGate, snap)
	}
}

// wireControlCallbacks binds the local index manager's build-status callback to
// the control plane.
func (s *storageAssembly) wireControlCallbacks(controlPlane *plane.LocalControlPlane, rn raft.RaftNode, cfg appConfig) {
	if s == nil || s.indexMgr == nil {
		return
	}
	s.indexMgr.RegisterBuildContextCallback(func(ctx context.Context, kbID string, versionID int64, status types.IndexStatus) error {
		if status == types.IndexStatusReady {
			if kb, err := rn.GetKB(ctx, kbID); err == nil {
				_ = s.indexMgr.EnforceDiskRetention(ctx, kbID, []int64{kb.ActiveVersionID, versionID})
			}
		}
		return reportIndexStatus(ctx, controlPlane, kbID, versionID, status)
	})
}

// startGCScanner binds the chain-tails source from LagCatchup and starts the
// background index GC scanner so the goroutine observes a fully wired provider.
func (s *storageAssembly) startGCScanner(chainTails func() map[string]int64) {
	if s == nil || s.indexMgr == nil {
		return
	}
	s.chainTailsSource = chainTails
	s.indexMgr.StartGCScanner()
}

func (s *storageAssembly) localKBDroppers() coordinator.KBStorageDroppers {
	if s == nil {
		return nil
	}
	return coordinator.KBStorageDroppers{
		coordinator.LocalKBDropper{
			IndexManager:   s.indexMgr,
			DocStore:       s.ds,
			ChunkStore:     s.chunkStore,
			ChunkDocMapper: s.cdm,
			VersionDocList: s.vd,
			VersionViews:   versionViewCleaner(s.versionViews),
		},
	}
}

func (s *storageAssembly) localVersionDroppers(rn raft.RaftNode, gcImpl *coordinator.ChunkGarbageCollectorImpl) coordinator.VersionStorageDroppers {
	if s == nil {
		return nil
	}
	return coordinator.VersionStorageDroppers{
		coordinator.LocalVersionDropper{
			IndexManager:   s.indexMgr,
			DocStore:       s.ds,
			VersionDocList: s.vd,
			VersionViews:   versionViewCleaner(s.versionViews),
			Metadata:       rn,
			ChunkReclaimer: gcImpl,
		},
	}
}

// initSyncHalves constructs the node-to-node transfer handlers for a storage node.
func (s *storageAssembly) initSyncHalves(cfg appConfig, rn raft.RaftNode) {
	if s == nil {
		return
	}
	s.syncLeader = stratumsync.NewLeaderHandler(
		s.ds.DB(),
		s.cdm.DB(),
		s.vd.DB(),
		s.chunkStore.VecstoreClient(),
	)
	s.localAdvancer = &lateVersionAdvancer{}
	s.syncFollower = stratumsync.NewFollower(s.ds, s.cdm, s.vd, s.chunkStore, s.indexMgr,
		stratumsync.WithFollowerVersionTombstones(s.versionTombstones),
		stratumsync.WithPullDialTimeout(cfg.PullDialTimeout),
		stratumsync.WithFollowerExecutionGate(s.executionGate),
		stratumsync.WithFollowerStoragePlacement(rn.(storage.Reader).GetKBPlacement, cfg.NodeID),
		stratumsync.WithFollowerLocalVersionAdvancer(s.localAdvancer),
		stratumsync.WithVersionDocIDSetHash(raft.DocIDSetHashReader{Node: rn}),
		stratumsync.WithFollowerPool(s.peerPool),
	)

	s.syncPusher = stratumsync.NewPusher(stratumsync.PusherConfig{
		Exporter: s.syncLeader,
		NodeID:   cfg.NodeID,
		Pool:     s.peerPool,
	})
}

// startGarbageCollector starts the background orphan-chunk sweep loop on
// storage-holding nodes.
func (s *storageAssembly) startGarbageCollector(ctx context.Context, gcImpl *coordinator.ChunkGarbageCollectorImpl) {
	if s == nil {
		return
	}
	go gcImpl.Run(ctx)
}

// bindDataPlane finishes wiring storage-local loops onto the already-constructed
// data plane and connects the follower's contiguous-version advancer callback.
func (s *storageAssembly) bindDataPlane(
	ctx context.Context,
	cfg appConfig,
	dataPlane *plane.LocalDataPlane,
) {
	if s == nil {
		return
	}
	if s.versionTombstones != nil {
		dataPlane.StartDeletedVersionRetry(ctx, cfg.DeletedVersionRetryInterval, cfg.TombstoneGrace)
	}
	if s.localAdvancer != nil {
		s.localAdvancer.fn = dataPlane.MarkVersionContiguous
	}
}

func (s *storageAssembly) configureDispatcher(dispatcherCfg *plane.CoordinatorDispatcherConfig, dataPlane *plane.LocalDataPlane) {
	if s == nil {
		return
	}
	if s.nodeState != nil {
		dispatcherCfg.Redundancy = s.nodeState.SetPeerRedundancy
	}
	dispatcherCfg.LocalWrite = dataPlane.WriteVersionData
}

// acquireServiceLease binds the storage service authorization gate to the
// execution gate and starts the renewer loop.
func (s *storageAssembly) acquireServiceLease(
	ctx context.Context,
	cfg appConfig,
	logger *zap.Logger,
	rn raft.RaftNode,
	topology *clusterTopology,
) {
	if s == nil {
		return
	}
	s.executionGate.SetServiceLease(s.serviceLease)

	go (&stratumsync.ServiceRenewer{
		NodeID:        cfg.NodeID,
		Address:       topology.localAddr,
		Gate:          s.serviceLease,
		ResolveLeader: topology.resolveControlLeader(rn),
		Directory: func(c context.Context) (storage.Directory, error) {
			return rn.(storage.NodeDirectoryReader).GetNodeStorageDirectory(c, cfg.NodeID)
		},
		Pool:   s.peerPool,
		Logger: logger.Named("authorization"),
	}).Run(ctx)
}

func (s *storageAssembly) pauseServiceLease() {
	if s == nil || s.serviceLease == nil {
		return
	}
	s.serviceLease.Pause()
}

func (s *storageAssembly) markServing(logger *zap.Logger) {
	if s == nil || s.nodeState == nil {
		return
	}
	if err := s.nodeState.Transition(nodestate.NodeServing); err != nil {
		logger.Error("node state: entering SERVING", zap.Error(err))
	}
}
