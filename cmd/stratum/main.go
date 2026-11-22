// Stratum is a distributed vector-search knowledge base engine: Raft-
// consensus-backed server that manages versioned document collections,
// splits documents into chunks, embeds them into vectors, indexes them
// with HNSW (Faiss), and serves similarity queries.
//
// See doc/docs/参考/历史/Stratum_设计文档v10.md for the full architecture.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"stratum/internal/authmeta"
	"stratum/internal/nodestate"
	"stratum/internal/plane"
	"stratum/internal/raft"
	"stratum/internal/rpcpool"
	"stratum/internal/storage"
	stratumsync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/wal"
	"stratum/service"
)

func main() {
	b := bootstrapNode()
	defer b.cleanup()
	defer func() { _ = b.logger.Sync() }()

	cfg, logger := b.cfg, b.logger
	walImpl, records := b.walImpl, b.records
	rn, raftNode, mark := b.rn, b.raftNode, b.mark
	dataDir := b.dataDir

	ctx := context.Background()
	storageLocal := cfg.Role != NodeRoleControl
	ctrl := buildControlContext(cfg, logger, rn, b.topology, b.storageSilence)

	var stor *storageAssembly
	if storageLocal {
		var err error
		stor, err = openStorageAssembly(cfg, dataDir, rn, logger, ctrl.controlPlane, ctrl.topology, b.peerPool)
		if err != nil {
			logger.Fatal("failed to initialize the storage layer", zap.Error(err))
		}
		defer stor.close()
	} else {
		logger.Info("starting without local storage: metadata only (control role)",
			zap.String("role", string(cfg.Role)))
	}

	a := assembleNode(ctx, cfg, logger, walImpl, rn, raftNode, b.applyGate, ctrl, stor, b.peerPool)
	epochDurable := a.startupAndHooks(records)

	stor.acquireServiceLease(ctx, cfg, logger, rn, a.topology)
	defer stor.pauseServiceLease()

	h := stor.coordinatorHandles()
	var (
		nodeState    *nodestate.Machine
		syncLeader   *stratumsync.LeaderHandler
		syncFollower *stratumsync.Follower
		serviceLease *storage.ServiceLease
	)
	if stor != nil {
		nodeState = stor.nodeState
		syncLeader = stor.syncLeader
		syncFollower = stor.syncFollower
		serviceLease = stor.serviceLease
	}

	rpcMetrics := service.NewRPCMetrics()
	grpcServer, lis, err := startGRPCServer(grpcDeps{
		migrationStagingDir: filepath.Join(dataDir, "migration-staging"),
		serviceLease:        serviceLease,
		rpcMetrics:          rpcMetrics,
		cfg:                 cfg,
		logger:              logger,
		mark:                mark,
		rn:                  rn,
		raftNode:            raftNode,
		storageLocal:        storageLocal,
		writeCoord:          a.writeCoord,
		deleteCoord:         a.deleteCoord,
		deleteVersionCoord:  a.deleteVersionCoord,
		indexMgr:            h.concreteIndexMgr,
		cdm:                 h.concreteCDM,
		vd:                  h.concreteVD,
		ds:                  h.concreteDocStore,
		versionViews:        stor.versionViewsIfNonNil(),
		chunkStore:          h.concreteChunk,
		walImpl:             walImpl,
		dataPlane:           a.dataPlane,
		syncLeader:          syncLeader,
		syncFollower:        syncFollower,
		controlPlane:        a.controlPlane,
		dataSources:         a.dataSources,
		livenessRegistry:    a.livenessRegistry,
		livenessGate:        a.livenessGate,
		versionTombstones:   h.versionTombstones,
		nodeState:           nodeState,
		resolveReplicaAddrs: a.resolveReplicaAddrs,
		peerPool:            b.peerPool,
	})
	if err != nil {
		logger.Fatal("failed to listen", zap.String("addr", cfg.GRPCAddr), zap.Error(err))
	}

	startShutdownWatcher(shutdownDeps{
		cfg:        cfg,
		logger:     logger,
		nodeState:  nodeState,
		grpcServer: grpcServer,
		indexMgr:   h.concreteIndexMgr,
		chunkStore: h.concreteChunk,
		rpcMetrics: rpcMetrics,
	})

	chainTails, chainTailsFn := newLagCatchup(lagCatchupDeps{
		cfg:          cfg,
		logger:       logger,
		storageLocal: storageLocal,
		dataPlane:    a.dataPlane,
		holdersCache: a.holdersCache,
		nodeState:    nodeState,
	})
	stor.startGCScanner(chainTailsFn)

	go stratumsync.NewHeartbeater(stratumsync.HeartbeaterConfig{
		NodeID:        cfg.NodeID,
		Interval:      cfg.SyncHeartbeatInterval,
		ResolveLeader: a.topology.resolveControlLeader(a.rn),
		ChainTails:    chainTails,
		Pool:          b.peerPool,
		Logger:        logger,
	}).Run(ctx)

	go plane.NewWALReclaimer(plane.WALReclaimerConfig{
		Reclaimer: a.dataPlane,
		Interval:  cfg.GCWALReclaimInterval,
		Logger:    logger,
	}).Run(ctx)

	logger.Info("Stratum gRPC server listening", zap.String("addr", cfg.GRPCAddr))
	stor.markServing(logger)

	go reportEpochWhenPeersAreUp(ctx, logger, a.dataPlane, a.controlPlane, epochDurable,
		cfg.EpochReportRetryInterval, cfg.EpochReportRetryWindow)

	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatal("gRPC server failed", zap.Error(err))
	}

	logger.Info("Stratum stopped")
}

// nodeBoot is what exists before anything is assembled: the configuration, the
// log, the WAL (scanned, not yet replayed), the metadata channel, and the trust
// mark. Everything after this is built FROM it.
type nodeBoot struct {
	cfg         appConfig
	logger      *zap.Logger
	dataDir     string
	walImpl     *wal.FileWAL
	records     []types.PendingRecord
	rn          raft.RaftNode
	raftNode    *raft.RaftNodeImpl
	topology    *clusterTopology
	applyGate   *raftApplyGate
	mark        *authmeta.Signer
	controlPool *rpcpool.Pool
	peerPool    *rpcpool.Pool

	storageSilence time.Duration
	cleanup        func()
}

// bootstrapNode parses flags, reads the configuration, opens the WAL, scans it
// for uncommitted records and builds the metadata channel. Startup failures that
// cannot be recovered from end the process here (logger.Fatal).
func bootstrapNode() *nodeBoot {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}

	logger.Info("Stratum starting")

	configFlag := flag.String("config", "", "path to YAML config file (optional; flags override file values)")
	dataDirFlag := flag.String("data-dir", "", "data directory (default /var/lib/stratum/node1)")
	grpcAddrFlag := flag.String("grpc-addr", "", "gRPC listen address (default 0.0.0.0:7000)")
	vecstoreAddrFlag := flag.String("vecstore-addr", "", "vecstore gRPC address (default 127.0.0.1:7100)")
	embedAddrFlag := flag.String("embed-addr", "", "embed service address (default http://localhost:8080)")
	storageSilenceWindowFlag := flag.Duration("storage-silence-window", 0,
		"how long a required replica may go without reporting before it stops counting as live (default: 3 report intervals)")
	flag.Parse()

	cfg := defaultConfig()
	if *configFlag != "" {
		loaded, err := loadConfig(*configFlag)
		if err != nil {
			logger.Fatal("failed to load config file", zap.String("path", *configFlag), zap.Error(err))
		}
		cfg = loaded
	}
	if *dataDirFlag != "" {
		cfg.DataDir = *dataDirFlag
	}
	if *grpcAddrFlag != "" {
		cfg.GRPCAddr = *grpcAddrFlag
	}
	if *vecstoreAddrFlag != "" {
		cfg.VecstoreGRPCAddr = *vecstoreAddrFlag
	}
	if *embedAddrFlag != "" {
		cfg.EmbedServiceAddr = *embedAddrFlag
	}

	logger = loggerAtLevel(logger, cfg.LogLevel)

	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "/var/lib/stratum/node1"
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		logger.Fatal("failed to create data directory", zap.String("path", dataDir), zap.Error(err))
	}

	walPath := dataDir + "/wal"
	walImpl, err := wal.NewFileWAL(walPath)
	if err != nil {
		logger.Fatal("failed to open WAL", zap.String("path", walPath), zap.Error(err))
	}

	records, err := walImpl.Recover(nil)
	if err != nil {
		logger.Fatal("WAL recovery scan failed", zap.Error(err))
	}
	if len(records) > 0 {
		logger.Info("WAL recovery: pending records found, will replay after coordinators are ready",
			zap.Int("count", len(records)))
	}

	var rn raft.RaftNode
	var raftNode *raft.RaftNodeImpl
	mark := authmeta.NewSigner([]byte(cfg.StationSecret), cfg.StationMarkTTL)
	if cfg.RequireAuthenticated && !mark.Enabled() {
		logger.Warn("require_authenticated is on but no station_secret is configured: " +
			"every client-facing call will be refused (set node.station_secret to the station's secret)")
	}
	topology := newClusterTopology(cfg)
	controlPool := rpcpool.NewPool(nil)
	peerPool := rpcpool.NewPool(nil)
	var applyGate *raftApplyGate
	if cfg.Role == NodeRoleStorage {
		rn = &raft.RemoteRaftNode{ControlAddrs: controlAddrsFromPeers(cfg.Peers), Mark: mark, Pool: controlPool}
		logger.Info("starting as a storage node: no Raft log, metadata read through the control cluster",
			zap.String("role", string(cfg.Role)), zap.Int("control_nodes", len(cfg.Peers)))
	} else {
		applyGate = newRaftApplyGate()
		raftNode, err = raft.NewRaftNodeImpl(raft.Config{
			NodeID:              cfg.NodeID,
			DataDir:             dataDir,
			RaftAddr:            cfg.RaftAddr,
			Peers:               cfg.Peers,
			WAL:                 walImpl,
			Logger:              logger.Named("raft"),
			MaxLogLength:        cfg.MaxLogLength,
			DeletedLogLimit:     cfg.DeletedLogLimit,
			HeartbeatInterval:   cfg.HeartbeatInterval,
			ElectionTimeoutMin:  cfg.ElectionTimeoutMin,
			ElectionTimeoutMax:  cfg.ElectionTimeoutMax,
			RPCTimeout:          cfg.RaftRPCTimeout,
			ProposeApplyTimeout: cfg.RaftProposeApplyTimeout,
		},
			raft.WithNodeID(cfg.NodeID),
			raft.WithForwarder(&raft.GRPCProposeForwarder{AddrByID: topology.peerAddr, Pool: controlPool}),
			raft.WithOnVersionCreated(applyGate.handleCreated),
			raft.WithOnVersionFailedPermanent(applyGate.handleFailed),
			raft.WithOnVersionCommittedAsLeader(applyGate.handleCommittedAsLeader),
		)
		if err != nil {
			_ = peerPool.Close()
			_ = controlPool.Close()
			logger.Fatal("failed to start RaftNode", zap.Error(err))
		}
		rn = raftNode
	}
	cleanup := func() {
		if applyGate != nil {
			applyGate.abort()
		}
		if raftNode != nil {
			raftNode.Stop()
		}
		_ = walImpl.Close()
		_ = peerPool.Close()
		_ = controlPool.Close()
	}
	return &nodeBoot{
		cfg:            cfg,
		logger:         logger,
		dataDir:        dataDir,
		walImpl:        walImpl,
		records:        records,
		rn:             rn,
		raftNode:       raftNode,
		topology:       topology,
		applyGate:      applyGate,
		mark:           mark,
		controlPool:    controlPool,
		peerPool:       peerPool,
		storageSilence: *storageSilenceWindowFlag,
		cleanup:        cleanup,
	}
}
