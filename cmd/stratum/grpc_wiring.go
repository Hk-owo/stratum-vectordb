package main

import (
	"context"
	"fmt"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
	"stratum/internal/authmeta"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/coordinator"
	"stratum/internal/docstore"
	"stratum/internal/index"
	"stratum/internal/nodestate"
	"stratum/internal/plane"
	"stratum/internal/raft"
	"stratum/internal/rpcpool"
	"stratum/internal/storage"
	stratumsync "stratum/internal/sync"
	"stratum/internal/tombstone"
	"stratum/internal/versiondoc"
	"stratum/internal/versionview"
	"stratum/internal/wal"

	"stratum/service"
)

// grpcDeps is what the gRPC surface is assembled from. The services a node
// answers follow from what it holds, so the two shapes below need different
// halves of this — a control node takes the control services, a storage node
// takes the storage ones, and a node playing both takes everything.
type grpcDeps struct {
	migrationStagingDir string
	serviceLease        *storage.ServiceLease
	cfg                 appConfig
	logger              *zap.Logger
	mark                *authmeta.Signer

	// The metadata channel and the local half's flag.
	rn           raft.RaftNode
	raftNode     *raft.RaftNodeImpl
	storageLocal bool

	// The orchestration the control services call.
	writeCoord         *coordinator.WriteCoordinatorImpl
	deleteCoord        *coordinator.DeleteCoordinatorImpl
	deleteVersionCoord *coordinator.DeleteVersionCoordinatorImpl

	// The storage the storage services read.
	indexMgr     *index.IndexManagerImpl
	cdm          *chunkdoc.PebbleChunkDocMapper
	vd           *versiondoc.PebbleVersionDocList
	ds           *docstore.PebbleDocStore
	versionViews *versionview.Store
	chunkStore   *chunkstore.VecstoreChunkStore
	walImpl      *wal.FileWAL
	dataPlane    *plane.LocalDataPlane

	// The node-to-node halves the DataSync service is assembled from.
	syncLeader        *stratumsync.LeaderHandler
	syncFollower      *stratumsync.Follower
	controlPlane      *plane.LocalControlPlane
	dataSources       *plane.DataSourceRegistry
	livenessRegistry  *plane.LivenessRegistry
	livenessGate      *plane.LeaderGate
	versionTombstones *tombstone.PebbleStore
	nodeState         *nodestate.Machine

	// resolveReplicaAddrs answers "who else holds this" for the presence probe.
	resolveReplicaAddrs func(context.Context) ([]string, error)

	// rpcMetrics counts what this server handles. Nil is the normal state for a
	// node started without a metrics endpoint, and every method on it is nil-safe.
	rpcMetrics *service.RPCMetrics

	peerPool *rpcpool.Pool
}

// migrationServingSurface keeps a nil index manager out of the interface: a typed
// nil would satisfy the interface and panic on first use.
func migrationServingSurface(m *index.IndexManagerImpl) stratumsync.MigrationServing {
	if m == nil {
		return nil
	}
	return m
}

// startGRPCServer builds the gRPC server and binds it. The caller owns Serve.
func startGRPCServer(d grpcDeps) (*grpc.Server, net.Listener, error) {
	cfg, logger, mark := d.cfg, d.logger, d.mark
	rn, raftNode, storageLocal := d.rn, d.raftNode, d.storageLocal
	writeCoord, deleteCoord, deleteVersionCoord := d.writeCoord, d.deleteCoord, d.deleteVersionCoord
	indexMgr, cdm, vd, ds := d.indexMgr, d.cdm, d.vd, d.ds
	versionViews, chunkStore, walImpl, dataPlane := d.versionViews, d.chunkStore, d.walImpl, d.dataPlane
	syncLeader, syncFollower := d.syncLeader, d.syncFollower
	controlPlane, dataSources := d.controlPlane, d.dataSources
	livenessRegistry, livenessGate := d.livenessRegistry, d.livenessGate
	versionTombstones, nodeState := d.versionTombstones, d.nodeState
	resolveReplicaAddrs := d.resolveReplicaAddrs

	//
	// Which services a node answers follows from what it holds. The control
	// services need a log to append a client write to; the storage services need
	// the local stores. Registering the wrong half would either panic on a nil
	// Raft node or accept writes a storage node could never commit.
	//
	// QueryService and AdminService are storage services in fact, even though
	// clients reach them on the same port: their constructors take the index
	// manager and the local stores directly, so only a node holding data can
	// build them at all.
	// §9.3(5): the authentication gate covers the client-facing surface only.
	// Both interceptors are installed together so a streaming method added later
	// cannot quietly bypass it.
	//
	// Recovery sits in FRONT of the gate: it has to be the outermost layer to
	// catch a panic from anything behind it, and the gate is itself code that
	// runs on the caller's input. Without it, one handler panic — a request that
	// drives an allocation off a client-supplied size, say — exits the process
	// and takes every other client's traffic with it (service/recovery.go).
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			service.RecoveryUnary(logger.Sugar().Errorf),
			// Behind recovery (so it never sees a panic) and in front of the
			// gates (so a refused call is counted too): the point of the series
			// is control-side traffic per method, and a rejected call is traffic
			// the node did handle.
			d.rpcMetrics.UnaryInterceptor(),
			service.UnaryAuthGate(cfg.RequireAuthenticated, mark),
			service.UnaryStorageServiceLease(d.serviceLease),
			service.UnaryStoragePlacement(rn.(storage.Reader).GetKBPlacement, cfg.NodeID),
		),
		grpc.ChainStreamInterceptor(
			service.RecoveryStream(logger.Sugar().Errorf),
			d.rpcMetrics.StreamInterceptor(),
			service.StreamAuthGate(cfg.RequireAuthenticated, mark),
			service.StreamStorageServiceLease(d.serviceLease),
		),
	)

	// The candidate replicas that may hold a version's data, as a plain address
	// list. Two consumers ask that question in different shapes — the admin
	// status view and the await path's data_missing probe — and sharing one
	// closure is what keeps them from answering differently. Resolved per call,
	// not once: resolveReplicaAddrs is late-bound (see its declaration), so
	// resolving eagerly here would freeze a topology the cluster may still be
	// forming.
	replicaAddrsForProbe := func() []string {
		addrs, err := resolveReplicaAddrs(context.Background())
		if err != nil {
			return nil
		}
		return addrs
	}
	kbReplicaAddrs := func(ctx context.Context, kbID string) ([]string, error) {
		snap, err := rn.(storage.Reader).GetKBPlacement(ctx, kbID)
		if err != nil {
			return nil, err
		}
		return snap.Addresses(0), nil
	}

	if raftNode != nil {
		kbSvc := service.NewKnowledgeBaseService(rn, writeCoord, deleteCoord, deleteVersionCoord,
			service.WithKBReplicaResolver(kbReplicaAddrs),
			// §4.3: whether a write can still reach quorum, judged from the storage nodes'
			// heartbeats, so a doomed write is refused before the version number,
			// the Raft entry and the retry budget are spent on it.
			service.WithKBStorageDegradationSource(storageGateSource{controlPlane}),
			service.WithKBLogger(logger),
			// The await path's data_missing probe asks which nodes may hold this
			// version and whether any of them actually has it.
			service.WithPresenceProbe(stratumsync.NewPresenceChecker(stratumsync.PresenceCheckerConfig{Pool: d.peerPool}), replicaAddrsForProbe),
			service.WithVersionWatcher(raftNode),
		)
		pb.RegisterKnowledgeBaseServiceServer(grpcServer, kbSvc)
	}

	// Data-missing detection (docs/参考/历史/Stratum_设计文档v13.md §7.12): the control layer
	// asks every candidate replica whether it holds a version whose data may
	// never have landed, and surfaces the ones nobody has.
	// These read the local stores by construction, so only a node holding data
	// can build them. A control node answers neither — with one exception, below.
	//
	// AdminService is registered on both shapes now, because its surface is not
	// uniform: the store-reading calls belong to a node that holds data, while
	// GetClusterStatus reads the Raft view and belongs to the node that owns it.
	// Registering it here alone left GetClusterStatus unreachable, which is what
	// broke the storage layer's cursor reporter (see control_admin.go).
	if storageLocal {
		presenceChecker := stratumsync.NewPresenceChecker(stratumsync.PresenceCheckerConfig{Pool: d.peerPool})
		querySvc := service.NewQueryService(rn, indexMgr, cdm, vd, ds, versionViews,
			service.WithLocalVersionReporter(dataPlane),
			service.WithBackfiller(dataPlane),
			service.WithIndexFetcher(dataPlane),
			service.WithQueryLogger(logger),
		)
		adminOpts := []service.AdminOption{
			service.WithAdminKBReplicaResolver(kbReplicaAddrs),
			service.WithGCPressureReporter(indexMgr),
			service.WithAdminLogger(logger),
			service.WithDeleteVersionCoordinator(deleteVersionCoord),
			service.WithAdminStorageDegradationSource(storageGateSource{controlPlane}),
			service.WithReclaimDiagnosticsSource(reclaimDiagnosticsSource{dataPlane}),
		}
		if nodeState != nil {
			adminOpts = append(adminOpts, service.WithNodeStateSource(nodeState))
		}
		adminSvc := service.NewAdminService(cfg.NodeID, rn, indexMgr, ds, chunkStore, walImpl,
			replicaAddrsForProbe,
			presenceChecker,
			adminOpts...,
		)
		pb.RegisterQueryServiceServer(grpcServer, querySvc)
		pb.RegisterAdminServiceServer(grpcServer, adminSvc)
	} else {
		// A node without stores answers Unimplemented for every admin call that
		// reads them — but GetClusterStatus it answers in full, because the Raft
		// view lives only here. Without this registration a storage node cannot
		// resolve the control leader at all: every heartbeat fails with
		// "Unimplemented: unknown service stratum.AdminService", and the chain tails
		// (lag catch-up) and the redundancy verdict go with it.
		pb.RegisterAdminServiceServer(grpcServer, newControlAdminService(cfg.NodeID, rn))
	}

	// One data-plane service per node: it both exports versions to peers (the
	// pull path) and receives pushed ones, and answers presence/cursor
	// queries (docs/参考/历史/Stratum_设计文档v13.md §7.2/§7.6).
	nodeHandler := stratumsync.NewNodeHandler(
		syncLeader,
		stratumsync.NewPushHandler(syncFollower, cfg.NodeID,
			stratumsync.WithExecutionGate(dataPlane.ExecutionGate(), rn.(storage.Reader)),
			stratumsync.WithStoragePlacement(rn.(storage.Reader).GetKBPlacement),
			// A migration target's serving step needs the index surface: it rebuilds
			// the local document filters, advances the data cursor and waits for the
			// active version's artifact before the task can be activated. A node
			// without an index manager passes no surface at all, so the step refuses
			// instead of reporting readiness it cannot check.
			stratumsync.WithMigrationServing(migrationServingSurface(indexMgr)),
			// docs/已实施/index-owner-sharding-plan.md §5.2: only the version's owner HEAD
			// builds, so a push landing here does not start a build on every replica
			// (the measured baseline was N builds per version, with §8.4's
			// distribution never sending a byte). An empty member set — a single-node
			// deployment, or a control node — leaves the behaviour unchanged.
			stratumsync.WithLocalVersion(dataPlane),
			// §9.3(2): receiving a version's records is what makes this node
			// hold it; without this the cursor stays 0 and the station's
			// freshness check refuses a replica whose data is complete.
			stratumsync.WithLocalVersionAdvancer(dataPlane),
			stratumsync.WithVersionDataDropper(writeCoord),
			stratumsync.WithVersionWriteWatcher(dataPlane),
			// §8.4(a): the presence probe's answer and its failures are only
			// visible in the log, so the receive side gets this node's logger.
			stratumsync.WithLogger(logger),
			// §8.5: record the writers' data-source announcements (they ride the
			// §7.3 confirmation) so this node can resolve a version's data
			// without asking the leader.
			stratumsync.WithDataSourceRegistry(dataSources),
			// §7.13.2: when the control layer dispatches a write to this node as
			// its coordinator, the storage-layer transaction runs here.
			stratumsync.WithVersionWriteExecutor(dataPlane),
			// §7.5: a lagging peer can pull the recorded changes for a version
			// range and replay them, instead of transferring each version's full
			// record set. The WAL already holds them for crash recovery.
			stratumsync.WithVersionChangesReader(walImpl),
			// Only the control leader keeps the record of the storage nodes' heartbeats.
			// The gate clears it on each leadership term, so a new leader never judges from
			// heartbeats it never received.
			stratumsync.WithHeartbeatRecorder(livenessRegistry, livenessGate.IsLeader),
			// docs/已实施/active-lag-detection-design.md: the leader is the only one that knows
			// where the chain ends, and the node is the one that must decide whether it has
			// fallen behind. A heartbeat's response says when the tails moved, and
			// GetChainTails serves them.
			stratumsync.WithChainTails(controlPlane),
			// The landing path's last line: a version this node already knows was
			// deleted is drained and refused here rather than written, so a late push
			// cannot re-land data whose metadata is gone. nil on a node with no table.
			tombstoneOption(versionTombstones)),
		// docs/已实施/index-owner-sharding-plan.md §7: the export half of the fetch.
		// Without it a peer asking this node for an artifact gets "this node does
		// not export index artifacts", every fetch fails, and every node falls
		// back to building — the cost the sharding exists to remove, silently
		// restored. Measured on the 3+3 cluster: 34 builds for 10 versions with
		// this missing, against 10 with it wired.
		stratumsync.WithNodeIndexReader(indexMgr),
		stratumsync.WithMigrationStaging(rn.(stratumsync.MigrationMetadata), d.migrationStagingDir),
		stratumsync.WithMigrationPool(d.peerPool),
		stratumsync.WithMigrationCleanup(coordinator.LocalKBDropper{IndexManager: indexMgr, DocStore: ds, ChunkStore: chunkStore, ChunkDocMapper: cdm, VersionDocList: vd, VersionViews: versionViewCleaner(versionViews)}),
		// §8.4(a) survives the push direction: this is the serving side's gate,
		// and with distribution pull-only it is the only one left.
		stratumsync.WithMaxConcurrentIndexFetch(cfg.IndexFetchConcurrency),
	)
	pb.RegisterDataSyncServiceServer(grpcServer, nodeHandler)
	pb.RegisterStorageAdminServiceServer(grpcServer, service.NewStorageAdminService(rn))

	// The internal service is how a peer forwards a proposal to this node —
	// which only a node holding a log can serve, so a storage node leaves it
	// unregistered rather than being asked to append to a log it does not have.
	if raftNode != nil {
		pb.RegisterInternalServiceServer(grpcServer, raft.NewInternalServiceServer(raftNode, func(kbID string) storage.Health {
			state, detail, known := controlPlane.StorageDegradation(kbID)
			return storage.Health{Known: known, Degraded: state == plane.StorageDegraded, Unavailable: state == plane.StorageUnavailable, Detail: detail}
		}))
	}

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}
	return grpcServer, lis, nil
}
