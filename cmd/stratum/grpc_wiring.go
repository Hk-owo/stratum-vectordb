package main

import (
	"context"
	"fmt"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
	"stratum/internal/authmeta"
	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/coordinator"
	"stratum/internal/docstore"
	"stratum/internal/index"
	"stratum/internal/nodestate"
	"stratum/internal/plane"
	"stratum/internal/raft"
	stratumsync "stratum/internal/sync"
	"stratum/internal/tombstone"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"

	"stratum/service"
)

// grpcDeps is what the gRPC surface is assembled from. The services a node
// answers follow from what it holds, so the two shapes below need different
// halves of this — a control node takes the control services, a storage node
// takes the storage ones, and a node playing both takes everything.
type grpcDeps struct {
	cfg    appConfig
	logger *zap.Logger
	mark   *authmeta.Signer

	// The metadata channel and the local half's flag.
	rn           raft.RaftNode
	raftNode     *raft.RaftNodeImpl
	storageLocal bool

	// The orchestration the control services call.
	writeCoord         *coordinator.WriteCoordinatorImpl
	deleteCoord        *coordinator.DeleteCoordinatorImpl
	deleteVersionCoord *coordinator.DeleteVersionCoordinatorImpl

	// The storage the storage services read.
	indexMgr    *index.IndexManagerImpl
	cdm         *chunkdoc.PebbleChunkDocMapper
	vd          *versiondoc.PebbleVersionDocList
	ds          *docstore.PebbleDocStore
	vBloomStore *bloom.VersionBloomStore
	chunkStore  *chunkstore.VecstoreChunkStore
	walImpl     *wal.FileWAL
	dataPlane   *plane.LocalDataPlane

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
}

// startGRPCServer builds the gRPC server and binds it. The caller owns Serve.
func startGRPCServer(d grpcDeps) (*grpc.Server, net.Listener, error) {
	cfg, logger, mark := d.cfg, d.logger, d.mark
	rn, raftNode, storageLocal := d.rn, d.raftNode, d.storageLocal
	writeCoord, deleteCoord, deleteVersionCoord := d.writeCoord, d.deleteCoord, d.deleteVersionCoord
	indexMgr, cdm, vd, ds := d.indexMgr, d.cdm, d.vd, d.ds
	vBloomStore, chunkStore, walImpl, dataPlane := d.vBloomStore, d.chunkStore, d.walImpl, d.dataPlane
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
			service.UnaryAuthGate(cfg.RequireAuthenticated, mark),
		),
		grpc.ChainStreamInterceptor(
			service.RecoveryStream(logger.Sugar().Errorf),
			service.StreamAuthGate(cfg.RequireAuthenticated, mark),
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

	if raftNode != nil {
		kbSvc := service.NewKnowledgeBaseService(rn, writeCoord, deleteCoord, deleteVersionCoord)
		// §4.3: whether a write can still reach quorum, judged from the storage nodes'
		// heartbeats, so a doomed write is refused before the version number,
		// the Raft entry and the retry budget are spent on it. An unreadable
		// verdict allows the write, which is how a leadership change stays a
		// failover rather than an outage.
		kbSvc.SetStorageDegradationSource(storageGateSource{controlPlane})
		// Per-stage write timings, at debug level. The storage node's own
		// stages (write: stage timings) already existed; what was missing is the
		// half the CLIENT waits for — the control node's handling up to the
		// version commit — which is only observable from here.
		kbSvc.SetLogger(logger)
		// The await path's data_missing probe asks the same two questions
		// AdminService's status view asks — "which nodes may hold this version"
		// and "does any of them actually have it" — so it is wired from the same
		// sources (docs/await-version-plan.md §6.4). A control node holds no
		// chunks itself, which is precisely why this probe goes over the network,
		// and an unreachable replica counts as unknown rather than missing.
		kbSvc.SetPresenceProbe(stratumsync.NewPresenceChecker(stratumsync.PresenceCheckerConfig{}), replicaAddrsForProbe)
		// The event-driven half of await (§12 item 1): the node tells the service
		// when a version's replicated state moves, so the wait wakes on the apply
		// instead of on the next poll. Only a node holding the state machine can do
		// this, which is exactly the node the service is registered on.
		kbSvc.SetVersionWatcher(raftNode)
		pb.RegisterKnowledgeBaseServiceServer(grpcServer, kbSvc)
	}

	// Data-missing detection (Stratum_设计文档v13.md §7.12): the control layer
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
		presenceChecker := stratumsync.NewPresenceChecker(stratumsync.PresenceCheckerConfig{})
		querySvc := service.NewQueryService(rn, indexMgr, cdm, vd, ds, vBloomStore)
		// §9.3(2): honouring a freshness credential requires being able to
		// answer "how far does my history reach". Without this the node can only
		// ignore credentials, which is not the same as refusing a stale answer.
		querySvc.SetLocalVersionReporter(dataPlane)
		// §9.3(2): the freshness check can also REPAIR a node that is behind —
		// pulling the history it is missing — instead of only refusing. Without
		// this a replica that missed a version's data can never fetch it, because
		// the only path that triggers a pull is the query being refused.
		querySvc.SetBackfiller(dataPlane)
		// docs/index-owner-sharding-plan.md §4.2: a node that has been asked for a
		// version first asks the peers for the artifact and only builds if nobody
		// has one. The router decides which node is asked (§5), so this is reached
		// on the versions a caller actually wants.
		querySvc.SetIndexFetcher(dataPlane)
		// Per-stage query timings, at debug level. Without this the query path
		// is opaque from the node's own log — localizing the O(candidates ×
		// documents) defect needed an outside-in probe plus temporary C++
		// instrumentation (v13 §5 #18).
		querySvc.SetLogger(logger)
		adminSvc := service.NewAdminService(cfg.NodeID, rn, indexMgr, ds, chunkStore, walImpl,
			replicaAddrsForProbe,
			presenceChecker,
		)
		// §8.6(d): surface versions whose dead weight cannot be collected because
		// too few replicas would remain serving. Nothing else clears that — the
		// remedy is a larger replica count — so it goes out with the other
		// needs-a-human signals rather than only into the node's log.
		adminSvc.SetGCPressureReporter(indexMgr)
		// The operator's three verbs for a FAILED_PERMANENT version (§10.1):
		// retrying one needs the index manager, abandoning one needs the
		// delete-version cleanup, and both are storage-side facts — so this is the
		// node that can serve them.
		adminSvc.SetLogger(logger)
		adminSvc.SetDeleteVersionCoordinator(deleteVersionCoord)
		// §4.5: report storage redundancy through health details. It stays out of
		// the health STATUS on purpose — a probe that reported UNHEALTHY here
		// would pull traffic off a node whose reads are still being served. The
		// verdict is unknown on a node that does not lead, and unknown reports
		// nothing rather than vouching for storage it cannot see.
		adminSvc.SetStorageDegradationSource(storageGateSource{controlPlane})
		// §7.5: "which replica kept this knowledge base's reclaim watermark unavailable" —
		// the state that otherwise stops WAL reclamation silently.
		adminSvc.SetReclaimDiagnosticsSource(reclaimDiagnosticsSource{dataPlane})
		// docs/storage-node-state-machine-plan.md §6: surface this node's own
		// phase and its not-at-rest versions through the existing admin entry
		// points. Inside this branch the machine always exists — the typed-nil
		// guard is here so a later move of this line cannot quietly turn into a
		// call on a nil receiver.
		if nodeState != nil {
			adminSvc.SetNodeStateSource(nodeState)
		}
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
	// queries (Stratum_设计文档v13.md §7.2/§7.6).
	nodeHandler := stratumsync.NewNodeHandler(
		syncLeader,
		stratumsync.NewPushHandler(syncFollower, cfg.NodeID,
			// docs/index-owner-sharding-plan.md §5.2: only the version's owner HEAD
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
			// docs/active-lag-detection-design.md: the leader is the only one that knows
			// where the chain ends, and the node is the one that must decide whether it has
			// fallen behind. A heartbeat's response says when the tails moved, and
			// GetChainTails serves them.
			stratumsync.WithChainTails(controlPlane),
			// The landing path's last line: a version this node already knows was
			// deleted is drained and refused here rather than written, so a late push
			// cannot re-land data whose metadata is gone. nil on a node with no table.
			tombstoneOption(versionTombstones)),
		// docs/index-owner-sharding-plan.md §7: the export half of the fetch.
		// Without it a peer asking this node for an artifact gets "this node does
		// not export index artifacts", every fetch fails, and every node falls
		// back to building — the cost the sharding exists to remove, silently
		// restored. Measured on the 3+3 cluster: 34 builds for 10 versions with
		// this missing, against 10 with it wired.
		stratumsync.WithNodeIndexReader(indexMgr),
		// §8.4(a) survives the push direction: this is the serving side's gate,
		// and with distribution pull-only it is the only one left.
		stratumsync.WithMaxConcurrentIndexFetch(cfg.IndexFetchConcurrency),
	)
	pb.RegisterDataSyncServiceServer(grpcServer, nodeHandler)

	// The internal service is how a peer forwards a proposal to this node —
	// which only a node holding a log can serve, so a storage node leaves it
	// unregistered rather than being asked to append to a log it does not have.
	if raftNode != nil {
		pb.RegisterInternalServiceServer(grpcServer, raft.NewInternalServiceServer(raftNode))
	}

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}
	return grpcServer, lis, nil
}
