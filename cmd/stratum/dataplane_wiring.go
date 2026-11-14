package main

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/coordinator"
	"stratum/internal/docstore"
	"stratum/internal/index"
	"stratum/internal/nodestate"
	"stratum/internal/plane"
	"stratum/internal/raft"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"

	stratumsync "stratum/internal/sync"
)

// dataPlaneDeps is everything the data plane's configuration is built from: the
// stores it replicates into, the halves it calls back on, and the two caches it
// reads without dialling anyone.
//
// It exists so the configuration can live in one place (config()) instead of
// interleaving ~150 lines of field assignments with the assembly of the things
// being assigned. The field names deliberately match the locals main() used, so
// moving the literal here changed no expression — only where it sits.
type dataPlaneDeps struct {
	cfg    appConfig
	logger *zap.Logger

	// The control layer it reports to and reads metadata from.
	rn           raft.RaftNode
	controlPlane *plane.LocalControlPlane

	// The local stores. Nil on a control node — the same nil fields the literal
	// carried, so a control node still builds a plane with no local data paths.
	indexMgr    *index.IndexManagerImpl
	vd          *versiondoc.PebbleVersionDocList
	ds          *docstore.PebbleDocStore
	cdm         *chunkdoc.PebbleChunkDocMapper
	chunkStore  *chunkstore.VecstoreChunkStore
	vBloomStore *bloom.VersionBloomStore
	walImpl     *wal.FileWAL

	// The two halves a write runs through: the transaction executor, and the
	// follower that pulls what other nodes wrote.
	writeCoord   *coordinator.WriteCoordinatorImpl
	syncFollower *stratumsync.Follower
	syncPusher   *stratumsync.Pusher

	// Where data comes from: the announced data sources, the holders the
	// catch-up learned, and the deployment shape they are all derived from.
	dataSources  *plane.DataSourceRegistry
	holdersCache *plane.HoldersCache
	topology     *clusterTopology

	// nodeState is nil on a control node; liveness is what answers "has this
	// version been deleted" without a round trip.
	nodeState *nodestate.Machine
	liveness  plane.VersionLivenessLister
}

// config is plane.LocalDataPlaneConfig for this node.
func (d dataPlaneDeps) config() plane.LocalDataPlaneConfig {
	cfg, logger := d.cfg, d.logger
	rn, controlPlane := d.rn, d.controlPlane
	indexMgr, vd, ds, cdm, chunkStore, vBloomStore := d.indexMgr, d.vd, d.ds, d.cdm, d.chunkStore, d.vBloomStore
	walImpl, writeCoord := d.walImpl, d.writeCoord
	syncFollower, syncPusher := d.syncFollower, d.syncPusher
	dataSources, holdersCache, topology := d.dataSources, d.holdersCache, d.topology
	nodeState, liveness := d.nodeState, d.liveness

	return plane.LocalDataPlaneConfig{
		IndexManager:  indexMgr,
		LocalVersions: vd,
		// §8.4 owner sharding is decided in the STATION, not here: the router
		// computes each version's owner set and routes queries to it, so a storage
		// node that is never asked never builds anything. This node's own rule is
		// the other half of that and needs no owner knowledge: when it IS asked for
		// a version, it takes an existing artifact if a peer has one and builds the
		// index itself if none does.
		Liveness: liveness,
		Puller:   syncFollower,
		WAL:      walImpl,
		// §7.8/docs/cursor-persistence-plan.md §3: the cursor is persisted in the
		// WAL this node already fsyncs, so a restart reads it back instead of
		// inferring it from index artifacts the retention policy may have
		// deleted. Same file, one more record type.
		CursorWAL:     walImpl,
		Executor:      writeCoord,
		Control:       controlPlane,
		Pusher:        replicaPusher{pusher: syncPusher},
		CursorQuerier: stratumsync.NewLocalVersionQuerier(stratumsync.PresenceCheckerConfig{}),
		// §10.6: a permanently failed version's data is reclaimed locally and
		// on every candidate replica.
		DataDropper: writeCoord,
		// A whole-KB deletion is ONE entry in the control layer's deletion log
		// (version 0), so its reclaim needs the KB-level dropper. Without it the
		// entry stays owed and is reported as such rather than silently dropped.
		KBDropper: coordinator.LocalKBDropper{
			IndexManager:   indexMgr,
			DocStore:       ds,
			ChunkStore:     chunkStore,
			ChunkDocMapper: cdm,
			VersionDocList: vd,
			VersionBloom:   vBloomStore,
		},
		CleanupBroadcaster: stratumsync.NewVersionDataCleaner(stratumsync.PresenceCheckerConfig{}),
		// §7.3: a replica that received a version stands ready to announce it
		// if the coordinator's confirmation never arrives.
		Presence: stratumsync.NewPresenceChecker(stratumsync.PresenceCheckerConfig{}),
		Digest:   syncFollower,
		// The announcement is only worth proposing while the data side is still
		// PENDING: this reads the local metadata replica so a takeover on an
		// already-durable version costs nothing. It also covers the catch-up
		// path, which arms the same check after replaying a version nobody
		// confirmed (docs/known-gaps.md §K).
		DataStatus: controlPlane,
		Confirmer:  stratumsync.NewConfirmBroadcaster(stratumsync.PresenceCheckerConfig{}),
		// §8.4: "建一次、分发 N 份" — the builder reads its own index files and
		// ships them; replicas install them instead of building their own.
		IndexReader:  indexMgr,
		IndexFetcher: stratumsync.NewIndexFetcher(),
		// docs/index-owner-sharding-plan.md §7: where an artifact FETCHED from a
		// peer lands. The same call the push direction ends in, so a fetched
		// artifact and a shipped one are indistinguishable once installed.
		IndexInstaller:  indexMgr,
		ResolveReplicas: topology.resolveReplicaAddrs,
		// The same topology resolveReplicaAddrs walks, counted this time: how many
		// replicas should hold a written version. fanOut reads it to recognise the
		// single-replica case, where an empty target list is the expected answer
		// rather than a peer list that failed to build. Declared storage group
		// when there is one (it includes this node), the Raft members otherwise —
		// the same choice resolveReplicaAddrs makes two lines up.
		ReplicaCount: topology.replicaCount(),
		// §7.5: a backfill can replay the changes a peer recorded instead of
		// pulling each version in full. Same dialer config as the other
		// peer-facing helpers.
		ChangesFetcher: stratumsync.NewVersionChangesPuller(stratumsync.PresenceCheckerConfig{}),
		// The control layer's bounded deletion log, read incrementally. Both shapes
		// answer it (a voter from its own state machine, a storage node through the
		// internal service), so the assertion succeeds for either role; a shape that
		// did not would simply leave the sweep on the live-chain path.
		Deletions: deletionLogSource(rn),
		// The three knobs that used to be constants. They are one deployment's
		// judgement about the trade between "how far may a node fall behind" and "how
		// much does a sweep cost", so they belong in the config file.
		DeletionLogPage:       cfg.ReconcileDeletionsLogPage,
		DeletionPagesPerSweep: cfg.ReconcileDeletionPages,
		ChainReconcileMinAge:  cfg.ReconcileChainMinAge,
		ChainReconcileMaxAge:  time.Duration(cfg.ReconcileDeletedVersionsIntervalS) * time.Second,
		DisableChainReconcile: !cfg.ReconcileDeletedVersionsPeriodic,
		Verify: func(ctx context.Context, kbID string, versionID int64) bool {
			return verifyVersionPull(ctx, rn, vd, kbID, versionID)
		},
		// Resolve answers "who holds this version's data": the §8.5 table first
		// (a writer announced itself), then the peers this node's catch-up asked,
		// then the pre-§8.5 answer — the leader. All three
		// are lookups; nothing here dials a peer, because this runs on the Raft
		// apply path.
		Resolve: plane.ResolverWithRegistry(dataSources, holdersCache, func(ctx context.Context, kbID string, versionID int64) (string, bool, error) {
			status, err := rn.GetClusterStatus(ctx)
			if err != nil {
				return "", false, fmt.Errorf("GetClusterStatus: %w", err)
			}
			if !status.HasLeader {
				logger.Warn("sync: no leader known, deferring version data pull")
				return "", false, nil
			}
			// 本节点就是 leader:存储层数据已由写路径直接落盘,无需(也不应)
			// 向自己发起拉取。尤其关键的是重启后 raft 重放历史日志时,每条
			// CreateVersion 都会走"非本节点提案"分支触发本回调;若在此处向
			// 不可达的 leader 地址发起阻塞式拉取,整个 apply 循环会
			// 卡死,后续所有日志永远无法应用。
			//
			// 注意本函数必须是"廉价、不探测"的查找：它跑在 Raft apply 路径上,
			// 任何在这里发起的阻塞式 RPC 都会拖住后续日志的应用。§8.5 的
			// "任意节点当协调者"就建立在这一点上：协调者写完主动告知
			// (dataSources 那张表),而不是让拉取方在这里逐个 peer 探测。
			if status.LeaderID == cfg.NodeID {
				return "", false, nil
			}
			addr, ok := topology.peerAddr(status.LeaderID)
			if !ok {
				return "", false, fmt.Errorf("sync: leader address unknown for node ID %d", status.LeaderID)
			}
			// §4 layer 3 的"回退问 leader"只在 **leader 自己也存数据** 时才成立 —— 那是
			// 两层分层之前的形状。分层后 leader 是控制节点：它没有数据层，唯一可能的
			// 回答是 `Unimplemented: this node exports no data`。把它的地址交出去不是
			// "开始一次拉取"，而是一个不可能收敛的循环 —— EnsureIndex 每次重试都会重新
			// 解析(必须如此:写入者要等自己的写落盘才公告自己)，lag catch-up 又每几秒
			// 重入一次，每一轮都白付一次到控制节点的 gRPC。实测(3+3 集群):单个知识库
			// 100+ 次失败，全部落在 Unimplemented 上，目标地址都是控制节点。
			//
			// 返回 ok=false 现在是安全的:EnsureIndex 把它变成可重试错误而不是静默成功，
			// 所以调用方会继续等第一层(写入者公告)或第二层(holders 镜像)指名一个真正
			// 持有该版本的副本。
			if !topology.canHoldData(addr) {
				logger.Debug("sync: the leader holds no data in this topology; deferring to the announced sources",
					zap.Int64("leader_id", status.LeaderID), zap.String("leader_addr", addr))
				return "", false, nil
			}
			return addr, true, nil
		}, topology.canHoldData),
		// §8.5: announce this node as the holder of the versions it writes, so
		// replicas learn where the data is. It is its own DataSyncService
		// address — the same gRPC endpoint serves every service.
		SelfDataSyncAddr: topology.localAddr,
		// data_plane.*: the peer-facing budgets. Zero takes the plane's own
		// defaults, so an unconfigured node behaves exactly as before.
		PullIdleTimeout:         cfg.PullIdleTimeout,
		PullMaxDuration:         cfg.PullMaxDuration,
		MaxInFlightWrites:       cfg.MaxInFlightWrites,
		CursorWriteTimeout:      cfg.CursorWriteTimeout,
		CursorRetryDelay:        cfg.CursorRetryDelay,
		ConfirmAttemptTimeout:   cfg.ConfirmAttemptTimeout,
		ConfirmRetryBackoff:     cfg.ConfirmRetryBackoff,
		ConfirmBudget:           cfg.ConfirmBudget,
		ReplicaPushTimeout:      cfg.ReplicaPushTimeout,
		PushTimeoutPerDoc:       cfg.PushTimeoutPerDoc,
		PeerCursorTimeout:       cfg.PeerCursorTimeout,
		TakeoverTimeout:         cfg.TakeoverTimeout,
		TakeoverCheckTimeout:    cfg.TakeoverCheckTimeout,
		TakeoverMaxAttempts:     cfg.TakeoverMaxAttempts,
		TakeoverMaxRetryDelay:   cfg.TakeoverMaxRetryDelay,
		TerminalReclaimInterval: cfg.TerminalReclaimInterval,
		TerminalReclaimAttempts: cfg.TerminalReclaimAttempts,
		// docs/storage-node-state-machine-plan.md §7: the machine this plane
		// maintains — the version layer's entries, the data cursor, and the two
		// accounts the cursor advances over. nil on a control node, where the
		// plane makes its own: it has no storage to be authoritative about, and
		// a concrete pointer keeps this a plain nil comparison.
		State:  nodeState,
		Logger: logger,
	}
}
