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
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	pb "stratum/api/proto/stratum"
	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/coordinator"
	"stratum/internal/docstore"
	"stratum/internal/embed"
	"stratum/internal/index"
	"stratum/internal/plane"
	"stratum/internal/raft"
	"stratum/internal/splitter"
	stratumsync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"
	"stratum/service"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Stratum starting")

	// --- Flags ---
	// Minimal flag surface for local/dev runs (the one-click startup script
	// and ad-hoc manual runs). Defaults match defaultConfig(); a YAML config
	// file (see configs/config1.yaml) can supply the full multi-node
	// deployment settings, with individual flags overriding file values.
	configFlag := flag.String("config", "", "path to YAML config file (optional; flags override file values)")
	dataDirFlag := flag.String("data-dir", "", "data directory (default /var/lib/stratum/node1)")
	grpcAddrFlag := flag.String("grpc-addr", "", "gRPC listen address (default 0.0.0.0:7000)")
	vecstoreAddrFlag := flag.String("vecstore-addr", "", "vecstore gRPC address (default 127.0.0.1:7100)")
	embedAddrFlag := flag.String("embed-addr", "", "embed service address (default http://localhost:8080)")
	flag.Parse()

	// --- Configuration ---
	// Load a YAML config file if provided; individual flags override file
	// values. With no config file, use the hardcoded single-node defaults
	// (matching configs/config1.yaml).
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
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "/var/lib/stratum/node1"
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		logger.Fatal("failed to create data directory", zap.String("path", dataDir), zap.Error(err))
	}

	// --- WAL path ---
	//
	// The data layers' paths live in buildStorageStack now; this one stays here
	// because Raft's log is not part of the storage layer — a control node has
	// one too.
	walPath := dataDir + "/wal"

	// --- Initialize WAL ---
	walImpl, err := wal.NewFileWAL(walPath)
	if err != nil {
		logger.Fatal("failed to open WAL", zap.String("path", walPath), zap.Error(err))
	}
	defer walImpl.Close()

	// --- Crash recovery (records scanned now; replayed once the
	// coordinator layer is ready below) ---
	records, err := walImpl.Recover(nil)
	if err != nil {
		logger.Fatal("WAL recovery scan failed", zap.Error(err))
	}
	if len(records) > 0 {
		logger.Info("WAL recovery: pending records found, will replay after coordinators are ready",
			zap.Int("count", len(records)))
	}

	// --- Metadata channel ---
	//
	// rn is how this process reaches replicated metadata, whoever holds it: its
	// own Raft node when it runs one, the remote proxy when it does not
	// (Stratum_设计文档v13.md §11 阶段 ④「存储集群独立进程」).
	//
	// A storage node keeps no log, votes in no election and never leads, so it
	// holds no RaftNodeImpl: raftNode stays nil and every control-layer callback
	// below is skipped. The metadata it needs — a knowledge base's quantizer
	// configuration, the version chain, who currently leads — is read over gRPC
	// through RemoteRaftNode, and the facts it produces travel back the same way.
	//
	// Every consumer below therefore takes rn, never raftImpl: the two shapes
	// differ in where the metadata lives, not in what it means.
	var rn raft.RaftNode
	var raftNode *raft.RaftNodeImpl
	if cfg.Role == NodeRoleStorage {
		rn = &raft.RemoteRaftNode{ControlAddrs: controlAddrsFromPeers(cfg.Peers)}
		logger.Info("starting as a storage node: no Raft log, metadata read through the control cluster",
			zap.String("role", string(cfg.Role)), zap.Int("control_nodes", len(cfg.Peers)))
	} else {
		raftNode, err = raft.NewRaftNodeImpl(raft.Config{
			NodeID:             cfg.NodeID,
			DataDir:            dataDir,
			RaftAddr:           cfg.RaftAddr,
			Peers:              cfg.Peers,
			WAL:                walImpl,
			Logger:             logger.Named("raft"),
			MaxLogLength:       cfg.MaxLogLength,
			HeartbeatInterval:  cfg.HeartbeatInterval,
			ElectionTimeoutMin: cfg.ElectionTimeoutMin,
			ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		})
		if err != nil {
			logger.Fatal("failed to start RaftNode", zap.Error(err))
		}
		defer raftNode.Stop()
		rn = raftNode
	}

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
	// leader-side aggregate storage nodes report their cursors to (§7.13.4).
	var syncLeader *stratumsync.LeaderHandler
	var syncFollower *stratumsync.Follower
	var syncPusher *stratumsync.Pusher
	var ds *docstore.PebbleDocStore
	var cdm *chunkdoc.PebbleChunkDocMapper
	var vd *versiondoc.PebbleVersionDocList
	var chunkStore *chunkstore.VecstoreChunkStore
	var chunkBloom *bloom.BitsAndBloomsFilter
	var vBloomStore *bloom.VersionBloomStore
	var chunkSplitter *splitter.SlidingWindowSplitter
	var indexMgr *index.IndexManagerImpl
	var embedClient *embed.HTTPEmbedClient

	ctx := context.Background()
	if storageLocal {
		stack, err := buildStorageStack(cfg, dataDir, rn, logger)
		if err != nil {
			logger.Fatal("failed to initialize the storage layer", zap.Error(err))
		}
		defer stack.Close()
		ds, cdm, vd, chunkStore = stack.DocStore, stack.ChunkDocMapper, stack.VersionDocList, stack.ChunkStore
		chunkBloom, vBloomStore = stack.ChunkBloom, stack.VersionBloom
		chunkSplitter, indexMgr, embedClient = stack.ChunkSplitter, stack.IndexManager, stack.EmbedClient
		logger.Info("storage layer initialized", zap.String("role", string(cfg.Role)))
	} else {
		logger.Info("starting without local storage: metadata only (control role)",
			zap.String("role", string(cfg.Role)))
	}

	// controlPlane is the storage layer's only channel to replicated state
	// (control-data-separation-design.md §4.2); stage ① runs it in-process.
	//
	// §7.13.4: the leader-side aggregate of storage-layer cursor reports, plus the
	// gate that clears it when this node takes over leadership. Clearing on
	// takeover is the point: a new leader must not answer "who holds version V"
	// from reports its predecessor collected. Both are created here because the
	// control plane is the view's first reader.
	dataVersionRegistry := plane.NewDataVersionRegistry()
	dataVersionGate := plane.NewLeaderGate(rn.IsLeader, dataVersionRegistry.Reset)
	// requiredReplicaIDs is assigned once peerAddrByID exists, further down.
	var requiredReplicaIDs func() ([]int64, error)
	controlPlane := plane.NewLocalControlPlane(rn,
		// §8.6(d): index-readiness reports name the reporter, so the control
		// layer can tell "how many replicas are serving this version" from
		// "someone is" — the rolling cleanup asks that before taking one out of
		// service.
		plane.WithNodeID(cfg.NodeID),
		plane.WithDataVersionView(dataVersionRegistry, dataVersionGate),
		// §7.5: the replica set a version must reach before the WAL changes behind
		// it become reclaimable. It comes from the cluster topology — never from the
		// aggregate — so a node that is merely silent cannot drop out of the
		// requirement. Assigned further down (once peerAddrByID exists) and read
		// through this closure, so the assembly order does not matter; until then it
		// reports no topology, which yields no watermark, i.e. "keep the changes".
		plane.WithRequiredReplicas(func() ([]int64, error) {
			if requiredReplicaIDs == nil {
				return nil, errors.New("stratum: replica topology not assembled yet")
			}
			return requiredReplicaIDs()
		}))
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
	// distributeIndex ships a freshly built index to the other replicas
	// (Stratum_设计文档v13.md §8.4). It is assigned once the data plane exists,
	// further down; until then a build stays local, which is exactly how the
	// cluster behaved before distribution existed.
	var distributeIndex func(kbID string, versionID int64)
	// A control node has no index manager, so no builds to observe.
	if indexMgr != nil {
		indexMgr.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
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
				if distributeIndex != nil {
					distributeIndex(kbID, versionID)
				}
			}
			return reportIndexStatus(context.Background(), controlPlane, kbID, versionID, status)
		})
	}

	// --- Coordinators ---
	// writeMu serializes CreateVersion write transactions (BEGIN through
	// COMMIT) and is shared with the orphan-chunk GC's reclaim phase so a
	// sweep re-validates and deletes candidates under mutual exclusion
	// with concurrent writes (see gc_impl.go reclaimOrphan). Both sides
	// MUST receive the same mutex.
	// dispatchVersionWrite and resolveReplicaAddrs are late-bound: the coordinator
	// needs a dispatcher, the dispatcher needs the data plane, and the data plane
	// needs the coordinator (§7.13.2). Routing them through these closures keeps
	// the assembly acyclic (the same trick distributeIndex uses).
	var dispatchVersionWrite func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error
	var resolveReplicaAddrs func(ctx context.Context) ([]string, error)

	var writeMu sync.Mutex
	writeCoord := coordinator.NewWriteCoordinatorImpl(coordinator.WriteCoordinatorConfig{
		MaxRetries:          cfg.WriteMaxRetries,
		RetryBaseIntervalMS: cfg.WriteRetryBaseMS,
		WriteMu:             &writeMu,
		WAL:                 walImpl,
		RaftNode:            rn,
		Splitter:            chunkSplitter,
		EmbedClient:         embedClient,
		ChunkBloom:          chunkBloom,
		VersionBloom:        vBloomStore,
		ChunkStore:          chunkStore,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vd,
		IndexManager:        indexMgr,
		// §7.13.2: a committed write is handed to the coordinator the control
		// layer picks, instead of being run by whoever accepted it. The dispatcher
		// is built further down (it needs the data plane), so the call goes through
		// the late-bound closure.
		Dispatch: func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
			if dispatchVersionWrite == nil {
				return fmt.Errorf("stratum: write dispatch is not wired")
			}
			return dispatchVersionWrite(ctx, kbID, versionID, parentVersionID, changes)
		},
		Logger: logger,
	})

	// A deleted knowledge base's data is reclaimed by whoever holds it: the local
	// stores on an all-in-one node, and the storage group on a control node that
	// keeps none. The data plane is assembled further down (it needs the
	// coordinators), so the choice is resolved on first use rather than guessed
	// here.
	//
	// dataPlane is declared here and assigned below for exactly that reason: a
	// control node's dropper needs it, and the two constructions would otherwise
	// have to be interleaved.
	var dataPlane *plane.LocalDataPlane
	kbDropper := coordinator.NewDeferredKBDropper(func() coordinator.KBStorageDropper {
		// The reclaim has two halves and a split deployment needs both: the
		// storage group holds the data, and this node may hold a copy of its
		// own. Broadcasting is what makes the delete work at all when the data
		// is not here — the local layer deletes alone would clear this node's
		// (possibly empty) stores and leave the storage group's data readable.
		//
		// This mirrors the data plane's own DropVersionData, which broadcasts
		// and drops locally rather than choosing one.
		droppers := coordinator.KBStorageDroppers{
			coordinator.BroadcastKBDropper{Metadata: rn, Dropper: dataPlane},
		}
		if cfg.Role != NodeRoleControl {
			droppers = append(droppers, coordinator.LocalKBDropper{
				IndexManager:   indexMgr,
				DocStore:       ds,
				ChunkStore:     chunkStore,
				ChunkDocMapper: cdm,
				VersionDocList: vd,
				VersionBloom:   vBloomStore,
			})
		}
		return droppers
	})
	deleteCoord := coordinator.NewDeleteCoordinatorImpl(coordinator.DeleteCoordinatorConfig{
		MaxRetries:          cfg.DeleteMaxRetries,
		RetryBaseIntervalMS: cfg.DeleteRetryBaseMS,
		WAL:                 walImpl,
		RaftNode:            rn,
		IndexManager:        indexMgr,
		DocStore:            ds,
		ChunkStore:          chunkStore,
		ChunkDocMapper:      cdm,
		VersionDocList:      vd,
		VersionBloom:        vBloomStore,
		Dropper:             kbDropper,
	})

	// Same two halves as the whole-KB dropper above, for the same reason: the
	// version's records live in the storage group under a split topology, and
	// this node may hold a copy of its own. The receiving node applies the
	// visibility anchor itself, since it is the one holding the records.
	versionDropper := coordinator.NewDeferredVersionDropper(func() coordinator.VersionStorageDropper {
		droppers := coordinator.VersionStorageDroppers{
			coordinator.BroadcastVersionDropper{Dropper: dataPlane},
		}
		if cfg.Role != NodeRoleControl {
			droppers = append(droppers, coordinator.LocalVersionDropper{
				IndexManager:   indexMgr,
				DocStore:       ds,
				VersionDocList: vd,
				VersionBloom:   vBloomStore,
				Metadata:       rn,
			})
		}
		return droppers
	})
	deleteVersionCoord := coordinator.NewDeleteVersionCoordinatorImpl(coordinator.DeleteVersionCoordinatorConfig{
		MaxRetries:          cfg.DeleteMaxRetries,
		RetryBaseIntervalMS: cfg.DeleteRetryBaseMS,
		WAL:                 walImpl,
		RaftNode:            rn,
		IndexManager:        indexMgr,
		DocStore:            ds,
		VersionDocList:      vd,
		VersionBloom:        vBloomStore,
		Dropper:             versionDropper,
	})

	// --- WAL crash recovery ---
	// Replays interrupted transactions now that the coordinator layer can
	// execute them. Three record kinds are handled:
	//   - DeleteMark:      resume the interrupted DeleteKnowledgeBase flow.
	//   - VersionDelete:   resume the interrupted DeleteVersion flow.
	//   - VersionWrite:    replay the version's storage writes from the
	//     transaction input persisted in the WAL's BEGIN record. A record
	//     without local transaction input (applied by a follower, or a
	//     legacy WAL) cannot be replayed here — the node's data integrity
	//     for it is restored by Raft log replay + DataSync instead — so it
	//     is surfaced via the replay counter for operators.
	//
	// A record that still cannot be replayed (after bounded in-process
	// retries) is skipped and counted, never fatal: it stays in the WAL
	// for the next restart and the node keeps starting up. See the policy
	// comment above runCrashRecovery.
	// Replaying interrupted transactions is storage-layer work: every record it
	// resumes (a version write, a delete) drives a storage coordinator. A control
	// node has none, so running this there dereferences nil and — since it runs
	// during startup — crash-loops the process. That is not a hypothetical: two
	// control nodes were killed and restarted by the fault-injection tests, both
	// came back into a restart loop, the cluster lost quorum, and every test
	// after it timed out waiting for a leader.
	//
	// Its WAL is not idle on a control node: Raft's own records live there. What
	// must not happen is treating those as storage transactions to resume.
	if storageLocal {
		runCrashRecovery(ctx, logger, records, writeCoord, deleteCoord, deleteVersionCoord, walImpl)
	}

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

	// --- Orphan-chunk garbage collector ---
	// The orphan-chunk sweep is a storage-layer task: it walks this node's
	// chunk mappings and reclaims what no version references.
	if storageLocal {
		gcImpl := coordinator.NewChunkGarbageCollectorImpl(coordinator.ChunkGarbageCollectorConfig{
			SweepIntervalSec: cfg.GCSweepIntervalSec,
			WriteMu:          &writeMu, // same mutex as WriteCoordinatorConfig.WriteMu
			RaftNode:         rn,
			ChunkDocMapper:   cdm,
			DocStore:         ds,
			ChunkStore:       chunkStore,
		})
		gcImpl.SetLogger(logger.Named("gc"))
		go gcImpl.Run(ctx)
	}

	// --- Data sync (leader→follower) ---
	// Leader handler: serves storage-layer data to followers via gRPC.
	if storageLocal {
		syncLeader = stratumsync.NewLeaderHandler(
			ds.DB(),
			cdm.DB(),
			vd.DB(),
			chunkStore.VecstoreClient(),
		)
	}

	// Build nodeID→ServiceAddr map for follower leader resolution.
	peerAddrByID := make(map[int64]string, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if p.ServiceAddr != "" {
			peerAddrByID[p.ID] = p.ServiceAddr
		}
	}

	// localAddr is this node's own service address as its peers see it: the
	// address the dispatcher falls back to for local coordination, and the one a
	// writer announces so replicas can find the data it just wrote. A storage
	// node appears in the storage group rather than the Raft member list.
	localAddr := peerAddrByID[cfg.NodeID]
	for _, node := range cfg.StorageNodes {
		if node.ID == cfg.NodeID {
			localAddr = node.Addr
		}
	}

	// resolveReplicaAddrs is the replica topology the write dispatcher walks
	// (§7.13.2) — the same list §8.4 distribution and the §8.5 confirmation use:
	// every member holds the full dataset, so the candidates are the peers other
	// than this node. Assigned here (once peerAddrByID exists) and consumed
	// through closures, so the assembly order does not matter.
	resolveReplicaAddrs = func(_ context.Context) ([]string, error) {
		// The declared storage group when there is one, otherwise the Raft
		// members: before the split every member also stored, and a deployment
		// that declares no storage.nodes keeps exactly that shape.
		if len(cfg.StorageNodes) > 0 {
			addrs := make([]string, 0, len(cfg.StorageNodes))
			for _, node := range cfg.StorageNodes {
				if node.ID == cfg.NodeID {
					continue // this node already holds its own copy
				}
				addrs = append(addrs, node.Addr)
			}
			return addrs, nil
		}
		addrs := make([]string, 0, len(peerAddrByID))
		for id, addr := range peerAddrByID {
			if id == cfg.NodeID {
				continue // this node already holds its own copy
			}
			addrs = append(addrs, addr)
		}
		return addrs, nil
	}

	// Any node may have a fact to report — a replica that just finished
	// writing, or a node running its startup reconcile. Raft still only
	// appends on the leader, so the access layer carries those proposals
	// there instead of the caller having to know who leads
	// (Stratum_设计文档v13.md §7.3/§7.8).
	// Only a node that holds a log forwards a proposal to whoever leads; a
	// storage node reports through the same commands, but they travel to a
	// control node that appends them (RemoteRaftNode).
	if raftNode != nil {
		raftNode.SetNodeID(cfg.NodeID)
		raftNode.SetForwarder(&raft.GRPCProposeForwarder{
			AddrByID: func(id int64) (string, bool) {
				addr, ok := peerAddrByID[id]
				return addr, ok
			},
		})
	}

	// §7.5: which nodes must hold a version before its WAL changes become
	// reclaimable. The static peer list (cfg.Peers) is the right source: "should
	// hold it" is a deployment fact, not a runtime observation — deriving it from
	// who has reported would let a silent node silently leave the requirement.
	requiredReplicaIDs = func() ([]int64, error) {
		// Same source rule as resolveReplicaAddrs: the declared storage group
		// when there is one, every Raft member otherwise.
		var ids []int64
		if len(cfg.StorageNodes) > 0 {
			ids = make([]int64, 0, len(cfg.StorageNodes))
			for _, node := range cfg.StorageNodes {
				ids = append(ids, node.ID)
			}
		} else {
			ids = make([]int64, 0, len(peerAddrByID))
			for id := range peerAddrByID {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return ids, nil
	}

	// Follower: pulls data when this node applies a version written by
	// the leader. The sync module is wired via OnVersionCreated.
	if storageLocal {
		syncFollower = stratumsync.NewFollower(ds, cdm, vd, chunkStore, indexMgr)
	}

	// DataPlane owns "does this node need the version's data, and where from"
	// (stage ① of the control/data separation: replication moved inside the
	// storage layer — control-data-separation-design.md §7).
	// Write-path replication: the coordinator exports the version's records to
	// the other replicas and requires a quorum of acknowledgements before the
	// storage layer reports it durable (Stratum_设计文档v13.md §7.1/§7.2).
	if storageLocal {
		syncPusher = stratumsync.NewPusher(stratumsync.PusherConfig{
			Exporter: syncLeader,
			NodeID:   cfg.NodeID,
		})
	}

	// §8.5: data-source announcements. A writer tells its replicas where a
	// version's data lives — the §7.3 confirmation carries its own address —
	// and the source lookup reads that table before falling back to the leader.
	// That keeps the lookup a map read, which matters because it runs on the
	// Raft apply path; probing peers there is what made the first attempt at
	// §8.5 unusable.
	dataSources := plane.NewDataSourceRegistry()

	dataPlane = plane.NewLocalDataPlane(plane.LocalDataPlaneConfig{
		IndexManager:  indexMgr,
		Puller:        syncFollower,
		WAL:           walImpl,
		Executor:      writeCoord,
		Control:       controlPlane,
		Pusher:        replicaPusher{pusher: syncPusher},
		CursorQuerier: stratumsync.NewLocalVersionQuerier(stratumsync.PresenceCheckerConfig{}),
		// §10.6: a permanently failed version's data is reclaimed locally and
		// on every candidate replica.
		DataDropper:        writeCoord,
		CleanupBroadcaster: stratumsync.NewVersionDataCleaner(stratumsync.PresenceCheckerConfig{}),
		// §7.3: a replica that received a version stands ready to announce it
		// if the coordinator's confirmation never arrives.
		Presence:  stratumsync.NewPresenceChecker(stratumsync.PresenceCheckerConfig{}),
		Digest:    syncFollower,
		Confirmer: stratumsync.NewConfirmBroadcaster(stratumsync.PresenceCheckerConfig{}),
		// §8.4: "建一次、分发 N 份" — the builder reads its own index files and
		// ships them; replicas install them instead of building their own.
		IndexReader:     indexMgr,
		IndexShipper:    stratumsync.NewIndexPusher(),
		ResolveReplicas: resolveReplicaAddrs,
		// §7.5: a backfill can replay the changes a peer recorded instead of
		// pulling each version in full. Same dialer config as the other
		// peer-facing helpers.
		ChangesFetcher: stratumsync.NewVersionChangesPuller(stratumsync.PresenceCheckerConfig{}),
		// §7.5/§6.4: the backfill asks the metadata whether a version still exists
		// before advancing its cursor over one whose pull returned no records.
		// "empty" and "deleted" are indistinguishable at the storage layer, and the
		// control plane is the side that knows.
		VersionExistence: controlPlane,
		Verify: func(ctx context.Context, kbID string, versionID int64) bool {
			return verifyVersionPull(ctx, rn, vd, kbID, versionID)
		},
		// Resolve answers "who holds this version's data": the §8.5 table first
		// (a writer announced itself), then the pre-§8.5 answer — the leader.
		// Both halves are lookups; nothing here dials a peer, because this runs
		// on the Raft apply path.
		Resolve: plane.ResolverWithRegistry(dataSources, func(ctx context.Context, kbID string, versionID int64) (string, bool, error) {
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
			// 不可达的 leader 地址发起阻塞式 PullVersion,整个 apply 循环会
			// 卡死,后续所有日志永远无法应用。
			//
			// 注意本函数必须是"廉价、不探测"的查找：它跑在 Raft apply 路径上,
			// 任何在这里发起的阻塞式 RPC 都会拖住后续日志的应用。§8.5 的
			// "任意节点当协调者"就建立在这一点上：协调者写完主动告知
			// (dataSources 那张表),而不是让拉取方在这里逐个 peer 探测。
			if status.LeaderID == cfg.NodeID {
				return "", false, nil
			}
			addr, ok := peerAddrByID[status.LeaderID]
			if !ok {
				return "", false, fmt.Errorf("sync: leader address unknown for node ID %d", status.LeaderID)
			}
			return addr, true, nil
		}),
		// §8.5: announce this node as the holder of the versions it writes, so
		// replicas learn where the data is. It is its own DataSyncService
		// address — the same gRPC endpoint serves every service.
		SelfDataSyncAddr: localAddr,
		Logger:           logger,
	})

	// One plane serves both paths: the write path runs its storage transaction
	// through it, and the read/sync path resolves pulls through it.
	writeCoord.SetDataPlane(dataPlane)

	// The pull path is the third: it is what knows a version's records have
	// landed here, and this plane is what owns the cursor §9.3(2) reads. Wired
	// after the plane exists because the follower is built well before it.
	if syncFollower != nil {
		syncFollower.SetLocalVersionAdvancer(dataPlane)
	}

	// §7.13.2: the dispatcher picks a write's coordinator and hands it the work.
	// It is built here because it needs the data plane (for the local fallback)
	// and the replica topology, and it is published through the closure the
	// coordinator captured above.
	// §7.13.2/§2.2: the write goes to a candidate from the KB's replica topology,
	// tried in order until one takes it.
	//
	// A control node dispatches too — it may be the leader at apply time, and it
	// holds no data. What it must NOT be is one of its own candidates: it is not a
	// replica. LocalWrite is precisely what puts this node into candidates() (that
	// list only ever gains selfAddr when localhost != nil), and on a control node
	// dataPlane is nil. A nil receiver's method value is legal to form, so nothing
	// complains there; it panics only when called, deep inside the write
	// transaction — which is how a nil pointer in a dispatcher surfaced as a
	// stack trace through the storage layer.
	//
	// So: set LocalWrite only where there is a data plane. On a control node the
	// candidates are the storage group and every dispatch is remote, which is what
	// the topology asks for.
	dispatcherCfg := plane.CoordinatorDispatcherConfig{
		Replicas: resolveReplicaAddrs,
		SelfAddr: localAddr,
		Logger:   logger,
	}
	if storageLocal {
		// Coordinating locally is the last resort: it keeps a single-node cluster
		// — or one whose peers are all unreachable — progressing without a second
		// code path for the write.
		dispatcherCfg.LocalWrite = dataPlane.WriteVersionData
	}
	writeDispatcher := plane.NewCoordinatorDispatcher(dispatcherCfg)
	dispatchVersionWrite = func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
		// The coordinator's contract reports only success or failure — which node
		// took the write is the dispatcher's business. The apply hook below, which
		// wants the address for its log line, calls writeDispatcher directly.
		_, err := writeDispatcher.Dispatch(ctx, kbID, versionID, parentVersionID, changes)
		return err
	}

	// §8.4: with the data plane in place, a completed build ships its index to
	// the other replicas so they load it instead of building their own. Best
	// effort — a replica that cannot be reached builds for itself, which is the
	// behaviour the cluster had before distribution existed.
	distributeIndex = func(kbID string, versionID int64) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := dataPlane.PushIndexToReplicas(ctx, kbID, versionID); err != nil {
			logger.Warn("index distribution failed",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		}
	}

	// Startup maintenance across the contract: the storage layer trims index
	// files by the retention policy, then reconciles — and the control layer
	// promotes versions whose READY proposal was lost
	// (control-data-separation-design.md §5.3/§7). Retention runs first so the
	// reconcile sees post-retention disk facts.
	// Both halves are about on-disk index facts this node is assumed to have.
	if storageLocal {
		if err := dataPlane.EnforceRetention(ctx, rn); err != nil {
			logger.Warn("index retention: ListKnowledgeBases failed", zap.Error(err))
		}
		reconcileIndexStatus(ctx, logger, dataPlane, controlPlane, rn, cfg.IndexRetentionCount)
	}

	// §10.6: a cleanup broadcast that failed is retried in the background, so
	// orphaned data does not depend on someone noticing a log line.
	dataPlane.StartCleanupRetries(ctx)

	// This callback is the apply loop's, so only a node with an apply loop has
	// it: a storage node is never told "a version was created", it is handed the
	// version's data directly (ExecuteVersionWrite).
	// Only a node that holds data answers this: it is the hook that pulls a
	// version's records here and builds its index. A control node's versions
	// live in the storage group, and it has no stores to pull them into.
	if storageLocal && raftNode != nil {
		raftNode.SetOnVersionCreated(func(kbID string, versionID int64) {
			// §8.6b: only the active version is worth building eagerly — it is what
			// ordinary queries hit. Every other version still gets its data (the
			// data has to be here for the version to count as durable) but no
			// index; a query that does reach one builds it lazily, by scanning if
			// the version is small and by waiting for a build if it is not.
			//
			// "Which version is active" is read from the replicated metadata rather
			// than pushed down: the storage layer already has a read-only view of
			// it (§7.0's MetadataLister), so no new control-layer channel is needed.
			active := false
			if kb, err := rn.GetKB(context.Background(), kbID); err == nil && kb.ActiveVersionID == versionID {
				active = true
			}
			if active {
				if err := dataPlane.EnsureIndex(context.Background(), kbID, versionID); err != nil {
					logger.Error("sync: version data pull did not converge",
						zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
				}
				return
			}
			if err := dataPlane.FetchVersionData(context.Background(), kbID, versionID); err != nil {
				logger.Error("sync: version data fetch did not converge",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			}
		})
	}

	// §7.13.2: only a node that leads AT APPLY TIME dispatches (see
	// RaftNodeImpl.SetOnVersionCommittedAsLeader) — one dispatch per write, not
	// one per replica. The changes come from the registration Execute left behind,
	// since the Raft command carries none (§7.7). TAKING it is what keeps the
	// apply hook and Execute's own background dispatch from both acting: whichever
	// arrives first wins, the other becomes a no-op.
	if raftNode != nil {
		raftNode.SetOnVersionCommittedAsLeader(func(kbID string, versionID, parentVersionID int64, clientRequestID string) {
			regParentID, changes, ok := writeCoord.TakePendingDispatch(kbID, clientRequestID)
			if !ok {
				// Nothing registered: this node never held the changes (it did not
				// accept the write), or the dispatch already happened. Leaving it is
				// correct — a retry by the writer is what fills the gap (§7.12).
				logger.Warn("version committed here without a pending dispatch",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
				return
			}
			if regParentID != 0 {
				parentVersionID = regParentID
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if _, err := writeDispatcher.Dispatch(ctx, kbID, versionID, parentVersionID, changes); err != nil {
				logger.Warn("write dispatch failed; the client's retry or the retry budget takes over",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			}
		})
	}

	// --- gRPC services ---
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
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(service.UnaryAuthGate(cfg.RequireAuthenticated)),
		grpc.ChainStreamInterceptor(service.StreamAuthGate(cfg.RequireAuthenticated)),
	)

	if raftNode != nil {
		kbSvc := service.NewKnowledgeBaseService(rn, writeCoord, deleteCoord, deleteVersionCoord)
		// §9.3(1)/§4.3(1): the station's route table reads "which nodes hold this
		// version" from the leader's §7.13.4 aggregate instead of probing storage
		// nodes itself. The adapter bridges plane's Holder to the service's own
		// type so the service does not import plane.
		kbSvc.SetVersionHolderSource(versionHolderSource{controlPlane})
		pb.RegisterKnowledgeBaseServiceServer(grpcServer, kbSvc)
	}

	// Data-missing detection (Stratum_设计文档v13.md §7.12): the control layer
	// asks every candidate replica whether it holds a version whose data may
	// never have landed, and surfaces the ones nobody has.
	// These read the local stores by construction, so only a node holding data
	// can build them. A control node answers neither.
	if storageLocal {
		presenceChecker := stratumsync.NewPresenceChecker(stratumsync.PresenceCheckerConfig{})
		querySvc := service.NewQueryService(rn, indexMgr, cdm, vd, ds, vBloomStore)
		// §9.3(2): honouring a freshness credential requires being able to
		// answer "how far does my history reach". Without this the node can only
		// ignore credentials, which is not the same as refusing a stale answer.
		querySvc.SetLocalVersionReporter(dataPlane)
		adminSvc := service.NewAdminService(cfg.NodeID, rn, indexMgr, ds, chunkStore, walImpl,
			func() []string {
				addrs, err := resolveReplicaAddrs(context.Background())
				if err != nil {
					return nil
				}
				return addrs
			},
			presenceChecker,
		)
		// §8.6(d): surface versions whose dead weight cannot be collected because
		// too few replicas would remain serving. Nothing else clears that — the
		// remedy is a larger replica count — so it goes out with the other
		// needs-a-human signals rather than only into the node's log.
		adminSvc.SetGCPressureReporter(indexMgr)
		pb.RegisterQueryServiceServer(grpcServer, querySvc)
		pb.RegisterAdminServiceServer(grpcServer, adminSvc)
	}

	// One data-plane service per node: it both exports versions to peers (the
	// pull path) and receives pushed ones, and answers presence/cursor
	// queries (Stratum_设计文档v13.md §7.2/§7.6).
	nodeHandler := stratumsync.NewNodeHandler(
		syncLeader,
		stratumsync.NewPushHandler(syncFollower, cfg.NodeID,
			stratumsync.WithLocalVersion(dataPlane),
			// §9.3(2): receiving a version's records is what makes this node
			// hold it; without this the cursor stays 0 and the station's
			// freshness check refuses a replica whose data is complete.
			stratumsync.WithLocalVersionAdvancer(dataPlane),
			stratumsync.WithVersionDataDropper(writeCoord),
			stratumsync.WithVersionWriteWatcher(dataPlane),
			stratumsync.WithIndexInstaller(indexMgr),
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
			// §7.13.4: only the control leader keeps the aggregate of the storage
			// layer's periodic cursor reports. The gate below clear-s it on each
			// leadership term, so a new leader never answers "who holds V" from
			// reports it never received.
			stratumsync.WithDataVersionAggregator(dataVersionRegistry, dataVersionGate.IsLeader),
			// §7.5: carry the reclaim watermark back on each accepted report, so the
			// node that WROTE the data can discard its recorded changes even when it
			// is not the leader (§7.13.2).
			stratumsync.WithReclaimWatermarks(controlPlane)),
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
		logger.Fatal("failed to listen", zap.String("addr", cfg.GRPCAddr), zap.Error(err))
	}

	// --- Signal handling ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
		grpcServer.GracefulStop()
	}()

	// §7.13.4: report this node's data cursors to the control leader every
	// interval. The leader it resolves is looked up per interval, never cached:
	// caching would pin the reporter to a leadership term that has ended. The
	// report is soft state, so a failed one is only logged and the next interval
	// sends the whole view again.
	go stratumsync.NewDataVersionReporter(stratumsync.DataVersionReporterConfig{
		NodeID: cfg.NodeID,
		// §3.1: the leader's aggregate hands this address to consumers (the
		// station's route table), so a holder is something they can dial rather
		// than an id each of them would have to map on its own.
		SelfAddr:     localAddr,
		DataVersions: dataPlane.DataVersionsSnapshot,
		ResolveLeader: func(rctx context.Context) (string, bool, error) {
			status, err := rn.GetClusterStatus(rctx)
			if err != nil {
				return "", false, fmt.Errorf("GetClusterStatus: %w", err)
			}
			if !status.HasLeader {
				return "", false, nil
			}
			addr, ok := peerAddrByID[status.LeaderID]
			if !ok {
				return "", false, fmt.Errorf("sync: leader address unknown for node ID %d", status.LeaderID)
			}
			return addr, true, nil
		},
		// §7.5: store the watermarks the leader carries back, so this node's WAL can
		// be reclaimed even when this node is not the leader.
		Watermarks: controlPlane,
		Logger:     logger,
	}).Run(ctx)

	// §7.5: reclaim the WAL's recorded changes once every replica holds the versions
	// behind them. This is a background loop on purpose — compacting rewrites a file,
	// and nothing on the apply path may block on it. Each pass asks the control plane
	// for the watermarks (locally as leader, or as carried back on the report), so a
	// node that writes data reclaims even when it does not lead.
	go plane.NewWALReclaimer(plane.WALReclaimerConfig{
		Reclaimer: dataPlane,
		Logger:    logger,
	}).Run(ctx)

	logger.Info("Stratum gRPC server listening", zap.String("addr", cfg.GRPCAddr))
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
// contract: a finished build reports the index ready, a failed one reports the
// version as unavailable (control-data-separation-design.md §4.2/§4.3).
func reportIndexStatus(ctx context.Context, cp plane.ControlPlane, kbID string, versionID int64, status types.IndexStatus) error {
	switch status {
	case types.IndexStatusReady:
		return cp.ReportIndexReady(ctx, kbID, versionID)
	case types.IndexStatusFailed:
		return cp.ReportAvailability(ctx, kbID, versionID, plane.AvailabilityUnavailable)
	default:
		return fmt.Errorf("index build reported unexpected status %v for version %d", status, versionID)
	}
}

func replayCoordinatorCall(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 1; attempt <= crashReplayMaxAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt < crashReplayMaxAttempts {
			select {
			case <-time.After(time.Duration(crashReplayRetryBaseMillis<<(attempt-1)) * time.Millisecond):
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
) {
	for _, rec := range records {
		switch rec.Type {
		case types.PendingRecordTypeDeleteMark:
			logger.Info("crash recovery: resuming interrupted knowledge-base deletion",
				zap.String("kb_id", rec.KBID))
			if err := replayCoordinatorCall(ctx, func() error { return dc.Execute(ctx, rec.KBID) }); err != nil {
				logger.Warn("crash recovery: skipping KB-deletion resume after retries exhausted",
					zap.String("kb_id", rec.KBID), zap.Error(err))
				w.IncrementReplayCounter(rec)
				continue
			}

		case types.PendingRecordTypeVersionDelete:
			logger.Info("crash recovery: resuming interrupted version deletion",
				zap.String("kb_id", rec.KBID), zap.Int64("version_id", rec.VersionID))
			if err := replayCoordinatorCall(ctx, func() error { return dvc.Execute(ctx, rec.KBID) }); err != nil {
				logger.Warn("crash recovery: skipping version-deletion resume after retries exhausted",
					zap.String("kb_id", rec.KBID), zap.Int64("version_id", rec.VersionID), zap.Error(err))
				w.IncrementReplayCounter(rec)
				continue
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
			err := replayCoordinatorCall(ctx, func() error {
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
func reconcileIndexStatus(ctx context.Context, logger *zap.Logger, dp *plane.LocalDataPlane, cp plane.ControlPlane, meta plane.MetadataLister, retentionCount int) {
	durable, err := dp.ReconcileIndexes(ctx, meta, retentionCount)
	if err != nil {
		logger.Warn("index reconcile: storage-layer reconcile failed", zap.Error(err))
		return
	}

	// §7.8: the contiguous cursor lives in memory, so a restarted node starts
	// from nothing and cannot tell how far behind it is. What it may claim
	// durable is therefore bounded by what a quorum of its peers still reports
	// holding. Without a replica set the local view stands — there is nobody to
	// disagree with.
	//
	// §7.9: the payload keeps the two sides separate — a scalar cursor per KB
	// for the data side (linearization makes a scalar sufficient), an explicit
	// version set for the index side (readiness cannot be collapsed).
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
				// No quorum means no safe claim. Reporting the local view
				// anyway is precisely the "control layer runs ahead of the
				// data" failure the epoch exists to prevent.
				logger.Warn("index reconcile: no quorum for a durable claim; skipping this knowledge base",
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
	// Internal collaboration (DataSyncService / InternalService) is never gated:
	// those calls carry no end user's credential, and requiring one would break
	// fan-out, catch-up and Raft forwarding outright.
	RequireAuthenticated bool

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

	VecstoreGRPCAddr string
	EmbedServiceAddr string

	IndexLRUCapacity         int
	IndexLoadWaitTimeout     time.Duration
	IndexCallbackMaxRetries  int
	IndexCallbackRetryBaseMS int

	// IndexRetentionCount keeps the most recent N on-disk index files per
	// knowledge base (gc.version_retention_count); <= 0 keeps everything.
	IndexRetentionCount int

	// IndexMemoryThresholdMB bounds estimated in-memory footprint of all
	// loaded indexes (index_manager.memory_threshold_mb); <= 0 disables.
	IndexMemoryThresholdMB int64

	// IndexColdThreshold is how long a version may go unqueried before
	// the background evaluator rebuilds it in the graph-free form
	// (index_manager.cold_threshold_ms, §8.6a); <= 0 disables the policy.
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

	// IndexColdSweepInterval is how often that evaluator re-reads the
	// access table (index_manager.cold_sweep_interval_ms); <= 0 means the
	// IndexManager's default.
	IndexColdSweepInterval time.Duration

	// IndexAppendMaxDeadRatio bounds the dead weight a §8.6(c) pure-append
	// reuse may carry (index_manager.append_max_dead_ratio); <= 0 means the
	// IndexManager's default, 1.0 disables the check.
	IndexAppendMaxDeadRatio float64

	// IndexGCEnabled turns on §8.6(d) collection (index_manager.gc_enabled). Off
	// by default: the scanner always runs and only reports, but collection
	// rewrites an artifact that is currently SERVING queries, so turning it on is
	// an operator's decision — the log line and the GetSystemStatus entry are the
	// signals that it is worth making.
	IndexGCEnabled bool

	// IndexServingReplicaMin is how many OTHER replicas must be serving a version
	// before this node takes itself out of service to collect it
	// (index_manager.serving_replica_min); <= 0 means the IndexManager's default.
	IndexServingReplicaMin int

	// IndexGCGraphRebuildRatio is the dead-share bar for rebuilding a graphed
	// artifact (index_manager.graph_rebuild_ratio); <= 0 means the IndexManager's
	// default. Much higher than IndexAppendMaxDeadRatio on purpose: a rebuild
	// costs the whole graph.
	IndexGCGraphRebuildRatio float64

	// IndexGCSweepInterval is how often the §8.6(d) scanner re-estimates the dead
	// share (index_manager.gc_sweep_interval_ms); <= 0 means the IndexManager's
	// default, negative disables the scanner. Shortening it is cheap — a scan only
	// reads local state — but collection candidates are only ever produced by a
	// scan, so disabling the scanner disables collection too.
	IndexGCSweepInterval time.Duration

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
}

// fileConfig mirrors the on-disk YAML schema (configs/config1.yaml and
// integration/docker/config{1,2,3}.yaml). Fields left unset fall back to
// defaultConfig()'s values.
type fileConfig struct {
	Node struct {
		NodeID               int64  `yaml:"node_id"`
		Role                 string `yaml:"role"`
		GRPCAddr             string `yaml:"grpc_addr"`
		RaftAddr             string `yaml:"raft_addr"`
		RequireAuthenticated bool   `yaml:"require_authenticated"`
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
	} `yaml:"raft"`

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
	} `yaml:"vecstore"`

	Embed struct {
		ServiceAddr string `yaml:"service_addr"`
	} `yaml:"embed"`

	IndexManager struct {
		LRUCapacity         int `yaml:"lru_capacity"`
		MemoryThresholdMB   int `yaml:"memory_threshold_mb"`
		LoadWaitTimeoutMS   int `yaml:"load_wait_timeout_ms"`
		CallbackMaxRetries  int `yaml:"callback_max_retries"`
		CallbackRetryBaseMS int `yaml:"callback_retry_base_interval_ms"`
		ColdThresholdMS     int `yaml:"cold_threshold_ms"`
		// BuildAbandonTimeoutMS is the §6 abandoned-artifact window. <= 0 takes
		// the default (30 minutes); negative disables the sweeper.
		BuildAbandonTimeoutMS int     `yaml:"build_abandon_timeout_ms"`
		ColdSweepIntervalMS   int     `yaml:"cold_sweep_interval_ms"`
		AppendMaxDeadRatio    float64 `yaml:"append_max_dead_ratio"`
		// §8.6(d) collection. GCEnabled is the opt-in: the scanner always runs (it
		// only reads), but rewriting a SERVING artifact happens only when an
		// operator says so.
		GCEnabled bool `yaml:"gc_enabled"`
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
	} `yaml:"index_manager"`

	WriteCoordinator struct {
		MaxRetries          int `yaml:"max_retries"`
		RetryBaseIntervalMS int `yaml:"retry_base_interval_ms"`
	} `yaml:"write_coordinator"`

	DeleteCoordinator struct {
		MaxRetries          int `yaml:"max_retries"`
		RetryBaseIntervalMS int `yaml:"retry_base_interval_ms"`
	} `yaml:"delete_coordinator"`

	BloomFilter struct {
		ExpectedItems     uint64  `yaml:"expected_items"`
		FalsePositiveRate float64 `yaml:"false_positive_rate"`
	} `yaml:"bloom_filter"`

	GC struct {
		VersionRetentionCount int `yaml:"version_retention_count"`
		SweepIntervalSec      int `yaml:"sweep_interval_s"`
	} `yaml:"gc"`
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
	if fc.Raft.MaxLogLength > 0 {
		cfg.MaxLogLength = uint64(fc.Raft.MaxLogLength)
	}
	if fc.Node.RequireAuthenticated {
		cfg.RequireAuthenticated = true
	}
	if fc.Node.Role != "" {
		role, err := parseNodeRole(fc.Node.Role)
		if err != nil {
			return cfg, fmt.Errorf("parse %s: %w", path, err)
		}
		cfg.Role = role
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
	if fc.IndexManager.LRUCapacity != 0 {
		cfg.IndexLRUCapacity = fc.IndexManager.LRUCapacity
	}
	if fc.IndexManager.MemoryThresholdMB != 0 {
		cfg.IndexMemoryThresholdMB = int64(fc.IndexManager.MemoryThresholdMB)
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
	// The abandoned-artifact window is independent of the cold policy: a node
	// that never reshapes a version still leaves remains behind when a build
	// dies, and this sweeper is the only thing that reclaims them (§8.8).
	// Parsing it inside the cold-threshold branch — where it used to live —
	// meant that setting only build_abandon_timeout_ms silently did nothing.
	if ms := fc.IndexManager.BuildAbandonTimeoutMS; ms != 0 {
		cfg.IndexBuildAbandonTimeout = time.Duration(ms) * time.Millisecond
	}
	if fc.IndexManager.ColdSweepIntervalMS != 0 {
		cfg.IndexColdSweepInterval = time.Duration(fc.IndexManager.ColdSweepIntervalMS) * time.Millisecond
	}
	if fc.IndexManager.AppendMaxDeadRatio != 0 {
		cfg.IndexAppendMaxDeadRatio = fc.IndexManager.AppendMaxDeadRatio
	}
	// §8.6(d). gc_enabled is a plain bool: absent and false both mean "collect
	// nothing", which is the only safe reading of a config file that predates the
	// feature.
	cfg.IndexGCEnabled = fc.IndexManager.GCEnabled
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

	return cfg, nil
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

		// Disk retention and memory thresholds default to disabled (0).
		// They activate only when the YAML config sets
		// gc.version_retention_count / index_manager.memory_threshold_mb.
		IndexRetentionCount:    0,
		IndexMemoryThresholdMB: 0,

		// §8.6a cold-version policy: also off by default, so an
		// unconfigured node keeps building a full HNSW graph for every
		// version. Enable with index_manager.cold_threshold_ms (and
		// optionally cold_sweep_interval_ms).
		IndexColdThreshold:     0,
		IndexColdSweepInterval: 0,

		// §8.6(c) pure-append reuse: 0 means the IndexManager's own default
		// ratio (20% dead vectors). Set append_max_dead_ratio to 1.0 to never
		// let dead weight force a rebuild.
		IndexAppendMaxDeadRatio: 0,

		// §8.6(d) collection: 0/false means the IndexManager's own defaults, which
		// are "do not collect" for gc_enabled and 2 for serving_replica_min. The
		// scanner still runs and reports, so a node that has never been configured
		// for collection will still say when collection would have been worth it.
		IndexGCEnabled:           false,
		IndexServingReplicaMin:   0,
		IndexGCGraphRebuildRatio: 0,
		IndexGCSweepInterval:     0,

		WriteMaxRetries:   3,
		WriteRetryBaseMS:  100,
		DeleteMaxRetries:  5,
		DeleteRetryBaseMS: 500,

		// Bloom filters: sized for ~1M keys at 1% false-positive rate,
		// matching the bloom_filter section in configs/config1.yaml.
		BloomExpectedItems:     1_000_000,
		BloomFalsePositiveRate: 0.01,

		GCSweepIntervalSec: 300,
	}
}

// versionHolderSource adapts the control plane's data-version aggregate to what
// the knowledge base service exposes to the station (§3.1).
//
// It exists to keep the dependency pointing one way: service does not import
// plane, so the two Holder types (identical in content) are bridged here, where
// both are already visible.
type versionHolderSource struct {
	cp *plane.LocalControlPlane
}

// DataVersionHolders reports which nodes hold kbID at or past versionID, and
// whether this node is the control leader — a non-leader has folded no reports,
// so its empty answer means "nobody I have heard from", never "nobody has it".
func (s versionHolderSource) DataVersionHolders(kbID string, versionID int64) ([]service.VersionHolder, bool) {
	holders, ok := s.cp.DataVersionHolders(kbID, versionID)
	if !ok {
		return nil, false
	}
	out := make([]service.VersionHolder, 0, len(holders))
	for _, h := range holders {
		out = append(out, service.VersionHolder{NodeID: h.NodeID, Address: h.Address})
	}
	return out, true
}
