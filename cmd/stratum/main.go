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
	"go.uber.org/zap/zapcore"
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

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	// Bound to the VARIABLE, not to the bootstrap logger: logging.level rebuilds
	// `logger` once the config file has been read (below), and that rebuilt one is
	// the one whose buffers need flushing.
	defer func() { _ = logger.Sync() }()

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
	// How long a required replica may go quiet before the storage-redundancy
	// verdict stops counting it live (docs/storage-degradation-signal-plan.md
	// §3.2). Zero keeps the plane's own default, which is three report intervals.
	//
	// A flag rather than a YAML key on purpose: it is one of the §10.4 numbers
	// still to be calibrated against a real deployment, and a wrong verdict is
	// retryable by design, so getting it wrong costs a retry rather than an
	// outage.
	storageSilenceWindowFlag := flag.Duration("storage-silence-window", 0,
		"how long a required replica may go without reporting before it stops counting as live (default: 3 report intervals)")
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

	// Now that the config file has been read (and the flags applied), honour
	// logging.level. The bootstrap logger above is info, which is also the
	// default; a deployment that asks for debug gets it from here on — including
	// the query path's per-stage timings.
	logger = loggerAtLevel(logger, cfg.LogLevel)

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
	var chunkSplitter splitter.ChunkSplitter
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
		}),
		// §3.2: how long a required replica may go quiet before the redundancy
		// verdict stops counting it live. Zero keeps the plane's default, so an
		// unconfigured node behaves like every node did before this existed.
		plane.WithStorageSilenceWindow(*storageSilenceWindowFlag))
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
		// §10.1: a version whose data never landed has to reach the failure
		// accounting instead of sitting PENDING forever — see AbandonDispatch.
		ControlPlane: controlPlane,
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
		// A replica that receives a version's data builds its document filter, exactly as
		// the writer's transaction does: the filter drops the search hits that are not in
		// the version, so one built before the data arrived is empty — and an empty filter
		// answers "nothing matched", with no error, for data this node holds in full.
		syncFollower.SetVersionBloom(vBloomStore)
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

	// §8.5's fourth layer (docs/data-source-holders-fallback-plan.md): the table
	// above is filled only by a writer's confirmation, which is a bounded
	// fire-and-forget broadcast. A replica that missed it has no source at all —
	// the leader this lookup falls back to is the control leader, which in a
	// two-tier deployment exports no data and refuses. This cache mirrors the
	// control leader's §7.13.4 aggregate (built from the report every storage
	// node already sends), so such a replica can still find one.
	//
	// The lookup stays a pure map read: it runs on the Raft apply path, where
	// dialling anyone is what made the first attempt at §8.5 unusable. The
	// misses it queues are serviced by the data-version report below, which
	// already runs on an interval and already resolves the leader.
	resolveControlLeader := func(rctx context.Context) (string, bool, error) {
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
	}
	// The mirror of the control leader's §7.13.4 aggregate, read by the data-source
	// lookup. It is filled FROM the report response (see the reporter wiring below),
	// not by a refresh of its own: the lookup may run on the Raft apply path, so it
	// can only read memory, and the heartbeat already talks to the leader every
	// interval — carrying the answer back is one map in a response that is being
	// sent anyway.
	holdersCache := plane.NewHoldersCache(plane.HoldersCacheConfig{
		Logger: logger,
	})

	dataPlane = plane.NewLocalDataPlane(plane.LocalDataPlaneConfig{
		IndexManager: indexMgr,
		Puller:       syncFollower,
		WAL:          walImpl,
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
		// The same topology resolveReplicaAddrs walks, counted this time: how many
		// replicas should hold a written version. fanOut reads it to recognise the
		// single-replica case, where an empty target list is the expected answer
		// rather than a peer list that failed to build. Declared storage group
		// when there is one (it includes this node), the Raft members otherwise —
		// the same choice resolveReplicaAddrs makes two lines up.
		ReplicaCount: func() int {
			if len(cfg.StorageNodes) > 0 {
				return len(cfg.StorageNodes)
			}
			return len(cfg.Peers)
		}(),
		// §8.4(a): bound how many distributions run at once; 0 takes the
		// default (DefaultMaxConcurrentIndexPush).
		MaxConcurrentIndexPush: cfg.IndexPushConcurrency,
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
		// (a writer announced itself), then the control leader's aggregate read
		// from the local mirror, then the pre-§8.5 answer — the leader. All three
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
	//
	// Fire-and-forget, deliberately. This runs inside the build callback, which
	// runs on a buildPool worker; shipping an index means reading the whole file
	// into memory and streaming it to N replicas, and PushIndexToReplicas parks
	// on a bounded semaphore while a distribution storm is in progress. Inline,
	// that would hold a build worker for the whole push — and under enough
	// storms every worker would queue behind distribution while the builds that
	// queries and writes are waiting on stall. The 2-minute timeout and
	// context.Background() below already described a call nobody waits for; this
	// makes it one.
	// One distribution per version at a time. The build callback is retried on
	// failure (invokeCallback, up to 4 attempts) and every attempt calls this
	// again; while distribution ran inline those attempts were strictly
	// sequential, so a version was never shipped twice at once. Off the worker
	// they can overlap — and overlapping ships of one version are exactly what
	// lets a receiver's installs interleave (measured: an index/sidecar checksum
	// mismatch on the receiving node). A ship already in flight is the same work,
	// so skip it; once it finishes, a later attempt may try again.
	distributing := sync.Map{}
	distributeIndex = func(kbID string, versionID int64) {
		key := fmt.Sprintf("%s\x00%d", kbID, versionID)
		if _, inFlight := distributing.LoadOrStore(key, struct{}{}); inFlight {
			return
		}
		go func() {
			defer distributing.Delete(key)
			// A panic here used to be caught by doBuild's recover, which marked
			// the version FAILED. Off the worker goroutine there is nothing above
			// to catch it, so it would take the process down.
			defer func() {
				if r := recover(); r != nil {
					logger.Error("index distribution panicked",
						zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Any("panic", r))
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := dataPlane.PushIndexToReplicas(ctx, kbID, versionID); err != nil {
				logger.Warn("index distribution failed",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			}
		}()
	}

	// Startup maintenance across the contract: the storage layer trims index
	// files by the retention policy, then reconciles — and the control layer
	// promotes versions whose READY proposal was lost
	// (control-data-separation-design.md §5.3/§7). Retention runs first so the
	// reconcile sees post-retention disk facts.
	// Both halves are about on-disk index facts this node is assumed to have.
	//
	// epochDurable is the durable set the reconcile established here. The §7.9
	// payload is published from it later — after the gRPC server is up, not here;
	// see reportEpochWhenPeersAreUp.
	var epochDurable []plane.VersionRef
	if storageLocal {
		if err := dataPlane.EnforceRetention(ctx, rn); err != nil {
			logger.Warn("index retention: ListKnowledgeBases failed", zap.Error(err))
		}
		epochDurable = reconcileIndexStatus(ctx, logger, dataPlane, controlPlane, rn, cfg.IndexRetentionCount)
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
			// §7.5: as of this apply the version EXISTS, whether or not this node
			// ends up holding it. Telling the plane is what keeps its cursor
			// honest — a version announced but not held blocks the cursor from
			// stepping over it, so a lost push can no longer make this node claim
			// an unbroken history it does not have.
			//
			// Announcing for EVERY applier (not only the ones that will fetch) is
			// deliberate: the node that never receives the data is exactly the one
			// that needs the gap recorded.
			dataPlane.AnnounceVersion(kbID, versionID)

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
			dispatchCommittedVersion(writeCoord, writeDispatcher, logger, kbID, versionID, parentVersionID, clientRequestID)
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
		// §9.3(1)/§4.3(1): the station's route table reads "which nodes hold this
		// version" from the leader's §7.13.4 aggregate instead of probing storage
		// nodes itself. The adapter bridges plane's Holder to the service's own
		// type so the service does not import plane.
		kbSvc.SetVersionHolderSource(versionHolderSource{controlPlane})
		// §4.3: the same aggregate's other answer — whether a write can still
		// reach quorum — so a doomed write is refused before the version number,
		// the Raft entry and the retry budget are spent on it. An unreadable
		// verdict allows the write, which is how a leadership change stays a
		// failover rather than an outage.
		kbSvc.SetStorageDegradationSource(versionHolderSource{controlPlane})
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
		// §4.5: report storage redundancy through health details. It stays out of
		// the health STATUS on purpose — a probe that reported UNHEALTHY here
		// would pull traffic off a node whose reads are still being served. The
		// verdict is unknown on a node that does not lead, and unknown reports
		// nothing rather than vouching for storage it cannot see.
		adminSvc.SetStorageDegradationSource(versionHolderSource{controlPlane})
		pb.RegisterQueryServiceServer(grpcServer, querySvc)
		pb.RegisterAdminServiceServer(grpcServer, adminSvc)
	} else {
		// A node without stores answers Unimplemented for every admin call that
		// reads them — but GetClusterStatus it answers in full, because the Raft
		// view lives only here. Without this registration a storage node cannot
		// resolve the control leader at all: every cursor report fails with
		// "Unimplemented: unknown service stratum.AdminService", so §7.13.4's
		// report never leaves the node, and the chain tails (lag catch-up), the
		// holder view and the station's read routing go with it.
		pb.RegisterAdminServiceServer(grpcServer, newControlAdminService(cfg.NodeID, rn))
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
			// §7.13.4: only the control leader keeps the aggregate of the storage
			// layer's periodic cursor reports. The gate below clear-s it on each
			// leadership term, so a new leader never answers "who holds V" from
			// reports it never received.
			stratumsync.WithDataVersionAggregator(dataVersionRegistry, dataVersionGate.IsLeader),
			// §7.5: carry the reclaim watermark back on each accepted report, so the
			// node that WROTE the data can discard its recorded changes even when it
			// is not the leader (§7.13.2).
			stratumsync.WithReclaimWatermarks(controlPlane),
			// docs/active-lag-detection-design.md: and the chain tail, for the same
			// reason — the leader is the only one that knows where the chain ends, and
			// the reporter is the one that must decide whether it has fallen behind.
			stratumsync.WithChainTails(controlPlane),
			// docs/data-source-holders-fallback-plan.md: and the holders, which the
			// reporter mirrors so its data-source lookup can answer without dialling —
			// that lookup may run on the Raft apply path, where it cannot ask.
			stratumsync.WithHolders(dataVersionRegistry)),
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

	// node.metrics_addr: the Prometheus endpoint, when the deployment asked for
	// one. Started before the signal handler so the handler can close it, and a
	// listen failure is only a warning — losing metrics must not keep the node
	// from serving.
	metricsSource := nodeMetricsSource{}
	if indexMgr != nil {
		metricsSource.LoadedIndexes = indexMgr.LoadedCount
	}
	if chunkStore != nil {
		metricsSource.ChunkStoreBytes = chunkStore.DiskUsage
	}
	metricsSrv, err := startMetricsServer(cfg.MetricsAddr, logger, metricsSource)
	if err != nil {
		logger.Warn("metrics endpoint not started", zap.Error(err))
	}

	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
		if metricsSrv != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = metricsSrv.Shutdown(shutdownCtx)
		}
		grpcServer.GracefulStop()
	}()

	// docs/active-lag-detection-design.md: turn the chain tail the leader sends back
	// into a background catch-up. There is no switch, and it is deliberately damped — a
	// random delay per signal and a bound on how many knowledge bases may catch up at
	// once — because the alternative is a node returning from an outage starting every
	// knowledge base together. It schedules nothing of its own: Ensure is the same call
	// a query makes, so the existing gates still apply.
	//
	// A control node gets NO catch-up, and that is not an optimisation. It has no
	// storage: no cursor to compare against a chain tail, and no puller to fetch with.
	// Wiring it anyway is what made every control node crash-loop (11–14 restarts, stack
	// at LagCatchup.catchUp → EnsureIndex → a nil *Follower) the moment the switch that
	// used to hide it was removed. "Can this node catch up at all" is a property of the
	// deployment, so the gate belongs here, in the assembly.
	var chainTails stratumsync.ChainTailSink
	if storageLocal {
		chainTails = plane.NewLagCatchup(plane.LagCatchupConfig{
			MinLagVersions:   int64(cfg.LagCatchupMinLagVersions),
			Jitter:           time.Duration(cfg.LagCatchupJitterMS) * time.Millisecond,
			MaxConcurrentKBs: cfg.LagCatchupMaxConcurrentKBs,
			Ensure:           dataPlane.EnsureIndex,
			Cursor:           dataPlane.DataVersionsSnapshot,
			Logger:           logger,
		})
		// Say so at startup: the pace it runs at is otherwise invisible until it does
		// something, and "nothing happened" is exactly what an operator needs to be able
		// to tell apart from "it is not wired".
		logger.Info("lag catch-up wired",
			zap.Int("min_lag_versions", cfg.LagCatchupMinLagVersions),
			zap.Int("jitter_ms", cfg.LagCatchupJitterMS),
			zap.Int("max_concurrent_kbs", cfg.LagCatchupMaxConcurrentKBs))
	}

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
		// Re-resolved every interval rather than cached, and shared with the
		// source lookup's holders client: both want the same address, and for the
		// same reason — §7.13.1's re-resolve instead of a cached forwarding path.
		ResolveLeader: resolveControlLeader,
		// §7.5: store the watermarks the leader carries back, so this node's WAL can
		// be reclaimed even when this node is not the leader.
		Watermarks: controlPlane,
		// And the chain tails, which are what tell this node it has fallen behind.
		ChainTails: chainTails,
		// And the control leader's answer to "who holds this version", which the
		// data-source lookup mirrors. That lookup may run on the apply path, so it
		// cannot ask for itself; this response is what fills the mirror.
		Holders: holdersCache,
		Logger:  logger,
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

	// §7.9: publish the epoch payload now that this node is serving. It cannot be
	// done during the reconcile above, because the payload's data side is a quorum
	// claim and every storage node is still inside its own reconcile at that
	// point — none of them serves until that returns, so each one sees only itself
	// and the claim fails for every knowledge base at once. The helper retries,
	// since "this node is serving" does not imply "its peers are".
	go reportEpochWhenPeersAreUp(ctx, logger, dataPlane, controlPlane, epochDurable)

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
// contract: a finished build reports the index ready, a failed one reports a
// failure on the INDEX side, which is where a build's retry budget and its
// terminal verdict live (Stratum_设计文档v13.md §10.1, §10.1b).
//
// It deliberately no longer goes through ReportAvailability: that channel
// describes the version's abstract availability, a different question from "did
// this build fail". Routing build failures through it set a retryable FAILED
// with no budget behind it — so nothing ever reached a verdict for the index
// side, and a build that kept failing simply kept reporting.
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
	// epochReportRetryInterval is how long to wait between attempts to publish a
	// payload that quorum was not ready for yet.
	epochReportRetryInterval = 3 * time.Second
	// epochReportRetryWindow bounds the retrying. Peers come up within seconds of
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
func reportEpochWhenPeersAreUp(ctx context.Context, logger *zap.Logger, dp *plane.LocalDataPlane, cp plane.ControlPlane, durable []plane.VersionRef) {
	deadline := time.Now().Add(epochReportRetryWindow)
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
		case <-time.After(epochReportRetryInterval):
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

	// ControlPlaneFailureBudget is the global default for how many failed
	// attempts a version tolerates before the control layer declares it
	// FAILED_PERMANENT (Stratum_设计文档v13.md §10.1), from
	// control_plane.failure_budget. Zero or negative keeps
	// plane.DefaultFailureBudget.
	//
	// It is only the default: the control plane also carries a per-KB override
	// (plane.DurabilityPolicy.MaxFailures). Nothing in the node assembly sets
	// that one today, so this value is what actually decides the verdict.
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

	// IndexPushConcurrency is how many index distributions (§8.4) may be in
	// flight at once (index_manager.push_concurrency); <= 0 means the data
	// plane's default (DefaultMaxConcurrentIndexPush). One distribution reads a
	// whole index file into memory and ships it to every replica, so this bounds
	// the builder's memory peak and outbound bandwidth together.
	IndexPushConcurrency int

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
	// Logging.Level (debug/info/warn/error, zap's names) becomes
	// appConfig.LogLevel. It used to be parsed by nobody at all: the node built an
	// info-level logger before the config file was even read, so setting
	// `logging.level: debug` did nothing.
	Logging struct {
		Level string `yaml:"level"`
	} `yaml:"logging"`

	Node struct {
		NodeID               int64  `yaml:"node_id"`
		Role                 string `yaml:"role"`
		GRPCAddr             string `yaml:"grpc_addr"`
		RaftAddr             string `yaml:"raft_addr"`
		MetricsAddr          string `yaml:"metrics_addr"`
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
	} `yaml:"vecstore"`

	Embed struct {
		ServiceAddr string `yaml:"service_addr"`
	} `yaml:"embed"`

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
		ColdSweepIntervalMS   int     `yaml:"cold_sweep_interval_ms"`
		AppendMaxDeadRatio    float64 `yaml:"append_max_dead_ratio"`
		// §3 codebook refresh. Either trigger fires and the build rebuilds from
		// scratch instead of appending, which is the only way to retrain the
		// quantizer. Both are inert for KBs whose quantizer does not learn a
		// codebook (OFF / SQ_FP16 / SQ_BF16); <= 0 takes the default.
		MaxCodebookDriftRatio float64 `yaml:"max_codebook_drift_ratio"`
		MaxCodebookAppends    int64   `yaml:"max_codebook_appends"`
		// MinBaselineVectors is the baseline size below which the drift ratio is
		// ignored, so a tiny KB does not rebuild on every version.
		MinBaselineVectors int64 `yaml:"min_codebook_baseline_vectors"`
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
		// BuildConcurrency is how many index builds may run at once. <= 0 takes the
		// IndexManager's default (the CPU count).
		//
		// Worth having as a knob rather than a constant: the right number trades
		// build throughput against how much a build steals from live queries, and
		// that trade depends on the deployment's disk and vecstore, not on anything
		// this repository can know.
		BuildConcurrency int `yaml:"build_concurrency"`
		// PushConcurrency is how many index distributions (§8.4) may be in flight
		// at once. <= 0 takes the data plane's default (4).
		//
		// It bounds a distribution storm's cost in network, source-node disk IO and
		// memory — one distribution reads the whole index file into memory before
		// shipping it to N replicas. Distribution is already asynchronous (it no
		// longer occupies a build worker), so this is its only gate.
		PushConcurrency int `yaml:"push_concurrency"`
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

	// LagCatchup is the background catch-up described in
	// docs/active-lag-detection-design.md: the node reads the chain tail the control
	// leader carries back on its cursor report and catches up when it is behind.
	LagCatchup struct {
		MinLagVersions   int `yaml:"min_lag_versions"`
		JitterMS         int `yaml:"jitter_ms"`
		MaxConcurrentKBs int `yaml:"max_concurrent_kbs"`
	} `yaml:"lag_catchup"`
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
	if fc.IndexManager.ColdSweepIntervalMS != 0 {
		cfg.IndexColdSweepInterval = time.Duration(fc.IndexManager.ColdSweepIntervalMS) * time.Millisecond
	}
	if fc.IndexManager.AppendMaxDeadRatio != 0 {
		cfg.IndexAppendMaxDeadRatio = fc.IndexManager.AppendMaxDeadRatio
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
	if fc.IndexManager.BuildConcurrency != 0 {
		cfg.IndexBuildConcurrency = fc.IndexManager.BuildConcurrency
	}
	if fc.IndexManager.PushConcurrency != 0 {
		cfg.IndexPushConcurrency = fc.IndexManager.PushConcurrency
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

		LogLevel: "info",

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

		// §3 codebook refresh: 0 means the IndexManager's own defaults (drift
		// ratio 0.25, 50 appends since training). Only a KB whose quantizer
		// learns a codebook (SQ8 / PQ) can trigger it.
		IndexMaxCodebookDriftRatio: 0,
		IndexMaxCodebookAppends:    0,
		// 0 => the IndexManager's floor (1000); negative removes it.
		IndexMinCodebookBaselineVectors: 0,

		// §8.6(d) collection: 0/false means the IndexManager's own defaults, which
		// are "do not collect" for gc_enabled and 2 for serving_replica_min. The
		// scanner still runs and reports, so a node that has never been configured
		// for collection will still say when collection would have been worth it.
		IndexGCEnabled:           false,
		IndexServingReplicaMin:   0,
		IndexGCGraphRebuildRatio: 0,
		IndexGCSweepInterval:     0,
		IndexBuildConcurrency:    0,

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

// versionHolderSource adapts the control plane's data-version aggregate to the
// two things the service layer asks of it: which nodes hold a version (§3.1), and
// whether the storage layer can still meet a write's durability contract (§4.3).
//
// It exists to keep the dependency pointing one way: service does not import
// plane, so the two Holder types (identical in content) are bridged here, where
// both are already visible. The redundancy verdict is bridged in the same place
// for the same reason, and both service callers that need it — the CreateVersion
// gate and HealthCheck — take this one adapter, so the two type systems meet in
// exactly one spot.
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

// StorageDegraded reports whether the storage layer is short of a quorum for
// kbID, with a real replica still answering.
//
// ok=false means the verdict is unavailable — this node does not lead, or its
// replica topology is not wired — and every caller treats that as "allow". See
// service.StorageDegradationSource: the verdict is soft state, so it may only
// ever cost a retry, never a refused write that would otherwise have succeeded.
func (s versionHolderSource) StorageDegraded(kbID string) (bool, string, bool) {
	return s.cp.StorageDegraded(kbID)
}

// StorageUnavailable is the other tier: not one required replica is live. The two
// are separate methods because service declares its own narrow interface rather
// than importing plane's state enum, and the pair of answers is what decides which
// sentinel a refusal carries (§4.1).
func (s versionHolderSource) StorageUnavailable(kbID string) (bool, string, bool) {
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
