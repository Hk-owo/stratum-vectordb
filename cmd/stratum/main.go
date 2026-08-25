// Stratum is a distributed vector-search knowledge base engine: Raft-
// consensus-backed server that manages versioned document collections,
// splits documents into chunks, embeds them into vectors, indexes them
// with HNSW (Faiss), and serves similarity queries.
//
// See doc/Stratum_设计文档v10.md for the full architecture.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	pb "stratum/api/proto/stratum"
	vecstorepb "stratum/api/proto/vecstore"
	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/coordinator"
	"stratum/internal/docstore"
	"stratum/internal/embed"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/splitter"
	"stratum/internal/sync"
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

	// --- Storage paths ---
	docStorePath := dataDir + "/docstore"
	chunkDocPath := dataDir + "/chunkdoc"
	versionDocPath := dataDir + "/versiondoc"
	walPath := dataDir + "/wal"
	vecstoreAddr := cfg.VecstoreGRPCAddr

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

	// --- PebbleDB stores ---
	ds, err := docstore.NewPebbleDocStore(docStorePath)
	if err != nil {
		logger.Fatal("failed to open DocStore", zap.String("path", docStorePath), zap.Error(err))
	}

	cdm, err := chunkdoc.NewPebbleChunkDocMapper(chunkDocPath)
	if err != nil {
		logger.Fatal("failed to open ChunkDocMapper", zap.String("path", chunkDocPath), zap.Error(err))
	}

	vd, err := versiondoc.NewPebbleVersionDocList(versionDocPath)
	if err != nil {
		logger.Fatal("failed to open VersionDocList", zap.String("path", versionDocPath), zap.Error(err))
	}

	// --- vecstore gRPC client ---
	chunkStore, err := chunkstore.NewVecstoreChunkStore(vecstoreAddr)
	if err != nil {
		logger.Fatal("failed to connect to vecstore", zap.String("addr", vecstoreAddr), zap.Error(err))
	}
	defer chunkStore.Close()

	// --- Raft ---
	raftImpl, err := raft.NewRaftNodeImpl(raft.Config{
		NodeID:             cfg.NodeID,
		DataDir:            dataDir,
		RaftAddr:           cfg.RaftAddr,
		Peers:              cfg.Peers,
		WAL:                walImpl,
		Logger:             logger.Named("raft"),
		HeartbeatInterval:  cfg.HeartbeatInterval,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
	})
	if err != nil {
		logger.Fatal("failed to start RaftNode", zap.Error(err))
	}
	defer raftImpl.Stop()

	// --- Embed client ---
	embedClient := embed.NewHTTPEmbedClient(cfg.EmbedServiceAddr, 30*time.Second)

	// --- Bloom filters ---
	// Real bits-and-blooms-backed implementations (replacing the Phase 0
	// mocks): the chunk-existence filter on the write path and the
	// version-document filter on the read path. Sized from config
	// (bloom_filter.expected_items / false_positive_rate).
	chunkBloom := bloom.NewBitsAndBloomsFilter(cfg.BloomExpectedItems, cfg.BloomFalsePositiveRate)
	vBloomStore := bloom.NewVersionBloomStore(dataDir, cfg.BloomExpectedItems, cfg.BloomFalsePositiveRate, vd)

	// Rebuild the chunk-existence filter from the authoritative
	// chunk-doc mapping (the design's "从 chunk store 重建" is served from
	// the Go-side mapping since the vecstore has no enumeration RPC; a
	// chunk whose mapping was already GC'd simply contributes a stale
	// positive that the write path's Exists confirmation resolves).
	// Best-effort: on a multi-node follower the Raft state machine may not
	// yet hold the full KB list right after startup; KBs missed here only
	// cost extra Exists round-trips on the write path, never correctness.
	ctx := context.Background()
	if kbs, err := raftImpl.ListKnowledgeBases(ctx); err == nil {
		for _, kb := range kbs {
			chunkIDs, err := cdm.ListChunkIDs(ctx, kb.KBID)
			if err != nil {
				logger.Warn("chunk bloom rebuild: ListChunkIDs failed", zap.String("kb_id", kb.KBID), zap.Error(err))
				continue
			}
			for _, cid := range chunkIDs {
				chunkBloom.Add(cid)
			}
		}
		logger.Info("chunk bloom filter rebuilt", zap.Int("kbs", len(kbs)))
	} else {
		logger.Warn("chunk bloom rebuild: ListKnowledgeBases failed; filter starts empty (write path degrades, stays correct)", zap.Error(err))
	}

	// --- ChunkSplitter ---
	chunkSplitter := &splitter.SlidingWindowSplitter{}

	// --- IndexManager ---
	indexMgr := index.NewIndexManager(index.IndexManagerConfig{
		LRUCapacity:         cfg.IndexLRUCapacity,
		LoadWaitTimeout:     cfg.IndexLoadWaitTimeout,
		CallbackMaxRetries:  cfg.IndexCallbackMaxRetries,
		CallbackRetryBaseMS: cfg.IndexCallbackRetryBaseMS,
		VecstoreAddr:        vecstoreAddr,
		IndexDataDir:        dataDir,
		IndexRetentionCount: cfg.IndexRetentionCount,
		MemoryThresholdMB:   cfg.IndexMemoryThresholdMB,
	})
	indexMgr.SetLogger(logger.Named("index"))
	// Build data sources: the IndexManager's async build reads the
	// version's document set from VersionDocList, reverse-looks-up chunk
	// IDs via ChunkDocMapper, and pulls each chunk vector from the
	// vecstore's ChunkStorageService (the same keys the write path used).
	// Without this wiring the build goroutine would invoke nil callbacks
	// and crash the process on the first CreateVersion.
	indexMgr.SetBuildDataSources(
		vd.ListDocIDs,
		cdm.ListChunkIDsByDocs,
		func(ctx context.Context, kbID, chunkID string) ([]float32, error) {
			resp, err := chunkStore.VecstoreClient().Read(ctx, &vecstorepb.ReadChunkRequest{
				Key: chunkstore.EncodeKey(kbID, chunkID),
			})
			if err != nil {
				return nil, err
			}
			return resp.GetVector(), nil
		},
	)
	indexMgr.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
		// Apply the on-disk retention policy after every successful build:
		// keep the newest cfg.IndexRetentionCount index files per KB, drop
		// older ones (rebuilt on demand via RebuildIndex). The active
		// version is shielded so a rolled-back active version's index is
		// never dropped. Best-effort: a policy failure is logged upstream
		// and does not fail the build.
		if status == types.IndexStatusReady {
			if kb, err := raftImpl.GetKB(context.Background(), kbID); err == nil {
				_ = indexMgr.EnforceDiskRetention(context.Background(), kbID, []int64{kb.ActiveVersionID, versionID})
			}
		}
		return raftImpl.ProposeUpdateVersionStatus(context.Background(), versionID, status)
	})
	defer indexMgr.Close()

	// --- Coordinators ---
	writeCoord := coordinator.NewWriteCoordinatorImpl(coordinator.WriteCoordinatorConfig{
		MaxRetries:          cfg.WriteMaxRetries,
		RetryBaseIntervalMS: cfg.WriteRetryBaseMS,
		WAL:                 walImpl,
		RaftNode:            raftImpl,
		Splitter:            chunkSplitter,
		EmbedClient:         embedClient,
		ChunkBloom:          chunkBloom,
		VersionBloom:        vBloomStore,
		ChunkStore:          chunkStore,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vd,
		IndexManager:        indexMgr,
	})

	deleteCoord := coordinator.NewDeleteCoordinatorImpl(coordinator.DeleteCoordinatorConfig{
		MaxRetries:          cfg.DeleteMaxRetries,
		RetryBaseIntervalMS: cfg.DeleteRetryBaseMS,
		WAL:                 walImpl,
		RaftNode:            raftImpl,
		IndexManager:        indexMgr,
		DocStore:            ds,
		ChunkStore:          chunkStore,
		ChunkDocMapper:      cdm,
		VersionDocList:      vd,
		VersionBloom:        vBloomStore,
	})

	deleteVersionCoord := coordinator.NewDeleteVersionCoordinatorImpl(coordinator.DeleteVersionCoordinatorConfig{
		MaxRetries:          cfg.DeleteMaxRetries,
		RetryBaseIntervalMS: cfg.DeleteRetryBaseMS,
		WAL:                 walImpl,
		RaftNode:            raftImpl,
		IndexManager:        indexMgr,
		DocStore:            ds,
		VersionDocList:      vd,
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
	if err := runCrashRecovery(ctx, logger, records, writeCoord, deleteCoord, deleteVersionCoord, walImpl); err != nil {
		logger.Fatal("crash recovery failed", zap.Error(err))
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
	enforceRetentionAtStartup(ctx, logger, raftImpl, indexMgr)
	reconcileIndexStatus(ctx, logger, raftImpl, indexMgr, cfg.IndexRetentionCount)

	// --- Orphan-chunk garbage collector ---
	gcImpl := coordinator.NewChunkGarbageCollectorImpl(coordinator.ChunkGarbageCollectorConfig{
		SweepIntervalSec: cfg.GCSweepIntervalSec,
		RaftNode:         raftImpl,
		ChunkDocMapper:   cdm,
		DocStore:         ds,
		ChunkStore:       chunkStore,
	})
	gcImpl.SetLogger(logger.Named("gc"))
	go gcImpl.Run(ctx)

	// --- Data sync (leader→follower) ---
	// Leader handler: serves storage-layer data to followers via gRPC.
	syncLeader := sync.NewLeaderHandler(
		ds.DB(),
		cdm.DB(),
		vd.DB(),
		chunkStore.VecstoreClient(),
	)

	// Build nodeID→ServiceAddr map for follower leader resolution.
	peerAddrByID := make(map[int64]string, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if p.ServiceAddr != "" {
			peerAddrByID[p.ID] = p.ServiceAddr
		}
	}

	// Follower: pulls data when this node applies a version written by
	// the leader. The sync module is wired via OnVersionCreated.
	syncFollower := sync.NewFollower(ds, cdm, vd, chunkStore, indexMgr)

	raftImpl.SetOnVersionCreated(func(kbID string, versionID int64) {
		ctx := context.Background()
		status, err := raftImpl.GetClusterStatus(ctx)
		if err != nil {
			logger.Error("sync: GetClusterStatus failed, cannot pull version data",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			return
		}
		if !status.HasLeader {
			logger.Warn("sync: no leader known, deferring version data pull",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
			return
		}
		// 本节点就是 leader:存储层数据已由写路径直接落盘,无需(也不应)
		// 向自己发起拉取。尤其关键的是重启后 raft 重放历史日志时,每条
		// CreateVersion 都会走"非本节点提案"分支触发本回调;若在此处向
		// 不可达的 leader 地址发起阻塞式 PullVersion,整个 apply 循环会
		// 卡死,后续所有日志永远无法应用。
		if status.LeaderID == cfg.NodeID {
			return
		}
		leaderAddr, ok := peerAddrByID[status.LeaderID]
		if !ok {
			logger.Error("sync: leader address unknown for node ID",
				zap.Int64("leader_id", status.LeaderID))
			return
		}

		if versionID <= 1 {
			// Initial version, created by CreateKnowledgeBase with no
			// document changes: there is nothing to pull, and no digest is
			// ever committed for it. Pull once (a no-op stream) and done.
			_ = syncFollower.PullVersion(ctx, leaderAddr, kbID, versionID)
			return
		}

		// Pull with digest verification, retrying until the data is
		// complete. The leader commits the version's document-ID set hash
		// (VersionMeta.DocIDSetHash) only after its storage writes finish,
		// so this node recomputes the hash from its local store after each
		// pull and retries until it matches — closing the race where the
		// pull arrives before the leader's writes land. If the digest never
		// arrives (a missed propose on the leader), a pull that produced
		// data is accepted (verifyVersionPull's fallback).
		deadline := time.Now().Add(30 * time.Second)
		backoff := 200 * time.Millisecond
		for {
			if err := syncFollower.PullVersion(ctx, leaderAddr, kbID, versionID); err != nil {
				logger.Warn("sync: PullVersion failed, will retry",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			} else if verifyVersionPull(ctx, raftImpl, vd, kbID, versionID) {
				return
			}
			if time.Now().After(deadline) {
				logger.Error("sync: version data pull did not converge",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
				return
			}
			time.Sleep(backoff)
			if backoff < 5*time.Second {
				backoff *= 2
			}
		}
	})

	// --- gRPC services ---
	kbSvc := service.NewKnowledgeBaseService(raftImpl, writeCoord, deleteCoord, deleteVersionCoord)
	querySvc := service.NewQueryService(raftImpl, indexMgr, cdm, vd, ds, vBloomStore)
	adminSvc := service.NewAdminService(cfg.NodeID, raftImpl, indexMgr, ds, chunkStore, walImpl)

	// --- gRPC server ---
	grpcServer := grpc.NewServer()
	pb.RegisterKnowledgeBaseServiceServer(grpcServer, kbSvc)
	pb.RegisterQueryServiceServer(grpcServer, querySvc)
	pb.RegisterAdminServiceServer(grpcServer, adminSvc)
	pb.RegisterDataSyncServiceServer(grpcServer, syncLeader)

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

	logger.Info("Stratum gRPC server listening", zap.String("addr", cfg.GRPCAddr))
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatal("gRPC server failed", zap.Error(err))
	}

	logger.Info("Stratum stopped")
}

// runCrashRecovery replays every WAL PendingRecord through the
// coordinator layer after startup. See the WAL package doc comment for
// the record semantics. Any failure aborts startup with a fatal error
// (the record is left in the WAL and the replay counter is bumped so
// operators can see it via GetSystemStatus); the next restart retries.
func runCrashRecovery(
	ctx context.Context,
	logger *zap.Logger,
	records []types.PendingRecord,
	wc coordinator.WriteCoordinator,
	dc coordinator.DeleteCoordinator,
	dvc coordinator.DeleteVersionCoordinator,
	w wal.WAL,
) error {
	for _, rec := range records {
		switch rec.Type {
		case types.PendingRecordTypeDeleteMark:
			logger.Info("crash recovery: resuming interrupted knowledge-base deletion",
				zap.String("kb_id", rec.KBID))
			if err := dc.Execute(ctx, rec.KBID); err != nil {
				w.IncrementReplayCounter(rec)
				return fmt.Errorf("crash recovery: resume KB deletion for %s: %w", rec.KBID, err)
			}

		case types.PendingRecordTypeVersionDelete:
			logger.Info("crash recovery: resuming interrupted version deletion",
				zap.String("kb_id", rec.KBID), zap.Int64("version_id", rec.VersionID))
			if err := dvc.Execute(ctx, rec.KBID); err != nil {
				w.IncrementReplayCounter(rec)
				return fmt.Errorf("crash recovery: resume version deletion for %s: %w", rec.KBID, err)
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
			if err := wc.ReplayVersionStorageWrites(ctx, rec.KBID, rec.ParentVersionID, rec.VersionID, rec.Changes); err != nil {
				w.IncrementReplayCounter(rec)
				return fmt.Errorf("crash recovery: replay version %d storage writes: %w", rec.VersionID, err)
			}
		}
	}
	return nil
}

// enforceRetentionAtStartup applies the disk retention policy once at
// startup, before reconcileIndexStatus runs: for every KB, drop on-disk
// index files older than the newest IndexRetentionCount versions, shielding
// the active version. This is a no-op when retention is unconfigured
// (count <= 0), which is also when the reconcile below never skips
// versions. Followers whose state machine has not yet caught up simply
// skip their KBs here; the policy is also enforced after every successful
// build (see IndexManagerImpl.doBuild).
func enforceRetentionAtStartup(ctx context.Context, logger *zap.Logger, rn raft.RaftNode, im index.IndexManager) {
	kbs, err := rn.ListKnowledgeBases(ctx)
	if err != nil {
		logger.Warn("index retention: ListKnowledgeBases failed", zap.Error(err))
		return
	}
	for _, kb := range kbs {
		if err := im.EnforceDiskRetention(ctx, kb.KBID, []int64{kb.ActiveVersionID}); err != nil {
			logger.Warn("index retention: EnforceDiskRetention failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
		}
	}
}

// reconcileIndexStatus derives each version's IndexStatus from the on-disk
// index fact (see the call site's comment for the decision table). It runs
// once at startup; versions not yet visible in the Raft state machine
// (follower still catching up) are converged by the sync path instead.
//
// retentionCount > 0 enables the disk retention policy: READY versions
// whose index file was intentionally dropped (older than the newest
// retentionCount versions) are NOT rebuilt here — they stay absent until a
// query/RebuildIndex asks for them, otherwise every restart would rebuild
// them only for the retention policy to drop them again.
func reconcileIndexStatus(ctx context.Context, logger *zap.Logger, rn raft.RaftNode, im index.IndexManager, retentionCount int) {
	kbs, err := rn.ListKnowledgeBases(ctx)
	if err != nil {
		logger.Warn("index reconcile: ListKnowledgeBases failed", zap.Error(err))
		return
	}
	for _, kb := range kbs {
		versions, err := rn.ListVersions(ctx, kb.KBID)
		if err != nil {
			logger.Warn("index reconcile: ListVersions failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
			continue
		}
		// retentionCutoff is the smallest version ID that is inside the
		// retention window; versions strictly below it are eligible to be
		// dropped by the retention policy and are skipped for rebuild.
		retentionCutoff := int64(-1)
		if retentionCount > 0 && len(versions) > retentionCount {
			ids := make([]int64, len(versions))
			for i, v := range versions {
				ids[i] = v.VersionID
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			retentionCutoff = ids[len(ids)-retentionCount]
		}
		for _, v := range versions {
			if v.IndexStatus == types.IndexStatusFailed {
				continue
			}
			exists, err := im.IndexExists(ctx, kb.KBID, v.VersionID)
			if err != nil {
				logger.Warn("index reconcile: IndexExists failed",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID), zap.Error(err))
				continue
			}
			switch {
			case v.IndexStatus == types.IndexStatusPending && exists:
				// The build finished and was persisted, but the READY
				// propose was lost. Derive the state from the disk fact.
				logger.Info("index reconcile: deriving READY from persisted index",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID))
				if err := rn.ProposeUpdateVersionStatus(ctx, v.VersionID, types.IndexStatusReady); err != nil {
					logger.Warn("index reconcile: propose READY failed",
						zap.Int64("version_id", v.VersionID), zap.Error(err))
				}
			case !exists && retentionCount > 0 && v.IndexStatus != types.IndexStatusFailed && v.VersionID < retentionCutoff:
				// Intentionally dropped by the disk retention policy
				// (PENDING or READY, outside the newest retentionCount
				// versions): leave absent, rebuild on demand.
				logger.Info("index reconcile: skipping rebuild of retention-dropped index",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID))
			case !exists:
				// PENDING without an index, or READY whose index was
				// lost: (re)build. TriggerBuild is idempotent; the build
				// re-persists and the callback re-proposes the status.
				logger.Info("index reconcile: (re)building missing index",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID),
					zap.String("status", v.IndexStatus.String()))
				if err := im.TriggerBuild(ctx, kb.KBID, v.VersionID); err != nil {
					logger.Warn("index reconcile: TriggerBuild failed",
						zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID), zap.Error(err))
				}
			}
		}
	}
}

// verifyVersionPull reports whether this node's local stores hold the
// complete data for (kbID, versionID): the locally computed document-ID
// set digest must match the digest the leader committed into the version
// metadata (see sync.VerifyDocIDSet). If the leader has not committed a
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

	ok, _, err := sync.VerifyDocIDSet(ctx, vd, kbID, versionID, metaHash)
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

// appConfig holds the startup configuration for a Stratum node.
type appConfig struct {
	NodeID   int64
	DataDir  string
	GRPCAddr string
	RaftAddr string
	Peers    []raft.PeerConfig

	// Raft timing. The kvraft defaults ([150ms, 300ms) election timeout)
	// are tuned for in-process tests; over a real network (e.g. Docker)
	// they cause perpetual split votes, so we widen them here.
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

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
		NodeID   int64  `yaml:"node_id"`
		GRPCAddr string `yaml:"grpc_addr"`
		RaftAddr string `yaml:"raft_addr"`
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
	} `yaml:"raft"`

	Storage struct {
		DataDir string `yaml:"data_dir"`
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
	if fc.Storage.DataDir != "" {
		cfg.DataDir = fc.Storage.DataDir
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
		NodeID:   1,
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
