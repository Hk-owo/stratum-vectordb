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

	// --- Crash recovery ---
	records, err := walImpl.Recover(nil)
	if err != nil {
		logger.Fatal("WAL recovery scan failed", zap.Error(err))
	}
	if len(records) > 0 {
		logger.Warn("WAL recovery: pending records found — recovery logic not yet implemented",
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
	chunkBloom := bloom.NewMockBloomFilter() // TODO: use real bits-and-blooms impl
	versionBloom := bloom.NewMockBloomFilter()

	// --- ChunkSplitter ---
	chunkSplitter := &splitter.SlidingWindowSplitter{}

	// --- IndexManager ---
	indexMgr := index.NewIndexManager(index.IndexManagerConfig{
		LRUCapacity:         cfg.IndexLRUCapacity,
		LoadWaitTimeout:     cfg.IndexLoadWaitTimeout,
		CallbackMaxRetries:  cfg.IndexCallbackMaxRetries,
		CallbackRetryBaseMS: cfg.IndexCallbackRetryBaseMS,
		VecstoreAddr:        vecstoreAddr,
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
	querySvc := service.NewQueryService(raftImpl, indexMgr, cdm, vd, ds, versionBloom)
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

	WriteMaxRetries   int
	WriteRetryBaseMS  int
	DeleteMaxRetries  int
	DeleteRetryBaseMS int
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

		WriteMaxRetries:   3,
		WriteRetryBaseMS:  100,
		DeleteMaxRetries:  5,
		DeleteRetryBaseMS: 500,
	}
}
