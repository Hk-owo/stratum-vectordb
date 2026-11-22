package main

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	vecstorepb "stratum/api/proto/vecstore"
	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/embed"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/rpcpool"
	"stratum/internal/splitter"
	"stratum/internal/storage"
	"stratum/internal/tombstone"
	"stratum/internal/versiondoc"
	"stratum/internal/versionview"
)

// storageStack is the storage layer's local state: its stores, the index
// manager built on them, the splitter, and the clients they reach out through.
//
// It is constructed in one place so the assembly can decide whether to
// construct it at all. A control node keeps no data storage of its own — the
// control layer owns replicated metadata only (docs/参考/历史/Stratum_设计文档v13.md §7.0) — and
// that decision has to be expressible as "build this, or do not", rather than as
// a condition repeated at each store below, where forgetting one would leave a
// control node quietly writing to a local disk it is not supposed to have.
//
// What a control node still has is a data directory and a WAL: Raft's own log
// and snapshots live there (see the metadata channel in main). "No storage" is
// about the data layers — documents, chunks, vectors, indexes — not about the
// log that makes this node a Raft member.
type storageStack struct {
	DocStore       *docstore.PebbleDocStore
	ChunkDocMapper *chunkdoc.PebbleChunkDocMapper
	VersionDocList *versiondoc.PebbleVersionDocList
	ChunkStore     *chunkstore.VecstoreChunkStore
	ChunkBloom     *bloom.BitsAndBloomsFilter
	VersionViews   *versionview.Store
	ChunkSplitter  splitter.ChunkSplitter
	IndexManager   *index.IndexManagerImpl
	EmbedClient    *embed.HTTPEmbedClient
	vecstoreConn   *grpc.ClientConn

	// Tombstones is this node's durable record of versions the control layer told
	// it are deleted. It lives beside the other stores because it is storage-layer
	// state: the reverse reconciliation can always re-derive the same answer from
	// the control layer, but this is what refuses a late arrival on the landing
	// path, with no round trip and no control tier reachable.
	Tombstones *tombstone.PebbleStore
}

// buildStorageStack opens every local store under dataDir and wires the index
// manager to them.
//
// rn is read only — the chunk-bloom rebuild and the index manager's metadata
// lookups — so a storage node passes its remote proxy and an all-in-one node its
// own Raft node without this caring which it is.
//
// buildState is the node state machine's build-status half
// (docs/已实施/storage-node-state-machine-plan.md §7, item 4). The index manager is
// where it belongs: every build — and every install of a distributed artifact —
// passes through that one component.
func buildStorageStack(
	cfg appConfig,
	dataDir string,
	rn raft.RaftNode,
	logger *zap.Logger,
	buildState index.BuildStateRecorder,
	replicaCounter index.ReplicaCounter,
	serviceLease *storage.ServiceLease,
	chainTailVersions func(ctx context.Context) (map[string]int64, error),
) (*storageStack, error) {
	ds, err := docstore.NewPebbleDocStore(dataDir + "/docstore")
	if err != nil {
		return nil, fmt.Errorf("open DocStore: %w", err)
	}

	cdm, err := chunkdoc.NewPebbleChunkDocMapper(dataDir + "/chunkdoc")
	if err != nil {
		return nil, fmt.Errorf("open ChunkDocMapper: %w", err)
	}

	vd, err := versiondoc.NewPebbleVersionDocList(dataDir + "/versiondoc")
	if err != nil {
		return nil, fmt.Errorf("open VersionDocList: %w", err)
	}

	tombstones, err := tombstone.NewPebbleStore(dataDir + "/tombstone")
	if err != nil {
		return nil, fmt.Errorf("open Tombstones: %w", err)
	}

	vecstoreDialer := rpcpool.NewDialer(rpcpool.DialConfig{
		KeepaliveInterval: cfg.VecstoreKeepaliveInterval,
		KeepaliveTimeout:  cfg.VecstoreKeepaliveTimeout,
	})
	vecstoreConn, err := vecstoreDialer.Dial(cfg.VecstoreGRPCAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to vecstore at %s: %w", cfg.VecstoreGRPCAddr, err)
	}

	chunkStore, err := chunkstore.NewVecstoreChunkStoreFromConn(vecstoreConn)
	if err != nil {
		_ = vecstoreConn.Close()
		return nil, fmt.Errorf("init chunkstore client for vecstore at %s: %w", cfg.VecstoreGRPCAddr, err)
	}

	stack := &storageStack{
		DocStore:       ds,
		ChunkDocMapper: cdm,
		VersionDocList: vd,
		ChunkStore:     chunkStore,
		Tombstones:     tombstones,
		EmbedClient:    embed.NewHTTPEmbedClient(cfg.EmbedServiceAddr, cfg.EmbedTimeout),
		vecstoreConn:   vecstoreConn,
	}

	// Chunk-existence bloom filter on the write path + unified three-tier
	// per-version read view cache on the query path.
	stack.ChunkBloom = bloom.NewBitsAndBloomsFilter(cfg.BloomExpectedItems, cfg.BloomFalsePositiveRate)
	maxViews := cfg.IndexLRUCapacity + 4
	if maxViews < 8 {
		maxViews = 8
	}
	stack.VersionViews = versionview.NewStore(versionview.Config{
		DataDir:        dataDir,
		MaxEntries:     maxViews,
		VersionDocList: vd,
		ChunkDocMapper: cdm,
	})

	// Rebuild the chunk-existence filter from the authoritative chunk-doc
	// mapping (the design's "从 chunk store 重建" is served from the Go-side
	// mapping since the vecstore has no enumeration RPC; a chunk whose mapping
	// was already GC'd simply contributes a stale positive that the write
	// path's Exists confirmation resolves). Best-effort: right after startup a
	// node's metadata view may not yet hold the full KB list, and KBs missed
	// here only cost extra Exists round-trips on the write path, never
	// correctness.
	ctx := context.Background()
	if kbs, err := rn.ListKnowledgeBases(ctx); err == nil {
		for _, kb := range kbs {
			chunkIDs, err := cdm.ListChunkIDs(ctx, kb.KBID)
			if err != nil {
				logger.Warn("chunk bloom rebuild: ListChunkIDs failed", zap.String("kb_id", kb.KBID), zap.Error(err))
				continue
			}
			for _, cid := range chunkIDs {
				stack.ChunkBloom.Add(cid)
			}
		}
		logger.Info("chunk bloom filter rebuilt", zap.Int("kbs", len(kbs)))
	} else {
		logger.Warn("chunk bloom rebuild: ListKnowledgeBases failed; filter starts empty (write path degrades, stays correct)", zap.Error(err))
	}

	// One splitter serves every knowledge base on this node; which algorithm
	// it applies comes from each KB's metadata, per write
	// (docs/已实施/content-defined-chunking-plan.md §3.8).
	stack.ChunkSplitter = splitter.NewDefault()

	indexMgr := index.NewIndexManager(index.IndexManagerConfig{
		LRUCapacity:                cfg.IndexLRUCapacity,
		LoadWaitTimeout:            cfg.IndexLoadWaitTimeout,
		CallbackMaxRetries:         cfg.IndexCallbackMaxRetries,
		CallbackRetryBaseMS:        cfg.IndexCallbackRetryBaseMS,
		VecstoreAddr:               cfg.VecstoreGRPCAddr,
		IndexDataDir:               dataDir,
		IndexRetentionCount:        cfg.IndexRetentionCount,
		RetentionProtectWindow:     cfg.IndexRetentionProtectWindow,
		RetentionProtectMax:        cfg.IndexRetentionProtectMax,
		MemoryThresholdMB:          cfg.IndexMemoryThresholdMB,
		CandidateN:                 cfg.IndexCandidateN,
		ColdThreshold:              cfg.IndexColdThreshold,
		AppendMaxDeadRatio:         cfg.IndexAppendMaxDeadRatio,
		GCRatioThreshold:           cfg.IndexGCRatioThreshold,
		MaxCodebookDriftRatio:      cfg.IndexMaxCodebookDriftRatio,
		MaxCodebookAppends:         cfg.IndexMaxCodebookAppends,
		MinCodebookBaselineVectors: cfg.IndexMinCodebookBaselineVectors,
		BuildAbandonTimeout:        cfg.IndexBuildAbandonTimeout,
		GCEnabled:                  cfg.IndexGCEnabled,
		IndexServingReplicaMin:     cfg.IndexServingReplicaMin,
		GCGraphRebuildRatio:        cfg.IndexGCGraphRebuildRatio,
		GCSweepInterval:            cfg.IndexGCSweepInterval,
		BuildConcurrency:           cfg.IndexBuildConcurrency,
		BruteForceMaxChunks:        cfg.IndexBruteForceMaxChunks,
		BuildAbandonSweepInterval:  cfg.IndexBuildAbandonSweepInterval,
		BuildRetryTimeout:          cfg.IndexBuildRetryTimeout,
		BuildRetryInterval:         cfg.IndexBuildRetryInterval,
		AccessPersistInterval:      cfg.IndexAccessPersistInterval,
		KeepaliveInterval:          cfg.VecstoreKeepaliveInterval,
		KeepaliveTimeout:           cfg.VecstoreKeepaliveTimeout,
		BuildState:                 buildState,
		VersionView:                stack.VersionViews,
		MapChunksByDocs:            cdm.MapChunksByDocs,
	},
		index.WithVecstoreConn(vecstoreConn),
		index.WithLogger(logger.Named("index")),
		index.WithServiceLease(serviceLease),
		index.WithGCReplicaCounter(replicaCounter, cfg.NodeID),
		index.WithActiveVersionsProvider(func(ctx context.Context) (map[string]int64, error) {
			kbs, err := rn.ListKnowledgeBases(ctx)
			if err != nil {
				return nil, err
			}
			active := make(map[string]int64, len(kbs))
			for _, kb := range kbs {
				if kb.ActiveVersionID == 0 {
					continue
				}
				active[kb.KBID] = kb.ActiveVersionID
			}
			return active, nil
		}),
		index.WithChainTailVersionsProvider(chainTailVersions),
		index.WithBuildDataSources(index.BuildDataSources{
			ListDocIDs:         vd.ListDocIDs,
			ListChunkIDsByDocs: cdm.ListChunkIDsByDocs,
			ReadChunkVector: func(ctx context.Context, kbID, chunkID string) ([]float32, error) {
				resp, err := chunkStore.VecstoreClient().Read(ctx, &vecstorepb.ReadChunkRequest{
					Key: chunkstore.EncodeKey(kbID, chunkID),
				})
				if err != nil {
					return nil, err
				}
				return resp.GetVector(), nil
			},
		}),
		index.WithKBMetaGetter(rn.GetKB),
		index.WithVersionParentGetter(func(ctx context.Context, kbID string, versionID int64) (int64, error) {
			versions, err := rn.ListVersions(ctx, kbID)
			if err != nil {
				return 0, err
			}
			for _, v := range versions {
				if v.VersionID == versionID {
					return v.ParentVersionID, nil
				}
			}
			return 0, nil
		}),
	)
	indexMgr.StartAbandonSweeper()

	stack.IndexManager = indexMgr
	return stack, nil
}

// Close releases what the stack opened and owns.
func (s *storageStack) Close() {
	if s == nil {
		return
	}
	// A close failure here has nothing to act on at shutdown: Pebble replays its log
	// on the next open, so the table's contents survive regardless.
	if s.Tombstones != nil {
		_ = s.Tombstones.Close()
	}
	if s.IndexManager != nil {
		// Stops workers/buildPool before closing the shared vecstore channel.
		_ = s.IndexManager.Close()
	}
	if s.ChunkStore != nil {
		_ = s.ChunkStore.Close()
	}
	if s.vecstoreConn != nil {
		_ = s.vecstoreConn.Close()
	}
}
