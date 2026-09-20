package main

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	vecstorepb "stratum/api/proto/vecstore"
	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/embed"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/splitter"
	"stratum/internal/versiondoc"
)

// storageStack is the storage layer's local state: its stores, the index
// manager built on them, the splitter, and the clients they reach out through.
//
// It is constructed in one place so the assembly can decide whether to
// construct it at all. A control node keeps no data storage of its own — the
// control layer owns replicated metadata only (Stratum_设计文档v13.md §7.0) — and
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
	VersionBloom   *bloom.VersionBloomStore
	ChunkSplitter  splitter.ChunkSplitter
	IndexManager   *index.IndexManagerImpl
	EmbedClient    *embed.HTTPEmbedClient
}

// buildStorageStack opens every local store under dataDir and wires the index
// manager to them.
//
// rn is read only — the chunk-bloom rebuild and the index manager's metadata
// lookups — so a storage node passes its remote proxy and an all-in-one node its
// own Raft node without this caring which it is.
func buildStorageStack(cfg appConfig, dataDir string, rn raft.RaftNode, logger *zap.Logger) (*storageStack, error) {
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

	chunkStore, err := chunkstore.NewVecstoreChunkStore(cfg.VecstoreGRPCAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to vecstore at %s: %w", cfg.VecstoreGRPCAddr, err)
	}

	stack := &storageStack{
		DocStore:       ds,
		ChunkDocMapper: cdm,
		VersionDocList: vd,
		ChunkStore:     chunkStore,
		EmbedClient:    embed.NewHTTPEmbedClient(cfg.EmbedServiceAddr, 30*time.Second),
	}

	// Bloom filters: the chunk-existence filter on the write path and the
	// version-document filter on the read path. Sized from config
	// (bloom_filter.expected_items / false_positive_rate).
	stack.ChunkBloom = bloom.NewBitsAndBloomsFilter(cfg.BloomExpectedItems, cfg.BloomFalsePositiveRate)
	stack.VersionBloom = bloom.NewVersionBloomStore(dataDir, cfg.BloomExpectedItems, cfg.BloomFalsePositiveRate, vd)

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
	// (docs/content-defined-chunking-plan.md §3.8).
	stack.ChunkSplitter = splitter.NewDefault()

	indexMgr := index.NewIndexManager(index.IndexManagerConfig{
		LRUCapacity:         cfg.IndexLRUCapacity,
		LoadWaitTimeout:     cfg.IndexLoadWaitTimeout,
		CallbackMaxRetries:  cfg.IndexCallbackMaxRetries,
		CallbackRetryBaseMS: cfg.IndexCallbackRetryBaseMS,
		VecstoreAddr:        cfg.VecstoreGRPCAddr,
		IndexDataDir:        dataDir,
		IndexRetentionCount: cfg.IndexRetentionCount,
		// Retention shield: keep recently-queried versions on disk even when
		// they fall outside the newest IndexRetentionCount.
		RetentionProtectWindow: cfg.IndexRetentionProtectWindow,
		RetentionProtectMax:    cfg.IndexRetentionProtectMax,
		MemoryThresholdMB:      cfg.IndexMemoryThresholdMB,
		// §2.2: the coarse-pass budget for quantized search. 0 leaves the field
		// off the request, so the vector store applies clamp(top_k × 8, 16, 4096).
		CandidateN:         cfg.IndexCandidateN,
		ColdThreshold:      cfg.IndexColdThreshold,
		ColdSweepInterval:  cfg.IndexColdSweepInterval,
		AppendMaxDeadRatio: cfg.IndexAppendMaxDeadRatio,
		GCRatioThreshold:   cfg.IndexGCRatioThreshold,
		// §3 codebook refresh (docs/codebook-refresh-plan.md §3). Firing either
		// trigger rebuilds the version from scratch, which is the only way to
		// retrain the quantizer. Gated by the KB's quantizer inside the
		// IndexManager: only SQ8 / PQ learn a codebook, so OFF / SQ_FP16 /
		// SQ_BF16 KBs never trigger it.
		MaxCodebookDriftRatio:      cfg.IndexMaxCodebookDriftRatio,
		MaxCodebookAppends:         cfg.IndexMaxCodebookAppends,
		MinCodebookBaselineVectors: cfg.IndexMinCodebookBaselineVectors,
		BuildAbandonTimeout:        cfg.IndexBuildAbandonTimeout,
		// §8.6(d) collection. NodeID is deliberately NOT set here: it arrives with
		// SetGCReplicaCounter below, together with the control-plane client that
		// answers "how many other replicas are serving" — and without that client
		// a node ID would be a number nobody could act on.
		GCEnabled:              cfg.IndexGCEnabled,
		IndexServingReplicaMin: cfg.IndexServingReplicaMin,
		GCGraphRebuildRatio:    cfg.IndexGCGraphRebuildRatio,
		GCSweepInterval:        cfg.IndexGCSweepInterval,
		BuildConcurrency:       cfg.IndexBuildConcurrency,
	})
	indexMgr.SetLogger(logger.Named("index"))
	// §6: reclaim index artifacts whose build was abandoned. Like the cold
	// evaluator this is local and passive — it reads only this node's own disk —
	// so it starts here rather than being driven from the control layer.  On by
	// default: the artifact it removes was never sealed by a Save, so nothing can
	// serve from it and nothing else will ever come back for it.
	indexMgr.StartAbandonSweeper()
	// §8.6a: the cold-version evaluator is a local, passive policy (it only
	// reads this node's access table), so it starts here rather than being
	// driven from the control layer. A no-op when no threshold is configured.
	indexMgr.StartColdPolicy()

	// §8.6(d) phase 1: report — do not yet clean — active versions whose sealed
	// artifact carries more dead weight than the current document set justifies.
	// The scan covers ACTIVE versions only, so it needs to know which version
	// each knowledge base is serving; that fact lives in the control layer's
	// metadata, which a storage node reads over gRPC like any other metadata.
	indexMgr.SetActiveVersionsProvider(func(ctx context.Context) (map[string]int64, error) {
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
	})

	// §8.6a: the cold evaluator walks the authoritative version set — every
	// version the control layer knows about — instead of this node's access
	// table, which is empty after a restart. Same metadata, same connection.
	indexMgr.SetVersionsProvider(func(ctx context.Context) (map[string][]int64, error) {
		kbs, err := rn.ListKnowledgeBases(ctx)
		if err != nil {
			return nil, err
		}
		out := make(map[string][]int64, len(kbs))
		for _, kb := range kbs {
			versions, err := rn.ListVersions(ctx, kb.KBID)
			if err != nil {
				logger.Warn("index: cold policy: ListVersions failed",
					zap.String("kb_id", kb.KBID), zap.Error(err))
				continue
			}
			ids := make([]int64, 0, len(versions))
			for _, v := range versions {
				ids = append(ids, v.VersionID)
			}
			out[kb.KBID] = ids
		}
		return out, nil
	})
	indexMgr.StartGCScanner()

	// Build data sources: the IndexManager's async build reads the version's
	// document IDs via ChunkDocMapper and pulls each chunk vector from the
	// vecstore's ChunkStorageService (the same keys the write path used).
	// Without this wiring the build goroutine would invoke nil callbacks and
	// crash the process on the first CreateVersion.
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
	// KB metadata source: async builds read the KB-level quantizer
	// configuration from the replicated metadata and forward it to the vecstore
	// with each Build RPC (Stratum_设计文档v12.md 2.4).
	indexMgr.SetKBMetaGetter(rn.GetKB)
	// §8.6(c): a pure-append version may start its index from its parent
	// version's artifact instead of rebuilding. The parent link lives in the
	// replicated version metadata, which the storage layer reads read-only.
	indexMgr.SetVersionParentGetter(func(ctx context.Context, kbID string, versionID int64) (int64, error) {
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
	})

	stack.IndexManager = indexMgr
	return stack, nil
}

// Close releases what the stack opened and owns.
func (s *storageStack) Close() {
	if s == nil {
		return
	}
	if s.ChunkStore != nil {
		s.ChunkStore.Close()
	}
	if s.IndexManager != nil {
		// Stops the cold-policy goroutine as well as the manager's own work.
		s.IndexManager.Close()
	}
}
