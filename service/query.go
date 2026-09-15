package service

import (
	"context"
	"errors"
	"sort"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/docstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
)

// QueryServiceImpl implements pb.QueryServiceServer.
type QueryServiceImpl struct {
	pb.UnimplementedQueryServiceServer

	raftNode       raft.RaftNode
	indexManager   index.IndexManager
	chunkDocMapper chunkdoc.ChunkDocMapper
	versionDocList versiondoc.VersionDocList
	docStore       docstore.DocStore
	vBloomStore    *bloom.VersionBloomStore

	// logger carries the per-stage timings of a query. Debug level and silent by
	// default: one line per query at info would be noise, while the point is that
	// this path had no observability at all — localizing the O(candidates ×
	// documents) bug took a temporary C++ probe (v13 §5 #18). See SetLogger.
	logger *zap.Logger

	// localVersion is where §9.3(2)'s freshness check reads this node's
	// contiguous cursor. nil means "cannot verify a credential", which makes the
	// check skip rather than fail — an unconfigured node behaves as it did
	// before §9 rather than refusing everything.
	localVersion LocalVersionReporter
}

// NewQueryService constructs a QueryServiceImpl.
// LocalVersionReporter reports this node's contiguous data cursor for a
// knowledge base — the highest version whose history it holds completely. It is
// what §9.3(2)'s freshness check compares against.
type LocalVersionReporter interface {
	LocalVersionOf(kbID string) int64
}

// SetLocalVersionReporter wires the cursor the freshness check reads. Optional:
// without it the node cannot verify a credential and therefore ignores one,
// which is the pre-§9 behaviour rather than a broken one.
func (s *QueryServiceImpl) SetLocalVersionReporter(r LocalVersionReporter) {
	s.localVersion = r
}

func NewQueryService(
	rn raft.RaftNode,
	im index.IndexManager,
	cdm chunkdoc.ChunkDocMapper,
	vdl versiondoc.VersionDocList,
	ds docstore.DocStore,
	vBloomStore *bloom.VersionBloomStore,
) *QueryServiceImpl {
	return &QueryServiceImpl{
		raftNode:       rn,
		indexManager:   im,
		chunkDocMapper: cdm,
		versionDocList: vdl,
		docStore:       ds,
		vBloomStore:    vBloomStore,
		logger:         zap.NewNop(),
	}
}

// SetLogger wires the logger that carries the per-stage timings below. Optional:
// without it those timings go nowhere, which is the pre-existing behaviour
// (nothing about correctness depends on them).
func (s *QueryServiceImpl) SetLogger(l *zap.Logger) {
	if l != nil {
		s.logger = l
	}
}

// Query implements QueryServiceServer.
func (s *QueryServiceImpl) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	kbID := req.KnowledgeBaseId

	// Per-stage timings, emitted at debug level by the deferred function below.
	//
	// Why it exists: this path had NO observability, so locating the
	// O(candidates × documents) defect (v13 §5 #18) took an outside-in probe plus
	// a temporary C++ instrumentation. A slow query must be answerable from the
	// node's own log: search (vecstore), filter (chunk→doc mapping + version
	// membership), read (per-document content), total.
	//
	// Deferred rather than printed on the success path so that a query failing
	// *slowly* also reports where it spent the time.
	qStart := time.Now()
	var stageSearch, stageFilter, stageRead time.Duration
	var stageMeta, stageBloom, stageChunkMap time.Duration
	var candidateCount, matched, chunkMapCalls int
	defer func() {
		s.logger.Debug("query: stage timings",
			zap.String("kb_id", kbID),
			zap.Int("top_k", int(req.GetTopK())),
			zap.Int("candidates", candidateCount),
			zap.Int("matched_docs", matched),
			zap.Int("chunkmap_calls", chunkMapCalls),
			zap.Int64("meta_us", stageMeta.Microseconds()),
			zap.Int64("bloom_us", stageBloom.Microseconds()),
			zap.Int64("chunkmap_us", stageChunkMap.Microseconds()),
			zap.Int64("search_us", stageSearch.Microseconds()),
			zap.Int64("filter_us", stageFilter.Microseconds()),
			zap.Int64("read_us", stageRead.Microseconds()),
			zap.Int64("total_us", time.Since(qStart).Microseconds()))
	}()

	// §9.3(2): honour the service station's freshness credential.
	//
	// A node whose contiguous history has not reached the version the caller
	// should be seeing can still answer — its data is complete, merely older —
	// and that is precisely the silent staleness §9.1 risk 1 describes: the
	// caller gets a well-formed result from the wrong point in time and has no
	// way to tell. Refusing makes it visible, and lets the station move to a
	// node that is current.
	if want := req.GetMinVersion(); want > 0 && s.localVersion != nil {
		if have := s.localVersion.LocalVersionOf(kbID); have < want {
			return nil, status.Errorf(codes.FailedPrecondition,
				"query: %s: local history reaches version %d, below the required %d",
				kbID, have, want)
		}
	}

	// Resolve version.
	var versionID int64
	if req.VersionId != nil {
		versionID = *req.VersionId
	} else {
		kb, err := s.raftNode.GetKB(ctx, kbID)
		if err != nil {
			return nil, stratumerrors.ToGRPCStatus(err)
		}
		versionID = kb.ActiveVersionID
	}

	// Check version status.
	//
	// meta_us: on a storage node this is a CONTROL-PLANE read over gRPC
	// (RemoteRaftNode → the control tier), and it runs on EVERY query. The
	// per-stage timings showed search+filter+read summing to ~1.1 ms while
	// total_us was ~10.9 ms, so the missing time is exactly this kind of
	// per-query plumbing — measure it instead of assuming.
	metaStart := time.Now()
	versions, err := s.raftNode.ListVersions(ctx, kbID)
	stageMeta = time.Since(metaStart)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	var targetVersion types.VersionMeta
	found := false
	for _, v := range versions {
		if v.VersionID == versionID {
			targetVersion = v
			found = true
			break
		}
	}
	if !found {
		return nil, stratumerrors.ToGRPCStatus(stratumerrors.ErrVersionNotFound)
	}
	if targetVersion.IndexStatus == types.IndexStatusPending {
		return nil, status.Error(codes.FailedPrecondition, "version is PENDING")
	}
	if targetVersion.IndexStatus == types.IndexStatusFailed {
		return nil, status.Error(codes.FailedPrecondition, "version index is FAILED")
	}

	// Search: get multi × topK chunk results for aggregation headroom.
	searchTopK := int(req.TopK) * 3
	if searchTopK < 10 {
		searchTopK = 10
	}
	if searchTopK > 1000 {
		searchTopK = 1000
	}

	searchStart := time.Now()
	searchResults, err := s.indexManager.Search(ctx, kbID, versionID, req.Vector, searchTopK)
	stageSearch = time.Since(searchStart)
	candidateCount = len(searchResults)
	if err != nil {
		// An empty version (no documents) has no index entry, so Search
		// reports ErrIndexNotReady. Treat a genuinely empty version as an
		// empty result set rather than an error.
		if errors.Is(err, stratumerrors.ErrIndexNotReady) {
			if docIDs, derr := s.versionDocList.ListDocIDs(ctx, kbID, versionID); derr == nil && len(docIDs) == 0 {
				return &pb.QueryResponse{Results: nil, VersionId: versionID}, nil
			}
		}
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Filter by threshold.
	threshold := float32(0)
	if req.Threshold != nil {
		threshold = *req.Threshold
	}

	// Map chunk results to document results.
	type docScore struct {
		docID  string
		scores []float32
	}
	docMap := make(map[string]*docScore)

	// Per-version document bloom filter. A Get failure (e.g. the version's
	// doc list is temporarily unavailable) degrades to no filtering: the
	// authoritative confirmations below still keep results correct, at the
	// cost of extra work.
	//
	// bloom_us covers the filter plus the version's doc-ID set (one prefix scan):
	// both are local Pebble reads, so this is what "reading our own storage" costs.
	bloomStart := time.Now()
	vBloom, vBloomErr := s.vBloomStore.Get(ctx, kbID, versionID)

	// The version's document-ID set, materialized ONCE for the whole query.
	//
	// It used to be fetched INSIDE the loop below — once per candidate chunk,
	// again per docID — and every fetch was a prefix scan returning the version's
	// entire document list, which was then walked linearly to answer "is this
	// docID in the version?". One query therefore cost O(candidates × documents).
	// Measured on the 3+3 cluster: 2,000 documents → 207 ms, 8,000 → 2.73 s, while
	// the HNSW search inside vecstore took 16 µs (probe: VECPROBE search
	// ntotal=18 took_us=16). A single fetch plus a set makes membership O(1) and
	// the whole query O(documents) once.
	//
	// A failed fetch leaves vDocs nil, and every membership test then fails —
	// the same conservative "cannot confirm, so skip" the per-candidate fetch
	// had when its own call errored.
	var vDocs map[string]struct{}
	if vBloomErr == nil {
		if ids, err := s.versionDocList.ListDocIDs(ctx, kbID, versionID); err == nil {
			vDocs = make(map[string]struct{}, len(ids))
			for _, id := range ids {
				vDocs[id] = struct{}{}
			}
		}
	}
	stageBloom = time.Since(bloomStart)

	filterStart := time.Now()
	for _, r := range searchResults {
		if r.Score < threshold {
			continue
		}
		chunkMapStart := time.Now()
		docIDs, err := s.chunkDocMapper.ListDocIDs(ctx, kbID, r.ChunkID)
		stageChunkMap += time.Since(chunkMapStart)
		chunkMapCalls++
		if err != nil {
			continue
		}
		for _, docID := range docIDs {
			// Version filter: a bloom miss means the document is
			// definitely not in this version's document set (no false
			// negatives), so it is skipped without an authoritative
			// check. A bloom hit may be a false positive, so it is
			// confirmed against the version doc list.
			if vBloomErr == nil {
				if !vBloom.Test(docID) {
					continue
				}
				if _, ok := vDocs[docID]; !ok {
					continue
				}
			}
			if ds, ok := docMap[docID]; ok {
				ds.scores = append(ds.scores, r.Score)
			} else {
				docMap[docID] = &docScore{docID: docID, scores: []float32{r.Score}}
			}
		}
	}
	stageFilter = time.Since(filterStart)
	matched = len(docMap)

	// Aggregate per-document scores.
	agg := req.Aggregation
	// 0 is the default (MEDIAN), so treat it as MEDIAN.

	type scoredDoc struct {
		docID   string
		score   float32
		content string
	}

	// Aggregate, rank, THEN read — in that order.
	//
	// Reading content used to happen while building the candidate list, so one
	// query read every matched document's text and threw almost all of it away
	// when it truncated to top_k. That is not a rare case: chunks are
	// content-addressed, so one chunk can belong to thousands of documents (a
	// corpus of near-identical documents collapses to a handful of chunks), and
	// the debug timings made it visible — `matched_docs: 1000`, `read_us: 8684`
	// out of `total_us: 22909` on a 2,000-document version, against
	// `search_us: 389`. Ranking first costs nothing (the scores are already in
	// memory) and turns O(matched) document reads into O(top_k).
	candidates := make([]scoredDoc, 0, len(docMap))
	for docID, ds := range docMap {
		candidates = append(candidates, scoredDoc{docID: docID, score: aggregate(ds.scores, agg)})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].score > candidates[j].score })

	// Read content for the winners, stopping as soon as top_k readable documents
	// are found. A candidate whose content cannot be read is skipped and the next
	// one takes its place, which is what the previous "read everything, then
	// truncate" ordering did as well.
	readStart := time.Now()
	results := make([]scoredDoc, 0, int(req.TopK))
	for _, cand := range candidates {
		if len(results) >= int(req.TopK) {
			break
		}
		content, err := s.docStore.ReadAt(ctx, kbID, cand.docID, versionID)
		if err != nil {
			continue
		}
		results = append(results, scoredDoc{docID: cand.docID, score: cand.score, content: string(content)})
	}
	stageRead = time.Since(readStart)

	out := make([]*pb.QueryResult, len(results))
	for i, r := range results {
		out[i] = &pb.QueryResult{
			DocId:   r.docID,
			Content: r.content,
			Score:   r.score,
		}
	}

	return &pb.QueryResponse{Results: out, VersionId: versionID}, nil
}

// aggregate combines per-chunk scores into a per-document score using the
// specified aggregation method.
func aggregate(scores []float32, method pb.AggregationMethod) float32 {
	if len(scores) == 0 {
		return 0
	}
	if len(scores) == 1 {
		return scores[0]
	}

	sort.Slice(scores, func(i, j int) bool { return scores[i] < scores[j] })

	switch method {
	case pb.AggregationMethod_AGGREGATION_METHOD_MAX:
		return scores[len(scores)-1]
	case pb.AggregationMethod_AGGREGATION_METHOD_MEAN:
		var sum float32
		for _, s := range scores {
			sum += s
		}
		return sum / float32(len(scores))
	case pb.AggregationMethod_AGGREGATION_METHOD_MEDIAN:
		fallthrough
	default:
		mid := len(scores) / 2
		if len(scores)%2 == 0 {
			return (scores[mid-1] + scores[mid]) / 2
		}
		return scores[mid]
	}
}

var _ pb.QueryServiceServer = (*QueryServiceImpl)(nil)
