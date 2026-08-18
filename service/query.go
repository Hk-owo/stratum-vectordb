package service

import (
	"context"
	"errors"
	"sort"

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
	versionBloom   bloom.BloomFilter
}

// NewQueryService constructs a QueryServiceImpl.
func NewQueryService(
	rn raft.RaftNode,
	im index.IndexManager,
	cdm chunkdoc.ChunkDocMapper,
	vdl versiondoc.VersionDocList,
	ds docstore.DocStore,
	vBloom bloom.BloomFilter,
) *QueryServiceImpl {
	return &QueryServiceImpl{
		raftNode:       rn,
		indexManager:   im,
		chunkDocMapper: cdm,
		versionDocList: vdl,
		docStore:       ds,
		versionBloom:   vBloom,
	}
}

// Query implements QueryServiceServer.
func (s *QueryServiceImpl) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	kbID := req.KnowledgeBaseId

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
	versions, err := s.raftNode.ListVersions(ctx, kbID)
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

	searchResults, err := s.indexManager.Search(ctx, kbID, versionID, req.Vector, searchTopK)
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

	for _, r := range searchResults {
		if r.Score < threshold {
			continue
		}
		docIDs, err := s.chunkDocMapper.ListDocIDs(ctx, kbID, r.ChunkID)
		if err != nil {
			continue
		}
		for _, docID := range docIDs {
			// Version filter: bloom check then authoritative check.
			if s.versionBloom.Test(docID) {
				// Confirm against version doc list.
				vdocs, err := s.versionDocList.ListDocIDs(ctx, kbID, versionID)
				if err != nil {
					continue
				}
				foundVD := false
				for _, vd := range vdocs {
					if vd == docID {
						foundVD = true
						break
					}
				}
				if !foundVD {
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

	// Aggregate per-document scores.
	agg := req.Aggregation
	// 0 is the default (MEDIAN), so treat it as MEDIAN.

	type scoredDoc struct {
		docID   string
		score   float32
		content string
	}
	var results []scoredDoc

	for docID, ds := range docMap {
		score := aggregate(ds.scores, agg)
		// Read document content.
		content, err := s.docStore.ReadAt(ctx, kbID, docID, versionID)
		if err != nil {
			continue
		}
		results = append(results, scoredDoc{docID: docID, score: score, content: string(content)})
	}

	// Sort by score descending.
	sort.Slice(results, func(i, j int) bool { return results[i].score > results[j].score })

	// Truncate to top_k.
	if int(req.TopK) < len(results) {
		results = results[:int(req.TopK)]
	}

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
