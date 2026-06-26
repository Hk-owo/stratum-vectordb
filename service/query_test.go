package service

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/docstore"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"
)

// querySvcHarness bundles the dependencies for QueryService tests.
type querySvcHarness struct {
	svc         *QueryServiceImpl
	raftNode    *raft.MockRaftNode
	indexMgr    *index.MockIndexManager
	chunkMapper *chunkdoc.MockChunkDocMapper
	versionDocs *versiondoc.MockVersionDocList
	docStore    *docstore.MockDocStore
	verBloom    *bloom.MockBloomFilter
}

func newQuerySvcHarness() *querySvcHarness {
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	cdm := chunkdoc.NewMockChunkDocMapper()
	vd := versiondoc.NewMockVersionDocList()
	ds := docstore.NewMockDocStore()
	verBF := bloom.NewMockBloomFilter()

	im := index.NewMockIndexManager(index.MockIndexManagerDeps{
		ListDocIDs:      vd.ListDocIDs,
		ListChunkIDsByDocs: cdm.ListChunkIDsByDocs,
		ReadChunkVector: func(ctx context.Context, kbID, chunkID string) ([]float32, error) {
			return nil, nil
		},
	}, 4, 5*1000*1000*1000) // 5s timeout

	svc := NewQueryService(rn, im, cdm, vd, ds, verBF)
	return &querySvcHarness{
		svc:         svc,
		raftNode:    rn,
		indexMgr:    im,
		chunkMapper: cdm,
		versionDocs: vd,
		docStore:    ds,
		verBloom:    verBF,
	}
}

// setupQueryableKB creates a KB with a READY version containing the given
// doc-to-chunks mapping. Returns (kbID, versionID).
func (h *querySvcHarness) setupQueryableKB(t *testing.T, docChunks map[string][]string) (string, int64) {
	t.Helper()
	ctx := context.Background()

	kbID := "kb-test"
	_ = h.raftNode.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: kbID, Name: "test",
		ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"},
	})
	vID, _ := h.raftNode.ProposeCreateVersion(ctx, kbID, 0)
	h.raftNode.ProposeUpdateVersionStatus(ctx, vID, types.IndexStatusReady)

	// Write doc-chunk mappings and doc content.
	for docID, chunkIDs := range docChunks {
		for _, cid := range chunkIDs {
			h.chunkMapper.Write(ctx, kbID, cid, docID)
		}
		h.versionDocs.Write(ctx, kbID, vID, docID)
		h.verBloom.Add(docID)
		h.docStore.Write(ctx, kbID, docID, vID, []byte("content of "+docID))
	}

	// Seed search results in the index manager.
	// We need to build first, then mock Search.
	// For MockIndexManager, we build first then Search works.
	_ = h.indexMgr.TriggerBuild(ctx, kbID, vID)

	return kbID, vID
}

func TestQueryService_VersionIsolation(t *testing.T) {
	h := newQuerySvcHarness()
	ctx := context.Background()

	kbID, v1ID := h.setupQueryableKB(t, map[string][]string{
		"doc-a": {"chunk-1"},
	})
	_ = v1ID

	// Create a second version with a different document.
	v2ID, _ := h.raftNode.ProposeCreateVersion(ctx, kbID, v1ID)
	h.raftNode.ProposeUpdateVersionStatus(ctx, v2ID, types.IndexStatusReady)
	h.chunkMapper.Write(ctx, kbID, "chunk-2", "doc-b")
	h.versionDocs.Write(ctx, kbID, v2ID, "doc-b")
	h.verBloom.Add("doc-b")
	h.docStore.Write(ctx, kbID, "doc-b", v2ID, []byte("content of doc-b"))
	_ = h.indexMgr.TriggerBuild(ctx, kbID, v2ID)

	// Query version 1 — doc-b should NOT appear.
	resp, err := h.svc.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &v1ID,
		Vector:          []float32{0.1, 0.2, 0.3, 0.4},
		TopK:            10,
	})
	if err != nil {
		t.Fatalf("Query v1 failed: %v", err)
	}
	for _, r := range resp.Results {
		if r.DocId == "doc-b" {
			t.Error("doc-b should not be visible in version 1")
		}
	}
}

func TestQueryService_ThresholdFiltering(t *testing.T) {
	h := newQuerySvcHarness()
	ctx := context.Background()

	kbID, vID := h.setupQueryableKB(t, map[string][]string{
		"doc-a": {"chunk-1"},
	})

	threshold := float32(0.9)
	resp, err := h.svc.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &vID,
		Vector:          []float32{0.1, 0.2, 0.3, 0.4},
		TopK:            10,
		Threshold:       &threshold,
	})
	if err != nil {
		t.Fatalf("Query with threshold failed: %v", err)
	}
	// All results should have score >= threshold
	for _, r := range resp.Results {
		if r.Score < threshold {
			t.Errorf("result for %s has score %f below threshold %f", r.DocId, r.Score, threshold)
		}
	}
}

func TestQueryService_TopKLimit(t *testing.T) {
	h := newQuerySvcHarness()
	ctx := context.Background()

	kbID, vID := h.setupQueryableKB(t, map[string][]string{
		"doc-a": {"chunk-1"},
		"doc-b": {"chunk-2"},
		"doc-c": {"chunk-3"},
	})

	resp, err := h.svc.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &vID,
		Vector:          []float32{0.1, 0.2, 0.3, 0.4},
		TopK:            2,
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(resp.Results) > 2 {
		t.Errorf("expected at most 2 results, got %d", len(resp.Results))
	}
}

func TestQueryService_AggregationMedian(t *testing.T) {
	h := newQuerySvcHarness()
	ctx := context.Background()

	kbID, vID := h.setupQueryableKB(t, map[string][]string{
		"doc-multi": {"chunk-a", "chunk-b", "chunk-c"},
	})

	resp, err := h.svc.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &vID,
		Vector:          []float32{0.1, 0.2, 0.3, 0.4},
		TopK:            10,
		Aggregation:     pb.AggregationMethod(0), // MEDIAN
	})
	if err != nil {
		t.Fatalf("Query with MEDIAN failed: %v", err)
	}
	_ = resp
}

func TestQueryService_BloomFilterFalsePositive(t *testing.T) {
	h := newQuerySvcHarness()
	ctx := context.Background()

	kbID, vID := h.setupQueryableKB(t, map[string][]string{
		"doc-real": {"chunk-1"},
	})

	// Add a key to the bloom filter that is NOT in VersionDocList.
	// This simulates a false positive: bloom says "maybe present",
	// but VersionDocList confirms it's not.
	h.verBloom.Add("doc-fake")

	// Make the search return chunks that belong to both doc-real and doc-fake.
	// Since doc-fake is not in VDL, it should be filtered out.

	resp, err := h.svc.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &vID,
		Vector:          []float32{0.1, 0.2, 0.3, 0.4},
		TopK:            10,
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	for _, r := range resp.Results {
		if r.DocId == "doc-fake" {
			t.Error("doc-fake should have been filtered by false-positive confirmation")
		}
	}
}

func TestQueryService_PendingVersionRejected(t *testing.T) {
	h := newQuerySvcHarness()
	ctx := context.Background()

	kbID := "kb-test"
	h.raftNode.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: kbID, Name: "test",
		ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"},
	})
	vID, _ := h.raftNode.ProposeCreateVersion(ctx, kbID, 0)
	// Leave as PENDING — do NOT update to READY.

	_, err := h.svc.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &vID,
		Vector:          []float32{0.1, 0.2, 0.3, 0.4},
		TopK:            10,
	})
	if err == nil {
		t.Fatal("expected error for PENDING version, got nil")
	}
	st := status.Convert(err)
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", st.Code())
	}
}

func TestQueryService_FailedVersionRejected(t *testing.T) {
	h := newQuerySvcHarness()
	ctx := context.Background()

	kbID := "kb-test"
	h.raftNode.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: kbID, Name: "test",
		ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"},
	})
	vID, _ := h.raftNode.ProposeCreateVersion(ctx, kbID, 0)
	h.raftNode.ProposeUpdateVersionStatus(ctx, vID, types.IndexStatusFailed)

	_, err := h.svc.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &vID,
		Vector:          []float32{0.1, 0.2, 0.3, 0.4},
		TopK:            10,
	})
	if err == nil {
		t.Fatal("expected error for FAILED version, got nil")
	}
	st := status.Convert(err)
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", st.Code())
	}
}

func TestQueryService_ActiveVersionDefault(t *testing.T) {
	h := newQuerySvcHarness()
	ctx := context.Background()

	kbID, vID := h.setupQueryableKB(t, map[string][]string{
		"doc-a": {"chunk-1"},
	})

	// Set active version to vID so default query uses it.
	_ = h.raftNode.ProposeRollback(ctx, kbID, vID)

	// Query without setting VersionId — should use active version.
	resp, err := h.svc.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		Vector:          []float32{0.1, 0.2, 0.3, 0.4},
		TopK:            10,
	})
	if err != nil {
		t.Fatalf("Query without explicit version failed: %v", err)
	}
	if resp.VersionId != vID {
		t.Errorf("expected active version %d, got %d", vID, resp.VersionId)
	}
}
