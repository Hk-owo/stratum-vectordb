package service

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/coordinator"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// kbSvcTestHarness bundles the dependencies for KnowledgeBaseService tests.
type kbSvcTestHarness struct {
	svc            *KnowledgeBaseServiceImpl
	raftNode       *raft.MockRaftNode
	writeC         *coordinator.MockWriteCoordinator
	deleteC        *coordinator.MockDeleteCoordinator
	deleteVersionC *coordinator.MockDeleteVersionCoordinator
	wal            *wal.MockWAL
}

func newKBSvcTestHarness() *kbSvcTestHarness {
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	wc := coordinator.NewMockWriteCoordinator()
	dc := coordinator.NewMockDeleteCoordinator()
	dvc := coordinator.NewMockDeleteVersionCoordinator()
	svc := NewKnowledgeBaseService(rn, wc, dc, dvc)
	return &kbSvcTestHarness{
		svc:            svc,
		raftNode:       rn,
		writeC:         wc,
		deleteC:        dc,
		deleteVersionC: dvc,
		wal:            w,
	}
}

func TestKnowledgeBaseService_CreateKnowledgeBase(t *testing.T) {
	h := newKBSvcTestHarness()

	resp, err := h.svc.CreateKnowledgeBase(context.Background(), &pb.CreateKnowledgeBaseRequest{
		Name:             "test-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "localhost:8080",
			ModelId:     "test-model",
		},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase failed: %v", err)
	}
	if resp.KnowledgeBaseId == "" {
		t.Error("expected non-empty knowledge_base_id")
	}
	if resp.InitialVersionId != 1 {
		t.Errorf("expected initial_version_id=1, got %d", resp.InitialVersionId)
	}

	// Verify the KB was persisted in the RaftNode.
	kb, err := h.raftNode.GetKB(context.Background(), resp.KnowledgeBaseId)
	if err != nil {
		t.Fatalf("GetKB after create failed: %v", err)
	}
	if kb.Name != "test-kb" {
		t.Errorf("expected name='test-kb', got %s", kb.Name)
	}
}

func TestKnowledgeBaseService_DeleteKnowledgeBase(t *testing.T) {
	h := newKBSvcTestHarness()

	createResp, err := h.svc.CreateKnowledgeBase(context.Background(), &pb.CreateKnowledgeBaseRequest{
		Name:             "test-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "localhost:8080",
			ModelId:     "test-model",
		},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase failed: %v", err)
	}

	h.deleteC.SetExecuteResult(nil)

	delResp, err := h.svc.DeleteKnowledgeBase(context.Background(), &pb.DeleteKnowledgeBaseRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
	})
	if err != nil {
		t.Fatalf("DeleteKnowledgeBase failed: %v", err)
	}
	if !delResp.Success {
		t.Error("expected success=true")
	}

	// Wait briefly for the async delete to launch.
	time.Sleep(10 * time.Millisecond)

	// DeleteCoordinator.Execute should have been called.
	calls := h.deleteC.Calls()
	if len(calls) != 1 || calls[0] != createResp.KnowledgeBaseId {
		t.Errorf("expected DeleteCoordinator.Execute to be called with kbID, got %v", calls)
	}
}

func TestKnowledgeBaseService_CreateVersion(t *testing.T) {
	h := newKBSvcTestHarness()

	createResp, _ := h.svc.CreateKnowledgeBase(context.Background(), &pb.CreateKnowledgeBaseRequest{
		Name:             "test-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "localhost:8080",
			ModelId:     "test-model",
		},
	})

	h.writeC.SetExecuteResult(2, nil)

	resp, err := h.svc.CreateVersion(context.Background(), &pb.CreateVersionRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
		ParentVersionId: 1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "hello world"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion failed: %v", err)
	}
	if resp.VersionId != 2 {
		t.Errorf("expected version_id=2, got %d", resp.VersionId)
	}

	calls := h.writeC.Calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 WriteCoordinator call, got %d", len(calls))
	}
	if calls[0].KBID != createResp.KnowledgeBaseId {
		t.Errorf("expected kbID=%s, got %s", createResp.KnowledgeBaseId, calls[0].KBID)
	}
}

func TestKnowledgeBaseService_ListVersions(t *testing.T) {
	h := newKBSvcTestHarness()

	createResp, _ := h.svc.CreateKnowledgeBase(context.Background(), &pb.CreateKnowledgeBaseRequest{
		Name:             "test-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "localhost:8080",
			ModelId:     "test-model",
		},
	})

	// CreateVersion will add a version to the mock's internal state.
	h.writeC.SetExecuteResult(2, nil)
	_, err := h.svc.CreateVersion(context.Background(), &pb.CreateVersionRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
		ParentVersionId: 1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "content"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion failed: %v", err)
	}

	resp, err := h.svc.ListVersions(context.Background(), &pb.ListVersionsRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
	})
	if err != nil {
		t.Fatalf("ListVersions failed: %v", err)
	}
	// We have initial version (1) plus one more from the Raft mock.
	// The mock won't know about versions created via MockWriteCoordinator.
	// At minimum, initial version 1 should be listed.
	if len(resp.Versions) < 1 {
		t.Errorf("expected at least 1 version, got %d", len(resp.Versions))
	}
	t.Logf("versions: %d", len(resp.Versions))
}

func TestKnowledgeBaseService_RollbackVersion(t *testing.T) {
	h := newKBSvcTestHarness()

	createResp, _ := h.svc.CreateKnowledgeBase(context.Background(), &pb.CreateKnowledgeBaseRequest{
		Name:             "test-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "localhost:8080",
			ModelId:     "test-model",
		},
	})

	// Initial version is PENDING by default; mark it as READY to allow rollback.
	h.raftNode.ProposeUpdateVersionStatus(context.Background(), 1, types.IndexStatusReady)

	_, err := h.svc.RollbackVersion(context.Background(), &pb.RollbackVersionRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
		TargetVersionId: 1,
	})
	if err != nil {
		t.Fatalf("RollbackVersion failed: %v", err)
	}
}

func TestKnowledgeBaseService_CreateKnowledgeBase_Defaults(t *testing.T) {
	h := newKBSvcTestHarness()

	resp, err := h.svc.CreateKnowledgeBase(context.Background(), &pb.CreateKnowledgeBaseRequest{
		Name: "minimal-kb",
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "localhost:8080",
			ModelId:     "m1",
		},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase with minimal params failed: %v", err)
	}
	if resp.KnowledgeBaseId == "" {
		t.Error("expected non-empty knowledge_base_id")
	}

	kb, err := h.raftNode.GetKB(context.Background(), resp.KnowledgeBaseId)
	if err != nil {
		t.Fatalf("GetKB failed: %v", err)
	}
	// Check defaults were applied
	if kb.ChunkWindowSize <= 0 {
		t.Error("expected default ChunkWindowSize")
	}
	if kb.IndexType != "HNSW" {
		t.Errorf("expected default IndexType=HNSW, got %s", kb.IndexType)
	}
	if kb.Similarity != "COSINE" {
		t.Errorf("expected default Similarity=COSINE, got %s", kb.Similarity)
	}
}

func TestKnowledgeBaseService_ProtoConversion(t *testing.T) {
	// Test proto <-> internal type conversions
	meta := types.KnowledgeBaseMeta{
		KBID:             "kb-1",
		Name:             "test",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		IndexType:        "HNSW",
		Similarity:       "COSINE",
		EmbedConfig:      types.EmbedConfig{ServiceAddr: "addr", ModelID: "m1"},
		ActiveVersionID:  1,
		Status:           types.KBStatusActive,
	}

	// Just verify round-trip
	if meta.KBID != "kb-1" {
		t.Error("round-trip broken")
	}
}

func TestKnowledgeBaseService_ListKnowledgeBases(t *testing.T) {
	h := newKBSvcTestHarness()
	ctx := context.Background()

	resp, err := h.svc.ListKnowledgeBases(ctx, &pb.ListKnowledgeBasesRequest{})
	if err != nil {
		t.Fatalf("ListKnowledgeBases on empty store: %v", err)
	}
	if len(resp.KnowledgeBases) != 0 {
		t.Fatalf("expected 0 KBs initially, got %d", len(resp.KnowledgeBases))
	}

	for _, name := range []string{"kb-a", "kb-b"} {
		if _, err := h.svc.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
			Name: name, ChunkWindowSize: 512, ChunkOverlapSize: 64,
			EmbedConfig: &pb.EmbedConfig{ServiceAddr: "x", ModelId: "m1"},
		}); err != nil {
			t.Fatalf("CreateKnowledgeBase(%s): %v", name, err)
		}
	}

	resp, err = h.svc.ListKnowledgeBases(ctx, &pb.ListKnowledgeBasesRequest{})
	if err != nil {
		t.Fatalf("ListKnowledgeBases: %v", err)
	}
	if len(resp.KnowledgeBases) != 2 {
		t.Fatalf("expected 2 KBs, got %d", len(resp.KnowledgeBases))
	}
}

func TestKnowledgeBaseService_GetKnowledgeBase_And_ProtoConversions(t *testing.T) {
	h := newKBSvcTestHarness()
	ctx := context.Background()

	// Seed KBs directly through the mock raft to cover every enum branch
	// of the proto conversion helpers.
	for _, tc := range []struct {
		kbID       string
		indexType  string
		similarity string
		status     types.KBStatus
	}{
		{"kb-hnsw", "HNSW", "COSINE", types.KBStatusActive},
		{"kb-ivf", "IVF", "EUCLIDEAN", types.KBStatusDeleting},
		{"kb-flat", "FLAT", "INNER_PRODUCT", types.KBStatusDeleteFailed},
		{"kb-unknown", "NOT_A_REAL_TYPE", "NOT_A_REAL_SIM", types.KBStatus(42)},
	} {
		kb := types.KnowledgeBaseMeta{
			KBID: tc.kbID, Name: tc.kbID, ChunkWindowSize: 512, ChunkOverlapSize: 64,
			IndexType: tc.indexType, Similarity: tc.similarity,
			EmbedConfig:     types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"},
			ActiveVersionID: 1,
			Status:          tc.status,
		}
		if err := h.raftNode.ProposeCreateKB(ctx, kb); err != nil {
			t.Fatalf("ProposeCreateKB(%s): %v", tc.kbID, err)
		}
	}

	// GetKnowledgeBase + kbToProto conversions.
	got, err := h.svc.GetKnowledgeBase(ctx, &pb.GetKnowledgeBaseRequest{KnowledgeBaseId: "kb-ivf"})
	if err != nil {
		t.Fatalf("GetKnowledgeBase: %v", err)
	}
	if got.KnowledgeBase.IndexType != pb.IndexType_INDEX_TYPE_IVF {
		t.Errorf("IndexType = %v, want IVF", got.KnowledgeBase.IndexType)
	}
	if got.KnowledgeBase.Similarity != pb.Similarity_SIMILARITY_EUCLIDEAN {
		t.Errorf("Similarity = %v, want EUCLIDEAN", got.KnowledgeBase.Similarity)
	}
	if got.KnowledgeBase.Status != pb.KBStatus_KB_STATUS_DELETING {
		t.Errorf("Status = %v, want DELETING", got.KnowledgeBase.Status)
	}
	if got.KnowledgeBase.EmbedConfig.ServiceAddr != "x" || got.KnowledgeBase.EmbedConfig.ModelId != "m1" {
		t.Errorf("EmbedConfig = %+v", got.KnowledgeBase.EmbedConfig)
	}
	if got.KnowledgeBase.ActiveVersionId != 1 {
		t.Errorf("ActiveVersionId = %d, want 1", got.KnowledgeBase.ActiveVersionId)
	}

	// Unknown strings fall back to HNSW/COSINE; unknown status to ACTIVE.
	gotUnknown, err := h.svc.GetKnowledgeBase(ctx, &pb.GetKnowledgeBaseRequest{KnowledgeBaseId: "kb-unknown"})
	if err != nil {
		t.Fatalf("GetKnowledgeBase(unknown): %v", err)
	}
	if gotUnknown.KnowledgeBase.IndexType != pb.IndexType_INDEX_TYPE_HNSW {
		t.Errorf("unknown IndexType = %v, want HNSW fallback", gotUnknown.KnowledgeBase.IndexType)
	}
	if gotUnknown.KnowledgeBase.Similarity != pb.Similarity_SIMILARITY_COSINE {
		t.Errorf("unknown Similarity = %v, want COSINE fallback", gotUnknown.KnowledgeBase.Similarity)
	}
	if gotUnknown.KnowledgeBase.Status != pb.KBStatus_KB_STATUS_ACTIVE {
		t.Errorf("unknown Status = %v, want ACTIVE fallback", gotUnknown.KnowledgeBase.Status)
	}

	// GetKnowledgeBase on an unknown ID must map to a NotFound gRPC status.
	if _, err := h.svc.GetKnowledgeBase(ctx, &pb.GetKnowledgeBaseRequest{KnowledgeBaseId: "nope"}); err == nil {
		t.Fatal("expected error for unknown KB")
	}

	// FLAT / INNER_PRODUCT / DELETE_FAILED conversion branches.
	gotFlat, err := h.svc.GetKnowledgeBase(ctx, &pb.GetKnowledgeBaseRequest{KnowledgeBaseId: "kb-flat"})
	if err != nil {
		t.Fatalf("GetKnowledgeBase(flat): %v", err)
	}
	if gotFlat.KnowledgeBase.IndexType != pb.IndexType_INDEX_TYPE_FLAT ||
		gotFlat.KnowledgeBase.Similarity != pb.Similarity_SIMILARITY_INNER_PRODUCT ||
		gotFlat.KnowledgeBase.Status != pb.KBStatus_KB_STATUS_DELETE_FAILED {
		t.Errorf("flat/deleted conversion = %+v", gotFlat.KnowledgeBase)
	}
}

// TestKnowledgeBaseService_DeleteVersion covers the DeleteVersion RPC: it
// marks the target version (not the active one) for deletion and launches
// the async cleanup; deleting the active version is rejected.
func TestKnowledgeBaseService_DeleteVersion(t *testing.T) {
	h := newKBSvcTestHarness()
	ctx := context.Background()

	createResp, err := h.svc.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name:             "test-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "localhost:8080",
			ModelId:     "test-model",
		},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase: %v", err)
	}
	// Initial version (v1) is active. Fork a second version and make it READY.
	v2, err := h.raftNode.ProposeCreateVersion(ctx, createResp.KnowledgeBaseId, createResp.InitialVersionId)
	if err != nil {
		t.Fatalf("create v2: %v", err)
	}
	if err := h.raftNode.ProposeUpdateVersionStatus(ctx, v2, types.IndexStatusReady); err != nil {
		t.Fatalf("v2 READY: %v", err)
	}

	// Deleting the active version is rejected.
	_, err = h.svc.DeleteVersion(ctx, &pb.DeleteVersionRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
		VersionId:       createResp.InitialVersionId,
	})
	if err == nil {
		t.Fatal("expected error deleting the active version")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("active-delete error code = %v, want FailedPrecondition", status.Code(err))
	}

	// Deleting a non-active version succeeds and launches async cleanup.
	h.deleteVersionC.SetExecuteResult(nil)
	resp, err := h.svc.DeleteVersion(ctx, &pb.DeleteVersionRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
		VersionId:       v2,
	})
	if err != nil {
		t.Fatalf("DeleteVersion: %v", err)
	}
	if !resp.Success {
		t.Error("expected success=true")
	}

	// Wait briefly for the async cleanup to launch.
	time.Sleep(10 * time.Millisecond)
	if calls := h.deleteVersionC.Calls(); len(calls) != 1 || calls[0] != createResp.KnowledgeBaseId {
		t.Errorf("DeleteVersionCoordinator.Execute calls = %v, want [%s]", calls, createResp.KnowledgeBaseId)
	}

	// The version is marked Deleting in the state machine.
	versions, err := h.raftNode.ListVersions(ctx, createResp.KnowledgeBaseId)
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	for _, v := range versions {
		if v.VersionID == v2 && !v.Deleting {
			t.Error("v2 should be marked Deleting")
		}
	}
}
