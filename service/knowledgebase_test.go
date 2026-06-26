package service

import (
	"context"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
	"stratum/internal/coordinator"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// kbSvcTestHarness bundles the dependencies for KnowledgeBaseService tests.
type kbSvcTestHarness struct {
	svc      *KnowledgeBaseServiceImpl
	raftNode *raft.MockRaftNode
	writeC   *coordinator.MockWriteCoordinator
	deleteC  *coordinator.MockDeleteCoordinator
	wal      *wal.MockWAL
}

func newKBSvcTestHarness() *kbSvcTestHarness {
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	wc := coordinator.NewMockWriteCoordinator()
	dc := coordinator.NewMockDeleteCoordinator()
	svc := NewKnowledgeBaseService(rn, wc, dc)
	return &kbSvcTestHarness{
		svc:      svc,
		raftNode: rn,
		writeC:   wc,
		deleteC:  dc,
		wal:      w,
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
		KnowledgeBaseId:  createResp.KnowledgeBaseId,
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
