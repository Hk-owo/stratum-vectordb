package integration_test

import (
	"context"
	"testing"

	pb "stratum/api/proto/stratum"
)

// TestIntegration_CreateVersionIdempotentRetry pins the end-to-end path of the
// client idempotency key — proto field → service → write coordinator → Raft
// state machine. A retry carrying the same key must return the version the
// first attempt allocated, which is what lets a client re-send the changes for
// a version whose data never landed (Stratum_设计文档v13.md §7.12).
func TestIntegration_CreateVersionIdempotentRetry(t *testing.T) {
	cluster := newTestCluster(t)
	defer cluster.Close()
	ctx := context.Background()

	createResp, err := cluster.KBClient.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name:             "test-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "mock:8080",
			ModelId:     "m1",
		},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase failed: %v", err)
	}
	kbID := createResp.KnowledgeBaseId
	// The parent the retried request names: a knowledge base no longer comes with a
	// version, so the root is created here (docs/cursor-persistence-plan.md §5).
	root := seedRootVersion(t, cluster.RaftNode, ctx, kbID)

	req := &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: root,
		ClientRequestId: "retry-key-1",
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "hello world"},
		},
	}

	first, err := cluster.KBClient.CreateVersion(ctx, req)
	if err != nil {
		t.Fatalf("first CreateVersion: %v", err)
	}

	retry, err := cluster.KBClient.CreateVersion(ctx, req)
	if err != nil {
		t.Fatalf("retried CreateVersion: %v", err)
	}
	if retry.VersionId != first.VersionId {
		t.Fatalf("retry returned v%d, want the original v%d: the client key did not reach the state machine",
			retry.VersionId, first.VersionId)
	}
}
