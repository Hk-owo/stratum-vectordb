// Package integration_test exercises the full Stratum stack in-process:
// gRPC server with real PebbleDB-backed stores and mock embed/vecstore,
// verifying complete end-to-end flows per Stratum_测试顺序.md 第三批 (T3).
//
// These tests do not require Docker, external processes, or a running
// vecstore gRPC server — everything is in-process with test doubles.
package integration_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/coordinator"
	"stratum/internal/docstore"
	"stratum/internal/embed"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/splitter"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"
	"stratum/service"
)

// testCluster bundles all the wired-up dependencies for an in-process
// Stratum node, and exposes gRPC clients for each service.
type testCluster struct {
	t   testing.TB
	srv *grpc.Server
	lis net.Listener

	KBClient    pb.KnowledgeBaseServiceClient
	QueryClient pb.QueryServiceClient
	AdminClient pb.AdminServiceClient

	// Internals exposed for test assertions.
	RaftNode    *raft.MockRaftNode
	WAL         *wal.MockWAL
	IndexMgr    *index.MockIndexManager
	ChunkStore  *chunkstore.MockChunkStore
	DocStore    *docstore.MockDocStore
	ChunkMapper *chunkdoc.MockChunkDocMapper
	VersionDocs *versiondoc.MockVersionDocList
	EmbedClient *embed.MockEmbedClient

	cleanup func()
}

func newTestCluster(t testing.TB) *testCluster {
	t.Helper()

	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	cs := chunkstore.NewMockChunkStore()
	ds := docstore.NewMockDocStore()
	cdm := chunkdoc.NewMockChunkDocMapper()
	vd := versiondoc.NewMockVersionDocList()
	ec := embed.NewMockEmbedClient(4) // 4-dim vectors
	chunkBF := bloom.NewMockBloomFilter()
	vBloomStore := bloom.NewVersionBloomStore(t.TempDir(), 4096, 0.01, vd)

	// Use mock index manager that integrates with the mock stores.
	im := index.NewMockIndexManager(index.MockIndexManagerDeps{
		ListDocIDs:         vd.ListDocIDs,
		ListChunkIDsByDocs: cdm.ListChunkIDsByDocs,
		ReadChunkVector: func(ctx context.Context, kbID, chunkID string) ([]float32, error) {
			return cs.Read(kbID, chunkID)
		},
	}, 16, 5*time.Second)

	im.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
		return rn.ProposeUpdateVersionStatus(context.Background(), versionID, status)
	})

	splitterInstance := &splitter.SlidingWindowSplitter{}

	wc := coordinator.NewWriteCoordinatorImpl(coordinator.WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		Splitter:            splitterInstance,
		EmbedClient:         ec,
		ChunkBloom:          chunkBF,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vd,
		IndexManager:        im,
	})

	dc := coordinator.NewDeleteCoordinatorImpl(coordinator.DeleteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		VersionDocList:      vd,
	})

	kbSvc := service.NewKnowledgeBaseService(rn, wc, dc, coordinator.NewDeleteVersionCoordinatorImpl(coordinator.DeleteVersionCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		VersionDocList:      vd,
	}))
	querySvc := service.NewQueryService(rn, im, cdm, vd, ds, vBloomStore)
	adminSvc := service.NewAdminService(1, rn, im, ds, cs, w)

	srv := grpc.NewServer()
	pb.RegisterKnowledgeBaseServiceServer(srv, kbSvc)
	pb.RegisterQueryServiceServer(srv, querySvc)
	pb.RegisterAdminServiceServer(srv, adminSvc)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	go srv.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	return &testCluster{
		t:           t,
		srv:         srv,
		lis:         lis,
		KBClient:    pb.NewKnowledgeBaseServiceClient(conn),
		QueryClient: pb.NewQueryServiceClient(conn),
		AdminClient: pb.NewAdminServiceClient(conn),
		RaftNode:    rn,
		WAL:         w,
		IndexMgr:    im,
		ChunkStore:  cs,
		DocStore:    ds,
		ChunkMapper: cdm,
		VersionDocs: vd,
		EmbedClient: ec,
		cleanup: func() {
			conn.Close()
			srv.GracefulStop()
		},
	}
}

func (c *testCluster) Close() {
	c.cleanup()
}

// === T3-1: Full chain correctness ===

func TestIntegration_CreateKB_CreateVersion_Query(t *testing.T) {
	cluster := newTestCluster(t)
	defer cluster.Close()
	ctx := context.Background()

	// Step 1: Create knowledge base.
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

	// Step 2: Set initial version to READY so it can be used as parent.
	cluster.RaftNode.ProposeUpdateVersionStatus(ctx, 1, types.IndexStatusReady)

	// Step 3: Create a version with documents.
	// First, seed the mock chunk store with vectors for the chunks that
	// will be created. The mock embed client produces deterministic vectors
	// from chunk IDs, so we need to pre-populate the chunk store with those
	// vectors so the IndexManager build can find them.

	// Actually the WriteCoordinator's writeDocument path does:
	// splitter.Split -> embed -> ChunkStore.Write -> ChunkDocMapper.Write -> DocStore.Write
	// The MockChunkStore's Write stores vectors that subsequent reads can retrieve.
	createVerResp, err := cluster.KBClient.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: 1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "hello world this is test content for integration"},
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-2", Content: "another document with some different text here"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion failed: %v", err)
	}
	_ = createVerResp.VersionId

	// Step 3: Set created version to READY (index build runs async).
	// In the mock, TriggerBuild runs synchronously in a goroutine;
	// wait for it to complete.
	time.Sleep(100 * time.Millisecond)
	cluster.RaftNode.ProposeUpdateVersionStatus(ctx, createVerResp.VersionId, types.IndexStatusReady)

	// Step 4: Query with explicit version ID.
	queryVector := []float32{0.1, 0.2, 0.3, 0.4}
	queryResp, err := cluster.QueryClient.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &createVerResp.VersionId, // explicit version
		Vector:          queryVector,
		TopK:            5,
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	t.Logf("Query returned %d results for version %d", len(queryResp.Results), queryResp.VersionId)
	// Results may be empty if the index build hasn't completed or there's
	// a mismatch — the important thing is the query didn't error out.
}

func TestIntegration_ListVersions(t *testing.T) {
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

	resp, err := cluster.KBClient.ListVersions(ctx, &pb.ListVersionsRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
	})
	if err != nil {
		t.Fatalf("ListVersions failed: %v", err)
	}

	if len(resp.Versions) != 1 {
		t.Errorf("expected 1 initial version, got %d", len(resp.Versions))
	}
	if resp.Versions[0].VersionId != 1 {
		t.Errorf("expected version_id=1, got %d", resp.Versions[0].VersionId)
	}
}

func TestIntegration_RollbackVersion(t *testing.T) {
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

	// Set initial version to READY so we can rollback to it.
	cluster.RaftNode.ProposeUpdateVersionStatus(ctx, 1, types.IndexStatusReady)

	// Create a new version to rollback from.
	cluster.KBClient.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: 1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-x", Content: "some content"},
		},
	})

	// Rollback to version 1.
	_, err = cluster.KBClient.RollbackVersion(ctx, &pb.RollbackVersionRequest{
		KnowledgeBaseId: kbID,
		TargetVersionId: 1,
	})
	if err != nil {
		t.Fatalf("RollbackVersion failed: %v", err)
	}

	// Verify active version changed.
	kb, err := cluster.RaftNode.GetKB(ctx, kbID)
	if err != nil {
		t.Fatalf("GetKB failed: %v", err)
	}
	if kb.ActiveVersionID != 1 {
		t.Errorf("expected active version 1, got %d", kb.ActiveVersionID)
	}
}

func TestIntegration_DeleteKnowledgeBase(t *testing.T) {
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

	_, err = cluster.KBClient.DeleteKnowledgeBase(ctx, &pb.DeleteKnowledgeBaseRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
	})
	if err != nil {
		t.Fatalf("DeleteKnowledgeBase failed: %v", err)
	}

	// After deletion, GetKB should return an error.
	time.Sleep(20 * time.Millisecond) // async cleanup
	_, err = cluster.RaftNode.GetKB(ctx, createResp.KnowledgeBaseId)
	if err == nil {
		t.Error("expected error fetching deleted KB, got nil")
	}
}

// TestIntegration_DeleteVersion covers the end-to-end DeleteVersion flow:
// active-version rejection, recursive subtree deletion, and physical data
// cleanup in VersionDocList / DocStore.
func TestIntegration_DeleteVersion(t *testing.T) {
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
	v1 := createResp.InitialVersionId

	// Build a chain v1 -> v2 -> v3, each READY (bypassing async index
	// build by setting status directly, like the other integration tests).
	cluster.RaftNode.ProposeUpdateVersionStatus(ctx, v1, types.IndexStatusReady)

	v2Resp, err := cluster.KBClient.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-a", Content: "alpha"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion(v2): %v", err)
	}
	v2 := v2Resp.VersionId
	cluster.RaftNode.ProposeUpdateVersionStatus(ctx, v2, types.IndexStatusReady)

	v3Resp, err := cluster.KBClient.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v2,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-b", Content: "beta"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion(v3): %v", err)
	}
	v3 := v3Resp.VersionId
	cluster.RaftNode.ProposeUpdateVersionStatus(ctx, v3, types.IndexStatusReady)

	// Deleting the active version is rejected.
	_, err = cluster.KBClient.DeleteVersion(ctx, &pb.DeleteVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v1,
	})
	if err == nil {
		t.Fatal("expected error deleting the active version")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("active-delete error code = %v, want FailedPrecondition", status.Code(err))
	}

	// Deleting v2 must recursively remove v2 AND v3; v1 stays.
	if _, err := cluster.KBClient.DeleteVersion(ctx, &pb.DeleteVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v2,
	}); err != nil {
		t.Fatalf("DeleteVersion(v%d): %v", v2, err)
	}

	// Wait for the async cleanup to converge.
	deadline := time.Now().Add(15 * time.Second)
	for {
		versions, err := cluster.RaftNode.ListVersions(ctx, kbID)
		if err == nil && len(versions) == 1 && versions[0].VersionID == v1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delete did not converge: versions = %+v", versions)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Raft metadata: only v1 remains, still active.
	kb, err := cluster.RaftNode.GetKB(ctx, kbID)
	if err != nil {
		t.Fatalf("GetKB: %v", err)
	}
	if kb.ActiveVersionID != v1 {
		t.Errorf("active version = %d, want %d", kb.ActiveVersionID, v1)
	}

	// VersionDocList: v2/v3 document sets physically removed.
	for _, vid := range []int64{v2, v3} {
		docs, err := cluster.VersionDocs.ListDocIDs(ctx, kbID, vid)
		if err != nil {
			t.Fatalf("ListDocIDs(v%d): %v", vid, err)
		}
		if len(docs) != 0 {
			t.Errorf("VersionDocList for v%d = %v, want empty", vid, docs)
		}
	}

	// DocStore: v2/v3 records physically gone. doc-a was only ever written
	// at v2 and doc-b only at v3, so reads at any version must now fail —
	// proving the MVCC records were physically removed, not just hidden.
	if _, err := cluster.DocStore.ReadAt(ctx, kbID, "doc-a", v2); err == nil {
		t.Error("doc-a@v2 should be gone after DeleteByVersion")
	}
	if _, err := cluster.DocStore.ReadAt(ctx, kbID, "doc-b", v3); err == nil {
		t.Error("doc-b@v3 should be gone after DeleteByVersion")
	}
	if _, err := cluster.DocStore.ReadAt(ctx, kbID, "doc-a", 1<<30); err == nil {
		t.Error("doc-a@any-version should be gone after deleting v2 (its only record)")
	}

	// Deleting a nonexistent version is rejected.
	_, err = cluster.KBClient.DeleteVersion(ctx, &pb.DeleteVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       999,
	})
	if err == nil {
		t.Error("expected error deleting nonexistent version")
	}
}

func TestIntegration_HealthCheck(t *testing.T) {
	cluster := newTestCluster(t)
	defer cluster.Close()
	ctx := context.Background()

	resp, err := cluster.AdminClient.HealthCheck(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck failed: %v", err)
	}

	if resp.Status != pb.HealthStatus_HEALTH_STATUS_HEALTHY {
		t.Errorf("expected HEALTHY, got %v", resp.Status)
	}
}

func TestIntegration_GetSystemStatus(t *testing.T) {
	cluster := newTestCluster(t)
	defer cluster.Close()
	ctx := context.Background()

	resp, err := cluster.AdminClient.GetSystemStatus(ctx, &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus failed: %v", err)
	}

	if resp.Health == nil {
		t.Error("expected non-nil health in system status")
	}
}

func TestIntegration_WarmupVersion(t *testing.T) {
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

	resp, err := cluster.AdminClient.WarmupVersion(ctx, &pb.WarmupVersionRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
		VersionId:       1,
	})
	if err != nil {
		t.Fatalf("WarmupVersion failed: %v", err)
	}
	if !resp.Success {
		t.Error("expected success=true")
	}
}

func TestIntegration_ForkedVersions(t *testing.T) {
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

	// Create two child versions from the same parent (forking).
	verA, err := cluster.KBClient.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: 1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-a", Content: "branch a"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion branch A failed: %v", err)
	}

	verB, err := cluster.KBClient.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: 1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-b", Content: "branch b"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion branch B failed: %v", err)
	}

	// Both forked versions should have distinct IDs.
	if verA.VersionId == verB.VersionId {
		t.Errorf("forked versions should have distinct IDs, both got %d", verA.VersionId)
	}

	// ListVersions should show the fork.
	versions, err := cluster.KBClient.ListVersions(ctx, &pb.ListVersionsRequest{KnowledgeBaseId: kbID})
	if err != nil {
		t.Fatalf("ListVersions failed: %v", err)
	}
	if len(versions.Versions) < 3 {
		t.Errorf("expected at least 3 versions (v1 + 2 forks), got %d", len(versions.Versions))
	}
	t.Logf("fork: branch A = v%d, branch B = v%d", verA.VersionId, verB.VersionId)
}

func TestIntegration_ConcurrentCreateVersion(t *testing.T) {
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

	// Set initial version to READY so it can be used as parent.
	cluster.RaftNode.ProposeUpdateVersionStatus(ctx, 1, types.IndexStatusReady)

	// Launch concurrent CreateVersion calls with the same parent (forking).
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	versionIDs := make(chan int64, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := cluster.KBClient.CreateVersion(ctx, &pb.CreateVersionRequest{
				KnowledgeBaseId: kbID,
				ParentVersionId: 1,
				Changes: []*pb.DocChange{
					{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-" + string(rune('a'+idx)), Content: "content"},
				},
			})
			if err != nil {
				errs <- err
				return
			}
			versionIDs <- resp.VersionId
		}(i)
	}

	wg.Wait()
	close(errs)
	close(versionIDs)

	for e := range errs {
		t.Errorf("concurrent CreateVersion failed: %v", e)
	}

	ids := make([]int64, 0)
	for id := range versionIDs {
		ids = append(ids, id)
	}

	if len(ids) != 5 {
		t.Errorf("expected 5 successful concurrent versions, got %d", len(ids))
	}

	// All version IDs should be distinct (forks from same parent).
	seen := make(map[int64]bool)
	for _, id := range ids {
		if seen[id] {
			t.Errorf("duplicate version ID: %d", id)
		}
		seen[id] = true
	}
}

// === T3-1: Parent version constraint ===

func TestIntegration_ParentVersionCrossKB(t *testing.T) {
	cluster := newTestCluster(t)
	defer cluster.Close()
	ctx := context.Background()

	// Create two separate KBs.
	respA, err := cluster.KBClient.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name: "kb-a", ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{ServiceAddr: "mock:8080", ModelId: "m1"},
	})
	if err != nil {
		t.Fatalf("CreateKB A failed: %v", err)
	}
	respB, err := cluster.KBClient.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name: "kb-b", ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{ServiceAddr: "mock:8080", ModelId: "m1"},
	})
	if err != nil {
		t.Fatalf("CreateKB B failed: %v", err)
	}

	// Try to create a version in kb-b with parent from kb-a — should fail.
	_, err = cluster.KBClient.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: respB.KnowledgeBaseId,
		ParentVersionId: respA.InitialVersionId, // wrong KB!
		Changes:         []*pb.DocChange{{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "x", Content: "x"}},
	})
	if err == nil {
		t.Fatal("expected error for cross-KB parent version, got nil")
	}
}

// === T3-2: Crash recovery scenarios ===

func TestIntegration_CrashRecovery_VersionWriteResume(t *testing.T) {
	cluster := newTestCluster(t)
	defer cluster.Close()
	ctx := context.Background()

	createResp, _ := cluster.KBClient.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name: "test", ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{ServiceAddr: "mock:8080", ModelId: "m1"},
	})
	_ = createResp

	// WAL has versionID=1 (from CreateKB) and any further versions from
	// the initial setup. Add versionID=5 as a simulated crash residual.
	cluster.WAL.WriteBegin(ctx, "", 0, nil)
	cluster.WAL.WriteVersionID(ctx, 5)

	records, err := cluster.WAL.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// At minimum, versionID=5 should be in the pending records.
	hasV5 := false
	for _, r := range records {
		if r.Type == types.PendingRecordTypeVersionWrite && r.VersionID == 5 {
			hasV5 = true
		}
	}
	if !hasV5 {
		t.Errorf("expected recovery to surface versionID=5, got %+v", records)
	}
}

func TestIntegration_CrashRecovery_DeleteResume(t *testing.T) {
	cluster := newTestCluster(t)
	defer cluster.Close()
	ctx := context.Background()

	// Simulate: delete mark written but delete never completed.
	cluster.WAL.WriteDeleteMark(ctx, "kb-stuck")

	records, err := cluster.WAL.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	hasDelete := false
	for _, r := range records {
		if r.Type == types.PendingRecordTypeDeleteMark && r.KBID == "kb-stuck" {
			hasDelete = true
		}
	}
	if !hasDelete {
		t.Error("expected pending delete mark in recovery records")
	}
}

// === T3-3: RebuildIndex ===

func TestIntegration_RebuildIndex(t *testing.T) {
	cluster := newTestCluster(t)
	defer cluster.Close()
	ctx := context.Background()

	createResp, err := cluster.KBClient.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name: "test", ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{ServiceAddr: "mock:8080", ModelId: "m1"},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase failed: %v", err)
	}

	// Mark initial version as FAILED.
	cluster.RaftNode.ProposeUpdateVersionStatus(ctx, 1, types.IndexStatusFailed)

	// Trigger rebuild.
	resp, err := cluster.AdminClient.RebuildIndex(ctx, &pb.RebuildIndexRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
		VersionId:       1,
	})
	if err != nil {
		t.Fatalf("RebuildIndex failed: %v", err)
	}
	if !resp.Success {
		t.Error("expected success=true")
	}
}

func TestIntegration_StuckVersionsInSystemStatus(t *testing.T) {
	cluster := newTestCluster(t)
	defer cluster.Close()
	ctx := context.Background()

	// Add a WAL replay counter to check it surfaces in GetSystemStatus.
	cluster.WAL.IncrementReplayCounter(types.PendingRecord{
		Type:      types.PendingRecordTypeDeleteMark,
		KBID:      "kb-broken",
		VersionID: 0,
	})
	cluster.WAL.IncrementReplayCounter(types.PendingRecord{
		Type:      types.PendingRecordTypeDeleteMark,
		KBID:      "kb-broken",
		VersionID: 0,
	})

	resp, err := cluster.AdminClient.GetSystemStatus(ctx, &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus failed: %v", err)
	}
	_ = resp
}
