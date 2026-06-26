// Package docker_test contains Stratum's T4 multi-node integration tests
// (Stratum_测试顺序.md 第四批). These tests require a running 3-node
// Docker Compose cluster and are protected by the "docker" build tag.
//
// Run:
//
//	docker compose -f integration/docker/docker-compose.yml up -d --wait
//	go test ./integration/docker/... -tags=docker -v -timeout 120s
//	docker compose -f integration/docker/docker-compose.yml down
//
//go:build docker
// +build docker

package docker_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
)

// nodeAddrs are the gRPC addresses of the 3 Stratum nodes in the
// Docker Compose cluster.
var nodeAddrs = []string{
	"localhost:17000",
	"localhost:17001",
	"localhost:17002",
}

func dialNode(addr string) (pb.KnowledgeBaseServiceClient, pb.QueryServiceClient, pb.AdminServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return pb.NewKnowledgeBaseServiceClient(conn),
		pb.NewQueryServiceClient(conn),
		pb.NewAdminServiceClient(conn),
		conn, nil
}

// === T4-1: Distributed correctness ===

func TestT4_MultiNode_Consistency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kb1, q1, _, c1, err := dialNode(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial node1: %v", err)
	}
	defer c1.Close()

	_, _, _, c2, err := dialNode(nodeAddrs[1])
	if err != nil {
		t.Fatalf("dial node2: %v", err)
	}
	defer c2.Close()

	// Create a KB on node 1.
	createResp, err := kb1.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name:             "multi-node-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "http://mock-embed:8080",
			ModelId:     "test-model",
		},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase on node1 failed: %v", err)
	}

	time.Sleep(2 * time.Second) // wait for Raft replication

	// Verify the same KB is visible from node 2.
	_, _, _, c2b, err := dialNode(nodeAddrs[1])
	if err != nil {
		t.Fatalf("dial node2 again: %v", err)
	}
	defer c2b.Close()
	kb2 := pb.NewKnowledgeBaseServiceClient(c2b)

	versions, err := kb2.ListVersions(ctx, &pb.ListVersionsRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
	})
	if err != nil {
		t.Fatalf("ListVersions on node2 failed: %v", err)
	}
	if len(versions.Versions) < 1 {
		t.Error("node2 should see the version created on node1")
	}

	// Query should also work.
	queryResp, err := q1.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
		VersionId:       &createResp.InitialVersionId,
		Vector:          make([]float32, 768),
		TopK:            5,
	})
	if err != nil {
		t.Fatalf("Query on node1 failed: %v", err)
	}
	_ = queryResp
}

// === T4-2: Fault tolerance ===

func TestT4_MinorityFaultTolerance(t *testing.T) {
	t.Skip("requires docker kill of a follower node — manual or CI-orchestrated")
}

func TestT4_LeaderFailover(t *testing.T) {
	t.Skip("requires docker kill of the leader node — manual or CI-orchestrated")
}

// === T4-3: Cluster performance ===

func TestT4_PerformanceBaseline(t *testing.T) {
	t.Skip("performance benchmarks — run with dedicated benchmarking harness")
}

// === T4-4: Storage efficiency ===

func TestT4_StorageEfficiency(t *testing.T) {
	t.Skip("requires populating 100万 chunks — long-running storage benchmark")
}
