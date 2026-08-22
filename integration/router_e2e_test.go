package integration_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
	"stratum/internal/raft"
	"stratum/internal/router"
)

// TestRouter_ThreeNodeCluster drives a real three-node cluster through the
// routing layer (the process-internal equivalent of running
// cmd/stratum-router in front of the cluster):
//
//   - writes via the router are forwarded to the current leader
//     (CreateKnowledgeBase / CreateVersion succeed);
//   - reads via the router succeed and are served by the cluster
//     (ListKnowledgeBases / Query);
//   - after the leader stops and a new one is elected, writes via the
//     router keep succeeding — the router re-discovers the leader on
//     failover.
//
// The short DiscoverTTL keeps re-discovery fast; the 100ms TTL is still
// well above a single gRPC round-trip.
func TestRouter_ThreeNodeCluster(t *testing.T) {
	// One vecstore subprocess per node, one shared mock embed server.
	vecAddrs := [3]string{}
	for i := range vecAddrs {
		vecAddrs[i] = startVecstoreServerForTest(t)
	}
	embedURL := startMockEmbedForTest(t, 4)

	var raftAddrs, grpcAddrs [3]string
	var baseDirs [3]string
	for i := 0; i < 3; i++ {
		raftAddrs[i] = freeLoopbackAddr(t)
		grpcAddrs[i] = freeLoopbackAddr(t)
		baseDirs[i] = filepath.Join(t.TempDir(), "node")
	}

	peers := []raft.PeerConfig{
		{ID: 1, RaftAddr: raftAddrs[0], ServiceAddr: grpcAddrs[0]},
		{ID: 2, RaftAddr: raftAddrs[1], ServiceAddr: grpcAddrs[1]},
		{ID: 3, RaftAddr: raftAddrs[2], ServiceAddr: grpcAddrs[2]},
	}
	addrByID := map[int64]string{1: grpcAddrs[0], 2: grpcAddrs[1], 3: grpcAddrs[2]}

	var nodes [3]*realNode
	for i := 0; i < 3; i++ {
		nodes[i] = newRealNodeWithAddrsAndDir(t, int64(i+1), peers, vecAddrs[i], embedURL,
			raftAddrs[i], grpcAddrs[i], baseDirs[i])
		wireSyncPull(t, nodes[i], addrByID)
	}

	// --- routing layer in front of the three nodes ---
	rt, err := router.NewRouter(router.Config{
		Addrs: []string{grpcAddrs[0], grpcAddrs[1], grpcAddrs[2]},
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	defer rt.Close()

	routerLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("router listen: %v", err)
	}
	routerGS := grpc.NewServer()
	pb.RegisterKnowledgeBaseServiceServer(routerGS, router.NewKBServer(rt))
	pb.RegisterQueryServiceServer(routerGS, router.NewQueryServer(rt))
	pb.RegisterAdminServiceServer(routerGS, router.NewAdminServer(rt))
	go routerGS.Serve(routerLis)
	defer routerGS.GracefulStop()

	routerConn, err := grpc.NewClient(routerLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("router dial: %v", err)
	}
	defer routerConn.Close()
	kbViaRouter := pb.NewKnowledgeBaseServiceClient(routerConn)
	queryViaRouter := pb.NewQueryServiceClient(routerConn)
	adminViaRouter := pb.NewAdminServiceClient(routerConn)

	ctx := context.Background()
	leader := waitForLeader(t, nodes[0], nodes[1], nodes[2])
	t.Logf("initial leader = node %d", leader.nodeID)

	// === write via router: CreateKnowledgeBase is forwarded to the leader ===
	kbResp, err := kbViaRouter.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{Name: "router-e2e"})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase via router: %v", err)
	}
	kbID := kbResp.KnowledgeBaseId
	v1 := kbResp.InitialVersionId

	// === read via router: ListKnowledgeBases round-trips through the cluster ===
	if err := waitListedViaRouter(ctx, kbViaRouter, kbID); err != nil {
		t.Fatalf("ListKnowledgeBases via router: %v", err)
	}

	// === write via router: CreateVersion with a doc change ===
	v2Resp, err := kbViaRouter.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "alpha"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion via router: %v", err)
	}
	v2 := v2Resp.VersionId
	leader.waitVersionReady(ctx, kbID, v2)

	// === read via router: Query returns the indexed doc ===
	if r := waitQueryDocViaRouter(t, queryViaRouter, kbID, v2, contentVector("alpha", 4), "doc-1"); r.Content != "alpha" {
		t.Errorf("query via router: content = %q, want alpha", r.Content)
	}

	// === failover: stop the leader, a new one is elected, writes keep working ===
	oldLeader := leader
	leader.Stop()
	t.Logf("stopped leader node %d", oldLeader.nodeID)
	var rest []*realNode
	for _, n := range nodes {
		if n != oldLeader {
			rest = append(rest, n)
		}
	}
	leader = waitForLeader(t, rest...)
	t.Logf("new leader = node %d", leader.nodeID)

	v3Resp, err := kbViaRouter.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v2,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-2", Content: "beta"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion via router after leader failover: %v", err)
	}
	v3 := v3Resp.VersionId
	leader.waitVersionReady(ctx, kbID, v3)
	if r := waitQueryDocViaRouter(t, queryViaRouter, kbID, v3, contentVector("beta", 4), "doc-2"); r.Content != "beta" {
		t.Errorf("query via router after failover: content = %q, want beta", r.Content)
	}

	// === admin read via router: GetClusterStatus reports a live cluster ===
	cs, err := adminViaRouter.GetClusterStatus(ctx, &pb.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("GetClusterStatus via router: %v", err)
	}
	if !cs.HasLeader {
		t.Error("GetClusterStatus via router: has_leader = false, want true")
	}
	if cs.LeaderId != leader.nodeID {
		t.Errorf("GetClusterStatus via router: leader_id = %d, want %d", cs.LeaderId, leader.nodeID)
	}
}

// waitListedViaRouter polls ListKnowledgeBases through the router until
// kbID appears. The write itself commits synchronously, but the router
// load-balances reads across nodes, so tolerate the first reads landing
// before the Raft apply has propagated to every node.
func waitListedViaRouter(ctx context.Context, c pb.KnowledgeBaseServiceClient, kbID string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.ListKnowledgeBases(ctx, &pb.ListKnowledgeBasesRequest{})
		if err == nil {
			for _, kb := range resp.KnowledgeBases {
				if kb.KnowledgeBaseId == kbID {
					return nil
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("kb %s never listed via router", kbID)
}

// waitQueryDocViaRouter polls the router's QueryService until docID is
// retrievable for the version. The router may serve the read from any
// node, and a follower's index build is asynchronous, so an eventually-
// consistent read is expected.
func waitQueryDocViaRouter(t *testing.T, c pb.QueryServiceClient, kbID string, versionID int64, vec []float32, docID string) *pb.QueryResult {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	backoff := 50 * time.Millisecond
	for {
		resp, err := c.Query(context.Background(), &pb.QueryRequest{
			KnowledgeBaseId: kbID,
			VersionId:       &versionID,
			Vector:          vec,
			TopK:            10,
		})
		if err == nil {
			if r := findResult(resp.Results, docID); r != nil {
				return r
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("query via router never returned %s on v%d; last err: %v", docID, versionID, err)
		}
		time.Sleep(backoff)
		if backoff < time.Second {
			backoff *= 2
		}
	}
}
