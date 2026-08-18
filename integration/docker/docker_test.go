// Package docker_test contains Stratum's T4 multi-node integration tests
// (Stratum_测试顺序.md 第四批). These tests require a running 3-node
// Docker Compose cluster and are protected by the "docker" build tag.
//
// Run:
//
//	docker compose -f integration/docker/docker-compose.yml up -d --wait
//	go test ./integration/docker/... -tags=docker -v -timeout 300s
//	docker compose -f integration/docker/docker-compose.yml down
//
//go:build docker
// +build docker

package docker_test

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
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

// nodeServices are the Compose service names, indexed the same as nodeAddrs.
var nodeServices = []string{
	"stratum-node1",
	"stratum-node2",
	"stratum-node3",
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

// === docker compose fault-injection helpers ===

// composeCmd runs a `docker compose` command from this package's directory
// (which holds docker-compose.yml). It fails the test on error.
func composeCmd(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// killNode SIGKILLs a node, simulating a crash (not a graceful stop).
func killNode(t *testing.T, service string) {
	t.Helper()
	t.Logf("killing %s", service)
	composeCmd(t, "kill", "-s", "SIGKILL", service)
}

// startNode restarts a previously killed node.
func startNode(t *testing.T, service string) {
	t.Helper()
	t.Logf("starting %s", service)
	composeCmd(t, "start", service)
}

// === leader discovery / data-visibility helpers ===

// newKBRequest builds a CreateKnowledgeBase request with a globally-unique
// name so re-running tests (against a stateful cluster) never collides.
func newKBRequest(label string) *pb.CreateKnowledgeBaseRequest {
	return &pb.CreateKnowledgeBaseRequest{
		Name:             fmt.Sprintf("%s-%d", label, time.Now().UnixNano()),
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "http://mock-embed:8080",
			ModelId:     "test-model",
		},
	}
}

// probeLeaderOnce tries to create a KB on each node once; Raft forwards no
// writes, so only the leader accepts. Returns the leader index and KB id.
func probeLeaderOnce(ctx context.Context, label string) (int, string, bool) {
	req := newKBRequest(label)
	for i, addr := range nodeAddrs {
		kb, _, _, conn, err := dialNode(addr)
		if err != nil {
			continue
		}
		resp, err := kb.CreateKnowledgeBase(ctx, req)
		conn.Close()
		if err == nil {
			return i, resp.KnowledgeBaseId, true
		}
	}
	return 0, "", false
}

// waitForLeader polls probeLeaderOnce until a leader accepts a write or the
// deadline passes. Used after killing a leader, when re-election takes a few
// seconds (election timeout is 2-4s).
func waitForLeader(t *testing.T, ctx context.Context, label string, timeout time.Duration) (leaderIdx int, kbID string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if idx, kb, ok := probeLeaderOnce(ctx, label); ok {
			return idx, kb
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for a leader to accept writes")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// nodeSeesKB reports whether a node has replicated the given KB's version
// metadata (i.e. it has caught up with the leader's Raft log).
func nodeSeesKB(ctx context.Context, addr, kbID string) bool {
	_, _, _, conn, err := dialNode(addr)
	if err != nil {
		return false
	}
	defer conn.Close()
	kb := pb.NewKnowledgeBaseServiceClient(conn)
	versions, err := kb.ListVersions(ctx, &pb.ListVersionsRequest{KnowledgeBaseId: kbID})
	if err != nil {
		return false
	}
	return len(versions.Versions) >= 1
}

// waitForNodeToSeeKB polls a node until it can ListVersions for kbID.
func waitForNodeToSeeKB(t *testing.T, ctx context.Context, addr, kbID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !nodeSeesKB(ctx, addr, kbID) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s to see KB %s", addr, kbID)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// === T4-1: Distributed correctness ===

func TestT4_MultiNode_Consistency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "consistency", 30*time.Second)
	leaderAddr := nodeAddrs[leaderIdx]
	t.Logf("leader is node %d (%s), KB %s", leaderIdx, leaderAddr, kbID)

	// Look up the initial version id on the leader so we can query it.
	_, _, _, conn, err := dialNode(leaderAddr)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	defer conn.Close()
	kb := pb.NewKnowledgeBaseServiceClient(conn)
	versions, err := kb.ListVersions(ctx, &pb.ListVersionsRequest{KnowledgeBaseId: kbID})
	if err != nil {
		t.Fatalf("ListVersions on leader: %v", err)
	}
	if len(versions.Versions) < 1 {
		t.Fatal("leader should see the version it created")
	}
	initialVersionID := versions.Versions[0].VersionId

	time.Sleep(2 * time.Second) // wait for Raft replication

	// Verify the same KB is visible from every node (replicated metadata).
	for i, addr := range nodeAddrs {
		if !nodeSeesKB(ctx, addr, kbID) {
			t.Errorf("node %d (%s) should see the version created on the leader", i, addr)
		}
	}

	// Query the (empty) initial version on the leader — must succeed and
	// return an empty result set.
	_, q, _, qconn, err := dialNode(leaderAddr)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	defer qconn.Close()
	queryResp, err := q.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &initialVersionID,
		Vector:          make([]float32, 768),
		TopK:            5,
	})
	if err != nil {
		t.Fatalf("Query on leader failed: %v", err)
	}
	if len(queryResp.Results) != 0 {
		t.Errorf("empty initial version: expected 0 results, got %d", len(queryResp.Results))
	}
}

// === T4-2: Fault tolerance ===

// TestT4_MinorityFaultTolerance kills one follower: with 2 of 3 nodes up the
// cluster still has a quorum and must keep accepting writes.
func TestT4_MinorityFaultTolerance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "minority", 30*time.Second)
	t.Logf("leader is node %d (%s)", leaderIdx, nodeAddrs[leaderIdx])

	// Kill a follower (any node other than the leader).
	followerIdx := (leaderIdx + 1) % 3
	followerSvc := nodeServices[followerIdx]
	killNode(t, followerSvc)
	defer func() {
		startNode(t, followerSvc)
		waitForLeader(t, context.Background(), "minority-restore", 30*time.Second)
	}()

	// The remaining 2 nodes still form a quorum: a new write must succeed.
	newLeaderIdx, newKBID := waitForLeader(t, ctx, "minority-write", 20*time.Second)
	t.Logf("post-kill write succeeded on node %d, KB %s", newLeaderIdx, newKBID)

	// The pre-kill KB must still be readable (no data loss).
	if !nodeSeesKB(ctx, nodeAddrs[newLeaderIdx], kbID) {
		t.Errorf("leader %d lost pre-fault KB %s", newLeaderIdx, kbID)
	}
}

// TestT4_LeaderFailover kills the leader, waits for a new leader to be
// elected, and verifies previously-committed data survives.
func TestT4_LeaderFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "failover", 30*time.Second)
	leaderSvc := nodeServices[leaderIdx]
	t.Logf("leader is node %d (%s), KB %s", leaderIdx, leaderSvc, kbID)

	// Give replication a moment to land on followers before the leader dies.
	time.Sleep(2 * time.Second)

	killNode(t, leaderSvc)
	defer func() {
		startNode(t, leaderSvc)
		waitForLeader(t, context.Background(), "failover-restore", 30*time.Second)
	}()

	// A new leader must be elected from the surviving majority.
	newLeaderIdx, _ := waitForLeader(t, ctx, "failover-new", 40*time.Second)
	if newLeaderIdx == leaderIdx {
		t.Fatalf("expected a different leader after killing node %d", leaderIdx)
	}
	t.Logf("new leader is node %d (%s)", newLeaderIdx, nodeAddrs[newLeaderIdx])

	// The pre-failover KB must survive on the new leader.
	waitForNodeToSeeKB(t, ctx, nodeAddrs[newLeaderIdx], kbID, 20*time.Second)
}

// TestT4_NodeRestartRecovery kills a node, restarts it, and verifies it
// catches up with the leader's log (data re-sync).
func TestT4_NodeRestartRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "restart", 30*time.Second)
	t.Logf("leader is node %d (%s)", leaderIdx, nodeAddrs[leaderIdx])

	// Kill and restart a follower (not the leader).
	victimIdx := (leaderIdx + 1) % 3
	victimSvc := nodeServices[victimIdx]
	victimAddr := nodeAddrs[victimIdx]

	killNode(t, victimSvc)
	startNode(t, victimSvc)
	defer func() {
		waitForLeader(t, context.Background(), "restart-restore", 30*time.Second)
	}()

	// The restarted node must catch up with the committed KB.
	waitForNodeToSeeKB(t, ctx, victimAddr, kbID, 40*time.Second)
	t.Logf("node %d (%s) caught up with KB %s", victimIdx, victimAddr, kbID)
}

// === T4-3: Cluster performance ===

func TestT4_PerformanceBaseline(t *testing.T) {
	t.Skip("performance benchmarks — run with dedicated benchmarking harness")
}

// === T4-4: Storage efficiency ===

func TestT4_StorageEfficiency(t *testing.T) {
	t.Skip("requires populating 100万 chunks — long-running storage benchmark")
}
