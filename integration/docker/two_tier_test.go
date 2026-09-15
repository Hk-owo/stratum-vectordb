// Two-tier topology tests (Stratum_设计文档v13.md §11 阶段 ④「存储集群独立进程」).
//
// These exercise what the split is for, and what the all-in-one T4 suite in
// docker_test.go cannot see: the control tier commits a write and the storage
// tier is what actually holds and serves it. Both tiers run from
// scripts/docker-cluster-both.sh:
//
//	scripts/docker-cluster-both.sh up
//	STRATUM_T4_NODE_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
//	STRATUM_T4_NODE_SERVICES=stratum-node-control1,stratum-node-control2,stratum-node-control3 \
//	go test ./integration/docker/... -tags=docker -run TwoTier -v -timeout 600s
//
// The defaults below already match that layout, so a bare run works against it
// while the all-in-one defaults in docker_test.go keep CI unchanged.
//
//go:build docker
// +build docker

package docker_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
)

// storageAddrs are the storage tier's gRPC addresses.
var storageAddrs = splitEnv("STRATUM_T4_STORAGE_ADDRS", "localhost:17100,localhost:17101,localhost:17102")

// storageServices are the storage tier's container names, indexed like
// storageAddrs — what the fault-injection helpers kill and start.
var storageServices = splitEnv("STRATUM_T4_STORAGE_SERVICES", "stratum-node-storage1,stratum-node-storage2,stratum-node-storage3")

// writeDocumentThroughControl submits one ADD and returns the version it
// allocated, re-resolving the leader as needed.
//
// The control tier's leadership can move between deciding where to send a write
// and sending it — most obviously right after another test has killed and
// restarted a control node. Treating "not leader" as fatal would make this a
// test of scheduling rather than of correctness, and a real client does not
// behave that way: it re-resolves and retries. The idempotency key makes that
// safe — a repeat reaches the same version instead of allocating another.
func writeDocumentThroughControl(t *testing.T, ctx context.Context, kbID, docID, content string) int64 {
	t.Helper()

	req := &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ClientRequestId: fmt.Sprintf("two-tier-%d", time.Now().UnixNano()),
		Changes: []*pb.DocChange{{
			Op:      pb.ChangeOp_CHANGE_OP_ADD,
			DocId:   docID,
			Content: content,
		}},
	}

	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	// A write is only accepted by the control tier — a storage node has no log
	// to commit it to — and by exactly one node of it at a time.
	for time.Now().Before(deadline) {
		for _, addr := range nodeAddrs {
			_, _, _, conn, err := dialNode(addr)
			if err != nil {
				lastErr = err
				continue
			}
			resp, err := pb.NewKnowledgeBaseServiceClient(conn).CreateVersion(ctx, req)
			conn.Close()
			if err == nil {
				return resp.GetVersionId()
			}
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("CreateVersion for %s never reached a leader: %v", kbID, lastErr)
	return 0
}

// serveFrom asks one address to query a version, with no retry.
func serveFrom(ctx context.Context, addr, kbID string, versionID int64) (*pb.QueryResponse, error) {
	return serveVector(ctx, addr, kbID, versionID, queryVector(768), 5)
}

// queryVector returns the vector a test query sends.
//
// Deterministic, and deliberately NOT all-zero. An all-zero vector is
// equidistant from every document, so the HNSW greedy walk has nothing to prune
// with and every measurement is inflated: on the same version it cost ~3.4× the
// random-vector latency, and before the O(candidates × documents) fix it was the
// difference between 90 ms and 10 s at 8,000 documents. A real caller sends a
// vector that came out of the embedder; tests should not measure a different
// workload than the one that ships (v13 §5 #15).
//
// Non-negative components, like the embedder's: a real embedder output is
// non-negative here (embed.deterministicVector divides hash bytes by 255 before
// normalizing), so a query vector with negative components sits in a different
// orthant and can score BELOW the query path's threshold of 0. That is correct
// behaviour — an anti-correlated document is not a match — but it makes a
// one-document fixture flaky in the worst way: the only candidate is pruned and
// the query "succeeds" with zero results, which reads as a replication failure
// rather than an unlucky vector.
func queryVector(dim int) []float32 {
	v := make([]float32, dim)
	rng := rand.New(rand.NewSource(20240915))
	for i := range v {
		v[i] = rng.Float32()
	}
	return v
}

// serveVector asks addr to query a version with an explicit vector, with no
// retry.
func serveVector(ctx context.Context, addr, kbID string, versionID int64, vector []float32, topK int) (*pb.QueryResponse, error) {
	_, q, _, conn, err := dialNode(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	version := versionID
	return q.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &version,
		Vector:          vector,
		TopK:            int32(topK),
	})
}

// awaitServable polls addr until it serves versionID with at least one result.
//
// A written version is not immediately queryable: the data has to land, the
// index has to exist, and on a replica that received its index from the builder
// that install happens after the write returns. Polling is the honest way to
// wait for that — asserting on the first try would only be testing scheduling.
func awaitServable(t *testing.T, ctx context.Context, addr, kbID string, versionID int64, timeout time.Duration) *pb.QueryResponse {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := serveFrom(ctx, addr, kbID, versionID)
		if err == nil && len(resp.GetResults()) > 0 {
			return resp
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr != nil {
		t.Logf("%s never served %s v%d (last error: %v)", addr, kbID, versionID, lastErr)
	}
	return nil
}

// TestTwoTier_EveryStorageNodeServesTheWrittenVersion is the core claim of the
// split: a write accepted and committed by the control tier becomes readable
// from the storage tier, on every storage node — because §8.4 ships the index
// the builder produced to the replicas instead of making each of them build.
//
// It is deliberately run against every storage address rather than one: a
// topology that only ever queried the coordinator would pass even if nothing
// was ever replicated.
func TestTwoTier_EveryStorageNodeServesTheWrittenVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "two-tier", 30*time.Second)
	t.Logf("control leader is node %d (%s), KB %s", leaderIdx, nodeAddrs[leaderIdx], kbID)

	versionID := writeDocumentThroughControl(t, ctx, kbID, "d1",
		"两段式拓扑：控制组提交版本，存储组落数据与索引。")
	t.Logf("committed %s v%d through the control tier", kbID, versionID)

	for i, addr := range storageAddrs {
		resp := awaitServable(t, ctx, addr, kbID, versionID, 60*time.Second)
		if resp == nil {
			t.Errorf("storage node %d (%s) never served %s v%d", i, addr, kbID, versionID)
			continue
		}
		got := resp.GetResults()[0]
		if got.GetDocId() != "d1" {
			t.Errorf("storage node %d (%s): docId = %q, want d1", i, addr, got.GetDocId())
		}
		t.Logf("storage node %d (%s) served v%d: %s score=%.4f", i, addr, versionID, got.GetDocId(), got.GetScore())
	}
}

// TestTwoTier_StorageGroupToleratesOneNodeDown keeps the storage tier's own
// quorum honest: with one of three storage nodes gone, the remaining two still
// form a majority, so a write must still reach durable and still be readable.
//
// This is the storage tier's fault tolerance, which is not the same property as
// the control tier's — killing a storage node leaves the Raft cluster intact,
// and that is the point of the split: the two failure domains move
// independently.
func TestTwoTier_StorageGroupToleratesOneNodeDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	if len(storageServices) < 3 || len(storageAddrs) < 3 {
		t.Skipf("needs at least 3 storage nodes, have %d", len(storageServices))
	}

	leaderIdx, kbID := waitForLeader(t, ctx, "two-tier-fault", 30*time.Second)
	t.Logf("control leader is node %d (%s), KB %s", leaderIdx, nodeAddrs[leaderIdx], kbID)

	victim := storageServices[0]
	killNode(t, victim)
	defer func() {
		startNode(t, victim)
	}()

	versionID := writeDocumentThroughControl(t, ctx, kbID, "d2",
		"存储组掉一个节点后，剩余两副本仍需达成 quorum 并可读。")
	t.Logf("committed %s v%d with %s down", kbID, versionID, victim)

	// The two survivors must serve it. Querying the killed node is pointless —
	// it is down — so the assertion is about the majority that is left.
	served := 0
	for i, addr := range storageAddrs[1:] {
		if resp := awaitServable(t, ctx, addr, kbID, versionID, 90*time.Second); resp != nil {
			served++
			t.Logf("survivor %d (%s) served v%d", i+1, addr, versionID)
		} else {
			t.Errorf("survivor %d (%s) never served %s v%d", i+1, addr, kbID, versionID)
		}
	}
	if served == 0 {
		t.Fatal("no surviving storage node served the version written while one was down")
	}
}
