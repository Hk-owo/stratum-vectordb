package integration_test

import (
	"context"
	"path/filepath"
	"testing"

	pb "stratum/api/proto/stratum"
	"stratum/internal/raft"
)

// TestRealStack_ThreeNodeCluster_FaultTolerance is the in-process
// equivalent of Stratum_测试顺序.md 第四批 (T4-1 / T4-2): three fully-real
// nodes (real Pebble stores + FileWAL + real vecstore subprocess +
// real IndexManager + real Raft + real coordinators/services + digest-
// verified DataSync pulls) forming one Raft cluster. It exercises:
//
//   - T4-1 多副本一致性: write on the leader, read from every node;
//   - T4-2 少数派故障: one follower stopped, writes/queries continue;
//   - T4-2 leader 切换不丢数据: leader stopped, a new leader is elected
//     among the survivors, historical data is still queryable and new
//     writes succeed;
//   - T4-2 节点重启恢复: a stopped follower restarts from the same on-disk
//     state (baseDir + addresses) and catches up on both raft log and
//     storage-layer data.
//
// Docker-based variants (docker kill/iptables network partitions, cluster
// performance/storage benchmarks) remain in integration/docker and require
// a docker daemon, which is unavailable in this environment.
func TestRealStack_ThreeNodeCluster_FaultTolerance(t *testing.T) {
	// One vecstore subprocess per node (independent RocksDB), one shared
	// mock embed server.
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

	ctx := context.Background()

	// === T4-1: multi-replica consistency ===
	leader := waitForLeader(t, nodes[0], nodes[1], nodes[2])
	t.Logf("initial leader = node %d", leader.nodeID)

	kbID, v1 := leader.createTestKB(ctx, "e2e-three-node")
	v2Resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "alpha"},
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-2", Content: "beta"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion v2: %v", err)
	}
	v2 := v2Resp.VersionId

	// Every node converges to READY and can answer the same query with the
	// same document — multi-replica consistency.
	for _, n := range nodes {
		n.waitVersionReady(ctx, kbID, v2)
	}
	for _, n := range nodes {
		if r := waitQueryDoc(t, n, kbID, v2, contentVector("alpha", 4), "doc-1"); r.Content != "alpha" {
			t.Errorf("node %d: doc-1 content = %q, want alpha", n.nodeID, r.Content)
		}
		if r := waitQueryDoc(t, n, kbID, v2, contentVector("beta", 4), "doc-2"); r.Content != "beta" {
			t.Errorf("node %d: doc-2 content = %q, want beta", n.nodeID, r.Content)
		}
	}

	// === T4-2: minority fault tolerance — stop one follower ===
	var stopped *realNode
	for _, n := range nodes {
		if n.nodeID != leader.nodeID {
			stopped = n
			break
		}
	}
	t.Logf("stopping follower node %d (minority fault)", stopped.nodeID)
	stopped.Stop()

	// The majority (leader + one follower) keeps serving: v3 write + query.
	v3Resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v2,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-3", Content: "gamma"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion v3 with one follower down: %v", err)
	}
	v3 := v3Resp.VersionId
	leader.waitVersionReady(ctx, kbID, v3)
	for _, n := range nodes {
		if n != stopped {
			n.waitVersionReady(ctx, kbID, v3)
		}
	}
	survivor1 := nodes[0]
	if survivor1 == leader || survivor1 == stopped {
		survivor1 = nodes[1]
	}
	if r := waitQueryDoc(t, survivor1, kbID, v3, contentVector("gamma", 4), "doc-3"); r == nil || r.Content != "gamma" {
		t.Errorf("survivor query after minority fault = %+v", r)
	}

	// === T4-2: node restart recovery — bring the stopped follower back ===
	idx := int(stopped.nodeID - 1)
	t.Logf("restarting node %d from its persisted state", stopped.nodeID)
	restarted := newRealNodeWithAddrsAndDir(t, stopped.nodeID, peers, vecAddrs[idx], embedURL,
		raftAddrs[idx], grpcAddrs[idx], baseDirs[idx])
	wireSyncPull(t, restarted, addrByID)
	nodes[idx] = restarted // bookkeeping: this slot now holds the restarted node

	// The restarted node catches up on the raft log (v3 READY) and on the
	// storage-layer data (digest-verified pulls), then answers queries for
	// both the new and the historical versions.
	restarted.waitVersionReady(ctx, kbID, v3)
	if r := waitQueryDoc(t, restarted, kbID, v3, contentVector("gamma", 4), "doc-3"); r == nil || r.Content != "gamma" {
		t.Errorf("restarted node v3 data = %+v", r)
	}
	if r := waitQueryDoc(t, restarted, kbID, v2, contentVector("alpha", 4), "doc-1"); r == nil || r.Content != "alpha" {
		t.Errorf("restarted node lost historical v2 data: %+v", r)
	}

	// === T4-2: leader failover — stop the leader, survivors re-elect ===
	oldLeader := leader
	t.Logf("stopping leader node %d (failover)", oldLeader.nodeID)
	oldLeader.Stop()

	var rest []*realNode
	for _, n := range nodes {
		if n != oldLeader {
			rest = append(rest, n)
		}
	}
	if len(rest) != 2 {
		t.Fatalf("expected 2 survivors after leader stop, got %d", len(rest))
	}
	newLeader := waitForLeader(t, rest...)
	t.Logf("new leader = node %d", newLeader.nodeID)

	// Historical data survives the leader change.
	if r := waitQueryDoc(t, newLeader, kbID, v2, contentVector("alpha", 4), "doc-1"); r == nil || r.Content != "alpha" {
		t.Errorf("new leader lost v2 data: %+v", r)
	}
	if r := waitQueryDoc(t, newLeader, kbID, v3, contentVector("gamma", 4), "doc-3"); r == nil || r.Content != "gamma" {
		t.Errorf("new leader lost v3 data: %+v", r)
	}

	// New writes on the new leader succeed (2/2 majority).
	v4Resp, err := newLeader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v3,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-4", Content: "delta"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion v4 on new leader: %v", err)
	}
	v4 := v4Resp.VersionId
	newLeader.waitVersionReady(ctx, kbID, v4)
	for _, n := range rest {
		n.waitVersionReady(ctx, kbID, v4)
	}

	// === T4-2: restart the old leader — it must catch up on v4 ===
	oldIdx := int(oldLeader.nodeID - 1)
	oldRestarted := newRealNodeWithAddrsAndDir(t, oldLeader.nodeID, peers, vecAddrs[oldIdx], embedURL,
		raftAddrs[oldIdx], grpcAddrs[oldIdx], baseDirs[oldIdx])
	wireSyncPull(t, oldRestarted, addrByID)

	oldRestarted.waitVersionReady(ctx, kbID, v4)
	if r := waitQueryDoc(t, oldRestarted, kbID, v4, contentVector("delta", 4), "doc-4"); r == nil || r.Content != "delta" {
		t.Errorf("old leader after restart: v4 data = %+v", r)
	}
	if r := waitQueryDoc(t, oldRestarted, kbID, v2, contentVector("alpha", 4), "doc-1"); r == nil || r.Content != "alpha" {
		t.Errorf("old leader after restart lost historical v2 data: %+v", r)
	}

	// All three replicas are consistent again.
	for _, n := range []*realNode{newLeader, oldRestarted, restarted} {
		if r := waitQueryDoc(t, n, kbID, v4, contentVector("delta", 4), "doc-4"); r == nil || r.Content != "delta" {
			t.Errorf("node %d did not converge to v4", n.nodeID)
		}
	}
}
