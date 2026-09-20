package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
	"stratum/internal/raft"
)

// TestRealStack_ThreeNodeCluster_SnapshotPipeline exercises the complete
// Raft snapshot pipeline across a real 3-node cluster (real Pebble stores,
// real FileWAL, real kvraft consensus over gRPC, real vecstore subprocess):
//
//   - 快照生成: while one follower is offline, the leader's log grows past a
//     small MaxLogLength and kvraft triggers a local snapshot — the log is
//     trimmed and a durable snapshot file is persisted on the leader;
//   - 快照传输: the follower rejoins far behind the leader's log base, so
//     incremental AppendEntries cannot catch it up — the leader pushes the
//     snapshot over the real gRPC InstallSnapshot RPC (this is the
//     multi-node snapshot-transfer path that the single-node snapshot tests
//     in internal/raft only simulate);
//   - 快照安装 + 存储层补齐: the follower installs the snapshot (raft state
//     machine restored, snapshot persisted, log trimmed) and then pulls
//     storage-layer data (docstore/vecstore) and builds indexes for every
//     snapshot-covered version — the data-sync hook must fire for versions
//     restored from a snapshot, not just for incrementally replicated ones;
//   - 收敛与一致性: post-recovery writes replicate to all three nodes and
//     historical snapshot-covered versions stay queryable everywhere.
func TestRealStack_ThreeNodeCluster_SnapshotPipeline(t *testing.T) {
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

	// Small compaction threshold: each CreateVersion adds ~3 raft entries
	// (cmdCreateVersion + cmdUpdateVersionSummary + cmdUpdateVersionStatus),
	// so a handful of versions is enough to push the leader's log past
	// MaxLogLength and force a local snapshot while the follower is offline.
	const maxLogLength = 24
	var nodes [3]*realNode
	for i := 0; i < 3; i++ {
		nodes[i] = newRealNodeWithAddrsAndDirOpts(t, int64(i+1), peers, vecAddrs[i], embedURL,
			raftAddrs[i], grpcAddrs[i], baseDirs[i], realNodeConfig{maxLogLength: maxLogLength})
		wireSyncPull(t, nodes[i], addrByID)
	}

	ctx := context.Background()
	leader := waitForLeader(t, nodes[0], nodes[1], nodes[2])
	t.Logf("initial leader = node %d", leader.nodeID)

	kbID, v1 := leader.createTestKB(ctx, "snapshot-pipeline")

	// Take a non-leader follower offline (its baseDir is kept, so it can
	// restart from persisted state). Before it goes offline its log is far
	// below the compaction threshold, so it must NOT have a snapshot yet.
	var offline *realNode
	for _, n := range nodes {
		if n.nodeID != leader.nodeID {
			offline = n
			break
		}
	}
	offlineIdx := int(offline.nodeID - 1)
	offlineSnap := filepath.Join(baseDirs[offlineIdx], "raft", "raft.snapshot")
	if _, err := os.Stat(offlineSnap); err == nil {
		t.Fatalf("node %d already has a snapshot before going offline", offline.nodeID)
	}
	offline.Stop()
	t.Logf("follower node %d offline; leader keeps writing", offline.nodeID)

	// Chain versions (each must reach READY before the next — a PENDING
	// parent is rejected) until the leader's local compaction persists a
	// snapshot file. The leader's log base has now moved past the offline
	// follower's last known index, so the follower can only catch up via
	// InstallSnapshot.
	leaderSnap := filepath.Join(leader.baseDir, "raft", "raft.snapshot")
	versions := []int64{v1}
	prev := v1
	for i := 2; ; i++ {
		resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
			KnowledgeBaseId: kbID,
			ParentVersionId: prev,
			Changes: []*pb.DocChange{
				{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: fmt.Sprintf("doc-%d", i), Content: fmt.Sprintf("content-%d", i)},
			},
		})
		if err != nil {
			t.Fatalf("CreateVersion v%d: %v", i, err)
		}
		v := resp.VersionId
		leader.waitVersionReady(ctx, kbID, v)
		versions = append(versions, v)
		prev = v

		// The snapshot write is asynchronous (apply loop); poll briefly.
		snapSeen := false
		for wait := 0; wait < 50; wait++ {
			if _, err := os.Stat(leaderSnap); err == nil {
				snapSeen = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if snapSeen {
			t.Logf("leader snapshot persisted after version %d (log compacted)", v)
			break
		}
		if i > 200 {
			t.Fatalf("leader never compacted its log after %d versions", i)
		}
	}
	if len(versions) < 3 {
		t.Fatalf("expected the snapshot to cover at least 3 versions, got %d", len(versions))
	}

	// Sanity: both in-cluster nodes see the full version list.
	for _, n := range nodes {
		if n == offline {
			continue
		}
		vs, err := n.raftNode.ListVersions(ctx, kbID)
		if err != nil || len(vs) != len(versions) {
			t.Fatalf("node %d: versions = %d, want %d (err=%v)", n.nodeID, len(vs), len(versions), err)
		}
	}

	// Restart the offline follower from the same baseDir/addresses. Its log
	// is far behind the leader's (now compacted) log, so the leader must
	// push its snapshot via InstallSnapshot; the follower installs it (raft
	// state machine restored) and then pulls storage-layer data for every
	// snapshot-covered version.
	t.Logf("restarting offline follower node %d from persisted state", offline.nodeID)
	restarted := newRealNodeWithAddrsAndDirOpts(t, offline.nodeID, peers, vecAddrs[offlineIdx], embedURL,
		raftAddrs[offlineIdx], grpcAddrs[offlineIdx], baseDirs[offlineIdx], realNodeConfig{maxLogLength: maxLogLength})
	wireSyncPull(t, restarted, addrByID)
	nodes[offlineIdx] = restarted // bookkeeping: this slot now holds the restarted node

	// The follower must come up through the InstallSnapshot path: the
	// snapshot file can only be written by installing a leader-pushed
	// snapshot (its own log never grew while it was offline).
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(offlineSnap); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d never installed the leader's snapshot (raft.snapshot missing)", offline.nodeID)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("node %d installed the leader's snapshot", offline.nodeID)

	// Raft metadata: every version must be visible (restored from the
	// snapshot plus any incremental catch-up).
	deadline = time.Now().Add(15 * time.Second)
	for {
		vs, err := restarted.raftNode.ListVersions(ctx, kbID)
		if err == nil && len(vs) == len(versions) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d: versions after snapshot install = %+v (err=%v), want %d",
				restarted.nodeID, vs, err, len(versions))
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Storage-layer convergence: snapshot-covered versions must be fully
	// queryable on the follower (data pulled from the leader + index
	// built). Spot-check an early version (deep inside the snapshot) and
	// the latest version.
	for _, idx := range []int{1, len(versions) - 1} {
		vID := versions[idx]
		doc := fmt.Sprintf("doc-%d", idx+1)
		content := fmt.Sprintf("content-%d", idx+1)
		restarted.waitVersionReady(ctx, kbID, vID)
		if r := waitQueryDoc(t, restarted, kbID, vID, contentVector(content, 4), doc); r == nil || r.Content != content {
			t.Fatalf("node %d: snapshot-covered v%d doc %q = %+v", restarted.nodeID, vID, doc, r)
		}
	}

	// New writes after recovery replicate to all three nodes.
	//
	// The leader is RE-RESOLVED here rather than reusing the handle from the start of
	// the test. Node 2 restarted, and a restart can move leadership — under load it
	// regularly does — so the node that led back then may answer "not leader" now.
	// That answer is retryable by contract (internal/router's isRetryableErr matches
	// Internal + "not leader", and internal/kvraft produces exactly that for a request
	// that reaches a non-leader), so the fixture retries it instead of failing a
	// cluster that is merely mid-election.
	var vNext int64
	for attempt := 0; ; attempt++ {
		cur := waitForLeader(t, nodes[0], nodes[1], nodes[2])
		resp, err := cur.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
			KnowledgeBaseId: kbID,
			ParentVersionId: prev,
			Changes: []*pb.DocChange{
				{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-after-recovery", Content: "recovered"},
			},
		})
		if err == nil {
			vNext = resp.VersionId
			break
		}
		if attempt >= 10 || !strings.Contains(err.Error(), "not leader") {
			t.Fatalf("CreateVersion after recovery: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, n := range nodes {
		n.waitVersionReady(ctx, kbID, vNext)
	}
	for _, n := range nodes {
		if r := waitQueryDoc(t, n, kbID, vNext, contentVector("recovered", 4), "doc-after-recovery"); r == nil || r.Content != "recovered" {
			t.Fatalf("node %d: post-recovery v%d query = %+v", n.nodeID, vNext, r)
		}
	}
	// Historical snapshot-covered data still queryable on every node.
	for _, n := range nodes {
		if r := waitQueryDoc(t, n, kbID, versions[1], contentVector("content-2", 4), "doc-2"); r == nil || r.Content != "content-2" {
			t.Fatalf("node %d: historical v%d query = %+v", n.nodeID, versions[1], r)
		}
	}

	t.Logf("snapshot pipeline OK: %d versions, leader node %d, recovered node %d",
		len(versions), leader.nodeID, restarted.nodeID)
}
