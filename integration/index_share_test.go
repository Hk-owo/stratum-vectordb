package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"stratum/internal/raft"

	pb "stratum/api/proto/stratum"
)

// indexFileOf reads a node's persisted index for (kbID, versionID) and returns
// nil when it is not there yet. baseDir is the node's data root; the index
// manager writes under <baseDir>/indexdata/index/<kbID>/<versionID>.index.
func indexFileOf(baseDir, kbID string, versionID int64) ([]byte, []byte) {
	path := filepath.Join(baseDir, "indexdata", "index", kbID, fmt.Sprintf("%d.index", versionID))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil
	}
	sidecar, err := os.ReadFile(path + ".ids")
	if err != nil {
		return data, nil
	}
	return data, sidecar
}

// waitForIndex polls until the node has a persisted index for the version, or
// the deadline passes.
func waitForIndex(t *testing.T, baseDir, kbID string, versionID int64, within time.Duration) ([]byte, []byte) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if data, sidecar := indexFileOf(baseDir, kbID, versionID); len(data) > 0 && len(sidecar) > 0 {
			return data, sidecar
		}
		if time.Now().After(deadline) {
			return nil, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestRealStack_IndexDistributionInstallsTheBuildersArtifact is the end-to-end
// proof of Stratum_设计文档v13.md §8.4: one node builds the index and the replica
// receives that same artifact instead of building its own.
//
// The assertion is byte-for-byte equality of the two index files. That is what
// distinguishes distribution from independent building: HNSW construction is
// randomized, so two nodes building the same version would not produce
// identical graph files. Equality can only come from one of them having been
// shipped the other's output.
func TestRealStack_IndexDistributionInstallsTheBuildersArtifact(t *testing.T) {
	vecAddrs := [2]string{
		startVecstoreServerForTest(t),
		startVecstoreServerForTest(t),
	}
	embedURL := startMockEmbedForTest(t, 4)

	var raftAddrs, grpcAddrs [2]string
	for i := range raftAddrs {
		raftAddrs[i] = freeLoopbackAddr(t)
		grpcAddrs[i] = freeLoopbackAddr(t)
	}
	peers := []raft.PeerConfig{
		{ID: 1, RaftAddr: raftAddrs[0], ServiceAddr: grpcAddrs[0]},
		{ID: 2, RaftAddr: raftAddrs[1], ServiceAddr: grpcAddrs[1]},
	}
	addrByID := map[int64]string{1: grpcAddrs[0], 2: grpcAddrs[1]}

	baseDirs := [2]string{t.TempDir(), t.TempDir()}
	var nodes [2]*realNode
	for i := 0; i < 2; i++ {
		nodes[i] = newRealNodeWithAddrsAndDir(t, int64(i+1), peers, vecAddrs[i], embedURL,
			raftAddrs[i], grpcAddrs[i], baseDirs[i])
		wireSyncPull(t, nodes[i], addrByID)
	}

	ctx := context.Background()
	leader := waitForLeader(t, nodes[0], nodes[1])
	t.Logf("leader = node %d", leader.nodeID)

	var follower *realNode
	var followerDir string
	for i, n := range nodes {
		if n.nodeID != leader.nodeID {
			follower, followerDir = n, baseDirs[i]
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}
	leaderDir := baseDirs[0]
	if leader.nodeID != 1 {
		leaderDir = baseDirs[1]
	}

	kbID, v1 := leader.createTestKB(ctx, "index-share-e2e")
	resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "alpha beta gamma"},
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-2", Content: "delta epsilon"},
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-3", Content: "zeta eta theta"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	versionID := resp.GetVersionId()
	t.Logf("created %s v%d", kbID, versionID)

	leaderIndex, leaderSidecar := waitForIndex(t, leaderDir, kbID, versionID, 60*time.Second)
	if leaderIndex == nil {
		t.Fatal("the leader never persisted an index for the version")
	}
	t.Logf("leader index: %d bytes + %d sidecar bytes", len(leaderIndex), len(leaderSidecar))

	followerIndex, followerSidecar := waitForIndex(t, followerDir, kbID, versionID, 60*time.Second)
	if followerIndex == nil {
		t.Fatal("the follower never got an index for the version (neither distributed nor built)")
	}

	if !bytes.Equal(followerIndex, leaderIndex) {
		t.Errorf("follower index is %d bytes and differs from the leader's %d bytes: it was not the builder's artifact",
			len(followerIndex), len(leaderIndex))
	}
	if !bytes.Equal(followerSidecar, leaderSidecar) {
		t.Error("follower sidecar differs from the leader's, so it did not install the shipped metadata")
	}
}
