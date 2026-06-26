package integration_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"

	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// TestMultiNode_RaftNodeImpl_3Node verifies that 3 RaftNodeImpl instances
// form a cluster, elect a leader, and replicate state machine commands.
func TestMultiNode_RaftNodeImpl_3Node(t *testing.T) {
	n := 3

	// Allocate free addresses for gRPC API and Raft RPC.
	raftAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		raftAddrs[i] = lis.Addr().String()
		lis.Close()
	}

	// Build peer list.
	peers := make([]raft.PeerConfig, n)
	for i := 0; i < n; i++ {
		peers[i] = raft.PeerConfig{
			ID:       int64(i + 1),
			RaftAddr: raftAddrs[i],
		}
	}

	// Phase 1: create transport + Raft instances for all nodes.
	// We need to do this in two phases because kvraft's transport
	// must be able to dial peers, and peers' gRPC servers must be
	// running before cross-connections are made.
	//
	// Strategy: create all nodes with StartGRPC first, then Run them
	// all at once so election timers start roughly simultaneously.

	type node struct {
		id   int64
		impl *raft.RaftNodeImpl
		wal  *wal.MockWAL
	}
	nodes := make([]*node, n)
	logger := zap.NewNop()

	for i := 0; i < n; i++ {
		id := int64(i + 1)
		w := wal.NewMockWAL()
		impl, err := raft.NewRaftNodeImpl(raft.Config{
			NodeID:   id,
			DataDir:  t.TempDir(),
			RaftAddr: raftAddrs[i],
			Peers:    peers,
			WAL:      w,
			Logger:   logger,
			HeartbeatInterval:  200 * time.Millisecond,
			ElectionTimeoutMin: 2000 * time.Millisecond,
			ElectionTimeoutMax: 4000 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("node %d: NewRaftNodeImpl: %v", id, err)
		}
		nodes[i] = &node{id: id, impl: impl, wal: w}
	}

	// Phase 2: wait for leader election.
	defer func() {
		for _, nd := range nodes {
			nd.impl.Stop()
		}
	}()

	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	var leader *node
	for time.Now().Before(deadline) {
		// Ask each node who it thinks the leader is.
		var leaderID int64
		for _, nd := range nodes {
			status, _ := nd.impl.GetClusterStatus(ctx)
			if status.HasLeader {
				leaderID = status.LeaderID
				break
			}
		}
		if leaderID == 0 {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		// Try to write on the node the cluster thinks is the leader.
		candidate := nodes[leaderID-1]
		err := candidate.impl.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
			KBID: fmt.Sprintf("kb-probe-%d", candidate.id), Name: "probe",
			ChunkWindowSize: 512, ChunkOverlapSize: 64,
			EmbedConfig: types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"},
		})
		if err == nil {
			leader = candidate
			break
		}
		t.Logf("probe node %d (cluster leader %d): %v", candidate.id, leaderID, err)
		time.Sleep(200 * time.Millisecond)
	}

	if leader == nil {
		t.Fatal("no leader elected within timeout")
	}

	t.Logf("leader: node %d", leader.id)

	// Phase 3: propose a KB on the leader, with retry for leader changes.
	kbID := "test-kb"
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		// Re-confirm leader before each attempt.
		status, _ := leader.impl.GetClusterStatus(ctx)
		if !status.HasLeader || status.LeaderID != leader.id {
			// Leader changed — find the new leader.
			for _, nd := range nodes {
				s, _ := nd.impl.GetClusterStatus(ctx)
				if s.HasLeader && s.LeaderID == nd.id {
					leader = nd
					break
				}
			}
		}
		err := leader.impl.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
			KBID: kbID, Name: "test",
			ChunkWindowSize: 512, ChunkOverlapSize: 64,
			EmbedConfig: types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"},
		})
		if err == nil {
			lastErr = nil
			break
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("ProposeCreateKB on leader after retries: %v", lastErr)
	}

	// Create a version.
	vID, err := leader.impl.ProposeCreateVersion(ctx, kbID, 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	_ = leader.impl.ProposeUpdateVersionStatus(ctx, vID, types.IndexStatusReady)

	// Wait for replication.
	time.Sleep(time.Second)

	// Verify from all nodes.
	for _, nd := range nodes {
		kb, err := nd.impl.GetKB(ctx, kbID)
		if err != nil {
			t.Errorf("node %d: GetKB failed: %v", nd.id, err)
			continue
		}
		if kb.Name != "test" {
			t.Errorf("node %d: expected name='test', got %q", nd.id, kb.Name)
		}

		versions, err := nd.impl.ListVersions(ctx, kbID)
		if err != nil {
			t.Errorf("node %d: ListVersions failed: %v", nd.id, err)
			continue
		}
		if len(versions) < 1 {
			t.Errorf("node %d: expected at least 1 version", nd.id)
		}
	}

	t.Logf("all %d nodes see KB %s with version %d", n, kbID, vID)
}
