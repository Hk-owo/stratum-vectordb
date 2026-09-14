package sync

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
)

// TestPushHandler_VersionPresence pins the answer the control layer relies on
// to tell a version that is merely waiting for its index build apart from one
// whose data never landed anywhere (Stratum_设计文档v13.md §7.12 step ①).
func TestPushHandler_VersionPresence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, addr := startPushServer(t, 7)
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewDataSyncServiceClient(conn)

	// Nothing applied yet: the node does not hold the version.
	resp, err := client.VersionPresence(ctx, &pb.VersionPresenceRequest{KnowledgeBaseId: "kb-1", VersionId: 5})
	if err != nil {
		t.Fatalf("VersionPresence: %v", err)
	}
	if resp.GetPresent() {
		t.Error("a node with no data for the version must answer present=false")
	}
	if resp.GetNodeId() != 7 {
		t.Errorf("node_id = %d, want 7 (the answering replica)", resp.GetNodeId())
	}

	// After the version is pushed, the same query answers true.
	leader, _, _ := startLeaderServer(t, newFakeVecstore())
	if err := leader.versionDoc.Write(ctx, "kb-1", 5, "doc-1"); err != nil {
		t.Fatal(err)
	}
	pusher := NewPusher(PusherConfig{Exporter: leader.handler, NodeID: 1})
	if _, err := pusher.PushVersion(ctx, addr, "kb-1", 5); err != nil {
		t.Fatalf("PushVersion: %v", err)
	}

	resp, err = client.VersionPresence(ctx, &pb.VersionPresenceRequest{KnowledgeBaseId: "kb-1", VersionId: 5})
	if err != nil {
		t.Fatalf("VersionPresence after push: %v", err)
	}
	if !resp.GetPresent() {
		t.Error("after a push the node must answer present=true")
	}

	// Presence is per version, not per knowledge base.
	resp, err = client.VersionPresence(ctx, &pb.VersionPresenceRequest{KnowledgeBaseId: "kb-1", VersionId: 6})
	if err != nil {
		t.Fatalf("VersionPresence v6: %v", err)
	}
	if resp.GetPresent() {
		t.Error("a version that was never pushed must answer present=false")
	}
}
