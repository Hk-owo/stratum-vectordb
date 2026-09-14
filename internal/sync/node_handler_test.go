package sync

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
)

// stubCursor is a LocalVersionReporter with a fixed answer.
type stubCursor struct{ version int64 }

func (s stubCursor) LocalVersionOf(string) int64 { return s.version }

var _ LocalVersionReporter = stubCursor{}

// TestPushHandler_LocalVersionReportsCursor pins the query a lagging node uses
// to pick a backfill source (Stratum_设计文档v13.md §7.6).
func TestPushHandler_LocalVersionReportsCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, addr := startPushServer(t, 4, WithLocalVersion(stubCursor{version: 11}))
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := pb.NewDataSyncServiceClient(conn).LocalVersion(ctx, &pb.LocalVersionRequest{
		KnowledgeBaseId: "kb-1",
	})
	if err != nil {
		t.Fatalf("LocalVersion: %v", err)
	}
	if resp.GetVersion() != 11 {
		t.Errorf("version = %d, want the reporter's cursor (11)", resp.GetVersion())
	}
	if resp.GetNodeId() != 4 {
		t.Errorf("node_id = %d, want 4", resp.GetNodeId())
	}
}

// Without a wired cursor the node answers 0 ("nothing known") rather than
// failing: it is then a useless backfill source, not a wrong one.
func TestPushHandler_LocalVersionWithoutCursorAnswersZero(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, addr := startPushServer(t, 5)
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := pb.NewDataSyncServiceClient(conn).LocalVersion(ctx, &pb.LocalVersionRequest{
		KnowledgeBaseId: "kb-1",
	})
	if err != nil {
		t.Fatalf("LocalVersion: %v", err)
	}
	if resp.GetVersion() != 0 {
		t.Errorf("version = %d, want 0 (nothing known) without a cursor", resp.GetVersion())
	}
}

// TestNodeHandler_ServesEveryDirection pins that the single service a node
// registers covers both directions plus the two queries — the shape the node
// assembly registers.
func TestNodeHandler_ServesEveryDirection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Exporter side: real stores + fake vecstore with one version's records.
	leader, _, _ := startLeaderServer(t, newFakeVecstore())
	kbID, versionID := "kb-node", int64(2)
	if err := leader.docStore.Write(ctx, kbID, "doc-1", versionID, []byte("content")); err != nil {
		t.Fatal(err)
	}
	if err := leader.versionDoc.Write(ctx, kbID, versionID, "doc-1"); err != nil {
		t.Fatal(err)
	}

	// Receiver side over its own stores, with a wired cursor.
	follower, _, _ := startPushServer(t, 9)
	cursor := stubCursor{version: versionID}
	node := NewNodeHandler(leader.handler, NewPushHandler(follower, 9, WithLocalVersion(cursor)))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterDataSyncServiceServer(srv, node)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.DialContext(ctx, lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewDataSyncServiceClient(conn)

	// Pull still works through the combined handler.
	stream, err := client.PullVersionData(ctx, &pb.PullVersionDataRequest{KnowledgeBaseId: kbID, VersionId: versionID})
	if err != nil {
		t.Fatalf("PullVersionData: %v", err)
	}
	entries := 0
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
		entries++
	}
	if entries == 0 {
		t.Error("the combined handler exported no records")
	}

	// Both queries answer through the receiver.
	if _, err := client.VersionPresence(ctx, &pb.VersionPresenceRequest{KnowledgeBaseId: kbID, VersionId: versionID}); err != nil {
		t.Fatalf("VersionPresence: %v", err)
	}
	cursorResp, err := client.LocalVersion(ctx, &pb.LocalVersionRequest{KnowledgeBaseId: kbID})
	if err != nil {
		t.Fatalf("LocalVersion: %v", err)
	}
	if cursorResp.GetVersion() != versionID {
		t.Errorf("cursor = %d, want %d", cursorResp.GetVersion(), versionID)
	}
}
