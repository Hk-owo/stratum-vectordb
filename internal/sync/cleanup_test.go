package sync

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
)

// stubSyncDropper records the local cleanups a push handler performs.
type stubSyncDropper struct {
	dropped []string
	err     error
}

func (d *stubSyncDropper) DropVersionStorage(_ context.Context, kbID string, versionID int64) error {
	if d.err != nil {
		return d.err
	}
	d.dropped = append(d.dropped, fmt.Sprintf("%s/%d", kbID, versionID))
	return nil
}

var _ VersionDataDropper = (*stubSyncDropper)(nil)

// DeleteVersionData is the receiving end of §10.6's cleanup broadcast: it must
// reclaim the version locally and say so.
func TestPushHandler_DeleteVersionDataReclaimsLocally(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dropper := &stubSyncDropper{}
	_, _, addr := startPushServer(t, 4, WithVersionDataDropper(dropper))
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := pb.NewDataSyncServiceClient(conn).DeleteVersionData(ctx, &pb.DeleteVersionDataRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       7,
		Reason:          "permanently failed version",
	})
	if err != nil {
		t.Fatalf("DeleteVersionData: %v", err)
	}
	if !resp.GetDropped() {
		t.Error("dropped = false, want true when the node reclaimed data")
	}
	if resp.GetNodeId() != 4 {
		t.Errorf("node_id = %d, want 4", resp.GetNodeId())
	}
	if len(dropper.dropped) != 1 || dropper.dropped[0] != "kb-1/7" {
		t.Errorf("local cleanups = %v, want kb-1/7", dropper.dropped)
	}
}

// A node that never received the version has nothing to drop; that is a normal
// outcome, not a failure the broadcaster should treat as an unreachable peer.
func TestPushHandler_DeleteVersionDataWithoutDropperIsNotAnError(t *testing.T) {
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

	resp, err := pb.NewDataSyncServiceClient(conn).DeleteVersionData(ctx, &pb.DeleteVersionDataRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       7,
	})
	if err != nil {
		t.Fatalf("DeleteVersionData: %v", err)
	}
	if resp.GetDropped() {
		t.Error("dropped = true, want false when nothing was wired")
	}
}

// A failing local drop is surfaced as an error so the broadcaster can log which
// replica could not be cleaned.
func TestPushHandler_DeleteVersionDataSurfacesDropFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, addr := startPushServer(t, 6, WithVersionDataDropper(&stubSyncDropper{err: errors.New("disk error")}))
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := pb.NewDataSyncServiceClient(conn).DeleteVersionData(ctx, &pb.DeleteVersionDataRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       7,
	}); err == nil {
		t.Fatal("a failed local drop must be reported to the broadcaster")
	}
}
