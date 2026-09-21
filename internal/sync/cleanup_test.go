package sync

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

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

// A node with no dropper wired cannot reclaim anything, and answering "success"
// there is exactly how leftover bytes lose the last metadata row that could have
// named them: the broadcaster would read success, the control layer would go on to
// remove the version, and this node would keep the data with nothing left to
// notice it (docs/known-gaps.md §B/§C). §10.6 states the principle for the local
// path — 一个悄悄什么都不做的清理，正是孤儿数据活下来的方式 — and this is its
// receiving end.
//
// Not to be confused with a node that never received the version: that one stays an
// ordinary success, because the reclaim is an idempotent prefix delete.
func TestPushHandler_DeleteVersionDataWithoutDropperFailsLoud(t *testing.T) {
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

	_, err = pb.NewDataSyncServiceClient(conn).DeleteVersionData(ctx, &pb.DeleteVersionDataRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       7,
	})
	if err == nil {
		t.Fatal("a node that cannot reclaim must report that, not answer success")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", got)
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

// fakeCleanupServer answers DeleteVersionData with a chosen dropped value, so the
// caller's handling of it can be exercised without a peer that actually misbehaves.
type fakeCleanupServer struct {
	pb.UnimplementedDataSyncServiceServer
	dropped bool
	calls   int
}

func (f *fakeCleanupServer) DeleteVersionData(context.Context, *pb.DeleteVersionDataRequest) (*pb.DeleteVersionDataResponse, error) {
	f.calls++
	return &pb.DeleteVersionDataResponse{Dropped: f.dropped, NodeId: 9}, nil
}

func startFakeCleanupServer(t *testing.T, srv pb.DataSyncServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	g := grpc.NewServer()
	pb.RegisterDataSyncServiceServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return lis.Addr().String()
}

// The broadcaster must not read "the reclaim did not run" as "cleaned up". A peer
// that holds nothing reports true (the delete is an idempotent prefix delete), so
// dropped=false is a peer that could not do the work — and the delete flow has to
// hear that BEFORE it removes the version's metadata, because afterwards nothing
// can name this node's leftovers (docs/known-gaps.md §B/§C). Older receiving builds
// answered exactly this way when they had no dropper wired.
func TestVersionDataCleaner_TreatsDroppedFalseAsAFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range []struct {
		name    string
		dropped bool
		wantErr bool
	}{
		{name: "reclaim ran", dropped: true},
		{name: "reclaim did not run", dropped: false, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fakeCleanupServer{dropped: tc.dropped}
			addr := startFakeCleanupServer(t, srv)

			err := NewVersionDataCleaner(PresenceCheckerConfig{}).DeleteVersionData(ctx, addr, "kb-1", 7, "test reclaim")
			if tc.wantErr && err == nil {
				t.Fatal("dropped=false must be an error, not a completed reclaim")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("DeleteVersionData: %v", err)
			}
			if srv.calls != 1 {
				t.Errorf("peer calls = %d, want 1", srv.calls)
			}
		})
	}
}
