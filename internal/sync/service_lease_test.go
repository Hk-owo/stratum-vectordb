package sync

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	pb "stratum/api/proto/stratum"
	"stratum/internal/storage"
)

type leaseGrantServer struct {
	pb.UnimplementedInternalServiceServer
	grant func(*pb.RenewStorageServiceRequest) *pb.RenewStorageServiceResponse
}

func (s leaseGrantServer) RenewStorageService(_ context.Context, req *pb.RenewStorageServiceRequest) (*pb.RenewStorageServiceResponse, error) {
	return s.grant(req), nil
}
func leaseRenewerTest(t *testing.T, grant func(*pb.RenewStorageServiceRequest) *pb.RenewStorageServiceResponse) (*ServiceRenewer, storage.Directory) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterInternalServiceServer(srv, leaseGrantServer{grant: grant})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	gate := storage.NewServiceLease(11, "s11")
	t.Cleanup(gate.Pause)
	r := &ServiceRenewer{NodeID: 11, Address: "s11", Gate: gate, ResolveLeader: func(context.Context) (string, bool, error) { return "control", true, nil }, Dial: func(ctx context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///control", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	}}
	t.Cleanup(r.close)
	d := storage.NewDirectory()
	if err := d.RegisterNodes(0, []storage.Node{{ID: 11, Addr: "s11"}}); err != nil {
		t.Fatal(err)
	}
	return r, d
}
func TestServiceRenewerReconcilesBeforeStartupAndAfterDirectoryChange(t *testing.T) {
	var revision atomic.Uint64
	revision.Store(1)
	r, d := leaseRenewerTest(t, func(req *pb.RenewStorageServiceRequest) *pb.RenewStorageServiceResponse {
		return &pb.RenewStorageServiceResponse{DurationMs: storage.ServiceLeaseDuration.Milliseconds(), DirectoryRevision: revision.Load(), RequestId: req.RequestId}
	})
	var reads int
	fail := false
	r.Directory = func(context.Context) (storage.Directory, error) {
		reads++
		if fail {
			return storage.Directory{}, errors.New("directory unavailable")
		}
		return d.Clone(), nil
	}
	if err := r.ReportOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	old, release, err := r.Gate.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := r.ReportOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if reads != 1 {
		t.Fatal("unchanged heartbeat scanned directory", reads)
	}
	revision.Store(2)
	d.Revision = 2
	fail = true
	if err := r.ReportOnce(context.Background()); err == nil {
		t.Fatal("reconcile error ignored")
	}
	if err := r.Gate.Check(); err == nil {
		t.Fatal("opened before reconcile")
	}
	fail = false
	if err := r.ReportOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Gate.CheckContext(old); err == nil {
		t.Fatal("old operation revived after reconnect")
	}
	if reads != 3 {
		t.Fatal(reads)
	}
}
func TestServiceRenewerRejectsDelayedGrantAndWrongRequest(t *testing.T) {
	for _, mode := range []string{"delayed", "wrong-request", "excess-duration"} {
		t.Run(mode, func(t *testing.T) {
			var r *ServiceRenewer
			r, d := leaseRenewerTest(t, func(req *pb.RenewStorageServiceRequest) *pb.RenewStorageServiceResponse {
				response := &pb.RenewStorageServiceResponse{DurationMs: storage.ServiceLeaseDuration.Milliseconds(), DirectoryRevision: 1, RequestId: req.RequestId}
				switch mode {
				case "delayed":
					r.Gate.Pause()
					time.Sleep(5 * time.Millisecond)
				case "wrong-request":
					response.RequestId = "stale"
				case "excess-duration":
					response.DurationMs++
				}
				return response
			})
			// Delay before the RPC is also charged to the grant. A very old start
			// cannot be turned into a new lease when the response finally arrives.
			if mode == "delayed" {
				r.now = func() time.Time { return time.Now().Add(-time.Minute) }
			}
			r.Directory = func(context.Context) (storage.Directory, error) { return d, nil }
			if err := r.ReportOnce(context.Background()); err == nil {
				t.Fatal("invalid grant accepted")
			}
			if err := r.Gate.Check(); err == nil {
				t.Fatal("invalid grant opened admission")
			}
		})
	}
}
