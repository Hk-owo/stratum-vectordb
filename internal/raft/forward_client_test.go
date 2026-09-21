package raft

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
)

// fakeInternalService stands in for the leader's internal service.
type fakeInternalService struct {
	pb.UnimplementedInternalServiceServer

	resp   *pb.ProposeResponse
	err    error
	gotCmd []byte
}

func (f *fakeInternalService) Propose(_ context.Context, req *pb.ProposeRequest) (*pb.ProposeResponse, error) {
	f.gotCmd = req.GetCommand()
	return f.resp, f.err
}

// startForwardTarget registers svc and returns its address plus a cleanup.
func startForwardTarget(t *testing.T, svc *fakeInternalService) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterInternalServiceServer(srv, svc)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func newTestForwarder(t *testing.T, addrs map[int64]string) *GRPCProposeForwarder {
	t.Helper()
	return &GRPCProposeForwarder{
		AddrByID: func(id int64) (string, bool) {
			addr, ok := addrs[id]
			return addr, ok
		},
		Dial: func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
			return grpc.DialContext(ctx, addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
		},
	}
}

// The command must arrive byte-for-byte: the leader has to apply exactly what
// the caller encoded, not a re-encoded copy of its own.
func TestGRPCProposeForwarder_SendsTheCommandAndReturnsTheOutcome(t *testing.T) {
	ctx := proposeCtx(t)
	svc := &fakeInternalService{resp: &pb.ProposeResponse{
		VersionId:         42,
		DeletedVersionIds: []int64{7, 8},
	}}
	addr := startForwardTarget(t, svc)
	f := newTestForwarder(t, map[int64]string{3: addr})

	cmd := []byte(`{"type":"CreateVersion"}`)
	got, err := f.ForwardPropose(ctx, 3, cmd)
	if err != nil {
		t.Fatalf("ForwardPropose: %v", err)
	}
	if string(svc.gotCmd) != string(cmd) {
		t.Errorf("leader received %q, want %q", svc.gotCmd, cmd)
	}
	if got.VersionID != 42 {
		t.Errorf("version id = %d, want 42", got.VersionID)
	}
	if len(got.DeletedVersionIDs) != 2 || got.DeletedVersionIDs[0] != 7 || got.DeletedVersionIDs[1] != 8 {
		t.Errorf("deleted ids = %v, want [7 8]", got.DeletedVersionIDs)
	}
	if got.Err != nil {
		t.Errorf("apply err = %v, want nil", got.Err)
	}
}

// An apply error travels as its sentinel name and is rebuilt on this side, so
// the caller's errors.Is still matches (Stratum_设计文档v13.md §7.3).
func TestGRPCProposeForwarder_RebuildsSentinelErrors(t *testing.T) {
	ctx := proposeCtx(t)
	svc := &fakeInternalService{resp: &pb.ProposeResponse{
		ErrorName:    "version_not_found",
		ErrorMessage: "version not found",
	}}
	addr := startForwardTarget(t, svc)
	f := newTestForwarder(t, map[int64]string{3: addr})

	got, err := f.ForwardPropose(ctx, 3, []byte("{}"))
	if err != nil {
		t.Fatalf("ForwardPropose: %v", err)
	}
	if !errors.Is(got.Err, stratumerrors.ErrVersionNotFound) {
		t.Fatalf("apply err = %v, want errors.Is(ErrVersionNotFound) to hold after the forward", got.Err)
	}
}

// An unrecognised name degrades to the message rather than a wrong category.
func TestGRPCProposeForwarder_UnknownSentinelFallsBackToMessage(t *testing.T) {
	ctx := proposeCtx(t)
	svc := &fakeInternalService{resp: &pb.ProposeResponse{
		ErrorName:    "some_error_from_the_future",
		ErrorMessage: "quota exceeded",
	}}
	addr := startForwardTarget(t, svc)
	f := newTestForwarder(t, map[int64]string{3: addr})

	got, err := f.ForwardPropose(ctx, 3, []byte("{}"))
	if err != nil {
		t.Fatalf("ForwardPropose: %v", err)
	}
	if got.Err == nil || got.Err.Error() != "quota exceeded" {
		t.Errorf("apply err = %v, want the message alone", got.Err)
	}
}

// A redirect means leadership moved mid-flight. It is reported so the caller
// can decide whether its deadline still allows another try.
func TestGRPCProposeForwarder_ReportsARedirect(t *testing.T) {
	ctx := proposeCtx(t)
	svc := &fakeInternalService{resp: &pb.ProposeResponse{LeaderId: 9}}
	addr := startForwardTarget(t, svc)
	f := newTestForwarder(t, map[int64]string{3: addr})

	_, err := f.ForwardPropose(ctx, 3, []byte("{}"))
	if err == nil {
		t.Fatal("want an error when the target redirects to another leader")
	}
	if !strings.Contains(err.Error(), "leader moved to 9") {
		t.Errorf("err = %v, want it to name the new leader", err)
	}
}

// An unknown address is an explicit failure, not a silent no-op.
func TestGRPCProposeForwarder_UnknownLeaderAddress(t *testing.T) {
	f := newTestForwarder(t, map[int64]string{})
	if _, err := f.ForwardPropose(proposeCtx(t), 3, []byte("{}")); err == nil {
		t.Fatal("want an error when the leader's address is unknown")
	}
}

// Without an address table the forwarder refuses rather than panicking.
func TestGRPCProposeForwarder_WithoutAddressTable(t *testing.T) {
	f := &GRPCProposeForwarder{}
	if _, err := f.ForwardPropose(proposeCtx(t), 3, []byte("{}")); err == nil {
		t.Fatal("want an error when no address table is wired")
	}
}
