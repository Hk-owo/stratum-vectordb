package service

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

func readyServiceGate(t *testing.T) *storage.ServiceLease {
	t.Helper()
	g := storage.NewServiceLease(1, "s1")
	t.Cleanup(g.Pause)
	d := storage.NewDirectory()
	if err := d.RegisterNodes(0, []storage.Node{{ID: 1, Addr: "s1"}}); err != nil {
		t.Fatal(err)
	}
	if err := g.Install(time.Now(), storage.ServiceLeaseDuration, d.Revision, d); err != nil {
		t.Fatal(err)
	}
	return g
}
func TestServiceLeaseUnaryRejectsExpiredResultAndKeepsRecoveryReachable(t *testing.T) {
	g := readyServiceGate(t)
	intercept := UnaryStorageServiceLease(g)
	_, err := intercept(context.Background(), &pb.QueryRequest{}, &grpc.UnaryServerInfo{FullMethod: pb.QueryService_Query_FullMethodName}, func(ctx context.Context, _ any) (any, error) { g.Pause(); return &pb.QueryResponse{}, nil })
	if serrors.ReasonOf(err) != "storage_service_paused" {
		t.Fatal("unleased success escaped:", err)
	}
	called := false
	_, err = intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: pb.QueryService_Query_FullMethodName}, func(context.Context, any) (any, error) { called = true; return nil, nil })
	if called || serrors.ReasonOf(err) != "storage_service_paused" {
		t.Fatal("closed gate admitted query")
	}
	for _, method := range []string{pb.DataSyncService_Heartbeat_FullMethodName, pb.DataSyncService_GetChainTails_FullMethodName, pb.DataSyncService_FenceKBReplica_FullMethodName, pb.AdminService_HealthCheck_FullMethodName} {
		_, err = intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { return nil, nil })
		if err != nil {
			t.Fatal("recovery/diagnostic method blocked:", method, err)
		}
	}
}

type leaseTestStream struct {
	grpc.ServerStream
	ctx  context.Context
	recv func()
}

func (s leaseTestStream) Context() context.Context { return s.ctx }
func (s leaseTestStream) RecvMsg(any) error {
	if s.recv != nil {
		s.recv()
	}
	return nil
}
func (s leaseTestStream) SendMsg(any) error { return nil }
func TestServiceLeaseStreamRejectsLateFrameAndOldGeneration(t *testing.T) {
	g := readyServiceGate(t)
	err := StreamStorageServiceLease(g)(nil, leaseTestStream{ctx: context.Background(), recv: g.Pause}, &grpc.StreamServerInfo{FullMethod: pb.DataSyncService_PushVersionData_FullMethodName}, func(_ any, ss grpc.ServerStream) error {
		if err := ss.RecvMsg(&pb.SyncEntry{}); serrors.ReasonOf(err) != "storage_service_paused" {
			t.Fatal("late frame accepted:", err)
		}
		return nil
	})
	if serrors.ReasonOf(err) != "storage_service_paused" {
		t.Fatal("stream completed after expiry:", err)
	}
}
