package router

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

type stubSyncClient struct {
	pb.DataSyncServiceClient
	err   error
	calls *int
}

func (s stubSyncClient) LocalVersion(context.Context, *pb.LocalVersionRequest, ...grpc.CallOption) (*pb.LocalVersionResponse, error) {
	*s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return &pb.LocalVersionResponse{}, nil
}

// TestProbeOnce_ClosesOnlyTheNodesThatAnswered also pins that a closed breaker costs
// nothing: a healthy cluster sends no probes.
func TestProbeOnce_ClosesOnlyTheNodesThatAnswered(t *testing.T) {
	cfg := defaultBreakerConfig
	cfg.cooldown = 0
	now := time.Now()

	var upCalls, downCalls, closedCalls int
	r := &Router{
		storageBreakers: []*breaker{tripped(cfg, now), tripped(cfg, now), newBreaker(cfg)},
		syncs: []pb.DataSyncServiceClient{
			stubSyncClient{calls: &upCalls},
			stubSyncClient{calls: &downCalls, err: status.Error(codes.Unavailable, "down")},
			stubSyncClient{calls: &closedCalls},
		},
	}
	for _, b := range r.storageBreakers {
		b.useExternalProbe()
	}

	r.probeOnce(context.Background())

	if upCalls != 1 || downCalls != 1 || closedCalls != 0 {
		t.Fatalf("probes up/down/closed = %d/%d/%d, want 1/1/0", upCalls, downCalls, closedCalls)
	}
	if !r.storageBreakers[0].allow(time.Now()) {
		t.Error("the node that answered must be back in service")
	}
	if r.storageBreakers[1].allow(time.Now()) {
		t.Error("the node that did not answer must stay out of service")
	}
}
