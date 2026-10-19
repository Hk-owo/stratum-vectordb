package router

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	stratumerrors "stratum/internal/errors"
)

func TestWireError_RestoresTheSentinelTheRouterWrapped(t *testing.T) {
	err := wireError(fmt.Errorf("router: no leader available: %w", stratumerrors.ErrNoLeaderYet))

	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable", got)
	}
	if got := stratumerrors.ReasonOf(err); got != "no_leader_yet" {
		t.Fatalf("reason = %q, want no_leader_yet", got)
	}
	if !isRetryableErr(err, true) {
		t.Fatal("an election in flight must read as retryable after the boundary")
	}
}

func TestWireError_LeavesAForwardedStatusAlone(t *testing.T) {
	forwarded := stratumerrors.ToGRPCStatus(stratumerrors.ErrIndexNotReady)
	wrapped := fmt.Errorf("router: no leader available after 3 attempt(s): %w", forwarded)

	got := wireError(wrapped)
	if got != wrapped {
		t.Fatalf("an error that already carries a reason must pass through unchanged, got %v", got)
	}
	if r := stratumerrors.ReasonOf(got); r != "index_not_ready" {
		t.Fatalf("reason = %q, want index_not_ready", r)
	}
}

func TestWireError_LeavesUnnamedErrorsAndNilAlone(t *testing.T) {
	if wireError(nil) != nil {
		t.Fatal("nil must stay nil")
	}
	plain := errors.New("router: all nodes unavailable")
	if got := wireError(plain); got != plain {
		t.Fatalf("an unnamed error must pass through unchanged, got %v", got)
	}
}

// The interceptor is what actually crosses the boundary, so it is checked through a
// real gRPC server: without it the client sees Unknown and no reason.
func TestWireErrorsUnary_ReasonSurvivesARealServer(t *testing.T) {
	for _, tc := range []struct {
		name        string
		interceptor bool
		wantCode    codes.Code
		wantReason  string
	}{
		{"without interceptor", false, codes.Unknown, ""},
		{"with interceptor", true, codes.Unavailable, "no_leader_yet"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lis := bufconn.Listen(1 << 16)
			var opts []grpc.ServerOption
			if tc.interceptor {
				opts = append(opts, grpc.ChainUnaryInterceptor(WireErrorsUnary()))
			}
			gs := grpc.NewServer(opts...)
			healthpb.RegisterHealthServer(gs, failingHealth{})
			go func() { _ = gs.Serve(lis) }()
			defer gs.Stop()

			conn, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()

			_, err = healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("code = %v, want %v (err: %v)", got, tc.wantCode, err)
			}
			if got := stratumerrors.ReasonOf(err); got != tc.wantReason {
				t.Fatalf("reason = %q, want %q", got, tc.wantReason)
			}
		})
	}
}

// failingHealth stands in for a station handler that returns the Router's own error.
type failingHealth struct {
	healthpb.UnimplementedHealthServer
}

func (failingHealth) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return nil, fmt.Errorf("router: no leader available: %w", stratumerrors.ErrNoLeaderYet)
}

// An election seen through every node ("not leader" everywhere) must leave the
// station as no_leader_yet, while the cause stays in the message.
func TestForwardWrite_ExhaustedElectionCrossesTheBoundaryAsNoLeaderYet(t *testing.T) {
	r := &Router{
		controlAddrs: []string{"a", "b", "c"},
		discoverer:   &fakeResolver{ok: false},
	}
	_, err := forwardWrite(r, context.Background(), "", 0, func(idx int, ctx context.Context) (string, error) {
		return "", notLeaderErr()
	})
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	if got := stratumerrors.Name(err); got != "no_leader_yet" {
		t.Fatalf("Name(err) = %q, want no_leader_yet (err: %v)", got, err)
	}
	wire := wireError(err)
	if got := status.Code(wire); got != codes.Unavailable {
		t.Fatalf("code after the boundary = %v, want Unavailable", got)
	}
	if got := stratumerrors.ReasonOf(wire); got != "no_leader_yet" {
		t.Fatalf("reason after the boundary = %q, want no_leader_yet", got)
	}
}

// A last error that already names its reason keeps it: that reason is the diagnosis.
func TestNoLeaderAfter_KeepsANamedCause(t *testing.T) {
	cause := stratumerrors.ToGRPCStatus(stratumerrors.ErrStorageUnavailable)
	err := noLeaderAfter("router: no leader available after 3 attempt(s)", cause)
	if got := stratumerrors.ReasonOf(wireError(err)); got != "storage_unavailable" {
		t.Fatalf("reason = %q, want storage_unavailable", got)
	}
}
