package raft

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/rpcpool"
)

// GRPCProposeForwarder carries proposals to the leader over the internal
// service. The node assembly wires it with the cluster's address table.
type GRPCProposeForwarder struct {
	// AddrByID resolves a node ID to its gRPC address.
	AddrByID func(id int64) (string, bool)
	// Pool supplies long-lived control-plane channels when Dial is nil.
	Pool *rpcpool.Pool
	// Dial is injectable for tests; nil uses Pool or rpcpool's default dialer.
	Dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

var _ ProposeForwarder = (*GRPCProposeForwarder)(nil)

// ForwardPropose asks the leader to run cmd and returns its apply outcome.
//
// A redirect (leader_id set) means leadership moved between the caller's check
// and this call. It is reported as an error rather than retried here: only the
// caller knows its own deadline and whether retrying is still worth it.
func (f *GRPCProposeForwarder) ForwardPropose(ctx context.Context, leaderID int64, cmd []byte) (ForwardedResult, error) {
	if f.AddrByID == nil {
		return ForwardedResult{}, errors.New("raft: forward proposal: no address table wired")
	}
	addr, ok := f.AddrByID(leaderID)
	if !ok {
		return ForwardedResult{}, fmt.Errorf("raft: forward proposal: no address for leader %d", leaderID)
	}
	conn, release, err := rpcpool.Acquire(ctx, f.Pool, f.Dial, nil, addr)
	if err != nil {
		return ForwardedResult{}, fmt.Errorf("raft: forward proposal: dial %s: %w", addr, err)
	}
	defer release()

	resp, err := pb.NewInternalServiceClient(conn).Propose(ctx, &pb.ProposeRequest{Command: cmd})
	if err != nil {
		return ForwardedResult{}, fmt.Errorf("raft: forward proposal to %s: %w", addr, err)
	}
	if moved := resp.GetLeaderId(); moved != 0 {
		return ForwardedResult{}, fmt.Errorf("raft: leader moved to %d during the forward", moved)
	}

	applyErr := stratumerrors.ByName(resp.GetErrorName())
	if applyErr == nil && resp.GetErrorMessage() != "" {
		// Unknown sentinel — a newer peer may know errors this build does not.
		// The message alone is all we have to pass along.
		applyErr = errors.New(resp.GetErrorMessage())
	}
	return ForwardedResult{
		VersionID:         resp.GetVersionId(),
		DeletedVersionIDs: resp.GetDeletedVersionIds(),
		Err:               applyErr,
	}, nil
}
