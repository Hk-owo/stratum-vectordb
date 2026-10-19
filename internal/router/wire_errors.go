package router

import (
	"context"

	"google.golang.org/grpc"

	stratumerrors "stratum/internal/errors"
)

// The station's handlers return the Router's errors as they are, and the Router
// builds some of its own: "no leader available" wraps stratumerrors.ErrNoLeaderYet
// with fmt.Errorf. A plain Go error that crosses the gRPC server boundary becomes
// `code = Unknown` with no ErrorInfo, so the sentinel's identity — the wire name the
// caller's retry decision is made on — is lost at exactly the hop that was meant to
// carry it. Measured in T4: every setup write during an election came back Unknown,
// and helpers that decide on the reason (not the code) treated a transient election
// as a permanent failure.
//
// wireError restores the identity at the boundary, and touches nothing else:
//   - an error that already carries a reason (a node's status forwarded through, with
//     or without a wrapping message) passes through unchanged;
//   - an error that wraps a named sentinel becomes that sentinel's status, with its
//     code and reason;
//   - anything else passes through unchanged, so gRPC maps it as it always has.
func wireError(err error) error {
	if err == nil {
		return nil
	}
	if stratumerrors.ReasonOf(err) != "" {
		return err
	}
	if stratumerrors.Name(err) == "" {
		return err
	}
	return stratumerrors.ToGRPCStatus(err)
}

// WireErrorsUnary applies wireError to every unary handler's result on the station.
func WireErrorsUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		return resp, wireError(err)
	}
}

// WireErrorsStream is the streaming counterpart of WireErrorsUnary.
func WireErrorsStream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return wireError(handler(srv, ss))
	}
}
