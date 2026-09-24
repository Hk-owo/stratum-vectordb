package service

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"stratum/internal/authmeta"
)

// ClientFacing reports whether fullMethod belongs to a service the outside world
// calls.
//
// This is the line the authentication gate needs, and it is a line the system
// already draws — no new classification is invented here:
//
//   - KnowledgeBaseService / QueryService / AdminService are the client-facing
//     surface. A caller, or the service station in front of them, calls these.
//   - DataSyncService / InternalService are how nodes collaborate: write
//     fan-out, cursor queries, epoch reports, Raft forwarding. They have no end
//     user behind them.
func ClientFacing(fullMethod string) bool {
	return hasPrefix(fullMethod, "/stratum.KnowledgeBaseService/") ||
		hasPrefix(fullMethod, "/stratum.QueryService/") ||
		hasPrefix(fullMethod, "/stratum.AdminService/")
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// ErrUnverifiedClientCall is what a client-facing call without the station's
// trust mark gets when the gate is on.
var ErrUnverifiedClientCall = status.Error(codes.Unauthenticated,
	"service: client-facing calls on this node must come through an authenticated service station")

// UnaryAuthGate requires the station's trust mark on client-facing calls when
// require is true, and passes internal collaboration through untouched.
//
// Why the gate exists: the station authenticates callers, but anything that can
// reach a node's port directly can step around it. Without this the permission
// model is decorative — and the failure is silent, which is the worse half: a
// station misconfigured to omit the mark keeps serving normally, so nothing
// looks wrong until data has already crossed a boundary it should not have.
//
// Why it is NOT a blanket requirement: node-to-node traffic carries no end
// user's credential and must never be asked for one. Fan-out, catch-up, epoch
// reports and Raft forwarding are the system talking to itself; gating them
// would mix internal coordination with external authorization and break those
// paths outright.
//
// require=false leaves every call accepted, which is what a deployment with no
// station needs. It is a deliberate default and not a safe one: the setting
// should be on wherever a station is in front of the cluster.
//
// verifier is what makes the mark a claim rather than a formality: it checks the
// HMAC the station stamped (internal/authmeta.Signer). A nil verifier — no shared
// secret configured — verifies NOTHING, so with require=true the node refuses
// every client-facing call. That direction is on purpose: an operator who turned
// the gate on wants it on, and "no key" must not quietly mean "no gate".
func UnaryAuthGate(require bool, verifier authmeta.Verifier) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if require && ClientFacing(info.FullMethod) && !verified(verifier, ctx) {
			return nil, ErrUnverifiedClientCall
		}
		return handler(ctx, req)
	}
}

// StreamAuthGate is UnaryAuthGate for streaming calls. Every client-facing RPC
// is unary today, so this exists so the gate cannot be half-installed by adding
// a streaming method later and forgetting the stream side.
func StreamAuthGate(require bool, verifier authmeta.Verifier) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if require && ClientFacing(info.FullMethod) && !verified(verifier, ss.Context()) {
			return ErrUnverifiedClientCall
		}
		return handler(srv, ss)
	}
}

// verified reports whether ctx carries a mark this node can vouch for. The nil
// check is not redundant with the interface: a typed-nil *Signer also answers
// false, and both mean the same thing here.
func verified(verifier authmeta.Verifier, ctx context.Context) bool {
	return verifier != nil && verifier.Verify(ctx)
}
