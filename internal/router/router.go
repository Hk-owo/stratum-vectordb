package router

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

// Config configures a Router.
type Config struct {
	// Addrs is the gRPC address of every cluster node. The router dials
	// all of them and discovers the leader among them.
	Addrs []string
}

// Router is the routing layer core: it holds connections to every cluster
// node, discovers the current leader, and routes unary calls between them.
// Write calls go to the leader (re-discovering on failover); read calls
// round-robin across all nodes with failover to the next node.
type Router struct {
	addrs      []string
	conns      []*grpc.ClientConn
	kbs        []pb.KnowledgeBaseServiceClient
	querys     []pb.QueryServiceClient
	admins     []pb.AdminServiceClient
	discoverer leaderResolver
	rr         atomic.Uint64
}

// NewRouter dials every node address (non-blocking, like the HTTP
// gateway) and builds the leader discoverer.
func NewRouter(cfg Config) (*Router, error) {
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("router: no node addresses configured")
	}
	r := &Router{addrs: append([]string(nil), cfg.Addrs...)}
	for _, a := range cfg.Addrs {
		conn, err := grpc.Dial(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			r.Close()
			return nil, err
		}
		r.conns = append(r.conns, conn)
		r.kbs = append(r.kbs, pb.NewKnowledgeBaseServiceClient(conn))
		r.querys = append(r.querys, pb.NewQueryServiceClient(conn))
		r.admins = append(r.admins, pb.NewAdminServiceClient(conn))
	}
	admins := make([]statusClient, len(r.admins))
	for i, a := range r.admins {
		admins[i] = a
	}
	r.discoverer = NewLeaderDiscoverer(admins)
	return r, nil
}

// Close closes every node connection.
func (r *Router) Close() {
	for _, c := range r.conns {
		_ = c.Close()
	}
}

// Forward routes a unary call to the right node: write methods (see
// isWriteMethod) go to the leader, everything else round-robins across
// nodes. fn performs the actual call against node index idx; the concrete
// request/response types stay static in the caller.
func Forward[T any](r *Router, ctx context.Context, fullMethod string, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	if isWriteMethod(fullMethod) {
		return forwardWrite(r, ctx, fn)
	}
	return forwardRead(r, ctx, fn)
}

// forwardWrite runs fn on the current leader. The leader is re-polled on
// every call (LeaderNow, no caching) so a stale leader can never keep
// writes pinned to a dead or demoted node. When the leader rejects the
// call (not leader, or the node went down), the call is retried on the
// re-polled leader. With no known leader it falls back to trying every
// node once. Non-retryable errors (validation, index failures, ...) are
// returned as-is.
func forwardWrite[T any](r *Router, ctx context.Context, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T
	for attempt := 0; attempt <= len(r.addrs); attempt++ {
		idx, ok := r.discoverer.LeaderNow(ctx)
		if !ok {
			// Election in progress or discovery failed: try every node
			// once — the real leader accepts the write if reachable.
			return tryAll(r, ctx, fn)
		}
		resp, err := fn(idx, ctx)
		if err == nil {
			return resp, nil
		}
		if !isRetryableErr(err) {
			return zero, err
		}
	}
	return zero, errors.New("router: no leader available")
}

// forwardRead runs fn on nodes in round-robin order, skipping to the next
// node when one is unreachable. The first successful response wins.
func forwardRead[T any](r *Router, ctx context.Context, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T
	n := len(r.addrs)
	start := int(r.rr.Add(1)-1) % n
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		resp, err := fn(idx, ctx)
		if err == nil {
			return resp, nil
		}
		if !isRetryableErr(err) {
			return zero, err
		}
	}
	return zero, errors.New("router: all nodes unavailable")
}

// tryAll runs fn against every node until one succeeds. Used as the
// fallback when no leader is known.
func tryAll[T any](r *Router, ctx context.Context, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T
	for idx := range r.addrs {
		resp, err := fn(idx, ctx)
		if err == nil {
			return resp, nil
		}
		if !isRetryableErr(err) {
			return zero, err
		}
	}
	return zero, errors.New("router: no leader available")
}

// isRetryableErr reports whether a forwarded call should retry on another
// node:
//   - kvraft.ErrNotLeader (codes.Internal + "not leader"): this node is
//     not the Raft leader, the router must re-discover and retry.
//   - codes.Unavailable: node down / connection failed (leader failover
//     or a stopped container).
//
// Everything else (validation errors, index failures, ...) is terminal.
func isRetryableErr(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	if st.Code() == codes.Unavailable {
		return true
	}
	return st.Code() == codes.Internal && strings.Contains(st.Message(), "not leader")
}
