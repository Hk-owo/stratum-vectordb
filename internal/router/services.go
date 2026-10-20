package router

import (
	"context"
	"strings"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
)

// KBServer re-exposes KnowledgeBaseService on the router. Every call is
// forwarded through the Router: writes go to the current leader, reads
// round-robin across nodes.
type KBServer struct {
	pb.UnimplementedKnowledgeBaseServiceServer
	r *Router
}

// NewKBServer constructs the KnowledgeBaseService server for a Router.
func NewKBServer(r *Router) *KBServer {
	return &KBServer{r: r}
}

func (s *KBServer) CreateKnowledgeBase(ctx context.Context, req *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.KnowledgeBaseService_CreateKnowledgeBase_FullMethodName, "", func(idx int, ctx context.Context) (*pb.CreateKnowledgeBaseResponse, error) {
		return s.r.kbs[idx].CreateKnowledgeBase(ctx, req)
	}))
}

func (s *KBServer) DeleteKnowledgeBase(ctx context.Context, req *pb.DeleteKnowledgeBaseRequest) (*pb.DeleteKnowledgeBaseResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.KnowledgeBaseService_DeleteKnowledgeBase_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.DeleteKnowledgeBaseResponse, error) {
		return s.r.kbs[idx].DeleteKnowledgeBase(ctx, req)
	}))
}

func (s *KBServer) CreateVersion(ctx context.Context, req *pb.CreateVersionRequest) (*pb.CreateVersionResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.KnowledgeBaseService_CreateVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.CreateVersionResponse, error) {
		return s.r.kbs[idx].CreateVersion(ctx, req)
	}))
}

func (s *KBServer) ListVersions(ctx context.Context, req *pb.ListVersionsRequest) (*pb.ListVersionsResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.KnowledgeBaseService_ListVersions_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.ListVersionsResponse, error) {
		return s.r.kbs[idx].ListVersions(ctx, req)
	}))
}

// AwaitVersion is a read: it holds no state, so the station may spread repeated
// calls across nodes, and a caller that reconnects lands wherever it lands.
func (s *KBServer) AwaitVersion(ctx context.Context, req *pb.AwaitVersionRequest) (*pb.AwaitVersionResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.KnowledgeBaseService_AwaitVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.AwaitVersionResponse, error) {
		return s.r.kbs[idx].AwaitVersion(ctx, req)
	}))
}

func (s *KBServer) RollbackVersion(ctx context.Context, req *pb.RollbackVersionRequest) (*pb.RollbackVersionResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.KnowledgeBaseService_RollbackVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.RollbackVersionResponse, error) {
		return s.r.kbs[idx].RollbackVersion(ctx, req)
	}))
}

func (s *KBServer) DeleteVersion(ctx context.Context, req *pb.DeleteVersionRequest) (*pb.DeleteVersionResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.KnowledgeBaseService_DeleteVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.DeleteVersionResponse, error) {
		return s.r.kbs[idx].DeleteVersion(ctx, req)
	}))
}

// DiscardVersion is a write: it removes replicated metadata, so it goes to the
// leader like every other proposal.
func (s *KBServer) DiscardVersion(ctx context.Context, req *pb.DiscardVersionRequest) (*pb.DiscardVersionResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.KnowledgeBaseService_DiscardVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.DiscardVersionResponse, error) {
		return s.r.kbs[idx].DiscardVersion(ctx, req)
	}))
}

func (s *KBServer) ListKnowledgeBases(ctx context.Context, req *pb.ListKnowledgeBasesRequest) (*pb.ListKnowledgeBasesResponse, error) {
	resp, err := Forward(s.r, ctx, pb.KnowledgeBaseService_ListKnowledgeBases_FullMethodName, "", func(idx int, ctx context.Context) (*pb.ListKnowledgeBasesResponse, error) {
		return s.r.kbs[idx].ListKnowledgeBases(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return s.keepVisibleKnowledgeBases(ctx, resp), nil
}

// keepVisibleKnowledgeBases drops the entries this caller may not read.
//
// The request names no knowledge base, so Authorize could only check the credential
// — and every entry it returned carries the KB's name, id and embed config. A token
// granted one KB therefore got an inventory of the whole cluster, which is the half
// of tenant isolation that does not depend on guessing an id (M16 of
// docs/code-review-2026-10-18.md). Filtering has to happen here: the service layer
// has no principal, by design (see authgate's ClientFacing comment), so the station
// is the only layer that knows who is asking.
func (s *KBServer) keepVisibleKnowledgeBases(ctx context.Context, resp *pb.ListKnowledgeBasesResponse) *pb.ListKnowledgeBasesResponse {
	if resp == nil || s.r.auth == nil {
		// No credential table: the deployment has no tenants, so there is nothing to
		// filter by. Filtering to nothing would break the table-less shape entirely.
		return resp
	}
	p, err := s.r.auth.Authenticate(ctx)
	if err != nil {
		// Forward already refused an unauthenticated caller; nothing to narrow.
		return resp
	}
	if p.AllKBs {
		return resp
	}
	kept := make([]*pb.KnowledgeBaseInfo, 0, len(resp.GetKnowledgeBases()))
	for _, kb := range resp.GetKnowledgeBases() {
		if p.Allows(kb.GetKnowledgeBaseId(), false) {
			kept = append(kept, kb)
		}
	}
	resp.KnowledgeBases = kept
	return resp
}

func (s *KBServer) GetKnowledgeBase(ctx context.Context, req *pb.GetKnowledgeBaseRequest) (*pb.GetKnowledgeBaseResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.KnowledgeBaseService_GetKnowledgeBase_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.GetKnowledgeBaseResponse, error) {
		return s.r.kbs[idx].GetKnowledgeBase(ctx, req)
	}))
}

// QueryServer re-exposes QueryService on the router. Queries are
// read-only, so they load-balance across all nodes.
type QueryServer struct {
	pb.UnimplementedQueryServiceServer
	r *Router
}

// NewQueryServer constructs the QueryService server for a Router.
func NewQueryServer(r *Router) *QueryServer {
	return &QueryServer{r: r}
}

func (s *QueryServer) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	// §9.3(2): attach the freshness credential — the version the control layer
	// says this knowledge base should be served at. A node whose cursor has not
	// reached it refuses rather than answering from a state that is complete but
	// stale, which is what turns §9.1 risk 1 from silent into visible.
	//
	// A caller that already set min_version keeps its own value: it is asking
	// for something stricter than the station's default, and there is no reason
	// to loosen that.
	// A knowledge base with no active version carries no credential: there is no
	// version to be fresh at, and attaching min_version 0 would have every query
	// refused by its own credential — version 0 exists nowhere
	// (docs/cursor-persistence-plan.md §5.3).
	if req.MinVersion == nil {
		if v, ok := s.r.ExpectedVersion(req.GetKnowledgeBaseId()); ok && v > 0 {
			req.MinVersion = &v
		}
	}
	// R1 of docs/code-review-2026-10-14.md: a query that names a version is
	// narrowed by THAT version, not by the knowledge base's active one. The two
	// are usually the same; when they are not, the caller is asking about a
	// historical version, and selecting nodes "fresh for the active version"
	// would pick exactly the ones least likely to still hold the old one.
	queryOpts := []ForwardOption{}
	if v := req.GetVersionId(); v != 0 {
		queryOpts = append(queryOpts, WithPinnedVersion(v))
	}
	resp, err := Forward(s.r, ctx, pb.QueryService_Query_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.QueryResponse, error) {
		return s.r.querys[idx].Query(ctx, req)
	}, queryOpts...)
	if err != nil {
		return nil, err
	}
	// §10.1: the storage layer's health rides the READ response, so a client that
	// only ever reads still learns the cluster is below quorum. Without it the
	// only way to find out is a write failing, which leaves a read-only caller
	// permanently blind to a storage layer that is one replica from being unable
	// to accept anything.
	//
	// The verdict comes from THIS station's snapshot, not from the node that
	// answered, and that is the whole reason the field is set here: the aggregate
	// is control-leader-side soft state, so a storage node does not have it. The
	// flag stays false when the snapshot has no verdict, which is the same value
	// as "healthy" — see the field's comment in the proto for why that collapse is
	// deliberate.
	if degraded, _, known := s.r.routes.DegradationSummary(); known {
		resp.StorageDegraded = degraded
	}
	return resp, nil
}

// AdminServer re-exposes AdminService on the router. Health/system reads
// load-balance; RebuildIndex/WarmupVersion are leader-bound writes.
type AdminServer struct {
	pb.UnimplementedAdminServiceServer
	r *Router
}

// NewAdminServer constructs the AdminService server for a Router.
func NewAdminServer(r *Router) *AdminServer {
	return &AdminServer{r: r}
}

// HealthCheck proxies the storage node's LOCAL health and appends the one thing
// that node cannot know: whether the storage layer as a whole is below quorum.
//
// The append is the point (docs/storage-degradation-signal-plan.md §10.1). The
// verdict lives in the control leader's aggregate, and the node answering
// HealthCheck is a storage node — its "am I the control leader" answer is
// constant false (RemoteRaftNode.IsLeader), so the line can never come from
// there. Without this, storage-layer health is invisible to any probe that does
// not attempt a write.
//
// It goes in Details and NOT in the status: below quorum a replica that still
// holds the data still serves reads, so a probe that turned this unhealthy would
// pull traffic off a node that is doing its job (§2 non-goals).
//
// An all-in-one node populates the same line from its own control plane (it IS
// the leader there), so the append is skipped when the line is already present
// rather than reported twice.
func (s *AdminServer) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	resp, err := Forward(s.r, ctx, pb.AdminService_HealthCheck_FullMethodName, "", func(idx int, ctx context.Context) (*pb.HealthCheckResponse, error) {
		return s.r.admins[idx].HealthCheck(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	degraded, detail, known := s.r.routes.DegradationSummary()
	if !known || !degraded || strings.Contains(resp.GetDetails(), "storage: ") {
		return resp, nil
	}
	// "ok" is what a node with no complaints says; keeping it in front of a
	// degradation would read as a contradiction, so it is replaced rather than
	// prefixed.
	if resp.Details == "" || resp.Details == "ok" {
		resp.Details = "storage: " + detail
	} else {
		resp.Details += "; storage: " + detail
	}
	return resp, nil
}

func (s *AdminServer) GetSystemStatus(ctx context.Context, req *pb.GetSystemStatusRequest) (*pb.GetSystemStatusResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.AdminService_GetSystemStatus_FullMethodName, "", func(idx int, ctx context.Context) (*pb.GetSystemStatusResponse, error) {
		return s.r.admins[idx].GetSystemStatus(ctx, req)
	}))
}

func (s *AdminServer) GetClusterStatus(ctx context.Context, req *pb.GetClusterStatusRequest) (*pb.GetClusterStatusResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.AdminService_GetClusterStatus_FullMethodName, "", func(idx int, ctx context.Context) (*pb.GetClusterStatusResponse, error) {
		return s.r.admins[idx].GetClusterStatus(ctx, req)
	}))
}

func (s *AdminServer) RebuildIndex(ctx context.Context, req *pb.RebuildIndexRequest) (*pb.RebuildIndexResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.AdminService_RebuildIndex_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.RebuildIndexResponse, error) {
		return s.r.admins[idx].RebuildIndex(ctx, req)
	}))
}

func (s *AdminServer) WarmupVersion(ctx context.Context, req *pb.WarmupVersionRequest) (*pb.WarmupVersionResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.AdminService_WarmupVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.WarmupVersionResponse, error) {
		return s.r.admins[idx].WarmupVersion(ctx, req)
	}))
}

// ListFailedVersions proxies the operator's work queue (§10.1). The knowledge base
// is optional and travels as the routing key, so a cluster-wide query is
// load-balanced like any other metadata read; an empty key is what Forward already
// does for GetSystemStatus.
func (s *AdminServer) ListFailedVersions(ctx context.Context, req *pb.ListFailedVersionsRequest) (*pb.ListFailedVersionsResponse, error) {
	resp, err := Forward(s.r, ctx, pb.AdminService_ListFailedVersions_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.ListFailedVersionsResponse, error) {
		return s.r.admins[idx].ListFailedVersions(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	// The knowledge base is optional here, so a cluster-wide query is served — and
	// every entry names a version of somebody's KB. Narrow it the same way
	// ListKnowledgeBases is narrowed (M16 of docs/code-review-2026-10-18.md).
	if s.r.auth == nil || req.GetKnowledgeBaseId() != "" {
		return resp, nil
	}
	p, authErr := s.r.auth.Authenticate(ctx)
	if authErr != nil || p.AllKBs {
		return resp, nil
	}
	kept := make([]*pb.FailedVersion, 0, len(resp.GetVersions()))
	for _, v := range resp.GetVersions() {
		if p.Allows(v.GetKbId(), false) {
			kept = append(kept, v)
		}
	}
	resp.Versions = kept
	return resp, nil
}

// ForceRetryVersion proxies an operator's retry of an index-side verdict. It is a
// write to replicated metadata, so it goes through the same Forward as the other
// operator RPCs and inherits their retry semantics.
func (s *AdminServer) ForceRetryVersion(ctx context.Context, req *pb.ForceRetryVersionRequest) (*pb.ForceRetryVersionResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.AdminService_ForceRetryVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.ForceRetryVersionResponse, error) {
		return s.r.admins[idx].ForceRetryVersion(ctx, req)
	}))
}

// ForceAbandonVersion proxies an operator's abandonment of a terminated version.
func (s *AdminServer) ForceAbandonVersion(ctx context.Context, req *pb.ForceAbandonVersionRequest) (*pb.ForceAbandonVersionResponse, error) {
	return forwarded(Forward(s.r, ctx, pb.AdminService_ForceAbandonVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.ForceAbandonVersionResponse, error) {
		return s.r.admins[idx].ForceAbandonVersion(ctx, req)
	}))
}

// forwarded keeps a forwarded call's error identity on the wire.
//
// gRPC maps a PLAIN error to codes.Unknown, and that erased exactly the distinction this
// package depends on: measured in the T4 run, CreateVersion answered
// `code = Unknown desc = router: no leader available: no leader available` — the router
// had wrapped the named no_leader_yet sentinel, and the server boundary relabelled it, so
// neither the test nor any client could tell "the cluster is electing" (retry) from a
// real failure. ToGRPCStatus is the single conversion point; it also PASSES THROUGH an
// error that is already a status (what a backend node returned), so a NotFound from the
// control tier keeps its code.
func forwarded[T any](resp T, err error) (T, error) {
	if err != nil {
		return resp, stratumerrors.ToGRPCStatus(err)
	}
	return resp, nil
}
