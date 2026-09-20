package router

import (
	"context"
	"strings"

	pb "stratum/api/proto/stratum"
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
	return Forward(s.r, ctx, pb.KnowledgeBaseService_CreateKnowledgeBase_FullMethodName, "", func(idx int, ctx context.Context) (*pb.CreateKnowledgeBaseResponse, error) {
		return s.r.kbs[idx].CreateKnowledgeBase(ctx, req)
	})
}

func (s *KBServer) DeleteKnowledgeBase(ctx context.Context, req *pb.DeleteKnowledgeBaseRequest) (*pb.DeleteKnowledgeBaseResponse, error) {
	return Forward(s.r, ctx, pb.KnowledgeBaseService_DeleteKnowledgeBase_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.DeleteKnowledgeBaseResponse, error) {
		return s.r.kbs[idx].DeleteKnowledgeBase(ctx, req)
	})
}

func (s *KBServer) CreateVersion(ctx context.Context, req *pb.CreateVersionRequest) (*pb.CreateVersionResponse, error) {
	return Forward(s.r, ctx, pb.KnowledgeBaseService_CreateVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.CreateVersionResponse, error) {
		return s.r.kbs[idx].CreateVersion(ctx, req)
	})
}

func (s *KBServer) ListVersions(ctx context.Context, req *pb.ListVersionsRequest) (*pb.ListVersionsResponse, error) {
	return Forward(s.r, ctx, pb.KnowledgeBaseService_ListVersions_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.ListVersionsResponse, error) {
		return s.r.kbs[idx].ListVersions(ctx, req)
	})
}

// AwaitVersion is a read: it holds no state, so the station may spread repeated
// calls across nodes, and a caller that reconnects lands wherever it lands.
func (s *KBServer) AwaitVersion(ctx context.Context, req *pb.AwaitVersionRequest) (*pb.AwaitVersionResponse, error) {
	return Forward(s.r, ctx, pb.KnowledgeBaseService_AwaitVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.AwaitVersionResponse, error) {
		return s.r.kbs[idx].AwaitVersion(ctx, req)
	})
}

func (s *KBServer) RollbackVersion(ctx context.Context, req *pb.RollbackVersionRequest) (*pb.RollbackVersionResponse, error) {
	return Forward(s.r, ctx, pb.KnowledgeBaseService_RollbackVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.RollbackVersionResponse, error) {
		return s.r.kbs[idx].RollbackVersion(ctx, req)
	})
}

func (s *KBServer) DeleteVersion(ctx context.Context, req *pb.DeleteVersionRequest) (*pb.DeleteVersionResponse, error) {
	return Forward(s.r, ctx, pb.KnowledgeBaseService_DeleteVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.DeleteVersionResponse, error) {
		return s.r.kbs[idx].DeleteVersion(ctx, req)
	})
}

// DiscardVersion is a write: it removes replicated metadata, so it goes to the
// leader like every other proposal.
func (s *KBServer) DiscardVersion(ctx context.Context, req *pb.DiscardVersionRequest) (*pb.DiscardVersionResponse, error) {
	return Forward(s.r, ctx, pb.KnowledgeBaseService_DiscardVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.DiscardVersionResponse, error) {
		return s.r.kbs[idx].DiscardVersion(ctx, req)
	})
}

func (s *KBServer) ListKnowledgeBases(ctx context.Context, req *pb.ListKnowledgeBasesRequest) (*pb.ListKnowledgeBasesResponse, error) {
	return Forward(s.r, ctx, pb.KnowledgeBaseService_ListKnowledgeBases_FullMethodName, "", func(idx int, ctx context.Context) (*pb.ListKnowledgeBasesResponse, error) {
		return s.r.kbs[idx].ListKnowledgeBases(ctx, req)
	})
}

func (s *KBServer) GetKnowledgeBase(ctx context.Context, req *pb.GetKnowledgeBaseRequest) (*pb.GetKnowledgeBaseResponse, error) {
	return Forward(s.r, ctx, pb.KnowledgeBaseService_GetKnowledgeBase_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.GetKnowledgeBaseResponse, error) {
		return s.r.kbs[idx].GetKnowledgeBase(ctx, req)
	})
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
	resp, err := Forward(s.r, ctx, pb.QueryService_Query_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.QueryResponse, error) {
		return s.r.querys[idx].Query(ctx, req)
	})
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
	return Forward(s.r, ctx, pb.AdminService_GetSystemStatus_FullMethodName, "", func(idx int, ctx context.Context) (*pb.GetSystemStatusResponse, error) {
		return s.r.admins[idx].GetSystemStatus(ctx, req)
	})
}

func (s *AdminServer) GetClusterStatus(ctx context.Context, req *pb.GetClusterStatusRequest) (*pb.GetClusterStatusResponse, error) {
	return Forward(s.r, ctx, pb.AdminService_GetClusterStatus_FullMethodName, "", func(idx int, ctx context.Context) (*pb.GetClusterStatusResponse, error) {
		return s.r.admins[idx].GetClusterStatus(ctx, req)
	})
}

func (s *AdminServer) RebuildIndex(ctx context.Context, req *pb.RebuildIndexRequest) (*pb.RebuildIndexResponse, error) {
	return Forward(s.r, ctx, pb.AdminService_RebuildIndex_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.RebuildIndexResponse, error) {
		return s.r.admins[idx].RebuildIndex(ctx, req)
	})
}

func (s *AdminServer) WarmupVersion(ctx context.Context, req *pb.WarmupVersionRequest) (*pb.WarmupVersionResponse, error) {
	return Forward(s.r, ctx, pb.AdminService_WarmupVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.WarmupVersionResponse, error) {
		return s.r.admins[idx].WarmupVersion(ctx, req)
	})
}

// ListFailedVersions proxies the operator's work queue (§10.1). The knowledge base
// is optional and travels as the routing key, so a cluster-wide query is
// load-balanced like any other metadata read; an empty key is what Forward already
// does for GetSystemStatus.
func (s *AdminServer) ListFailedVersions(ctx context.Context, req *pb.ListFailedVersionsRequest) (*pb.ListFailedVersionsResponse, error) {
	return Forward(s.r, ctx, pb.AdminService_ListFailedVersions_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.ListFailedVersionsResponse, error) {
		return s.r.admins[idx].ListFailedVersions(ctx, req)
	})
}

// ForceRetryVersion proxies an operator's retry of an index-side verdict. It is a
// write to replicated metadata, so it goes through the same Forward as the other
// operator RPCs and inherits their retry semantics.
func (s *AdminServer) ForceRetryVersion(ctx context.Context, req *pb.ForceRetryVersionRequest) (*pb.ForceRetryVersionResponse, error) {
	return Forward(s.r, ctx, pb.AdminService_ForceRetryVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.ForceRetryVersionResponse, error) {
		return s.r.admins[idx].ForceRetryVersion(ctx, req)
	})
}

// ForceAbandonVersion proxies an operator's abandonment of a terminated version.
func (s *AdminServer) ForceAbandonVersion(ctx context.Context, req *pb.ForceAbandonVersionRequest) (*pb.ForceAbandonVersionResponse, error) {
	return Forward(s.r, ctx, pb.AdminService_ForceAbandonVersion_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.ForceAbandonVersionResponse, error) {
		return s.r.admins[idx].ForceAbandonVersion(ctx, req)
	})
}
