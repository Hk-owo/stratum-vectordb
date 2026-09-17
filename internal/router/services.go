package router

import (
	"context"

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
	return Forward(s.r, ctx, pb.QueryService_Query_FullMethodName, req.GetKnowledgeBaseId(), func(idx int, ctx context.Context) (*pb.QueryResponse, error) {
		return s.r.querys[idx].Query(ctx, req)
	})
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

func (s *AdminServer) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return Forward(s.r, ctx, pb.AdminService_HealthCheck_FullMethodName, "", func(idx int, ctx context.Context) (*pb.HealthCheckResponse, error) {
		return s.r.admins[idx].HealthCheck(ctx, req)
	})
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
