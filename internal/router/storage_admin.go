package router

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "stratum/api/proto/stratum"
)

type StorageAdminServer struct {
	pb.UnimplementedStorageAdminServiceServer
	r *Router
}

func NewStorageAdminServer(r *Router) *StorageAdminServer { return &StorageAdminServer{r: r} }
func (s *StorageAdminServer) authorize(ctx context.Context) error {
	if s.r.auth == nil {
		return nil
	}
	p, err := s.r.auth.Authorize(ctx, "", true)
	if err != nil {
		return err
	}
	if !p.StorageAdmin {
		return status.Error(codes.PermissionDenied, "storage administration requires an explicit storage_admin credential")
	}
	return nil
}

func (s *StorageAdminServer) GetDirectory(ctx context.Context, req *pb.GetStorageDirectoryRequest) (*pb.GetStorageDirectoryResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	return forwarded(Forward(s.r, ctx, pb.StorageAdminService_GetDirectory_FullMethodName, "", func(idx int, c context.Context) (*pb.GetStorageDirectoryResponse, error) {
		return pb.NewStorageAdminServiceClient(s.r.conns[idx]).GetDirectory(c, req)
	}))
}

func (s *StorageAdminServer) RegisterNodes(ctx context.Context, req *pb.RegisterStorageNodesRequest) (*pb.StorageAdminResult, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	return forwarded(Forward(s.r, ctx, pb.StorageAdminService_RegisterNodes_FullMethodName, "", func(idx int, c context.Context) (*pb.StorageAdminResult, error) {
		return pb.NewStorageAdminServiceClient(s.r.conns[idx]).RegisterNodes(c, req)
	}))
}

func (s *StorageAdminServer) ManageNode(ctx context.Context, req *pb.ManageStorageNodeRequest) (*pb.StorageAdminResult, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	return forwarded(Forward(s.r, ctx, pb.StorageAdminService_ManageNode_FullMethodName, "", func(idx int, c context.Context) (*pb.StorageAdminResult, error) {
		return pb.NewStorageAdminServiceClient(s.r.conns[idx]).ManageNode(c, req)
	}))
}

func (s *StorageAdminServer) MigrateKB(ctx context.Context, req *pb.MigrateStorageKBRequest) (*pb.StorageAdminResult, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	return forwarded(Forward(s.r, ctx, pb.StorageAdminService_MigrateKB_FullMethodName, "", func(idx int, c context.Context) (*pb.StorageAdminResult, error) {
		return pb.NewStorageAdminServiceClient(s.r.conns[idx]).MigrateKB(c, req)
	}))
}

func (s *StorageAdminServer) CancelMigration(ctx context.Context, req *pb.CancelStorageMigrationRequest) (*pb.StorageAdminResult, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	return forwarded(Forward(s.r, ctx, pb.StorageAdminService_CancelMigration_FullMethodName, "", func(idx int, c context.Context) (*pb.StorageAdminResult, error) {
		return pb.NewStorageAdminServiceClient(s.r.conns[idx]).CancelMigration(c, req)
	}))
}

func (s *StorageAdminServer) DrainNode(ctx context.Context, req *pb.DrainStorageNodeRequest) (*pb.StorageAdminResult, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	return forwarded(Forward(s.r, ctx, pb.StorageAdminService_DrainNode_FullMethodName, "", func(idx int, c context.Context) (*pb.StorageAdminResult, error) {
		return pb.NewStorageAdminServiceClient(s.r.conns[idx]).DrainNode(c, req)
	}))
}
