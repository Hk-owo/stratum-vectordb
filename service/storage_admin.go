package service

import (
	"context"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/wire"
)

type StorageAdminService struct {
	pb.UnimplementedStorageAdminServiceServer
	backend any
}

func NewStorageAdminService(backend any) *StorageAdminService {
	return &StorageAdminService{backend: backend}
}
func storageAdminResult(err error) (*pb.StorageAdminResult, error) {
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	return &pb.StorageAdminResult{}, nil
}
func (s *StorageAdminService) GetDirectory(ctx context.Context, _ *pb.GetStorageDirectoryRequest) (*pb.GetStorageDirectoryResponse, error) {
	m, ok := s.backend.(storage.MaintenanceManager)
	if !ok {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
	}
	d, err := m.GetStorageMaintenance(ctx)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	return wire.StorageDirectoryToProto(d), nil
}
func (s *StorageAdminService) RegisterNodes(ctx context.Context, req *pb.RegisterStorageNodesRequest) (*pb.StorageAdminResult, error) {
	m, ok := s.backend.(storage.Manager)
	if !ok {
		return storageAdminResult(serrors.ErrPlacementChangeUnsafe)
	}
	var nodes []storage.Node
	for _, n := range req.GetNodes() {
		nodes = append(nodes, storage.Node{ID: n.GetNodeId(), Addr: n.GetAddress(), State: storage.NodeState(n.GetManagementState())})
	}
	return storageAdminResult(m.ProposeRegisterStorageNodes(ctx, req.GetExpectedRevision(), nodes))
}
func (s *StorageAdminService) ManageNode(ctx context.Context, req *pb.ManageStorageNodeRequest) (*pb.StorageAdminResult, error) {
	m, ok := s.backend.(storage.MaintenanceManager)
	if !ok {
		return storageAdminResult(serrors.ErrPlacementChangeUnsafe)
	}
	serviceReq := storage.ServiceRequest{NodeID: req.GetNodeId(), ExpectedRevision: req.GetExpectedRevision(), Address: req.GetAddress(), OperationID: req.GetOperationId(), BlockRevision: req.GetBlockRevision()}
	switch req.GetAction() {
	case "block", "resume":
		sm, ok := s.backend.(storage.ServiceManager)
		if !ok {
			return storageAdminResult(serrors.ErrPlacementChangeUnsafe)
		}
		if req.Action == "block" {
			return storageAdminResult(sm.ProposeBlockStorageService(ctx, serviceReq))
		}
		return storageAdminResult(sm.ProposeResumeStorageService(ctx, serviceReq))
	case "confirm_revoked":
		sm, ok := s.backend.(interface {
			WaitStorageServiceRevoked(context.Context, storage.ServiceRequest) error
		})
		if !ok {
			return storageAdminResult(serrors.ErrPlacementChangeUnsafe)
		}
		return storageAdminResult(sm.WaitStorageServiceRevoked(ctx, serviceReq))
	default:
		return storageAdminResult(m.ProposeManageStorageNode(ctx, storage.NodeChange{NodeID: req.GetNodeId(), ExpectedRevision: req.GetExpectedRevision(), Action: req.GetAction(), Address: req.GetAddress()}))
	}
}
func (s *StorageAdminService) MigrateKB(ctx context.Context, req *pb.MigrateStorageKBRequest) (*pb.StorageAdminResult, error) {
	m, ok := s.backend.(storage.MigrationManager)
	if !ok {
		return storageAdminResult(serrors.ErrPlacementChangeUnsafe)
	}
	return storageAdminResult(m.ProposeBeginKBMigration(ctx, storage.MigrationRequest{OperationID: req.GetOperationId(), KBID: req.GetKnowledgeBaseId(), ExpectedEpoch: req.GetExpectedEpoch(), FromID: req.GetFromNodeId(), ToID: req.GetToNodeId(), Fault: req.GetFault()}))
}
func (s *StorageAdminService) CancelMigration(ctx context.Context, req *pb.CancelStorageMigrationRequest) (*pb.StorageAdminResult, error) {
	m, ok := s.backend.(storage.MigrationManager)
	if !ok {
		return storageAdminResult(serrors.ErrPlacementChangeUnsafe)
	}
	return storageAdminResult(m.ProposeCancelKBMigration(ctx, req.GetOperationId()))
}
func (s *StorageAdminService) DrainNode(ctx context.Context, req *pb.DrainStorageNodeRequest) (*pb.StorageAdminResult, error) {
	m, ok := s.backend.(interface {
		ProposeDrainStorageNode(context.Context, storage.DrainRequest) error
	})
	if !ok {
		return storageAdminResult(serrors.ErrPlacementChangeUnsafe)
	}
	return storageAdminResult(m.ProposeDrainStorageNode(ctx, storage.DrainRequest{OperationID: req.GetOperationId(), NodeID: req.GetNodeId(), ExpectedRevision: req.GetExpectedRevision(), TargetIDs: req.GetTargetNodeIds(), Fault: req.GetFault()}))
}
