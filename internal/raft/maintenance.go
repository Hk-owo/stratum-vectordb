package raft

import (
	"context"
	"google.golang.org/grpc"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/wire"
)

func (s *InternalServiceServer) GetStorageMaintenance(ctx context.Context, _ *pb.GetStorageDirectoryRequest) (*pb.GetStorageDirectoryResponse, error) {
	d, err := s.node.GetStorageMaintenance(ctx)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	return wire.StorageDirectoryToProto(d), nil
}
func (r *RemoteRaftNode) GetStorageMaintenance(ctx context.Context) (storage.Directory, error) {
	var d storage.Directory
	err := r.readAtAnyControl(ctx, func(c context.Context, conn *grpc.ClientConn) error {
		resp, err := pb.NewInternalServiceClient(conn).GetStorageMaintenance(c, &pb.GetStorageDirectoryRequest{})
		if err == nil {
			d = wire.StorageDirectoryFromProto(resp)
		}
		return err
	})
	return d, err
}
func (r *MockRaftNode) GetStorageMaintenance(ctx context.Context) (storage.Directory, error) {
	return r.StorageDirectory(ctx)
}

func (r *RemoteRaftNode) ProposeActivateKBMigration(ctx context.Context, op string, a storage.MigrationActivation) error {
	res, err := r.propose(ctx, command{Type: cmdActivateKBMigration, OperationID: op, Activation: &a})
	if err != nil {
		return err
	}
	return res.Err
}

func (r *MockRaftNode) ProposeActivateKBMigration(ctx context.Context, op string, a storage.MigrationActivation) error {
	return r.applyMigration(ctx, command{Type: cmdActivateKBMigration, OperationID: op, Activation: &a})
}

func (r *RemoteRaftNode) ProposeAckStorageCleanup(ctx context.Context, id string) error {
	res, err := r.propose(ctx, command{Type: cmdAckStorageCleanup, OperationID: id})
	if err != nil {
		return err
	}
	return res.Err
}

func (r *MockRaftNode) ProposeAckStorageCleanup(ctx context.Context, id string) error {
	return r.applyMigration(ctx, command{Type: cmdAckStorageCleanup, OperationID: id})
}

func (r *RemoteRaftNode) ProposeManageStorageNode(ctx context.Context, req storage.NodeChange) error {
	res, err := r.propose(ctx, command{Type: cmdManageStorageNode, NodeChange: &req})
	if err != nil {
		return err
	}
	return res.Err
}

func (r *MockRaftNode) ProposeManageStorageNode(ctx context.Context, req storage.NodeChange) error {
	return r.applyMigration(ctx, command{Type: cmdManageStorageNode, NodeChange: &req})
}

func (r *RemoteRaftNode) ProposeDrainStorageNode(ctx context.Context, req storage.DrainRequest) error {
	res, err := r.propose(ctx, command{Type: cmdDrainStorageNode, Drain: &req})
	if err != nil {
		return err
	}
	return res.Err
}

func (r *MockRaftNode) ProposeDrainStorageNode(ctx context.Context, req storage.DrainRequest) error {
	return r.applyMigration(ctx, command{Type: cmdDrainStorageNode, Drain: &req})
}
