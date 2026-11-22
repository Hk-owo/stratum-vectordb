package raft

import (
	"context"
	"google.golang.org/grpc"
	"stratum/internal/wire"

	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

func (s *InternalServiceServer) RenewStorageService(ctx context.Context, req *pb.RenewStorageServiceRequest) (*pb.RenewStorageServiceResponse, error) {
	if req.GetNodeId() <= 0 || req.GetAddress() == "" || req.GetRequestId() == "" {
		return nil, serrors.ToGRPCStatus(serrors.ErrInvalidArgument)
	}
	res, err := s.node.serviceProposal(ctx, cmdRenewStorageService, storage.ServiceRequest{NodeID: req.GetNodeId(), Address: req.GetAddress()})
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	return &pb.RenewStorageServiceResponse{DurationMs: storage.ServiceLeaseDuration.Milliseconds(), DirectoryRevision: res.ServiceRevision, RequestId: req.GetRequestId()}, nil
}

func (r *MockRaftNode) serviceProposal(ctx context.Context, kind commandType, req storage.ServiceRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	sm := stateMachine{directory: r.directory}
	result := sm.applyService(command{Type: kind, Service: &req})
	r.directory = sm.directory
	return result.Err
}
func (r *MockRaftNode) ProposeBlockStorageService(ctx context.Context, req storage.ServiceRequest) error {
	return r.serviceProposal(ctx, cmdBlockStorageService, req)
}
func (r *MockRaftNode) ProposeResumeStorageService(ctx context.Context, req storage.ServiceRequest) error {
	return r.serviceProposal(ctx, cmdResumeStorageService, req)
}

func (r *RemoteRaftNode) ProposeBlockStorageService(ctx context.Context, req storage.ServiceRequest) error {
	res, err := r.propose(ctx, command{Type: cmdBlockStorageService, Service: &req})
	if err != nil {
		return err
	}
	return res.Err
}
func (r *RemoteRaftNode) ProposeResumeStorageService(ctx context.Context, req storage.ServiceRequest) error {
	res, err := r.propose(ctx, command{Type: cmdResumeStorageService, Service: &req})
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) GetNodeStorageDirectory(ctx context.Context, nodeID int64) (storage.Directory, error) {
	var result storage.Directory
	err := r.readAtAnyControl(ctx, func(c context.Context, conn *grpc.ClientConn) error {
		resp, err := pb.NewInternalServiceClient(conn).GetStorageDirectory(c, &pb.GetStorageDirectoryRequest{NodeId: nodeID})
		if err == nil {
			result = wire.StorageDirectoryFromProto(resp)
		}
		return err
	})
	return result, err
}

var _ storage.ServiceManager = (*RaftNodeImpl)(nil)
var _ storage.ServiceManager = (*RemoteRaftNode)(nil)
var _ storage.ServiceManager = (*MockRaftNode)(nil)
