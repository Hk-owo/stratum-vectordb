package raft

import (
	"context"
	"google.golang.org/grpc"
	"stratum/internal/wire"
	"time"

	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

func (sm *stateMachine) applyService(c command) applyResult {
	if c.Service == nil {
		return applyResult{Err: serrors.ErrInvalidArgument}
	}
	var err error
	switch c.Type {
	case cmdRenewStorageService:
		err = sm.directory.AuthorizeService(*c.Service)
	case cmdBlockStorageService:
		err = sm.directory.BlockService(*c.Service)
	case cmdConfirmStorageServiceRevoked:
		err = sm.directory.ConfirmServiceRevoked(*c.Service)
	case cmdResumeStorageService:
		err = sm.directory.ResumeService(*c.Service)
	default:
		err = serrors.ErrInvalidArgument
	}
	result := applyResult{Err: err, ServiceRevision: sm.directory.Revision}
	if b, ok := sm.directory.ServiceBlocks[c.Service.NodeID]; ok {
		result.ServiceBlockRevision = b.Revision
	}
	return result
}

// serviceProposal is deliberately leader-local. Granting on the basis of a
// forwarded result or an IsLeader-only check would let an isolated old leader
// invent authorization without a current quorum.
func (r *RaftNodeImpl) serviceProposal(ctx context.Context, kind commandType, req storage.ServiceRequest) (applyResult, error) {
	if !r.IsLeader() {
		return applyResult{}, serrors.ErrNoLeaderYet
	}
	term := r.raft.Term()
	res, err := r.proposeAndWait(ctx, command{Type: kind, Service: &req})
	if err != nil {
		return res, err
	}
	if res.Err != nil {
		return res, res.Err
	}
	if !r.IsLeader() || r.raft.Term() != term || res.ServiceRevision == 0 {
		return res, serrors.ErrNoLeaderYet
	}
	return res, nil
}

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

func (r *RaftNodeImpl) ProposeBlockStorageService(ctx context.Context, req storage.ServiceRequest) error {
	_, err := r.serviceProposal(ctx, cmdBlockStorageService, req)
	return err
}
func (r *RaftNodeImpl) ProposeResumeStorageService(ctx context.Context, req storage.ServiceRequest) error {
	_, err := r.serviceProposal(ctx, cmdResumeStorageService, req)
	return err
}

// WaitStorageServiceRevoked starts AFTER a fresh quorum-confirmed block. Every
// call (including on a new leader or after restart) waits the full protocol
// bound. No persisted wall-clock expiry or empty leader-local cache is used as
// proof. Future cutover must still validate this block generation and term.
func (r *RaftNodeImpl) WaitStorageServiceRevoked(ctx context.Context, req storage.ServiceRequest) error {
	term := r.raft.Term()
	result, err := r.serviceProposal(ctx, cmdBlockStorageService, req)
	if err != nil {
		return err
	}
	req.BlockRevision = result.ServiceBlockRevision
	timer := time.NewTimer(storage.ServiceRevocationWait)
	defer timer.Stop()
	poll := time.NewTicker(proposalApplyPoll)
	defer poll.Stop()
	waiting := true
	for waiting {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
			if !r.IsLeader() || r.raft.Term() != term {
				return serrors.ErrNoLeaderYet
			}
		case <-timer.C:
			waiting = false
		}
	}
	if !r.IsLeader() || r.raft.Term() != term {
		return serrors.ErrNoLeaderYet
	}
	_, err = r.serviceProposal(ctx, cmdConfirmStorageServiceRevoked, req)
	return err
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

func (r *RaftNodeImpl) GetNodeStorageDirectory(ctx context.Context, nodeID int64) (storage.Directory, error) {
	if err := ctx.Err(); err != nil {
		return storage.Directory{}, err
	}
	r.sm.mu.RLock()
	defer r.sm.mu.RUnlock()
	d := r.sm.directory.ForNode(nodeID)
	for kb := range d.Placements {
		d.ActiveVersions[kb] = r.sm.kbs[kb].ActiveVersionID
	}
	return d, nil
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
