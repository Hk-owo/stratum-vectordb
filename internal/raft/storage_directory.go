package raft

import (
	"context"

	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"

	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wire"
)

var _ storage.Manager = (*RaftNodeImpl)(nil)
var _ storage.Manager = (*RemoteRaftNode)(nil)

// Creation with an explicit initial set never reassigns an existing identity.
func newCreateKBWithPlacementCommand(kb types.KnowledgeBaseMeta, ids []int64) command {
	c := newCreateKBCommand(kb)
	c.Placement = &storage.Placement{KBID: kb.KBID, Epoch: 1, ReplicaIDs: append([]int64(nil), ids...), RequiredReplicas: len(ids), OperationID: "create/" + kb.KBID}
	return c
}

func (r *RemoteRaftNode) ProposeCreateKBWithPlacement(ctx context.Context, kb types.KnowledgeBaseMeta, ids []int64) error {
	res, err := r.propose(ctx, newCreateKBWithPlacementCommand(kb, ids))
	if err != nil {
		return err
	}
	return res.Err
}

func registerStorageNodesCommand(revision uint64, nodes []storage.Node) command {
	return command{Type: cmdRegisterStorageNodes, DirectoryRevision: revision, StorageNodes: &storage.NodeRegistration{Nodes: append([]storage.Node(nil), nodes...)}}
}

func assignKBPlacementCommand(p storage.Placement) command {
	p = p.Clone()
	return command{Type: cmdAssignKBPlacement, Placement: &p}
}

func (r *RemoteRaftNode) ProposeRegisterStorageNodes(ctx context.Context, revision uint64, nodes []storage.Node) error {
	res, err := r.propose(ctx, registerStorageNodesCommand(revision, nodes))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) ProposeAssignKBPlacement(ctx context.Context, p storage.Placement) error {
	res, err := r.propose(ctx, assignKBPlacementCommand(p))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) readStorageDirectory(ctx context.Context, kbID string) (storage.Directory, error) {
	var out storage.Directory
	err := r.readAtAnyControl(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := pb.NewInternalServiceClient(conn).GetStorageDirectory(ctx, &pb.GetStorageDirectoryRequest{KnowledgeBaseId: kbID})
		if err != nil {
			return err
		}
		out = wire.StorageDirectoryFromProto(resp)
		return nil
	})
	if reason := serrors.ByName(serrors.ReasonOf(err)); reason != nil {
		return storage.Directory{}, reason
	}
	return out, err
}

func (r *RemoteRaftNode) StorageDirectory(ctx context.Context) (storage.Directory, error) {
	return r.readStorageDirectory(ctx, "")
}

func (r *RemoteRaftNode) GetKBPlacement(ctx context.Context, kbID string) (storage.Snapshot, error) {
	if kbID == "" {
		return storage.Snapshot{}, serrors.ErrInvalidArgument
	}
	d, err := r.readStorageDirectory(ctx, kbID)
	if err != nil {
		return storage.Snapshot{}, err
	}
	return d.Resolve(kbID)
}
