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

func (impl *RaftNodeImpl) ProposeCreateKBWithPlacement(ctx context.Context, kb types.KnowledgeBaseMeta, ids []int64) error {
	res, err := impl.proposeAndWait(ctx, newCreateKBWithPlacementCommand(kb, ids))
	if err != nil {
		return err
	}
	return res.Err
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

func (impl *RaftNodeImpl) ProposeRegisterStorageNodes(ctx context.Context, revision uint64, nodes []storage.Node) error {
	res, err := impl.proposeAndWait(ctx, registerStorageNodesCommand(revision, nodes))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeAssignKBPlacement(ctx context.Context, p storage.Placement) error {
	res, err := impl.proposeAndWait(ctx, assignKBPlacementCommand(p))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) StorageDirectory(ctx context.Context) (storage.Directory, error) {
	if err := ctx.Err(); err != nil {
		return storage.Directory{}, err
	}
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	d := impl.sm.directory.Clone()
	d.ActiveVersions = make(map[string]int64, len(d.Placements))
	for id := range d.Placements {
		d.ActiveVersions[id] = impl.sm.kbs[id].ActiveVersionID
	}
	return d, nil
}

func (impl *RaftNodeImpl) GetKBPlacement(ctx context.Context, kbID string) (storage.Snapshot, error) {
	if kbID == "" {
		return storage.Snapshot{}, serrors.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return storage.Snapshot{}, err
	}
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	if _, deleted := impl.sm.directory.Tombstones[kbID]; deleted {
		return storage.Snapshot{}, serrors.ErrKnowledgeBaseDeleted
	}
	kb, ok := impl.sm.kbs[kbID]
	if !ok {
		return storage.Snapshot{}, serrors.ErrKnowledgeBaseNotFound
	}
	if kb.Status != types.KBStatusActive {
		return storage.Snapshot{}, serrors.ErrKnowledgeBaseDeleted
	}
	snap, err := impl.sm.directory.Resolve(kbID)
	snap.ActiveVersion = kb.ActiveVersionID
	return snap, err
}

// StorageDirectoryRevision is O(1); periodic heartbeats never scan the directory.
func (impl *RaftNodeImpl) StorageDirectoryRevision() uint64 {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	return impl.sm.directory.Revision
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
