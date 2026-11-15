package raft

import (
	"context"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
)

var _ storage.Manager = (*MockRaftNode)(nil)

func (r *MockRaftNode) StorageDirectoryRevision() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.directory.Revision
}

func (r *MockRaftNode) ProposeCreateKBWithPlacement(ctx context.Context, kb types.KnowledgeBaseMeta, ids []int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.kbs[kb.KBID]; exists {
		return serrors.ErrPlacementChangeUnsafe
	}
	if len(r.directory.Nodes) == 0 {
		return serrors.ErrPlacementUnknown
	}
	c := newCreateKBWithPlacementCommand(kb, ids)
	return r.createKBLocked(kb, c.Placement)
}

func (r *MockRaftNode) ProposeRegisterStorageNodes(ctx context.Context, revision uint64, nodes []storage.Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.directory.RegisterNodes(revision, nodes)
}

func (r *MockRaftNode) ProposeAssignKBPlacement(ctx context.Context, p storage.Placement) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, deleted := r.directory.Tombstones[p.KBID]; deleted {
		return serrors.ErrKnowledgeBaseDeleted
	}
	kb, ok := r.kbs[p.KBID]
	if !ok {
		return serrors.ErrKnowledgeBaseNotFound
	}
	if kb.Status != types.KBStatusActive {
		return serrors.ErrKnowledgeBaseDeleted
	}
	return r.directory.AssignInitial(p)
}

func (r *MockRaftNode) StorageDirectory(ctx context.Context) (storage.Directory, error) {
	if err := ctx.Err(); err != nil {
		return storage.Directory{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.directory.Clone()
	d.ActiveVersions = make(map[string]int64, len(d.Placements))
	for id := range d.Placements {
		d.ActiveVersions[id] = r.kbs[id].ActiveVersionID
	}
	return d, nil
}

func (r *MockRaftNode) GetKBPlacement(ctx context.Context, kbID string) (storage.Snapshot, error) {
	if kbID == "" {
		return storage.Snapshot{}, serrors.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return storage.Snapshot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, deleted := r.directory.Tombstones[kbID]; deleted {
		return storage.Snapshot{}, serrors.ErrKnowledgeBaseDeleted
	}
	kb, ok := r.kbs[kbID]
	if !ok {
		return storage.Snapshot{}, serrors.ErrKnowledgeBaseNotFound
	}
	if kb.Status != types.KBStatusActive {
		return storage.Snapshot{}, serrors.ErrKnowledgeBaseDeleted
	}
	snap, err := r.directory.Resolve(kbID)
	snap.ActiveVersion = kb.ActiveVersionID
	return snap, err
}
