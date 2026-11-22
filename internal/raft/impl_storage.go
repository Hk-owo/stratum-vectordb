package raft

import (
	"context"
	"time"

	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
)

func (impl *RaftNodeImpl) ProposeCreateKBWithPlacement(ctx context.Context, kb types.KnowledgeBaseMeta, ids []int64) error {
	res, err := impl.proposeAndWait(ctx, newCreateKBWithPlacementCommand(kb, ids))
	if err != nil {
		return err
	}
	return res.Err
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

func (r *RaftNodeImpl) ListKBMigrations(ctx context.Context) ([]storage.Migration, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.sm.mu.RLock()
	defer r.sm.mu.RUnlock()
	return migrationList(r.sm.directory), nil
}

func (r *RaftNodeImpl) ProposeBeginKBMigration(ctx context.Context, req storage.MigrationRequest) error {
	res, err := r.proposeAndWait(ctx, beginMigrationCommand(req))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RaftNodeImpl) ProposeFenceKBMigration(ctx context.Context, op string) error {
	res, err := r.proposeAndWait(ctx, fenceMigrationCommand(op))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RaftNodeImpl) ProposeAckKBMigrationFence(ctx context.Context, op string, epoch uint64, id int64) error {
	res, err := r.proposeAndWait(ctx, ackFenceCommand(op, epoch, id))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RaftNodeImpl) ProposeCancelKBMigration(ctx context.Context, op string) error {
	res, err := r.proposeAndWait(ctx, cancelMigrationCommand(op))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RaftNodeImpl) ProposeRecordKBMigrationStage(ctx context.Context, op string, receipt storage.MigrationStageReceipt) error {
	res, err := r.proposeAndWait(ctx, recordMigrationStageCommand(op, receipt))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RaftNodeImpl) ProposeRecordKBMigrationInstall(ctx context.Context, op string, receipt storage.MigrationInstallReceipt) error {
	res, err := r.proposeAndWait(ctx, recordMigrationInstallCommand(op, receipt))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RaftNodeImpl) ProposeRecordKBMigrationServe(ctx context.Context, op string, receipt storage.MigrationServeReceipt) error {
	res, err := r.proposeAndWait(ctx, recordMigrationServeCommand(op, receipt))
	if err != nil {
		return err
	}
	return res.Err
}

// Every destructive maintenance operation reads behind a quorum-confirmed
// barrier. A leader isolated from the quorum cannot authorize stale cleanup.
func (r *RaftNodeImpl) GetStorageMaintenance(ctx context.Context) (storage.Directory, error) {
	if !r.IsLeader() {
		return storage.Directory{}, serrors.ErrNoLeaderYet
	}
	term := r.raft.Term()
	res, err := r.proposeAndWait(ctx, command{Type: cmdGetStorageMaintenance})
	if err != nil {
		return storage.Directory{}, err
	}
	if res.Err != nil {
		return storage.Directory{}, res.Err
	}
	if !res.MigrationRead || !r.IsLeader() || r.raft.Term() != term {
		return storage.Directory{}, serrors.ErrNoLeaderYet
	}
	d, err := r.StorageDirectory(ctx)
	if !r.IsLeader() || r.raft.Term() != term {
		return storage.Directory{}, serrors.ErrNoLeaderYet
	}
	return d, err
}

func (r *RaftNodeImpl) ProposeActivateKBMigration(ctx context.Context, op string, a storage.MigrationActivation) error {
	res, err := r.proposeAndWait(ctx, command{Type: cmdActivateKBMigration, OperationID: op, Activation: &a})
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RaftNodeImpl) ProposeAckStorageCleanup(ctx context.Context, id string) error {
	res, err := r.proposeAndWait(ctx, command{Type: cmdAckStorageCleanup, OperationID: id})
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RaftNodeImpl) ProposeManageStorageNode(ctx context.Context, req storage.NodeChange) error {
	res, err := r.proposeAndWait(ctx, command{Type: cmdManageStorageNode, NodeChange: &req})
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RaftNodeImpl) ProposeDrainStorageNode(ctx context.Context, req storage.DrainRequest) error {
	res, err := r.proposeAndWait(ctx, command{Type: cmdDrainStorageNode, Drain: &req})
	if err != nil {
		return err
	}
	return res.Err
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

func (r *RaftNodeImpl) GetKBMigrationTransfer(ctx context.Context, op string) (storage.MigrationTransferView, error) {
	var zero storage.MigrationTransferView
	if op == "" {
		return zero, serrors.ErrInvalidArgument
	}
	if !r.IsLeader() {
		return zero, serrors.ErrNoLeaderYet
	}
	term := r.raft.Term()
	result, err := r.proposeAndWait(ctx, command{Type: cmdGetKBMigrationTransfer, OperationID: op})
	if err != nil {
		return zero, err
	}
	if result.Err != nil {
		return zero, result.Err
	}
	if !r.IsLeader() || term != r.raft.Term() || !result.MigrationRead {
		return zero, serrors.ErrNoLeaderYet
	}
	r.sm.mu.RLock()
	view, err := r.sm.migrationTransferView(op)
	r.sm.mu.RUnlock()
	if err != nil {
		return zero, err
	}
	if !r.IsLeader() || term != r.raft.Term() {
		return zero, serrors.ErrNoLeaderYet
	}
	return view, nil
}
