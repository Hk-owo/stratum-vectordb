package raft

import (
	"context"
	"slices"
	"sort"

	"go.uber.org/zap"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
)

var _ storage.MigrationManager = (*RaftNodeImpl)(nil)
var _ storage.MigrationManager = (*RemoteRaftNode)(nil)
var _ storage.MigrationManager = (*MockRaftNode)(nil)

func migrationList(d storage.Directory) []storage.Migration {
	var out []storage.Migration
	for _, m := range d.Migrations {
		out = append(out, m.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Request.OperationID < out[j].Request.OperationID })
	return out
}
func (r *RaftNodeImpl) ListKBMigrations(ctx context.Context) ([]storage.Migration, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.sm.mu.RLock()
	defer r.sm.mu.RUnlock()
	return migrationList(r.sm.directory), nil
}
func (r *RemoteRaftNode) ListKBMigrations(ctx context.Context) ([]storage.Migration, error) {
	d, err := r.StorageDirectory(ctx)
	if err != nil {
		return nil, err
	}
	return migrationList(d), nil
}
func (r *MockRaftNode) ListKBMigrations(ctx context.Context) ([]storage.Migration, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return migrationList(r.directory), nil
}

func (sm *stateMachine) migrationBlocksCommand(c command) bool {
	kbID := c.KBID
	switch c.Type {
	case cmdMarkKBDeleting, cmdMarkKBDeleteFailed, cmdRemoveKBMeta, cmdRollback, cmdMarkVersionDeleting, cmdRemoveVersionMeta:
	case cmdRetryVersion:
		kbID = sm.versions[c.VersionID].KBID
	case cmdMarkVersionFailedPermanent:
		v, exists := sm.versions[c.VersionID]
		if c.FailureSide == types.FailureSideIndex || !exists || v.DataStatus == types.DataStatusFailedPermanent {
			return false
		}
		m, ok := sm.directory.ActiveMigration(kbID)
		return ok && m.BlocksDataWrites()
	case cmdDiscardVersion:
		m, ok := sm.directory.ActiveMigration(kbID)
		return ok && m.BlocksDataWrites()
	default:
		return false
	}
	_, active := sm.directory.ActiveMigration(kbID)
	return active
}

// The caller holds the state-machine lock. This is metadata-only; it never
// performs a storage RPC or waits for node-local work to drain.
func (sm *stateMachine) applyMigration(c command) error {
	switch c.Type {
	case cmdDrainStorageNode:
		if c.Drain == nil {
			return serrors.ErrInvalidArgument
		}
		for kb, p := range sm.directory.Placements {
			if !p.Contains(c.Drain.NodeID) {
				continue
			}
			meta, ok := sm.kbs[kb]
			if !ok || meta.Status != types.KBStatusActive {
				return serrors.ErrKnowledgeBaseDeleted
			}
			for _, id := range sm.versionsByKB[kb] {
				if sm.versions[id].Deleting {
					return serrors.ErrMigrationBusy
				}
			}
		}
		return sm.directory.DrainNode(*c.Drain)
	case cmdAckStorageCleanup:
		return sm.directory.AckCleanup(c.OperationID)
	case cmdManageStorageNode:
		if c.NodeChange == nil {
			return serrors.ErrInvalidArgument
		}
		return sm.directory.ManageNode(*c.NodeChange)
	case cmdActivateKBMigration:
		if c.Activation == nil {
			return serrors.ErrInvalidArgument
		}
		m, ok := sm.directory.Migrations[c.OperationID]
		if !ok {
			return serrors.ErrMigrationNotFound
		}
		var manifest storage.MigrationManifest
		if m.Phase != storage.MigrationActivated {
			_, read, e := sm.migrationManifest(c.OperationID)
			if e != nil {
				return e
			}
			manifest = read
		}
		wasActivated := m.Phase == storage.MigrationActivated
		if err := sm.directory.ActivateMigration(c.OperationID, *c.Activation, manifest); err != nil {
			return err
		}
		if !wasActivated {
			for _, id := range sm.versionsByKB[m.Request.KBID] {
				v := sm.versions[id]
				v.IndexReadyNodes = slices.DeleteFunc(slices.Clone(v.IndexReadyNodes), func(n int64) bool { return n == m.Request.FromID })
				sm.versions[id] = v
			}
		}
		return nil
	case cmdRecordKBMigrationStage:
		if c.StageReceipt == nil {
			return serrors.ErrInvalidArgument
		}
		_, manifest, err := sm.migrationManifest(c.OperationID)
		if err != nil {
			return err
		}
		return sm.directory.RecordMigrationStage(c.OperationID, *c.StageReceipt, manifest)
	case cmdRecordKBMigrationInstall:
		if c.InstallReceipt == nil {
			return serrors.ErrInvalidArgument
		}
		_, manifest, err := sm.migrationManifest(c.OperationID)
		if err != nil {
			return err
		}
		return sm.directory.RecordMigrationInstall(c.OperationID, *c.InstallReceipt, manifest)
	case cmdRecordKBMigrationServe:
		if c.ServeReceipt == nil {
			return serrors.ErrInvalidArgument
		}
		_, manifest, err := sm.migrationManifest(c.OperationID)
		if err != nil {
			return err
		}
		return sm.directory.RecordMigrationServe(c.OperationID, *c.ServeReceipt, manifest)
	case cmdBeginKBMigration:
		if c.Migration == nil {
			return serrors.ErrInvalidArgument
		}
		req := *c.Migration
		if _, exists := sm.directory.Migrations[req.OperationID]; exists {
			return sm.directory.BeginMigration(req)
		}
		kb, ok := sm.kbs[req.KBID]
		if !ok {
			return serrors.ErrKnowledgeBaseNotFound
		}
		if kb.Status != types.KBStatusActive {
			return serrors.ErrKnowledgeBaseDeleted
		}
		for _, id := range sm.versionsByKB[req.KBID] {
			if sm.versions[id].Deleting {
				return serrors.ErrMigrationBusy
			}
		}
		return sm.directory.BeginMigration(req)
	case cmdFenceKBMigration:
		m, ok := sm.directory.Migrations[c.OperationID]
		if !ok {
			return serrors.ErrMigrationNotFound
		}
		if m.Phase == storage.MigrationDraining {
			for _, id := range sm.versionsByKB[m.Request.KBID] {
				v := sm.versions[id]
				if v.Deleting || v.DataStatus == types.DataStatusPending {
					return serrors.ErrMigrationBusy
				}
			}
		}
		return sm.directory.StartFencing(c.OperationID)
	case cmdAckKBMigrationFence:
		return sm.directory.AckFence(c.OperationID, c.PlacementEpoch, c.NodeID)
	case cmdCancelKBMigration:
		return sm.directory.CancelMigration(c.OperationID)
	default:
		return serrors.ErrInvalidArgument
	}
}

func beginMigrationCommand(req storage.MigrationRequest) command {
	return command{Type: cmdBeginKBMigration, Migration: &req}
}
func fenceMigrationCommand(op string) command {
	return command{Type: cmdFenceKBMigration, OperationID: op}
}
func ackFenceCommand(op string, epoch uint64, id int64) command {
	return command{Type: cmdAckKBMigrationFence, OperationID: op, PlacementEpoch: epoch, NodeID: id}
}
func cancelMigrationCommand(op string) command {
	return command{Type: cmdCancelKBMigration, OperationID: op}
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
func (r *RemoteRaftNode) ProposeBeginKBMigration(ctx context.Context, req storage.MigrationRequest) error {
	res, err := r.propose(ctx, beginMigrationCommand(req))
	if err != nil {
		return err
	}
	return res.Err
}
func (r *RemoteRaftNode) ProposeFenceKBMigration(ctx context.Context, op string) error {
	res, err := r.propose(ctx, fenceMigrationCommand(op))
	if err != nil {
		return err
	}
	return res.Err
}
func (r *RemoteRaftNode) ProposeAckKBMigrationFence(ctx context.Context, op string, epoch uint64, id int64) error {
	res, err := r.propose(ctx, ackFenceCommand(op, epoch, id))
	if err != nil {
		return err
	}
	return res.Err
}
func (r *RemoteRaftNode) ProposeCancelKBMigration(ctx context.Context, op string) error {
	res, err := r.propose(ctx, cancelMigrationCommand(op))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *MockRaftNode) applyMigration(ctx context.Context, c command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	sm := &stateMachine{directory: r.directory, kbs: r.kbs, versions: r.versions, versionsByKB: r.versionsByKB}
	err := sm.apply(ctx, c, nil, zap.NewNop()).Err
	r.directory = sm.directory
	return err
}

func (r *MockRaftNode) migrationBlocksLocked(c command) bool {
	return (&stateMachine{directory: r.directory, versions: r.versions}).migrationBlocksCommand(c)
}
func (r *MockRaftNode) ProposeBeginKBMigration(ctx context.Context, req storage.MigrationRequest) error {
	return r.applyMigration(ctx, beginMigrationCommand(req))
}
func (r *MockRaftNode) ProposeFenceKBMigration(ctx context.Context, op string) error {
	return r.applyMigration(ctx, fenceMigrationCommand(op))
}
func (r *MockRaftNode) ProposeAckKBMigrationFence(ctx context.Context, op string, epoch uint64, id int64) error {
	return r.applyMigration(ctx, ackFenceCommand(op, epoch, id))
}
func (r *MockRaftNode) ProposeCancelKBMigration(ctx context.Context, op string) error {
	return r.applyMigration(ctx, cancelMigrationCommand(op))
}

func recordMigrationStageCommand(op string, receipt storage.MigrationStageReceipt) command {
	return command{Type: cmdRecordKBMigrationStage, OperationID: op, StageReceipt: &receipt}
}
func (r *RaftNodeImpl) ProposeRecordKBMigrationStage(ctx context.Context, op string, receipt storage.MigrationStageReceipt) error {
	res, err := r.proposeAndWait(ctx, recordMigrationStageCommand(op, receipt))
	if err != nil {
		return err
	}
	return res.Err
}
func (r *RemoteRaftNode) ProposeRecordKBMigrationStage(ctx context.Context, op string, receipt storage.MigrationStageReceipt) error {
	res, err := r.propose(ctx, recordMigrationStageCommand(op, receipt))
	if err != nil {
		return err
	}
	return res.Err
}
func (r *MockRaftNode) ProposeRecordKBMigrationStage(ctx context.Context, op string, receipt storage.MigrationStageReceipt) error {
	return r.applyMigration(ctx, recordMigrationStageCommand(op, receipt))
}

func recordMigrationInstallCommand(op string, receipt storage.MigrationInstallReceipt) command {
	return command{Type: cmdRecordKBMigrationInstall, OperationID: op, InstallReceipt: &receipt}
}
func (r *RaftNodeImpl) ProposeRecordKBMigrationInstall(ctx context.Context, op string, receipt storage.MigrationInstallReceipt) error {
	res, err := r.proposeAndWait(ctx, recordMigrationInstallCommand(op, receipt))
	if err != nil {
		return err
	}
	return res.Err
}
func (r *RemoteRaftNode) ProposeRecordKBMigrationInstall(ctx context.Context, op string, receipt storage.MigrationInstallReceipt) error {
	res, err := r.propose(ctx, recordMigrationInstallCommand(op, receipt))
	if err != nil {
		return err
	}
	return res.Err
}
func (r *MockRaftNode) ProposeRecordKBMigrationInstall(ctx context.Context, op string, receipt storage.MigrationInstallReceipt) error {
	return r.applyMigration(ctx, recordMigrationInstallCommand(op, receipt))
}

func recordMigrationServeCommand(op string, receipt storage.MigrationServeReceipt) command {
	return command{Type: cmdRecordKBMigrationServe, OperationID: op, ServeReceipt: &receipt}
}
func (r *RaftNodeImpl) ProposeRecordKBMigrationServe(ctx context.Context, op string, receipt storage.MigrationServeReceipt) error {
	res, err := r.proposeAndWait(ctx, recordMigrationServeCommand(op, receipt))
	if err != nil {
		return err
	}
	return res.Err
}
func (r *RemoteRaftNode) ProposeRecordKBMigrationServe(ctx context.Context, op string, receipt storage.MigrationServeReceipt) error {
	res, err := r.propose(ctx, recordMigrationServeCommand(op, receipt))
	if err != nil {
		return err
	}
	return res.Err
}
func (r *MockRaftNode) ProposeRecordKBMigrationServe(ctx context.Context, op string, receipt storage.MigrationServeReceipt) error {
	return r.applyMigration(ctx, recordMigrationServeCommand(op, receipt))
}
