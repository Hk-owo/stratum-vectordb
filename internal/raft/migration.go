package raft

import (
	"context"
	"sort"

	"go.uber.org/zap"
	"stratum/internal/storage"
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
