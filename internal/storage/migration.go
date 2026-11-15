package storage

import (
	"context"
	"slices"
	"sort"

	serrors "stratum/internal/errors"
)

type MigrationPhase string

const (
	MigrationDraining MigrationPhase = "draining"
	MigrationFencing  MigrationPhase = "fencing"
	// Fenced means prepared, NOT copied or switched. No effective replica set
	// changes until a separate, verified data-transfer protocol is implemented.
	MigrationFenced MigrationPhase = "fenced"
	// Staged means the target holds a verified, isolated artifact. It is still
	// not a replica: ordinary data operations remain refused.
	MigrationStaged MigrationPhase = "staged"
	// Installed means the target wrote that artifact into its own ordinary stores
	// and re-verified the result. Effective membership still does not change, and
	// no ordinary write is admitted until a separate activation commits it.
	MigrationInstalled MigrationPhase = "installed"
	MigrationCancelled MigrationPhase = "cancelled"
	MigrationActivated MigrationPhase = "activated"
)

type MigrationRequest struct {
	Fault         bool   `json:"fault,omitempty"`
	OperationID   string `json:"operation_id"`
	KBID          string `json:"kb_id"`
	ExpectedEpoch uint64 `json:"expected_epoch"`
	FromID        int64  `json:"from_id"`
	ToID          int64  `json:"to_id"`
}

type Migration struct {
	Stage         *MigrationStageReceipt   `json:"stage,omitempty"`
	Install       *MigrationInstallReceipt `json:"install,omitempty"`
	Serve         *MigrationServeReceipt   `json:"serve,omitempty"`
	FailedBlock   ServiceBlock             `json:"failed_block,omitempty"`
	Request       MigrationRequest         `json:"request"`
	Old           Placement                `json:"old"`
	NewReplicaIDs []int64                  `json:"new_replica_ids"`
	Phase         MigrationPhase           `json:"phase"`
	FencedIDs     []int64                  `json:"fenced_ids"`
}

func (m Migration) Clone() Migration {
	if m.Stage != nil {
		receipt := *m.Stage
		m.Stage = &receipt
	}
	if m.Install != nil {
		receipt := *m.Install
		m.Install = &receipt
	}
	if m.Serve != nil {
		receipt := *m.Serve
		m.Serve = &receipt
	}
	m.Old = m.Old.Clone()
	m.NewReplicaIDs = slices.Clone(m.NewReplicaIDs)
	m.FencedIDs = slices.Clone(m.FencedIDs)
	return m
}

func (m Migration) Active() bool {
	return m.Phase != MigrationCancelled && m.Phase != MigrationActivated
}
func (m Migration) BlocksDataWrites() bool {
	return m.Phase == MigrationFencing || m.Phase == MigrationFenced || m.Phase == MigrationStaged || m.Phase == MigrationInstalled
}

func (m Migration) FenceIDs() []int64 {
	ids := append(slices.Clone(m.Old.ReplicaIDs), m.Request.ToID)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return slices.Compact(ids)
}

// FenceIDs remains the complete resource/cleanup union. Only RequiredFenceIDs
// excludes the failed member, and only with a committed revocation proof.
func (m Migration) RequiredFenceIDs() []int64 {
	ids := m.FenceIDs()
	if m.Request.Fault && m.FailedBlock.Revoked {
		return slices.DeleteFunc(ids, func(id int64) bool { return id == m.Request.FromID })
	}
	return ids
}
func (d Directory) ValidateFailedBlock(m Migration) error {
	if !m.Request.Fault {
		return nil
	}
	current, ok := d.ServiceBlocks[m.Request.FromID]
	if !ok || current.OperationID != m.FailedBlock.OperationID || current.Revision != m.FailedBlock.Revision {
		return serrors.ErrPlacementStale
	}
	if !current.Revoked || !m.FailedBlock.Revoked {
		return serrors.ErrMigrationBusy
	}
	return nil
}

func (d Directory) ActiveMigration(kbID string) (Migration, bool) {
	for _, m := range d.Migrations {
		if m.Request.KBID == kbID && m.Active() {
			return m.Clone(), true
		}
	}
	return Migration{}, false
}

func (d *Directory) BeginMigration(req MigrationRequest) error {
	if req.OperationID == "" || req.KBID == "" || req.ExpectedEpoch == 0 || req.ExpectedEpoch == ^uint64(0) || req.FromID <= 0 || req.ToID <= 0 || req.FromID == req.ToID {
		return serrors.ErrInvalidArgument
	}
	if previous, ok := d.Migrations[req.OperationID]; ok {
		if previous.Request == req {
			return nil
		}
		return serrors.ErrMigrationConflict
	}
	p, err := d.Lookup(req.KBID)
	if err != nil {
		return err
	}
	if p.Epoch != req.ExpectedEpoch {
		return serrors.ErrPlacementStale
	}
	if d.HasPendingCleanup(req.KBID) {
		return serrors.ErrMigrationInProgress
	}
	if _, ok := d.ActiveMigration(req.KBID); ok {
		return serrors.ErrMigrationInProgress
	}
	if !p.Contains(req.FromID) || p.Contains(req.ToID) {
		return serrors.ErrInvalidArgument
	}
	target, ok := d.Nodes[req.ToID]
	_, blocked := d.ServiceBlocks[req.ToID]
	if blocked || !ok || target.State != NodeActive || target.Addr == "" {
		return serrors.ErrInvalidArgument
	}
	next := slices.Clone(p.ReplicaIDs)
	for i, id := range next {
		if id == req.FromID {
			next[i] = req.ToID
		}
	}
	sort.Slice(next, func(i, j int) bool { return next[i] < next[j] })
	var failedBlock ServiceBlock
	if req.Fault {
		if _, ok := d.Nodes[req.FromID]; !ok {
			return serrors.ErrPlacementUnknown
		}
		if _, ok := d.ServiceBlocks[req.FromID]; !ok {
			if err := d.BlockService(ServiceRequest{NodeID: req.FromID, OperationID: "fault/" + req.OperationID, ExpectedRevision: d.Revision}); err != nil {
				return err
			}
		}
		failedBlock = d.ServiceBlocks[req.FromID]
		if failedBlock.OperationID == "" || failedBlock.Revision == 0 {
			return serrors.ErrPlacementUnknown
		}
	}
	if d.Migrations == nil {
		d.Migrations = make(map[string]Migration)
	}
	d.Migrations[req.OperationID] = Migration{Request: req, Old: p.Clone(), NewReplicaIDs: next, Phase: MigrationDraining, FailedBlock: failedBlock}
	d.Revision++
	return nil
}

func (d *Directory) StartFencing(operationID string) error {
	m, ok := d.Migrations[operationID]
	if !ok {
		return serrors.ErrMigrationNotFound
	}
	if m.Phase == MigrationFencing || m.Phase == MigrationFenced {
		return nil
	}
	if m.Phase != MigrationDraining {
		return serrors.ErrMigrationConflict
	}
	if err := d.ValidateFailedBlock(m); err != nil {
		return err
	}
	p, err := d.Lookup(m.Request.KBID)
	if err != nil {
		return err
	}
	if !equalPlacement(p, m.Old) {
		return serrors.ErrPlacementStale
	}
	m.Phase = MigrationFencing
	d.Migrations[operationID] = m
	d.Revision++
	return nil
}

func (d *Directory) AckFence(operationID string, epoch uint64, nodeID int64) error {
	m, ok := d.Migrations[operationID]
	if !ok {
		return serrors.ErrMigrationNotFound
	}
	if err := d.ValidateFailedBlock(m); err != nil {
		return err
	}
	if epoch != m.Request.ExpectedEpoch {
		return serrors.ErrPlacementStale
	}
	if m.Phase != MigrationFencing && m.Phase != MigrationFenced {
		return serrors.ErrMigrationConflict
	}
	if !slices.Contains(m.RequiredFenceIDs(), nodeID) {
		return serrors.ErrInvalidArgument
	}
	if slices.Contains(m.FencedIDs, nodeID) {
		return nil
	}
	m.FencedIDs = append(slices.Clone(m.FencedIDs), nodeID)
	sort.Slice(m.FencedIDs, func(i, j int) bool { return m.FencedIDs[i] < m.FencedIDs[j] })
	if len(m.FencedIDs) == len(m.RequiredFenceIDs()) {
		m.Phase = MigrationFenced
	}
	d.Migrations[operationID] = m
	d.Revision++
	return nil
}

// Cancel keeps membership unchanged. Once fencing may have reached a disk,
// resuming uses a NEW epoch so an acknowledged old-epoch fence stays valid.
func (d *Directory) CancelMigration(operationID string) error {
	m, ok := d.Migrations[operationID]
	if !ok {
		return serrors.ErrMigrationNotFound
	}
	if m.Phase == MigrationCancelled {
		return nil
	}
	p, err := d.Lookup(m.Request.KBID)
	if err != nil {
		return err
	}
	if !equalPlacement(p, m.Old) {
		return serrors.ErrPlacementStale
	}
	if m.Phase != MigrationDraining {
		if p.Epoch == ^uint64(0) {
			return serrors.ErrMigrationConflict
		}
		p.Epoch++
		p.OperationID = "cancel/" + operationID
		d.Placements[p.KBID] = p
	}
	if m.Phase == MigrationActivated {
		return serrors.ErrMigrationConflict
	}
	d.migrationCleanups(m, true)
	m.Phase = MigrationCancelled
	d.Migrations[operationID] = m
	d.Revision++
	return nil
}

type MigrationManager interface {
	Reader
	ListKBMigrations(context.Context) ([]Migration, error)
	ProposeBeginKBMigration(context.Context, MigrationRequest) error
	ProposeFenceKBMigration(context.Context, string) error
	ProposeAckKBMigrationFence(context.Context, string, uint64, int64) error
	ProposeCancelKBMigration(context.Context, string) error
	ProposeRecordKBMigrationStage(context.Context, string, MigrationStageReceipt) error
	ProposeRecordKBMigrationInstall(context.Context, string, MigrationInstallReceipt) error
	ProposeRecordKBMigrationServe(context.Context, string, MigrationServeReceipt) error
}
