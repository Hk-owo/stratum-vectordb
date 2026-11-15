package storage

import (
	"context"
	"fmt"
	"slices"
	serrors "stratum/internal/errors"
)

// Cleanup is a durable obligation, retained after acknowledgement to reject
// delayed retries. ArtifactsOnly never authorizes deleting a live replica.
type Cleanup struct {
	ID            string `json:"id"`
	KBID          string `json:"kb_id"`
	OperationID   string `json:"operation_id"`
	NodeID        int64  `json:"node_id"`
	Epoch         uint64 `json:"epoch"`
	ArtifactsOnly bool   `json:"artifacts_only"`
	Done          bool   `json:"done"`
}

type MigrationActivation struct {
	Epoch   uint64                  `json:"epoch"`
	Install MigrationInstallReceipt `json:"install"`
	Serve   MigrationServeReceipt   `json:"serve"`
}

type MaintenanceManager interface {
	Reader
	GetStorageMaintenance(context.Context) (Directory, error)
	ProposeActivateKBMigration(context.Context, string, MigrationActivation) error
	ProposeAckStorageCleanup(context.Context, string) error
	ProposeManageStorageNode(context.Context, NodeChange) error
}

func (d *Directory) addCleanup(c Cleanup) {
	if d.Cleanups == nil {
		d.Cleanups = map[string]Cleanup{}
	}
	d.Cleanups[c.ID] = c
}
func (d Directory) HasPendingCleanup(kb string) bool {
	for _, c := range d.Cleanups {
		if c.KBID == kb && !c.Done {
			return true
		}
	}
	return false
}
func (d *Directory) migrationCleanups(m Migration, cancelled bool) {
	// Target always owns stage/install files. On cancellation its formal data
	// must also be removed; after activation those bytes are the new replica.
	d.addCleanup(Cleanup{ID: "migration/" + m.Request.OperationID + "/target", KBID: m.Request.KBID, OperationID: m.Request.OperationID, NodeID: m.Request.ToID, Epoch: m.Old.Epoch, ArtifactsOnly: !cancelled})
	if !cancelled {
		d.addCleanup(Cleanup{ID: "migration/" + m.Request.OperationID + "/source", KBID: m.Request.KBID, OperationID: m.Request.OperationID, NodeID: m.Request.FromID, Epoch: m.Old.Epoch})
	}
}
func (d *Directory) ActivateMigration(op string, a MigrationActivation, manifest MigrationManifest) error {
	m, ok := d.Migrations[op]
	if !ok {
		return serrors.ErrMigrationNotFound
	}
	if m.Phase == MigrationActivated {
		if a.Epoch == m.Old.Epoch && m.Install != nil && m.Serve != nil && *m.Install == a.Install && *m.Serve == a.Serve {
			return nil
		}
		return serrors.ErrMigrationConflict
	}
	if a.Epoch != m.Old.Epoch || m.Old.Epoch == ^uint64(0) || m.Phase != MigrationInstalled || m.Install == nil || m.Serve == nil || *m.Install != a.Install {
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
	// Serving state is checked again by the live target under its import gate.
	// A restart may reconstruct a different (still valid) cursor; validate this
	// fresh receipt against authority rather than requiring a volatile fact to
	// equal its historical value. The install proof must remain identical.
	validation := *d
	validation.Migrations = map[string]Migration{op: m.Clone()}
	candidate := validation.Migrations[op]
	candidate.Serve = nil
	validation.Migrations[op] = candidate
	if err := validation.RecordMigrationServe(op, a.Serve, manifest); err != nil {
		return err
	}
	if err := d.RecordMigrationInstall(op, a.Install, manifest); err != nil {
		return err
	}
	if len(m.NewReplicaIDs) != p.RequiredReplicas {
		return serrors.ErrMigrationConflict
	}
	want := slices.Clone(p.ReplicaIDs)
	for i, id := range want {
		if id == m.Request.FromID {
			want[i] = m.Request.ToID
		}
	}
	slices.Sort(want)
	if !slices.Equal(want, m.NewReplicaIDs) {
		return serrors.ErrMigrationConflict
	}
	for _, id := range m.RequiredFenceIDs() {
		if !slices.Contains(m.FencedIDs, id) {
			return serrors.ErrMigrationBusy
		}
	}
	for _, id := range want {
		n, ok := d.Nodes[id]
		if !ok || n.State == NodeRetired {
			return serrors.ErrPlacementUnknown
		}
		if id == m.Request.ToID && n.State != NodeActive {
			return serrors.ErrMigrationBusy
		}
		if _, blocked := d.ServiceBlocks[id]; blocked {
			return serrors.ErrStorageServicePaused
		}
	}
	p.Epoch++
	p.ReplicaIDs = slices.Clone(want)
	p.OperationID = "activate/" + op
	d.Placements[p.KBID] = p
	freshServe := a.Serve
	m.Serve = &freshServe
	m.Phase = MigrationActivated
	d.Migrations[op] = m
	d.migrationCleanups(m, false)
	d.Revision++
	return nil
}
func (d *Directory) AckCleanup(id string) error {
	c, ok := d.Cleanups[id]
	if !ok {
		return serrors.ErrInvalidArgument
	}
	if c.Done {
		return nil
	}
	c.Done = true
	d.Cleanups[id] = c
	d.Revision++
	return nil
}

type NodeChange struct {
	NodeID           int64  `json:"node_id"`
	ExpectedRevision uint64 `json:"expected_revision"`
	Action           string `json:"action"`
	Address          string `json:"address,omitempty"`
}

func (d Directory) NodeReferenced(id int64) bool {
	for _, p := range d.Placements {
		if p.Contains(id) {
			return true
		}
	}
	for _, m := range d.Migrations {
		if m.Active() && slices.Contains(m.FenceIDs(), id) {
			return true
		}
	}
	for _, c := range d.Cleanups {
		if c.NodeID == id && !c.Done {
			return true
		}
	}
	return false
}
func (d *Directory) ManageNode(req NodeChange) error {
	n, ok := d.Nodes[req.NodeID]
	if !ok {
		return serrors.ErrInvalidArgument
	}
	if n.State == NodeRetired {
		if req.Action == "retire" {
			return nil
		}
		return serrors.ErrInvalidArgument
	}
	if (req.Action == "address" && n.Addr == req.Address) || (req.Action == "drain" && n.State == NodeDraining) {
		return nil
	}
	if req.ExpectedRevision != d.Revision {
		return serrors.ErrPlacementStale
	}
	switch req.Action {
	case "drain":
		for _, m := range d.Migrations {
			if m.Active() && m.Request.ToID == n.ID {
				return serrors.ErrMigrationInProgress
			}
		}
		n.State = NodeDraining
	case "address", "retire":
		if d.NodeReferenced(n.ID) {
			return serrors.ErrMigrationInProgress
		}
		b, ok := d.ServiceBlocks[n.ID]
		if !ok || !b.Revoked {
			return serrors.ErrStorageServicePaused
		}
		if req.Action == "retire" {
			n.State = NodeRetired
		} else {
			if req.Address == "" {
				return serrors.ErrInvalidArgument
			}
			for id, old := range d.Nodes {
				if id != n.ID && old.Addr == req.Address {
					return serrors.ErrInvalidArgument
				}
			}
			n.Addr = req.Address
		}
	default:
		return fmt.Errorf("%w: unknown node action", serrors.ErrInvalidArgument)
	}
	if n != d.Nodes[n.ID] {
		d.Nodes[n.ID] = n
		d.Revision++
	}
	return nil
}

type DrainRequest struct {
	OperationID      string  `json:"operation_id"`
	NodeID           int64   `json:"node_id"`
	ExpectedRevision uint64  `json:"expected_revision"`
	TargetIDs        []int64 `json:"target_ids"`
	Fault            bool    `json:"fault"`
}

// Plan the whole drain on a copy; one inadmissible KB cannot leave a partial
// directory mutation. Existing task identities make retries stable.
func (d *Directory) DrainNode(req DrainRequest) error {
	if req.OperationID == "" || req.NodeID <= 0 || len(req.TargetIDs) == 0 {
		return serrors.ErrInvalidArgument
	}
	n, ok := d.Nodes[req.NodeID]
	if !ok || n.State == NodeRetired {
		return serrors.ErrInvalidArgument
	}
	if n.State != NodeDraining && req.ExpectedRevision != d.Revision {
		return serrors.ErrPlacementStale
	}
	for _, m := range d.Migrations {
		if m.Active() && m.Request.ToID == n.ID {
			return serrors.ErrMigrationInProgress
		}
	}
	copy := d.Clone()
	n.State = NodeDraining
	copy.Nodes[n.ID] = n
	targets := slices.Clone(req.TargetIDs)
	slices.Sort(targets)
	targets = slices.Compact(targets)
	kbs := make([]string, 0, len(copy.Placements))
	for kb := range copy.Placements {
		kbs = append(kbs, kb)
	}
	slices.Sort(kbs)
	for _, kb := range kbs {
		p := copy.Placements[kb]
		if !p.Contains(n.ID) {
			continue
		}
		op := "drain/" + req.OperationID + "/" + kb
		if previous, ok := copy.Migrations[op]; ok {
			if previous.Request.FromID != n.ID || previous.Request.Fault != req.Fault || !previous.Active() {
				return serrors.ErrMigrationConflict
			}
			continue
		}
		target := int64(0)
		for _, id := range targets {
			candidate, ok := copy.Nodes[id]
			_, blocked := copy.ServiceBlocks[id]
			if ok && !blocked && candidate.State == NodeActive && !p.Contains(id) {
				target = id
				break
			}
		}
		if target == 0 {
			return serrors.ErrPlacementChangeUnsafe
		}
		if err := copy.BeginMigration(MigrationRequest{OperationID: op, KBID: kb, ExpectedEpoch: p.Epoch, FromID: n.ID, ToID: target, Fault: req.Fault}); err != nil {
			return err
		}
	}
	// A no-op retry does not manufacture a new revision.
	if d.Nodes[n.ID] != n {
		copy.Revision++
	}
	*d = copy
	return nil
}
