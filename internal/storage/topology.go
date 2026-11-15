// Package storage defines the storage-management contract. Directory state is
// authoritative only after a control Raft commit; heartbeats never mutate it.
package storage

import (
	"context"
	"fmt"
	"sort"

	serrors "stratum/internal/errors"
)

type NodeState string

const (
	NodeActive   NodeState = "active"
	NodeDraining NodeState = "draining"
	NodeRetired  NodeState = "retired"
)

// Node.ID is stable across address changes and process restarts.
type Node struct {
	ID    int64     `json:"id"`
	Addr  string    `json:"addr"`
	State NodeState `json:"state"`
}

type NodeRegistration struct {
	Nodes []Node `json:"nodes"`
}

// Health is advisory leader-local state, never a Raft durability proof.
type Health struct {
	Known       bool
	Degraded    bool
	Unavailable bool
	Detail      string
}

// Placement is one indivisible configuration. RequiredReplicas is a policy,
// never a count of nodes which happened to report. KBID is the internal KB
// identity, not its user-visible name.
type Placement struct {
	KBID             string  `json:"kb_id"`
	Epoch            uint64  `json:"epoch"`
	ReplicaIDs       []int64 `json:"replica_ids"`
	RequiredReplicas int     `json:"required_replicas"`
	OperationID      string  `json:"operation_id"`
}

func (p Placement) Clone() Placement {
	p.ReplicaIDs = append([]int64(nil), p.ReplicaIDs...)
	return p
}

func (p Placement) Contains(id int64) bool {
	for _, member := range p.ReplicaIDs {
		if member == id {
			return true
		}
	}
	return false
}

func (p Placement) Quorum() int { return p.RequiredReplicas/2 + 1 }

// Tombstone has no TTL. Deleting business metadata does not remove the fence
// or forget which physical replicas still require cleanup.
type Tombstone struct {
	KBID       string  `json:"kb_id"`
	Epoch      uint64  `json:"epoch"`
	CleanupIDs []int64 `json:"cleanup_ids"`
}

// Directory is a detached snapshot. Revision changes for placements, nodes,
// and tombstones, independently of the version-chain generation.
type Directory struct {
	Cleanups       map[string]Cleanup     `json:"cleanups,omitempty"`
	ServiceBlocks  map[int64]ServiceBlock `json:"service_blocks,omitempty"`
	Migrations     map[string]Migration   `json:"migrations,omitempty"`
	ActiveVersions map[string]int64       `json:"active_versions,omitempty"`
	Revision       uint64                 `json:"revision"`
	Nodes          map[int64]Node         `json:"nodes"`
	Placements     map[string]Placement   `json:"placements"`
	Tombstones     map[string]Tombstone   `json:"tombstones"`
}

func NewDirectory() Directory {
	return Directory{Nodes: make(map[int64]Node), Placements: make(map[string]Placement), Tombstones: make(map[string]Tombstone)}
}

func (d Directory) Clone() Directory {
	out := NewDirectory()
	out.Revision = d.Revision
	if d.Cleanups != nil {
		out.Cleanups = make(map[string]Cleanup, len(d.Cleanups))
		for id, c := range d.Cleanups {
			out.Cleanups[id] = c
		}
	}
	if d.ServiceBlocks != nil {
		out.ServiceBlocks = make(map[int64]ServiceBlock, len(d.ServiceBlocks))
		for id, b := range d.ServiceBlocks {
			out.ServiceBlocks[id] = b
		}
	}
	if d.Migrations != nil {
		out.Migrations = make(map[string]Migration, len(d.Migrations))
		for op, m := range d.Migrations {
			out.Migrations[op] = m.Clone()
		}
	}
	if d.ActiveVersions != nil {
		out.ActiveVersions = make(map[string]int64, len(d.ActiveVersions))
		for kb, v := range d.ActiveVersions {
			out.ActiveVersions[kb] = v
		}
	}
	for id, node := range d.Nodes {
		out.Nodes[id] = node
	}
	for id, p := range d.Placements {
		out.Placements[id] = p.Clone()
	}
	for id, t := range d.Tombstones {
		t.CleanupIDs = append([]int64(nil), t.CleanupIDs...)
		out.Tombstones[id] = t
	}
	return out
}

// Lookup distinguishes deleted identity, unknown placement, and a valid
// single-replica placement. The Raft adapter distinguishes an unknown KB from
// a known KB which has not yet been registered.
func (d Directory) Lookup(kbID string) (Placement, error) {
	if _, deleted := d.Tombstones[kbID]; deleted {
		return Placement{}, serrors.ErrKnowledgeBaseDeleted
	}
	p, ok := d.Placements[kbID]
	if !ok {
		return Placement{}, serrors.ErrPlacementUnknown
	}
	return p.Clone(), nil
}

// Resolve binds addresses and policy from the SAME snapshot. A missing member
// is an error, never permission to reduce the quorum denominator.
func (d Directory) Resolve(kbID string) (Snapshot, error) {
	p, err := d.Lookup(kbID)
	if err != nil {
		return Snapshot{}, err
	}
	if p.KBID != kbID || p.Epoch == 0 || p.RequiredReplicas < 1 || len(p.ReplicaIDs) != p.RequiredReplicas {
		return Snapshot{}, serrors.ErrPlacementUnknown
	}
	out := Snapshot{Revision: d.Revision, Placement: p, ActiveVersion: d.ActiveVersions[kbID]}
	if m, ok := d.ActiveMigration(kbID); ok {
		out.Migration = &m
	}
	seen := make(map[int64]bool, len(p.ReplicaIDs))
	seenAddr := make(map[string]bool, len(p.ReplicaIDs))
	for _, id := range p.ReplicaIDs {
		n, ok := d.Nodes[id]
		if !ok || id <= 0 || n.ID != id || n.Addr == "" || seen[id] || seenAddr[n.Addr] || (n.State != NodeActive && n.State != NodeDraining) {
			return Snapshot{}, serrors.ErrPlacementUnknown
		}
		seen[id] = true
		seenAddr[n.Addr] = true
		out.Nodes = append(out.Nodes, n)
	}
	return out, nil
}

type Snapshot struct {
	Migration     *Migration
	ActiveVersion int64
	Revision      uint64
	Placement     Placement
	Nodes         []Node
}

// Reader and Manager are the control/storage boundary. Raft-backed adapters
// implement these without adding another consensus group.
type Reader interface {
	StorageDirectory(context.Context) (Directory, error)
	GetKBPlacement(context.Context, string) (Snapshot, error)
}

type Manager interface {
	Reader
	ProposeRegisterStorageNodes(context.Context, uint64, []Node) error
	ProposeAssignKBPlacement(context.Context, Placement) error
}

// RegisterNodes adds available nodes or updates management state with CAS.
// Omission never removes a node; adding one never changes an existing KB.
// Address changes remain fenced until endpoint service authorization is available.
func (d *Directory) RegisterNodes(expected uint64, nodes []Node) error {
	if len(nodes) == 0 {
		return serrors.ErrInvalidArgument
	}
	seenID, seenAddr := map[int64]bool{}, map[string]bool{}
	changed := false
	for i := range nodes {
		n := nodes[i]
		if n.State == "" {
			n.State = NodeActive
		}
		if n.ID <= 0 || n.Addr == "" || seenID[n.ID] || seenAddr[n.Addr] || (n.State != NodeActive && n.State != NodeDraining) {
			return serrors.ErrInvalidArgument
		}
		seenID[n.ID], seenAddr[n.Addr] = true, true
		if n.State == NodeDraining {
			for _, m := range d.Migrations {
				if m.Active() && m.Request.ToID == n.ID {
					return serrors.ErrMigrationInProgress
				}
			}
		}
		for id, old := range d.Nodes {
			if id != n.ID && old.Addr == n.Addr {
				return serrors.ErrInvalidArgument
			}
		}
		if old, ok := d.Nodes[n.ID]; ok && old.State == NodeRetired {
			return serrors.ErrPlacementChangeUnsafe
		}
		if old, ok := d.Nodes[n.ID]; ok && old.Addr != n.Addr {
			return serrors.ErrPlacementChangeUnsafe
		}
		if d.Nodes[n.ID] != n {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if expected != d.Revision {
		return serrors.ErrPlacementStale
	}
	if d.Nodes == nil {
		d.Nodes = make(map[int64]Node)
	}
	for _, n := range nodes {
		if n.State == "" {
			n.State = NodeActive
		}
		d.Nodes[n.ID] = n
	}
	// A KB deleted before the legacy topology was registered still needs the
	// original full cleanup set once it becomes known.
	for id, t := range d.Tombstones {
		if len(t.CleanupIDs) == 0 {
			t.CleanupIDs = d.NodeIDs()
			d.Tombstones[id] = t
		}
	}
	d.Revision++
	return nil
}

// AssignInitial registers a legacy KB's original FULL replica set. A new KB's
// partial set uses AssignNew atomically with creation. Neither path changes an
// existing placement: that still requires a migration.
func (d *Directory) AssignInitial(p Placement) error {
	return d.assign(p, true)
}

// AssignNew is only used atomically with creation of a new internal KB
// identity. Existing identities still need a migration to change membership.
func (d *Directory) AssignNew(p Placement) error { return d.assign(p, false) }

func (d *Directory) assign(p Placement, requireAll bool) error {
	if p.KBID == "" || p.OperationID == "" || p.Epoch != 1 || p.RequiredReplicas < 1 || len(p.ReplicaIDs) != p.RequiredReplicas {
		return serrors.ErrInvalidArgument
	}
	if _, deleted := d.Tombstones[p.KBID]; deleted {
		return serrors.ErrKnowledgeBaseDeleted
	}
	p = p.Clone()
	sort.Slice(p.ReplicaIDs, func(i, j int) bool { return p.ReplicaIDs[i] < p.ReplicaIDs[j] })
	for i, id := range p.ReplicaIDs {
		if id <= 0 || (i > 0 && p.ReplicaIDs[i-1] == id) {
			return serrors.ErrInvalidArgument
		}
	}
	if old, ok := d.Placements[p.KBID]; ok {
		if equalPlacement(old, p) {
			return nil
		}
		return serrors.ErrPlacementChangeUnsafe
	}
	if requireAll && len(p.ReplicaIDs) != len(d.Nodes) {
		return serrors.ErrPlacementChangeUnsafe
	}
	for _, id := range p.ReplicaIDs {
		n, ok := d.Nodes[id]
		_, blocked := d.ServiceBlocks[id]
		if !ok || n.State != NodeActive || blocked {
			return fmt.Errorf("%w: storage node %d is not active", serrors.ErrInvalidArgument, id)
		}
	}
	if d.Placements == nil {
		d.Placements = make(map[string]Placement)
	}
	d.Placements[p.KBID] = p
	d.Revision++
	return nil
}

func equalPlacement(a, b Placement) bool {
	if a.KBID != b.KBID || a.Epoch != b.Epoch || a.RequiredReplicas != b.RequiredReplicas || a.OperationID != b.OperationID || len(a.ReplicaIDs) != len(b.ReplicaIDs) {
		return false
	}
	for i := range a.ReplicaIDs {
		if a.ReplicaIDs[i] != b.ReplicaIDs[i] {
			return false
		}
	}
	return true
}

func (d Directory) NodeIDs() []int64 {
	ids := make([]int64, 0, len(d.Nodes))
	for id := range d.Nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (d Directory) InitialPlacement(kbID, operationID string) Placement {
	ids := make([]int64, 0, len(d.Nodes))
	for _, id := range d.NodeIDs() {
		if _, blocked := d.ServiceBlocks[id]; d.Nodes[id].State == NodeActive && !blocked {
			ids = append(ids, id)
		}
	}
	return Placement{KBID: kbID, Epoch: 1, ReplicaIDs: ids, RequiredReplicas: len(ids), OperationID: operationID}
}

// Delete fences an internal identity before any cleanup RPC can run. Repeated
// calls preserve the original cleanup targets and epoch.
func (d *Directory) Delete(kbID string) {
	if _, ok := d.Tombstones[kbID]; ok {
		return
	}
	if d.Tombstones == nil {
		d.Tombstones = make(map[string]Tombstone)
	}
	p := d.Placements[kbID]
	if len(p.ReplicaIDs) == 0 {
		p.ReplicaIDs = d.NodeIDs()
	}
	d.Tombstones[kbID] = Tombstone{KBID: kbID, Epoch: p.Epoch + 1, CleanupIDs: append([]int64(nil), p.ReplicaIDs...)}
	for _, id := range p.ReplicaIDs {
		cid := fmt.Sprintf("delete/%s/%d", kbID, id)
		d.addCleanup(Cleanup{ID: cid, KBID: kbID, NodeID: id, Epoch: p.Epoch + 1})
	}
	delete(d.Placements, kbID)
	d.Revision++
}
