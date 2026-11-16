package storage

import (
	"errors"
	"reflect"
	"testing"

	serrors "stratum/internal/errors"
)

// Node membership changes are the operations an operator can get wrong, so every
// refusal here is a fence: a retired identity must stay retired, an address must
// not move under a serving endpoint, and a drain must not leave half the
// directory rewritten.
// rulesDirectoryForCutover is the shared fixture: five active nodes with KB "A"
// on {1,2,3}, so node 5 is free and node 4 is a legal migration target.
func rulesDirectoryForCutover(t *testing.T) Directory {
	t.Helper()
	d, _ := migrationDirectory(t)
	return d
}

func TestManageNode_RefusesEveryUnsafeChange(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(d *Directory)
		change func(d Directory) NodeChange
		want   error
	}{
		{
			name: "unknown node",
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 9, ExpectedRevision: d.Revision, Action: "drain"}
			},
			want: serrors.ErrInvalidArgument,
		},
		{
			name:   "retired node asked to retire again",
			mutate: func(d *Directory) { n := d.Nodes[5]; n.State = NodeRetired; d.Nodes[5] = n },
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 5, ExpectedRevision: d.Revision, Action: "retire"}
			},
		},
		{
			name:   "retired node asked to do anything else",
			mutate: func(d *Directory) { n := d.Nodes[5]; n.State = NodeRetired; d.Nodes[5] = n },
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 5, ExpectedRevision: d.Revision, Action: "drain"}
			},
			want: serrors.ErrInvalidArgument,
		},
		{
			name: "address already correct",
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 5, ExpectedRevision: d.Revision, Action: "address", Address: d.Nodes[5].Addr}
			},
		},
		{
			name:   "already draining",
			mutate: func(d *Directory) { n := d.Nodes[5]; n.State = NodeDraining; d.Nodes[5] = n },
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 5, ExpectedRevision: d.Revision, Action: "drain"}
			},
		},
		{
			name: "stale revision",
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 5, ExpectedRevision: d.Revision + 1, Action: "drain"}
			},
			want: serrors.ErrPlacementStale,
		},
		{
			name: "a migration is about to land on this node",
			mutate: func(d *Directory) {
				d.Migrations = map[string]Migration{
					"op": {Request: MigrationRequest{OperationID: "op", KBID: "A", FromID: 1, ToID: 5}, Phase: MigrationStaged},
				}
			},
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 5, ExpectedRevision: d.Revision, Action: "drain"}
			},
			want: serrors.ErrMigrationInProgress,
		},
		{
			name: "address change under a placed replica",
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 1, ExpectedRevision: d.Revision, Action: "address", Address: "moved"}
			},
			want: serrors.ErrMigrationInProgress,
		},
		{
			name: "retire without a revocation proof",
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 5, ExpectedRevision: d.Revision, Action: "retire"}
			},
			want: serrors.ErrStorageServicePaused,
		},
		{
			name: "address change without a revocation proof",
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 5, ExpectedRevision: d.Revision, Action: "address", Address: "moved"}
			},
			want: serrors.ErrStorageServicePaused,
		},
		{
			name: "address change to nothing",
			mutate: func(d *Directory) {
				d.ServiceBlocks = map[int64]ServiceBlock{5: {OperationID: "revoke", Revision: 1, Revoked: true}}
			},
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 5, ExpectedRevision: d.Revision, Action: "address"}
			},
			want: serrors.ErrInvalidArgument,
		},
		{
			name: "address change onto a taken address",
			mutate: func(d *Directory) {
				d.ServiceBlocks = map[int64]ServiceBlock{5: {OperationID: "revoke", Revision: 1, Revoked: true}}
			},
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 5, ExpectedRevision: d.Revision, Action: "address", Address: d.Nodes[1].Addr}
			},
			want: serrors.ErrInvalidArgument,
		},
		{
			name: "unknown action",
			change: func(d Directory) NodeChange {
				return NodeChange{NodeID: 5, ExpectedRevision: d.Revision, Action: "promote"}
			},
			want: serrors.ErrInvalidArgument,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := rulesDirectoryForCutover(t)
			if tc.mutate != nil {
				tc.mutate(&d)
			}
			req := tc.change(d)
			before := d.Clone()
			err := d.ManageNode(req)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("err = %v, want a no-op acceptance", err)
				}
			} else if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if errors.Is(err, serrors.ErrInvalidArgument) || errors.Is(err, serrors.ErrStorageServicePaused) ||
				errors.Is(err, serrors.ErrMigrationInProgress) || errors.Is(err, serrors.ErrPlacementStale) {
				if !reflect.DeepEqual(before, d) {
					t.Error("a refused change still mutated the directory")
				}
			}
		})
	}

	d := rulesDirectoryForCutover(t)
	revision := d.Revision
	if err := d.ManageNode(NodeChange{NodeID: 5, ExpectedRevision: revision, Action: "drain"}); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if d.Nodes[5].State != NodeDraining || d.Revision != revision+1 {
		t.Errorf("drain not recorded: %+v rev=%d", d.Nodes[5], d.Revision)
	}

	revoked := rulesDirectoryForCutover(t)
	revoked.ServiceBlocks = map[int64]ServiceBlock{5: {OperationID: "revoke", Revision: 1, Revoked: true}}
	if err := revoked.ManageNode(NodeChange{NodeID: 5, ExpectedRevision: revoked.Revision, Action: "retire"}); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if revoked.Nodes[5].State != NodeRetired {
		t.Errorf("state = %q, want retired", revoked.Nodes[5].State)
	}
}

func TestNodeReferenced_CountsPlacementsMigrationsAndCleanups(t *testing.T) {
	d := rulesDirectoryForCutover(t)
	if !d.NodeReferenced(1) {
		t.Error("a placed replica is not referenced")
	}
	if d.NodeReferenced(5) {
		t.Error("an unused node is reported as referenced")
	}
	// An in-flight migration references its source and target even though the
	// target is not a replica yet.
	d.Migrations = map[string]Migration{
		"op": {Request: MigrationRequest{OperationID: "op", KBID: "A", FromID: 3, ToID: 5}, Old: d.Placements["A"], Phase: MigrationFencing},
	}
	if !d.NodeReferenced(5) {
		t.Error("a migration target is not referenced")
	}
	// A settled migration references nothing: its bytes are gone.
	d.Migrations["op"] = Migration{Request: MigrationRequest{OperationID: "op", KBID: "A", FromID: 3, ToID: 5}, Phase: MigrationCancelled}
	if d.NodeReferenced(5) {
		t.Error("a cancelled migration still counts as a reference")
	}
	// Unfinished cleanups do, because somebody still has data there.
	d.addCleanup(Cleanup{ID: "c", KBID: "A", NodeID: 5})
	if !d.NodeReferenced(5) {
		t.Error("a pending cleanup is not a reference")
	}
	if err := d.AckCleanup("c"); err != nil {
		t.Fatal(err)
	}
	if d.NodeReferenced(5) {
		t.Error("a completed cleanup still counts as a reference")
	}
}

func TestAckCleanup_IsIdempotentAndRefusesUnknownIDs(t *testing.T) {
	d := rulesDirectoryForCutover(t)
	if err := d.AckCleanup("missing"); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Errorf("unknown cleanup = %v", err)
	}
	d.addCleanup(Cleanup{ID: "c", KBID: "A", NodeID: 5})
	revision := d.Revision
	if err := d.AckCleanup("c"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if !d.Cleanups["c"].Done || d.Revision != revision+1 {
		t.Errorf("cleanup not recorded: %+v rev=%d", d.Cleanups["c"], d.Revision)
	}
	revision = d.Revision
	if err := d.AckCleanup("c"); err != nil || d.Revision != revision {
		t.Errorf("repeated ack = %v (revision %d)", err, d.Revision)
	}
}

func TestDrainNode_PlansTheWholeNodeOrNothing(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(d *Directory)
		req    func(d Directory) DrainRequest
		want   error
	}{
		{
			name: "no operation id",
			req: func(d Directory) DrainRequest {
				return DrainRequest{NodeID: 1, ExpectedRevision: d.Revision, TargetIDs: []int64{5}}
			},
			want: serrors.ErrInvalidArgument,
		},
		{
			name: "no node",
			req: func(d Directory) DrainRequest {
				return DrainRequest{OperationID: "x", ExpectedRevision: d.Revision, TargetIDs: []int64{5}}
			},
			want: serrors.ErrInvalidArgument,
		},
		{
			name: "no targets",
			req: func(d Directory) DrainRequest {
				return DrainRequest{OperationID: "x", NodeID: 1, ExpectedRevision: d.Revision}
			},
			want: serrors.ErrInvalidArgument,
		},
		{
			name: "unknown node",
			req: func(d Directory) DrainRequest {
				return DrainRequest{OperationID: "x", NodeID: 9, ExpectedRevision: d.Revision, TargetIDs: []int64{5}}
			},
			want: serrors.ErrInvalidArgument,
		},
		{
			name:   "retired node",
			mutate: func(d *Directory) { n := d.Nodes[1]; n.State = NodeRetired; d.Nodes[1] = n },
			req: func(d Directory) DrainRequest {
				return DrainRequest{OperationID: "x", NodeID: 1, ExpectedRevision: d.Revision, TargetIDs: []int64{5}}
			},
			want: serrors.ErrInvalidArgument,
		},
		{
			name: "stale revision",
			req: func(d Directory) DrainRequest {
				return DrainRequest{OperationID: "x", NodeID: 1, ExpectedRevision: d.Revision + 1, TargetIDs: []int64{5}}
			},
			want: serrors.ErrPlacementStale,
		},
		{
			name: "a migration is about to land on this node",
			mutate: func(d *Directory) {
				d.Migrations = map[string]Migration{
					"op": {Request: MigrationRequest{OperationID: "op", KBID: "A", FromID: 3, ToID: 1}, Phase: MigrationStaged},
				}
			},
			req: func(d Directory) DrainRequest {
				return DrainRequest{OperationID: "x", NodeID: 1, ExpectedRevision: d.Revision, TargetIDs: []int64{5}}
			},
			want: serrors.ErrMigrationInProgress,
		},
		{
			name: "no admissible replacement",
			req: func(d Directory) DrainRequest {
				// 4 and 5 are free, but only 1 is offered as a target.
				return DrainRequest{OperationID: "x", NodeID: 3, ExpectedRevision: d.Revision, TargetIDs: []int64{1}}
			},
			want: serrors.ErrPlacementChangeUnsafe,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := rulesDirectoryForCutover(t)
			if tc.mutate != nil {
				tc.mutate(&d)
			}
			before := d.Clone()
			if err := d.DrainNode(tc.req(d)); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if !reflect.DeepEqual(before, d) {
				t.Error("a refused drain still mutated the directory")
			}
		})
	}

	// A drain that would need a replacement for a KB the node does not serve is
	// fine: only the KBs that actually hold data on it are migrated.
	d := rulesDirectoryForCutover(t)
	if err := d.AssignNew(Placement{KBID: "B", Epoch: 1, ReplicaIDs: []int64{5}, RequiredReplicas: 1, OperationID: "create/B"}); err != nil {
		t.Fatal(err)
	}
	if err := d.DrainNode(DrainRequest{OperationID: "x", NodeID: 5, ExpectedRevision: d.Revision, TargetIDs: []int64{4}}); err != nil {
		t.Fatalf("drain node 5: %v", err)
	}
	if d.Nodes[5].State != NodeDraining {
		t.Errorf("state = %q, want draining", d.Nodes[5].State)
	}
	m, ok := d.Migrations["drain/x/B"]
	if !ok {
		t.Fatalf("no drain task for B: %+v", d.Migrations)
	}
	if m.Request.FromID != 5 || m.Request.ToID != 4 || !m.Active() {
		t.Errorf("drain task = %+v", m)
	}

	// A retry with a changed request is a conflict, not a second plan: the
	// identity is what makes a lost-response retry safe.
	changed := DrainRequest{OperationID: "x", NodeID: 5, TargetIDs: []int64{4}, Fault: true}
	if err := d.DrainNode(changed); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("changed retry = %v, want ErrMigrationConflict", err)
	}
}
