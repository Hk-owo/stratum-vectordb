package storage

import (
	"errors"
	"reflect"
	"testing"

	serrors "stratum/internal/errors"
)

// rulesDirectory is the state a real migration starts from: three active
// nodes, KB "kb" on {1,2}, so node 3 is the only legal target.
func rulesDirectory(t *testing.T) Directory {
	t.Helper()
	d := NewDirectory()
	if err := d.RegisterNodes(0, []Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}}); err != nil {
		t.Fatal(err)
	}
	// A partial set on purpose: node 3 is then the only legal migration target.
	if err := d.AssignNew(Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"}); err != nil {
		t.Fatal(err)
	}
	return d
}

func migrationRequest() MigrationRequest {
	return MigrationRequest{OperationID: "op-1", KBID: "kb", ExpectedEpoch: 1, FromID: 1, ToID: 3}
}

// BeginMigration is the only way a task comes into existence, so every one of its
// refusals is a membership guarantee. A request that slips past these would move
// a replica set without a migration, which is exactly what the directory exists
// to prevent.
func TestBeginMigration_RefusesEveryUnsafeRequest(t *testing.T) {
	valid := migrationRequest()
	cases := []struct {
		name   string
		mutate func(d *Directory)
		req    func(r MigrationRequest) MigrationRequest
		want   error
	}{
		{name: "no operation id", req: func(r MigrationRequest) MigrationRequest { r.OperationID = ""; return r }, want: serrors.ErrInvalidArgument},
		{name: "no knowledge base", req: func(r MigrationRequest) MigrationRequest { r.KBID = ""; return r }, want: serrors.ErrInvalidArgument},
		{name: "epoch zero", req: func(r MigrationRequest) MigrationRequest { r.ExpectedEpoch = 0; return r }, want: serrors.ErrInvalidArgument},
		{name: "epoch max", req: func(r MigrationRequest) MigrationRequest { r.ExpectedEpoch = ^uint64(0); return r }, want: serrors.ErrInvalidArgument},
		{name: "no source", req: func(r MigrationRequest) MigrationRequest { r.FromID = 0; return r }, want: serrors.ErrInvalidArgument},
		{name: "no target", req: func(r MigrationRequest) MigrationRequest { r.ToID = 0; return r }, want: serrors.ErrInvalidArgument},
		{name: "source equals target", req: func(r MigrationRequest) MigrationRequest { r.ToID = r.FromID; return r }, want: serrors.ErrInvalidArgument},
		{
			name: "knowledge base is gone",
			mutate: func(d *Directory) {
				d.Delete("kb")
			},
			want: serrors.ErrKnowledgeBaseDeleted,
		},
		{
			name:   "placement is unknown",
			mutate: func(d *Directory) { delete(d.Placements, "kb") },
			want:   serrors.ErrPlacementUnknown,
		},
		{name: "epoch is stale", req: func(r MigrationRequest) MigrationRequest { r.ExpectedEpoch = 2; return r }, want: serrors.ErrPlacementStale},
		{
			name:   "an earlier cleanup is still pending",
			mutate: func(d *Directory) { d.addCleanup(Cleanup{ID: "c1", KBID: "kb", NodeID: 1}) },
			want:   serrors.ErrMigrationInProgress,
		},
		{
			name: "another migration is already active",
			mutate: func(d *Directory) {
				d.Migrations = map[string]Migration{"other": {Request: MigrationRequest{OperationID: "other", KBID: "kb"}, Phase: MigrationFencing}}
			},
			want: serrors.ErrMigrationInProgress,
		},
		{name: "source is not a member", req: func(r MigrationRequest) MigrationRequest { r.FromID = 3; return r }, want: serrors.ErrInvalidArgument},
		{name: "target is already a member", req: func(r MigrationRequest) MigrationRequest { r.ToID = 2; return r }, want: serrors.ErrInvalidArgument},
		{
			name: "target is not registered",
			req:  func(r MigrationRequest) MigrationRequest { r.ToID = 9; return r },
			want: serrors.ErrInvalidArgument,
		},
		{
			name: "target is draining",
			mutate: func(d *Directory) {
				d.Nodes[3] = Node{ID: 3, Addr: "s3", State: NodeDraining}
			},
			want: serrors.ErrInvalidArgument,
		},
		{
			name:   "target has no address",
			mutate: func(d *Directory) { d.Nodes[3] = Node{ID: 3, State: NodeActive} },
			want:   serrors.ErrInvalidArgument,
		},
		{
			name: "target is service blocked",
			mutate: func(d *Directory) {
				d.ServiceBlocks = map[int64]ServiceBlock{3: {OperationID: "blocked", Revision: 1}}
			},
			want: serrors.ErrInvalidArgument,
		},
		{
			name: "fault mode whose source is not registered",
			req:  func(r MigrationRequest) MigrationRequest { r.Fault = true; return r },
			mutate: func(d *Directory) {
				delete(d.Nodes, 1)
			},
			want: serrors.ErrPlacementUnknown,
		},
		{
			// The block exists but carries no operation: it is not a proof of
			// anything, so the task cannot be created from it.
			name: "fault mode with a block that proves nothing",
			req:  func(r MigrationRequest) MigrationRequest { r.Fault = true; return r },
			mutate: func(d *Directory) {
				d.ServiceBlocks = map[int64]ServiceBlock{1: {}}
			},
			want: serrors.ErrPlacementUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := rulesDirectory(t)
			if tc.mutate != nil {
				tc.mutate(&d)
			}
			req := valid
			if tc.req != nil {
				req = tc.req(req)
			}
			before := d.Clone()
			err := d.BeginMigration(req)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if !reflect.DeepEqual(before, d) {
				t.Errorf("a refused request changed the directory: %+v", d)
			}
		})
	}
}

func TestBeginMigration_RecordsTheTaskAndItsFenceSet(t *testing.T) {
	d := rulesDirectory(t)
	revision := d.Revision
	if err := d.BeginMigration(migrationRequest()); err != nil {
		t.Fatalf("begin: %v", err)
	}
	m, ok := d.Migrations["op-1"]
	if !ok {
		t.Fatal("task was not recorded")
	}
	if m.Phase != MigrationDraining {
		t.Errorf("phase = %q, want draining: nothing may move before the fence", m.Phase)
	}
	if !reflect.DeepEqual(m.Old.ReplicaIDs, []int64{1, 2}) {
		t.Errorf("old placement = %+v, want the frozen original", m.Old)
	}
	if !reflect.DeepEqual(m.NewReplicaIDs, []int64{2, 3}) {
		t.Errorf("new replica set = %v, want the source replaced and the set sorted", m.NewReplicaIDs)
	}
	if !reflect.DeepEqual(d.Placements["kb"].ReplicaIDs, []int64{1, 2}) {
		t.Error("beginning a migration changed the effective membership")
	}
	if d.Revision != revision+1 {
		t.Errorf("revision = %d, want %d", d.Revision, revision+1)
	}

	// The same request again is a lost-response retry, not a second migration.
	revision = d.Revision
	if err := d.BeginMigration(migrationRequest()); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if d.Revision != revision {
		t.Error("retry bumped the revision")
	}
	// A different request under a used operation id is a conflict, never a
	// silent reinterpretation of an operation somebody may already have acted on.
	other := migrationRequest()
	other.ToID = 2
	if err := d.BeginMigration(other); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatalf("reused operation id = %v, want ErrMigrationConflict", err)
	}
}

// Fault mode blocks the failed member's service as part of beginning the task:
// the revocation proof has to exist before anything else waits on it.
func TestBeginMigration_FaultModeBlocksTheSourceService(t *testing.T) {
	d := rulesDirectory(t)
	req := migrationRequest()
	req.Fault = true
	if err := d.BeginMigration(req); err != nil {
		t.Fatalf("begin: %v", err)
	}
	block, ok := d.ServiceBlocks[1]
	if !ok {
		t.Fatal("the failed member's service was not blocked")
	}
	if block.OperationID != "fault/op-1" || block.Revoked {
		t.Errorf("block = %+v, want a pending fault block", block)
	}
	m := d.Migrations["op-1"]
	if m.FailedBlock != block {
		t.Errorf("failed block = %+v, want the one that was just committed (%+v)", m.FailedBlock, block)
	}
	// The failed member stays in the fence set until the revocation is actually
	// committed: "the node is broken" is not yet "the node may be skipped".
	if !reflect.DeepEqual(m.RequiredFenceIDs(), []int64{1, 2, 3}) {
		t.Errorf("required fence set = %v, want the whole union while the revocation is pending", m.RequiredFenceIDs())
	}
	m.FailedBlock.Revoked = true
	if !reflect.DeepEqual(m.RequiredFenceIDs(), []int64{2, 3}) {
		t.Errorf("required fence set after revocation = %v, want the failed member dropped", m.RequiredFenceIDs())
	}
}

func TestStartFencing_OnlyDrainingTasksWithAnUnchangedPlacement(t *testing.T) {
	base := func(t *testing.T) Directory {
		t.Helper()
		d := rulesDirectory(t)
		if err := d.BeginMigration(migrationRequest()); err != nil {
			t.Fatal(err)
		}
		return d
	}

	if err := (&Directory{}).StartFencing("missing"); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown operation = %v", err)
	}

	alreadyFenced := base(t)
	m := alreadyFenced.Migrations["op-1"]
	m.Phase = MigrationFenced
	alreadyFenced.Migrations["op-1"] = m
	revision := alreadyFenced.Revision
	if err := alreadyFenced.StartFencing("op-1"); err != nil || alreadyFenced.Revision != revision {
		t.Errorf("already fenced = %v (revision %d)", err, alreadyFenced.Revision)
	}

	installed := base(t)
	m = installed.Migrations["op-1"]
	m.Phase = MigrationInstalled
	installed.Migrations["op-1"] = m
	if err := installed.StartFencing("op-1"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("installed task = %v, want ErrMigrationConflict", err)
	}

	faulted := rulesDirectory(t)
	faultReq := migrationRequest()
	faultReq.Fault = true
	if err := faulted.BeginMigration(faultReq); err != nil {
		t.Fatal(err)
	}
	if err := faulted.StartFencing("op-1"); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Errorf("fault without revocation = %v, want ErrMigrationBusy", err)
	}

	moved := base(t)
	p := moved.Placements["kb"]
	p.Epoch = 5
	moved.Placements["kb"] = p
	if err := moved.StartFencing("op-1"); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("placement moved = %v, want ErrPlacementStale", err)
	}

	gone := base(t)
	delete(gone.Placements, "kb")
	if err := gone.StartFencing("op-1"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("placement gone = %v, want ErrPlacementUnknown", err)
	}

	happy := base(t)
	revision = happy.Revision
	if err := happy.StartFencing("op-1"); err != nil {
		t.Fatalf("fence: %v", err)
	}
	if happy.Migrations["op-1"].Phase != MigrationFencing || happy.Revision != revision+1 {
		t.Errorf("fencing was not recorded: %+v", happy.Migrations["op-1"])
	}
}

func TestAckFence_RequiresTheRightEpochAndMember(t *testing.T) {
	fencing := func(t *testing.T) Directory {
		t.Helper()
		d := rulesDirectory(t)
		if err := d.BeginMigration(migrationRequest()); err != nil {
			t.Fatal(err)
		}
		if err := d.StartFencing("op-1"); err != nil {
			t.Fatal(err)
		}
		return d
	}

	if err := (&Directory{}).AckFence("missing", 1, 1); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown operation = %v", err)
	}

	staleEpoch := fencing(t)
	if err := staleEpoch.AckFence("op-1", 2, 1); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("wrong epoch = %v, want ErrPlacementStale", err)
	}

	notFencing := rulesDirectory(t)
	if err := notFencing.BeginMigration(migrationRequest()); err != nil {
		t.Fatal(err)
	}
	if err := notFencing.AckFence("op-1", 1, 1); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("draining task = %v, want ErrMigrationConflict", err)
	}

	outsider := fencing(t)
	if err := outsider.AckFence("op-1", 1, 9); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Errorf("non-member ack = %v, want ErrInvalidArgument", err)
	}

	// Both members have to acknowledge before the set counts as fenced, and a
	// repeated ack for the same node must not advance anything.
	d := fencing(t)
	if err := d.AckFence("op-1", 1, 1); err != nil {
		t.Fatalf("first ack: %v", err)
	}
	if m := d.Migrations["op-1"]; m.Phase != MigrationFencing || !reflect.DeepEqual(m.FencedIDs, []int64{1}) {
		t.Fatalf("after first ack: %+v", m)
	}
	revision := d.Revision
	if err := d.AckFence("op-1", 1, 1); err != nil || d.Revision != revision {
		t.Errorf("repeated ack = %v (revision %d)", err, d.Revision)
	}
	// The set is the whole union of the old members and the target, so it takes
	// a third acknowledgement before the task counts as fenced.
	if err := d.AckFence("op-1", 1, 2); err != nil {
		t.Fatalf("second ack: %v", err)
	}
	if m := d.Migrations["op-1"]; m.Phase != MigrationFencing {
		t.Fatalf("fenced before every member acknowledged: %+v", m)
	}
	if err := d.AckFence("op-1", 1, 3); err != nil {
		t.Fatalf("third ack: %v", err)
	}
	if m := d.Migrations["op-1"]; m.Phase != MigrationFenced || !reflect.DeepEqual(m.FencedIDs, []int64{1, 2, 3}) {
		t.Errorf("after third ack: %+v", m)
	}
}

// Cancel never leaves the effective membership changed, and after a fence it
// bumps the epoch so a node that acknowledged the old fence stays fenced.
func TestCancelMigration_KeepsMembershipAndAdvancesTheEpoch(t *testing.T) {
	draining := func(t *testing.T) Directory {
		t.Helper()
		d := rulesDirectory(t)
		if err := d.BeginMigration(migrationRequest()); err != nil {
			t.Fatal(err)
		}
		return d
	}

	if err := (&Directory{}).CancelMigration("missing"); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown operation = %v", err)
	}

	d := draining(t)
	before := d.Placements["kb"]
	if err := d.CancelMigration("op-1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if m := d.Migrations["op-1"]; m.Phase != MigrationCancelled {
		t.Errorf("phase = %q, want cancelled", m.Phase)
	}
	if !equalPlacement(before, d.Placements["kb"]) {
		t.Errorf("cancelling a draining task changed the placement: %+v", d.Placements["kb"])
	}
	revision := d.Revision
	if err := d.CancelMigration("op-1"); err != nil || d.Revision != revision {
		t.Errorf("repeated cancel = %v (revision %d)", err, d.Revision)
	}

	// Cancelling after the fence is not a rollback: the epoch moves on.
	fenced := draining(t)
	if err := fenced.StartFencing("op-1"); err != nil {
		t.Fatal(err)
	}
	if err := fenced.AckFence("op-1", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := fenced.AckFence("op-1", 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := fenced.AckFence("op-1", 1, 3); err != nil {
		t.Fatal(err)
	}
	if err := fenced.CancelMigration("op-1"); err != nil {
		t.Fatalf("cancel fenced: %v", err)
	}
	p := fenced.Placements["kb"]
	if p.Epoch != 2 || p.OperationID != "cancel/op-1" {
		t.Errorf("placement after cancelled fence = %+v, want epoch 2 under the cancel operation", p)
	}
	if !reflect.DeepEqual(p.ReplicaIDs, before.ReplicaIDs) {
		t.Errorf("replica set = %v, want the untouched original %v", p.ReplicaIDs, before.ReplicaIDs)
	}

	// A cancelled fence on an exhausted epoch cannot be resumed safely.
	exhausted := draining(t)
	if err := exhausted.StartFencing("op-1"); err != nil {
		t.Fatal(err)
	}
	maxed := exhausted.Placements["kb"]
	maxed.Epoch = ^uint64(0)
	exhausted.Placements["kb"] = maxed
	// Keep the frozen original in step so the placement check is what fails.
	m := exhausted.Migrations["op-1"]
	m.Old = maxed
	exhausted.Migrations["op-1"] = m
	if err := exhausted.CancelMigration("op-1"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("cancelled on a maxed epoch = %v, want ErrMigrationConflict", err)
	}

	// Neither can an activated task be cancelled: those bytes are a replica now.
	activated := draining(t)
	m = activated.Migrations["op-1"]
	m.Phase = MigrationActivated
	activated.Migrations["op-1"] = m
	if err := activated.CancelMigration("op-1"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("cancel activated = %v, want ErrMigrationConflict", err)
	}

	lost := draining(t)
	delete(lost.Placements, "kb")
	if err := lost.CancelMigration("op-1"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("placement gone = %v, want ErrPlacementUnknown", err)
	}

	moved := draining(t)
	p = moved.Placements["kb"]
	p.Epoch = 7
	moved.Placements["kb"] = p
	if err := moved.CancelMigration("op-1"); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("placement moved = %v, want ErrPlacementStale", err)
	}
}

// ActiveMigration answers "is somebody already moving this KB", so only tasks the
// directory still considers in flight may match.
func TestActiveMigration_IgnoresSettledTasks(t *testing.T) {
	d := rulesDirectory(t)
	if _, ok := d.ActiveMigration("kb"); ok {
		t.Fatal("a directory with no migrations reported one")
	}
	for phase, active := range map[MigrationPhase]bool{
		MigrationDraining:  true,
		MigrationFencing:   true,
		MigrationStaged:    true,
		MigrationCancelled: false,
		MigrationActivated: false,
	} {
		d.Migrations = map[string]Migration{"op": {Request: MigrationRequest{OperationID: "op", KBID: "kb"}, Phase: phase}}
		got, ok := d.ActiveMigration("kb")
		if ok != active {
			t.Errorf("phase %q: active = %v, want %v", phase, ok, active)
		}
		if ok && got.Request.OperationID != "op" {
			t.Errorf("phase %q: got %+v", phase, got)
		}
	}
	// The answer is a copy: a caller mutating it must not reach the directory.
	d.Migrations = map[string]Migration{"op": {Request: MigrationRequest{OperationID: "op", KBID: "kb"}, Phase: MigrationFencing, NewReplicaIDs: []int64{2, 3}}}
	got, _ := d.ActiveMigration("kb")
	got.NewReplicaIDs[0] = 99
	if d.Migrations["op"].NewReplicaIDs[0] != 2 {
		t.Error("ActiveMigration handed out the live slice")
	}
}
