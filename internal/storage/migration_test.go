package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"

	serrors "stratum/internal/errors"
)

func migrationDirectory(t *testing.T) (Directory, MigrationRequest) {
	t.Helper()
	d := NewDirectory()
	if err := d.RegisterNodes(0, []Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}, {ID: 4, Addr: "s4"}, {ID: 5, Addr: "s5"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.AssignNew(Placement{KBID: "A", Epoch: 1, ReplicaIDs: []int64{1, 2, 3}, RequiredReplicas: 3, OperationID: "create/A"}); err != nil {
		t.Fatal(err)
	}
	return d, MigrationRequest{OperationID: "move-A", KBID: "A", ExpectedEpoch: 1, FromID: 3, ToID: 4}
}

func TestMigrationPreparationIsIdempotentAndNeverActivatesLearner(t *testing.T) {
	d, req := migrationDirectory(t)
	before := d.Placements["A"].Clone()
	if err := d.BeginMigration(req); err != nil {
		t.Fatal(err)
	}
	rev := d.Revision
	if err := d.BeginMigration(req); err != nil || d.Revision != rev {
		t.Fatal("lost begin ACK retry:", err)
	}
	other := req
	other.ToID = 5
	if err := d.BeginMigration(other); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal(err)
	}
	other.OperationID = "another"
	if err := d.BeginMigration(other); !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal(err)
	}
	if err := d.StartFencing(req.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := d.AckFence(req.OperationID, 9, 1); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Fatal(err)
	}
	if err := d.AckFence(req.OperationID, 1, 5); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2, 3, 4} {
		if err := d.AckFence(req.OperationID, 1, id); err != nil {
			t.Fatal(err)
		}
	}
	rev = d.Revision
	if err := d.AckFence(req.OperationID, 1, 1); err != nil || d.Revision != rev {
		t.Fatal("duplicate node ACK counted twice:", err)
	}
	if d.Migrations[req.OperationID].Phase != MigrationFenced {
		t.Fatal("required nodes were not all fenced")
	}
	if !reflect.DeepEqual(d.Placements["A"], before) {
		t.Fatal("preparation changed effective configuration")
	}
	s, err := d.Resolve("A")
	if err != nil {
		t.Fatal(err)
	}
	if s.Placement.Contains(4) || len(s.Nodes) != 3 || s.Placement.Quorum() != 2 {
		t.Fatal("learner became query/quorum candidate")
	}
}

func TestMigrationCancellationPreservesFencesAndOldMembership(t *testing.T) {
	d, req := migrationDirectory(t)
	if err := d.BeginMigration(req); err != nil {
		t.Fatal(err)
	}
	if err := d.StartFencing(req.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := d.AckFence(req.OperationID, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := d.CancelMigration(req.OperationID); err != nil {
		t.Fatal(err)
	}
	s, err := d.Resolve("A")
	if err != nil {
		t.Fatal(err)
	}
	if s.Migration != nil || s.Placement.Epoch != 2 || !reflect.DeepEqual(s.Placement.ReplicaIDs, []int64{1, 2, 3}) {
		t.Fatal("cancel removed the epoch fence or changed members:", s)
	}
	if err := d.AckFence(req.OperationID, 1, 2); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("late ACK revived cancelled task:", err)
	}
	rev := d.Revision
	if err := d.CancelMigration(req.OperationID); err != nil || d.Revision != rev {
		t.Fatal("cancel retry changed epoch twice:", err)
	}
	next := req
	next.OperationID = "next"
	if err := d.BeginMigration(next); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Fatal(err)
	}
	next.ExpectedEpoch = 2
	for id := range d.Cleanups {
		if err := d.AckCleanup(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.BeginMigration(next); err != nil {
		t.Fatal(err)
	}
	if err := d.CancelMigration(req.OperationID); err != nil {
		t.Fatal(err)
	}
	if m, ok := d.ActiveMigration("A"); !ok || m.Request.OperationID != "next" {
		t.Fatal("old cancellation cleared new task")
	}
}

func TestMigrationDrainCancelDoesNotNeedlesslyBumpEpoch(t *testing.T) {
	d, req := migrationDirectory(t)
	if err := d.BeginMigration(req); err != nil {
		t.Fatal(err)
	}
	if err := d.CancelMigration(req.OperationID); err != nil {
		t.Fatal(err)
	}
	if d.Placements["A"].Epoch != 1 {
		t.Fatal("unfenced preparation changed epoch")
	}
}

func TestMigrationSnapshotsAreDetached(t *testing.T) {
	d, req := migrationDirectory(t)
	if err := d.BeginMigration(req); err != nil {
		t.Fatal(err)
	}
	clone := d.Clone()
	clone.Migrations[req.OperationID].Old.ReplicaIDs[0] = 99
	clone.Migrations[req.OperationID].NewReplicaIDs[0] = 98
	s, err := d.Resolve("A")
	if err != nil {
		t.Fatal(err)
	}
	ctx := Bind(context.Background(), s)
	s.Migration.Old.ReplicaIDs[0] = 97
	bound, _ := Bound(ctx, "A")
	bound.Migration.NewReplicaIDs[0] = 96
	boundAgain, _ := Bound(ctx, "A")
	if d.Migrations[req.OperationID].Old.ReplicaIDs[0] != 1 || boundAgain.Migration.Old.ReplicaIDs[0] != 1 || boundAgain.Migration.NewReplicaIDs[0] != 1 {
		t.Fatal("migration snapshot aliases live state")
	}
}
