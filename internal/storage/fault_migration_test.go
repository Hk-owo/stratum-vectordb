package storage

import (
	"errors"
	"reflect"
	serrors "stratum/internal/errors"
	"testing"
)

func TestFaultMigrationBlocksNodeButCannotSkipAckUntilRevocationCommits(t *testing.T) {
	d, req := migrationDirectory(t)
	req.Fault = true
	before := d.Placements[req.KBID].Clone()
	if err := d.BeginMigration(req); err != nil {
		t.Fatal(err)
	}
	m := d.Migrations[req.OperationID]
	b := d.ServiceBlocks[req.FromID]
	if b.OperationID == "" || b.Revision == 0 || b.Revoked || m.FailedBlock != b {
		t.Fatal("missing committed block generation")
	}
	if err := d.AuthorizeService(ServiceRequest{NodeID: req.FromID, Address: "s3"}); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Fatal(err)
	}
	if err := d.StartFencing(req.OperationID); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Fatal("skipped authorization grace", err)
	}
	if !reflect.DeepEqual(m.RequiredFenceIDs(), []int64{1, 2, 3, 4}) {
		t.Fatal("unproved failure shrank requirements")
	}
	request := ServiceRequest{NodeID: 3, OperationID: b.OperationID, BlockRevision: b.Revision + 1}
	if err := d.ConfirmServiceRevoked(request); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Fatal("wrong generation proof accepted", err)
	}
	request.BlockRevision = b.Revision
	if err := d.ConfirmServiceRevoked(request); err != nil {
		t.Fatal(err)
	}
	revision := d.Revision
	if err := d.ConfirmServiceRevoked(request); err != nil || d.Revision != revision {
		t.Fatal("proof retry was not idempotent", err)
	}
	m = d.Migrations[req.OperationID]
	if !reflect.DeepEqual(m.RequiredFenceIDs(), []int64{1, 2, 4}) || !reflect.DeepEqual(m.FenceIDs(), []int64{1, 2, 3, 4}) {
		t.Fatal("cleanup union lost failed node")
	}
	if err := d.StartFencing(req.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := d.AckFence(req.OperationID, 1, 3); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Fatal("accepted fake failed-node ACK", err)
	}
	for _, id := range []int64{1, 2, 4} {
		if err := d.AckFence(req.OperationID, 1, id); err != nil {
			t.Fatal(err)
		}
	}
	m = d.Migrations[req.OperationID]
	if m.Phase != MigrationFenced || !reflect.DeepEqual(m.FencedIDs, []int64{1, 2, 4}) {
		t.Fatal("survivors did not prepare", m)
	}
	if !equalPlacement(d.Placements[req.KBID], before) {
		t.Fatal("preparation activated empty target")
	}
	request.ExpectedRevision = d.Revision
	if err := d.ResumeService(request); !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal("active task lost its service block", err)
	}
	if err := d.CancelMigration(req.OperationID); err != nil {
		t.Fatal(err)
	}
	if !d.ServiceBlocks[3].Revoked {
		t.Fatal("KB cancellation silently reopened failed node")
	}
	if d.Placements[req.KBID].Epoch != 2 || d.Placements[req.KBID].Contains(4) {
		t.Fatal("cancel changed membership or reused fenced epoch")
	}
}
func TestFaultMigrationReusesOneNodeRevocationAcrossKBsAndSurvivesClone(t *testing.T) {
	d, req := migrationDirectory(t)
	req.Fault = true
	if err := d.AssignNew(Placement{KBID: "B", Epoch: 1, ReplicaIDs: []int64{1, 3}, RequiredReplicas: 2, OperationID: "create/B"}); err != nil {
		t.Fatal(err)
	}
	if err := d.BeginMigration(req); err != nil {
		t.Fatal(err)
	}
	b := d.ServiceBlocks[3]
	if err := d.ConfirmServiceRevoked(ServiceRequest{NodeID: 3, OperationID: b.OperationID, BlockRevision: b.Revision}); err != nil {
		t.Fatal(err)
	}
	next := req
	next.OperationID = "replace-B"
	next.KBID = "B"
	next.ToID = 5
	if err := d.BeginMigration(next); err != nil {
		t.Fatal(err)
	}
	if d.Migrations[next.OperationID].FailedBlock != d.ServiceBlocks[3] || !d.Migrations[next.OperationID].FailedBlock.Revoked {
		t.Fatal("durable proof was lost across tasks")
	}
	clone := d.Clone()
	m := clone.Migrations[next.OperationID]
	m.FailedBlock.Revoked = false
	clone.Migrations[next.OperationID] = m
	if !d.Migrations[next.OperationID].FailedBlock.Revoked {
		t.Fatal("snapshot aliases proof")
	}
	normal := req
	normal.Fault = false
	if err := d.BeginMigration(normal); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("operation ID reused with changed mode", err)
	}
}
func TestFaultMigrationInvalidTargetDoesNotSuspendUnrelatedNode(t *testing.T) {
	d, req := migrationDirectory(t)
	req.Fault = true
	req.ToID = 99
	before := d.Clone()
	if err := d.BeginMigration(req); err == nil {
		t.Fatal("unknown target accepted")
	}
	if !reflect.DeepEqual(before, d) {
		t.Fatal("failed begin changed global service authorization")
	}
}

func TestFaultMigrationNewFailureCannotReusePriorRevocation(t *testing.T) {
	d, req := migrationDirectory(t)
	req.Fault = true
	if err := d.BeginMigration(req); err != nil {
		t.Fatal(err)
	}
	b := d.ServiceBlocks[3]
	proof := ServiceRequest{NodeID: 3, OperationID: b.OperationID, BlockRevision: b.Revision}
	if err := d.ConfirmServiceRevoked(proof); err != nil {
		t.Fatal(err)
	}
	if err := d.CancelMigration(req.OperationID); err != nil {
		t.Fatal(err)
	}
	proof.ExpectedRevision = d.Revision
	if err := d.ResumeService(proof); err != nil {
		t.Fatal(err)
	}
	for id := range d.Cleanups {
		if err := d.AckCleanup(id); err != nil {
			t.Fatal(err)
		}
	}
	req.OperationID = "second-failure"
	if err := d.BeginMigration(req); err != nil {
		t.Fatal(err)
	}
	current := d.ServiceBlocks[3]
	if current.Revision == b.Revision || current.Revoked || d.Migrations[req.OperationID].FailedBlock.Revoked {
		t.Fatal("new failure inherited an old grant-expiry proof")
	}
	if err := d.ConfirmServiceRevoked(proof); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Fatal("old confirmation marked new failure safe", err)
	}
	if err := d.StartFencing(req.OperationID); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Fatal("new failure skipped wait", err)
	}
}
