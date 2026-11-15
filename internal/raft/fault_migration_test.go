package raft

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/wire"
)

func TestFaultMigrationSchedulerUsesDurableRevocationWithoutFakeFailedAck(t *testing.T) {
	r, req := migrationNode(t)
	req.Fault = true
	ctx := context.Background()
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	revoked := 0
	calls := map[int64]int{}
	scheduler := &storage.FenceScheduler{Manager: r, IsLeader: func() bool { return true }, Versions: r.ListVersions, Revoke: func(c context.Context, node int64, b storage.ServiceBlock) error {
		revoked++
		if deadline, ok := c.Deadline(); !ok || time.Until(deadline) <= storage.ServiceRevocationWait {
			t.Fatal("ordinary RPC budget cut off revocation wait")
		}
		// The unit fake represents the end of a leader's full wait. Production
		// supplies RaftNodeImpl.WaitStorageServiceRevoked instead.
		if err := r.serviceProposal(c, cmdConfirmStorageServiceRevoked, storage.ServiceRequest{NodeID: node, OperationID: b.OperationID, BlockRevision: b.Revision}); err != nil {
			return err
		}
		return context.DeadlineExceeded // committed proof, lost response
	}, Fence: func(_ context.Context, n storage.Node, m storage.Migration) error {
		calls[n.ID]++
		if n.ID == req.FromID {
			t.Fatal("contacted failed replica")
		}
		return nil
	}}
	if err := scheduler.Step(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := scheduler.Step(ctx); err != nil {
		t.Fatal(err)
	}
	// A replacement leader reuses the COMMITTED proof, not the previous
	// process's timer. Missing/uncommitted proofs always start a new wait.
	scheduler.Revoke = func(context.Context, int64, storage.ServiceBlock) error {
		t.Fatal("repeated committed revocation")
		return nil
	}
	if err := scheduler.Step(ctx); err != nil {
		t.Fatal(err)
	}
	d, _ := r.StorageDirectory(ctx)
	m := d.Migrations[req.OperationID]
	if revoked != 1 || m.Phase != storage.MigrationFenced || !reflect.DeepEqual(m.FencedIDs, []int64{1, 2, 4}) || len(calls) != 3 {
		t.Fatal("wrong preparation result", m, calls, revoked)
	}
	if !d.Placements["A"].Contains(req.FromID) || d.Placements["A"].Contains(req.ToID) {
		t.Fatal("prepared target activated without complete data")
	}
	restored := wire.StorageDirectoryFromProto(wire.StorageDirectoryToProto(d))
	if !reflect.DeepEqual(d, restored) {
		t.Fatal("fault task/proof lost through directory RPC")
	}
}

func TestFaultMigrationSchedulerCannotTrustCallbackWithoutCommittedProof(t *testing.T) {
	r, req := migrationNode(t)
	req.Fault = true
	ctx := context.Background()
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	calls := 0
	s := &storage.FenceScheduler{Manager: r, IsLeader: func() bool { return true }, Versions: r.ListVersions, Revoke: func(context.Context, int64, storage.ServiceBlock) error { calls++; return nil }, Fence: func(context.Context, storage.Node, storage.Migration) error {
		t.Fatal("uncommitted proof used")
		return nil
	}}
	for i := 0; i < 3; i++ {
		if err := s.Step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	d, _ := r.StorageDirectory(ctx)
	if calls != 3 || d.Migrations[req.OperationID].Phase != storage.MigrationDraining || d.ServiceBlocks[req.FromID].Revoked {
		t.Fatal("local callback authorized member omission")
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Fatal("manual fencing skipped grace", err)
	}
}

func TestFaultMigrationSnapshotRestoresRevocationAndStillBlocksNewVersions(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	apply := func(c command) error { return sm.apply(ctx, c, w, zap.NewNop()).Err }
	if err := apply(registerStorageNodesCommand(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}})); err != nil {
		t.Fatal(err)
	}
	if err := apply(newCreateKBWithPlacementCommand(testKB("A"), []int64{1, 2})); err != nil {
		t.Fatal(err)
	}
	req := storage.MigrationRequest{Fault: true, OperationID: "fault-A", KBID: "A", ExpectedEpoch: 1, FromID: 2, ToID: 3}
	if err := apply(beginMigrationCommand(req)); err != nil {
		t.Fatal(err)
	}
	b := sm.directory.ServiceBlocks[2]
	if err := apply(command{Type: cmdConfirmStorageServiceRevoked, Service: &storage.ServiceRequest{NodeID: 2, OperationID: b.OperationID, BlockRevision: b.Revision}}); err != nil {
		t.Fatal(err)
	}
	if err := apply(fenceMigrationCommand(req.OperationID)); err != nil {
		t.Fatal(err)
	}
	if err := apply(ackFenceCommand(req.OperationID, 1, 1)); err != nil {
		t.Fatal(err)
	}
	data, err := sm.serialize()
	if err != nil {
		t.Fatal(err)
	}
	recovered := newStateMachine()
	if err := recovered.restore(data); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sm.directory, recovered.directory) {
		t.Fatal("snapshot lost fault block/proof")
	}
	if err := recovered.apply(ctx, ackFenceCommand(req.OperationID, 1, 3), w, zap.NewNop()).Err; err != nil {
		t.Fatal(err)
	}
	if recovered.directory.Migrations[req.OperationID].Phase != storage.MigrationFenced {
		t.Fatal("recovered preparation did not finish")
	}
	if err := recovered.apply(ctx, newCreateVersionCommand("A", 0, ""), w, zap.NewNop()).Err; !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal("recovery forgot write barrier", err)
	}
}

func TestFaultMigrationSchedulerSharesRevocationAcrossSamePassKBs(t *testing.T) {
	r, req := migrationNode(t)
	req.Fault = true
	ctx := context.Background()
	if err := r.ProposeCreateKBWithPlacement(ctx, testKB("B"), []int64{2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	next := req
	next.KBID = "B"
	next.OperationID = req.OperationID + "-B"
	if err := r.ProposeBeginKBMigration(ctx, next); err != nil {
		t.Fatal(err)
	}
	waits := 0
	s := &storage.FenceScheduler{Manager: r, IsLeader: func() bool { return true }, Versions: r.ListVersions, Fence: func(context.Context, storage.Node, storage.Migration) error { return nil }, Revoke: func(c context.Context, id int64, b storage.ServiceBlock) error {
		waits++
		return r.serviceProposal(c, cmdConfirmStorageServiceRevoked, storage.ServiceRequest{NodeID: id, OperationID: b.OperationID, BlockRevision: b.Revision})
	}}
	for i := 0; i < 3; i++ {
		if err := s.Step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	d, _ := r.StorageDirectory(ctx)
	if waits != 1 || d.Migrations[req.OperationID].Phase != storage.MigrationFenced || d.Migrations[next.OperationID].Phase != storage.MigrationFenced {
		t.Fatal("repeated node wait for each KB", waits, d.Migrations)
	}
}
