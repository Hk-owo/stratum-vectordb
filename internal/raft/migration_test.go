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
	"stratum/internal/types"
	"stratum/internal/wal"
)

func migrationNode(t *testing.T) (*MockRaftNode, storage.MigrationRequest) {
	t.Helper()
	r := NewMockRaftNode(wal.NewMockWAL())
	ctx := context.Background()
	if err := r.ProposeRegisterStorageNodes(ctx, 0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}, {ID: 4, Addr: "s4"}}); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeCreateKBWithPlacement(ctx, testKB("A"), []int64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	return r, storage.MigrationRequest{OperationID: "move", KBID: "A", ExpectedEpoch: 1, FromID: 3, ToID: 4}
}

func TestMigrationRaftAdmissionDrainsDataNotIndexState(t *testing.T) {
	r, req := migrationNode(t)
	ctx := context.Background()
	v, err := r.ProposeCreateVersion(ctx, "A", 0, WithClientRequestID("existing"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	before := r.nextVersionID
	if _, err := r.ProposeCreateVersion(ctx, "A", 0); !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal("new version admitted:", err)
	}
	if r.nextVersionID != before {
		t.Fatal("rejected write consumed version ID")
	}
	if got, err := r.ProposeCreateVersion(ctx, "A", 0, WithClientRequestID("existing")); err != nil || got != v {
		t.Fatal("existing write cannot finish during drain:", got, err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Fatal("pending data bypassed drain:", err)
	}
	if err := r.ProposeUpdateVersionStatus(ctx, v, types.IndexStatusReady, 1); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Fatal("index READY was treated as durable data:", err)
	}
	if err := r.ProposeMarkVersionDataDurable(ctx, v); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	for _, run := range []func() error{
		func() error { return r.ProposeMarkKBDeleting(ctx, "A") },
		func() error { return r.ProposeRemoveKBMeta(ctx, "A") },
		func() error { return r.ProposeRollback(ctx, "A", v) },
		func() error {
			_, err := r.ProposeMarkVersionDeleting(ctx, "A", v, types.VersionDeleteSubtree)
			return err
		},
		func() error { return r.ProposeRemoveVersionMeta(ctx, "A", v) },
	} {
		if err := run(); !errors.Is(err, serrors.ErrMigrationInProgress) {
			t.Fatal("lifecycle mutation raced migration:", err)
		}
	}
	if err := r.ProposeCancelKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	s, err := r.GetKBPlacement(ctx, "A")
	if err != nil || s.Placement.Epoch != 2 || s.Migration != nil {
		t.Fatalf("cancel did not resume new epoch: %+v %v", s, err)
	}
}

func TestMigrationDrainCanDiscardAnAbandonedPendingVersion(t *testing.T) {
	r, req := migrationNode(t)
	ctx := context.Background()
	v, err := r.ProposeCreateVersion(ctx, "A", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeDiscardVersion(ctx, "A", v); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationSnapshotRetainsTaskBarrierAndAcknowledgements(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	apply := func(c command) error { return sm.apply(ctx, c, w, zap.NewNop()).Err }
	if err := apply(registerStorageNodesCommand(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}, {ID: 4, Addr: "s4"}})); err != nil {
		t.Fatal(err)
	}
	if err := apply(newCreateKBWithPlacementCommand(testKB("A"), []int64{1, 2, 3})); err != nil {
		t.Fatal(err)
	}
	req := storage.MigrationRequest{OperationID: "move", KBID: "A", ExpectedEpoch: 1, FromID: 3, ToID: 4}
	for _, c := range []command{beginMigrationCommand(req), fenceMigrationCommand(req.OperationID), ackFenceCommand(req.OperationID, 1, 1)} {
		if err := apply(c); err != nil {
			t.Fatal(err)
		}
	}
	data, err := sm.serialize()
	if err != nil {
		t.Fatal(err)
	}
	restored := newStateMachine()
	if err := restored.restore(data); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sm.directory, restored.directory) {
		t.Fatal("migration state lost in snapshot")
	}
	if err := restored.apply(ctx, newCreateVersionCommand("A", 0, "new"), w, zap.NewNop()).Err; !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal("restart forgot admission barrier:", err)
	}
	// A new controller skips the durable ACK, reissues only unfinished work,
	// and stops at FENCED without changing effective membership.
	manager := &RaftNodeImpl{sm: restored}
	tasks, err := manager.ListKBMigrations(ctx)
	if err != nil || len(tasks) != 1 || !reflect.DeepEqual(tasks[0].FencedIDs, []int64{1}) {
		t.Fatal(tasks, err)
	}
	for _, id := range []int64{2, 3, 4} {
		if err := restored.apply(ctx, ackFenceCommand(req.OperationID, 1, id), w, zap.NewNop()).Err; err != nil {
			t.Fatal(err)
		}
	}
	if restored.directory.Migrations[req.OperationID].Phase != storage.MigrationFenced || restored.directory.Placements["A"].Contains(4) {
		t.Fatal("preparation activated learner")
	}
}

func TestMigrationRealRaftPersistsAndCancelsPreparation(t *testing.T) {
	r, _ := newTestRaftNodeImpl(t)
	ctx := proposeCtx(t)
	if err := r.ProposeRegisterStorageNodes(ctx, 0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeCreateKBWithPlacement(ctx, testKB("A"), []int64{1}); err != nil {
		t.Fatal(err)
	}
	req := storage.MigrationRequest{OperationID: "move", KBID: "A", ExpectedEpoch: 1, FromID: 1, ToID: 2}
	for _, run := range []func() error{
		func() error { return r.ProposeBeginKBMigration(ctx, req) },
		func() error { return r.ProposeFenceKBMigration(ctx, "move") },
		func() error { return r.ProposeAckKBMigrationFence(ctx, "move", 1, 1) },
		func() error { return r.ProposeAckKBMigrationFence(ctx, "move", 1, 2) },
	} {
		if err := run(); err != nil {
			t.Fatal(err)
		}
	}
	s, err := r.GetKBPlacement(ctx, "A")
	if err != nil || s.Migration == nil || s.Migration.Phase != storage.MigrationFenced {
		t.Fatal(s, err)
	}
	if _, err := r.ProposeCreateVersion(ctx, "A", 0); !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal(err)
	}
	if err := r.ProposeCancelKBMigration(ctx, "move"); err != nil {
		t.Fatal(err)
	}
	s, err = r.GetKBPlacement(ctx, "A")
	if err != nil || s.Placement.Epoch != 2 || !reflect.DeepEqual(s.Placement.ReplicaIDs, []int64{1}) {
		t.Fatal(s, err)
	}
}

func TestMigrationPreparationSurvivesCompactionAndProcessRestart(t *testing.T) {
	ctx := proposeCtx(t)
	dir, addr := t.TempDir(), freeLoopbackAddrForTest(t)
	start := func() *RaftNodeImpl {
		r, err := NewRaftNodeImpl(Config{NodeID: 1, DataDir: dir, RaftAddr: addr, WAL: wal.NewMockWAL(), MaxLogLength: 4, HeartbeatInterval: 15 * time.Millisecond, ElectionTimeoutMin: 30 * time.Millisecond, ElectionTimeoutMax: 60 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.Stop)
		if !waitForCond(t, 2*time.Second, r.IsLeader) {
			t.Fatal("no leader")
		}
		return r
	}
	r := start()
	if err := r.ProposeRegisterStorageNodes(ctx, 0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeCreateKBWithPlacement(ctx, testKB("A"), []int64{1}); err != nil {
		t.Fatal(err)
	}
	req := storage.MigrationRequest{OperationID: "restart", KBID: "A", ExpectedEpoch: 1, FromID: 1, ToID: 2}
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := r.ProposeAckKBMigrationFence(ctx, req.OperationID, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	if !waitForCond(t, 5*time.Second, func() bool {
		data, _, err := r.persister.LoadSnapshot()
		if err != nil || len(data) == 0 {
			return false
		}
		sm := newStateMachine()
		if sm.restore(data) != nil {
			return false
		}
		m := sm.directory.Migrations[req.OperationID]
		return m.Phase == storage.MigrationFencing && reflect.DeepEqual(m.FencedIDs, []int64{1})
	}) {
		t.Fatal("migration preparation was not snapshotted")
	}
	r.Stop()
	r = start()
	s, err := r.GetKBPlacement(ctx, "A")
	if err != nil || s.Migration == nil || !reflect.DeepEqual(s.Migration.FencedIDs, []int64{1}) {
		t.Fatalf("restart lost durable task ACK: %+v %v", s, err)
	}
	if _, err := r.ProposeCreateVersion(ctx, "A", 0); !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal("restart admitted a new version:", err)
	}
	if err := r.ProposeAckKBMigrationFence(ctx, req.OperationID, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeCancelKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ProposeCreateVersion(ctx, "A", 0); err != nil {
		t.Fatal("cancel did not reopen admission:", err)
	}
}
