package raft

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wire"
)

func TestMigrationStageSnapshotAndControlManifestValidation(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	apply := func(c command) error { return sm.apply(ctx, c, w, zap.NewNop()).Err }
	if err := apply(registerStorageNodesCommand(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}})); err != nil {
		t.Fatal(err)
	}
	if err := apply(newCreateKBWithPlacementCommand(testKB("A"), []int64{1, 2})); err != nil {
		t.Fatal(err)
	}
	created := sm.apply(ctx, newCreateVersionCommand("A", 0, ""), w, zap.NewNop())
	if created.Err != nil {
		t.Fatal(created.Err)
	}
	if err := apply(newUpdateVersionSummaryCommand(created.VersionID, types.EmptyDocIDSetHash)); err != nil {
		t.Fatal(err)
	}
	req := storage.MigrationRequest{OperationID: "stage-A", KBID: "A", ExpectedEpoch: 1, FromID: 2, ToID: 3}
	for _, c := range []command{beginMigrationCommand(req), fenceMigrationCommand(req.OperationID), ackFenceCommand(req.OperationID, 1, 1), ackFenceCommand(req.OperationID, 1, 2), ackFenceCommand(req.OperationID, 1, 3)} {
		if err := apply(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := apply(newUpdateVersionSummaryCommand(created.VersionID, types.EmptyDocIDSetHash)); err != nil {
		t.Fatal("exact durable confirmation retry refused", err)
	}
	wrong := types.EmptyDocIDSetHash[:63] + "0"
	if err := apply(newUpdateVersionSummaryCommand(created.VersionID, wrong)); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatal("frozen manifest changed via a late digest", err)
	}
	fail := command{Type: cmdMarkVersionFailedPermanent, KBID: "A", VersionID: created.VersionID, FailureSide: types.FailureSideData}
	if err := apply(fail); !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal("late DATA failure changed frozen requirements", err)
	}
	manifest, err := storage.BuildMigrationManifest("A", 0, []types.VersionMeta{sm.versions[created.VersionID]})
	if err != nil {
		t.Fatal(err)
	}
	receipt := storage.MigrationStageReceipt{SourceID: 1, TargetID: 3, ManifestHash: manifest.Hash(), PayloadHash: types.EmptyDocIDSetHash, Bytes: 128}
	bad := receipt
	bad.ManifestHash = types.EmptyDocIDSetHash
	if err := apply(recordMigrationStageCommand(req.OperationID, bad)); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("control trusted target manifest blindly", err)
	}
	if err := apply(recordMigrationStageCommand(req.OperationID, receipt)); err != nil {
		t.Fatal(err)
	}
	data, err := sm.serialize()
	if err != nil {
		t.Fatal(err)
	}
	restored := newStateMachine()
	if err := restored.restore(data); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.directory, sm.directory) {
		t.Fatal("stage receipt lost on snapshot recovery")
	}
	roundtrip := wire.StorageDirectoryFromProto(wire.StorageDirectoryToProto(restored.directory))
	if !reflect.DeepEqual(roundtrip, restored.directory) {
		t.Fatal("wire projection lost stage receipt")
	}
	if !restored.directory.Migrations[req.OperationID].BlocksDataWrites() || restored.directory.Placements["A"].Contains(3) {
		t.Fatal("restore promoted staged data")
	}
}

func TestMigrationStageSchedulerCancelsTransferOnLeadershipLoss(t *testing.T) {
	r, req := migrationNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2, 3, 4} {
		if err := r.ProposeAckKBMigrationFence(ctx, req.OperationID, 1, id); err != nil {
			t.Fatal(err)
		}
	}
	var leader atomic.Bool
	leader.Store(true)
	entered := make(chan struct{})
	finished := make(chan error, 1)
	scheduler := &storage.FenceScheduler{Manager: r, IsLeader: leader.Load, Stage: func(c context.Context, target, source storage.Node, m storage.Migration) (storage.MigrationStageReceipt, error) {
		close(entered)
		<-c.Done()
		return storage.MigrationStageReceipt{}, c.Err()
	}}
	go func() { finished <- scheduler.Step(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("stage did not begin")
	}
	leader.Store(false)
	select {
	case err := <-finished:
		if !errors.Is(err, serrors.ErrNoLeaderYet) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("former leader did not cancel stage")
	}
	snapshot, err := r.GetKBPlacement(ctx, "A")
	if err != nil || snapshot.Migration.Phase != storage.MigrationFenced || snapshot.Migration.Stage != nil {
		t.Fatal("former leader recorded stage", snapshot, err)
	}
}
