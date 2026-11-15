package raft

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"go.uber.org/zap"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wire"
)

// stageMigrationTask drives one task from begin to STAGED, returning the receipt
// the control layer committed.
func stageMigrationTask(t *testing.T, r *MockRaftNode, req storage.MigrationRequest) storage.MigrationStageReceipt {
	t.Helper()
	ctx := context.Background()
	v, err := r.ProposeCreateVersion(ctx, req.KBID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeUpdateVersionSummary(ctx, v, types.EmptyDocIDSetHash); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeMarkVersionDataDurable(ctx, v); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatalf("fence: %v", err)
	}
	for _, id := range []int64{1, 2, 3, 4} {
		if err := r.ProposeAckKBMigrationFence(ctx, req.OperationID, req.ExpectedEpoch, id); err != nil {
			t.Fatalf("ack %d: %v", id, err)
		}
	}
	// The mock never activates a version, so the manifest's active id is 0 while
	// the retained DATA version is v — exactly the shape the control layer builds.
	manifest, err := storage.BuildMigrationManifest(req.KBID, 0, []types.VersionMeta{{KBID: req.KBID, VersionID: v, DataStatus: types.DataStatusDurable, DocIDSetHash: types.EmptyDocIDSetHash}})
	if err != nil {
		t.Fatal(err)
	}
	stage := storage.MigrationStageReceipt{SourceID: 1, TargetID: req.ToID, ManifestHash: manifest.Hash(), PayloadHash: types.EmptyDocIDSetHash, Bytes: 256}
	if err := r.ProposeRecordKBMigrationStage(ctx, req.OperationID, stage); err != nil {
		t.Fatalf("stage: %v", err)
	}
	return stage
}

func TestMigrationInstallRecordedNeverChangesMembershipAndSurvivesRecovery(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	apply := func(c command) error { return sm.apply(ctx, c, w, zap.NewNop()).Err }
	if err := apply(registerStorageNodesCommand(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}, {ID: 4, Addr: "s4"}})); err != nil {
		t.Fatal(err)
	}
	if err := apply(newCreateKBWithPlacementCommand(testKB("A"), []int64{1, 2, 3})); err != nil {
		t.Fatal(err)
	}
	created := sm.apply(ctx, newCreateVersionCommand("A", 0, ""), w, zap.NewNop())
	if created.Err != nil {
		t.Fatal(created.Err)
	}
	if err := apply(newUpdateVersionSummaryCommand(created.VersionID, types.EmptyDocIDSetHash)); err != nil {
		t.Fatal(err)
	}
	if err := apply(newMarkDataDurableCommand(created.VersionID)); err != nil {
		t.Fatal(err)
	}
	req := storage.MigrationRequest{OperationID: "install-A", KBID: "A", ExpectedEpoch: 1, FromID: 3, ToID: 4}
	for _, c := range []command{beginMigrationCommand(req), fenceMigrationCommand(req.OperationID), ackFenceCommand(req.OperationID, 1, 1), ackFenceCommand(req.OperationID, 1, 2), ackFenceCommand(req.OperationID, 1, 3), ackFenceCommand(req.OperationID, 1, 4)} {
		if err := apply(c); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := storage.BuildMigrationManifest("A", sm.kbs["A"].ActiveVersionID, []types.VersionMeta{sm.versions[created.VersionID]})
	if err != nil {
		t.Fatal(err)
	}
	stage := storage.MigrationStageReceipt{SourceID: 1, TargetID: 4, ManifestHash: manifest.Hash(), PayloadHash: types.EmptyDocIDSetHash, Bytes: 256}
	if err := apply(recordMigrationStageCommand(req.OperationID, stage)); err != nil {
		t.Fatal(err)
	}
	install := storage.MigrationInstallReceipt{SourceID: 1, TargetID: 4, ManifestHash: manifest.Hash(), StageHash: stage.PayloadHash, Versions: 1, Documents: 2, Chunks: 0}
	bad := install
	bad.Versions = 0
	if err := apply(recordMigrationInstallCommand(req.OperationID, bad)); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("control recorded an installation of no DATA version:", err)
	}
	if err := apply(recordMigrationInstallCommand(req.OperationID, install)); err != nil {
		t.Fatal(err)
	}
	task := sm.directory.Migrations[req.OperationID]
	if task.Phase != storage.MigrationInstalled || !task.BlocksDataWrites() || sm.directory.Placements["A"].Contains(4) || sm.directory.Placements["A"].Epoch != 1 {
		t.Fatal("installation activated the target")
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
		t.Fatal("install receipt lost on snapshot recovery")
	}
	roundtrip := wire.StorageDirectoryFromProto(wire.StorageDirectoryToProto(restored.directory))
	if !reflect.DeepEqual(roundtrip, restored.directory) {
		t.Fatal("wire projection lost install receipt")
	}
}

func TestMigrationInstallSchedulerRecordsOnlyVerifiedReceipts(t *testing.T) {
	r, req := migrationNode(t)
	stageMigrationTask(t, r, req)
	ctx, cancel := context.WithTimeout(context.Background(), 5e9)
	defer cancel()
	var called bool
	scheduler := &storage.FenceScheduler{Manager: r, IsLeader: func() bool { return true }, Install: func(c context.Context, target storage.Node, m storage.Migration) (storage.MigrationInstallReceipt, error) {
		called = true
		return storage.MigrationInstallReceipt{SourceID: m.Stage.SourceID, TargetID: target.ID, ManifestHash: m.Stage.ManifestHash, StageHash: "not-the-stage-hash", Versions: 1}, nil
	}}
	if err := scheduler.Step(ctx); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("unverified receipt advanced the task:", err)
	}
	if !called {
		t.Fatal("install was never attempted")
	}
	snapshot, err := r.GetKBPlacement(ctx, req.KBID)
	if err != nil || snapshot.Migration.Phase != storage.MigrationStaged || snapshot.Migration.Install != nil {
		t.Fatal("scheduler recorded an unverified installation", snapshot, err)
	}
	scheduler.Install = func(c context.Context, target storage.Node, m storage.Migration) (storage.MigrationInstallReceipt, error) {
		return storage.MigrationInstallReceipt{SourceID: m.Stage.SourceID, TargetID: target.ID, ManifestHash: m.Stage.ManifestHash, StageHash: m.Stage.PayloadHash, Versions: 1}, nil
	}
	if err := scheduler.Step(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = r.GetKBPlacement(ctx, req.KBID)
	if err != nil || snapshot.Migration.Phase != storage.MigrationInstalled || snapshot.Migration.Install == nil || snapshot.Placement.Contains(req.ToID) {
		t.Fatal("verified installation was not recorded, or it activated the target", snapshot, err)
	}
	if !snapshot.Placement.Contains(req.FromID) || snapshot.Placement.Epoch != req.ExpectedEpoch {
		t.Fatal("installation changed the effective replica set")
	}
}

func TestMigrationServeRecordedWithoutActivationAndSurvivesRecovery(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	apply := func(c command) error { return sm.apply(ctx, c, w, zap.NewNop()).Err }
	if err := apply(registerStorageNodesCommand(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}, {ID: 4, Addr: "s4"}})); err != nil {
		t.Fatal(err)
	}
	if err := apply(newCreateKBWithPlacementCommand(testKB("A"), []int64{1, 2, 3})); err != nil {
		t.Fatal(err)
	}
	created := sm.apply(ctx, newCreateVersionCommand("A", 0, ""), w, zap.NewNop())
	if created.Err != nil {
		t.Fatal(created.Err)
	}
	if err := apply(newUpdateVersionSummaryCommand(created.VersionID, types.EmptyDocIDSetHash)); err != nil {
		t.Fatal(err)
	}
	if err := apply(newMarkDataDurableCommand(created.VersionID)); err != nil {
		t.Fatal(err)
	}
	req := storage.MigrationRequest{OperationID: "serve-A", KBID: "A", ExpectedEpoch: 1, FromID: 3, ToID: 4}
	for _, c := range []command{beginMigrationCommand(req), fenceMigrationCommand(req.OperationID), ackFenceCommand(req.OperationID, 1, 1), ackFenceCommand(req.OperationID, 1, 2), ackFenceCommand(req.OperationID, 1, 3), ackFenceCommand(req.OperationID, 1, 4)} {
		if err := apply(c); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := storage.BuildMigrationManifest("A", sm.kbs["A"].ActiveVersionID, []types.VersionMeta{sm.versions[created.VersionID]})
	if err != nil {
		t.Fatal(err)
	}
	stage := storage.MigrationStageReceipt{SourceID: 1, TargetID: 4, ManifestHash: manifest.Hash(), PayloadHash: types.EmptyDocIDSetHash, Bytes: 256}
	if err := apply(recordMigrationStageCommand(req.OperationID, stage)); err != nil {
		t.Fatal(err)
	}
	install := storage.MigrationInstallReceipt{SourceID: 1, TargetID: 4, ManifestHash: manifest.Hash(), StageHash: stage.PayloadHash, Versions: 1}
	if err := apply(recordMigrationInstallCommand(req.OperationID, install)); err != nil {
		t.Fatal(err)
	}
	serve := storage.MigrationServeReceipt{TargetID: 4, ManifestHash: manifest.Hash(), ActiveVersion: manifest.ActiveVersion, CursorVersion: created.VersionID, BloomVersions: 1, IndexReady: true}
	// The control layer refuses serving evidence that does not match the frozen
	// DATA record: a target cannot swap manifests between the two steps.
	bad := serve
	bad.ManifestHash = types.EmptyDocIDSetHash
	if err := apply(recordMigrationServeCommand(req.OperationID, bad)); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("serving evidence with a foreign manifest was accepted:", err)
	}
	if err := apply(recordMigrationServeCommand(req.OperationID, serve)); err != nil {
		t.Fatal(err)
	}
	task := sm.directory.Migrations[req.OperationID]
	if task.Phase != storage.MigrationInstalled || task.Serve == nil || !task.BlocksDataWrites() || sm.directory.Placements["A"].Contains(4) {
		t.Fatal("serving evidence activated the target")
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
		t.Fatal("serving evidence lost on snapshot recovery")
	}
	if got := wire.StorageDirectoryFromProto(wire.StorageDirectoryToProto(restored.directory)); !reflect.DeepEqual(got, restored.directory) {
		t.Fatal("wire projection lost serving evidence")
	}
}

func TestMigrationServeSchedulerRecordsOnlyVerifiedEvidence(t *testing.T) {
	r, req := migrationNode(t)
	stageMigrationTask(t, r, req)
	ctx, cancel := context.WithTimeout(context.Background(), 5e9)
	defer cancel()
	// Install first: serving evidence has no meaning before installation.
	r.mu.Lock()
	task := r.directory.Migrations[req.OperationID]
	r.mu.Unlock()
	install := storage.MigrationInstallReceipt{SourceID: task.Stage.SourceID, TargetID: req.ToID, ManifestHash: task.Stage.ManifestHash, StageHash: task.Stage.PayloadHash, Versions: 1}
	if err := r.ProposeRecordKBMigrationInstall(ctx, req.OperationID, install); err != nil {
		t.Fatal(err)
	}
	var served bool
	scheduler := &storage.FenceScheduler{Manager: r, IsLeader: func() bool { return true }, Serve: func(c context.Context, target storage.Node, m storage.Migration) (storage.MigrationServeReceipt, error) {
		served = true
		return storage.MigrationServeReceipt{TargetID: target.ID, ManifestHash: m.Install.ManifestHash, ActiveVersion: 0, CursorVersion: 0, BloomVersions: 1, IndexReady: true}, nil
	}}
	if err := scheduler.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if !served {
		t.Fatal("serving step was never attempted")
	}
	snapshot, err := r.GetKBPlacement(ctx, req.KBID)
	if err != nil || snapshot.Migration.Serve == nil || snapshot.Migration.Phase != storage.MigrationInstalled || snapshot.Placement.Contains(req.ToID) {
		t.Fatal("serving evidence was not recorded, or it activated the target", snapshot, err)
	}
}
