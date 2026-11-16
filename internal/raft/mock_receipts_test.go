package raft

import (
	"context"
	"errors"
	"testing"

	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
)

// receiptNode is a MockRaftNode holding a FENCED migration "move" for KB "kb" on
// {1,2}, with one durable version — the state a receipt command is recorded against.
func receiptNode(t *testing.T) (*MockRaftNode, storage.MigrationManifest) {
	t.Helper()
	node := mockStorageNode(t)
	node.kbs["kb"] = types.KnowledgeBaseMeta{KBID: "kb", Status: types.KBStatusActive, ActiveVersionID: 1}
	node.versions[1] = types.VersionMeta{VersionID: 1, KBID: "kb", DataStatus: types.DataStatusDurable, DocIDSetHash: types.EmptyDocIDSetHash}
	node.versionsByKB["kb"] = []int64{1}
	ctx := context.Background()
	req := storage.MigrationRequest{KBID: "kb", OperationID: "move", ExpectedEpoch: 1, FromID: 2, ToID: 3}
	if err := node.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := node.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2, 3} {
		if err := node.ProposeAckKBMigrationFence(ctx, req.OperationID, 1, id); err != nil {
			t.Fatal(err)
		}
	}
	sm := &stateMachine{directory: node.directory, kbs: node.kbs, versions: node.versions, versionsByKB: node.versionsByKB}
	_, manifest, err := sm.migrationManifest(req.OperationID)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	return node, manifest
}

// A stage receipt that matches the committed task becomes evidence; one that does
// not is refused before anything is recorded.
func TestMockRaftNode_RecordsAStageReceipt(t *testing.T) {
	ctx := context.Background()
	node, manifest := receiptNode(t)
	receipt := storage.MigrationStageReceipt{SourceID: 1, TargetID: 3, ManifestHash: manifest.Hash(), PayloadHash: manifest.Hash(), Entries: 1, Bytes: 10}
	if err := node.ProposeRecordKBMigrationStage(ctx, "move", receipt); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if node.directory.Migrations["move"].Phase != storage.MigrationStaged {
		t.Errorf("phase = %q, want staged", node.directory.Migrations["move"].Phase)
	}
	// The same receipt again is a lost-response retry.
	if err := node.ProposeRecordKBMigrationStage(ctx, "move", receipt); err != nil {
		t.Errorf("retry: %v", err)
	}

	// A second, different receipt for the same task is a conflict: the artifact is
	// already recorded and may not be re-described.
	mismatched := receipt
	mismatched.Bytes = 11
	if err := node.ProposeRecordKBMigrationStage(ctx, "move", mismatched); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("mismatched receipt = %v, want ErrMigrationConflict", err)
	}
}

// The install and serve receipts follow the stage one, and each is only acceptable
// when it describes the artifact the previous step committed.
func TestMockRaftNode_RecordsInstallAndServeReceipts(t *testing.T) {
	ctx := context.Background()
	node, manifest := receiptNode(t)

	install := storage.MigrationInstallReceipt{SourceID: 1, TargetID: 3, ManifestHash: manifest.Hash(), StageHash: manifest.Hash(), Versions: 1}
	if err := node.ProposeRecordKBMigrationInstall(ctx, "move", install); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("install before stage = %v, want ErrMigrationConflict", err)
	}

	stage := storage.MigrationStageReceipt{SourceID: 1, TargetID: 3, ManifestHash: manifest.Hash(), PayloadHash: manifest.Hash(), Entries: 1, Bytes: 10}
	if err := node.ProposeRecordKBMigrationStage(ctx, "move", stage); err != nil {
		t.Fatal(err)
	}
	if err := node.ProposeRecordKBMigrationInstall(ctx, "move", install); err != nil {
		t.Fatalf("install: %v", err)
	}
	if node.directory.Migrations["move"].Phase != storage.MigrationInstalled {
		t.Errorf("phase = %q, want installed", node.directory.Migrations["move"].Phase)
	}

	serve := storage.MigrationServeReceipt{TargetID: 3, ManifestHash: manifest.Hash(), ActiveVersion: 1, CursorVersion: 1, BloomVersions: 1, IndexReady: true}
	if err := node.ProposeRecordKBMigrationServe(ctx, "move", serve); err != nil {
		t.Fatalf("serve: %v", err)
	}
	if node.directory.Migrations["move"].Serve == nil {
		t.Fatal("serving evidence was not recorded")
	}

	// Evidence that does not cover the active version cannot be recorded: a target
	// whose cursor stops short could not answer a query for it.
	behind := serve
	behind.CursorVersion = 0
	if err := node.ProposeRecordKBMigrationServe(ctx, "move", behind); !errors.Is(err, serrors.ErrMigrationConflict) && !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Errorf("lagging evidence = %v, want it refused", err)
	}

	// A cancelled context stops the proposal: nothing is recorded on nobody's
	// behalf.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := node.ProposeRecordKBMigrationStage(cancelled, "move", stage); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled stage = %v, want context.Canceled", err)
	}
}

// Activation is the command that changes membership, so it is only accepted for a
// task whose install and serve receipts are the ones already committed.
func TestMockRaftNode_ActivatesAnInstalledTask(t *testing.T) {
	ctx := context.Background()
	node, manifest := receiptNode(t)
	stage := storage.MigrationStageReceipt{SourceID: 1, TargetID: 3, ManifestHash: manifest.Hash(), PayloadHash: manifest.Hash(), Entries: 1, Bytes: 10}
	if err := node.ProposeRecordKBMigrationStage(ctx, "move", stage); err != nil {
		t.Fatal(err)
	}
	install := storage.MigrationInstallReceipt{SourceID: 1, TargetID: 3, ManifestHash: manifest.Hash(), StageHash: manifest.Hash(), Versions: 1}
	if err := node.ProposeRecordKBMigrationInstall(ctx, "move", install); err != nil {
		t.Fatal(err)
	}
	serve := storage.MigrationServeReceipt{TargetID: 3, ManifestHash: manifest.Hash(), ActiveVersion: 1, CursorVersion: 1, BloomVersions: 1, IndexReady: true}
	if err := node.ProposeRecordKBMigrationServe(ctx, "move", serve); err != nil {
		t.Fatal(err)
	}

	// An activation that does not name the committed receipts is a conflict.
	if err := node.ProposeActivateKBMigration(ctx, "move", storage.MigrationActivation{Epoch: 1}); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("empty activation = %v, want ErrMigrationConflict", err)
	}
	activation := storage.MigrationActivation{Epoch: 1, Install: install, Serve: serve}
	if err := node.ProposeActivateKBMigration(ctx, "move", activation); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if node.directory.Migrations["move"].Phase != storage.MigrationActivated {
		t.Errorf("phase = %q, want activated", node.directory.Migrations["move"].Phase)
	}
	placement := node.directory.Placements["kb"]
	if placement.Epoch != 2 || placement.Contains(2) || !placement.Contains(3) {
		t.Errorf("placement = %+v, want the migrated membership at the new epoch", placement)
	}
	// The same activation again is a retry, not a second cutover.
	if err := node.ProposeActivateKBMigration(ctx, "move", activation); err != nil {
		t.Errorf("activation retry = %v", err)
	}
}
