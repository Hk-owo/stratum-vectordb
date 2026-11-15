package storage

import (
	"errors"
	"reflect"
	"testing"

	serrors "stratum/internal/errors"
	"stratum/internal/types"
)

func stagedMigrationTask(t *testing.T) (Directory, MigrationRequest, MigrationManifest, MigrationStageReceipt) {
	t.Helper()
	d, req := migrationDirectory(t)
	if err := d.BeginMigration(req); err != nil {
		t.Fatal(err)
	}
	if err := d.StartFencing(req.OperationID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2, 3, 4} {
		if err := d.AckFence(req.OperationID, 1, id); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := BuildMigrationManifest("A", 1, []types.VersionMeta{{KBID: "A", VersionID: 1, DataStatus: types.DataStatusDurable, DocIDSetHash: types.EmptyDocIDSetHash}})
	if err != nil {
		t.Fatal(err)
	}
	stage := MigrationStageReceipt{SourceID: 1, TargetID: 4, ManifestHash: manifest.Hash(), PayloadHash: types.EmptyDocIDSetHash, Bytes: 128}
	if err := d.RecordMigrationStage(req.OperationID, stage, manifest); err != nil {
		t.Fatal(err)
	}
	return d, req, manifest, stage
}

func installReceiptFor(stage MigrationStageReceipt) MigrationInstallReceipt {
	return MigrationInstallReceipt{SourceID: stage.SourceID, TargetID: stage.TargetID, ManifestHash: stage.ManifestHash, StageHash: stage.PayloadHash, Versions: 1, Documents: 3, Chunks: 2}
}

func TestMigrationInstallRequiresFrozenStageAndMatchingEvidence(t *testing.T) {
	d, req, manifest, stage := stagedMigrationTask(t)
	placementBefore := d.Placements["A"].Clone()
	good := installReceiptFor(stage)

	// Every field of the proof is checked against something the control layer
	// already froze; a target cannot install a different artifact or task.
	for name, mutate := range map[string]func(*MigrationInstallReceipt){
		"target":        func(r *MigrationInstallReceipt) { r.TargetID = 9 },
		"source":        func(r *MigrationInstallReceipt) { r.SourceID = 2 },
		"manifest":      func(r *MigrationInstallReceipt) { r.ManifestHash = types.EmptyDocIDSetHash },
		"stage":         func(r *MigrationInstallReceipt) { r.StageHash = types.EmptyDocIDSetHash[:62] + "aa" },
		"version-count": func(r *MigrationInstallReceipt) { r.Versions = 0 },
	} {
		bad := good
		mutate(&bad)
		if err := d.RecordMigrationInstall(req.OperationID, bad, manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
			t.Fatalf("%s: accepted an installation that does not match the frozen task: %v", name, err)
		}
	}
	if d.Migrations[req.OperationID].Phase != MigrationStaged {
		t.Fatal("rejected installation advanced the task")
	}
	if err := d.RecordMigrationInstall(req.OperationID, good, manifest); err != nil {
		t.Fatal(err)
	}
	recorded := d.Migrations[req.OperationID]
	if recorded.Phase != MigrationInstalled || !recorded.BlocksDataWrites() || !reflect.DeepEqual(placementBefore, d.Placements["A"]) {
		t.Fatal("installation changed membership, epoch or write admission")
	}
	revision := d.Revision
	if err := d.RecordMigrationInstall(req.OperationID, good, manifest); err != nil || d.Revision != revision {
		t.Fatal("lost install ACK retry was not idempotent", err)
	}
	other := good
	other.Documents++
	if err := d.RecordMigrationInstall(req.OperationID, other, manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("a different installation replaced the recorded one")
	}
	clone := d.Clone()
	clone.Migrations[req.OperationID].Install.Documents = 99
	if d.Migrations[req.OperationID].Install.Documents != good.Documents {
		t.Fatal("install receipt pointer aliases a snapshot")
	}
	if err := d.CancelMigration(req.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := d.RecordMigrationInstall(req.OperationID, good, manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("late installation revived a cancelled task")
	}
	if d.Placements["A"].Epoch != 2 {
		t.Fatal("cancel reused the fenced epoch")
	}
}

func TestMigrationInstallRequiresStagedArtifactFirst(t *testing.T) {
	d, req := migrationDirectory(t)
	if err := d.BeginMigration(req); err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildMigrationManifest("A", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt := MigrationInstallReceipt{SourceID: 1, TargetID: 4, ManifestHash: manifest.Hash(), StageHash: types.EmptyDocIDSetHash}
	if err := d.RecordMigrationInstall(req.OperationID, receipt, manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("installed without a staged artifact:", err)
	}
}

func TestMigrationServeRequiresInstalledDataAndLocalEvidence(t *testing.T) {
	d, req, manifest, stage := stagedMigrationTask(t)
	install := installReceiptFor(stage)
	serve := MigrationServeReceipt{TargetID: 4, ManifestHash: manifest.Hash(), ActiveVersion: manifest.ActiveVersion, CursorVersion: 1, BloomVersions: 1, IndexReady: true}
	if err := d.RecordMigrationServe(req.OperationID, serve, manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("serving evidence recorded before installation:", err)
	}
	if err := d.RecordMigrationInstall(req.OperationID, install, manifest); err != nil {
		t.Fatal(err)
	}
	placementBefore := d.Placements["A"].Clone()
	for name, mutate := range map[string]func(*MigrationServeReceipt){
		"target":      func(r *MigrationServeReceipt) { r.TargetID = 9 },
		"manifest":    func(r *MigrationServeReceipt) { r.ManifestHash = types.EmptyDocIDSetHash },
		"active":      func(r *MigrationServeReceipt) { r.ActiveVersion = 7 },
		"cursor":      func(r *MigrationServeReceipt) { r.CursorVersion = 0 },
		"bloom-count": func(r *MigrationServeReceipt) { r.BloomVersions = 0 },
		"index":       func(r *MigrationServeReceipt) { r.IndexReady = false },
	} {
		bad := serve
		mutate(&bad)
		err := d.RecordMigrationServe(req.OperationID, bad, manifest)
		if !errors.Is(err, serrors.ErrMigrationConflict) && !errors.Is(err, serrors.ErrMigrationBusy) {
			t.Fatalf("%s: accepted unserviceable evidence: %v", name, err)
		}
	}
	if d.Migrations[req.OperationID].Serve != nil {
		t.Fatal("rejected serving evidence was recorded")
	}
	if err := d.RecordMigrationServe(req.OperationID, serve, manifest); err != nil {
		t.Fatal(err)
	}
	task := d.Migrations[req.OperationID]
	if task.Phase != MigrationInstalled || task.Serve == nil || *task.Serve != serve || !task.BlocksDataWrites() || !reflect.DeepEqual(placementBefore, d.Placements["A"]) {
		t.Fatal("serving evidence changed the phase, write admission or membership")
	}
	revision := d.Revision
	if err := d.RecordMigrationServe(req.OperationID, serve, manifest); err != nil || d.Revision != revision {
		t.Fatal("lost serving retry was not idempotent", err)
	}
	other := serve
	other.CursorVersion++
	if err := d.RecordMigrationServe(req.OperationID, other, manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("a different serving receipt replaced the recorded one")
	}
	clone := d.Clone()
	clone.Migrations[req.OperationID].Serve.CursorVersion = 99
	if d.Migrations[req.OperationID].Serve.CursorVersion != serve.CursorVersion {
		t.Fatal("serving receipt pointer aliases a snapshot")
	}
}
