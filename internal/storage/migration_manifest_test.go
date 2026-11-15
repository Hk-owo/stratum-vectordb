package storage

import (
	"errors"
	"reflect"
	"testing"

	serrors "stratum/internal/errors"
	"stratum/internal/types"
)

func TestMigrationManifestStableAcrossOrderAndIndexOnlyChanges(t *testing.T) {
	versions := []types.VersionMeta{{KBID: "kb", VersionID: 2, ParentVersionID: 1, DataStatus: types.DataStatusDurable, DocIDSetHash: types.EmptyDocIDSetHash}, {KBID: "kb", VersionID: 1, DataStatus: types.DataStatusDurable, DocIDSetHash: types.EmptyDocIDSetHash}}
	first, err := BuildMigrationManifest("kb", 2, versions)
	if err != nil {
		t.Fatal(err)
	}
	versions[0], versions[1] = versions[1], versions[0]
	versions[0].IndexStatus = types.IndexStatusReady
	versions[0].IndexReadyNodes = []int64{9}
	second, err := BuildMigrationManifest("kb", 2, versions)
	if err != nil || first.Hash() != second.Hash() {
		t.Fatal("advisory index updates changed DATA requirements", err)
	}
	versions[0].DataStatus = types.DataStatusPending
	if _, err := BuildMigrationManifest("kb", 2, versions); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Fatal("pending DATA frozen into manifest", err)
	}
	versions[0].DataStatus = types.DataStatusDurable
	versions[0].DocIDSetHash = ""
	if _, err := BuildMigrationManifest("kb", 2, versions); err == nil {
		t.Fatal("unverifiable durable version accepted")
	}
}
func TestMigrationStageReceiptDoesNotActivateOrAliasAndCannotReviveCancellation(t *testing.T) {
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
	manifest, err := BuildMigrationManifest("A", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt := MigrationStageReceipt{SourceID: 1, TargetID: 4, ManifestHash: manifest.Hash(), PayloadHash: types.EmptyDocIDSetHash, Entries: 0, Bytes: 128}
	before := d.Placements["A"].Clone()
	bad := receipt
	bad.TargetID = 9
	if err := d.RecordMigrationStage(req.OperationID, bad, manifest); err == nil {
		t.Fatal("wrong target ACK accepted")
	}
	if err := d.RecordMigrationStage(req.OperationID, receipt, manifest); err != nil {
		t.Fatal(err)
	}
	revision := d.Revision
	if err := d.RecordMigrationStage(req.OperationID, receipt, manifest); err != nil || d.Revision != revision {
		t.Fatal("lost ACK retry was not idempotent", err)
	}
	if d.Migrations[req.OperationID].Phase != MigrationStaged || !d.Migrations[req.OperationID].BlocksDataWrites() || !reflect.DeepEqual(before, d.Placements["A"]) {
		t.Fatal("stage activated or resumed writes")
	}
	clone := d.Clone()
	clone.Migrations[req.OperationID].Stage.SourceID = 9
	if d.Migrations[req.OperationID].Stage.SourceID != 1 {
		t.Fatal("receipt pointer aliases a snapshot")
	}
	if err := d.CancelMigration(req.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := d.RecordMigrationStage(req.OperationID, receipt, manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("late receipt revived cancelled task", err)
	}
	if d.Placements["A"].Epoch != 2 {
		t.Fatal("cancel reused fenced epoch")
	}
}
