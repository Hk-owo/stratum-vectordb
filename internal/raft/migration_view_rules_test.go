package raft

import (
	"errors"
	"testing"

	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
)

// fenceTask puts a migration in the requested phase on the state machine's
// directory, so the transfer view has something to describe.
func fenceTask(sm *stateMachine, phase storage.MigrationPhase) storage.Migration {
	p := sm.directory.Placements["kb"].Clone()
	m := storage.Migration{
		Request:   storage.MigrationRequest{OperationID: "move", KBID: "kb", ExpectedEpoch: 1, FromID: 2, ToID: 3},
		Old:       p,
		FencedIDs: []int64{1, 2, 3},
		Phase:     phase,
	}
	if sm.directory.Migrations == nil {
		sm.directory.Migrations = map[string]storage.Migration{}
	}
	sm.directory.Migrations["move"] = m
	return m
}

// The transfer view is what a storage node is handed instead of the whole
// directory, so it may only be produced for a task whose bytes are frozen: anything
// else has no manifest to verify against.
func TestMigrationManifest_RefusesTasksWithoutFrozenBytes(t *testing.T) {
	sm, _ := migrationSM(t)

	if _, _, err := sm.migrationManifest("missing"); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown operation = %v, want ErrMigrationNotFound", err)
	}

	fenceTask(sm, storage.MigrationDraining)
	if _, _, err := sm.migrationManifest("move"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("draining task = %v, want ErrMigrationConflict", err)
	}

	fenceTask(sm, storage.MigrationFenced)
	m, manifest, err := sm.migrationManifest("move")
	if err != nil {
		t.Fatalf("fenced task: %v", err)
	}
	if manifest.KBID != "kb" || manifest.ActiveVersion != 1 || len(manifest.Versions) != 1 {
		t.Errorf("manifest = %+v", manifest)
	}
	if m.Request.OperationID != "move" {
		t.Errorf("task = %+v", m)
	}

	// A version whose data is still in flight has no frozen document set, so there
	// is no manifest to hand out.
	sm.versions[2] = types.VersionMeta{VersionID: 2, ParentVersionID: 1, KBID: "kb", DataStatus: types.DataStatusPending}
	sm.versionsByKB["kb"] = []int64{1, 2}
	if _, _, err := sm.migrationManifest("move"); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Errorf("pending version = %v, want ErrMigrationBusy", err)
	}
	sm.versionsByKB["kb"] = []int64{1}
	delete(sm.versions, 2)

	// A KB that is gone or being deleted has no bytes worth transferring.
	meta := sm.kbs["kb"]
	meta.Status = types.KBStatusDeleting
	sm.kbs["kb"] = meta
	if _, _, err := sm.migrationManifest("move"); !errors.Is(err, serrors.ErrKnowledgeBaseDeleted) {
		t.Errorf("deleting KB = %v, want ErrKnowledgeBaseDeleted", err)
	}
	delete(sm.kbs, "kb")
	if _, _, err := sm.migrationManifest("move"); !errors.Is(err, serrors.ErrKnowledgeBaseDeleted) {
		t.Errorf("unknown KB = %v, want ErrKnowledgeBaseDeleted", err)
	}
}

// The view carries exactly what the target needs to verify the artifact: the
// placement, the frozen task, the active version and the nodes involved — nothing
// about any other KB.
func TestMigrationTransferView_CarriesTheTaskAndItsNodes(t *testing.T) {
	sm, _ := migrationSM(t)
	sm.directory.ServiceBlocks = map[int64]storage.ServiceBlock{1: {OperationID: "block", Revision: 2}}
	fenceTask(sm, storage.MigrationFenced)

	view, err := sm.migrationTransferView("move")
	if err != nil {
		t.Fatalf("view: %v", err)
	}
	if view.Manifest.KBID != "kb" || view.Manifest.ActiveVersion != 1 {
		t.Errorf("manifest = %+v", view.Manifest)
	}
	if len(view.Directory.Placements) != 1 || !view.Directory.Placements["kb"].Contains(1) {
		t.Errorf("placements = %+v", view.Directory.Placements)
	}
	if view.Directory.ActiveVersions["kb"] != 1 {
		t.Errorf("active versions = %v", view.Directory.ActiveVersions)
	}
	if len(view.Directory.Migrations) != 1 || view.Directory.Migrations["move"].Phase != storage.MigrationFenced {
		t.Errorf("migrations = %+v", view.Directory.Migrations)
	}
	if block, ok := view.Directory.ServiceBlocks[1]; !ok || block.OperationID != "block" {
		t.Errorf("service blocks = %+v, want the revocation proof carried along", view.Directory.ServiceBlocks)
	}
	// The fence union (old members plus the target) is what the target needs in
	// order to reach every source; nodes outside it are not included.
	for _, id := range []int64{1, 2, 3} {
		if _, ok := view.Directory.Nodes[id]; !ok {
			t.Errorf("node %d missing from the view", id)
		}
	}

	// A task with no committed revocation proof cannot produce a view: the members
	// differ from what the task claims.
	faulted := fenceTask(sm, storage.MigrationFenced)
	faulted.Request.Fault = true
	faulted.FailedBlock = storage.ServiceBlock{OperationID: "block", Revision: 2}
	sm.directory.Migrations["move"] = faulted
	sm.directory.ServiceBlocks[2] = storage.ServiceBlock{OperationID: "block", Revision: 2} // committed, not revoked
	if _, err := sm.migrationTransferView("move"); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Errorf("fault without a committed revocation = %v, want ErrMigrationBusy", err)
	}

	// A placement that disappeared makes the task unreadable rather than empty.
	fenceTask(sm, storage.MigrationFenced)
	delete(sm.directory.Placements, "kb")
	if _, err := sm.migrationTransferView("move"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("missing placement = %v, want ErrPlacementUnknown", err)
	}
}
