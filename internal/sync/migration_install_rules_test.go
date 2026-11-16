package sync

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

// installHandler wires the transfer handlers with a fixed directory snapshot and a
// nil-but-present follower, which is enough to reach every refusal that happens
// before any ordinary store is touched.
func installHandler(t *testing.T, nodeID int64, view storage.MigrationTransferView) *NodeHandler {
	t.Helper()
	handler := viewHandler(t, nodeID, migrationViewMetadata{view: view})
	handler.exporter = nil
	handler.receiver.placements = nil
	return handler
}

func installView(t *testing.T, mutate func(*storage.Directory, *storage.MigrationManifest)) storage.MigrationTransferView {
	t.Helper()
	view := transferView(t, mutate)
	task := view.Directory.Migrations["move"]
	task.Stage = &storage.MigrationStageReceipt{SourceID: 1, TargetID: 3, ManifestHash: "manifest", PayloadHash: "payload"}
	task.Phase = storage.MigrationStaged
	view.Directory.Migrations["move"] = task
	return view
}

// The installation handlers write into this node's ordinary stores, so nothing may
// happen until the task, the target identity and the local artifact all agree.
func TestInstallKBMigration_RefusesBeforeTouchingTheStores(t *testing.T) {
	ctx := context.Background()
	request := testTransferRequest()

	if _, err := NewNodeHandler(nil, nil).InstallKBMigration(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("unwired handler = %v, want FailedPrecondition", err)
	}

	view := installView(t, nil)
	bare := installHandler(t, 3, view)
	bare.exporter = nil
	if _, err := bare.InstallKBMigration(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("no exporter = %v, want FailedPrecondition", err)
	}

	noFollower := installHandler(t, 3, view)
	noFollower.receiver.follower = nil
	if _, err := noFollower.InstallKBMigration(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("no follower = %v, want FailedPrecondition", err)
	}

	emptyDir := installHandler(t, 3, view)
	emptyDir.migration.dir = ""
	if _, err := emptyDir.InstallKBMigration(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("no staging directory = %v, want FailedPrecondition", err)
	}

	// Another node asking to install this task's artifact is not its target.
	wrongTarget := installHandler(t, 1, view)
	wrongTarget.receiver.follower = &Follower{}
	if _, err := wrongTarget.InstallKBMigration(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("non-target node = %v, want FailedPrecondition", err)
	}

	// A task with no staging receipt has nothing to install.
	noStage := installView(t, func(d *storage.Directory, _ *storage.MigrationManifest) {})
	m := noStage.Directory.Migrations["move"]
	m.Stage = nil
	noStage.Directory.Migrations["move"] = m
	staging := installHandler(t, 3, noStage)
	staging.receiver.follower = &Follower{}
	if _, err := staging.InstallKBMigration(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("no stage receipt = %v, want FailedPrecondition", err)
	}

	// The staging artifact has to be there and be a regular file.
	missing := installHandler(t, 3, view)
	missing.receiver.follower = &Follower{}
	if _, err := missing.InstallKBMigration(ctx, request); status.Code(err) == codes.OK {
		t.Error("a missing staging artifact was accepted")
	}
	dirAsArtifact := installHandler(t, 3, view)
	dirAsArtifact.receiver.follower = &Follower{}
	if err := os.MkdirAll(migrationStagePath(dirAsArtifact.migration.dir, "kb", "move"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := dirAsArtifact.InstallKBMigration(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("directory as artifact = %v, want FailedPrecondition", err)
	}

	// A damaged install record is an error, never "not installed yet".
	recordPath := migrationInstallPath(dirAsArtifact.migration.dir, "kb", "move")
	if err := os.WriteFile(recordPath, []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readInstallRecord(recordPath); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("corrupt record = %v, want a data error", err)
	}
	if err := dirAsArtifact.migration.writeInstallRecord(recordPath, migrationInstallRecord{OperationID: "move", KBID: "kb", Epoch: 1, Receipt: storage.MigrationInstallReceipt{TargetID: 3}}); err != nil {
		t.Fatalf("write record: %v", err)
	}
	// A record that names another task is a conflict, not an upgrade.
	if _, err := dirAsArtifact.InstallKBMigration(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("foreign record = %v, want FailedPrecondition", err)
	}

	// A cancelled call does not wait for the semaphore: the client is gone.
	busy := installHandler(t, 3, view)
	busy.receiver.follower = &Follower{}
	busy.migration.sem <- struct{}{}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := busy.InstallKBMigration(cancelled, request); err == nil {
		t.Error("a cancelled installation still ran")
	}
	<-busy.migration.sem
}

// The record write has the same durability sequence as the artifact: content fsync,
// atomic rename, directory fsync. A path whose directory does not exist must fail
// instead of leaving a half-written record behind.
func TestWriteInstallRecord_FailsWithoutADirectory(t *testing.T) {
	dir := t.TempDir()
	stager := &migrationStager{dir: dir}
	path := migrationInstallPath(dir, "kb", "move")
	if err := stager.writeInstallRecord(path, migrationInstallRecord{OperationID: "move", KBID: "kb", Epoch: 1, Receipt: storage.MigrationInstallReceipt{TargetID: 3}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	record, err := readInstallRecord(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if record.OperationID != "move" || record.Receipt.TargetID != 3 {
		t.Errorf("record = %+v", record)
	}
	// No temp file may be left behind after a successful write.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".tmp" {
			t.Errorf("temp file left behind: %s", entry.Name())
		}
	}

	missing := &migrationStager{dir: filepath.Join(dir, "gone")}
	if err := missing.writeInstallRecord(filepath.Join(dir, "gone", "record"), migrationInstallRecord{}); err == nil {
		t.Error("a record was written into a directory that does not exist")
	}
	// An injected fsync failure refuses the record: a receipt that is not on disk
	// must never be reported as one.
	syncFails := errors.New("fsync refused")
	strict := &migrationStager{dir: dir, syncDir: func(string) error { return syncFails }}
	if err := strict.writeInstallRecord(migrationInstallPath(dir, "kb", "other"), migrationInstallRecord{OperationID: "other", KBID: "kb", Epoch: 1, Receipt: storage.MigrationInstallReceipt{TargetID: 3}}); !errors.Is(err, syncFails) {
		t.Errorf("failed directory fsync = %v, want %v", err, syncFails)
	}
}

// Serving evidence is only meaningful for data this task installed, so the serving
// handler refuses a task without an installation, a missing record, or a foreign one.
func TestPrepareKBMigrationServing_RequiresAnInstalledArtifact(t *testing.T) {
	ctx := context.Background()
	request := testTransferRequest()

	installedView := func(t *testing.T, mutate func(*storage.Directory, *storage.MigrationManifest)) storage.MigrationTransferView {
		t.Helper()
		view := installView(t, mutate)
		task := view.Directory.Migrations["move"]
		task.Phase = storage.MigrationInstalled
		task.Install = &storage.MigrationInstallReceipt{SourceID: 1, TargetID: 3, ManifestHash: "manifest", StageHash: "payload"}
		view.Directory.Migrations["move"] = task
		return view
	}

	handler := viewHandler(t, 3, migrationViewMetadata{view: installedView(t, nil)})
	handler.exporter = nil
	handler.receiver.follower = &Follower{}
	handler.receiver.serving = pausingIndex{}
	handler.receiver.localVersion = localVersionStub{}
	if _, err := handler.prepareMigrationServing(ctx, request, false); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("serving evidence without a record = %v, want FailedPrecondition", err)
	}
	// The record has to describe the committed task exactly.
	record := migrationInstallRecord{
		OperationID: "move",
		KBID:        "kb",
		Epoch:       1,
		Receipt:     storage.MigrationInstallReceipt{SourceID: 1, TargetID: 3, ManifestHash: "manifest", StageHash: "payload"},
	}
	if err := handler.migration.writeInstallRecord(migrationInstallPath(handler.migration.dir, "kb", "move"), record); err != nil {
		t.Fatal(err)
	}

	staged := installHandler(t, 3, installView(t, nil))
	staged.receiver.follower = &Follower{}
	staged.receiver.serving = pausingIndex{}
	staged.receiver.localVersion = localVersionStub{}
	if _, err := staged.prepareMigrationServing(ctx, request, false); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("staged task asked to serve = %v, want FailedPrecondition", err)
	}

	// A target that is not this node cannot produce serving evidence.
	outsider := viewHandler(t, 1, migrationViewMetadata{view: installedView(t, nil)})
	outsider.exporter = nil
	outsider.receiver.follower = &Follower{}
	outsider.receiver.serving = pausingIndex{}
	outsider.receiver.localVersion = localVersionStub{}
	if _, err := outsider.prepareMigrationServing(ctx, request, false); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("non-target serving = %v, want FailedPrecondition", err)
	}
}

// localVersionStub is the local cursor report the serving path needs.
type localVersionStub struct{}

func (localVersionStub) LocalVersionOf(string) int64 { return 1 }

// Activation is the last step before membership changes, so the handler refuses any
// task that is not installed-and-served, and answers an already-activated task from
// the committed record instead of re-running the work.
func TestActivateKBMigration_RequiresInstalledAndServed(t *testing.T) {
	ctx := context.Background()
	request := testTransferRequest()

	if _, err := NewNodeHandler(nil, nil).ActivateKBMigration(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("unwired handler = %v, want FailedPrecondition", err)
	}

	view := installView(t, nil)
	viewOnly := viewHandler(t, 3, migrationViewMetadata{view: view})
	viewOnly.migration.metadata = viewOnlyMetadata{}
	if _, err := viewOnly.ActivateKBMigration(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("metadata without maintenance = %v, want FailedPrecondition", err)
	}

	metadata := &cleanupMetadata{directory: view.Directory}
	failing := viewHandler(t, 3, metadata)
	failing.migration.metadata = metadata
	metadata.maintenanceErr = serrors.ErrNoLeaderYet
	if _, err := failing.ActivateKBMigration(ctx, request); status.Code(err) == codes.OK {
		t.Error("an unreadable directory was accepted as activation evidence")
	}

	// An already activated task is answered from the committed serving receipt, and
	// only when the request names exactly that task.
	activated := view.Directory.Clone()
	task := activated.Migrations["move"]
	task.Phase = storage.MigrationActivated
	task.Serve = &storage.MigrationServeReceipt{TargetID: 3, ManifestHash: "manifest", ActiveVersion: 1, IndexReady: true}
	task.Stage = &storage.MigrationStageReceipt{SourceID: 1, TargetID: 3, ManifestHash: "manifest", PayloadHash: "payload"}
	activated.Migrations["move"] = task
	committed := &cleanupMetadata{directory: activated}
	done := viewHandler(t, 3, committed)
	done.migration.metadata = committed
	receipt, err := done.ActivateKBMigration(ctx, request)
	if err != nil {
		t.Fatalf("already activated: %v", err)
	}
	if receipt.GetActiveVersion() != 1 || receipt.GetTargetNodeId() != 3 {
		t.Errorf("receipt = %+v, want the committed serving evidence", receipt)
	}

	// The same task, asked about with a different source, is a conflict rather than
	// an invitation to re-activate somebody else's work.
	wrongSource := &pb.MigrationTransferRequest{KnowledgeBaseId: "kb", OperationId: "move", ExpectedEpoch: 1, SourceNodeId: 2}
	if _, err := done.ActivateKBMigration(ctx, wrongSource); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("wrong source = %v, want FailedPrecondition", err)
	}
	// A task that is not installed cannot be activated at all.
	stagedMetadata := &cleanupMetadata{directory: view.Directory}
	staged := viewHandler(t, 3, stagedMetadata)
	staged.migration.metadata = stagedMetadata
	staged.exporter = nil
	staged.receiver.follower = &Follower{}
	staged.receiver.serving = pausingIndex{}
	staged.receiver.localVersion = localVersionStub{}
	if _, err := staged.ActivateKBMigration(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("staged task = %v, want FailedPrecondition", err)
	}
}
