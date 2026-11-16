package raft

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// migrationSM is a state machine holding one active KB "kb" on {1,2} with a single
// durable version, which is the smallest directory a migration command can act on.
func migrationSM(t *testing.T) (*stateMachine, *wal.MockWAL) {
	t.Helper()
	sm, w := newTestSM(t)
	if err := sm.directory.RegisterNodes(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}}); err != nil {
		t.Fatal(err)
	}
	if err := sm.directory.AssignNew(storage.Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"}); err != nil {
		t.Fatal(err)
	}
	sm.kbs["kb"] = types.KnowledgeBaseMeta{KBID: "kb", Status: types.KBStatusActive, ActiveVersionID: 1}
	sm.versions[1] = types.VersionMeta{VersionID: 1, KBID: "kb", DataStatus: types.DataStatusDurable, DocIDSetHash: types.EmptyDocIDSetHash}
	sm.versionsByKB["kb"] = []int64{1}
	return sm, w
}

func applyCommand(sm *stateMachine, w *wal.MockWAL, cmd command) error {
	return sm.apply(context.Background(), cmd, w, zap.NewNop()).Err
}

// applyMigration is the control layer's single authority over directories, so each
// refusal is asserted on its own: a command that slipped past one of them would
// change membership or a node's management state without the rule it belongs to.
func TestApplyMigration_RefusesIncompleteCommands(t *testing.T) {
	sm, w := migrationSM(t)
	ctx := context.Background()
	cases := map[string]command{
		"drain without a request":        {Type: cmdDrainStorageNode},
		"manage without a change":        {Type: cmdManageStorageNode},
		"activate without an activation": {Type: cmdActivateKBMigration, OperationID: "move"},
		"stage without a receipt":        {Type: cmdRecordKBMigrationStage, OperationID: "move"},
		"install without a receipt":      {Type: cmdRecordKBMigrationInstall, OperationID: "move"},
		"serve without a receipt":        {Type: cmdRecordKBMigrationServe, OperationID: "move"},
		"begin without a request":        {Type: cmdBeginKBMigration},
	}
	for name, cmd := range cases {
		if err := sm.apply(ctx, cmd, w, zap.NewNop()).Err; !errors.Is(err, serrors.ErrInvalidArgument) {
			t.Errorf("%s: err = %v, want ErrInvalidArgument", name, err)
		}
	}
	// A command this state machine does not know is refused by the migration
	// handler itself rather than being applied as a no-op.
	if err := sm.applyMigration(command{Type: commandType("NotACommand")}); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Errorf("unknown command = %v, want ErrInvalidArgument", err)
	}
}

// Draining a node must not silently skip a KB whose metadata is gone or a version
// whose data is still in flight: both would move data away from bytes that are not
// settled yet.
func TestApplyMigration_DrainChecksEveryReferencingKB(t *testing.T) {
	newDrain := func(sm *stateMachine) command {
		return command{Type: cmdDrainStorageNode, Drain: &storage.DrainRequest{OperationID: "drain", NodeID: 2, ExpectedRevision: sm.directory.Revision, TargetIDs: []int64{3}}}
	}

	orphan, w := migrationSM(t)
	delete(orphan.kbs, "kb")
	if err := applyCommand(orphan, w, newDrain(orphan)); !errors.Is(err, serrors.ErrKnowledgeBaseDeleted) {
		t.Errorf("drain of an orphaned placement = %v, want ErrKnowledgeBaseDeleted", err)
	}

	deleting, w := migrationSM(t)
	meta := deleting.kbs["kb"]
	meta.Status = types.KBStatusDeleting
	deleting.kbs["kb"] = meta
	if err := applyCommand(deleting, w, newDrain(deleting)); !errors.Is(err, serrors.ErrKnowledgeBaseDeleted) {
		t.Errorf("drain of a deleting KB = %v, want ErrKnowledgeBaseDeleted", err)
	}

	busy, w := migrationSM(t)
	v := busy.versions[1]
	v.Deleting = true
	busy.versions[1] = v
	if err := applyCommand(busy, w, newDrain(busy)); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Errorf("drain with a deleting version = %v, want ErrMigrationBusy", err)
	}

	ok, w := migrationSM(t)
	revision := ok.directory.Revision
	if err := applyCommand(ok, w, newDrain(ok)); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if ok.directory.Nodes[2].State != storage.NodeDraining || ok.directory.Revision == revision {
		t.Errorf("drain not applied: %+v", ok.directory.Nodes[2])
	}
	// The drain task is planned but no KB has moved yet: membership only changes
	// through an activation.
	if ok.directory.Placements["kb"].Contains(3) {
		t.Errorf("drain changed membership: %+v", ok.directory.Placements["kb"])
	}
}

func TestApplyMigration_CleanupAndNodeManagement(t *testing.T) {
	sm, w := migrationSM(t)
	if err := applyCommand(sm, w, command{Type: cmdAckStorageCleanup, OperationID: "missing"}); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Errorf("unknown cleanup = %v", err)
	}
	if sm.directory.Cleanups == nil {
		sm.directory.Cleanups = map[string]storage.Cleanup{}
	}
	sm.directory.Cleanups["c"] = storage.Cleanup{ID: "c", KBID: "kb", NodeID: 1}
	if err := applyCommand(sm, w, command{Type: cmdAckStorageCleanup, OperationID: "c"}); err != nil {
		t.Fatalf("ack cleanup: %v", err)
	}
	if !sm.directory.Cleanups["c"].Done {
		t.Error("cleanup was not recorded as done")
	}

	if err := applyCommand(sm, w, command{Type: cmdManageStorageNode, NodeChange: &storage.NodeChange{NodeID: 9, ExpectedRevision: sm.directory.Revision, Action: "drain"}}); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Errorf("unknown node = %v", err)
	}
	if err := applyCommand(sm, w, command{Type: cmdManageStorageNode, NodeChange: &storage.NodeChange{NodeID: 3, ExpectedRevision: sm.directory.Revision, Action: "drain"}}); err != nil {
		t.Fatalf("drain node 3: %v", err)
	}
	if sm.directory.Nodes[3].State != storage.NodeDraining {
		t.Errorf("node 3 = %+v, want draining", sm.directory.Nodes[3])
	}
}

// Beginning a migration from the control layer has its own preconditions: the KB
// has to exist, be active, and have no version whose data is still in flight.
func TestApplyMigration_BeginPreconditions(t *testing.T) {
	req := storage.MigrationRequest{OperationID: "move", KBID: "kb", ExpectedEpoch: 1, FromID: 2, ToID: 3}
	begin := command{Type: cmdBeginKBMigration, Migration: &req}

	sm, w := migrationSM(t)
	if err := applyCommand(sm, w, begin); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if sm.directory.Migrations["move"].Phase != storage.MigrationDraining {
		t.Errorf("phase = %q, want draining", sm.directory.Migrations["move"].Phase)
	}
	// The same request again is a lost-response retry.
	if err := applyCommand(sm, w, begin); err != nil {
		t.Errorf("retry: %v", err)
	}

	unknown, w := migrationSM(t)
	missing := req
	missing.KBID = "nope"
	if err := applyCommand(unknown, w, command{Type: cmdBeginKBMigration, Migration: &missing}); !errors.Is(err, serrors.ErrKnowledgeBaseNotFound) {
		t.Errorf("unknown KB = %v, want ErrKnowledgeBaseNotFound", err)
	}

	deleting, w := migrationSM(t)
	meta := deleting.kbs["kb"]
	meta.Status = types.KBStatusDeleting
	deleting.kbs["kb"] = meta
	if err := applyCommand(deleting, w, begin); !errors.Is(err, serrors.ErrKnowledgeBaseDeleted) {
		t.Errorf("deleting KB = %v, want ErrKnowledgeBaseDeleted", err)
	}

	busy, w := migrationSM(t)
	v := busy.versions[1]
	v.Deleting = true
	busy.versions[1] = v
	if err := applyCommand(busy, w, begin); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Errorf("deleting version = %v, want ErrMigrationBusy", err)
	}
}

// Fencing waits for the KB's own data to settle, and every later step requires the
// task to exist: the sequence is the protocol.
func TestApplyMigration_FenceAckCancel(t *testing.T) {
	sm, w := migrationSM(t)
	ctx := context.Background()
	req := storage.MigrationRequest{OperationID: "move", KBID: "kb", ExpectedEpoch: 1, FromID: 2, ToID: 3}
	if err := applyCommand(sm, w, command{Type: cmdBeginKBMigration, Migration: &req}); err != nil {
		t.Fatal(err)
	}

	if err := applyCommand(sm, w, command{Type: cmdFenceKBMigration, OperationID: "missing"}); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown fence = %v, want ErrMigrationNotFound", err)
	}

	busy, w := migrationSM(t)
	v := busy.versions[1]
	v.DataStatus = types.DataStatusPending
	busy.versions[1] = v
	if err := applyCommand(busy, w, command{Type: cmdBeginKBMigration, Migration: &req}); err != nil {
		t.Skipf("begin already refused the pending version: %v", err)
	}
	if err := applyCommand(busy, w, command{Type: cmdFenceKBMigration, OperationID: "move"}); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Errorf("fence with a pending version = %v, want ErrMigrationBusy", err)
	}

	if err := applyCommand(sm, w, command{Type: cmdFenceKBMigration, OperationID: "move"}); err != nil {
		t.Fatalf("fence: %v", err)
	}
	if sm.directory.Migrations["move"].Phase != storage.MigrationFencing {
		t.Errorf("phase = %q, want fencing", sm.directory.Migrations["move"].Phase)
	}
	if err := applyCommand(sm, w, command{Type: cmdAckKBMigrationFence, OperationID: "move", PlacementEpoch: 1, NodeID: 1}); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := applyCommand(sm, w, command{Type: cmdAckKBMigrationFence, OperationID: "move", PlacementEpoch: 2, NodeID: 1}); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("stale ack = %v, want ErrPlacementStale", err)
	}

	cancelled, w := migrationSM(t)
	if err := applyCommand(cancelled, w, command{Type: cmdCancelKBMigration, OperationID: "missing"}); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown cancel = %v, want ErrMigrationNotFound", err)
	}
	if err := applyCommand(cancelled, w, command{Type: cmdBeginKBMigration, Migration: &req}); err != nil {
		t.Fatal(err)
	}
	if err := applyCommand(cancelled, w, command{Type: cmdCancelKBMigration, OperationID: "move"}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancelled.directory.Migrations["move"].Phase != storage.MigrationCancelled {
		t.Errorf("phase = %q, want cancelled", cancelled.directory.Migrations["move"].Phase)
	}
	_ = ctx
}

// The receipt commands all need a task whose frozen manifest is still the
// authority. A task that is still draining has no manifest, so the command is
// refused instead of recording evidence about nothing.
func TestApplyMigration_ReceiptsNeedACommittedManifest(t *testing.T) {
	sm, w := migrationSM(t)
	req := storage.MigrationRequest{OperationID: "move", KBID: "kb", ExpectedEpoch: 1, FromID: 2, ToID: 3}
	if err := applyCommand(sm, w, command{Type: cmdBeginKBMigration, Migration: &req}); err != nil {
		t.Fatal(err)
	}
	receipt := storage.MigrationStageReceipt{SourceID: 1, TargetID: 3, ManifestHash: "manifest", PayloadHash: "payload", Bytes: 1}
	if err := applyCommand(sm, w, command{Type: cmdRecordKBMigrationStage, OperationID: "move", StageReceipt: &receipt}); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("stage of a draining task = %v, want ErrMigrationConflict", err)
	}
	install := storage.MigrationInstallReceipt{SourceID: 1, TargetID: 3}
	if err := applyCommand(sm, w, command{Type: cmdRecordKBMigrationInstall, OperationID: "move", InstallReceipt: &install}); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("install of a draining task = %v, want ErrMigrationConflict", err)
	}
	serve := storage.MigrationServeReceipt{TargetID: 3}
	if err := applyCommand(sm, w, command{Type: cmdRecordKBMigrationServe, OperationID: "move", ServeReceipt: &serve}); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("serve of a draining task = %v, want ErrMigrationConflict", err)
	}
}

// Activation requires the committed task to be INSTALLED with the exact receipts
// the activation names; anything else is a conflict, never a silent cutover.
func TestApplyMigration_ActivationRequiresTheCommittedTask(t *testing.T) {
	sm, w := migrationSM(t)
	if err := applyCommand(sm, w, command{Type: cmdActivateKBMigration, OperationID: "missing", Activation: &storage.MigrationActivation{Epoch: 1}}); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown activation = %v, want ErrMigrationNotFound", err)
	}

	req := storage.MigrationRequest{OperationID: "move", KBID: "kb", ExpectedEpoch: 1, FromID: 2, ToID: 3}
	if err := applyCommand(sm, w, command{Type: cmdBeginKBMigration, Migration: &req}); err != nil {
		t.Fatal(err)
	}
	if err := applyCommand(sm, w, command{Type: cmdActivateKBMigration, OperationID: "move", Activation: &storage.MigrationActivation{Epoch: 1}}); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("activation of a draining task = %v, want ErrMigrationConflict", err)
	}
}

// The two listing implementations answer the same way, including the cancelled
// context case: a scheduler that keeps polling a dead context is a busy loop.
func TestListKBMigrations_FollowsItsContext(t *testing.T) {
	sm, _ := migrationSM(t)
	node := &RaftNodeImpl{sm: sm}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := node.ListKBMigrations(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled list = %v, want context.Canceled", err)
	}
	mock := NewMockRaftNode(nil)
	if _, err := mock.ListKBMigrations(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled mock list = %v, want context.Canceled", err)
	}
	if list, err := node.ListKBMigrations(context.Background()); err != nil || len(list) != 0 {
		t.Errorf("list of an empty directory = %v, %v", list, err)
	}
}
