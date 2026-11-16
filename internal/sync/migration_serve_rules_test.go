package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

// indexStub answers the index surface the serving step needs, with the failure the
// test asks for.
type indexStub struct {
	exists    bool
	existsErr error
	buildErr  error
	builds    int
}

func (i *indexStub) IndexExists(context.Context, string, int64) (bool, error) {
	return i.exists, i.existsErr
}

func (i *indexStub) TriggerBuildBackfill(context.Context, string, int64) error {
	i.builds++
	return i.buildErr
}

// waitMigrationIndex never vouches for an artifact it did not see: a failing probe
// or a failing build keeps the task INSTALLED, and an existing artifact is accepted
// without scheduling anything.
func TestWaitMigrationIndex(t *testing.T) {
	ctx := context.Background()
	manifest := storage.MigrationManifest{KBID: "kb", ActiveVersion: 1}

	failing := &indexStub{existsErr: serrors.ErrIndexNotReady}
	h := viewHandler(t, 3, migrationViewMetadata{})
	h.receiver.serving = failing
	if _, err := h.waitMigrationIndex(ctx, testTransferRequest(), manifest); !errors.Is(err, serrors.ErrIndexNotReady) {
		t.Errorf("probe failure = %v, want it surfaced", err)
	}

	present := &indexStub{exists: true}
	h.receiver.serving = present
	ready, err := h.waitMigrationIndex(ctx, testTransferRequest(), manifest)
	if err != nil || !ready {
		t.Errorf("existing artifact = %v, %v; want it accepted", ready, err)
	}
	if present.builds != 0 {
		t.Error("an existing artifact was rebuilt")
	}

	building := &indexStub{buildErr: serrors.ErrIndexLoadTimeout}
	h.receiver.serving = building
	if _, err := h.waitMigrationIndex(ctx, testTransferRequest(), manifest); !errors.Is(err, serrors.ErrIndexLoadTimeout) {
		t.Errorf("build failure = %v, want it surfaced", err)
	}

	// A build that never finishes leaves the task as it was: the scan gives up
	// inside its budget instead of blocking the scheduler.
	slow := &indexStub{}
	h.receiver.serving = slow
	h.migration.serveWait = 20 * time.Millisecond
	if _, err := h.waitMigrationIndex(ctx, testTransferRequest(), manifest); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Errorf("slow build = %v, want ErrMigrationBusy so the task stays installed", err)
	}
	if slow.builds != 1 {
		t.Errorf("builds = %d, want exactly one attempt per pass", slow.builds)
	}
}

// The serving step asks the transfer view again before recording anything: a task
// that moved on is a conflict, never evidence about the old one.
func TestCheckMigrationServingTask_RefusesAMovedTask(t *testing.T) {
	ctx := context.Background()
	view := transferView(t, nil)
	task := view.Directory.Migrations["move"]
	task.Phase = storage.MigrationInstalled
	task.Stage = &storage.MigrationStageReceipt{SourceID: 1, TargetID: 3, ManifestHash: view.Manifest.Hash(), PayloadHash: view.Manifest.Hash()}
	task.Install = &storage.MigrationInstallReceipt{SourceID: 1, TargetID: 3, ManifestHash: view.Manifest.Hash(), StageHash: view.Manifest.Hash()}
	view.Directory.Migrations["move"] = task

	h := viewHandler(t, 3, migrationViewMetadata{view: view})
	if err := h.checkMigrationServingTask(ctx, testTransferRequest(), task, view.Manifest); err != nil {
		t.Fatalf("matching task: %v", err)
	}
	// A different install receipt is another artifact's evidence.
	other := task
	foreign := *task.Install
	foreign.StageHash = "other"
	other.Install = &foreign
	if err := h.checkMigrationServingTask(ctx, testTransferRequest(), other, view.Manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("foreign install receipt = %v, want ErrMigrationConflict", err)
	}
	// A manifest that no longer matches the frozen one is a conflict too.
	if err := h.checkMigrationServingTask(ctx, testTransferRequest(), task, storage.MigrationManifest{KBID: "kb", ActiveVersion: 9}); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("different manifest = %v, want ErrMigrationConflict", err)
	}
	// A task the directory no longer reports as installed is refused.
	moved := transferView(t, nil)
	h2 := viewHandler(t, 3, migrationViewMetadata{view: moved})
	if err := h2.checkMigrationServingTask(ctx, testTransferRequest(), task, moved.Manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("staged task = %v, want ErrMigrationConflict", err)
	}
}
