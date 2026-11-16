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

// cleanupMetadata serves one directory snapshot and records the acknowledgements,
// so the cleanup RPC can be driven without a control plane.
type cleanupMetadata struct {
	migrationFixtureMetadata
	directory      storage.Directory
	maintenanceErr error
	ackErr         error
	acked          []string
	reads          int
	onLaterRead    func(*cleanupMetadata)
}

func (m *cleanupMetadata) GetStorageMaintenance(context.Context) (storage.Directory, error) {
	m.reads++
	if m.reads > 1 && m.onLaterRead != nil {
		m.onLaterRead(m)
	}
	return m.directory.Clone(), m.maintenanceErr
}

func (m *cleanupMetadata) ProposeAckStorageCleanup(_ context.Context, id string) error {
	m.acked = append(m.acked, id)
	return m.ackErr
}

// viewOnlyMetadata answers the transfer view only: a node wired without the
// maintenance surface must be told so, not have the assertion blow up later.
type viewOnlyMetadata struct{}

func (viewOnlyMetadata) GetKBMigrationTransfer(context.Context, string) (storage.MigrationTransferView, error) {
	return storage.MigrationTransferView{}, nil
}

// pausingIndex stands in for the index surface: the cleanup path pauses a KB's
// builds before deleting its artifact, and that pause can fail.
type pausingIndex struct{ pauseErr error }

func (pausingIndex) IndexExists(context.Context, string, int64) (bool, error) { return true, nil }
func (pausingIndex) TriggerBuildBackfill(context.Context, string, int64) error {
	return nil
}
func (i pausingIndex) PauseKnowledgeBase(context.Context, string) error { return i.pauseErr }
func (pausingIndex) ResumeKnowledgeBase(string)                         {}

func cleanupHandler(t *testing.T, metadata *cleanupMetadata, task storage.Cleanup, nodeID int64) (*NodeHandler, *storage.ExecutionGate, *kbDropper) {
	t.Helper()
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	metadata.directory = storage.NewDirectory()
	if err := metadata.directory.RegisterNodes(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: nodeID, Addr: "s2"}}); err != nil {
		t.Fatal(err)
	}
	if task.ID != "" {
		metadata.directory.Cleanups = map[string]storage.Cleanup{task.ID: task}
	}
	receiver := NewPushHandler(nil, nodeID)
	receiver.executionGate = gate
	handler := NewNodeHandler(nil, receiver, WithMigrationStaging(metadata, t.TempDir()))
	dropper := &kbDropper{}
	WithMigrationCleanup(dropper)(handler)
	return handler, gate, dropper
}

// The cleanup RPC is what actually deletes a replica's bytes, so every precondition
// is asserted: an admitted cleanup that should have been refused deletes data the
// directory still considers live.
func TestCleanupStorage_RefusesWorkItDoesNotOwn(t *testing.T) {
	ctx := context.Background()
	request := &pb.CleanupStorageRequest{CleanupId: "c"}
	task := storage.Cleanup{ID: "c", KBID: "kb", NodeID: 2, Epoch: 2, ArtifactsOnly: true}

	if _, err := NewNodeHandler(nil, nil).CleanupStorage(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("unwired handler = %v, want FailedPrecondition", err)
	}

	// A staged directory that cannot be prepared stops the cleanup: removing files
	// from an unknown layout is how a live artifact gets deleted by mistake.
	unprepared, _, _ := cleanupHandler(t, &cleanupMetadata{}, task, 2)
	unprepared.migration.dir = ""
	if _, err := unprepared.CleanupStorage(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("empty staging directory = %v, want FailedPrecondition", err)
	}

	// The metadata has to be able to answer the maintenance read at all.
	viewOnly, _, _ := cleanupHandler(t, &cleanupMetadata{}, task, 2)
	viewOnly.migration.metadata = viewOnlyMetadata{}
	if _, err := viewOnly.CleanupStorage(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("metadata without maintenance = %v, want FailedPrecondition", err)
	}

	cases := []struct {
		name   string
		task   storage.Cleanup
		mutate func(m *cleanupMetadata, h *NodeHandler)
		want   codes.Code
	}{
		{
			name:   "maintenance read fails",
			task:   task,
			mutate: func(m *cleanupMetadata, _ *NodeHandler) { m.maintenanceErr = serrors.ErrNoLeaderYet },
			want:   codes.Unavailable,
		},
		{
			name: "unknown cleanup",
			task: storage.Cleanup{},
			want: codes.FailedPrecondition,
		},
		{
			name: "cleanup for another node",
			task: task,
			mutate: func(m *cleanupMetadata, _ *NodeHandler) {
				m.directory.Cleanups["c"] = storage.Cleanup{ID: "c", KBID: "kb", NodeID: 3, Epoch: 2}
			},
			want: codes.FailedPrecondition,
		},
		{
			// An artifacts-only cleanup deliberately ignores a newer fence: it only
			// deletes files. A full cleanup may not, because it drops data.
			name: "fence moved past a full cleanup",
			task: storage.Cleanup{ID: "c", KBID: "kb", NodeID: 2, Epoch: 2},
			mutate: func(_ *cleanupMetadata, h *NodeHandler) {
				if err := h.receiver.executionGate.Fence(context.Background(), "kb", 5, "newer"); err != nil {
					t.Fatalf("seed fence: %v", err)
				}
			},
			want: codes.Aborted,
		},
		{
			// The task is re-read under the maintenance scope: the value read before
			// waiting for the gate is not evidence after it.
			name: "the task changed while waiting for the gate",
			task: task,
			mutate: func(m *cleanupMetadata, _ *NodeHandler) {
				m.onLaterRead = func(m *cleanupMetadata) {
					m.directory.Cleanups["c"] = storage.Cleanup{ID: "c", KBID: "kb", NodeID: 2, Epoch: 3, ArtifactsOnly: true}
				}
			},
			want: codes.FailedPrecondition,
		},
		{
			name: "a full cleanup still finds the KB placed here",
			task: storage.Cleanup{ID: "c", KBID: "kb", NodeID: 2, Epoch: 2},
			mutate: func(m *cleanupMetadata, _ *NodeHandler) {
				m.directory.Placements["kb"] = storage.Placement{KBID: "kb", Epoch: 3, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"}
			},
			want: codes.FailedPrecondition,
		},
		{
			name:   "no dropper wired for a full cleanup",
			task:   storage.Cleanup{ID: "c", KBID: "kb", NodeID: 2, Epoch: 2},
			mutate: func(_ *cleanupMetadata, h *NodeHandler) { h.cleanup = nil },
			want:   codes.FailedPrecondition,
		},
		{
			name: "index maintenance refuses to pause",
			task: storage.Cleanup{ID: "c", KBID: "kb", NodeID: 2, Epoch: 2},
			mutate: func(_ *cleanupMetadata, h *NodeHandler) {
				h.receiver.serving = pausingIndex{pauseErr: serrors.ErrIndexMaintenance}
			},
			want: codes.FailedPrecondition,
		},
		{
			name:   "recording the acknowledgement fails",
			task:   task,
			mutate: func(m *cleanupMetadata, _ *NodeHandler) { m.ackErr = serrors.ErrPlacementStale },
			want:   codes.Aborted,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &cleanupMetadata{}
			handler, _, _ := cleanupHandler(t, metadata, tc.task, 2)
			if tc.mutate != nil {
				tc.mutate(metadata, handler)
			}
			_, err := handler.CleanupStorage(ctx, request)
			if status.Code(err) != tc.want {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if tc.want == codes.OK && len(metadata.acked) != 1 {
				t.Errorf("acked = %v, want the cleanup acknowledged once", metadata.acked)
			}
		})
	}

	// A cleanup already acknowledged is a no-op, not an error: the control layer
	// retries it after every leader change.
	done := storage.Cleanup{ID: "c", KBID: "kb", NodeID: 2, Epoch: 2, ArtifactsOnly: true, Done: true}
	handler, _, _ := cleanupHandler(t, &cleanupMetadata{}, done, 2)
	if _, err := handler.CleanupStorage(ctx, request); err != nil {
		t.Errorf("already done = %v", err)
	}
}

// The successful paths: an artifacts-only cleanup deletes one operation's files, a
// full cleanup additionally drops the KB's local copy through the injected dropper.
func TestCleanupStorage_DeletesArtifactsAndDropsTheKnowledgeBase(t *testing.T) {
	ctx := context.Background()
	request := &pb.CleanupStorageRequest{CleanupId: "c"}

	artifacts := storage.Cleanup{ID: "c", KBID: "kb", NodeID: 2, Epoch: 2, ArtifactsOnly: true, OperationID: "move"}
	metadata := &cleanupMetadata{}
	handler, _, dropper := cleanupHandler(t, metadata, artifacts, 2)
	stagePath := migrationStagePath(handler.migration.dir, "kb", "move")
	installPath := migrationInstallPath(handler.migration.dir, "kb", "move")
	for _, path := range []string{stagePath, installPath} {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("artifact"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := handler.CleanupStorage(ctx, request); err != nil {
		t.Fatalf("artifacts-only cleanup: %v", err)
	}
	for _, path := range []string{stagePath, installPath} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s was not removed: %v", filepath.Base(path), err)
		}
	}
	if len(metadata.acked) != 1 || metadata.acked[0] != "c" {
		t.Errorf("acked = %v, want the cleanup recorded", metadata.acked)
	}
	// An artifacts-only cleanup never drops the KB's own copy.
	if len(dropper.dropped) != 0 {
		t.Errorf("dropped = %v, want nothing for an artifacts-only cleanup", dropper.dropped)
	}

	full := storage.Cleanup{ID: "c", KBID: "kb", NodeID: 2, Epoch: 2}
	fullMetadata := &cleanupMetadata{}
	fullHandler, _, fullDropper := cleanupHandler(t, fullMetadata, full, 2)
	if _, err := fullHandler.CleanupStorage(ctx, request); err != nil {
		t.Fatalf("full cleanup: %v", err)
	}
	if len(fullDropper.dropped) != 1 || fullDropper.dropped[0] != "kb" {
		t.Errorf("dropped = %v, want the KB dropped once", fullDropper.dropped)
	}
	if len(fullMetadata.acked) != 1 {
		t.Errorf("acked = %v", fullMetadata.acked)
	}

	// A cancelled call does not wait for the semaphore: the client is gone.
	busy := artifacts
	busyHandler, _, _ := cleanupHandler(t, &cleanupMetadata{}, busy, 2)
	busyHandler.migration.sem <- struct{}{}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	// A cancelled call does not wait for the semaphore: the client is gone, and
	// the error carries that through rather than looking like a placement problem.
	if _, err := busyHandler.CleanupStorage(cancelled, request); err == nil || status.Code(err) == codes.OK {
		t.Errorf("cancelled cleanup = %v, want an error", err)
	}
	<-busyHandler.migration.sem
}

// A previous process can leave temp files behind; they must be swept before a new
// transfer starts, and nothing else in the directory may be touched.
func TestMigrationStager_PrepareDirectorySweepsOnlyTempFiles(t *testing.T) {
	dir := t.TempDir()
	stager := &migrationStager{dir: dir}
	keep := filepath.Join(dir, "keep.me")
	for _, name := range []string{".transfer-1.tmp", ".install-2.tmp", ".other.tmp", "artifact.stage"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(keep, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := stager.prepareDirectory(); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	remaining, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, entry := range remaining {
		names[entry.Name()] = true
	}
	if names[".transfer-1.tmp"] || names[".install-2.tmp"] {
		t.Errorf("transfer temp files survived the sweep: %v", names)
	}
	for _, want := range []string{".other.tmp", "artifact.stage", "keep.me"} {
		if !names[want] {
			t.Errorf("%s was swept away", want)
		}
	}

	// A directory that cannot be created is refused before anything is deleted.
	blocked := filepath.Join(dir, "file-as-dir")
	if err := os.WriteFile(blocked, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := (&migrationStager{dir: blocked}).prepareDirectory(); err == nil {
		t.Error("a file used as the staging directory was accepted")
	}
}
