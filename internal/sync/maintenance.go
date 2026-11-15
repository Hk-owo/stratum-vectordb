package sync

import (
	"context"
	"os"
	"path/filepath"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"strings"
)

type MigrationCleanup interface {
	DropKnowledgeBase(context.Context, string) error
}
type MigrationIndexMaintenance interface {
	PauseKnowledgeBase(context.Context, string) error
	ResumeKnowledgeBase(string)
}

func WithMigrationCleanup(dropper MigrationCleanup) NodeHandlerOption {
	return func(h *NodeHandler) { h.cleanup = dropper }
}

func (h *NodeHandler) CleanupStorage(ctx context.Context, req *pb.CleanupStorageRequest) (*pb.CleanupStorageResponse, error) {
	if h.migration == nil || h.receiver == nil || h.receiver.executionGate == nil {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
	}
	select {
	case h.migration.sem <- struct{}{}:
		defer func() { <-h.migration.sem }()
	case <-ctx.Done():
		return nil, serrors.ToGRPCStatus(ctx.Err())
	}
	if err := h.migration.prepareDirectory(); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	manager, ok := h.migration.metadata.(storage.MaintenanceManager)
	if !ok {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
	}
	d, err := manager.GetStorageMaintenance(ctx)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	task, ok := d.Cleanups[req.GetCleanupId()]
	if !ok || task.NodeID != h.receiver.nodeID {
		return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
	}
	if task.Done {
		return &pb.CleanupStorageResponse{}, nil
	}
	guarded, release, err := h.receiver.executionGate.EnterMaintenance(ctx, task)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	defer release()
	ctx = guarded
	d, err = manager.GetStorageMaintenance(ctx)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	current, ok := d.Cleanups[task.ID]
	if !ok || current != task {
		return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
	}
	if !task.ArtifactsOnly {
		if p, ok := d.Placements[task.KBID]; ok && p.Contains(task.NodeID) {
			return nil, serrors.ToGRPCStatus(serrors.ErrNotKBReplica)
		}
		if h.cleanup == nil {
			return nil, serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
		}
		if indexes, ok := h.receiver.serving.(MigrationIndexMaintenance); ok {
			if err := indexes.PauseKnowledgeBase(ctx, task.KBID); err != nil {
				return nil, serrors.ToGRPCStatus(err)
			}
		}
		if err := h.cleanup.DropKnowledgeBase(ctx, task.KBID); err != nil {
			return nil, serrors.ToGRPCStatus(err)
		}
	}
	if task.OperationID != "" {
		for _, path := range []string{migrationStagePath(h.migration.dir, task.KBID, task.OperationID), migrationInstallPath(h.migration.dir, task.KBID, task.OperationID)} {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return nil, serrors.ToGRPCStatus(err)
			}
		}
		if err := h.migration.syncDirectory(h.migration.dir); err != nil {
			return nil, serrors.ToGRPCStatus(err)
		}
	}
	if err := manager.ProposeAckStorageCleanup(ctx, task.ID); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	return &pb.CleanupStorageResponse{}, nil
}

// Called only under the node's migration semaphore. A previous process can
// leave unfinished temp files, but cannot leave a live writer after restart.
func (s *migrationStager) prepareDirectory() error {
	if s.dir == "" {
		return serrors.ErrPlacementChangeUnsafe
	}
	if err := os.MkdirAll(s.dir, 0700); err != nil {
		return err
	}
	if err := syncMigrationDir(filepath.Dir(s.dir)); err != nil {
		return err
	}
	files, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	changed := false
	for _, f := range files {
		name := f.Name()
		if strings.HasSuffix(name, ".tmp") && (strings.HasPrefix(name, ".transfer-") || strings.HasPrefix(name, ".install-")) {
			if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
				return err
			}
			changed = true
		}
	}
	if changed {
		return syncMigrationDir(s.dir)
	}
	return nil
}
