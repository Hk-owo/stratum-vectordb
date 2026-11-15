package index

import (
	"context"
	"stratum/internal/storage"
	"testing"
)

func TestMaintenancePausesBuildsAndNewImportResumesDeletedKB(t *testing.T) {
	im := newIndexManagerWithData(t)
	defer im.Close()
	ctx := context.Background()
	if err := im.PauseKnowledgeBase(ctx, "kb"); err != nil {
		t.Fatal(err)
	}
	if err := im.TriggerBuildBackfill(ctx, "kb", 1); err == nil {
		t.Fatal("cleanup admitted a new build")
	}
	if err := im.InstallIndex(ctx, "kb", 1, []byte("old"), nil); err == nil {
		t.Fatal("cleanup admitted a late index install")
	}
	im.mu.Lock()
	im.deletedKBs["kb"] = true
	im.deletedVersions[indexKey{"kb", 1}] = true
	im.mu.Unlock()
	im.ResumeKnowledgeBase("kb")
	im.mu.Lock()
	blocked := im.pausedKB["kb"] || im.deletedKBs["kb"] || im.deletedVersions[indexKey{"kb", 1}]
	im.mu.Unlock()
	if blocked {
		t.Fatal("new import inherited old cleanup tombstones")
	}
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service := storage.NewServiceLease(1, "s1")
	gate.SetServiceLease(service)
	im.SetServiceLease(service)
	c, release, err := gate.EnterMaintenance(ctx, storage.Cleanup{ID: "cleanup", KBID: "kb", Epoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := im.EvictByKB(c, "kb"); err != nil {
		t.Fatal("revoked service could not perform authorized cleanup", err)
	}
	release()
	if err := service.CheckContext(c); err == nil {
		t.Fatal("released cleanup context bypasses closed lease")
	}
}
