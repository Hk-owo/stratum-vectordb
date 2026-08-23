package coordinator

import (
	"context"
	"testing"

	"stratum/internal/docstore"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"
)

// buildDeleteVersionFixture wires a MockRaftNode with a version chain
// v1 -> v2 -> v3 (v1 READY and active, v2/v3 READY), pre-populated
// docstore and versiondoc data, and returns everything the coordinator
// needs plus the freshly marked-Deleting v2/v3 state.
func buildDeleteVersionFixture(t *testing.T) (*DeleteVersionCoordinatorImpl, *raft.MockRaftNode, *docstore.MockDocStore, *versiondoc.MockVersionDocList, *deleteTestIndexManager, *wal.MockWAL, int64, int64) {
	t.Helper()
	ctx := context.Background()
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	ds := docstore.NewMockDocStore()
	vdl := versiondoc.NewMockVersionDocList()
	im := newDeleteTestIndexManager()

	// KB + version chain.
	if err := rn.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "kb-1", ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"}, Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	v1, err := rn.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatalf("create v1: %v", err)
	}
	// Simulate the storage writes completing (WriteCoordinator would write
	// these commits); otherwise Recover reports pending VersionWrite records.
	if err := w.WriteCommit(ctx, v1); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if err := rn.ProposeUpdateVersionStatus(ctx, v1, types.IndexStatusReady); err != nil {
		t.Fatalf("v1 READY: %v", err)
	}
	if err := rn.ProposeRollback(ctx, "kb-1", v1); err != nil {
		t.Fatalf("rollback to v1: %v", err)
	}
	v2, err := rn.ProposeCreateVersion(ctx, "kb-1", v1)
	if err != nil {
		t.Fatalf("create v2: %v", err)
	}
	if err := w.WriteCommit(ctx, v2); err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	if err := rn.ProposeUpdateVersionStatus(ctx, v2, types.IndexStatusReady); err != nil {
		t.Fatalf("v2 READY: %v", err)
	}
	v3, err := rn.ProposeCreateVersion(ctx, "kb-1", v2)
	if err != nil {
		t.Fatalf("create v3: %v", err)
	}
	if err := w.WriteCommit(ctx, v3); err != nil {
		t.Fatalf("commit v3: %v", err)
	}
	if err := rn.ProposeUpdateVersionStatus(ctx, v3, types.IndexStatusReady); err != nil {
		t.Fatalf("v3 READY: %v", err)
	}

	// Storage data for all three versions.
	mustWriteDoc(t, ds, "kb-1", "doc-a", v1, "a-v1")
	mustWriteDoc(t, ds, "kb-1", "doc-b", v2, "b-v2")
	mustWriteDoc(t, ds, "kb-1", "doc-c", v3, "c-v3")
	for _, docID := range []string{"doc-a", "doc-b"} {
		if err := vdl.Write(ctx, "kb-1", v2, docID); err != nil {
			t.Fatalf("vdl v2 write: %v", err)
		}
	}
	if err := vdl.Write(ctx, "kb-1", v3, "doc-c"); err != nil {
		t.Fatalf("vdl v3 write: %v", err)
	}

	// Mark v2 for deletion (recursively marks v3).
	if err := rn.ProposeMarkVersionDeleting(ctx, "kb-1", v2); err != nil {
		t.Fatalf("mark v2 deleting: %v", err)
	}

	coord := NewDeleteVersionCoordinatorImpl(DeleteVersionCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		VersionDocList:      vdl,
	})
	return coord, rn, ds, vdl, im, w, v2, v3
}

func mustWriteDoc(t *testing.T, ds *docstore.MockDocStore, kbID, docID string, versionID int64, content string) {
	t.Helper()
	if err := ds.Write(context.Background(), kbID, docID, versionID, []byte(content)); err != nil {
		t.Fatalf("docstore write %s/%s v%d: %v", kbID, docID, versionID, err)
	}
}

func TestDeleteVersionCoordinator_FullCleanup(t *testing.T) {
	ctx := context.Background()
	coord, rn, ds, vdl, im, w, v2, v3 := buildDeleteVersionFixture(t)

	if err := coord.Execute(ctx, "kb-1"); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Raft state machine: v2 and v3 gone, v1 (active) untouched.
	versions, err := rn.ListVersions(ctx, "kb-1")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("ListVersions after cleanup = %v, want exactly one version", versions)
	}
	// The surviving version must still be the active one (v1).
	kb, err := rn.GetKB(ctx, "kb-1")
	if err != nil {
		t.Fatalf("GetKB: %v", err)
	}
	if versions[0].VersionID != kb.ActiveVersionID {
		t.Errorf("surviving version %d != active %d", versions[0].VersionID, kb.ActiveVersionID)
	}

	// Index discarded for both deleted versions.
	if len(im.discarded) != 2 {
		t.Fatalf("Discard calls = %v, want 2", im.discarded)
	}
	seen := map[int64]bool{}
	for _, d := range im.discarded {
		if d.kbID != "kb-1" {
			t.Errorf("Discard for wrong KB %q", d.kbID)
		}
		seen[d.versionID] = true
	}
	if !seen[v2] || !seen[v3] {
		t.Errorf("Discard versions = %v, want v2=%d and v3=%d", im.discarded, v2, v3)
	}

	// VersionDocList: v2/v3 doc sets removed.
	for _, v := range []int64{v2, v3} {
		ids, err := vdl.ListDocIDs(ctx, "kb-1", v)
		if err != nil {
			t.Fatalf("ListDocIDs(v%d): %v", v, err)
		}
		if len(ids) != 0 {
			t.Errorf("VersionDocList for v%d = %v, want empty", v, ids)
		}
	}

	// DocStore: v2/v3 records physically gone; v1 record intact.
	if _, err := ds.ReadAt(ctx, "kb-1", "doc-b", v2); err == nil {
		t.Error("doc-b@v2 should be gone after DeleteByVersion")
	}
	if _, err := ds.ReadAt(ctx, "kb-1", "doc-c", v3); err == nil {
		t.Error("doc-c@v3 should be gone after DeleteByVersion")
	}
	if content, err := ds.ReadAt(ctx, "kb-1", "doc-a", kb.ActiveVersionID); err != nil || string(content) != "a-v1" {
		t.Errorf("doc-a@active = %q, %v; want a-v1", content, err)
	}

	// WAL: delete marks have matching completes — nothing pending.
	pending, err := w.Recover(ctx)
	if err != nil {
		t.Fatalf("WAL Recover: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("pending records = %v, want none", pending)
	}

	// Idempotent: re-running finds no Deleting versions and succeeds.
	if err := coord.Execute(ctx, "kb-1"); err != nil {
		t.Errorf("second Execute: %v", err)
	}
}

func TestDeleteVersionCoordinator_CrashResume(t *testing.T) {
	ctx := context.Background()
	// Fixture already has v2/v3 marked Deleting. Simulate a crash right
	// after the delete mark was written but before any cleanup ran.
	coord, rn, _, _, _, w, v2, _ := buildDeleteVersionFixture(t)
	if err := w.WriteVersionDeleteMark(ctx, "kb-1", v2); err != nil {
		t.Fatalf("WriteVersionDeleteMark: %v", err)
	}

	// The WAL reports the unfinished deletion…
	pending, err := w.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(pending) == 0 {
		t.Fatal("expected a pending VersionDelete record after crash")
	}

	// …and re-running Execute resumes and completes the cleanup.
	if err := coord.Execute(ctx, "kb-1"); err != nil {
		t.Fatalf("Execute after crash: %v", err)
	}
	versions, err := rn.ListVersions(ctx, "kb-1")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("ListVersions after resume = %v, want only v1", versions)
	}
	pending, err = w.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("pending records = %v, want none after resumed cleanup", pending)
	}
}
