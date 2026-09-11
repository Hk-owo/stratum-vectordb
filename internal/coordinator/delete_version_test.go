package coordinator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"stratum/internal/bloom"
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

	// Mark v2 for deletion (SUBTREE mode: recursively marks v3 as well).
	if _, err := rn.ProposeMarkVersionDeleting(ctx, "kb-1", v2, types.VersionDeleteSubtree); err != nil {
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

// TestDeleteVersionCoordinator_KeepsRecordsSurvivorReads pins the
// dependency-aware reclaim: removing a middle version (SINGLE) must not
// delete the MVCC records its surviving child still reads. Those records are
// written incrementally (only a version's own changes) and read by falling
// back through versions, so reclaiming them would silently drop documents
// from — or serve stale content to — the surviving version.
func TestDeleteVersionCoordinator_KeepsRecordsSurvivorReads(t *testing.T) {
	ctx := context.Background()
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	ds := docstore.NewMockDocStore()
	vdl := versiondoc.NewMockVersionDocList()
	im := newDeleteTestIndexManager()

	if err := rn.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "kb-1", ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"}, Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}

	// v1 introduces doc-a; v2 introduces doc-b (doc-a untouched); v3
	// introduces doc-c. Mirrors the write path: only changed documents get
	// an MVCC entry.
	v1, err := rn.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatalf("create v1: %v", err)
	}
	if err := w.WriteCommit(ctx, v1); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if err := rn.ProposeUpdateVersionStatus(ctx, v1, types.IndexStatusReady); err != nil {
		t.Fatalf("v1 READY: %v", err)
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
	if err := rn.ProposeRollback(ctx, "kb-1", v3); err != nil {
		t.Fatalf("rollback to v3: %v", err)
	}

	mustWriteDoc(t, ds, "kb-1", "doc-a", v1, "a-v1")
	mustWriteDoc(t, ds, "kb-1", "doc-b", v2, "b-v2")
	mustWriteDoc(t, ds, "kb-1", "doc-c", v3, "c-v3")
	for _, d := range []string{"doc-a"} {
		if err := vdl.Write(ctx, "kb-1", v1, d); err != nil {
			t.Fatalf("vdl v1 write: %v", err)
		}
	}
	for _, d := range []string{"doc-a", "doc-b"} {
		if err := vdl.Write(ctx, "kb-1", v2, d); err != nil {
			t.Fatalf("vdl v2 write: %v", err)
		}
	}
	for _, d := range []string{"doc-a", "doc-b", "doc-c"} {
		if err := vdl.Write(ctx, "kb-1", v3, d); err != nil {
			t.Fatalf("vdl v3 write: %v", err)
		}
	}

	// SINGLE: remove v2 only. v3 is re-parented onto v1 and survives.
	if _, err := rn.ProposeMarkVersionDeleting(ctx, "kb-1", v2, types.VersionDeleteSingle); err != nil {
		t.Fatalf("mark v2 deleting (SINGLE): %v", err)
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
	if err := coord.Execute(ctx, "kb-1"); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if _, ok := rn.GetVersion(v2); ok {
		t.Error("v2 metadata should be gone after cleanup")
	}
	// v3 still resolves doc-b through v2's retained MVCC entry.
	content, err := ds.ReadAt(ctx, "kb-1", "doc-b", v3)
	if err != nil || string(content) != "b-v2" {
		t.Errorf("doc-b at v3 = (%q, %v), want (\"b-v2\", nil): v2's record must survive while v3 reads it", content, err)
	}
	if content, err := ds.ReadAt(ctx, "kb-1", "doc-a", v3); err != nil || string(content) != "a-v1" {
		t.Errorf("doc-a at v3 = (%q, %v), want (\"a-v1\", nil)", content, err)
	}
}

// TestDeleteVersionCoordinator_KeepsRecordsForForkedDescendant pins the anchor
// choice against a fork. The smallest surviving version above the deleted one
// can be a SIBLING branch rather than a descendant:
//
//	v1 ├─ v2 ├─ v4   (delete v2, keep v3 and v4)
//	   └─ v3
//
// with v2 < v3 < v4. Reads resolve purely by version number (ReadAt returns
// the largest versionID <= the query, regardless of the parent chain), so the
// sibling anchor v3 still sees v2's document and the dependency check keeps
// the record for the descendant v4 as well.
func TestDeleteVersionCoordinator_KeepsRecordsForForkedDescendant(t *testing.T) {
	ctx := context.Background()
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	ds := docstore.NewMockDocStore()
	vdl := versiondoc.NewMockVersionDocList()
	im := newDeleteTestIndexManager()

	if err := rn.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "kb-1", ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"}, Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	create := func(parent int64) int64 {
		t.Helper()
		id, err := rn.ProposeCreateVersion(ctx, "kb-1", parent)
		if err != nil {
			t.Fatalf("create version (parent %d): %v", parent, err)
		}
		if err := w.WriteCommit(ctx, id); err != nil {
			t.Fatalf("commit v%d: %v", id, err)
		}
		if err := rn.ProposeUpdateVersionStatus(ctx, id, types.IndexStatusReady); err != nil {
			t.Fatalf("v%d READY: %v", id, err)
		}
		return id
	}
	v1 := create(0)
	v2 := create(v1)
	v3 := create(v1) // sibling branch, created after v2
	v4 := create(v2) // descendant of v2, created after v3
	if !(v2 < v3 && v3 < v4) {
		t.Fatalf("fixture: want v2 < v3 < v4, got %d < %d < %d", v2, v3, v4)
	}
	if err := rn.ProposeRollback(ctx, "kb-1", v4); err != nil {
		t.Fatalf("rollback to v4: %v", err)
	}

	// doc-a comes from v1; doc-b is introduced by v2 only.
	mustWriteDoc(t, ds, "kb-1", "doc-a", v1, "a-v1")
	mustWriteDoc(t, ds, "kb-1", "doc-b", v2, "b-v2")
	writeDocIDs := func(versionID int64, docIDs ...string) {
		t.Helper()
		for _, d := range docIDs {
			if err := vdl.Write(ctx, "kb-1", versionID, d); err != nil {
				t.Fatalf("vdl v%d write %s: %v", versionID, d, err)
			}
		}
	}
	writeDocIDs(v1, "doc-a")
	writeDocIDs(v2, "doc-a", "doc-b")
	writeDocIDs(v3, "doc-a")          // v3 inherits v1, which has no doc-b
	writeDocIDs(v4, "doc-a", "doc-b") // v4 inherits v2

	// SINGLE removes v2; the sibling v3 and the descendant v4 both survive.
	if _, err := rn.ProposeMarkVersionDeleting(ctx, "kb-1", v2, types.VersionDeleteSingle); err != nil {
		t.Fatalf("mark v2 deleting (SINGLE): %v", err)
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
	if err := coord.Execute(ctx, "kb-1"); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if _, ok := rn.GetVersion(v2); ok {
		t.Error("v2 metadata should be gone after cleanup")
	}
	// v4 must still read doc-b through v2's retained record. Deleting it here
	// would silently serve v4 either nothing or an older value.
	content, err := ds.ReadAt(ctx, "kb-1", "doc-b", v4)
	if err != nil || string(content) != "b-v2" {
		t.Errorf("doc-b at v4 = (%q, %v), want (\"b-v2\", nil): v2's record must survive for its forked descendant", content, err)
	}
	// The sibling anchor reads the same record, which is what makes the
	// global-minimum anchor a sound choice.
	if content, err := ds.ReadAt(ctx, "kb-1", "doc-b", v3); err != nil || string(content) != "b-v2" {
		t.Errorf("doc-b at v3 (sibling) = (%q, %v), want (\"b-v2\", nil)", content, err)
	}
}

// TestDeleteVersionCoordinator_DeletesVersionBloomFiles verifies the cleanup
// reclaims the deleted versions' on-disk document bloom filters while the
// surviving version's filter stays. Before this step existed, one file per
// deleted version leaked until the whole knowledge base was dropped.
func TestDeleteVersionCoordinator_DeletesVersionBloomFiles(t *testing.T) {
	ctx := context.Background()
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	ds := docstore.NewMockDocStore()
	vdl := versiondoc.NewMockVersionDocList()
	im := newDeleteTestIndexManager()

	const kbID = "kb-1"
	if err := rn.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: kbID, Name: kbID, ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"}, Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	create := func(parent int64) int64 {
		t.Helper()
		id, err := rn.ProposeCreateVersion(ctx, kbID, parent)
		if err != nil {
			t.Fatalf("create version (parent %d): %v", parent, err)
		}
		if err := w.WriteCommit(ctx, id); err != nil {
			t.Fatalf("commit v%d: %v", id, err)
		}
		if err := rn.ProposeUpdateVersionStatus(ctx, id, types.IndexStatusReady); err != nil {
			t.Fatalf("v%d READY: %v", id, err)
		}
		return id
	}
	v1 := create(0)
	v2 := create(v1)
	v3 := create(v2)

	dir := t.TempDir()
	vBloom := bloom.NewVersionBloomStore(dir, 1000, 0.01, vdl)
	bloomFile := func(versionID int64) string {
		return filepath.Join(dir, "bloom-version", kbID, fmt.Sprintf("%d.bloom", versionID))
	}
	for _, vid := range []int64{v1, v2, v3} {
		if _, err := vBloom.BuildAndPersist(kbID, vid, []string{"doc-a"}); err != nil {
			t.Fatalf("BuildAndPersist v%d: %v", vid, err)
		}
		if _, err := os.Stat(bloomFile(vid)); err != nil {
			t.Fatalf("v%d bloom file should exist before cleanup: %v", vid, err)
		}
	}

	// SUBTREE removes v2 and v3; v1 survives.
	if _, err := rn.ProposeMarkVersionDeleting(ctx, kbID, v2, types.VersionDeleteSubtree); err != nil {
		t.Fatalf("mark v2 deleting (SUBTREE): %v", err)
	}
	coord := NewDeleteVersionCoordinatorImpl(DeleteVersionCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		VersionDocList:      vdl,
		VersionBloom:        vBloom,
	})
	if err := coord.Execute(ctx, kbID); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	for _, vid := range []int64{v2, v3} {
		if _, err := os.Stat(bloomFile(vid)); !os.IsNotExist(err) {
			t.Errorf("v%d bloom file should be gone after cleanup, stat err = %v", vid, err)
		}
	}
	if _, err := os.Stat(bloomFile(v1)); err != nil {
		t.Errorf("v1 bloom file should survive cleanup: %v", err)
	}
}
