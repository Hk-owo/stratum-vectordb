package raft

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// newTestRaftNodeImpl constructs a single-node RaftNodeImpl wired to a
// fresh in-memory WAL and a temp-dir-backed kvraft instance, already
// running and elected leader (single-node clusters always win their own
// election — see internal/kvraft's single-node fixes).
func newTestRaftNodeImpl(t *testing.T, opts ...RaftOption) (*RaftNodeImpl, *wal.MockWAL) {
	t.Helper()
	w := wal.NewMockWAL()
	impl, err := NewRaftNodeImpl(Config{
		NodeID:             1,
		DataDir:            t.TempDir(),
		RaftAddr:           freeLoopbackAddrForTest(t),
		WAL:                w,
		ElectionTimeoutMin: 30 * time.Millisecond,
		ElectionTimeoutMax: 60 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
	}, opts...)
	if err != nil {
		t.Fatalf("NewRaftNodeImpl: %v", err)
	}
	t.Cleanup(impl.Stop)

	if !waitForCond(t, 2*time.Second, impl.raft.IsLeader) {
		t.Fatalf("single-node RaftNodeImpl never became leader")
	}
	return impl, w
}

// proposeCtx is the context these tests propose with: a DEADLINE rather than
// context.Background(), so a proposal whose result goes missing fails with the
// awaiting test named instead of hanging the package.
//
// That is not hypothetical. The suite used Background() everywhere, and a dropped
// apply result — see rememberSettledLocked: Propose returns an index before its
// caller can register a waiter for it — blocked one test forever. CI reported
// `FAIL stratum/internal/raft 180.007s`, the package's own timeout, with every
// other test's outcome lost in the dump. The product-side fix removes that window;
// this keeps any future one diagnosable as a failure rather than a hang.
func proposeCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func freeLoopbackAddrForTest(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func waitForCond(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestRaftNodeImpl_CreateKBAndGetKB(t *testing.T) {
	ctx := proposeCtx(t)
	impl, _ := newTestRaftNodeImpl(t)

	kb := types.KnowledgeBaseMeta{KBID: "kb1", Name: "test-kb"}
	if err := impl.ProposeCreateKB(ctx, kb); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}

	got, err := impl.GetKB(ctx, "kb1")
	if err != nil {
		t.Fatalf("GetKB: %v", err)
	}
	if got.KBID != "kb1" || got.Name != "test-kb" {
		t.Fatalf("GetKB = %+v, want KBID=kb1 Name=test-kb", got)
	}
	if got.Status != types.KBStatusActive {
		t.Fatalf("GetKB.Status = %v, want Active", got.Status)
	}
}

func TestRaftNodeImpl_GetKB_NotFound(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	_, err := impl.GetKB(context.Background(), "missing")
	if !errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Fatalf("err = %v, want ErrKnowledgeBaseNotFound", err)
	}
}

func TestRaftNodeImpl_VersionIDMonotonic(t *testing.T) {
	ctx := proposeCtx(t)
	impl, _ := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")

	v1, err := impl.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion #1: %v", err)
	}
	mustUpdateStatusImpl(t, impl, v1, types.IndexStatusReady)

	v2, err := impl.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion #2: %v", err)
	}
	if v2 <= v1 {
		t.Fatalf("version IDs not monotonic: v1=%d v2=%d", v1, v2)
	}
}

func TestRaftNodeImpl_ParentMustBeSameKB(t *testing.T) {
	ctx := proposeCtx(t)
	impl, _ := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")
	mustCreateKBImpl(t, impl, "kb2")

	v1, err := impl.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	mustUpdateStatusImpl(t, impl, v1, types.IndexStatusReady)

	_, err = impl.ProposeCreateVersion(ctx, "kb2", v1)
	if !errors.Is(err, stratumerrors.ErrInvalidParentVersion) {
		t.Fatalf("err = %v, want ErrInvalidParentVersion (cross-KB parent)", err)
	}
}

func TestRaftNodeImpl_ParentMustNotBePending(t *testing.T) {
	ctx := proposeCtx(t)
	impl, _ := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")

	v1, err := impl.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	// v1 stays PENDING.

	_, err = impl.ProposeCreateVersion(ctx, "kb1", v1)
	if !errors.Is(err, stratumerrors.ErrParentNotDurable) {
		t.Fatalf("err = %v, want ErrParentNotDurable (a PENDING parent is a \"not yet\", not an invalid request)", err)
	}
}

// The version chain is strictly linear: a parent may have at most one child,
// because docstore.ReadAt resolves a document by numeric version order, which
// is only equivalent to the ancestor chain when there are no forks.
func TestRaftNodeImpl_ForkRejected(t *testing.T) {
	ctx := proposeCtx(t)
	impl, _ := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")

	v1, err := impl.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	mustUpdateStatusImpl(t, impl, v1, types.IndexStatusReady)

	v2, err := impl.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion (first child): %v", err)
	}
	mustUpdateStatusImpl(t, impl, v2, types.IndexStatusReady)

	if _, err := impl.ProposeCreateVersion(ctx, "kb1", v1); !errors.Is(err, stratumerrors.ErrInvalidParentVersion) {
		t.Fatalf("second child of v%d = %v, want ErrInvalidParentVersion", v1, err)
	}

	// The chain may continue from the only child.
	if _, err := impl.ProposeCreateVersion(ctx, "kb1", v2); err != nil {
		t.Fatalf("chained child of v%d failed: %v", v2, err)
	}

	versions, err := impl.ListVersions(ctx, "kb1")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	parentCount := 0
	for _, v := range versions {
		if v.ParentVersionID == v1 {
			parentCount++
		}
	}
	if parentCount != 1 {
		t.Fatalf("expected 1 child of v1, found %d", parentCount)
	}
}

func TestRaftNodeImpl_ProposeRemoveKBMeta_Idempotent(t *testing.T) {
	ctx := proposeCtx(t)
	impl, _ := newTestRaftNodeImpl(t)

	if err := impl.ProposeRemoveKBMeta(ctx, "ghost"); err != nil {
		t.Fatalf("ProposeRemoveKBMeta on nonexistent KB = %v, want nil (idempotent)", err)
	}

	mustCreateKBImpl(t, impl, "kb1")
	if err := impl.ProposeRemoveKBMeta(ctx, "kb1"); err != nil {
		t.Fatalf("ProposeRemoveKBMeta #1: %v", err)
	}
	if err := impl.ProposeRemoveKBMeta(ctx, "kb1"); err != nil {
		t.Fatalf("ProposeRemoveKBMeta #2 (already removed) = %v, want nil (idempotent)", err)
	}
}

func TestRaftNodeImpl_ProposeCreateVersion_WritesWALBeforeReturning(t *testing.T) {
	ctx := proposeCtx(t)
	impl, w := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")

	versionID, err := impl.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}

	pending := w.PendingVersionIDs()
	found := false
	for _, id := range pending {
		if id == versionID {
			found = true
		}
	}
	if !found {
		t.Fatalf("WAL has no VERSION_ID record for newly created version %d; PendingVersionIDs = %v", versionID, pending)
	}
}

func TestRaftNodeImpl_Rollback(t *testing.T) {
	ctx := proposeCtx(t)
	impl, _ := newTestRaftNodeImpl(t)
	mustCreateKBImpl(t, impl, "kb1")

	v1, _ := impl.ProposeCreateVersion(ctx, "kb1", 0)
	mustUpdateStatusImpl(t, impl, v1, types.IndexStatusReady)
	v2, _ := impl.ProposeCreateVersion(ctx, "kb1", v1)
	mustUpdateStatusImpl(t, impl, v2, types.IndexStatusReady)

	if err := impl.ProposeRollback(ctx, "kb1", v1); err != nil {
		t.Fatalf("ProposeRollback: %v", err)
	}
	kb, err := impl.GetKB(ctx, "kb1")
	if err != nil {
		t.Fatalf("GetKB: %v", err)
	}
	if kb.ActiveVersionID != v1 {
		t.Fatalf("ActiveVersionID = %d, want %d", kb.ActiveVersionID, v1)
	}
}

func TestRaftNodeImpl_GetClusterStatus(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	status, err := impl.GetClusterStatus(context.Background())
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if !status.HasLeader || status.MemberCount < 1 {
		t.Fatalf("GetClusterStatus = %+v, want a healthy single-node status", status)
	}
}

// TestRaftNodeImpl_SingleNodeSurvivesRestart covers the Phase 3 test node
// "单节点 leader 切换后元数据不丢失": a single-node deployment's metadata
// (knowledge bases and versions) must survive the node going away and a
// new incarnation taking over leadership (simulated here as a full
// process restart against the same data directory).
func TestRaftNodeImpl_SingleNodeSurvivesRestart(t *testing.T) {
	ctx := proposeCtx(t)
	dataDir := t.TempDir()
	w := wal.NewMockWAL()

	impl1, err := NewRaftNodeImpl(Config{
		NodeID:             1,
		DataDir:            dataDir,
		RaftAddr:           freeLoopbackAddrForTest(t),
		WAL:                w,
		ElectionTimeoutMin: 30 * time.Millisecond,
		ElectionTimeoutMax: 60 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRaftNodeImpl: %v", err)
	}
	if !waitForCond(t, 2*time.Second, impl1.raft.IsLeader) {
		t.Fatalf("never became leader")
	}

	mustCreateKBImpl(t, impl1, "kb1")
	v1, err := impl1.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	mustUpdateStatusImpl(t, impl1, v1, types.IndexStatusReady)

	impl1.Stop()

	// New incarnation, same data directory and WAL: this simulates the
	// "old leader process dies, a new one takes over" scenario for a
	// single-node deployment.
	impl2, err := NewRaftNodeImpl(Config{
		NodeID:             1,
		DataDir:            dataDir,
		RaftAddr:           freeLoopbackAddrForTest(t),
		WAL:                w,
		ElectionTimeoutMin: 30 * time.Millisecond,
		ElectionTimeoutMax: 60 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRaftNodeImpl (restart): %v", err)
	}
	t.Cleanup(impl2.Stop)
	if !waitForCond(t, 2*time.Second, impl2.raft.IsLeader) {
		t.Fatalf("new incarnation never became leader")
	}

	// Becoming leader triggers an asynchronous no-op proposal (see
	// internal/kvraft's SendVoteRequest doc comment) whose commit is what
	// actually makes pre-restart entries visible; this can lag slightly
	// behind IsLeader() becoming true, so poll rather than checking once.
	var kb types.KnowledgeBaseMeta
	if !waitForCond(t, 2*time.Second, func() bool {
		var err error
		kb, err = impl2.GetKB(ctx, "kb1")
		return err == nil
	}) {
		t.Fatalf("GetKB after restart never succeeded (KB metadata lost across restart)")
	}
	if kb.KBID != "kb1" {
		t.Fatalf("KB metadata lost across restart: %+v", kb)
	}

	versions, err := impl2.ListVersions(ctx, "kb1")
	if err != nil {
		t.Fatalf("ListVersions after restart: %v", err)
	}
	found := false
	for _, v := range versions {
		if v.VersionID == v1 && v.IndexStatus == types.IndexStatusReady {
			found = true
		}
	}
	if !found {
		t.Fatalf("version %d lost or status not preserved across restart: %+v", v1, versions)
	}

	// The version counter itself must also have survived (not reset to
	// 1), or a post-restart CreateVersion could collide with a
	// pre-restart version ID.
	v2, err := impl2.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion after restart: %v", err)
	}
	if v2 <= v1 {
		t.Fatalf("post-restart version ID %d is not greater than pre-restart version ID %d (counter did not survive restart)", v2, v1)
	}
}

func mustCreateKBImpl(t *testing.T, impl *RaftNodeImpl, kbID string) {
	t.Helper()
	if err := impl.ProposeCreateKB(proposeCtx(t), types.KnowledgeBaseMeta{KBID: kbID}); err != nil {
		t.Fatalf("ProposeCreateKB(%s): %v", kbID, err)
	}
}

func mustUpdateStatusImpl(t *testing.T, impl *RaftNodeImpl, versionID int64, status types.IndexStatus) {
	t.Helper()
	// A version only reaches INDEX_READY after its data has landed, so a test that
	// uses READY to mean "this version is complete" must mark the data side too:
	// the parent gate reads the DATA side now (stateMachine.applyCreateVersion).
	// Doing it here keeps every fixture as truthful as production.
	if status == types.IndexStatusReady {
		if err := impl.ProposeMarkVersionDataDurable(proposeCtx(t), versionID); err != nil {
			t.Fatalf("ProposeMarkVersionDataDurable(%d): %v", versionID, err)
		}
	}
	if err := impl.ProposeUpdateVersionStatus(proposeCtx(t), versionID, status, 0); err != nil {
		t.Fatalf("ProposeUpdateVersionStatus(%d, %v): %v", versionID, status, err)
	}
}

func TestRaftNodeImpl_StorageAndVersionHelpers(t *testing.T) {
	ctx := proposeCtx(t)
	impl, _ := newTestRaftNodeImpl(t)

	// StorageDirectoryRevision, ProposeAssignKBPlacement, migration stage/install/serve/activate,
	// cleanup ack, node manage/drain, and service block/resume.
	if err := impl.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{KBID: "kb-init"}); err != nil {
		t.Fatal(err)
	}
	if err := impl.ProposeRegisterStorageNodes(ctx, 0, directoryTestNodes); err != nil {
		t.Fatal(err)
	}
	rev0 := impl.StorageDirectoryRevision()
	if rev0 == 0 {
		t.Fatal("expected non-zero StorageDirectoryRevision after node registration")
	}

	if err := impl.ProposeAssignKBPlacement(ctx, storage.Placement{
		KBID:             "kb-init",
		Epoch:            1,
		ReplicaIDs:       []int64{1, 2, 3},
		RequiredReplicas: 3,
		OperationID:      "init-place",
	}); err != nil {
		t.Fatalf("ProposeAssignKBPlacement: %v", err)
	}
	if err := impl.ProposeCreateKBWithPlacement(ctx, types.KnowledgeBaseMeta{KBID: "kb-mig"}, []int64{1, 2}); err != nil {
		t.Fatal(err)
	}

	op := "mig-op-1"
	if err := impl.ProposeBeginKBMigration(ctx, storage.MigrationRequest{
		OperationID:   op,
		KBID:          "kb-mig",
		ExpectedEpoch: 1,
		FromID:        2,
		ToID:          3,
	}); err != nil {
		t.Fatalf("ProposeBeginKBMigration: %v", err)
	}
	if err := impl.ProposeFenceKBMigration(ctx, op); err != nil {
		t.Fatal(err)
	}
	for _, nid := range []int64{1, 2, 3} {
		if err := impl.ProposeAckKBMigrationFence(ctx, op, 1, nid); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := storage.BuildMigrationManifest("kb-mig", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	stageReceipt := storage.MigrationStageReceipt{
		SourceID:     1,
		TargetID:     3,
		ManifestHash: manifest.Hash(),
		PayloadHash:  types.EmptyDocIDSetHash,
		Bytes:        64,
	}
	if err := impl.ProposeRecordKBMigrationStage(ctx, op, stageReceipt); err != nil {
		t.Fatalf("ProposeRecordKBMigrationStage: %v", err)
	}
	installReceipt := storage.MigrationInstallReceipt{
		SourceID:     1,
		TargetID:     3,
		ManifestHash: manifest.Hash(),
		StageHash:    stageReceipt.PayloadHash,
	}
	if err := impl.ProposeRecordKBMigrationInstall(ctx, op, installReceipt); err != nil {
		t.Fatalf("ProposeRecordKBMigrationInstall: %v", err)
	}
	serveReceipt := storage.MigrationServeReceipt{
		TargetID:     3,
		ManifestHash: manifest.Hash(),
		IndexReady:   true,
	}
	if err := impl.ProposeRecordKBMigrationServe(ctx, op, serveReceipt); err != nil {
		t.Fatalf("ProposeRecordKBMigrationServe: %v", err)
	}
	if err := impl.ProposeActivateKBMigration(ctx, op, storage.MigrationActivation{
		Epoch:   1,
		Install: installReceipt,
		Serve:   serveReceipt,
	}); err != nil {
		t.Fatalf("ProposeActivateKBMigration: %v", err)
	}
	dirAfterAct, err := impl.StorageDirectory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for cid, c := range dirAfterAct.Cleanups {
		if c.KBID == "kb-mig" {
			if err := impl.ProposeAckStorageCleanup(ctx, cid); err != nil {
				t.Fatalf("ProposeAckStorageCleanup(%s): %v", cid, err)
			}
		}
	}

	rev := impl.StorageDirectoryRevision()
	if err := impl.ProposeRegisterStorageNodes(ctx, rev, append(append([]storage.Node(nil), directoryTestNodes...), storage.Node{ID: 4, Addr: "s4"})); err != nil {
		t.Fatal(err)
	}
	rev = impl.StorageDirectoryRevision()
	if err := impl.ProposeBlockStorageService(ctx, storage.ServiceRequest{NodeID: 3, ExpectedRevision: rev, OperationID: "svc-op"}); err != nil {
		t.Fatalf("ProposeBlockStorageService: %v", err)
	}
	dir, err := impl.StorageDirectory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := impl.ProposeResumeStorageService(ctx, storage.ServiceRequest{
		NodeID:           3,
		ExpectedRevision: dir.Revision,
		OperationID:      "svc-op",
		BlockRevision:    dir.ServiceBlocks[3].Revision,
	}); err != nil {
		t.Fatalf("ProposeResumeStorageService: %v", err)
	}
	rev = impl.StorageDirectoryRevision()
	if err := impl.ProposeManageStorageNode(ctx, storage.NodeChange{NodeID: 2, ExpectedRevision: rev, Action: "drain"}); err != nil {
		t.Fatalf("ProposeManageStorageNode: %v", err)
	}
	rev = impl.StorageDirectoryRevision()
	if err := impl.ProposeDrainStorageNode(ctx, storage.DrainRequest{OperationID: "drain-op", NodeID: 2, ExpectedRevision: rev, TargetIDs: []int64{4}}); err != nil {
		t.Fatalf("ProposeDrainStorageNode: %v", err)
	}

	// ChainGeneration, VersionLiveness, ProposeRetryVersion, DeletingKBIDs.
	term, seq0 := impl.ChainGeneration()
	if term == 0 {
		t.Fatal("expected non-zero term from ChainGeneration")
	}
	v1, err := impl.ProposeCreateVersion(ctx, "kb-mig", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := impl.ProposeMarkVersionFailedPermanent(ctx, "kb-mig", v1, types.FailureSideIndex, "boom", 1); err != nil {
		t.Fatal(err)
	}
	if err := impl.ProposeRetryVersion(ctx, "kb-mig", v1, types.FailureSideIndex); err != nil {
		t.Fatalf("ProposeRetryVersion: %v", err)
	}
	_, seq1 := impl.ChainGeneration()
	if seq1 <= seq0 {
		t.Fatalf("expected ChainGeneration seq to advance (%d -> %d)", seq0, seq1)
	}
	from := int64(0)
	to := v1
	alive, alloc, err := impl.VersionLiveness(ctx, "kb-mig", &from, &to)
	if err != nil || len(alive) != 1 || alive[0] != v1 || alloc < v1 {
		t.Fatalf("VersionLiveness = (%v, %d, %v)", alive, alloc, err)
	}
	if _, _, err := impl.VersionLiveness(ctx, "missing", nil, nil); !errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Fatalf("VersionLiveness missing err = %v", err)
	}
	if err := impl.ProposeMarkKBDeleting(ctx, "kb-mig"); err != nil {
		t.Fatal(err)
	}
	delKBs, err := impl.DeletingKBIDs(ctx)
	if err != nil || len(delKBs) != 1 || delKBs[0] != "kb-mig" {
		t.Fatalf("DeletingKBIDs = (%v, %v)", delKBs, err)
	}
}
