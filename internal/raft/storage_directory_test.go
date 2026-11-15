package raft

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wal"
)

var directoryTestNodes = []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}}

func TestStorageDirectoryApplySnapshotAndDeletionFence(t *testing.T) {
	sm, w := newTestSM(t)
	apply := func(c command) error { return sm.apply(context.Background(), c, w, zap.NewNop()).Err }
	if err := apply(registerStorageNodesCommand(0, directoryTestNodes)); err != nil {
		t.Fatal(err)
	}
	if err := apply(newCreateKBCommand(testKB("old"))); err != nil {
		t.Fatal(err)
	}
	snap, err := (&RaftNodeImpl{sm: sm}).GetKBPlacement(context.Background(), "old")
	if err != nil || snap.Placement.RequiredReplicas != 3 {
		t.Fatalf("atomic create and placement: %+v %v", snap, err)
	}
	copy := sm.deepCopy()
	copy.Directory.Placements["old"].ReplicaIDs[0] = 99
	if sm.directory.Placements["old"].ReplicaIDs[0] != 1 {
		t.Fatal("async snapshot aliases live placement")
	}
	if err := apply(newMarkKBDeletingCommand("old")); err != nil {
		t.Fatal(err)
	}
	if err := apply(newRemoveKBMetaCommand("old")); err != nil {
		t.Fatal(err)
	}
	data, err := sm.serialize()
	if err != nil {
		t.Fatal(err)
	}
	restored := newStateMachine()
	if err := restored.restore(data); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sm.directory, restored.directory) {
		t.Fatal("directory or tombstone lost in snapshot")
	}
	if err := restored.apply(context.Background(), newCreateKBCommand(testKB("old")), w, zap.NewNop()).Err; !errors.Is(err, serrors.ErrKnowledgeBaseDeleted) {
		t.Fatal("late creation resurrected identity:", err)
	}
	p := sm.directory.InitialPlacement("old", "late")
	if err := restored.apply(context.Background(), assignKBPlacementCommand(p), w, zap.NewNop()).Err; !errors.Is(err, serrors.ErrKnowledgeBaseDeleted) {
		t.Fatal("late assignment resurrected identity:", err)
	}
	if err := restored.apply(context.Background(), newCreateKBCommand(testKB("new")), w, zap.NewNop()).Err; err != nil {
		t.Fatal(err)
	}
}

func TestStorageDirectoryOldSnapshotAndInvalidCommands(t *testing.T) {
	// Gob snapshots predating P1 have no Directory field.
	data, err := encodeSnapshot(snapshotState{KBs: map[string]types.KnowledgeBaseMeta{"legacy": testKB("legacy")}})
	if err != nil {
		t.Fatal(err)
	}
	sm := newStateMachine()
	if err := sm.restore(data); err != nil {
		t.Fatal(err)
	}
	impl := &RaftNodeImpl{sm: sm}
	if _, err := impl.GetKBPlacement(context.Background(), "legacy"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Fatal("known KB without placement:", err)
	}
	if _, err := impl.GetKBPlacement(context.Background(), "missing"); !errors.Is(err, serrors.ErrKnowledgeBaseNotFound) {
		t.Fatal("missing KB:", err)
	}
	for _, c := range []command{{Type: cmdCreateKB}, {Type: cmdAssignKBPlacement}, {Type: cmdRegisterStorageNodes}} {
		if err := sm.apply(context.Background(), c, nil, zap.NewNop()).Err; !errors.Is(err, serrors.ErrInvalidArgument) {
			t.Fatalf("%s: %v", c.Type, err)
		}
	}
	if err := sm.apply(context.Background(), registerStorageNodesCommand(0, directoryTestNodes), nil, zap.NewNop()).Err; err != nil {
		t.Fatal(err)
	}
	p := sm.directory.InitialPlacement("legacy", "op")
	if err := sm.apply(context.Background(), assignKBPlacementCommand(p), nil, zap.NewNop()).Err; err != nil {
		t.Fatal(err)
	}
}

func TestStorageBootstrapLegacyAndNewKnowledgeBases(t *testing.T) {
	r := NewMockRaftNode(wal.NewMockWAL())
	ctx := context.Background()
	if err := r.ProposeCreateKB(ctx, testKB("legacy")); err != nil {
		t.Fatal(err)
	}
	if err := storage.Bootstrap(ctx, r, r, directoryTestNodes); err != nil {
		t.Fatal(err)
	}
	before, _ := r.StorageDirectory(ctx)
	if err := storage.Bootstrap(ctx, r, r, directoryTestNodes); err != nil {
		t.Fatal("retry:", err)
	}
	after, _ := r.StorageDirectory(ctx)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("bootstrap retry changed the directory")
	}
	if err := r.ProposeCreateKB(ctx, testKB("new")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"legacy", "new"} {
		s, err := r.GetKBPlacement(ctx, id)
		if err != nil || s.Placement.RequiredReplicas != 3 {
			t.Fatalf("%s: %+v %v", id, s, err)
		}
	}
	if err := storage.Bootstrap(ctx, r, r, directoryTestNodes[:2]); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Fatal("configuration change silently shrank placement:", err)
	}
}

type bootstrapLostAck struct {
	*MockRaftNode
	loseNext bool
}

func (m *bootstrapLostAck) ProposeAssignKBPlacement(ctx context.Context, p storage.Placement) error {
	if err := m.MockRaftNode.ProposeAssignKBPlacement(ctx, p); err != nil {
		return err
	}
	if m.loseNext {
		m.loseNext = false
		return context.DeadlineExceeded
	}
	return nil
}

type bootstrapInventoryFunc func(context.Context) ([]types.KnowledgeBaseMeta, error)

func (f bootstrapInventoryFunc) ListKnowledgeBases(ctx context.Context) ([]types.KnowledgeBaseMeta, error) {
	return f(ctx)
}

func TestStorageBootstrapRecoversLostAckAndConcurrentCreation(t *testing.T) {
	ctx := context.Background()
	r := NewMockRaftNode(wal.NewMockWAL())
	for _, id := range []string{"a", "b"} {
		if err := r.ProposeCreateKB(ctx, testKB(id)); err != nil {
			t.Fatal(err)
		}
	}
	m := &bootstrapLostAck{MockRaftNode: r, loseNext: true}
	if err := storage.Bootstrap(ctx, m, r, directoryTestNodes); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expected lost assignment ACK:", err)
	}
	if err := storage.Bootstrap(ctx, m, r, directoryTestNodes); err != nil {
		t.Fatal("restart/re-election retry:", err)
	}
	// Create after Bootstrap has read its directory, before its KB scan. The
	// atomic CreateKB assignment wins and the stale scan must not overwrite it.
	inv := bootstrapInventoryFunc(func(ctx context.Context) ([]types.KnowledgeBaseMeta, error) {
		if err := r.ProposeCreateKB(ctx, testKB("racing")); err != nil {
			return nil, err
		}
		return r.ListKnowledgeBases(ctx)
	})
	if err := storage.Bootstrap(ctx, r, inv, directoryTestNodes); err != nil {
		t.Fatal("concurrent creation:", err)
	}
	snap, err := r.GetKBPlacement(ctx, "racing")
	if err != nil || snap.Placement.OperationID != "create/racing" {
		t.Fatalf("concurrent assignment overwritten: %+v %v", snap, err)
	}
	for _, id := range []string{"a", "b"} {
		if _, err := r.GetKBPlacement(ctx, id); err != nil {
			t.Fatal("legacy registration incomplete:", err)
		}
	}
}

func TestStorageDirectoryRemoteProposalsReadsAndErrors(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := proposeCtx(t)
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterInternalServiceServer(srv, NewInternalServiceServer(impl))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	t.Cleanup(func() { _ = lis.Close() })
	remote := &RemoteRaftNode{ControlAddrs: map[int64]string{1: "control"}, Dial: func(context.Context, string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///control", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	}}
	t.Cleanup(func() {
		remote.connMu.Lock()
		defer remote.connMu.Unlock()
		for _, c := range remote.conns {
			_ = c.Close()
		}
	})
	if err := remote.ProposeCreateKB(ctx, testKB("legacy")); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.GetKBPlacement(ctx, "legacy"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Fatal("unknown sentinel lost:", err)
	}
	if err := remote.ProposeRegisterStorageNodes(ctx, 0, directoryTestNodes); err != nil {
		t.Fatal(err)
	}
	if err := remote.ProposeCreateKBWithPlacement(ctx, testKB("selected"), []int64{3, 1}); err != nil {
		t.Fatal(err)
	}
	selected, err := remote.GetKBPlacement(ctx, "selected")
	if err != nil || !reflect.DeepEqual(selected.Placement.ReplicaIDs, []int64{1, 3}) {
		t.Fatalf("explicit creation: %+v %v", selected, err)
	}
	migrationReq := storage.MigrationRequest{OperationID: "remote-move", KBID: "selected", ExpectedEpoch: 1, FromID: 3, ToID: 2}
	if err := remote.ProposeBeginKBMigration(ctx, migrationReq); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.ProposeCreateVersion(ctx, "selected", 0); !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal("migration admission error lost across RPC:", err)
	}
	if err := remote.ProposeFenceKBMigration(ctx, migrationReq.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := remote.ProposeAckKBMigrationFence(ctx, migrationReq.OperationID, 1, 1); err != nil {
		t.Fatal(err)
	}
	selected, err = remote.GetKBPlacement(ctx, "selected")
	if err != nil || selected.Migration == nil || selected.Migration.Request != migrationReq || !reflect.DeepEqual(selected.Migration.FencedIDs, []int64{1}) {
		t.Fatalf("targeted directory RPC lost migration: %+v %v", selected, err)
	}
	jobs, err := remote.ListKBMigrations(ctx)
	if err != nil || len(jobs) != 1 || jobs[0].Phase != storage.MigrationFencing {
		t.Fatal("full directory RPC lost task:", jobs, err)
	}
	if err := remote.ProposeCancelKBMigration(ctx, migrationReq.OperationID); err != nil {
		t.Fatal(err)
	}
	selected, err = remote.GetKBPlacement(ctx, "selected")
	if err != nil || selected.Migration != nil || selected.Placement.Epoch != 2 {
		t.Fatal("cancelled task leaked into operation snapshot:", selected, err)
	}
	if err := remote.ProposeCreateKBWithPlacement(ctx, testKB("invalid"), []int64{9}); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Fatal("invalid node was accepted:", err)
	}
	if _, err := impl.GetKB(ctx, "invalid"); !errors.Is(err, serrors.ErrKnowledgeBaseNotFound) {
		t.Fatal("rejected placement left business metadata behind:", err)
	}
	p := storage.Placement{KBID: "legacy", Epoch: 1, ReplicaIDs: []int64{3, 1, 2}, RequiredReplicas: 3, OperationID: "explicit"}
	if err := remote.ProposeAssignKBPlacement(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := remote.ProposeAssignKBPlacement(ctx, p); err != nil {
		t.Fatal("duplicate operation:", err)
	}
	snap, err := remote.GetKBPlacement(ctx, "legacy")
	if err != nil || !reflect.DeepEqual(snap.Placement.ReplicaIDs, []int64{1, 2, 3}) || snap.Placement.Quorum() != 2 {
		t.Fatalf("%+v %v", snap, err)
	}
	p.KBID, p.ReplicaIDs, p.RequiredReplicas = "partial", []int64{1}, 1
	if err := remote.ProposeCreateKB(ctx, testKB("partial")); err != nil {
		t.Fatal(err)
	}
	if err := remote.ProposeAssignKBPlacement(ctx, p); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Fatal("unsafe sentinel lost:", err)
	}
	if err := remote.ProposeMarkKBDeleting(ctx, "legacy"); err != nil {
		t.Fatal(err)
	}
	if err := remote.ProposeRemoveKBMeta(ctx, "legacy"); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.GetKBPlacement(ctx, "legacy"); !errors.Is(err, serrors.ErrKnowledgeBaseDeleted) {
		t.Fatal("deleted sentinel lost:", err)
	}
	if _, err := remote.GetKBPlacement(ctx, "absent"); !errors.Is(err, serrors.ErrKnowledgeBaseNotFound) {
		t.Fatal("missing sentinel lost:", err)
	}
	d, err := remote.StorageDirectory(ctx)
	if err != nil || len(d.Tombstones["legacy"].CleanupIDs) != 3 {
		t.Fatalf("remote tombstone: %+v %v", d, err)
	}
	for id, c := range d.Cleanups {
		if c.KBID == "selected" {
			if err := remote.ProposeAckStorageCleanup(ctx, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	stageReq := storage.MigrationRequest{OperationID: "remote-stage", KBID: "selected", ExpectedEpoch: 2, FromID: 3, ToID: 2}
	if err := remote.ProposeBeginKBMigration(ctx, stageReq); err != nil {
		t.Fatal(err)
	}
	if err := remote.ProposeFenceKBMigration(ctx, stageReq.OperationID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2, 3} {
		if err := remote.ProposeAckKBMigrationFence(ctx, stageReq.OperationID, 2, id); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := storage.BuildMigrationManifest("selected", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt := storage.MigrationStageReceipt{SourceID: 1, TargetID: 2, ManifestHash: manifest.Hash(), PayloadHash: types.EmptyDocIDSetHash, Bytes: 128}
	if err := remote.ProposeRecordKBMigrationStage(ctx, stageReq.OperationID, receipt); err != nil {
		t.Fatal(err)
	}
	staged, err := remote.GetKBPlacement(ctx, "selected")
	if err != nil || staged.Migration == nil || staged.Migration.Stage == nil || *staged.Migration.Stage != receipt || staged.Migration.Phase != storage.MigrationStaged || staged.Placement.Contains(2) {
		t.Fatal("remote stage proof missing or activated", staged, err)
	}
	view, err := remote.GetKBMigrationTransfer(ctx, stageReq.OperationID)
	if err != nil || view.Manifest.Hash() != receipt.ManifestHash || view.Directory.Migrations[stageReq.OperationID].Stage == nil || *view.Directory.Migrations[stageReq.OperationID].Stage != receipt {
		t.Fatal("quorum migration view lost metadata", view, err)
	}
	delete(view.Directory.Placements, "selected")
	again, err := impl.GetKBMigrationTransfer(ctx, stageReq.OperationID)
	if err != nil || again.Directory.Placements["selected"].Epoch != 2 {
		t.Fatal("read view aliases authoritative state", again, err)
	}

	install := storage.MigrationInstallReceipt{SourceID: 1, TargetID: 2, ManifestHash: manifest.Hash(), StageHash: receipt.PayloadHash}
	for _, v := range manifest.Versions {
		if v.DataStatus == types.DataStatusDurable {
			install.Versions++
		}
	}
	if err := remote.ProposeRecordKBMigrationInstall(ctx, stageReq.OperationID, install); err != nil {
		t.Fatal(err)
	}
	serve := storage.MigrationServeReceipt{TargetID: 2, ManifestHash: manifest.Hash(), ActiveVersion: manifest.ActiveVersion, CursorVersion: manifest.ActiveVersion, BloomVersions: install.Versions, IndexReady: true}
	if err := remote.ProposeRecordKBMigrationServe(ctx, stageReq.OperationID, serve); err != nil {
		t.Fatal(err)
	}
	if err := remote.ProposeActivateKBMigration(ctx, stageReq.OperationID, storage.MigrationActivation{Epoch: 2, Install: install, Serve: serve}); err != nil {
		t.Fatal(err)
	}
	maintenance, err := remote.GetStorageMaintenance(ctx)
	if err != nil || maintenance.Placements["selected"].Epoch != 3 || !maintenance.Placements["selected"].Contains(2) || len(maintenance.Cleanups) == 0 {
		t.Fatal("maintenance RPC lost cutover/cleanup", maintenance, err)
	}
	data, err := impl.sm.serialize()
	if err != nil {
		t.Fatal(err)
	}
	restored := newStateMachine()
	if err := restored.restore(data); err != nil {
		t.Fatal(err)
	}
	if restored.directory.Migrations[stageReq.OperationID].Phase != storage.MigrationActivated || !reflect.DeepEqual(restored.directory.Cleanups, maintenance.Cleanups) {
		t.Fatal("snapshot lost activation/cleanup")
	}

}

func TestStorageDirectorySurvivesSnapshotRestart(t *testing.T) {
	ctx := proposeCtx(t)
	dir, addr := t.TempDir(), freeLoopbackAddrForTest(t)
	start := func() *RaftNodeImpl {
		r, err := NewRaftNodeImpl(Config{NodeID: 1, DataDir: dir, RaftAddr: addr, WAL: wal.NewMockWAL(), MaxLogLength: 4, ElectionTimeoutMin: 30 * time.Millisecond, ElectionTimeoutMax: 60 * time.Millisecond, HeartbeatInterval: 15 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.Stop)
		if !waitForCond(t, 2*time.Second, r.IsLeader) {
			t.Fatal("no leader")
		}
		return r
	}
	r := start()
	if err := r.ProposeRegisterStorageNodes(ctx, 0, directoryTestNodes); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"live", "deleted"} {
		if err := r.ProposeCreateKB(ctx, testKB(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.ProposeMarkKBDeleting(ctx, "deleted"); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeRemoveKBMeta(ctx, "deleted"); err != nil {
		t.Fatal(err)
	}
	want, _ := r.StorageDirectory(ctx)
	// A few more entries force a snapshot after the tombstone commit.
	for i := 0; i < 5; i++ {
		if err := r.ProposeRegisterStorageNodes(ctx, want.Revision, directoryTestNodes); err != nil {
			t.Fatal(err)
		}
	}
	if !waitForCond(t, 5*time.Second, func() bool {
		data, _, err := r.persister.LoadSnapshot()
		if err != nil || len(data) == 0 {
			return false
		}
		sm := newStateMachine()
		return sm.restore(data) == nil && sm.directory.Revision == want.Revision
	}) {
		t.Fatal("directory snapshot never persisted")
	}
	r.Stop()
	r = start()
	got, err := r.StorageDirectory(ctx)
	if err != nil || !reflect.DeepEqual(want, got) {
		t.Fatalf("after restart: got=%+v want=%+v err=%v", got, want, err)
	}
}

func TestStorageDirectorySurvivesControlLeaderFailover(t *testing.T) {
	ctx := proposeCtx(t)
	peers := make([]PeerConfig, 3)
	for i := range peers {
		peers[i] = PeerConfig{ID: int64(i + 1), RaftAddr: freeLoopbackAddrForTest(t)}
	}
	nodes := make([]*RaftNodeImpl, 3)
	for i := range peers {
		r, err := NewRaftNodeImpl(Config{NodeID: peers[i].ID, DataDir: t.TempDir(), RaftAddr: peers[i].RaftAddr, Peers: peers, WAL: wal.NewMockWAL(), ElectionTimeoutMin: 300 * time.Millisecond, ElectionTimeoutMax: 600 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = r
		t.Cleanup(r.Stop)
	}
	leaderIndex := -1
	findLeader := func() bool {
		for i, r := range nodes {
			if r != nil && r.IsLeader() {
				leaderIndex = i
				return true
			}
		}
		return false
	}
	if !waitForCond(t, 10*time.Second, findLeader) {
		t.Fatal("no initial leader")
	}
	leader := nodes[leaderIndex]
	if err := leader.ProposeRegisterStorageNodes(ctx, 0, directoryTestNodes); err != nil {
		t.Fatal(err)
	}
	if err := leader.ProposeCreateKB(ctx, testKB("kb")); err != nil {
		t.Fatal(err)
	}
	if err := leader.ProposeCreateKBWithPlacement(ctx, testKB("moving"), []int64{1, 2}); err != nil {
		t.Fatal(err)
	}
	migrationReq := storage.MigrationRequest{OperationID: "failover-move", KBID: "moving", ExpectedEpoch: 1, FromID: 2, ToID: 3}
	if err := leader.ProposeBeginKBMigration(ctx, migrationReq); err != nil {
		t.Fatal(err)
	}
	if err := leader.ProposeFenceKBMigration(ctx, migrationReq.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := leader.ProposeAckKBMigrationFence(ctx, migrationReq.OperationID, 1, 1); err != nil {
		t.Fatal(err)
	}
	serviceDir, _ := leader.StorageDirectory(ctx)
	if err := leader.ProposeBlockStorageService(ctx, storage.ServiceRequest{NodeID: 1, OperationID: "blocked-through-failover", ExpectedRevision: serviceDir.Revision}); err != nil {
		t.Fatal(err)
	}
	want, _ := leader.StorageDirectory(ctx)
	if !waitForCond(t, 5*time.Second, func() bool {
		for _, r := range nodes {
			d, err := r.StorageDirectory(ctx)
			if err != nil || !reflect.DeepEqual(d, want) {
				return false
			}
		}
		return true
	}) {
		t.Fatal("directory did not replicate to followers")
	}
	leader.Stop()
	nodes[leaderIndex] = nil
	if !waitForCond(t, 10*time.Second, findLeader) {
		t.Fatal("no replacement leader")
	}
	leader = nodes[leaderIndex]
	if _, err := NewInternalServiceServer(leader).RenewStorageService(ctx, &pb.RenewStorageServiceRequest{NodeId: 1, Address: "s1", RequestId: "after-failover"}); serrors.ReasonOf(err) != "storage_service_paused" {
		t.Fatal("failover reopened blocked node:", err)
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelWait()
	if err := leader.WaitStorageServiceRevoked(waitCtx, storage.ServiceRequest{NodeID: 1, OperationID: "blocked-through-failover"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("new leader skipped revocation grace:", err)
	}
	if err := leader.ProposeRegisterStorageNodes(ctx, want.Revision, []storage.Node{{ID: 1, Addr: "s1", State: storage.NodeDraining}}); err != nil {
		t.Fatal(err)
	}
	snap, err := leader.GetKBPlacement(ctx, "kb")
	if err != nil || snap.Placement.RequiredReplicas != 3 || len(snap.Nodes) != 3 {
		t.Fatalf("failover or draining shrank policy: %+v %v", snap, err)
	}
	if snap.Revision <= want.Revision || snap.Placement.Epoch != 1 {
		t.Fatal("node change must change directory revision without inventing a placement epoch")
	}
	moving, err := leader.GetKBPlacement(ctx, "moving")
	if err != nil || moving.Migration == nil || moving.Migration.Phase != storage.MigrationFencing || !reflect.DeepEqual(moving.Migration.FencedIDs, []int64{1}) {
		t.Fatalf("leader failover lost preparation state: %+v %v", moving, err)
	}
	if _, err := leader.ProposeCreateVersion(ctx, "moving", 0); !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal("replacement leader forgot write admission barrier:", err)
	}
}

func TestStorageSnapshotRecoveryDoesNotConfusePendingIndexWithMissingData(t *testing.T) {
	sm := newStateMachine()
	sm.versions = map[int64]types.VersionMeta{
		1: {VersionID: 1, IndexStatus: types.IndexStatusPending, DataStatus: types.DataStatusDurable},
		2: {VersionID: 2, IndexStatus: types.IndexStatusPending, DataStatus: types.DataStatusPending},
		3: {VersionID: 3, IndexStatus: types.IndexStatusReady, DataStatus: types.DataStatusFailedPermanent},
		4: {VersionID: 4, IndexStatus: types.IndexStatusReady, DataStatus: types.DataStatusDurable, Deleting: true},
	}
	got := map[int64]bool{}
	for _, v := range sm.listAllVersions() {
		got[v.VersionID] = true
	}
	if !reflect.DeepEqual(got, map[int64]bool{1: true, 2: true}) {
		t.Fatal("snapshot skipped a version requiring recovery or resurrected retired data:", got)
	}
}

func TestStorageInitializePreservesDynamicDirectoryAfterRestart(t *testing.T) {
	ctx := context.Background()
	r := NewMockRaftNode(wal.NewMockWAL())
	if err := storage.Initialize(ctx, r, directoryTestNodes); err != nil {
		t.Fatal(err)
	}
	d, _ := r.StorageDirectory(ctx)
	if err := r.ProposeCreateKB(ctx, testKB("original")); err != nil {
		t.Fatal(err)
	}
	d, _ = r.StorageDirectory(ctx)
	if err := r.ProposeRegisterStorageNodes(ctx, d.Revision, []storage.Node{{ID: 9, Addr: "s9"}, {ID: 1, Addr: directoryTestNodes[0].Addr, State: storage.NodeDraining}}); err != nil {
		t.Fatal(err)
	}
	before, _ := r.StorageDirectory(ctx)
	if err := storage.Initialize(ctx, r, directoryTestNodes[:1]); err != nil {
		t.Fatal("old bootstrap configuration overwrote runtime directory:", err)
	}
	after, _ := r.StorageDirectory(ctx)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("initialization reset dynamic state")
	}
	if err := r.ProposeCreateKB(ctx, testKB("after-add")); err != nil {
		t.Fatal(err)
	}
	original, err := r.GetKBPlacement(ctx, "original")
	if err != nil {
		t.Fatal(err)
	}
	if original.Placement.Contains(9) || !original.Placement.Contains(1) {
		t.Fatal("existing policy changed")
	}
	fresh, err := r.GetKBPlacement(ctx, "after-add")
	if err != nil {
		t.Fatal(err)
	}
	if !fresh.Placement.Contains(9) || fresh.Placement.Contains(1) {
		t.Fatal("new KB ignored runtime directory")
	}
}
