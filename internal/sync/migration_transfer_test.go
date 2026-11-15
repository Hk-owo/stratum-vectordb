package sync

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/pebbleutil"
	"stratum/internal/raft"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wal"
)

type migrationFixtureMetadata interface {
	raft.RaftNode
	storage.Manager
	storage.MigrationManager
	storage.MaintenanceManager
	storage.MigrationTransferReader
	ProposeCreateKBWithPlacement(context.Context, types.KnowledgeBaseMeta, []int64) error
}
type migrationTransferFixture struct {
	metadata       migrationFixtureMetadata
	source, target *leaderFixture
	targetHandler  *NodeHandler
	request        *pb.MigrationTransferRequest
	operation      storage.MigrationRequest
	versions       []int64
	root           string
}

func migrationBufDial(t *testing.T, handler pb.DataSyncServiceServer) func(context.Context, string) (*grpc.ClientConn, error) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pb.RegisterDataSyncServiceServer(server, handler)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return func(ctx context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///migration", grpc.WithContextDialer(func(c context.Context, _ string) (net.Conn, error) { return listener.DialContext(c) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
}
func fillMigrationSource(t *testing.T, f *leaderFixture, ids []int64) {
	t.Helper()
	ctx := context.Background()
	for i, body := range [][]byte{[]byte("old-body"), []byte("new-body"), nil} {
		if err := f.docStore.Write(ctx, "kb", "doc", ids[i], body); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids[:2] {
		if err := f.versionDoc.Write(ctx, "kb", id, "doc"); err != nil {
			t.Fatal(err)
		}
	}
	for _, chunk := range []string{"old-chunk", "new-chunk"} {
		if err := f.chunkDoc.Write(ctx, "kb", chunk, "doc"); err != nil {
			t.Fatal(err)
		}
		f.vecstore.vectors[encodeVecstoreKey("kb", chunk)] = []float32{1, 2, 3}
	}
}
func newMigrationTransferFixture(t *testing.T, activeIndex ...int) *migrationTransferFixture {
	return newMigrationTransferFixtureWithMetadata(t, raft.NewMockRaftNode(wal.NewMockWAL()), activeIndex...)
}
func newMigrationTransferFixtureWithMetadata(t *testing.T, r migrationFixtureMetadata, activeIndex ...int) *migrationTransferFixture {
	t.Helper()
	ctx := context.Background()
	if err := r.ProposeRegisterStorageNodes(ctx, 0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}}); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeCreateKBWithPlacement(ctx, types.KnowledgeBaseMeta{KBID: "kb"}, []int64{1, 2}); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	parent := int64(0)
	for i := 0; i < 3; i++ {
		id, err := r.ProposeCreateVersion(ctx, "kb", parent)
		if err != nil {
			t.Fatal(err)
		}
		digest := types.EmptyDocIDSetHash
		if i < 2 {
			digest = ComputeDocIDSetHash([]string{"doc"})
		}
		if err := r.ProposeUpdateVersionSummary(ctx, id, digest); err != nil {
			t.Fatal(err)
		}
		if err := r.ProposeMarkVersionDataDurable(ctx, id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		parent = id
	}
	active := 2
	if len(activeIndex) > 0 {
		active = activeIndex[0]
	}
	if err := r.ProposeRollback(ctx, "kb", ids[active]); err != nil {
		t.Fatal(err)
	}
	op := storage.MigrationRequest{KBID: "kb", OperationID: "move", ExpectedEpoch: 1, FromID: 2, ToID: 3}
	if err := r.ProposeBeginKBMigration(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, op.OperationID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2, 3} {
		if err := r.ProposeAckKBMigrationFence(ctx, op.OperationID, 1, id); err != nil {
			t.Fatal(err)
		}
	}
	source, target := newLeaderFixture(t), newLeaderFixture(t)
	fillMigrationSource(t, source, ids)
	root := t.TempDir()
	sourceHandler := NewNodeHandler(source.handler, NewPushHandler(nil, 1, WithStoragePlacement(r.GetKBPlacement)), WithMigrationStaging(r, t.TempDir()))
	targetHandler := NewNodeHandler(target.handler, NewPushHandler(nil, 3, WithStoragePlacement(r.GetKBPlacement)), WithMigrationStaging(r, root))
	targetHandler.migration.dial = migrationBufDial(t, sourceHandler)
	return &migrationTransferFixture{metadata: r, source: source, target: target, targetHandler: targetHandler, operation: op, request: &pb.MigrationTransferRequest{KnowledgeBaseId: "kb", OperationId: "move", ExpectedEpoch: 1, SourceNodeId: 1}, versions: ids, root: root}
}
func TestMigrationTransferStagesHistoricalMVCCAndEmptyVersionWithoutPublishing(t *testing.T) {
	f := newMigrationTransferFixture(t)
	ctx := context.Background()
	if err := f.target.docStore.Write(ctx, "kb", "existing-local", 1, []byte("untouched")); err != nil {
		t.Fatal(err)
	}
	receipt, err := f.targetHandler.StageKBMigration(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.GetTargetNodeId() != 3 || receipt.GetSourceNodeId() != 1 || receipt.GetByteCount() == 0 || receipt.GetEntryCount() == 0 {
		t.Fatal(receipt)
	}
	path := migrationStagePath(f.root, "kb", "move")
	artifact, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Close()
	tombstone := false
	visible := map[int64]string{}
	ends := map[int64]bool{}
	for {
		frame, err := readMigrationFrame(artifact)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e := frame.GetEntry(); e != nil && e.EntryType == pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE {
			if frame.VersionId == 0 && e.VersionId == f.versions[2] && len(e.Payload) == 1 && e.Payload[0] == tagTombstone {
				tombstone = true
			}
			if frame.VersionId > 0 {
				visible[frame.VersionId] = string(e.Payload[1:])
			}
		}
		if frame.VersionEnd {
			ends[frame.VersionId] = true
		}
	}
	if !tombstone || visible[f.versions[0]] != "old-body" || visible[f.versions[1]] != "new-body" || !ends[f.versions[2]] {
		t.Fatal("MVCC/history/empty marker lost", tombstone, visible, ends)
	}
	if data, err := f.target.docStore.ReadAt(ctx, "kb", "existing-local", 1); err != nil || string(data) != "untouched" {
		t.Fatal("staging mutated formal storage", string(data), err)
	}
	if docs, err := f.target.versionDoc.ListDocIDs(ctx, "kb", f.versions[0]); err != nil || len(docs) != 0 {
		t.Fatal("staging advanced formal document data", docs, err)
	}
	if _, err := f.targetHandler.receiver.bindPlacement(ctx, "kb"); !errors.Is(err, serrors.ErrNotKBReplica) {
		t.Fatal("learner became an ordinary replica", err)
	}
	reads := len(f.source.vecstore.readKeys)
	restored := NewNodeHandler(f.target.handler, NewPushHandler(nil, 3), WithMigrationStaging(f.metadata, f.root))
	restored.migration.dial = func(context.Context, string) (*grpc.ClientConn, error) {
		t.Fatal("durable stage retry contacted source")
		return nil, nil
	}
	retry, err := restored.StageKBMigration(ctx, f.request)
	if err != nil || !proto.Equal(receipt, retry) || len(f.source.vecstore.readKeys) != reads {
		t.Fatal("restart did not reuse verified artifact", retry, err)
	}
	if err := f.metadata.ProposeRecordKBMigrationStage(ctx, "move", stageReceiptFromProto(receipt)); err != nil {
		t.Fatal(err)
	}
	snap, err := f.metadata.GetKBPlacement(ctx, "kb")
	if err != nil || snap.Migration.Phase != storage.MigrationStaged || snap.Placement.Contains(3) || snap.Placement.Epoch != 1 {
		t.Fatal("stage receipt activated learner", snap, err)
	}
}
func TestMigrationTransferRejectsMissingSourceDataAndNeverPublishesPartialArtifact(t *testing.T) {
	for _, missing := range []string{"vector", "body", "document-set", "forward-relation", "reverse-relation"} {
		t.Run(missing, func(t *testing.T) {
			f := newMigrationTransferFixture(t)
			ctx := context.Background()
			switch missing {
			case "vector":
				delete(f.source.vecstore.vectors, encodeVecstoreKey("kb", "old-chunk"))
			case "body":
				if err := f.source.docStore.DeleteByVersion(ctx, "kb", f.versions[0]); err != nil {
					t.Fatal(err)
				}
			case "forward-relation", "reverse-relation":
				direction := byte('F')
				parts := []string{"kb", "old-chunk", "doc"}
				if missing == "reverse-relation" {
					direction = 'R'
					parts = []string{"kb", "doc", "old-chunk"}
				}
				key := []byte{direction}
				for _, part := range parts {
					key = append(key, pebbleutil.EncodeString(part)...)
				}
				if err := f.source.chunkDoc.DB().Delete(key, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "document-set":
				if err := f.source.versionDoc.DeleteByVersion(ctx, "kb", f.versions[0]); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.targetHandler.StageKBMigration(ctx, f.request); err == nil {
				t.Fatal("incomplete source produced a receipt")
			}
			if _, err := os.Stat(migrationStagePath(f.root, "kb", "move")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("partial artifact was published", err)
			}
			files, err := os.ReadDir(f.root)
			if err != nil || len(files) != 0 {
				t.Fatal("temporary data leaked", files, err)
			}
		})
	}
}
func TestMigrationTransferCorruptArtifactFailsClosed(t *testing.T) {
	f := newMigrationTransferFixture(t)
	ctx := context.Background()
	if _, err := f.targetHandler.StageKBMigration(ctx, f.request); err != nil {
		t.Fatal(err)
	}
	path := migrationStagePath(f.root, "kb", "move")
	if err := os.WriteFile(path, []byte{0, 0, 0, 0}, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.targetHandler.StageKBMigration(ctx, f.request); serrors.ReasonOf(err) != "migration_conflict" {
		t.Fatal("corrupt artifact acknowledged", err)
	}
	if snap, err := f.metadata.GetKBPlacement(ctx, "kb"); err != nil || snap.Migration.Phase != storage.MigrationFenced {
		t.Fatal(snap, err)
	}
}
func TestMigrationTransferCancelledTaskCannotAcknowledgeCachedArtifact(t *testing.T) {
	f := newMigrationTransferFixture(t)
	ctx := context.Background()
	if _, err := f.targetHandler.StageKBMigration(ctx, f.request); err != nil {
		t.Fatal(err)
	}
	if err := f.metadata.ProposeCancelKBMigration(ctx, "move"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.targetHandler.StageKBMigration(ctx, f.request); err == nil {
		t.Fatal("cancelled task acknowledged historical artifact")
	}
}

func TestMigrationTransferDurabilityFailureMustBeRetriedBeforeAck(t *testing.T) {
	f := newMigrationTransferFixture(t)
	ctx := context.Background()
	fail := true
	f.targetHandler.migration.syncDir = func(path string) error {
		if path == f.root && fail {
			return errors.New("directory fsync failed")
		}
		return syncMigrationDir(path)
	}
	if _, err := f.targetHandler.StageKBMigration(ctx, f.request); err == nil {
		t.Fatal("fsync failure acknowledged")
	}
	if _, err := os.Stat(migrationStagePath(f.root, "kb", "move")); err != nil {
		t.Fatal("test did not reach rename", err)
	}
	reads := len(f.source.vecstore.readKeys)
	if _, err := f.targetHandler.StageKBMigration(ctx, f.request); err == nil {
		t.Fatal("cached existence converted fsync failure into ACK")
	}
	fail = false
	if _, err := f.targetHandler.StageKBMigration(ctx, f.request); err != nil {
		t.Fatal(err)
	}
	if len(f.source.vecstore.readKeys) != reads {
		t.Fatal("complete artifact was downloaded again")
	}
}
func TestMigrationTransferSchedulerFallsBackToCompleteSourceAndPersistsStage(t *testing.T) {
	f := newMigrationTransferFixture(t)
	ctx := context.Background()
	second := newLeaderFixture(t)
	fillMigrationSource(t, second, f.versions)
	delete(f.source.vecstore.vectors, encodeVecstoreKey("kb", "old-chunk"))
	secondHandler := NewNodeHandler(second.handler, NewPushHandler(nil, 2), WithMigrationStaging(f.metadata, t.TempDir()))
	firstDial := f.targetHandler.migration.dial
	secondDial := migrationBufDial(t, secondHandler)
	f.targetHandler.migration.dial = func(c context.Context, addr string) (*grpc.ClientConn, error) {
		if addr == "s2" {
			return secondDial(c, addr)
		}
		return firstDial(c, addr)
	}
	client := &FenceClient{Dial: migrationBufDial(t, f.targetHandler)}
	defer client.Close()
	scheduler := &storage.FenceScheduler{Manager: f.metadata, IsLeader: func() bool { return true }, Versions: f.metadata.ListVersions, Stage: client.Stage}
	if err := scheduler.Step(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.metadata.GetKBPlacement(ctx, "kb")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Migration.Phase != storage.MigrationStaged || snapshot.Migration.Stage == nil || snapshot.Migration.Stage.SourceID != 2 || snapshot.Placement.Contains(3) {
		t.Fatal("fallback/stage persistence failed", snapshot)
	}
	scheduler.Stage = func(context.Context, storage.Node, storage.Node, storage.Migration) (storage.MigrationStageReceipt, error) {
		t.Fatal("staged task retransmitted after scheduler restart")
		return storage.MigrationStageReceipt{}, nil
	}
	if err := scheduler.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.metadata.ProposeCreateVersion(ctx, "kb", f.versions[2]); !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal("stage incorrectly resumed writes", err)
	}
}

type captureMigrationStream struct {
	grpc.ServerStream
	ctx    context.Context
	frames []*pb.MigrationDataFrame
}

func (s *captureMigrationStream) Context() context.Context { return s.ctx }
func (s *captureMigrationStream) Send(f *pb.MigrationDataFrame) error {
	s.frames = append(s.frames, proto.Clone(f).(*pb.MigrationDataFrame))
	return nil
}

type interruptedMigrationExporter struct {
	pb.UnimplementedDataSyncServiceServer
	frames []*pb.MigrationDataFrame
	tail   error
	after  func()
}

func (s interruptedMigrationExporter) ExportKBMigration(_ *pb.MigrationTransferRequest, stream pb.DataSyncService_ExportKBMigrationServer) error {
	for _, f := range s.frames {
		if err := stream.Send(f); err != nil {
			return err
		}
	}
	if s.after != nil {
		s.after()
	}
	return s.tail
}
func TestMigrationTransferRejectsTruncationTransportFailureAndLateCancellation(t *testing.T) {
	for _, mode := range []string{"missing-footer", "failure-after-footer", "cancel-after-footer"} {
		t.Run(mode, func(t *testing.T) {
			f := newMigrationTransferFixture(t)
			ctx := context.Background()
			source := NewNodeHandler(f.source.handler, NewPushHandler(nil, 1), WithMigrationStaging(f.metadata, t.TempDir()))
			captured := &captureMigrationStream{ctx: ctx}
			if err := source.ExportKBMigration(f.request, captured); err != nil {
				t.Fatal(err)
			}
			fake := interruptedMigrationExporter{frames: captured.frames}
			switch mode {
			case "missing-footer":
				fake.frames = fake.frames[:len(fake.frames)-1]
			case "failure-after-footer":
				fake.tail = status.Error(codes.Unavailable, "transport failed")
			case "cancel-after-footer":
				fake.after = func() {
					if err := f.metadata.ProposeCancelKBMigration(ctx, "move"); err != nil {
						t.Error(err)
					}
				}
			}
			f.targetHandler.migration.dial = migrationBufDial(t, fake)
			if _, err := f.targetHandler.StageKBMigration(ctx, f.request); err == nil {
				t.Fatal("incomplete/cancelled transfer acknowledged")
			}
			if _, err := os.Stat(migrationStagePath(f.root, "kb", "move")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid transfer published artifact", err)
			}
		})
	}
}
