package sync

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/versiondoc"
)

// startPushServer hosts a PushHandler over real Pebble stores, returning the
// follower (so tests can inspect what was applied), its build trigger, and the
// listener address.
func startPushServer(t *testing.T, nodeID int64, opts ...PushHandlerOption) (*Follower, *recordingTrigger, string) {
	t.Helper()
	ds, err := docstore.NewPebbleDocStore(t.TempDir())
	if err != nil {
		t.Fatalf("docstore: %v", err)
	}
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	vd, err := versiondoc.NewPebbleVersionDocList(t.TempDir())
	if err != nil {
		t.Fatalf("versiondoc: %v", err)
	}
	t.Cleanup(func() {
		ds.Close()
		cdm.Close()
		vd.Close()
	})

	trigger := &recordingTrigger{}
	follower := NewFollower(ds, cdm, vd, chunkstore.NewMockChunkStore(), trigger)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterDataSyncServiceServer(srv, NewPushHandler(follower, nodeID, opts...))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	return follower, trigger, lis.Addr().String()
}

// TestPusher_PushVersion_EndToEnd is the module-interaction test for the push
// direction (Stratum_设计文档v13.md §7.2): a real LeaderHandler exports over real
// gRPC into a real PushHandler whose stores are real PebbleDB instances. The
// target must end up with the dataset the pull path would have produced, and
// must schedule its own build.
func TestPusher_PushVersion_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leader, _, _ := startLeaderServer(t, newFakeVecstore())
	kbID, versionID := "kb-push", int64(9)
	if err := leader.docStore.Write(ctx, kbID, "doc-1", versionID, []byte("pushed content")); err != nil {
		t.Fatal(err)
	}
	if err := leader.chunkDoc.Write(ctx, kbID, "c-1", "doc-1"); err != nil {
		t.Fatal(err)
	}
	if err := leader.versionDoc.Write(ctx, kbID, versionID, "doc-1"); err != nil {
		t.Fatal(err)
	}
	leader.vecstore.vectors[encodeVecstoreKey(kbID, "c-1")] = []float32{1, 2, 3, 4}

	follower, trigger, addr := startPushServer(t, 2)

	pusher := NewPusher(PusherConfig{Exporter: leader.handler, NodeID: 1})
	ack, err := pusher.PushVersion(ctx, addr, kbID, versionID)
	if err != nil {
		t.Fatalf("PushVersion: %v", err)
	}
	if ack.GetAcceptorId() != 2 {
		t.Errorf("ack acceptor = %d, want 2 (the replica that applied the stream)", ack.GetAcceptorId())
	}
	if ack.GetVersionId() != versionID || ack.GetKnowledgeBaseId() != kbID {
		t.Errorf("ack = (kb %q, v%d), want (%q, %d)", ack.GetKnowledgeBaseId(), ack.GetVersionId(), kbID, versionID)
	}

	// The target's stores hold what the pull path would have written.
	if got, err := follower.docStore.ReadAt(ctx, kbID, "doc-1", versionID); err != nil || string(got) != "pushed content" {
		t.Errorf("target docstore = (%q, %v), want (pushed content, nil)", got, err)
	}
	docs, err := follower.versionDoc.ListDocIDs(ctx, kbID, versionID)
	if err != nil || len(docs) != 1 || docs[0] != "doc-1" {
		t.Errorf("target versiondoc = %v, %v", docs, err)
	}
	if chunkIDs, err := follower.chunkDoc.ListChunkIDsByDocs(ctx, kbID, []string{"doc-1"}); err != nil || len(chunkIDs) != 1 || chunkIDs[0] != "c-1" {
		t.Errorf("target chunkdoc = %v, %v", chunkIDs, err)
	}
	// The target builds NOTHING (§8.6b). A push delivers data; the version is
	// built when something asks for it, and §8.4 owner sharding decides which node
	// that is. The handler used to build here for every landing, gated on "am I the
	// owner head" — correct about who, wrong about whether.
	if trigger.len() != 0 {
		t.Errorf("TriggerBuild calls = %d, want 0: a push must not build", trigger.len())
	}
}

// Pushing the same version twice must leave the target unchanged: every record
// is keyed and idempotent, so a retried push after a lost acknowledgement is
// safe.
func TestPusher_PushVersion_IdempotentOnRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leader, _, _ := startLeaderServer(t, newFakeVecstore())
	kbID, versionID := "kb-push", int64(3)
	if err := leader.docStore.Write(ctx, kbID, "doc-1", versionID, []byte("once")); err != nil {
		t.Fatal(err)
	}
	if err := leader.versionDoc.Write(ctx, kbID, versionID, "doc-1"); err != nil {
		t.Fatal(err)
	}

	follower, _, addr := startPushServer(t, 3)
	pusher := NewPusher(PusherConfig{Exporter: leader.handler, NodeID: 1})

	for i := 0; i < 2; i++ {
		if _, err := pusher.PushVersion(ctx, addr, kbID, versionID); err != nil {
			t.Fatalf("PushVersion #%d: %v", i+1, err)
		}
	}
	if docs, err := follower.versionDoc.ListDocIDs(ctx, kbID, versionID); err != nil || len(docs) != 1 {
		t.Errorf("after a retried push the target versiondoc = %v, %v (want exactly one doc)", docs, err)
	}
}

// An unreachable target must fail loudly rather than silently dropping the
// replication, since the coordinator counts acknowledgements against quorum.
func TestPusher_PushVersion_UnreachableTargetFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	leader, _, _ := startLeaderServer(t, newFakeVecstore())
	pusher := NewPusher(PusherConfig{Exporter: leader.handler, NodeID: 1})

	// Reserve a port and close it again so nothing is listening.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := lis.Addr().String()
	lis.Close()

	if _, err := pusher.PushVersion(ctx, deadAddr, "kb-push", 1); err == nil {
		t.Fatal("pushing to an unreachable replica must return an error")
	}
}

// recordingAdvancer records the versions a received push/pull declared this
// node to hold.
type recordingAdvancer struct {
	mu     sync.Mutex
	marked []string
}

func (a *recordingAdvancer) MarkVersionContiguous(kbID string, versionID int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.marked = append(a.marked, kbID+"/"+strconv.FormatInt(versionID, 10))
}

func (a *recordingAdvancer) got() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.marked...)
}

// TestPusher_PushVersion_AdvancesTheCursor pins the fix for the read path.
//
// Receiving a version's records is what makes a node hold it, and §9.3(2)'s
// freshness check reads the cursor to decide whether the node may answer a
// query. Without this the replica had every record on disk and still answered
// "local history reaches version 0", so the station refused it as stale and the
// read failed — the failure mode TestT4_DataVolume and
// TestT4_MultiNode_Consistency both hit.
func TestPusher_PushVersion_AdvancesTheCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leader, _, _ := startLeaderServer(t, newFakeVecstore())
	kbID, versionID := "kb-cursor", int64(5)
	if err := leader.docStore.Write(ctx, kbID, "doc-1", versionID, []byte("content")); err != nil {
		t.Fatal(err)
	}
	if err := leader.versionDoc.Write(ctx, kbID, versionID, "doc-1"); err != nil {
		t.Fatal(err)
	}

	adv := &recordingAdvancer{}
	_, _, addr := startPushServer(t, 4, WithLocalVersionAdvancer(adv))

	if _, err := NewPusher(PusherConfig{Exporter: leader.handler, NodeID: 1}).PushVersion(ctx, addr, kbID, versionID); err != nil {
		t.Fatalf("PushVersion: %v", err)
	}

	got := adv.got()
	if len(got) != 1 || got[0] != kbID+"/"+strconv.FormatInt(versionID, 10) {
		t.Fatalf("cursor after push = %v, want exactly [%s/%d]", got, kbID, versionID)
	}
}

// TestFollower_PullVersion_AdvancesTheCursor is the same fact over the pull
// direction: a node that fetched a version's records holds it too, so the
// cursor must move there as well.
func TestFollower_PullVersion_AdvancesTheCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leader, _, leaderAddr := startLeaderServer(t, newFakeVecstore())
	kbID, versionID := "kb-pull-cursor", int64(6)
	if err := leader.docStore.Write(ctx, kbID, "doc-1", versionID, []byte("pulled content")); err != nil {
		t.Fatal(err)
	}
	if err := leader.versionDoc.Write(ctx, kbID, versionID, "doc-1"); err != nil {
		t.Fatal(err)
	}

	adv := &recordingAdvancer{}
	follower, _, _ := startPushServer(t, 5)
	follower.SetLocalVersionAdvancer(adv)

	if err := follower.PullVersionData(ctx, leaderAddr, kbID, versionID); err != nil {
		t.Fatalf("PullVersionData: %v", err)
	}

	got := adv.got()
	if len(got) != 1 || got[0] != kbID+"/"+strconv.FormatInt(versionID, 10) {
		t.Fatalf("cursor after pull = %v, want exactly [%s/%d]", got, kbID, versionID)
	}
}

// A pushed entry that cannot be applied has to be logged where it happens: the sender
// only sees its stream end early (`stream ChunkDocMapper forward: EOF`), and before this
// line neither side recorded why.
func TestPushHandler_PushVersionData_LogsAnEntryItCannotApply(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	_, _, addr := startPushServer(t, 7, WithLogger(zap.New(core)))

	stream, err := dialSyncServer(t, addr).PushVersionData(context.Background())
	if err != nil {
		t.Fatalf("PushVersionData: %v", err)
	}
	if err := stream.Send(&pb.SyncEntry{EntryType: pb.SyncEntryType(9999), KbId: "kb-1", VersionId: 7}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Fatal("the entry could not be applied, so the push must fail")
	}

	entries := logs.FilterMessageSnippet("could not be applied").All()
	if len(entries) != 1 {
		t.Fatalf("warnings = %+v, want one naming the failed entry", logs.All())
	}
	fields := entries[0].ContextMap()
	if fields["kb_id"] != "kb-1" || fields["version_id"] != int64(7) {
		t.Errorf("fields = %v, want kb-1 v7", fields)
	}
	if _, ok := fields["error"]; !ok {
		t.Errorf("fields = %v, want the cause", fields)
	}
}

// exportUntilStopped sends entries until the stream refuses one, like a real export does.
type exportUntilStopped struct {
	entry *pb.SyncEntry
	pause time.Duration
}

func (e exportUntilStopped) ExportVersion(_ context.Context, _ string, _ int64, send func(*pb.SyncEntry) error) error {
	for {
		if err := send(e.entry); err != nil {
			return fmt.Errorf("sync: stream ChunkDocMapper forward: %w", err)
		}
		time.Sleep(e.pause)
	}
}

// Send only ever says EOF once a stream has ended. The failure has to say why it ended,
// because a broken replica and a push that ran out of time call for different fixes.
func TestPusher_ReportsWhatEndedTheStream(t *testing.T) {
	_, _, addr := startPushServer(t, 7)

	t.Run("the receiver refused", func(t *testing.T) {
		pusher := NewPusher(PusherConfig{Exporter: exportUntilStopped{
			entry: &pb.SyncEntry{EntryType: pb.SyncEntryType(9999), KbId: "kb-1", VersionId: 7},
			pause: time.Millisecond,
		}, NodeID: 1})

		_, err := pusher.PushVersion(context.Background(), addr, "kb-1", 7)
		if err == nil || !strings.Contains(err.Error(), "unknown SyncEntryType") {
			t.Fatalf("err = %v, want the receiver's reason", err)
		}
		if !strings.Contains(err.Error(), "ChunkDocMapper forward") {
			t.Errorf("err = %v, want where the export stopped too", err)
		}
	})

	t.Run("the push ran out of time", func(t *testing.T) {
		pusher := NewPusher(PusherConfig{Exporter: exportUntilStopped{
			entry: &pb.SyncEntry{EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST, KbId: "kb-1", VersionId: 7, DocId: "doc-1"},
			pause: time.Millisecond,
		}, NodeID: 1})
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := pusher.PushVersion(ctx, addr, "kb-1", 7)
		if err == nil || !strings.Contains(err.Error(), "DeadlineExceeded") {
			t.Fatalf("err = %v, want the deadline named", err)
		}
	})
}
