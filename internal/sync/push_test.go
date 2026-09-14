package sync

import (
	"context"
	"net"
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
	if trigger.len() == 0 {
		t.Error("the target must schedule its own index build after applying a pushed version")
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
