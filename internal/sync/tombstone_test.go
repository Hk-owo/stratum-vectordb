package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
	"stratum/internal/tombstone"
)

// A version this node knows was deleted is refused on the push path: nothing of it
// is written, and the sender is told SUCCESS.
//
// Success is the deliberate half. An error would make the coordinator retry a
// version that cannot be wanted again (its metadata is gone), and every retry would
// be refused the same way — so the answer has to be "there is nothing for you to do
// here", which is what an idempotent prefix delete of data that is already gone
// says as well.
func TestPushHandler_RefusesAPushedVersionItKnowsWasDeleted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := tombstone.NewMemoryStore()
	if err := store.Note(ctx, "kb-push", 9, time.Now()); err != nil {
		t.Fatalf("Note: %v", err)
	}

	leader, _, _ := startLeaderServer(t, newFakeVecstore())
	kbID, versionID := "kb-push", int64(9)
	if err := leader.docStore.Write(ctx, kbID, "doc-1", versionID, []byte("pushed content")); err != nil {
		t.Fatal(err)
	}
	if err := leader.versionDoc.Write(ctx, kbID, versionID, "doc-1"); err != nil {
		t.Fatal(err)
	}

	follower, _, addr := startPushServer(t, 2, WithVersionTombstones(store))

	ack, err := NewPusher(PusherConfig{Exporter: leader.handler, NodeID: 1}).PushVersion(ctx, addr, kbID, versionID)
	if err != nil {
		t.Fatalf("PushVersion: %v", err)
	}
	if ack.GetVersionId() != versionID || ack.GetKnowledgeBaseId() != kbID {
		t.Errorf("ack = (kb %q, v%d), want the version the sender pushed", ack.GetKnowledgeBaseId(), ack.GetVersionId())
	}

	// Nothing landed: the document set is the observable half of "this node did not
	// take the version".
	docs, err := follower.versionDoc.ListDocIDs(ctx, kbID, versionID)
	if err != nil {
		t.Fatalf("ListDocIDs: %v", err)
	}
	if len(docs) != 0 {
		t.Fatalf("versiondoc holds %v after a refused push, want nothing", docs)
	}
}

// The pull path refuses before it dials: the source is never asked for data this
// node already knows is deleted. The error is the package's own verdict, so the
// caller can skip rather than fail.
func TestFollower_PullOfADeletedVersionIsRefusedBeforeDialling(t *testing.T) {
	store := tombstone.NewMemoryStore()
	ctx := context.Background()
	if err := store.Note(ctx, "kb-1", 7, time.Now()); err != nil {
		t.Fatalf("Note: %v", err)
	}

	// No stores: the refusal must happen before anything is opened or dialled. The
	// address below has no listener, so a dial would fail with a transport error
	// instead of the verdict this test asserts on.
	follower := NewFollower(nil, nil, nil, nil, nil)
	follower.SetVersionTombstones(store)

	// A port nothing listens on: if the refusal worked, the dial never happens and
	// the error is the tombstone verdict rather than a connection failure.
	err := follower.PullVersionData(ctx, "127.0.0.1:1", "kb-1", 7)
	if !errors.Is(err, errVersionTombstoned) {
		t.Fatalf("PullVersionData = %v, want errVersionTombstoned", err)
	}
}

// The deletion path records the notification BEFORE it reclaims, so a crash in
// between leaves "told, not cleaned up" — a state this node can finish by itself.
// The entry then moves to Done, where it stays for the grace window so a late
// arrival is still refused.
func TestPushHandler_DeleteVersionDataRecordsAndFinishesTheTombstone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := tombstone.NewMemoryStore()
	dropper := &stubSyncDropper{}
	_, _, addr := startPushServer(t, 4, WithVersionDataDropper(dropper), WithVersionTombstones(store))

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := pb.NewDataSyncServiceClient(conn).DeleteVersionData(ctx, &pb.DeleteVersionDataRequest{
		KnowledgeBaseId: "kb-1", VersionId: 7,
	}); err != nil {
		t.Fatalf("DeleteVersionData: %v", err)
	}

	gone, err := store.Contains(ctx, "kb-1", 7)
	if err != nil {
		t.Fatalf("Contains: %v", err)
	}
	if !gone {
		t.Fatal("the notification was not recorded; a late push would re-land the version")
	}
	pending, err := store.Pending(ctx)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("Pending = %+v, want none — the reclaim succeeded, so nothing is owed", pending)
	}
}

// A reclaim that fails must leave the entry OWED: the tombstone is what remembers
// the deletion after the RPC's retries are spent, and the local retry loop is what
// pays it.
func TestPushHandler_DeleteVersionDataLeavesTheTombstonePendingWhenTheReclaimFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := tombstone.NewMemoryStore()
	dropper := &stubSyncDropper{err: errors.New("disk refused the delete")}
	_, _, addr := startPushServer(t, 4, WithVersionDataDropper(dropper), WithVersionTombstones(store))

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := pb.NewDataSyncServiceClient(conn).DeleteVersionData(ctx, &pb.DeleteVersionDataRequest{
		KnowledgeBaseId: "kb-1", VersionId: 7,
	}); err == nil {
		t.Fatal("expected the RPC to fail when the reclaim failed")
	}

	pending, err := store.Pending(ctx)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 || pending[0].KBID != "kb-1" || pending[0].VersionID != 7 {
		t.Fatalf("Pending = %+v, want the unfinished kb-1/v7", pending)
	}
}
