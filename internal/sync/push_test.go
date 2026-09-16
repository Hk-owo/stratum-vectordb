package sync

import (
	"context"
	"net"
	"strconv"
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

	if err := follower.PullVersion(ctx, leaderAddr, kbID, versionID); err != nil {
		t.Fatalf("PullVersion: %v", err)
	}

	got := adv.got()
	if len(got) != 1 || got[0] != kbID+"/"+strconv.FormatInt(versionID, 10) {
		t.Fatalf("cursor after pull = %v, want exactly [%s/%d]", got, kbID, versionID)
	}
}

// fixedHolders is a HolderSource with a fixed table.
type fixedHolders struct {
	holders map[string][]string
}

func (h *fixedHolders) HolderAddresses(kbID string, _ int64) []string {
	return h.holders[kbID]
}

// The leader answers about EVERY knowledge base it knows, not only the ones the
// reporter named.
//
// A node that missed a whole chain names nothing — its own cursor map is empty — so
// an answer restricted to the named set hands it back exactly the emptiness it
// already has, and it never learns there is something to be behind on. Measured: a
// replica that had been offline for 55 versions reported reported_kbs=0, and every
// response came back with no tails at all.
func TestPushHandler_ReportDataVersions_AnswersAboutChainsTheReporterNeverNamed(t *testing.T) {
	rec := &recordingRecorder{}
	// A DIFFERENT node says it went through kb-1; the reporter below says nothing.
	rec.Record(11, "other:7001", map[string]int64{"kb-1": 55})

	h := NewPushHandler(nil, 7,
		WithDataVersionAggregator(rec, func() bool { return true }),
		WithChainTails(&fixedChainTails{tails: map[string]int64{"kb-1": 55}}),
		WithHolders(&fixedHolders{holders: map[string][]string{"kb-1": {"other:7001"}}}),
	)

	resp, err := h.ReportDataVersions(context.Background(), &pb.ReportDataVersionsRequest{
		NodeId:  3,
		Address: "me:7003",
		// This node holds nothing at all: it names no knowledge base.
		DataVersions: map[string]int64{},
	})
	if err != nil {
		t.Fatalf("ReportDataVersions: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatal("the leader must accept the report")
	}
	if got := resp.GetChainTails()["kb-1"]; got != 55 {
		t.Errorf("chain_tails = %v, want kb-1:55 even though the reporter named nothing",
			resp.GetChainTails())
	}
	addrs := resp.GetHolders()["kb-1"].GetAddresses()
	if len(addrs) != 1 || addrs[0] != "other:7001" {
		t.Errorf("holders = %v, want kb-1:[other:7001]", resp.GetHolders())
	}
}
