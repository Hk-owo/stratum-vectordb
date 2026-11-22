package sync

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkstore"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

// Five real gRPC receivers with independent Pebble stores. Even a stale caller
// which tries every physical node cannot land a KB on an unassigned receiver.
func TestPlacementFiveNodeOverlappingReceivers(t *testing.T) {
	ctx := context.Background()
	dir := storage.NewDirectory()
	resolve := func(_ context.Context, kb string) (storage.Snapshot, error) { return dir.Resolve(kb) }
	var followers []*Follower
	var nodes []storage.Node
	for id := int64(1); id <= 5; id++ {
		f, _, addr := startPushServer(t, id, WithStoragePlacement(resolve))
		followers = append(followers, f)
		nodes = append(nodes, storage.Node{ID: id, Addr: addr})
	}
	if err := dir.RegisterNodes(0, nodes); err != nil {
		t.Fatal(err)
	}
	for kb, ids := range map[string][]int64{"A": {1, 2, 3}, "B": {2, 4, 5}} {
		if err := dir.AssignNew(storage.Placement{KBID: kb, Epoch: 1, ReplicaIDs: ids, RequiredReplicas: 3, OperationID: kb}); err != nil {
			t.Fatal(err)
		}
	}
	source, _, _ := startLeaderServer(t, newFakeVecstore())
	pusher := NewPusher(PusherConfig{Exporter: source.handler, NodeID: 2})
	for _, kb := range []string{"A", "B"} {
		if err := source.docStore.Write(ctx, kb, "doc", 1, []byte(kb)); err != nil {
			t.Fatal(err)
		}
		if err := source.versionDoc.Write(ctx, kb, 1, "doc"); err != nil {
			t.Fatal(err)
		}
		snap, err := dir.Resolve(kb)
		if err != nil {
			t.Fatal(err)
		}
		op := storage.Bind(ctx, snap)
		for i, n := range nodes {
			_, err := pusher.PushVersion(op, n.Addr, kb, 1)
			if snap.Placement.Contains(n.ID) {
				if err != nil {
					t.Fatalf("%s member %d: %v", kb, n.ID, err)
				}
			} else if serrors.ReasonOf(err) != "not_kb_replica" {
				t.Fatalf("%s outsider %d was accepted: %v", kb, n.ID, err)
			}
			docs, readErr := followers[i].versionDoc.ListDocIDs(ctx, kb, 1)
			if readErr != nil {
				t.Fatal(readErr)
			}
			want := 0
			if snap.Placement.Contains(n.ID) {
				want = 1
			}
			if len(docs) != want {
				t.Fatalf("%s node %d stored %v", kb, n.ID, docs)
			}
		}
		// Obsolete task epoch is refused before version 2 can land.
		snap.Placement.Epoch = 2
		_, err = pusher.PushVersion(storage.Bind(ctx, snap), nodes[1].Addr, kb, 1)
		if serrors.ReasonOf(err) != "placement_stale" {
			t.Fatal("stale epoch was accepted:", err)
		}
	}
	puller := NewFollower(followers[4].docStore, followers[4].chunkDoc, followers[4].versionDoc, chunkstore.NewMockChunkStore(), nil,
		WithFollowerStoragePlacement(resolve, 5),
	)
	if err := puller.PullVersionData(ctx, "unused", "A", 1); !errors.Is(err, serrors.ErrNotKBReplica) {
		t.Fatal("nonmember attempted background pull:", err)
	}
}

type placedChainTails struct {
	*fixedChainTails
	directory storage.Directory
}

func (s *placedChainTails) StorageDirectory(context.Context) (storage.Directory, error) {
	return s.directory.Clone(), nil
}
func (s *placedChainTails) GetKBPlacement(_ context.Context, kb string) (storage.Snapshot, error) {
	return s.directory.Resolve(kb)
}
func (s *placedChainTails) StorageDirectoryRevision() uint64 { return s.directory.Revision }

func TestPlacementChainTailsFilteredAndHeartbeatConstantSize(t *testing.T) {
	d := storage.NewDirectory()
	var nodes []storage.Node
	for id := int64(1); id <= 5; id++ {
		nodes = append(nodes, storage.Node{ID: id, Addr: fmt.Sprintf("s%d", id)})
	}
	if err := d.RegisterNodes(0, nodes); err != nil {
		t.Fatal(err)
	}
	for kb, ids := range map[string][]int64{"A": {1, 2, 3}, "B": {2, 4, 5}} {
		if err := d.AssignNew(storage.Placement{KBID: kb, Epoch: 1, ReplicaIDs: ids, RequiredReplicas: 3, OperationID: kb}); err != nil {
			t.Fatal(err)
		}
	}
	s := &placedChainTails{fixedChainTails: &fixedChainTails{term: 1, seq: 2, tails: map[string]int64{"A": 7, "B": 8}}, directory: d}
	h := NewPushHandler(nil, 9, WithChainTails(s), WithStoragePlacement(s.GetKBPlacement), WithHeartbeatRecorder(&recordingRecorder{}, func() bool { return true }))
	resp, err := h.GetChainTails(context.Background(), &pb.GetChainTailsRequest{NodeId: 1})
	if err != nil || !reflect.DeepEqual(resp.GetChainTails(), map[string]int64{"A": 7}) {
		t.Fatalf("%v %v", resp, err)
	}
	before := s.tailsCalls
	beat, err := h.Heartbeat(context.Background(), &pb.HeartbeatRequest{NodeId: 1})
	if err != nil || beat.GetDirectoryRevision() != d.Revision || s.tailsCalls != before {
		t.Fatal("heartbeat scanned KBs instead of reading revision:", beat, err)
	}
}

func TestPlacementRevisionRefreshesTailsWithoutChainChange(t *testing.T) {
	d := storage.NewDirectory()
	if err := d.RegisterNodes(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 3, Addr: "s3"}}); err != nil {
		t.Fatal(err)
	}
	s := &placedChainTails{fixedChainTails: &fixedChainTails{term: 1, seq: 7, tails: map[string]int64{"A": 5}}, directory: d}
	_, _, addr := startPushServer(t, 9, WithStoragePlacement(s.GetKBPlacement), WithChainTails(s), WithHeartbeatRecorder(&recordingRecorder{}, func() bool { return true }))
	sink := &recordingChainTails{}
	r := chainTailReporter(addr, sink)
	defer r.closeConn()
	if err := r.ReportOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.lastOne) != 0 {
		t.Fatal("unassigned node received tails:", sink.lastOne)
	}
	if err := s.directory.AssignNew(storage.Placement{KBID: "A", Epoch: 1, ReplicaIDs: []int64{3}, RequiredReplicas: 1, OperationID: "new"}); err != nil {
		t.Fatal(err)
	}
	if err := r.ReportOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.tailsCalls != 2 || sink.lastOne["A"] != 5 {
		t.Fatal("new assignment was missed with unchanged chain generation:", sink.lastOne, s.tailsCalls)
	}
}

func TestPlacementPushKeepsTargetVersionForInheritedMVCCDocuments(t *testing.T) {
	ctx := context.Background()
	source, _, _ := startLeaderServer(t, newFakeVecstore())
	if err := source.docStore.Write(ctx, "kb", "old", 1, []byte("inherited")); err != nil {
		t.Fatal(err)
	}
	if err := source.docStore.Write(ctx, "kb", "new", 2, []byte("added")); err != nil {
		t.Fatal(err)
	}
	for _, doc := range []string{"old", "new"} {
		if err := source.versionDoc.Write(ctx, "kb", 2, doc); err != nil {
			t.Fatal(err)
		}
	}
	f, _, addr := startPushServer(t, 2)
	ack, err := NewPusher(PusherConfig{Exporter: source.handler, NodeID: 1}).PushVersion(ctx, addr, "kb", 2)
	if err != nil || ack.GetVersionId() != 2 {
		t.Fatalf("inherited MVCC row changed transfer identity: %v %v", ack, err)
	}
	docs, err := f.versionDoc.ListDocIDs(ctx, "kb", 2)
	if err != nil || len(docs) != 2 {
		t.Fatal("incomplete inherited document set:", docs, err)
	}
}

func TestPlacementEmptyPushStillCarriesTargetIdentity(t *testing.T) {
	source, _, _ := startLeaderServer(t, newFakeVecstore())
	_, _, addr := startPushServer(t, 2)
	ack, err := NewPusher(PusherConfig{Exporter: source.handler, NodeID: 1}).PushVersion(context.Background(), addr, "empty", 7)
	if err != nil || ack.GetKnowledgeBaseId() != "empty" || ack.GetVersionId() != 7 {
		t.Fatalf("empty version lost operation identity: %v %v", ack, err)
	}
}

type failReceiptFlush struct{ chunkstore.ChunkStore }

func (failReceiptFlush) FlushWAL(context.Context) error { return errors.New("fsync failed") }

func TestPlacementPushDoesNotAcknowledgeFailedDurabilityBarrier(t *testing.T) {
	source, _, _ := startLeaderServer(t, newFakeVecstore())
	f, _, addr := startPushServer(t, 2)
	f.chunkStore = failReceiptFlush{f.chunkStore}
	if _, err := NewPusher(PusherConfig{Exporter: source.handler, NodeID: 1}).PushVersion(context.Background(), addr, "empty", 7); err == nil {
		t.Fatal("receiver acknowledged before the durability barrier")
	}
}
