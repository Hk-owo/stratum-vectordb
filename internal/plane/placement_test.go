package plane

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/raft"
	"stratum/internal/storage"
	stratumsync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/wal"
)

func overlapDirectory(t *testing.T) storage.Directory {
	t.Helper()
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
	return d
}

func resolverOf(d storage.Directory) storage.Resolver {
	return func(_ context.Context, kb string) (storage.Snapshot, error) { return d.Resolve(kb) }
}

type placedPush struct{ kb, addr, epoch string }
type placementPusher struct {
	calls chan placedPush
	fail  bool
}

func (p *placementPusher) PushVersion(ctx context.Context, addr, kb string, _ int64) (int64, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	var epoch string
	if v := md.Get("x-stratum-placement-epoch"); len(v) > 0 {
		epoch = v[0]
	}
	p.calls <- placedPush{kb, addr, epoch}
	if p.fail {
		return 0, errors.New("offline")
	}
	var id int64
	_, _ = fmt.Sscanf(addr, "s%d", &id)
	return id, nil
}

func TestPlacementOverlappingKBWritesDoNotExpand(t *testing.T) {
	dir := overlapDirectory(t)
	for _, kb := range []string{"A", "B"} {
		p := &placementPusher{calls: make(chan placedPush, 5)}
		dp := newFanOutPlane([]string{"outside"}, p)
		dp.nodeID = 2
		dp.placements = resolverOf(dir)
		if err := dp.WriteVersionData(context.Background(), kb, 1, 0, nil); err != nil {
			t.Fatal(err)
		}
		var got []string
		for i := 0; i < 2; i++ {
			select {
			case c := <-p.calls:
				if c.kb != kb || c.epoch != "1" {
					t.Fatal("operation identity/epoch missing:", c)
				}
				got = append(got, c.addr)
			case <-time.After(time.Second):
				t.Fatal("replica was not contacted")
			}
		}
		sort.Strings(got)
		want := map[string][]string{"A": {"s1", "s3"}, "B": {"s4", "s5"}}[kb]
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s contacted %v want %v", kb, got, want)
		}
		select {
		case unexpected := <-p.calls:
			t.Fatal("extra replica:", unexpected)
		default:
		}
	}
}

func TestPlacementOneSnapshotPerWriteAndOfflineMembersRemainRequired(t *testing.T) {
	dir := overlapDirectory(t)
	p := &placementPusher{calls: make(chan placedPush, 5), fail: true}
	dp := newFanOutPlane(nil, p)
	dp.nodeID = 2
	var reads atomic.Int32
	dp.placements = func(ctx context.Context, kb string) (storage.Snapshot, error) { reads.Add(1); return dir.Resolve(kb) }
	if err := dp.WriteVersionData(context.Background(), "A", 1, 0, nil); err == nil {
		t.Fatal("one of three replicas acknowledged below quorum")
	}
	if reads.Load() != 1 {
		t.Fatal("write mixed placement reads:", reads.Load())
	}
	if len(p.calls) != 2 {
		t.Fatal("silent members disappeared from the topology")
	}
}

func TestPlacementNonMembersCannotWriteResumePullOrBuild(t *testing.T) {
	dir := overlapDirectory(t)
	dp := NewLocalDataPlane(LocalDataPlaneConfig{Placement: resolverOf(dir), NodeID: 4})
	ctx := context.Background()
	for _, run := range []func() error{
		func() error { return dp.WriteVersionData(ctx, "A", 1, 0, nil) },
		func() error { return dp.ResumeVersionWrite(ctx, "A", 1, 0, nil) },
		func() error { return dp.FetchVersionData(ctx, "A", 1) },
		func() error { return dp.EnsureIndex(ctx, "A", 1) },
	} {
		if err := run(); !errors.Is(err, serrors.ErrNotKBReplica) {
			t.Fatal("nonmember reached storage work:", err)
		}
	}
}

func TestPlacementReclaimUsesEachKBRequiredSet(t *testing.T) {
	dir := overlapDirectory(t)
	w := &recordingCompactorWAL{}
	q := &peerCursors{cursors: map[string]map[string]int64{"s1": {"A": 8}, "s3": {"A": 7}, "s4": {"B": 5}, "s5": {"B": 6}}}
	dp := reclaimPlane(t, w, q, nil, "A", "B")
	dp.placements = resolverOf(dir)
	dp.nodeID = 2
	used, err := dp.ReclaimChanges(context.Background())
	if err != nil || !reflect.DeepEqual(used, map[string]int64{"A": 7, "B": 5}) {
		t.Fatalf("watermarks=%v err=%v", used, err)
	}
	// Each peer is queried only for its own KB; a global scan would ask twice.
	for addr, count := range q.asked {
		if count != 1 {
			t.Fatalf("%s queried %d times across unrelated KBs", addr, count)
		}
	}
}

func TestPlacementDispatcherNeverAppendsNonmemberSelf(t *testing.T) {
	dir := overlapDirectory(t)
	d := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{Placement: resolverOf(dir), SelfAddr: "control", LocalWrite: func(context.Context, string, int64, int64, []types.DocChange) error { panic("nonmember fallback") }})
	got, err := d.candidates(context.Background(), "A")
	if err != nil || !reflect.DeepEqual(got, []string{"s1", "s2", "s3"}) {
		t.Fatalf("%v %v", got, err)
	}
}

func TestPlacementHealthIsPerKBAndDoesNotDropSilentMembers(t *testing.T) {
	dir := overlapDirectory(t)
	reg := NewLivenessRegistry()
	reg.Record(1)
	reg.Record(2)
	cp := NewLocalControlPlane(&stubMeta{}, WithKBPlacement(resolverOf(dir)), WithLivenessView(reg, NewLeaderGate(func() bool { return true }, nil)))
	a, _, knownA := cp.StorageDegradation("A")
	b, _, knownB := cp.StorageDegradation("B")
	if !knownA || !knownB || a != StorageHealthy || b != StorageDegraded {
		t.Fatalf("A=%v/%v B=%v/%v", a, knownA, b, knownB)
	}
}

func TestPlacementControlPlaneFeedsDirectoryToHeartbeatAndTails(t *testing.T) {
	ctx := context.Background()
	d := overlapDirectory(t)
	rn := raft.NewMockRaftNode(wal.NewMockWAL())
	var nodes []storage.Node
	for _, n := range d.Nodes {
		nodes = append(nodes, n)
	}
	if err := rn.ProposeRegisterStorageNodes(ctx, 0, nodes); err != nil {
		t.Fatal(err)
	}
	versions := make(map[string]int64)
	for _, kb := range []string{"A", "B"} {
		if err := rn.ProposeCreateKBWithPlacement(ctx, types.KnowledgeBaseMeta{KBID: kb}, d.Placements[kb].ReplicaIDs); err != nil {
			t.Fatal(err)
		}
		id, err := rn.ProposeCreateVersion(ctx, kb, 0)
		if err != nil {
			t.Fatal(err)
		}
		versions[kb] = id
	}
	cp := NewLocalControlPlane(rn, WithKBPlacement(rn.GetKBPlacement))
	h := stratumsync.NewPushHandler(nil, 9, stratumsync.WithStoragePlacement(cp.GetKBPlacement), stratumsync.WithChainTails(cp), stratumsync.WithHeartbeatRecorder(NewLivenessRegistry(), func() bool { return true }))
	resp, err := h.GetChainTails(ctx, &pb.GetChainTailsRequest{NodeId: 1})
	if err != nil || !reflect.DeepEqual(resp.GetChainTails(), map[string]int64{"A": versions["A"]}) {
		t.Fatalf("production control-plane boundary: %v %v", resp, err)
	}
	beat, err := h.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: 1})
	if err != nil || beat.GetDirectoryRevision() != rn.StorageDirectoryRevision() {
		t.Fatal("revision lost across control-plane boundary:", beat, err)
	}
}

type wrongNodePusher struct{}

func (wrongNodePusher) PushVersion(context.Context, string, string, int64) (int64, error) {
	return 2, nil
}

func TestPlacementWrongAcceptorCannotFormQuorum(t *testing.T) {
	dir := overlapDirectory(t)
	dp := newFanOutPlane(nil, wrongNodePusher{})
	dp.nodeID = 2
	dp.placements = resolverOf(dir)
	if err := dp.WriteVersionData(context.Background(), "A", 1, 0, nil); err == nil {
		t.Fatal("the same physical node was counted as multiple replicas")
	}
}
