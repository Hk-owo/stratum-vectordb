package router

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/owner"
	"stratum/internal/storage"
)

func testRouteDirectory(t *testing.T) storage.Directory {
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
	d.ActiveVersions = map[string]int64{"A": 9, "B": 10}
	return d
}

func directoryRouter(t *testing.T, refresh func(context.Context) (routeSnapshot, error)) *Router {
	t.Helper()
	r := &Router{requireDirectory: true, storageAddrs: []string{"s1", "s2", "s3", "s4", "s5"}, ownerInactiveK: 1}
	r.routes = NewRouteTable(refresh, time.Minute, nil)
	r.routes.Refresh(context.Background())
	return r
}

func TestDirectoryRoutesOnlyOverlappingKBCandidates(t *testing.T) {
	d := testRouteDirectory(t)
	r := directoryRouter(t, func(context.Context) (routeSnapshot, error) {
		return routeSnapshot{directory: &d, expected: d.ActiveVersions}, nil
	})
	for _, kb := range []string{"A", "B"} {
		var tried []int
		_, _ = Forward(r, context.Background(), pb.QueryService_Query_FullMethodName, kb, func(idx int, ctx context.Context) (int, error) {
			snap, ok := storage.Bound(ctx, kb)
			if !ok || snap.ActiveVersion != d.ActiveVersions[kb] {
				t.Fatal("inconsistent route snapshot")
			}
			tried = append(tried, idx)
			return 0, serrors.ToGRPCStatus(serrors.ErrIndexNotReady)
		})
		sort.Ints(tried)
		want := map[string][]int{"A": {0, 1, 2}, "B": {1, 3, 4}}[kb]
		if !reflect.DeepEqual(tried, want) {
			t.Fatalf("%s candidates=%v want=%v", kb, tried, want)
		}
	}
}

func TestDirectoryUnknownNeverScansStorageAndDeletionStaysDistinct(t *testing.T) {
	d := testRouteDirectory(t)
	d.Delete("A")
	r := directoryRouter(t, func(context.Context) (routeSnapshot, error) { return routeSnapshot{directory: &d}, nil })
	for kb, reason := range map[string]string{"absent": "placement_unknown", "A": "knowledge_base_deleted"} {
		_, err := Forward(r, context.Background(), pb.QueryService_Query_FullMethodName, kb, func(int, context.Context) (int, error) { t.Fatal("unknown/deleted KB scanned storage"); return 0, nil })
		if serrors.ReasonOf(err) != reason {
			t.Fatalf("%s: %v", kb, err)
		}
	}
}

func TestDirectoryRefreshAfterStaleResponseIsBounded(t *testing.T) {
	d := testRouteDirectory(t)
	var refreshes atomic.Int32
	r := directoryRouter(t, func(context.Context) (routeSnapshot, error) {
		dir := d.Clone()
		if refreshes.Add(1) > 1 {
			p := dir.Placements["A"]
			p.Epoch = 2
			dir.Placements["A"] = p
			dir.ActiveVersions["A"] = 11
		}
		return routeSnapshot{directory: &dir, expected: dir.ActiveVersions}, nil
	})
	var calls int
	got, err := Forward(r, context.Background(), pb.QueryService_Query_FullMethodName, "A", func(_ int, ctx context.Context) (int, error) {
		calls++
		snap, _ := storage.Bound(ctx, "A")
		if snap.Placement.Epoch == 1 {
			return 0, serrors.ToGRPCStatus(serrors.ErrPlacementStale)
		}
		if snap.ActiveVersion != 11 {
			t.Fatal("refreshed placement mixed with previous active version")
		}
		return 7, nil
	})
	if err != nil || got != 7 || calls != 2 || refreshes.Load() != 2 {
		t.Fatalf("got=%d err=%v calls=%d refreshes=%d", got, err, calls, refreshes.Load())
	}
}

func TestDirectoryRefreshCoalescesConcurrentMisses(t *testing.T) {
	var calls atomic.Int32
	table := NewRouteTable(func(context.Context) (routeSnapshot, error) { calls.Add(1); return routeSnapshot{}, nil }, time.Minute, nil)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); table.RefreshAfter(context.Background(), 0) }()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("refresh storm:", calls.Load())
	}
}

func TestDirectoryMemberReorderKeepsConnectionIdentity(t *testing.T) {
	d := testRouteDirectory(t)
	s, err := d.Resolve("A")
	if err != nil {
		t.Fatal(err)
	}
	s.Nodes = []storage.Node{s.Nodes[2], s.Nodes[0], s.Nodes[1]}
	r := &Router{storageAddrs: []string{"s1", "s2", "s3", "s4", "s5"}}
	tiers, err := r.directoryTiers(s, 9, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var flat []int
	for _, tier := range tiers {
		flat = append(flat, tier...)
	}
	sort.Ints(flat)
	if !reflect.DeepEqual(flat, []int{0, 1, 2}) {
		t.Fatal("directory order changed connection identity:", flat)
	}
}

func TestDirectoryHealthGatesOnlyItsOwnKnowledgeBase(t *testing.T) {
	d := testRouteDirectory(t)
	r := directoryRouter(t, func(context.Context) (routeSnapshot, error) {
		return routeSnapshot{directory: &d, health: map[string]storage.Health{
			"A": {Known: true}, "B": {Known: true, Degraded: true, Detail: "one of three replicas live"},
		}}, nil
	})
	if err := r.writeGate("A"); err != nil {
		t.Fatal("healthy KB blocked by unrelated replicas:", err)
	}
	if err := r.writeGate("B"); serrors.ReasonOf(err) != "kb_storage_degraded" {
		t.Fatal("KB-scoped health missing:", err)
	}
	if err := r.writeGate("unknown"); err != nil {
		t.Fatal("unknown soft state must allow the actual write to decide:", err)
	}
}

func TestDirectoryPinnedVersionUsesItsOwnOwnerWithinKB(t *testing.T) {
	d := testRouteDirectory(t)
	r := directoryRouter(t, func(context.Context) (routeSnapshot, error) { return routeSnapshot{directory: &d}, nil })
	want := owner.Owners("A", 3, []string{"s1", "s2", "s3"}, 1)[0]
	got, err := Forward(r, context.Background(), pb.QueryService_Query_FullMethodName, "A", func(i int, _ context.Context) (string, error) { return r.storageAddrs[i], nil }, WithPinnedVersion(3))
	if err != nil || got != want {
		t.Fatalf("pinned version owner=%s want=%s err=%v", got, want, err)
	}
}
