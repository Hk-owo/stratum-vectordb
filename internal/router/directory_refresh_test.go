package router

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "stratum/api/proto/stratum"
)

// dirFakeControl is a control node's InternalService as far as the directory
// refresh is concerned: it either answers with a directory or refuses like a
// non-leader does.
type dirFakeControl struct {
	pb.UnimplementedInternalServiceServer
	leader bool
	calls  atomic.Int32
}

func (f *dirFakeControl) GetStorageDirectory(context.Context, *pb.GetStorageDirectoryRequest) (*pb.GetStorageDirectoryResponse, error) {
	f.calls.Add(1)
	if !f.leader {
		// What a follower answers (service/StorageAdmin + the leader-only directory read):
		// "no leader available" is the shape the router sees.
		return nil, status.Error(codes.Unavailable, "router: no leader available")
	}
	return &pb.GetStorageDirectoryResponse{Revision: 7}, nil
}

// startDirPeers brings up n control peers on one bufconn listener each and wires
// them into a Router the way NewRouter does (directories in address order).
func startDirPeers(t *testing.T, leaders ...bool) (*Router, []*dirFakeControl, func()) {
	t.Helper()
	r := &Router{requireDirectory: true}
	controls := make([]*dirFakeControl, 0, len(leaders))
	closers := make([]func(), 0, len(leaders))
	for _, leader := range leaders {
		ctl := &dirFakeControl{leader: leader}
		lis := bufconn.Listen(1 << 20)
		srv := grpc.NewServer()
		pb.RegisterInternalServiceServer(srv, ctl)
		go func() { _ = srv.Serve(lis) }()

		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		r.directories = append(r.directories, pb.NewInternalServiceClient(conn))
		controls = append(controls, ctl)
		closers = append(closers, func() { _ = conn.Close(); srv.Stop() })
	}
	cleanup := func() {
		for _, c := range closers {
			c()
		}
	}
	return r, controls, cleanup
}

// TestDirectoryRefreshRemembersTheControlNodeThatAnswered is the regression for a
// measured waste: with the leader at index 1 of 3, every refresh used to ask the
// (non-leader) first node, fail, and then succeed on the second — one guaranteed
// useless round trip per refresh, forever.
//
// Measured on the 5-storage-node cluster during a performance run: 1,414
// `Unavailable/GetStorageDirectory` calls against 1,380 successful ones, all on the
// node at the head of the list.
func TestDirectoryRefreshRemembersTheControlNodeThatAnswered(t *testing.T) {
	r, controls, cleanup := startDirPeers(t, false, true, false)
	defer cleanup()

	if _, err := r.refreshRouteSnapshot(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	// The first refresh has to discover the leader, so two probes is correct.
	if got := controls[0].calls.Load(); got != 1 {
		t.Errorf("non-leader was asked %d time(s) on the first refresh, want 1 (discovery)", got)
	}
	if got := controls[1].calls.Load(); got != 1 {
		t.Errorf("leader was asked %d time(s) on the first refresh, want 1", got)
	}

	// Every later refresh goes straight to the node that answered.
	for i := 0; i < 5; i++ {
		if _, err := r.refreshRouteSnapshot(context.Background()); err != nil {
			t.Fatalf("refresh %d: %v", i+2, err)
		}
	}
	if got := controls[0].calls.Load(); got != 1 {
		t.Errorf("non-leader was asked %d time(s) after the first refresh, want 1 — the refresh is still walking the list from the top", got)
	}
	if got := controls[1].calls.Load(); got != 6 {
		t.Errorf("leader was asked %d time(s), want 6 (once per refresh)", got)
	}
}

// TestDirectoryRefreshFollowsLeadershipChange: the remembered index must not pin
// the router to a node that stopped leading — one failed probe, then it moves.
func TestDirectoryRefreshFollowsLeadershipChange(t *testing.T) {
	r, controls, cleanup := startDirPeers(t, false, true, false)
	defer cleanup()

	if _, err := r.refreshRouteSnapshot(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	// Leadership moves to control3.
	controls[1].leader = false
	controls[2].leader = true
	if _, err := r.refreshRouteSnapshot(context.Background()); err != nil {
		t.Fatalf("refresh after the leadership change: %v", err)
	}
	if got := controls[2].calls.Load(); got != 1 {
		t.Errorf("new leader was asked %d time(s), want 1", got)
	}
	before := controls[1].calls.Load()
	for i := 0; i < 3; i++ {
		if _, err := r.refreshRouteSnapshot(context.Background()); err != nil {
			t.Fatalf("refresh %d after the change: %v", i, err)
		}
	}
	if got := controls[1].calls.Load(); got != before {
		t.Errorf("the deposed leader was asked %d more time(s) after the new leader answered, want 0", got-before)
	}
}

// TestDirOrder covers the arithmetic that the two tests above exercise only
// through a live connection: wrap-around, and a remembered index that no longer
// exists (a configuration with fewer control nodes).
func TestDirOrder(t *testing.T) {
	cases := []struct {
		name      string
		preferred int
		n         int
		want      []int
	}{
		{"zero nodes", 0, 0, nil},
		{"single node", 0, 1, []int{0}},
		{"from the head", 0, 3, []int{0, 1, 2}},
		{"from the middle", 1, 3, []int{1, 2, 0}},
		{"from the tail", 2, 3, []int{2, 0, 1}},
		{"stale index beyond the list", 9, 3, []int{0, 1, 2}},
		{"negative index", -1, 3, []int{0, 1, 2}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := dirOrder(c.preferred, c.n)
			if len(got) != len(c.want) {
				t.Fatalf("dirOrder(%d, %d) = %v, want %v", c.preferred, c.n, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("dirOrder(%d, %d) = %v, want %v", c.preferred, c.n, got, c.want)
				}
			}
			// Every node appears exactly once, whatever the starting point.
			seen := make(map[int]bool, len(got))
			for _, idx := range got {
				if seen[idx] {
					t.Fatalf("dirOrder(%d, %d) repeats %d: %v", c.preferred, c.n, idx, got)
				}
				seen[idx] = true
			}
		})
	}
}

// TestDirectoryRefreshLearnsAcrossConcurrentRefreshes guards the atomic: two
// goroutines refreshing at once must not produce a wrong index or a race report.
func TestDirectoryRefreshLearnsAcrossConcurrentRefreshes(t *testing.T) {
	r, _, cleanup := startDirPeers(t, false, true, false)
	defer cleanup()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.refreshRouteSnapshot(context.Background()); err != nil {
				t.Errorf("concurrent refresh: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := r.dirPreferred.Load(); got != 1 {
		t.Errorf("dirPreferred = %d, want 1 (the node that answered)", got)
	}
}
