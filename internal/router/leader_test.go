package router

import (
	"context"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

// fakeStatusClient returns a canned GetClusterStatus response and counts
// invocations.
type fakeStatusClient struct {
	resp  *pb.GetClusterStatusResponse
	err   error
	calls atomic.Int32
}

func (f *fakeStatusClient) GetClusterStatus(context.Context, *pb.GetClusterStatusRequest, ...grpc.CallOption) (*pb.GetClusterStatusResponse, error) {
	f.calls.Add(1)
	return f.resp, f.err
}

func statusResp(nodeID, leaderID int64, hasLeader bool) *pb.GetClusterStatusResponse {
	return &pb.GetClusterStatusResponse{
		NodeId:    nodeID,
		HasLeader: hasLeader,
		LeaderId:  leaderID,
	}
}

func TestLeaderDiscoverer_SingleLeader(t *testing.T) {
	c := &fakeStatusClient{resp: statusResp(7, 7, true)}
	d := NewLeaderDiscoverer([]statusClient{c})

	idx, ok := d.LeaderNow(context.Background())
	if !ok {
		t.Fatal("Leader() ok = false, want true")
	}
	if idx != 0 {
		t.Errorf("Leader() idx = %d, want 0", idx)
	}
}

func TestLeaderDiscoverer_MajorityVote(t *testing.T) {
	// node 1 (idx 0) is reported as leader by two nodes, node 3 (idx 2)
	// by one: majority wins.
	admins := []statusClient{
		&fakeStatusClient{resp: statusResp(1, 1, true)},
		&fakeStatusClient{resp: statusResp(2, 1, true)},
		&fakeStatusClient{resp: statusResp(3, 3, true)},
	}
	d := NewLeaderDiscoverer(admins)

	idx, ok := d.LeaderNow(context.Background())
	if !ok {
		t.Fatal("Leader() ok = false, want true")
	}
	if idx != 0 {
		t.Errorf("Leader() idx = %d, want 0 (node 1)", idx)
	}
}

func TestLeaderDiscoverer_NoLeader(t *testing.T) {
	admins := []statusClient{
		&fakeStatusClient{resp: statusResp(1, 0, false)},
		&fakeStatusClient{resp: statusResp(2, 0, false)},
	}
	d := NewLeaderDiscoverer(admins)

	if _, ok := d.LeaderNow(context.Background()); ok {
		t.Error("Leader() ok = true, want false (no leader)")
	}
}

func TestLeaderDiscoverer_UnreachableNode(t *testing.T) {
	// Follower idx 1 is down; the leader and another follower still agree
	// on leader=1 (idx 0).
	admins := []statusClient{
		&fakeStatusClient{resp: statusResp(1, 1, true)},
		&fakeStatusClient{err: status.Error(codes.Unavailable, "down")},
		&fakeStatusClient{resp: statusResp(3, 1, true)},
	}
	d := NewLeaderDiscoverer(admins)

	idx, ok := d.LeaderNow(context.Background())
	if !ok {
		t.Fatal("Leader() ok = false, want true")
	}
	if idx != 0 {
		t.Errorf("Leader() idx = %d, want 0 (node 1)", idx)
	}
}

func TestLeaderDiscoverer_LeaderUnreachable(t *testing.T) {
	// The leader itself is down: no reachable node reports itself as
	// leader, and the leader ID is not resolvable to an index.
	admins := []statusClient{
		&fakeStatusClient{err: status.Error(codes.Unavailable, "down")},
		&fakeStatusClient{resp: statusResp(2, 1, true)},
		&fakeStatusClient{resp: statusResp(3, 1, true)},
	}
	d := NewLeaderDiscoverer(admins)

	if _, ok := d.LeaderNow(context.Background()); ok {
		t.Error("Leader() ok = true, want false (leader unreachable)")
	}
}

// TestLeaderDiscoverer_NoCache pins the no-caching contract: every
// LeaderNow call re-polls the cluster, so the returned leader can never
// be stale.
func TestLeaderDiscoverer_NoCache(t *testing.T) {
	c := &fakeStatusClient{resp: statusResp(7, 7, true)}
	d := NewLeaderDiscoverer([]statusClient{c})

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, ok := d.LeaderNow(ctx); !ok {
			t.Fatalf("LeaderNow #%d failed", i)
		}
	}
	if got := c.calls.Load(); got != 3 {
		t.Errorf("GetClusterStatus calls = %d, want 3 (every call re-polls)", got)
	}
}
