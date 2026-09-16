package router

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

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

// blockingStatusClient answers only when its own probe context ends: it is what
// a black-holed node looks like from the station — the TCP connection is
// accepted, but no reply ever comes.
type blockingStatusClient struct{ calls atomic.Int32 }

func (b *blockingStatusClient) GetClusterStatus(ctx context.Context, _ *pb.GetClusterStatusRequest, _ ...grpc.CallOption) (*pb.GetClusterStatusResponse, error) {
	b.calls.Add(1)
	<-ctx.Done()
	return nil, status.Error(codes.DeadlineExceeded, "no answer")
}

// An unresponsive node must cost its own probe budget and no more: each probe
// is bounded independently, so discovery still converges while the caller's
// deadline is far from over.
//
// The failure this pins: with every probe sharing the caller's context, one
// dead node held discovery until that deadline expired. Every leader-bound
// write begins with LeaderNow, so a single dead node made the cluster look
// unable to accept writes — which is how T4's minority-fault test failed
// (kill a follower, then "timed out waiting for a leader to accept writes").
func TestLeaderDiscoverer_UnresponsiveNodeDoesNotPinDiscovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	slow := &blockingStatusClient{}
	// idx 1 and idx 2 both report node 2 as the leader, so the majority vote
	// resolves to node 2 — which is admins[1].
	d := NewLeaderDiscoverer([]statusClient{
		slow,
		&fakeStatusClient{resp: statusResp(2, 2, true)},
		&fakeStatusClient{resp: statusResp(3, 2, true)},
	})

	start := time.Now()
	idx, ok := d.LeaderNow(ctx)
	elapsed := time.Since(start)

	if !ok || idx != 1 {
		t.Fatalf("LeaderNow() = (%d, %v), want (1, true)", idx, ok)
	}
	if slow.calls.Load() == 0 {
		t.Fatal("the unresponsive node was never probed")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("LeaderNow took %v: an unresponsive node must not pin discovery", elapsed)
	}
}

// A caller with less budget than one probe is never granted more than it has:
// the probe bound is a ceiling, not a floor that outlives the caller.
func TestLeaderDiscoverer_ProbeRespectsTighterCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	d := NewLeaderDiscoverer([]statusClient{&blockingStatusClient{}})

	start := time.Now()
	if _, ok := d.LeaderNow(ctx); ok {
		t.Fatal("no node reported a leader; ok must be false")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("LeaderNow took %v, want it bounded by the caller's 150ms deadline", elapsed)
	}
}
