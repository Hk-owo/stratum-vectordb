package router

import (
	"context"
	"errors"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

func notLeaderErr() error {
	return status.Error(codes.Internal, "kvraft: not leader")
}

func unavailableErr() error {
	return status.Error(codes.Unavailable, "connection unavailable")
}

// fakeResolver is a scriptable leaderResolver: each LeaderNow call pops
// the next index from order (the last one repeats).
type fakeResolver struct {
	mu       sync.Mutex
	order    []int
	ok       bool
	nowCalls int
}

// LeaderNow pops the next scripted leader index. nowCalls counts
// invocations so tests can assert that writes never reuse a cached
// leader.
func (f *fakeResolver) LeaderNow(context.Context) (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nowCalls++
	if len(f.order) == 0 {
		return 0, f.ok
	}
	idx := f.order[0]
	if len(f.order) > 1 {
		f.order = f.order[1:]
	}
	return idx, f.ok
}

func TestForwardWrite_LeaderSuccess(t *testing.T) {
	r := &Router{
		addrs:      []string{"a", "b", "c"},
		discoverer: &fakeResolver{order: []int{1}, ok: true},
	}
	got, err := forwardWrite(r, context.Background(), func(idx int, ctx context.Context) (string, error) {
		if idx != 1 {
			t.Errorf("fn called with idx %d, want 1 (leader)", idx)
		}
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("forwardWrite: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %q, want ok", got)
	}
}

// TestForwardWrite_AlwaysRediscover pins the fix for stale-leader writes:
// every write must re-poll the cluster (LeaderNow) instead of serving the
// TTL cache, so a leader change can never leave writes pinned to an old
// leader for up to a second.
func TestForwardWrite_AlwaysRediscover(t *testing.T) {
	fr := &fakeResolver{order: []int{0, 1}, ok: true}
	r := &Router{addrs: []string{"a", "b", "c"}, discoverer: fr}

	// Two back-to-back writes. With TTL caching the second would hit the
	// cached leader without re-polling; with LeaderNow it must re-poll.
	for i := 0; i < 2; i++ {
		if _, err := forwardWrite(r, context.Background(), func(idx int, ctx context.Context) (string, error) {
			return "ok", nil
		}); err != nil {
			t.Fatalf("write %d: forwardWrite: %v", i, err)
		}
	}
	if fr.nowCalls != 2 {
		t.Errorf("LeaderNow calls = %d, want 2 (every write re-discovers)", fr.nowCalls)
	}
}

func TestForwardWrite_NotLeaderRediscover(t *testing.T) {
	fr := &fakeResolver{order: []int{1, 2}, ok: true}
	r := &Router{addrs: []string{"a", "b", "c"}, discoverer: fr}

	got, err := forwardWrite(r, context.Background(), func(idx int, ctx context.Context) (string, error) {
		if idx == 1 {
			return "", notLeaderErr() // stale leader: redirect
		}
		if idx == 2 {
			return "ok", nil
		}
		t.Errorf("fn called with unexpected idx %d", idx)
		return "", errors.New("unexpected")
	})
	if err != nil {
		t.Fatalf("forwardWrite: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %q, want ok", got)
	}
	if fr.nowCalls != 2 {
		t.Errorf("LeaderNow calls = %d, want 2 (retry re-discovers)", fr.nowCalls)
	}
}

func TestForwardWrite_UnavailableRediscover(t *testing.T) {
	fr := &fakeResolver{order: []int{2, 1}, ok: true}
	r := &Router{addrs: []string{"a", "b", "c"}, discoverer: fr}

	got, err := forwardWrite(r, context.Background(), func(idx int, ctx context.Context) (string, error) {
		if idx == 2 {
			return "", unavailableErr() // stale leader went down
		}
		if idx == 1 {
			return "ok", nil // re-discovered leader accepts
		}
		t.Errorf("fn called with unexpected idx %d", idx)
		return "", errors.New("unexpected")
	})
	if err != nil {
		t.Fatalf("forwardWrite: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %q, want ok", got)
	}
	if fr.nowCalls != 2 {
		t.Errorf("LeaderNow calls = %d, want 2 (retry re-discovers)", fr.nowCalls)
	}
}

func TestForwardWrite_NoLeaderTryAll(t *testing.T) {
	r := &Router{
		addrs:      []string{"a", "b", "c"},
		discoverer: &fakeResolver{ok: false}, // no leader known
	}
	got, err := forwardWrite(r, context.Background(), func(idx int, ctx context.Context) (string, error) {
		if idx == 1 {
			return "ok", nil // node 1 happens to accept the write
		}
		return "", notLeaderErr()
	})
	if err != nil {
		t.Fatalf("forwardWrite: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %q, want ok", got)
	}
}

func TestForwardWrite_NoLeaderAllFail(t *testing.T) {
	r := &Router{
		addrs:      []string{"a", "b", "c"},
		discoverer: &fakeResolver{ok: false},
	}
	_, err := forwardWrite(r, context.Background(), func(idx int, ctx context.Context) (string, error) {
		return "", notLeaderErr()
	})
	if err == nil || err.Error() != "router: no leader available" {
		t.Errorf("err = %v, want 'router: no leader available'", err)
	}
}

func TestForwardWrite_NonRetryable(t *testing.T) {
	fr := &fakeResolver{order: []int{1}, ok: true}
	r := &Router{addrs: []string{"a", "b", "c"}, discoverer: fr}

	want := status.Error(codes.InvalidArgument, "bad request")
	_, err := forwardWrite(r, context.Background(), func(idx int, ctx context.Context) (string, error) {
		return "", want
	})
	if err != want {
		t.Errorf("err = %v, want the original non-retryable error", err)
	}
	if fr.nowCalls != 1 {
		t.Errorf("LeaderNow calls = %d, want 1 (no retry)", fr.nowCalls)
	}
}

func TestForwardRead_RoundRobin(t *testing.T) {
	r := &Router{addrs: []string{"a", "b", "c"}}
	var hits [3]int
	for i := 0; i < 6; i++ {
		_, err := forwardRead(r, context.Background(), func(idx int, ctx context.Context) (string, error) {
			hits[idx]++
			return "ok", nil
		})
		if err != nil {
			t.Fatalf("forwardRead: %v", err)
		}
	}
	if hits != [3]int{2, 2, 2} {
		t.Errorf("hits = %v, want even distribution [2 2 2]", hits)
	}
}

func TestForwardRead_Failover(t *testing.T) {
	r := &Router{addrs: []string{"a", "b", "c"}}
	got, err := forwardRead(r, context.Background(), func(idx int, ctx context.Context) (string, error) {
		if idx == 0 {
			return "", unavailableErr()
		}
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("forwardRead: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %q, want ok", got)
	}
}

func TestForwardRead_AllDown(t *testing.T) {
	r := &Router{addrs: []string{"a", "b", "c"}}
	_, err := forwardRead(r, context.Background(), func(idx int, ctx context.Context) (string, error) {
		return "", unavailableErr()
	})
	if err == nil || err.Error() != "router: all nodes unavailable" {
		t.Errorf("err = %v, want 'router: all nodes unavailable'", err)
	}
}

func TestForwardRead_NonRetryable(t *testing.T) {
	r := &Router{addrs: []string{"a", "b", "c"}}
	want := status.Error(codes.NotFound, "not found")
	_, err := forwardRead(r, context.Background(), func(idx int, ctx context.Context) (string, error) {
		return "", want
	})
	if err != want {
		t.Errorf("err = %v, want the original non-retryable error", err)
	}
}

func TestForward_SwitchesByMethod(t *testing.T) {
	// Write methods route via the resolver; read methods round-robin.
	fr := &fakeResolver{order: []int{2}, ok: true}
	r := &Router{addrs: []string{"a", "b", "c"}, discoverer: fr}
	ctx := context.Background()

	writeIdx := -1
	if _, err := Forward(r, ctx, pb.KnowledgeBaseService_CreateVersion_FullMethodName, func(idx int, ctx context.Context) (string, error) {
		writeIdx = idx
		return "ok", nil
	}); err != nil {
		t.Fatalf("Forward(write): %v", err)
	}
	if writeIdx != 2 {
		t.Errorf("write routed to idx %d, want 2 (leader)", writeIdx)
	}

	readIdx := -1
	if _, err := Forward(r, ctx, pb.QueryService_Query_FullMethodName, func(idx int, ctx context.Context) (string, error) {
		readIdx = idx
		return "ok", nil
	}); err != nil {
		t.Fatalf("Forward(read): %v", err)
	}
	if readIdx != 0 {
		t.Errorf("read routed to idx %d, want 0 (round-robin start)", readIdx)
	}
}
