package plane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// stubPusher records pushes and fails for the targets listed in fail.
//
// The mutex is not decoration: fan-out pushes run concurrently, and since
// quorum is what the caller waits for, a push past quorum settles AFTER fan-out
// returns — so the test reads this while a goroutine may still be appending.
type stubPusher struct {
	mu    sync.Mutex
	fail  map[string]bool
	calls []string
}

func (p *stubPusher) PushVersion(_ context.Context, targetAddr, _ string, _ int64) (int64, error) {
	p.mu.Lock()
	p.calls = append(p.calls, targetAddr)
	p.mu.Unlock()
	if p.fail[targetAddr] {
		return 0, errors.New("replica unreachable")
	}
	return 42, nil
}

// attempted returns the targets pushed so far, as a copy.
func (p *stubPusher) attempted() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

var _ VersionPusher = (*stubPusher)(nil)

// newFanOutPlane builds a plane whose local transaction succeeds and whose
// replica set is targets.
func newFanOutPlane(targets []string, pusher VersionPusher) *LocalDataPlane {
	tr := &tracer{}
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
		Pusher:       pusher,
		ResolveReplicas: func(context.Context) ([]string, error) {
			return targets, nil
		},
	})
}

func TestQuorumSize(t *testing.T) {
	cases := map[int]int{0: 0, 1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4, 7: 4}
	for n, want := range cases {
		if got := QuorumSize(n); got != want {
			t.Errorf("QuorumSize(%d) = %d, want %d", n, got, want)
		}
	}
}

// With two targets the quorum is 2 of 3, so a single unreachable replica must
// not fail the write (Stratum_设计文档v13.md §7.1).
func TestLocalDataPlane_FanOutToleratesOneUnreachableReplica(t *testing.T) {
	pusher := &stubPusher{fail: map[string]bool{"peer-b": true}}
	dp := newFanOutPlane([]string{"peer-a", "peer-b"}, pusher)

	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 3, nil); err != nil {
		t.Fatalf("WriteVersionData with one reachable replica: %v", err)
	}
	// Both targets must eventually be attempted — but only quorum is waited for,
	// so the push on the unreachable replica settles in the background (and, with
	// the per-replica budget, at most replicaPushTimeout later). Poll instead of
	// assuming it already happened.
	deadline := time.Now().Add(2 * time.Second)
	for len(pusher.attempted()) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := pusher.attempted(); len(got) != 2 {
		t.Errorf("pushes = %v, want both targets attempted", got)
	}
}

// Losing both targets leaves 1 of 3 acknowledgements — below quorum — so the
// write must fail rather than report the version durable.
func TestLocalDataPlane_FanOutFailsBelowQuorum(t *testing.T) {
	pusher := &stubPusher{fail: map[string]bool{"peer-a": true, "peer-b": true}}
	dp := newFanOutPlane([]string{"peer-a", "peer-b"}, pusher)

	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 3, nil); err == nil {
		t.Fatal("WriteVersionData must fail when acknowledgements fall below quorum")
	}
}

// A single-replica deployment (no resolver) has nothing to fan out to: the
// local write is the whole quorum and no push is attempted, which is what
// keeps the single-node and test defaults behaving exactly as before.
func TestLocalDataPlane_FanOutWithoutReplicasIsLocalOnly(t *testing.T) {
	pusher := &stubPusher{}
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
		Pusher:       pusher,
	})

	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 3, nil); err != nil {
		t.Fatalf("WriteVersionData without a replica set: %v", err)
	}
	if len(pusher.calls) != 0 {
		t.Errorf("pushes = %v, want none (no replica resolver configured)", pusher.calls)
	}
}

// An empty target list behaves like no replication at all.
func TestLocalDataPlane_FanOutWithEmptyTargetsIsLocalOnly(t *testing.T) {
	pusher := &stubPusher{}
	dp := newFanOutPlane(nil, pusher)

	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 3, nil); err != nil {
		t.Fatalf("WriteVersionData with an empty replica set: %v", err)
	}
	if len(pusher.calls) != 0 {
		t.Errorf("pushes = %v, want none", pusher.calls)
	}
}

// A resolver failure must surface: silently skipping replication would let the
// write report the version durable with no replica holding it.
func TestLocalDataPlane_FanOutResolverFailureSurfaces(t *testing.T) {
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
		Pusher:       &stubPusher{},
		ResolveReplicas: func(context.Context) ([]string, error) {
			return nil, errors.New("membership unavailable")
		},
	})

	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 3, nil); err == nil {
		t.Fatal("a replica-resolution failure must surface rather than degrade to no replication")
	}
}
