package plane

import (
	"sync/atomic"
	"testing"
)

// Takeover must fire exactly on the follower→leader edge: it is what clears
// leadership-scoped soft state, so firing late means serving a predecessor's
// view, and firing repeatedly means discarding live reports.
func TestLeaderGate_TakeoverFiresOnTheTransitionOnly(t *testing.T) {
	var leader atomic.Bool
	var takeovers atomic.Int32
	gate := NewLeaderGate(func() bool { return leader.Load() }, func() { takeovers.Add(1) })

	// Still a follower: no takeover, however often it is asked.
	for i := 0; i < 3; i++ {
		if gate.IsLeader() {
			t.Fatal("a follower reported itself as leader")
		}
	}
	if got := takeovers.Load(); got != 0 {
		t.Fatalf("takeovers while a follower = %d, want 0", got)
	}

	leader.Store(true)
	if !gate.IsLeader() {
		t.Fatal("a leader reported itself as a follower")
	}
	if got := takeovers.Load(); got != 1 {
		t.Fatalf("takeovers on taking leadership = %d, want 1", got)
	}

	// Staying leader must not keep clearing: the reports arriving now are live.
	for i := 0; i < 3; i++ {
		gate.IsLeader()
	}
	if got := takeovers.Load(); got != 1 {
		t.Fatalf("takeovers while leading = %d, want 1 (clearing live reports would keep the aggregate empty)", got)
	}

	// Losing and regaining leadership is a NEW term: clear again.
	leader.Store(false)
	gate.IsLeader()
	leader.Store(true)
	gate.IsLeader()
	if got := takeovers.Load(); got != 2 {
		t.Fatalf("takeovers after a new term = %d, want 2", got)
	}
}

// A nil check means "never the leader" — the single-node and test default — and
// the hook must simply not fire.
func TestLeaderGate_NilCheckIsNeverLeader(t *testing.T) {
	var takeovers atomic.Int32
	gate := NewLeaderGate(nil, func() { takeovers.Add(1) })
	if gate.IsLeader() {
		t.Error("a gate with no check must never claim leadership")
	}
	if got := takeovers.Load(); got != 0 {
		t.Errorf("takeovers = %d, want 0", got)
	}
}

// A nil hook is legal: a caller may want the derived answer without the clearing.
func TestLeaderGate_NilHookIsFine(t *testing.T) {
	gate := NewLeaderGate(func() bool { return true }, nil)
	if !gate.IsLeader() {
		t.Error("IsLeader must still answer with no hook wired")
	}
}

// The gate is read from concurrent callers (report handling and query paths), so
// the transition must be observed exactly once even under contention.
func TestLeaderGate_ConcurrentReadsFireTakeoverOnce(t *testing.T) {
	var leader atomic.Bool
	var takeovers atomic.Int32
	gate := NewLeaderGate(func() bool { return leader.Load() }, func() { takeovers.Add(1) })

	leader.Store(true)
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 50; j++ {
				gate.IsLeader()
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if got := takeovers.Load(); got != 1 {
		t.Errorf("takeovers under concurrency = %d, want exactly 1", got)
	}
}
