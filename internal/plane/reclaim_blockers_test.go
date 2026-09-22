package plane

import (
	"sync/atomic"
	"testing"
	"time"
)

// ReclaimBlockers is the diagnosis the bare "unknown" cannot carry: WHICH required replica
// is holding the watermark, and why. The three reasons have to stay distinguishable,
// because they call for different actions — a node that has never reported is a node that
// has not started (or is not in the topology it thinks it is), one that reports but not
// about THIS knowledge base is a placement question, and one that went quiet is usually
// hardware.
func TestLocalControlPlane_ReclaimBlockersNamesEachReason(t *testing.T) {
	reg := NewDataVersionRegistry()
	var leader atomic.Bool
	gate := NewLeaderGate(func() bool { return leader.Load() }, reg.Reset)
	c := NewLocalControlPlane(nil,
		WithDataVersionView(reg, gate),
		WithRequiredReplicas(func() ([]int64, error) { return []int64{1, 2, 3}, nil }))

	leader.Store(true)
	gate.IsLeader() // prime the takeover clear
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	reg.Record(2, "10.0.0.2:7000", map[string]int64{"kb-other": 9})
	// Node 3 never reports.

	blockers, ok := c.ReclaimBlockers("kb-1")
	if !ok {
		t.Fatal("a leader with an aggregate and a topology must be able to answer")
	}
	byNode := make(map[int64]ReclaimBlocker, len(blockers))
	for _, b := range blockers {
		byNode[b.NodeID] = b
	}
	if len(blockers) != 2 {
		t.Fatalf("blockers = %+v, want exactly nodes 2 and 3", blockers)
	}
	if _, blocked := byNode[1]; blocked {
		t.Errorf("node 1 reports a fresh cursor for kb-1; it must not appear: %+v", byNode[1])
	}
	if got := byNode[2].Reason; got != ReclaimBlockerNoCursor {
		t.Errorf("node 2 reason = %q, want %q: it reports, but said nothing about kb-1",
			got, ReclaimBlockerNoCursor)
	}
	if got := byNode[3].Reason; got != ReclaimBlockerNeverReported {
		t.Errorf("node 3 reason = %q, want %q", got, ReclaimBlockerNeverReported)
	}
}

// The stale case is the one with a number worth quoting: an operator wants to know how far
// that replica got and when it last spoke, because "reported 9 an hour ago" and "never
// reported" are different incidents.
func TestLocalControlPlane_ReclaimBlockersQuoteTheLastReport(t *testing.T) {
	reg := NewDataVersionRegistry()
	var leader atomic.Bool
	gate := NewLeaderGate(func() bool { return leader.Load() }, reg.Reset)
	c := NewLocalControlPlane(nil,
		WithDataVersionView(reg, gate),
		WithRequiredReplicas(func() ([]int64, error) { return []int64{1, 2}, nil }),
		WithStorageSilenceWindow(time.Nanosecond))

	leader.Store(true)
	gate.IsLeader()
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	reg.Record(2, "10.0.0.2:7000", map[string]int64{"kb-1": 7})
	time.Sleep(time.Millisecond)

	blockers, ok := c.ReclaimBlockers("kb-1")
	if !ok {
		t.Fatal("a leader with an aggregate and a topology must be able to answer")
	}
	if len(blockers) != 2 {
		t.Fatalf("blockers = %+v, want both replicas (every report is past the window)", blockers)
	}
	byNode := make(map[int64]ReclaimBlocker, len(blockers))
	for _, b := range blockers {
		byNode[b.NodeID] = b
	}
	if got := byNode[2].Reason; got != ReclaimBlockerStale {
		t.Errorf("node 2 reason = %q, want %q", got, ReclaimBlockerStale)
	}
	if b := byNode[2]; b.Reached != 7 || b.ReportedAt.IsZero() {
		t.Errorf("node 2 blocker = %+v, want the cursor it last claimed (7) and when it said so", b)
	}
}

// ok=false is "nobody here can tell", and it must NOT be confused with an empty list
// ("nothing is blocked"): a caller that shows this to an operator would otherwise render a
// follower's inability to answer as a clean bill of health.
func TestLocalControlPlane_ReclaimBlockersAreUnavailableOffTheLeader(t *testing.T) {
	reg := NewDataVersionRegistry()
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	followerGate := NewLeaderGate(func() bool { return false }, nil)
	follower := NewLocalControlPlane(nil,
		WithDataVersionView(reg, followerGate),
		WithRequiredReplicas(func() ([]int64, error) { return []int64{1}, nil }))

	if blockers, ok := follower.ReclaimBlockers("kb-1"); ok {
		t.Errorf("a follower answered %+v; it has no aggregate to judge from", blockers)
	}

	// The contrast: a leader whose replicas are all fresh answers empty-and-available.
	leaderReg := NewDataVersionRegistry()
	var leader atomic.Bool
	leaderGate := NewLeaderGate(func() bool { return leader.Load() }, leaderReg.Reset)
	leaderC := NewLocalControlPlane(nil,
		WithDataVersionView(leaderReg, leaderGate),
		WithRequiredReplicas(func() ([]int64, error) { return []int64{1}, nil }))
	leader.Store(true)
	leaderGate.IsLeader()
	leaderReg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})

	if blockers, ok := leaderC.ReclaimBlockers("kb-1"); !ok || len(blockers) != 0 {
		t.Errorf("blockers = (%+v, %v), want (empty, true): every required replica is reporting and fresh",
			blockers, ok)
	}
}
