package plane

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// reclaimFixture wires a leader-side control plane with an aggregate, a replica
// set, and a switch that makes the node the leader.
func reclaimFixture(t *testing.T, required ...int64) (*LocalControlPlane, *DataVersionRegistry) {
	t.Helper()
	reg := NewDataVersionRegistry()
	var leader atomic.Bool
	gate := NewLeaderGate(func() bool { return leader.Load() }, reg.Reset)
	c := NewLocalControlPlane(nil,
		WithDataVersionView(reg, gate),
		WithRequiredReplicas(func() ([]int64, error) { return required, nil }))
	leader.Store(true)
	// Prime the gate: the first IsLeader() of a new term clears the aggregate
	// (that is the point of the takeover hook), so the tests record their reports
	// afterwards — which is also what happens in production, where reports only
	// start arriving once a leader exists.
	gate.IsLeader()
	return c, reg
}

// The watermark is the SLOWEST replica: a version is safe to forget only once every
// peer that needs the delta has it.
func TestLocalControlPlane_ReclaimWatermarkIsTheSlowestReplica(t *testing.T) {
	c, reg := reclaimFixture(t, 1, 2, 3)
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	reg.Record(2, "10.0.0.2:7000", map[string]int64{"kb-1": 4})
	reg.Record(3, "10.0.0.3:7000", map[string]int64{"kb-1": 7})

	got, ok := c.ReclaimableChangesThrough("kb-1")
	if !ok {
		t.Fatal("a fully reported replica set must produce a watermark")
	}
	if got != 4 {
		t.Errorf("watermark = %d, want 4 (the slowest replica decides)", got)
	}
}

// A replica that has never reported makes the answer UNKNOWN, not "excepted": its
// silence is not evidence that it does not need the changes.
func TestLocalControlPlane_ReclaimWatermarkUnknownWhenAReplicaIsSilent(t *testing.T) {
	c, reg := reclaimFixture(t, 1, 2, 3)
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	reg.Record(2, "10.0.0.2:7000", map[string]int64{"kb-1": 9})
	// Node 3 never reported.

	if got, ok := c.ReclaimableChangesThrough("kb-1"); ok {
		t.Errorf("watermark = (%d, true), want unknown: one required replica has not reported", got)
	}
}

// A replica that reported nothing for THIS knowledge base is equally unknown: it
// may hold other KBs, and its own cursor here was never stated.
func TestLocalControlPlane_ReclaimWatermarkUnknownWhenAReplicaOmitsTheKB(t *testing.T) {
	c, reg := reclaimFixture(t, 1, 2)
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	reg.Record(2, "10.0.0.2:7000", map[string]int64{"kb-2": 9}) // reports, but not for kb-1

	if _, ok := c.ReclaimableChangesThrough("kb-1"); ok {
		t.Error("want unknown: node 2 never said anything about kb-1")
	}
}

// Every failure to establish the watermark answers the same way, because every one
// of them resolves to "keep the data".
func TestLocalControlPlane_ReclaimWatermarkUnavailableWithoutTheMeansToKnow(t *testing.T) {
	reg := NewDataVersionRegistry()
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	gate := NewLeaderGate(func() bool { return true }, nil)

	t.Run("not the leader", func(t *testing.T) {
		followerGate := NewLeaderGate(func() bool { return false }, nil)
		c := NewLocalControlPlane(nil,
			WithDataVersionView(reg, followerGate),
			WithRequiredReplicas(func() ([]int64, error) { return []int64{1}, nil }))
		if _, ok := c.ReclaimableChangesThrough("kb-1"); ok {
			t.Error("a follower has no authoritative view")
		}
	})

	t.Run("no replica set wired", func(t *testing.T) {
		c := NewLocalControlPlane(nil, WithDataVersionView(reg, gate))
		if _, ok := c.ReclaimableChangesThrough("kb-1"); ok {
			t.Error("without a topology there is no requirement to check against")
		}
	})

	t.Run("empty replica set", func(t *testing.T) {
		c := NewLocalControlPlane(nil,
			WithDataVersionView(reg, gate),
			WithRequiredReplicas(func() ([]int64, error) { return nil, nil }))
		if _, ok := c.ReclaimableChangesThrough("kb-1"); ok {
			t.Error("an empty replica set is not a watermark of 0 — it is no information at all")
		}
	})

	t.Run("topology read fails", func(t *testing.T) {
		c := NewLocalControlPlane(nil,
			WithDataVersionView(reg, gate),
			WithRequiredReplicas(func() ([]int64, error) { return nil, errors.New("cluster status unavailable") }))
		if _, ok := c.ReclaimableChangesThrough("kb-1"); ok {
			t.Error("an unreadable topology must not be read as an empty requirement")
		}
	})

	t.Run("no aggregate wired", func(t *testing.T) {
		c := NewLocalControlPlane(nil, WithRequiredReplicas(func() ([]int64, error) { return []int64{1}, nil }))
		if _, ok := c.ReclaimableChangesThrough("kb-1"); ok {
			t.Error("without reports there is nothing to check")
		}
	})
}

// A single-replica cluster reclaims through whatever that replica reported — the
// same rule, just with nothing to take a minimum over.
func TestLocalControlPlane_ReclaimWatermarkSingleReplica(t *testing.T) {
	c, reg := reclaimFixture(t, 1)
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 6})

	got, ok := c.ReclaimableChangesThrough("kb-1")
	if !ok || got != 6 {
		t.Errorf("watermark = (%d, %v), want (6, true)", got, ok)
	}
}

// A LEADER's watermark is its own aggregate's answer, and "my aggregate cannot answer"
// is one of those answers — the value an earlier leader carried back on a report's
// response is NOT a substitute for it. Two things are wrong with consulting it: it was
// computed from evidence this term may never have received (which is why LeaderGate
// clears the aggregate on takeover), and it SELF-PERPETUATES, because this node's own
// report response carries the answer back in and SetLeaderWatermarks replaces the map
// wholesale — so a frozen value would keep re-justifying itself for as long as a required
// replica stays away (a swapped disk being rebuilt, a storage node that has not started).
//
// What getting this wrong costs is not a stale read but a FROZEN one: tombstone pruning
// would keep advancing through versions that replica may still ask about, and
// ReclaimChanges would discard the deltas it needs to catch up.
func TestLocalControlPlane_ReclaimWatermarkLeaderDoesNotFallBackToACarriedBackValue(t *testing.T) {
	c, reg := reclaimFixture(t, 1, 2)
	// What a report's response carried back while this node was still a follower: stored,
	// but not admissible for a node that leads.
	c.SetLeaderWatermarks(map[string]int64{"kb-1": 9})
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	// Node 2 is required and has never reported, so the aggregate cannot answer.

	if got, ok := c.ReclaimableChangesThrough("kb-1"); ok {
		t.Errorf("watermark = (%d, true), want unknown: a leader judges from its own "+
			"aggregate, never from a value a previous term carried back", got)
	}
}

// The fallback IS the mechanism for a node that writes data but does not lead: it has
// no authoritative aggregate, so what the leader sent is all it has. Pinned so that
// narrowing the fallback to non-leaders stays a visible, deliberate change instead of
// silently taking this path away too.
func TestLocalControlPlane_ReclaimWatermarkFollowerUsesTheCarriedBackValue(t *testing.T) {
	followerGate := NewLeaderGate(func() bool { return false }, nil)
	reg := NewDataVersionRegistry()
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	c := NewLocalControlPlane(nil,
		WithDataVersionView(reg, followerGate),
		WithRequiredReplicas(func() ([]int64, error) { return []int64{1}, nil }))
	c.SetLeaderWatermarks(map[string]int64{"kb-1": 7})

	got, ok := c.ReclaimableChangesThrough("kb-1")
	if !ok || got != 7 {
		t.Errorf("watermark = (%d, %v), want (7, true): a follower has no aggregate to judge from", got, ok)
	}
}

// A takeover clears the aggregate — that is the hook's whole purpose, since the aggregate
// is a per-term fact — and the watermarks a PREVIOUS leader carried back must not outlive
// it either. This is the rule above seen from the shape an election produces: the node now
// LEADS, its aggregate is empty by design, and a required replica has not reported to this
// term — three facts that together say "unknown", which must not be answered with a number
// from the term the aggregate just dropped.
func TestLocalControlPlane_ReclaimWatermarkDoesNotSurviveItsOwnTakeover(t *testing.T) {
	reg := NewDataVersionRegistry()
	var leader atomic.Bool
	gate := NewLeaderGate(func() bool { return leader.Load() }, reg.Reset)
	c := NewLocalControlPlane(nil,
		WithDataVersionView(reg, gate),
		WithRequiredReplicas(func() ([]int64, error) { return []int64{1, 2}, nil }))

	// While this node was a follower, an accepted report response carried this back.
	c.SetLeaderWatermarks(map[string]int64{"kb-1": 9})

	// It takes over: the aggregate goes (that is what the hook does), and the carried-back
	// value stops being admissible with it.
	leader.Store(true)
	gate.IsLeader()
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	// Node 2 has not reported to this term, so the aggregate cannot answer.

	if got, ok := c.ReclaimableChangesThrough("kb-1"); ok {
		t.Errorf("watermark = (%d, true), want unknown: the value outlived the aggregate the "+
			"takeover cleared", got)
	}
}

// A required replica that reported ONCE and then went quiet — a swapped disk, a paused
// container, a partition — must not keep voting with its last cursor. A cursor is
// evidence from the term it was reported in, and REPORTING is the only thing that
// replaces it, so without a freshness rule a swapped disk holds the watermark still: the
// value never falls out of the aggregate on its own, and pruning keeps advancing through
// versions that disk may still ask about. This is the same judgement
// StorageDegradation makes, from the same number (ReportedAt against silenceWindow), and
// the two must not disagree about when a report stopped counting.
//
// The window is a NANOSECOND and the report is made a millisecond before it is read, so
// the record is stale many orders of magnitude over: that is how the case reaches
// "reported, then silent" without a clock of its own. No carried-back value is stored
// either, so the aggregate is the only thing that could answer.
func TestLocalControlPlane_ReclaimWatermarkUnknownWhenAReportedReplicaGoesStale(t *testing.T) {
	reg := NewDataVersionRegistry()
	var leader atomic.Bool
	gate := NewLeaderGate(func() bool { return leader.Load() }, reg.Reset)
	c := NewLocalControlPlane(nil,
		WithDataVersionView(reg, gate),
		WithRequiredReplicas(func() ([]int64, error) { return []int64{1, 2}, nil }),
		WithStorageSilenceWindow(time.Nanosecond))

	leader.Store(true)
	gate.IsLeader() // prime the takeover clear
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	reg.Record(2, "10.0.0.2:7000", map[string]int64{"kb-1": 9})
	// Node 2 now swaps its disk and stops reporting. Long past any silence window.
	time.Sleep(time.Millisecond)

	if got, ok := c.ReclaimableChangesThrough("kb-1"); ok {
		t.Errorf("watermark = (%d, true), want unknown: a report that old is not evidence that "+
			"the replica is caught up now, and pruning through it would advance past versions "+
			"the swapped disk may still ask about", got)
	}
}

// The counterpart, and the reason the freshness rule is usable at all: a report INSIDE
// the window still counts. An election or an ordinary restart costs a report or two, and
// if that were enough to make the answer unknown, reclaim would stall every time
// leadership moved. Same fixture as above with the default window (three report
// intervals) and a report made a moment ago.
func TestLocalControlPlane_ReclaimWatermarkKeepsAFreshReport(t *testing.T) {
	reg := NewDataVersionRegistry()
	var leader atomic.Bool
	gate := NewLeaderGate(func() bool { return leader.Load() }, reg.Reset)
	c := NewLocalControlPlane(nil,
		WithDataVersionView(reg, gate),
		WithRequiredReplicas(func() ([]int64, error) { return []int64{1, 2}, nil }))

	leader.Store(true)
	gate.IsLeader() // prime the takeover clear
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	reg.Record(2, "10.0.0.2:7000", map[string]int64{"kb-1": 7})

	got, ok := c.ReclaimableChangesThrough("kb-1")
	if !ok || got != 7 {
		t.Errorf("watermark = (%d, %v), want (7, true): a fresh report must still count, "+
			"or a missed report would stall reclaim", got, ok)
	}
}
