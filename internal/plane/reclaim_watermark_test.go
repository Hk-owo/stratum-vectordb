package plane

import (
	"errors"
	"sync/atomic"
	"testing"
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
