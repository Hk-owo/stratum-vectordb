package plane

import (
	"sync/atomic"
	"testing"
)

// The view is only available while this node leads, and "unavailable" must never
// collapse into "nobody holds it": the latter would justify deleting data a
// healthy node actually has (§10.6).
func TestLocalControlPlane_DataVersionHoldersAnswersOnlyAsLeader(t *testing.T) {
	reg := NewDataVersionRegistry()
	reg.Record(2, map[string]int64{"kb-1": 7})
	reg.Record(3, map[string]int64{"kb-1": 4})

	var leader atomic.Bool
	gate := NewLeaderGate(func() bool { return leader.Load() }, reg.Reset)
	// rn is nil on purpose: answering "who holds V" reads only the soft aggregate,
	// never the replicated state machine.
	c := NewLocalControlPlane(nil, WithDataVersionView(reg, gate))

	if holders, ok := c.DataVersionHolders("kb-1", 4); ok || holders != nil {
		t.Fatalf("as a follower: (%v, %v), want (nil, false) — an unavailable answer", holders, ok)
	}

	leader.Store(true)
	// Taking over clears the aggregate: the first query of a new term sees no
	// reports at all, and says so with ok=true rather than pretending to know.
	if holders, ok := c.DataVersionHolders("kb-1", 4); !ok || len(holders) != 0 {
		t.Fatalf("right after takeover: (%v, %v), want (empty, true)", holders, ok)
	}

	reg.Record(2, map[string]int64{"kb-1": 7})
	holders, ok := c.DataVersionHolders("kb-1", 4)
	if !ok {
		t.Fatal("a leader with a report must answer")
	}
	if len(holders) != 1 || holders[0] != 2 {
		t.Errorf("holders = %v, want [2] — the takeover cleared every report, and only node 2 has re-reported since", holders)
	}

	leader.Store(false)
	if holders, ok := c.DataVersionHolders("kb-1", 4); ok || holders != nil {
		t.Errorf("after losing leadership: (%v, %v), want (nil, false)", holders, ok)
	}
}

// With no aggregate wired the answer is unavailable, not empty — the same
// distinction, one level lower.
func TestLocalControlPlane_DataVersionHoldersUnavailableWithoutView(t *testing.T) {
	c := NewLocalControlPlane(nil)
	if holders, ok := c.DataVersionHolders("kb-1", 1); ok || holders != nil {
		t.Errorf("with no view wired: (%v, %v), want (nil, false)", holders, ok)
	}
}
