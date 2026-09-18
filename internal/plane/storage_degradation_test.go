package plane

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// A quorum of live replicas is the bar. Below it a write cannot reach its
// durability target, and naming that state is what this signal exists for
// (docs/storage-degradation-signal-plan.md §3.1).
func TestDataVersionRegistry_DegradeBelowQuorum(t *testing.T) {
	reg := NewDataVersionRegistry()
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	// Nodes 2 and 3 have never reported.

	d := reg.Degrade([]int64{1, 2, 3}, time.Now(), DefaultStorageSilenceWindow)

	if !d.Known {
		t.Fatal("reports exist and a replica set was given, so the verdict is knowable")
	}
	if d.State != StorageDegraded {
		t.Errorf("State = %s, want DEGRADED: 1 of 3 required replicas is live and the quorum is 2", d.State)
	}
	if d.Live != 1 || d.Required != 3 || d.Quorum != 2 {
		t.Errorf("verdict = %d of %d (quorum %d), want 1 of 3 (quorum 2)", d.Live, d.Required, d.Quorum)
	}
	if len(d.Silent) != 2 || d.Silent[0] != 2 || d.Silent[1] != 3 {
		t.Errorf("Silent = %v, want [2 3] in ascending order", d.Silent)
	}
	// The diagnosis has to say which replicas look missing and why: "never
	// reported" and "went quiet" are different faults, and an operator reading a
	// refusal has to be able to tell them apart.
	for _, want := range []string{
		"1 of 3 required replicas live",
		"below the quorum of 2",
		"2 (never reported)",
		"3 (never reported)",
	} {
		if !strings.Contains(d.Detail, want) {
			t.Errorf("Detail = %q, want it to contain %q", d.Detail, want)
		}
	}
}

// Exactly a quorum is HEALTHY; one short is not. The boundary is the whole
// question, so both sides of it get a case.
func TestDataVersionRegistry_DegradeAtQuorumIsHealthy(t *testing.T) {
	reg := NewDataVersionRegistry()
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	reg.Record(2, "10.0.0.2:7000", map[string]int64{"kb-1": 9})
	// Node 3 is silent, and that is allowed: 2 of 3 is a quorum.

	d := reg.Degrade([]int64{1, 2, 3}, time.Now(), DefaultStorageSilenceWindow)

	if !d.Known || d.State != StorageHealthy {
		t.Errorf("State = %s (known=%v), want HEALTHY: 2 of 3 is exactly a quorum", d.State, d.Known)
	}
	if len(d.Silent) != 1 || d.Silent[0] != 3 {
		t.Errorf("Silent = %v, want the one quiet replica named even though the verdict is healthy", d.Silent)
	}
}

// The window IS the hysteresis §3.2 asks for: a report that arrived inside it keeps
// its replica live, and one that fell outside it does not. Time is a parameter, so
// the test moves the clock rather than sleeping.
func TestDataVersionRegistry_DegradeWindowIsTheHysteresis(t *testing.T) {
	reg := NewDataVersionRegistry()
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})

	required := []int64{1, 2, 3}
	window := 30 * time.Second

	if d := reg.Degrade(required, time.Now(), window); d.State != StorageDegraded {
		t.Errorf("a report inside the window must count as live: State = %s, want DEGRADED (1 of 3 live)", d.State)
	}

	// Same report, clock moved past the window: it is stale now, and nothing is
	// left answering — which is the extreme tier, not merely "short".
	d := reg.Degrade(required, time.Now().Add(window+time.Second), window)
	if d.State != StorageUnavailable {
		t.Errorf("a report older than the window must not count: State = %s, want UNAVAILABLE", d.State)
	}
	if !strings.Contains(d.Detail, "1 (last report") {
		t.Errorf("Detail = %q, want the age of the last report", d.Detail)
	}
}

// The two failure tiers are separate states, and the boundary between them is
// exactly "is anyone at all still answering" (docs/storage-degradation-signal-plan.md
// §4.1): one live replica out of three is degraded, zero is unavailable.
func TestDataVersionRegistry_DegradeTiers(t *testing.T) {
	reg := NewDataVersionRegistry()
	required := []int64{1, 2, 3}

	t.Run("one replica answering is DEGRADED", func(t *testing.T) {
		reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
		d := reg.Degrade(required, time.Now(), DefaultStorageSilenceWindow)
		if d.State != StorageDegraded {
			t.Errorf("State = %s, want DEGRADED: 1 of 3 is short of the quorum of 2 but someone answers", d.State)
		}
		if d.Live != 1 {
			t.Errorf("Live = %d, want 1", d.Live)
		}
	})

	t.Run("nobody answering is UNAVAILABLE", func(t *testing.T) {
		empty := NewDataVersionRegistry() // not one report for this replica set
		empty.Record(99, "10.0.0.99:7000", map[string]int64{"kb-1": 9})
		d := empty.Degrade(required, time.Now(), DefaultStorageSilenceWindow)
		if d.State != StorageUnavailable {
			t.Errorf("State = %s, want UNAVAILABLE: none of the required replicas is live", d.State)
		}
		if d.Live != 0 || d.Quorum != 2 {
			t.Errorf("Live/Quorum = %d/%d, want 0/2", d.Live, d.Quorum)
		}
		// The diagnosis has to say what makes it the extreme tier, or an operator
		// cannot tell it from "one short".
		if !strings.Contains(d.Detail, "no required replica is live") {
			t.Errorf("Detail = %q, want it to say nobody is answering", d.Detail)
		}
		if got := d.State.String(); got != "UNAVAILABLE" {
			t.Errorf("String() = %q, want UNAVAILABLE", got)
		}
	})
}

// The two reducers the service layer consumes have to disagree in exactly the way
// the states do, because the pair is what picks the sentinel.
func TestLocalControlPlane_StorageTierReducers(t *testing.T) {
	gate := NewLeaderGate(func() bool { return true }, nil)

	cases := []struct {
		name            string
		reportedNodes   []int64
		wantDegraded    bool
		wantUnavailable bool
	}{
		{"a quorum live", []int64{1, 2}, false, false},
		{"short of a quorum", []int64{1}, true, false},
		// Node 99 is not in the replica set: the aggregate is non-empty, so this is
		// a KNOWABLE "nobody required is answering" — distinct from the empty
		// aggregate below, which is "unknown" and has to fail open.
		{"nobody required is answering", []int64{99}, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewDataVersionRegistry()
			for _, nodeID := range tc.reportedNodes {
				reg.Record(nodeID, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
			}
			c := NewLocalControlPlane(nil,
				WithDataVersionView(reg, gate),
				WithRequiredReplicas(func() ([]int64, error) { return []int64{1, 2, 3}, nil }))

			degraded, _, degradedOK := c.StorageDegraded("kb-1")
			unavailable, _, unavailableOK := c.StorageUnavailable("kb-1")
			if !degradedOK || !unavailableOK {
				t.Fatalf("both reducers must have a verdict here (ok = %v / %v)", degradedOK, unavailableOK)
			}
			if degraded != tc.wantDegraded || unavailable != tc.wantUnavailable {
				t.Errorf("reducers = (degraded=%v, unavailable=%v), want (%v, %v)",
					degraded, unavailable, tc.wantDegraded, tc.wantUnavailable)
			}
		})
	}

	t.Run("unknown leaves both false", func(t *testing.T) {
		c := NewLocalControlPlane(nil,
			WithDataVersionView(NewDataVersionRegistry(), gate),
			WithRequiredReplicas(func() ([]int64, error) { return []int64{1, 2, 3}, nil }))
		degraded, _, _ := c.StorageDegraded("kb-1")
		unavailable, _, _ := c.StorageUnavailable("kb-1")
		if degraded || unavailable {
			t.Errorf("reducers = (%v, %v), want both false: a caller checking either alone must fail open", degraded, unavailable)
		}
	})
}

// Every way of not knowing is UNKNOWN, never "unavailable" — a fresh leader, an
// unwired replica set and an empty aggregate all have to let the write through, or
// the signal itself becomes the outage it was meant to warn about (§3.3).
func TestDataVersionRegistry_DegradeUnknownIsNotUnavailable(t *testing.T) {
	t.Run("empty aggregate", func(t *testing.T) {
		reg := NewDataVersionRegistry()
		if d := reg.Degrade([]int64{1, 2, 3}, time.Now(), DefaultStorageSilenceWindow); d.Known {
			t.Error("a leader that has folded no report cannot judge redundancy")
		}
	})

	t.Run("after a leadership change", func(t *testing.T) {
		reg := NewDataVersionRegistry()
		reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
		reg.Reset() // what LeaderGate does on the false→true edge
		if d := reg.Degrade([]int64{1}, time.Now(), DefaultStorageSilenceWindow); d.Known {
			t.Error("a new term must not answer redundancy from a predecessor's reports")
		}
	})

	t.Run("no replica set", func(t *testing.T) {
		reg := NewDataVersionRegistry()
		reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
		if d := reg.Degrade(nil, time.Now(), DefaultStorageSilenceWindow); d.Known {
			t.Error("without a topology there is nothing to hold the reports against")
		}
	})

	t.Run("a zero window takes the default", func(t *testing.T) {
		reg := NewDataVersionRegistry()
		reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
		d := reg.Degrade([]int64{1}, time.Now(), 0)
		if !d.Known || d.State != StorageHealthy {
			t.Errorf("Degrade(window=0) = (%s, known=%v), want the default window and HEALTHY", d.State, d.Known)
		}
	})
}

// StorageDegradation is the verdict behind the leader gate and the topology hook,
// so the ways of NOT being able to judge it are checked through that entry point
// too — that is the one the write gates actually call.
func TestLocalControlPlane_StorageDegradation(t *testing.T) {
	t.Run("below quorum, and the diagnosis names the knowledge base", func(t *testing.T) {
		c, reg := reclaimFixture(t, 1, 2, 3)
		reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})

		state, detail, ok := c.StorageDegradation("kb-1")
		if !ok || state != StorageDegraded {
			t.Fatalf("StorageDegradation = (%s, ok=%v), want DEGRADED", state, ok)
		}
		if !strings.HasPrefix(detail, "kb-1: ") {
			t.Errorf("detail = %q, want the knowledge base named", detail)
		}
	})

	t.Run("not the leader", func(t *testing.T) {
		reg := NewDataVersionRegistry()
		reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
		c := NewLocalControlPlane(nil,
			WithDataVersionView(reg, NewLeaderGate(func() bool { return false }, nil)),
			WithRequiredReplicas(func() ([]int64, error) { return []int64{1}, nil }))
		if _, _, ok := c.StorageDegradation("kb-1"); ok {
			t.Error("a follower holds no authoritative aggregate")
		}
	})

	t.Run("no replica set wired", func(t *testing.T) {
		reg := NewDataVersionRegistry()
		reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
		c := NewLocalControlPlane(nil,
			WithDataVersionView(reg, NewLeaderGate(func() bool { return true }, nil)))
		if _, _, ok := c.StorageDegradation("kb-1"); ok {
			t.Error("without a replica set there is no requirement to judge against")
		}
	})

	t.Run("topology read fails", func(t *testing.T) {
		reg := NewDataVersionRegistry()
		reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
		c := NewLocalControlPlane(nil,
			WithDataVersionView(reg, NewLeaderGate(func() bool { return true }, nil)),
			WithRequiredReplicas(func() ([]int64, error) {
				return nil, errors.New("cluster status unavailable")
			}))
		if _, _, ok := c.StorageDegradation("kb-1"); ok {
			t.Error("an unreadable topology must not be read as an empty requirement")
		}
	})

	t.Run("no aggregate wired", func(t *testing.T) {
		c := NewLocalControlPlane(nil,
			WithRequiredReplicas(func() ([]int64, error) { return []int64{1}, nil }))
		if _, _, ok := c.StorageDegradation("kb-1"); ok {
			t.Error("without reports there is nothing to judge")
		}
	})

	// The one-bit reducer must never claim "degraded" when it is in fact
	// UNKNOWN: a caller that checks only the flag still has to fail open.
	t.Run("the boolean reducer fails open", func(t *testing.T) {
		reg := NewDataVersionRegistry() // empty: nothing folded yet
		c := NewLocalControlPlane(nil,
			WithDataVersionView(reg, NewLeaderGate(func() bool { return true }, nil)),
			WithRequiredReplicas(func() ([]int64, error) { return []int64{1}, nil }))
		degraded, _, ok := c.StorageDegraded("kb-1")
		if ok || degraded {
			t.Errorf("StorageDegraded = (degraded=%v, ok=%v), want (false, false)", degraded, ok)
		}
	})
}

// A conversation about one knowledge base cannot reach the verdict: with a single
// cluster-wide replica topology every KB shares the answer (§3.1). Pinning that
// here keeps the simplification deliberate rather than a surprise on the day per-KB
// placement lands (§10.2).
func TestLocalControlPlane_StorageDegradationIgnoresTheKnowledgeBase(t *testing.T) {
	c, reg := reclaimFixture(t, 1, 2)
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})

	busyState, _, busyOK := c.StorageDegradation("kb-1")
	quietState, _, quietOK := c.StorageDegradation("kb-never-mentioned")

	if busyOK != quietOK || busyState != quietState {
		t.Errorf("verdict varies by KB: (%s, known=%v) vs (%s, known=%v); one replica topology means one answer",
			busyState, busyOK, quietState, quietOK)
	}
	if busyState != StorageDegraded {
		t.Errorf("State = %s, want DEGRADED: 1 of 2 required replicas is live and the quorum is 2", busyState)
	}
}

// The window is configurable, and a non-positive value keeps the default rather
// than disabling the verdict: "no window" would mean every replica is permanently
// stale, which reads as a permanent outage.
func TestLocalControlPlane_StorageSilenceWindowOption(t *testing.T) {
	reg := NewDataVersionRegistry()
	reg.Record(1, "10.0.0.1:7000", map[string]int64{"kb-1": 9})
	gate := NewLeaderGate(func() bool { return true }, nil)

	t.Run("a wider window keeps a stale report live", func(t *testing.T) {
		c := NewLocalControlPlane(nil,
			WithDataVersionView(reg, gate),
			WithRequiredReplicas(func() ([]int64, error) { return []int64{1}, nil }),
			WithStorageSilenceWindow(time.Hour))
		if _, _, ok := c.StorageDegradation("kb-1"); !ok {
			t.Fatal("a report from a moment ago is inside an hour-wide window")
		}
	})

	t.Run("a non-positive window keeps the default", func(t *testing.T) {
		c := NewLocalControlPlane(nil,
			WithDataVersionView(reg, gate),
			WithRequiredReplicas(func() ([]int64, error) { return []int64{1}, nil }),
			WithStorageSilenceWindow(0))
		if c.silenceWindow != 0 {
			t.Errorf("silenceWindow = %s, want the zero value that Degrade reads as the default", c.silenceWindow)
		}
		if _, _, ok := c.StorageDegradation("kb-1"); !ok {
			t.Error("a fresh report is live under the default window too")
		}
	})
}
