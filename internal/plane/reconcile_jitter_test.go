package plane

import (
	"testing"
	"time"
)

// The jitter must land in [0, interval): it scatters the phase, it does not extend
// the cadence. An out-of-range value would either delay a pass by a whole extra
// interval or, if negative, turn time.After into an immediate fire for every node —
// exactly the synchronisation the jitter exists to remove.
func TestReconcileJitterStaysWithinTheInterval(t *testing.T) {
	const interval = time.Minute
	for i := 0; i < 1000; i++ {
		got := reconcileJitter(interval)
		if got < 0 || got >= interval {
			t.Fatalf("reconcileJitter(%v) = %v, want [0, %v)", interval, got, interval)
		}
	}
}

// And it must actually scatter: with an interval of a minute, 64 draws landing on
// one single value would mean the nodes stay in lockstep.
func TestReconcileJitterScattersThePhase(t *testing.T) {
	const interval = time.Minute
	seen := make(map[time.Duration]struct{})
	for i := 0; i < 64; i++ {
		seen[reconcileJitter(interval)] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("reconcileJitter produced %d distinct value(s) in 64 draws; want it to scatter", len(seen))
	}
}

// The caller has already substituted the default for a non-positive interval, but
// the draw must stay sane if it is ever handed one directly: zero delay, never a
// negative time.After.
func TestReconcileJitterNonPositiveInterval(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		if got := reconcileJitter(interval); got != 0 {
			t.Errorf("reconcileJitter(%v) = %v, want 0", interval, got)
		}
	}
}
