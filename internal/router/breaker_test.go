package router

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

// TestBreaker_TripsAfterRepeatedFailures pins the closed→open edge and the
// reason minSamples exists: a node must not be taken out of rotation by one
// unlucky call.
func TestBreaker_TripsAfterRepeatedFailures(t *testing.T) {
	b := newBreaker(defaultBreakerConfig)
	now := time.Now()

	for i := 0; i < defaultBreakerConfig.minSamples-1; i++ {
		b.record(now, false)
	}
	if got := b.stateNow(now); got != breakerClosed {
		t.Fatalf("state = %v after %d failures, want closed (minSamples = %d)",
			got, defaultBreakerConfig.minSamples-1, defaultBreakerConfig.minSamples)
	}

	b.record(now, false)
	if got := b.stateNow(now); got != breakerOpen {
		t.Fatalf("state = %v, want open", got)
	}
	if b.allow(now) {
		t.Error("an open breaker must refuse traffic for the duration of its cooldown")
	}
}

// TestBreaker_CooldownThenHalfOpenClosesOnSuccess walks the whole cycle:
// open → (cooldown) → half-open → (probes succeed) → closed.
func TestBreaker_CooldownThenHalfOpenClosesOnSuccess(t *testing.T) {
	b := newBreaker(defaultBreakerConfig)
	now := time.Now()
	for i := 0; i <= defaultBreakerConfig.minSamples; i++ {
		b.record(now, false)
	}
	if got := b.stateNow(now); got != breakerOpen {
		t.Fatalf("state = %v, want open", got)
	}

	later := now.Add(defaultBreakerConfig.cooldown + time.Millisecond)
	if !b.allow(later) {
		t.Fatal("after the cooldown the breaker must admit a probe")
	}
	if got := b.stateNow(later); got != breakerHalfOpen {
		t.Fatalf("state = %v, want half-open", got)
	}

	for i := 0; i < defaultBreakerConfig.halfOpenProbes; i++ {
		if !b.allow(later) {
			t.Fatalf("probe %d refused while half-open", i)
		}
		b.record(later, true)
	}
	if got := b.stateNow(later); got != breakerClosed {
		t.Fatalf("state = %v after successful probes, want closed", got)
	}
}

// TestBreaker_HalfOpenFailureReopens pins the other exit from half-open: the
// node was given its chance and did not take it, so it goes back to open rather
// than being re-admitted.
func TestBreaker_HalfOpenFailureReopens(t *testing.T) {
	b := newBreaker(defaultBreakerConfig)
	now := time.Now()
	for i := 0; i <= defaultBreakerConfig.minSamples; i++ {
		b.record(now, false)
	}

	later := now.Add(defaultBreakerConfig.cooldown + time.Millisecond)
	b.allow(later) // consumes the first probe, moving to half-open
	b.record(later, false)

	if got := b.stateNow(later); got != breakerOpen {
		t.Fatalf("state = %v after a failed probe, want open", got)
	}
}

// TestBreaker_HalfOpenAdmitsABoundedNumberOfProbes keeps a still-sick node from
// being re-tested by every in-flight request at once.
func TestBreaker_HalfOpenAdmitsABoundedNumberOfProbes(t *testing.T) {
	b := newBreaker(defaultBreakerConfig)
	now := time.Now()
	for i := 0; i <= defaultBreakerConfig.minSamples; i++ {
		b.record(now, false)
	}
	later := now.Add(defaultBreakerConfig.cooldown + time.Millisecond)

	admitted := 0
	for i := 0; i < 10; i++ {
		if b.allow(later) {
			admitted++
		}
	}
	if admitted != defaultBreakerConfig.halfOpenProbes {
		t.Errorf("half-open admitted %d probes, want %d", admitted, defaultBreakerConfig.halfOpenProbes)
	}
}

// TestForward_SkipsACircuitBrokenNode is the point of the whole mechanism: a
// node the breaker has taken out must stop receiving traffic without every
// client paying its timeout first.
func TestForward_SkipsACircuitBrokenNode(t *testing.T) {
	cfg := defaultBreakerConfig
	r := &Router{
		storageAddrs: []string{"s1", "s2", "s3"},
		storageBreakers: []*breaker{
			newBreaker(cfg), newBreaker(cfg), newBreaker(cfg),
		},
	}

	now := time.Now()
	for i := 0; i <= cfg.minSamples; i++ {
		r.storageBreakers[0].record(now, false)
	}

	seen := map[int]int{}
	for i := 0; i < 6; i++ {
		if _, err := Forward(r, context.Background(), "/stratum.QueryService/Query", "",
			func(idx int, _ context.Context) (int, error) {
				seen[idx]++
				return idx, nil
			}); err != nil {
			t.Fatalf("Forward: %v", err)
		}
	}

	if seen[0] != 0 {
		t.Errorf("a circuit-broken node received %d calls, want 0 (routes: %v)", seen[0], seen)
	}
	if seen[1] == 0 || seen[2] == 0 {
		t.Errorf("healthy nodes must carry the traffic, got %v", seen)
	}
}

// TestForward_AllNodesBrokenIsAnErrorRatherThanASilentSuccess keeps the failure
// mode honest: with every candidate out, the caller must be told so.
func TestForward_AllNodesBrokenIsAnErrorRatherThanASilentSuccess(t *testing.T) {
	cfg := defaultBreakerConfig
	r := &Router{
		storageAddrs:    []string{"s1"},
		storageBreakers: []*breaker{newBreaker(cfg)},
	}
	now := time.Now()
	for i := 0; i <= cfg.minSamples; i++ {
		r.storageBreakers[0].record(now, false)
	}

	if _, err := Forward(r, context.Background(), "/stratum.QueryService/Query", "",
		func(idx int, _ context.Context) (int, error) { return idx, nil }); err == nil {
		t.Fatal("expected an error when every node is circuit-broken")
	}
}

// TestBreaker_BusinessFailuresDoNotTripIt pins the distinction observe draws.
//
// A breaker asks "can this node serve?", not "did this request succeed?". The
// failure that made this concrete: a client polling a version that is still
// building its index gets FailedPrecondition from every healthy replica, and
// counting those as failures blacklisted all three within minSamples attempts —
// a health mechanism causing the outage it exists to prevent.
func TestBreaker_BusinessFailuresDoNotTripIt(t *testing.T) {
	for name, err := range map[string]error{
		"version still building":   status.Error(codes.FailedPrecondition, "version is PENDING"),
		"no such knowledge base":   status.Error(codes.NotFound, "knowledge base not found"),
		"this node is not leader":  status.Error(codes.Internal, "kvraft: not leader"),
		"caller may not read this": status.Error(codes.PermissionDenied, "denied"),
	} {
		t.Run(name, func(t *testing.T) {
			r := &Router{storageBreakers: []*breaker{newBreaker(defaultBreakerConfig)}}
			for i := 0; i < 20; i++ {
				r.storageBreakers[0].observe(err)
			}
			if got := r.storageBreakers[0].stateNow(time.Now()); got != breakerClosed {
				t.Errorf("a node answering %v stays in rotation, got %v", err, got)
			}
		})
	}
}

// TestBreaker_NodeLevelFailuresDoTripIt is the other half: unreachable and
// unresponsive are what the breaker is for.
func TestBreaker_NodeLevelFailuresDoTripIt(t *testing.T) {
	for name, err := range map[string]error{
		"unreachable":              status.Error(codes.Unavailable, "connection refused"),
		"accepted, never answered": status.Error(codes.DeadlineExceeded, "deadline exceeded"),
		"shedding load":            status.Error(codes.ResourceExhausted, "overloaded"),
	} {
		t.Run(name, func(t *testing.T) {
			r := &Router{storageBreakers: []*breaker{newBreaker(defaultBreakerConfig)}}
			for i := 0; i <= defaultBreakerConfig.minSamples; i++ {
				r.storageBreakers[0].observe(err)
			}
			if got := r.storageBreakers[0].stateNow(time.Now()); got != breakerOpen {
				t.Errorf("a node that is %s must trip the breaker, got %v", name, got)
			}
		})
	}
}
