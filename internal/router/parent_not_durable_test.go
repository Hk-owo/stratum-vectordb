package router

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
)

// Both are answers from a healthy node, and another node would give the same one: asking
// around only spends the candidate budget.
func TestIsRetryableErr_SettledAnswersAreNotRetriedElsewhere(t *testing.T) {
	for _, sentinel := range []error{stratumerrors.ErrParentNotDurable, stratumerrors.ErrVersionDataSettled} {
		err := stratumerrors.ToGRPCStatus(fmt.Errorf("v7: %w", sentinel))
		for _, forWrite := range []bool{false, true} {
			if isRetryableErr(err, forWrite) {
				t.Errorf("%v (forWrite=%v) must not be retried on another node", sentinel, forWrite)
			}
		}
		if nodeLevelFailure(err) {
			t.Errorf("%v is a business answer and must not count against the node", sentinel)
		}
	}
}

// parent_not_durable reaches the caller by name after one attempt and leaves the breakers
// alone. Measured before it had its own code: a burst of these made the station report
// "every node for this layer is circuit-broken" while every control node was healthy.
func TestForward_ParentNotDurableReachesTheCallerWithoutTrippingABreaker(t *testing.T) {
	breakers := []*breaker{newBreaker(defaultBreakerConfig), newBreaker(defaultBreakerConfig), newBreaker(defaultBreakerConfig)}
	r := &Router{
		controlAddrs:    []string{"a", "b", "c"},
		discoverer:      &fakeResolver{order: []int{0, 1, 2}, ok: true},
		controlBreakers: breakers,
	}

	refused := stratumerrors.ToGRPCStatus(fmt.Errorf("parent 7: %w", stratumerrors.ErrParentNotDurable))
	var last error
	calls := 0
	for i := 0; i < defaultBreakerConfig.minSamples*3; i++ {
		_, last = Forward(r, context.Background(), pb.KnowledgeBaseService_CreateVersion_FullMethodName, "kb-1",
			func(int, context.Context) (string, error) {
				calls++
				return "", refused
			})
		if got := stratumerrors.ReasonOf(last); got != "parent_not_durable" {
			t.Fatalf("attempt %d: reason = %q (err %v), want parent_not_durable", i, got, last)
		}
	}
	if want := defaultBreakerConfig.minSamples * 3; calls != want {
		t.Errorf("control node asked %d times, want %d: one per Forward", calls, want)
	}
	for i, b := range breakers {
		if !b.allow(time.Now()) {
			t.Errorf("breaker %d opened on a business refusal", i)
		}
	}
}

// A write that found a leader and never asked it must not say there is no leader: that
// sent the diagnosis looking for an election that never happened.
func TestForward_BrokenLeaderIsNamedAndTheWriteIsNotSent(t *testing.T) {
	cfg := defaultBreakerConfig
	cfg.cooldown = time.Hour
	r := &Router{
		controlAddrs:    []string{"a", "b", "c"},
		discoverer:      &fakeResolver{order: []int{0, 0, 0, 0}, ok: true},
		controlBreakers: []*breaker{tripped(cfg, time.Now()), newBreaker(cfg), newBreaker(cfg)},
	}

	calls := 0
	_, err := Forward(r, context.Background(), pb.KnowledgeBaseService_CreateVersion_FullMethodName, "kb-1",
		func(int, context.Context) (string, error) {
			calls++
			return "ok", nil
		})

	if !errors.Is(err, stratumerrors.ErrControlCircuitOpen) {
		t.Fatalf("err = %v, want ErrControlCircuitOpen", err)
	}
	if calls != 0 {
		t.Errorf("calls = %d, want 0", calls)
	}
	if errors.Is(err, stratumerrors.ErrNoLeaderYet) {
		t.Error("a leader was found, so this must not read as no_leader_yet")
	}
}

// The breaker does not count a read the caller walked away from, however many there are.
func TestForward_ExpiredCallerDoesNotOpenAReadBreaker(t *testing.T) {
	breakers := []*breaker{newBreaker(defaultBreakerConfig)}
	r := &Router{controlAddrs: []string{"a"}, controlBreakers: breakers}

	for i := 0; i < defaultBreakerConfig.minSamples*2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		_, _ = Forward(r, ctx, pb.KnowledgeBaseService_AwaitVersion_FullMethodName, "kb-1",
			func(_ int, attempt context.Context) (string, error) {
				<-ctx.Done()
				return "", status.FromContextError(attempt.Err()).Err()
			})
		cancel()
	}
	if got := breakers[0].stateNow(time.Now()); got != breakerClosed {
		t.Fatalf("state = %v, want closed", got)
	}
}
