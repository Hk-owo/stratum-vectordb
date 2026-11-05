package router

import (
	"context"
	"fmt"
	"testing"
	"time"

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
