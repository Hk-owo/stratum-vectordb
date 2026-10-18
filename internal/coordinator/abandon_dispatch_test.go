package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/plane"
	"stratum/internal/types"
)

// recordingControlPlane records what AbandonDispatch reported, and whether the
// context it was handed was still alive when it arrived.
type recordingControlPlane struct {
	plane.ControlPlane // embedded nil: only the methods below are ever called

	mu       sync.Mutex
	reported bool
	live     bool
	class    types.FailureClass
}

func (c *recordingControlPlane) ReportVersionFailure(
	ctx context.Context, _ string, _ int64, _ types.FailureSide, class types.FailureClass, _ string,
) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reported = true
	c.live = ctx.Err() == nil
	c.class = class
	return false, nil
}

func (c *recordingControlPlane) snapshot() (reported, live bool, class types.FailureClass) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reported, c.live, c.class
}

// TestAbandonDispatch_RecordsDespiteExpiredCallerContext pins H10 of
// docs/code-review-2026-10-18.md.
//
// A dispatch that ran out of budget is the ordinary reason to abandon a write, and
// the context passed in is the very one that just expired. Reporting the failure
// runs a Raft round trip, so inheriting that context guaranteed the report failed
// too: the single give-up path could not record the giving-up, and the version sat
// PENDING with its retry budget already spent — the exact hole AbandonDispatch was
// introduced to close.
func TestAbandonDispatch_RecordsDespiteExpiredCallerContext(t *testing.T) {
	cp := &recordingControlPlane{}
	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{ControlPlane: cp})

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if ctx.Err() == nil {
		t.Fatal("precondition: the caller's context must already be expired")
	}

	coord.AbandonDispatch(ctx, "kb-1", 7, types.FailureTransient, "dispatch ran out of budget")

	reported, live, _ := cp.snapshot()
	if !reported {
		t.Fatal("the give-up was never reported to the control plane")
	}
	if !live {
		t.Error("the report was handed an already-cancelled context: it cannot succeed")
	}
}

// TestDispatchFailureClass pins the other half of the same call site: a permanent
// refusal must not spend the retry budget before reaching the verdict it could reach
// immediately.
func TestDispatchFailureClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want types.FailureClass
	}{
		{
			name: "knowledge base is gone",
			err:  fmt.Errorf("resolve candidates: %w", stratumerrors.ErrKnowledgeBaseNotFound),
			want: types.FailureFatalGlobal,
		},
		{
			name: "parent version rejected",
			err:  fmt.Errorf("validate: %w", stratumerrors.ErrInvalidParentVersion),
			want: types.FailureFatalGlobal,
		},
		{
			name: "empty change list",
			err:  stratumerrors.ErrEmptyChanges,
			want: types.FailureFatalGlobal,
		},
		{
			name: "budget expired",
			err:  context.DeadlineExceeded,
			want: types.FailureTransient,
		},
		{
			name: "peer blip",
			err:  errors.New("connection reset by peer"),
			want: types.FailureTransient,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dispatchFailureClass(tc.err); got != tc.want {
				t.Errorf("dispatchFailureClass(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
