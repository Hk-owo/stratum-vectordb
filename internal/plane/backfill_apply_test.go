package plane

import (
	"context"
	"strings"
	"testing"

	"stratum/internal/types"
)

// TestLocalDataPlane_ApplyBackfillChanges_StopsAtTheLocalTransaction pins the
// §7.5 catch-up entry point: it replays a version's changes into the local
// stores and advances the cursor — and stops there.
//
// Each layer WriteVersionData adds is wrong for this caller, most of them
// harmfully so:
//   - fan-out would push the version at the very peers this node is copying it
//     from — a node catching up does not notify the nodes it is learning from;
//   - the durable report would re-confirm a version whose durability was settled
//     long ago (that is exactly why the peer's WAL range was still readable);
//   - the index build would spend work on a historical version that, per §8.6b,
//     may never be queried;
//   - and the failure report is the *coordinator's* channel: letting a node that
//     merely failed to fetch someone else's history reach §10.1's terminal
//     verdict would be a way to have a perfectly healthy version deleted.
func TestLocalDataPlane_ApplyBackfillChanges_StopsAtTheLocalTransaction(t *testing.T) {
	tr := &tracer{}
	index := &stubIndexStore{}
	pusher := &stubPusher{}
	control := &stubControl{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
		Pusher:       pusher,
		Control:      control,
		IndexManager: index,
		ResolveReplicas: func(context.Context) ([]string, error) {
			return []string{"peer:7001"}, nil
		},
	})

	err := dp.ApplyBackfillChanges(context.Background(), "kb-1", 5, 4,
		[]types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc-1"}})
	if err != nil {
		t.Fatalf("ApplyBackfillChanges: %v", err)
	}

	// The local transaction ran: BEGIN -> storage write -> COMMIT.
	trace := strings.Join(tr.trace, ",")
	for _, want := range []string{"begin:kb-1:4", "write:5", "commit:5"} {
		if !strings.Contains(trace, want) {
			t.Errorf("trace = %q, want it to contain %q (the local transaction must run)", trace, want)
		}
	}
	// ...and the version now counts as held.
	if got := dp.LocalVersionOf("kb-1"); got != 5 {
		t.Errorf("localVersion = %d, want 5 (the cursor must advance)", got)
	}

	// None of the layers that are wrong for a catching-up node fired.
	if len(pusher.calls) != 0 {
		t.Errorf("fan-out happened (%v): a catching-up node must not push the version at the peers it is copying from", pusher.calls)
	}
	if len(index.triggered) != 0 {
		t.Errorf("index build scheduled (%v): a historical version builds lazily or not at all (§8.6b)", index.triggered)
	}
	if len(control.failures) != 0 {
		t.Errorf("failure reported (%v): that verdict channel belongs to the coordinator, not to a node that failed to fetch history", control.failures)
	}
}
