package plane

import (
	"context"
	"sync"
	"testing"
)

// TestLocalDataPlane_EnsureIndex_ReResolvesSourceEachAttempt pins the "the
// announcement arrives late" case of §8.5: the first resolution may legitimately
// answer "the leader" while the real holder is a coordinator that is not the
// leader and has not finished writing — so its announcement is not in the table
// yet. An implementation that resolves once and then keeps using that answer
// pulls from a pointless source until the timeout expires, never noticing the
// announcement that arrived in the meantime.
func TestLocalDataPlane_EnsureIndex_ReResolvesSourceEachAttempt(t *testing.T) {
	var mu sync.Mutex
	resolves := 0
	puller := &sourceRecordingPuller{}
	verifies := 0

	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		Puller:       puller,
		Verify: func(context.Context, string, int64) bool {
			verifies++
			// The data only counts as complete once the coordinator's writes
			// have landed, i.e. from the third attempt on.
			return verifies >= 3
		},
		Resolve: func(context.Context, string, int64) (string, bool, error) {
			mu.Lock()
			defer mu.Unlock()
			resolves++
			if resolves < 3 {
				// Nothing announced yet: the pre-§8.5 answer is the leader,
				// which does not hold a version a non-leader coordinated.
				return "leader:7000", true, nil
			}
			return "coordinator:7001", true, nil
		},
	})

	if err := dp.EnsureIndex(context.Background(), "kb-1", 2); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}

	if len(puller.sources) == 0 {
		t.Fatal("no pull was attempted")
	}
	if puller.sources[0] != "leader:7000" {
		t.Fatalf("first pull source = %q, want the initial leader fallback", puller.sources[0])
	}
	if last := puller.sources[len(puller.sources)-1]; last != "coordinator:7001" {
		t.Fatalf("last pull source = %q, want the newly announced coordinator (all: %v)",
			last, puller.sources)
	}
}
