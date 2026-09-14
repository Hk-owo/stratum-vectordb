package index

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestBlockedCollections_KeepsTheFirstSightingAndSorts: what makes this signal
// useful is its AGE and its ORDER — one tells an operator whether this is a moment
// or a deployment that needs changing, the other makes repeated status calls
// comparable.
func TestBlockedCollections_KeepsTheFirstSightingAndSorts(t *testing.T) {
	im := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: time.Second})
	im.logger = zap.NewNop()

	if got := im.BlockedCollections(); got != nil {
		t.Fatalf("nothing is blocked yet, got %+v", got)
	}

	im.noteGCBlocked(indexKey{"kb-b", 3}, 0.5, 1, 2)
	first := im.BlockedCollections()
	if len(first) != 1 || first[0].KBID != "kb-b" || first[0].VersionID != 3 {
		t.Fatalf("blocked = %+v, want one entry for kb-b v3", first)
	}
	seen := first[0].Since

	// A repeat sighting refreshes the numbers but NOT the age: refreshing the age
	// would make a long-standing problem look perpetually new.
	time.Sleep(2 * time.Millisecond)
	im.noteGCBlocked(indexKey{"kb-b", 3}, 0.7, 0, 2)
	again := im.BlockedCollections()
	if len(again) != 1 {
		t.Fatalf("a repeat sighting must not add an entry: %+v", again)
	}
	if !again[0].Since.Equal(seen) {
		t.Fatalf("since = %v, want the FIRST sighting %v", again[0].Since, seen)
	}
	if again[0].DeadShare != 0.7 || again[0].OthersServing != 0 {
		t.Fatalf("the numbers must refresh with the latest sighting: %+v", again[0])
	}

	// Deterministic order, so an alerting surface does not reshuffle between calls.
	im.noteGCBlocked(indexKey{"kb-a", 9}, 0.4, 1, 2)
	im.noteGCBlocked(indexKey{"kb-a", 2}, 0.4, 1, 2)
	got := im.BlockedCollections()
	want := []struct {
		kb string
		v  int64
	}{{"kb-a", 2}, {"kb-a", 9}, {"kb-b", 3}}
	if len(got) != len(want) {
		t.Fatalf("blocked = %+v, want %d entries", got, len(want))
	}
	for i, w := range want {
		if got[i].KBID != w.kb || got[i].VersionID != w.v {
			t.Fatalf("entry %d = %s v%d, want %s v%d", i, got[i].KBID, got[i].VersionID, w.kb, w.v)
		}
	}
}

// TestClearGCBlocked_ForgetsTheVersion: the report has to mean "stuck NOW". A
// version that was collected must stop being reported, or the signal becomes noise
// nobody can act on.
func TestClearGCBlocked_ForgetsTheVersion(t *testing.T) {
	im := NewIndexManager(IndexManagerConfig{LRUCapacity: 4, LoadWaitTimeout: time.Second})
	im.logger = zap.NewNop()

	im.noteGCBlocked(indexKey{"kb-1", 7}, 0.5, 1, 2)
	if got := im.BlockedCollections(); len(got) != 1 {
		t.Fatalf("precondition: the version should be recorded, got %+v", got)
	}
	im.clearGCBlocked(indexKey{"kb-1", 7})
	if got := im.BlockedCollections(); got != nil {
		t.Fatalf("after a successful collection the version must not still look stuck: %+v", got)
	}
}

// TestCollectCandidates_RecordsWhyItSkipped is the wiring: the skip path has to
// feed the report, not just the log. This is the "every replica is needed" case
// §8.6(d) insists must not be endured in silence.
func TestCollectCandidates_RecordsWhyItSkipped(t *testing.T) {
	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:            4,
		LoadWaitTimeout:        time.Second,
		IndexDataDir:           t.TempDir(),
		GCEnabled:              true,
		IndexServingReplicaMin: 3,
		NodeID:                 1,
	})
	im.logger = zap.NewNop()
	im.SetGCReplicaCounter(&gcCountingCounter{others: 2}, 1) // 2 < 3: no slack

	im.collectCandidates(context.Background(), []gcCandidate{{KBID: "kb-1", VersionID: 7, DeadShare: 0.5}})

	got := im.BlockedCollections()
	if len(got) != 1 {
		t.Fatalf("blocked = %+v, want the skipped version recorded", got)
	}
	if got[0].OthersServing != 2 || got[0].MinimumRequired != 3 || got[0].VersionID != 7 {
		t.Fatalf("record = %+v, want others=2 minimum=3 v7", got[0])
	}
}
