package plane

import (
	"sync"
	"testing"
	"time"
)

// The mirror must not outlive the aggregate it copies: that aggregate is soft
// state, scoped to a leadership term, and rebuilt from nothing after an
// election.
func TestHoldersCacheExpiresEntries(t *testing.T) {
	now := time.Now()
	holders := NewHoldersCache(HoldersCacheConfig{
		TTL: time.Second,
		Now: func() time.Time { return now },
	})
	holders.Store("kb-1", 3, []string{"holder:7001"})

	now = now.Add(999 * time.Millisecond)
	if _, ok := holders.Lookup("kb-1", 3); !ok {
		t.Fatal("an entry inside its TTL must still answer")
	}

	now = now.Add(2 * time.Second)
	if _, ok := holders.Lookup("kb-1", 3); ok {
		t.Fatal("an expired entry must not answer")
	}
	if holders.Len() != 0 {
		t.Fatalf("the expired entry should have been dropped, Len = %d", holders.Len())
	}
	if got := holders.Stats().Expired; got != 1 {
		t.Fatalf("Expired = %d, want 1", got)
	}
}

// Every miss is a question about the MOST RECENT answer: keeping an older,
// higher-versioned entry would silently shrink the candidate set for later
// higher questions.
func TestHoldersCacheReplacesOlderAnswer(t *testing.T) {
	holders := NewHoldersCache(HoldersCacheConfig{})
	holders.Store("kb-1", 10, []string{"holder-a:7001"})
	holders.Store("kb-1", 6, []string{"holder-b:7002"})

	if addrs, ok := holders.Lookup("kb-1", 6); !ok || len(addrs) != 1 || addrs[0] != "holder-b:7002" {
		t.Fatalf("Lookup(6) = (%v, %v), want the newest answer", addrs, ok)
	}
	if _, ok := holders.Lookup("kb-1", 10); ok {
		t.Fatal("the replaced entry must not answer a question it no longer describes")
	}
}

func TestHoldersCacheBoundsEntriesAndEvictsTheOldest(t *testing.T) {
	now := time.Now()
	holders := NewHoldersCache(HoldersCacheConfig{
		Limit: 2,
		Now:   func() time.Time { return now },
	})
	holders.Store("kb-1", 1, []string{"holder:7001"})
	now = now.Add(time.Second)
	holders.Store("kb-2", 1, []string{"holder:7002"})
	now = now.Add(time.Second)
	holders.Store("kb-3", 1, []string{"holder:7003"})

	if holders.Len() != 2 {
		t.Fatalf("Len = %d, want the limit of 2", holders.Len())
	}
	if _, ok := holders.Lookup("kb-1", 1); ok {
		t.Fatal("the oldest entry must be the one evicted")
	}
	if _, ok := holders.Lookup("kb-3", 1); !ok {
		t.Fatal("the newest entry must survive")
	}
	if got := holders.Stats().Evicted; got != 1 {
		t.Fatalf("Evicted = %d, want 1", got)
	}
}

// Lookup hands out a copy: callers keep the list to retry against, and must not
// be able to reach back into the entry.
func TestHoldersCacheLookupReturnsACopy(t *testing.T) {
	holders := NewHoldersCache(HoldersCacheConfig{})
	holders.Store("kb-1", 3, []string{"holder-a:7001"})

	addrs, ok := holders.Lookup("kb-1", 3)
	if !ok {
		t.Fatal("expected a hit")
	}
	addrs[0] = "tampered:7009"

	again, _ := holders.Lookup("kb-1", 3)
	if again[0] != "holder-a:7001" {
		t.Fatalf("Lookup = %q after the caller mutated its copy, want the stored address", again[0])
	}
}

// "Nobody holds it" must not enter the cache as if it were a fact about where the
// data IS: an empty holder list is the leader saying "nobody I have heard from",
// and freezing that would turn one quiet interval into a lasting negative.
func TestHoldersCacheRefusesToStoreEmptyAnswers(t *testing.T) {
	holders := NewHoldersCache(HoldersCacheConfig{})
	holders.Store("kb-1", 3, nil)
	holders.Store("kb-1", 3, []string{})
	holders.StoreHolders(map[string][]string{"kb-1": nil, "kb-2": {}}, map[string]int64{"kb-1": 3, "kb-2": 3})

	if holders.Len() != 0 {
		t.Fatalf("an empty answer must not be stored, Len = %d", holders.Len())
	}
	if got := holders.Stats().Stored; got != 0 {
		t.Fatalf("Stored = %d, want nothing stored", got)
	}
}

// The response's holders are keyed by knowledge base and are good THROUGH the
// version named alongside them (on the report response, the chain tail). An entry
// with no usable version has no defined direction, so it must not enter the cache:
// storing it at some default would let "these nodes reached 6" answer a question
// about 10 — the one direction that is not valid.
func TestHoldersCacheStoreHoldersNeedsAVersion(t *testing.T) {
	holders := NewHoldersCache(HoldersCacheConfig{})

	holders.StoreHolders(
		map[string][]string{
			"kb-1": {"holder:7001"},
			"kb-2": {"holder:7002"}, // no version alongside it
			"kb-3": {"holder:7003"}, // a version that is not usable
			"kb-4": {"holder:7004"}, // usable
		},
		map[string]int64{"kb-1": 6, "kb-3": 0, "kb-4": 9},
	)

	if _, ok := holders.Lookup("kb-1", 6); !ok {
		t.Fatal("an answer with a version must be usable for that version")
	}
	if _, ok := holders.Lookup("kb-1", 4); !ok {
		t.Fatal("a node holding 6 holds 4 too, so the entry must answer lower asks")
	}
	if _, ok := holders.Lookup("kb-1", 10); ok {
		t.Fatal("a node that reached 6 says nothing about 10")
	}
	if _, ok := holders.Lookup("kb-2", 3); ok {
		t.Fatal("an answer with no version must not be stored at all")
	}
	if _, ok := holders.Lookup("kb-3", 3); ok {
		t.Fatal("an answer with a non-positive version must not be stored at all")
	}
	if _, ok := holders.Lookup("kb-4", 9); !ok {
		t.Fatal("the entry that did have a version must be there")
	}
	if holders.Len() != 2 {
		t.Fatalf("Len = %d, want only the entries that had a usable version", holders.Len())
	}
}

// A nil cache is what an unwired node passes around: every method has to be
// safe so the resolver needs no branch.
func TestHoldersCacheNilIsInert(t *testing.T) {
	var holders *HoldersCache
	holders.Store("kb-1", 1, []string{"holder:7001"})
	holders.StoreHolders(map[string][]string{"kb-1": {"holder:7001"}}, map[string]int64{"kb-1": 1})
	if _, ok := holders.Lookup("kb-1", 1); ok {
		t.Fatal("a nil cache must not answer")
	}
	if holders.Len() != 0 {
		t.Fatalf("a nil cache must stay empty, Len = %d", holders.Len())
	}
	if got := holders.Stats(); got != (HoldersCacheStats{}) {
		t.Fatalf("Stats = %+v, want the zero value", got)
	}
}

// The cache is written from the report loop while the lookup runs on the apply
// path, so the two have to be able to overlap.
func TestHoldersCacheIsConcurrencySafe(t *testing.T) {
	holders := NewHoldersCache(HoldersCacheConfig{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(writer bool) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if writer {
					holders.StoreHolders(
						map[string][]string{"kb-1": {"holder:7001"}},
						map[string]int64{"kb-1": int64(j + 1)},
					)
				}
				_, _ = holders.Lookup("kb-1", int64(j))
			}
		}(i == 0)
	}
	wg.Wait()
}
