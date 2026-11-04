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
// data IS: an empty holder list is only "no peer answered",
// and freezing that would turn one quiet interval into a lasting negative.
func TestHoldersCacheRefusesToStoreEmptyAnswers(t *testing.T) {
	holders := NewHoldersCache(HoldersCacheConfig{})
	holders.Store("kb-1", 3, nil)
	holders.Store("kb-1", 3, []string{})

	if holders.Len() != 0 {
		t.Fatalf("an empty answer must not be stored, Len = %d", holders.Len())
	}
	if got := holders.Stats().Stored; got != 0 {
		t.Fatalf("Stored = %d, want nothing stored", got)
	}
}

// A nil cache is what an unwired node passes around: every method has to be
// safe so the resolver needs no branch.
func TestHoldersCacheNilIsInert(t *testing.T) {
	var holders *HoldersCache
	holders.Store("kb-1", 1, []string{"holder:7001"})
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

// The cache is written from the catch-up while the lookup runs on the apply
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
					holders.Store("kb-1", int64(j+1), []string{"holder:7001"})
				}
				_, _ = holders.Lookup("kb-1", int64(j))
			}
		}(i == 0)
	}
	wg.Wait()
}
