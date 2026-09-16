package plane

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// The holders cache mirrors the control leader's §7.13.4 aggregate, so the
// data-source lookup can answer "who can serve this version" when §8.5's
// announced-holder table misses — instead of falling through to the leader, which
// in a two-tier deployment exports no data at all
// (docs/data-source-holders-fallback-plan.md).
//
// The one hard constraint it is built around: that lookup runs on the Raft apply
// path (replaying history re-runs every CreateVersion through it), so reading the
// answer must be a pure map read and nothing here may dial. The cache is filled
// from the report RESPONSE instead — the heartbeat this node already sends to the
// leader every interval (internal/sync's DataVersionReporter), which is off the
// apply path. There is no refresh call to make: the answer comes back with the
// report.
const (
	// DefaultHoldersCacheTTL is how long one answer stays usable. Three report
	// intervals (DefaultDataVersionReportInterval = 5s): long enough to ride out
	// a heartbeat's jitter, short enough that a stale view cannot outlive the
	// aggregate it mirrors — that aggregate is soft state, scoped to the
	// leader's term, and rebuilt from scratch after an election.
	DefaultHoldersCacheTTL = 15 * time.Second

	// DefaultHoldersCacheLimit bounds the cache by knowledge base. Entries are
	// only kept for knowledge bases the leader actually answered about, so this is
	// a long-run backstop rather than a working set — like DataSourceRegistry's
	// small table, the answer is an optimisation: losing it costs one interval's
	// answer, never correctness.
	DefaultHoldersCacheLimit = 512
)

// HoldersCacheConfig wires a HoldersCache.
type HoldersCacheConfig struct {
	// TTL overrides DefaultHoldersCacheTTL.
	TTL time.Duration
	// Limit bounds the cache by knowledge base. <= 0 takes
	// DefaultHoldersCacheLimit.
	Limit int

	// Now overrides the clock, so tests can age an entry without sleeping.
	// Optional.
	Now func() time.Time

	// Logger receives the diagnostics. Optional.
	Logger *zap.Logger
}

// holdersKey identifies one cached question.
type holdersKey struct {
	kbID      string
	versionID int64
}

// holdersEntry is ONE successful answer for a knowledge base, plus the version
// it was fetched with.
//
// queriedVersion >= v is what makes the entry reusable for v: a node whose
// cursor reached queriedVersion holds every version below it too (the cursor is
// a contiguous prefix and every replica holds the whole data set), so
// P(queriedVersion) ⊆ P(v) is a valid — if narrower — source set for v. The
// reverse does NOT hold, which is why the entry never answers a question about
// a HIGHER version than the one it was fetched with: "these nodes reached 6" is
// no evidence about who reached 10.
type holdersEntry struct {
	queriedVersion int64
	addrs          []string
	fetchedAt      time.Time
}

// HoldersCache is a mirror of the control leader's §7.13.4 aggregate, plus the
// queue of questions it could not answer yet.
//
// The entries are a MIRROR of soft state. They expire on their own (the
// aggregate keeps changing as nodes report and restart), and an unknown answer
// is never stored, because "I could not find out" must not be frozen into
// "nobody has it" — the two lead to opposite actions.
type HoldersCache struct {
	ttl    time.Duration
	limit  int
	now    func() time.Time
	logger *zap.Logger

	mu      sync.RWMutex
	entries map[string]holdersEntry

	// Counters for the layer's observability. They are plain numbers under the
	// same lock as the map: the cache is small and the per-query cost of the lock
	// was already paid by the lookup.
	stats HoldersCacheStats
}

// HoldersCacheStats are the counters that make the layer visible.
//
// A knowledge base the leader said nothing about is simply absent from the cache
// and counted as a Miss: there is no separate "asked and got nothing" counter any
// more, because nothing here asks. Whether the leader is answering at all is
// visible in the response (internal/sync's report log has the counts).
type HoldersCacheStats struct {
	// Hit / Miss count lookups.
	Hit  uint64
	Miss uint64
	// Stored counts answers that entered the cache.
	Stored uint64
	// Expired counts entries dropped for age on lookup, and Evicted those
	// dropped past the limit — together they calibrate the TTL and limit.
	Expired uint64
	Evicted uint64
}

// NewHoldersCache returns an empty cache that refreshes through cfg.Client.
func NewHoldersCache(cfg HoldersCacheConfig) *HoldersCache {
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = DefaultHoldersCacheTTL
	}
	limit := cfg.Limit
	if limit <= 0 {
		limit = DefaultHoldersCacheLimit
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &HoldersCache{
		ttl:     ttl,
		limit:   limit,
		now:     now,
		logger:  logger,
		entries: make(map[string]holdersEntry),
	}
}

// Lookup answers "which nodes can serve versionID" from the last successful
// answer for that knowledge base.
//
// ok=false means "no usable entry": never fetched, expired, evicted, or fetched
// with a LOWER version than the one being asked about (a narrower node set is
// not evidence about a higher version). The returned slice is a copy: callers
// retry against it and must not be able to reach back into the entry.
//
// This is a pure map read and never dials anyone — it is on the Raft apply path.
func (c *HoldersCache) Lookup(kbID string, versionID int64) ([]string, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[kbID]
	if !ok {
		c.stats.Miss++
		return nil, false
	}
	// A higher version is not covered by a lower one, even a fresh one.
	if versionID > entry.queriedVersion {
		c.stats.Miss++
		return nil, false
	}
	if c.now().Sub(entry.fetchedAt) >= c.ttl {
		delete(c.entries, kbID)
		c.stats.Expired++
		c.stats.Miss++
		return nil, false
	}
	c.stats.Hit++
	return append([]string(nil), entry.addrs...), true
}

// Store records a successful, truthy answer together with the version it was
// fetched with.
//
// An empty list must NOT be stored: "no node I have heard from" is not a fact
// about where data lives, and caching it would let one quiet interval outlive
// the report that corrects it. A new answer replaces the previous one for that
// knowledge base — it is the answer to the most recent question, and keeping an
// older, higher-versioned entry would silently shrink the candidate set for
// later, lower questions.
func (c *HoldersCache) Store(kbID string, versionID int64, addrs []string) {
	if c == nil || len(addrs) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[kbID] = holdersEntry{
		queriedVersion: versionID,
		addrs:          append([]string(nil), addrs...),
		fetchedAt:      c.now(),
	}
	c.stats.Stored++
	for len(c.entries) > c.limit {
		c.evictOldestLocked()
	}
}

// StoreHolders records a whole response's worth of answers under one lock.
//
// through is the version each answer holds through — on the report response it is the
// chain tail. An entry with no usable version is skipped rather than stored at some
// default, because the direction matters: "these nodes reached 6" may answer a
// question about 4 (they hold that too) and must never answer one about 10.
func (c *HoldersCache) StoreHolders(holders map[string][]string, through map[string]int64) {
	if c == nil || len(holders) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for kbID, addrs := range holders {
		if len(addrs) == 0 {
			continue
		}
		versionID, ok := through[kbID]
		if !ok || versionID <= 0 {
			continue
		}
		c.entries[kbID] = holdersEntry{
			queriedVersion: versionID,
			addrs:          append([]string(nil), addrs...),
			fetchedAt:      c.now(),
		}
		c.stats.Stored++
	}
	for len(c.entries) > c.limit {
		c.evictOldestLocked()
	}
}

// evictOldestLocked drops the least recently fetched entry. Called with the
// lock held and only when the limit is exceeded, so the O(n) scan is amortised
// over a write that already happened rather than paid per lookup.
func (c *HoldersCache) evictOldestLocked() {
	var (
		oldestKB string
		oldest   time.Time
		found    bool
	)
	for kb, entry := range c.entries {
		if !found || entry.fetchedAt.Before(oldest) {
			oldestKB, oldest, found = kb, entry.fetchedAt, true
		}
	}
	if found {
		delete(c.entries, oldestKB)
		c.stats.Evicted++
	}
}

// Stats returns a snapshot of the counters. Diagnostics and tests.
func (c *HoldersCache) Stats() HoldersCacheStats {
	if c == nil {
		return HoldersCacheStats{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

// Len reports how many knowledge bases have a cached answer. Diagnostics and
// tests.
func (c *HoldersCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
