package router

import (
	"testing"
	"time"
)

// clock is a manually advanced clock, so decay can be observed without sleeping.
type clock struct{ at time.Time }

func newClock() *clock {
	return &clock{at: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)}
}

func (c *clock) Now() time.Time          { return c.at }
func (c *clock) Advance(d time.Duration) { c.at = c.at.Add(d) }

func newTestPriority() (*indexPriority, *clock) {
	c := newClock()
	return newIndexPriority(time.Minute, c.Now), c
}

// TestIndexPriority_NoHalfLifeMeansNoLadder: the zero value is the switch. A
// deployment that has not opted in must get exactly one tier, whatever the
// traffic.
func TestIndexPriority_NoHalfLifeMeansNoLadder(t *testing.T) {
	p := newIndexPriority(0, nil)
	if p != nil {
		t.Fatal("a zero half-life must produce no table at all, so the routing path never consults one")
	}
	// And the nil receiver is usable, because the routing code calls it
	// unconditionally.
	if got := p.Observe("kb-1", 1); got != 0 {
		t.Errorf("Observe on a nil table = %d, want 0", got)
	}
	if got := p.Tier("kb-1", 1); got != 0 {
		t.Errorf("Tier on a nil table = %d, want 0", got)
	}
	if got := p.Len(); got != 0 {
		t.Errorf("Len on a nil table = %d, want 0", got)
	}
}

// TestIndexPriority_ClimbsWithTraffic: a version asked about steadily earns
// rungs. The convergence value is 1/(1-e^(-Δt/τ)), so querying four times per
// half-life settles above the first bar (4) and once per half-life settles below
// it — which is what makes the ladder mean "queries per half-life" rather than
// "queries ever".
func TestIndexPriority_ClimbsWithTraffic(t *testing.T) {
	const kbID = "kb-1"
	const versionID = int64(7)

	t.Run("four queries per half-life earns a rung", func(t *testing.T) {
		p, c := newTestPriority()
		step := time.Minute / 4
		tier := 0
		for i := 0; i < 40; i++ {
			tier = p.Observe(kbID, versionID)
			c.Advance(step)
		}
		if tier < 1 {
			t.Fatalf("tier after 40 queries at 4/half-life = %d, want >= 1 (steady score ~4.5)", tier)
		}
	})

	t.Run("one query per half-life stays at the bottom", func(t *testing.T) {
		p, c := newTestPriority()
		tier := 0
		for i := 0; i < 40; i++ {
			tier = p.Observe(kbID, versionID)
			c.Advance(time.Minute)
		}
		if tier != 0 {
			t.Fatalf("tier after 40 queries at 1/half-life = %d, want 0 (steady score ~1.6)", tier)
		}
	})
}

// TestIndexPriority_DecaysOnReadNotOnWrite is the property that keeps "hot" from
// meaning "was once hot": a version that was hammered and then never asked for
// again must drop tiers as time passes, without needing another query to arrive.
func TestIndexPriority_DecaysOnReadNotOnWrite(t *testing.T) {
	p, c := newTestPriority()
	const kbID = "kb-1"
	const versionID = int64(7)

	// Drive it into the top tier.
	for i := 0; i < 40; i++ {
		p.Observe(kbID, versionID)
		c.Advance(time.Minute / 4)
	}
	if got := p.Tier(kbID, versionID); got == 0 {
		t.Fatalf("tier after heavy traffic = %d, want >= 1", got)
	}

	// Then stop querying entirely. Only the clock moves.
	c.Advance(10 * time.Minute)
	dropped := false
	for i := 0; i < 20; i++ {
		if p.Tier(kbID, versionID) == 0 {
			dropped = true
			break
		}
		c.Advance(time.Minute)
	}
	if !dropped {
		t.Fatal("a version nobody queries kept its tier; decay must be applied on read, not only on the next query")
	}
}

// TestIndexPriority_HysteresisHoldsTheBoundary: without it, a score sitting on a
// threshold would flip k on consecutive queries — and every flip changes which
// replicas are expected to hold the index, which is a far more expensive thing to
// oscillate than a counter.
func TestIndexPriority_HysteresisHoldsTheBoundary(t *testing.T) {
	p, c := newTestPriority()
	key := versionKey{"kb-1", 1}

	// lastUpdate is a second in the past, not "now": a zero elapsed time takes a
	// fast path that returns the remembered tier, and this test is about what the
	// decayed score implies.
	past := c.at.Add(-time.Second)

	// Sitting in tier 1 with a score just under the UP bar (4) but above the DOWN
	// bar (4*0.7 = 2.8): it must stay put rather than fall back.
	p.entries[key] = priorityEntry{score: 3.5, tier: 1, lastUpdate: past}
	if got := p.Tier("kb-1", 1); got != 1 {
		t.Fatalf("tier at score 3.5 = %d, want 1 — the DOWN bar is 2.8, not 4", got)
	}

	// And a version at tier 0 with the same score must NOT be promoted: the UP
	// bar is 4, and no amount of being close to it counts.
	p.entries[key] = priorityEntry{score: 3.5, tier: 0, lastUpdate: past}
	if got := p.Tier("kb-1", 1); got != 0 {
		t.Fatalf("tier at score 3.5 from below = %d, want 0 — the UP bar is 4", got)
	}

	// Below the DOWN bar it does fall.
	p.entries[key] = priorityEntry{score: 2.0, tier: 1, lastUpdate: past}
	if got := p.Tier("kb-1", 1); got != 0 {
		t.Fatalf("tier at score 2.0 = %d, want 0", got)
	}
}

// TestIndexPriority_TierCannotExceedTheLadder: a very hot version tops out at the
// last tier, and the caller caps k at the replica count from there.
func TestIndexPriority_TierCannotExceedTheLadder(t *testing.T) {
	p, c := newTestPriority()
	key := versionKey{"kb-1", 1}
	p.entries[key] = priorityEntry{score: 1e9, tier: len(p.up) - 1, lastUpdate: c.Now()}

	if got := p.Tier("kb-1", 1); got != len(p.up)-1 {
		t.Fatalf("tier at score 1e9 = %d, want %d (the top rung)", got, len(p.up)-1)
	}
}

// TestIndexPriority_SweepDropsOnlyColdBottomTierEntries: a sweep may only forget
// things nothing would have asked about anyway — bottom tier (so no k depends on
// it) AND untouched past the ttl (so it is not recent evidence).
func TestIndexPriority_SweepDropsOnlyColdBottomTierEntries(t *testing.T) {
	p, c := newTestPriority()

	// Cold bottom tier: droppable.
	p.entries[versionKey{"kb-1", 1}] = priorityEntry{score: 0.1, tier: 0, lastUpdate: c.at.Add(-10 * time.Minute)}
	// Recent bottom tier: kept, because its lowness may just be newness.
	p.entries[versionKey{"kb-1", 2}] = priorityEntry{score: 0.1, tier: 0, lastUpdate: c.Now()}
	// Hot: kept, obviously.
	p.entries[versionKey{"kb-1", 3}] = priorityEntry{score: 20, tier: 2, lastUpdate: c.at.Add(-10 * time.Minute)}

	p.sweepLocked(c.Now())

	if got := p.Len(); got != 2 {
		t.Fatalf("entries after sweep = %d, want 2 (only the cold bottom-tier one goes)", got)
	}
	if _, ok := p.entries[versionKey{"kb-1", 2}]; !ok {
		t.Error("a recently-touched bottom-tier entry was dropped; lowness may just mean newness")
	}
	if _, ok := p.entries[versionKey{"kb-1", 3}]; !ok {
		t.Error("a hot entry was dropped; dropping it would change its k")
	}
}

// TestIndexPriority_SweepIsTriggeredByGrowth: the sweep is lazy — it runs when the
// map has grown past a bound, not on a timer. A deployment with few versions pays
// nothing for it.
func TestIndexPriority_SweepIsTriggeredByGrowth(t *testing.T) {
	p, c := newTestPriority()
	p.cleanupAt = 8
	// One entry old enough to be swept, plus enough traffic to cross the bound.
	p.entries[versionKey{"kb-old", 1}] = priorityEntry{score: 0.1, tier: 0, lastUpdate: c.at.Add(-time.Hour)}

	for i := int64(0); i <= int64(p.cleanupAt); i++ {
		p.Observe("kb-1", i)
	}
	if _, ok := p.entries[versionKey{"kb-old", 1}]; ok {
		t.Fatal("crossing the size bound did not trigger a sweep")
	}
}

// TestOwnerKFor_PriorityRaisesKForHotVersions is the routing half: the tier has
// to actually reach the owner count, or the whole table is bookkeeping.
func TestOwnerKFor_PriorityRaisesKForHotVersions(t *testing.T) {
	c := newClock()
	r := shardedRouter(2)
	r.priority = newIndexPriority(time.Minute, c.Now)

	const kbID = "kb-1"
	const hot, cold = int64(7), int64(8)

	// Drive v7 hard; never touch v8.
	for i := 0; i < 40; i++ {
		r.priority.Observe(kbID, hot)
		c.Advance(time.Minute / 4)
	}

	hotK := r.ownerKFor(kbID, hot)
	coldK := r.ownerKFor(kbID, cold)
	if hotK <= coldK {
		t.Fatalf("k(hot) = %d, k(cold) = %d — a frequently queried version must keep more owners", hotK, coldK)
	}
	if coldK != r.ownerInactiveK {
		t.Fatalf("k(cold) = %d, want the base %d", coldK, r.ownerInactiveK)
	}
	if hotK > len(r.storageAddrs) {
		t.Fatalf("k(hot) = %d, want it capped at the replica count %d", hotK, len(r.storageAddrs))
	}
}

// TestOwnerKFor_ActiveVersionIgnoresPriority: the active version already owns the
// whole replica set, so a ladder can neither raise nor lower it. Getting this
// wrong would let a quiet active version be sharded.
func TestOwnerKFor_ActiveVersionIgnoresPriority(t *testing.T) {
	c := newClock()
	r := shardedRouter(2)
	r.priority = newIndexPriority(time.Minute, c.Now)

	// Version 5 is the routing table's expected (active) version, and it has never
	// been queried — the coldest thing in the table.
	if got := r.ownerKFor("kb-1", 5); got != 0 {
		t.Fatalf("k(active) = %d, want 0 (every member)", got)
	}
}

// TestOwnerKFor_NoPriorityTableIsJustTheBase: with the ladder off, k is the
// configured base for every non-active version, which is the behaviour before
// priorities existed.
func TestOwnerKFor_NoPriorityTableIsJustTheBase(t *testing.T) {
	r := shardedRouter(2)
	if r.priority != nil {
		t.Fatal("shardedRouter must not enable the ladder by default")
	}
	for _, v := range []int64{7, 8, 9} {
		if got := r.ownerKFor("kb-1", v); got != 2 {
			t.Fatalf("k(v%d) = %d, want the base 2", v, got)
		}
	}
}
