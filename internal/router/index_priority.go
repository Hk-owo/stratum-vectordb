package router

import (
	"math"
	"sync"
	"time"
)

// indexPriority tracks how recently each version has been QUERIED, so a version
// nobody asks for can hold a small index owner set while a version the cluster
// keeps reading gets a larger one.
//
// Three properties are deliberate, and each rules out a more obvious design:
//
//   - It is LOCAL and unsynchronised. There is no shared table, no reporting, no
//     consensus. Two stations may disagree about a version's tier, and that is
//     fine: an owner set computed at k=2 is a SUBSET of one computed at k=3, so a
//     disagreement costs one replica's worth of coverage rather than a
//     correctness problem (docs/index-owner-sharding-plan.md §1).
//   - It samples on the read path rather than scanning on a timer. A ticker
//     would touch every entry forever to keep a decay curve nobody is looking at;
//     sampling touches exactly the entries something asked about, and an entry
//     nothing asks about simply stops being updated — which is the signal.
//   - Every decision is HYSTERETIC. A score hovering at a threshold would flip k
//     on consecutive queries, and each flip changes which replicas are expected to
//     hold the index. Upgrade and downgrade therefore use different bars.
type indexPriority struct {
	halfLife time.Duration
	// up[i] is the score at which a version rises INTO tier i. up[0] is 0: every
	// version starts at the bottom tier.
	up []float64
	// down[i] is the score below which a version falls OUT of tier i. Always
	// strictly below up[i], which is what makes the boundary sticky.
	down []float64

	// cleanupAt is how many entries may exist before a sweep is worth doing.
	// Reaching it does not mean anything is wrong — it means the map has stopped
	// being able to be walked for free.
	cleanupAt int
	// ttl is how long an entry may sit at the bottom tier without being touched
	// before it is dropped. Dropping it is safe precisely because it is at the
	// bottom tier: its k would not change.
	ttl time.Duration

	now func() time.Time

	mu      sync.Mutex
	entries map[versionKey]priorityEntry
}

type versionKey struct {
	kbID      string
	versionID int64
}

type priorityEntry struct {
	score float64
	// tier is remembered rather than derived, because the hysteresis needs to
	// know which side of a boundary this version is coming from.
	tier       int
	lastUpdate time.Time
}

// DefaultPriorityTable is the tier ladder: a version needs a handful of recent
// queries to earn its first extra owner, and a few times that to earn the next.
//
// The numbers are a §10.4-style placeholder — geometric rather than linear
// because "how many queries in a half-life" spans orders of magnitude between a
// busy and a quiet deployment, and a linear ladder would put every version in one
// tier in either. To be calibrated against real traffic.
var DefaultPriorityTable = []float64{0, 4, 16}

// DefaultPriorityHalfLife is how long it takes a version's score to decay by
// half. Ten minutes is long enough that an ordinary gap between queries does not
// demote a version, and short enough that a version which genuinely stopped being
// read releases its extra owners within a working session.
//
// It is the default for router.Config.OwnerPriorityHalfLife — the station's
// -index-owner-priority-half-life flag takes this value unless told otherwise,
// and 0 there disables the ladder entirely.
const DefaultPriorityHalfLife = 10 * time.Minute

// priorityHysteresis is how far below an UP bar a score must fall to cross back
// down. 0.7 means a version that reached a tier at 4.0 keeps it until it decays
// under 2.8.
const priorityHysteresis = 0.7

// defaultPriorityTTLMultiple is how many half-lives an untouched bottom-tier
// entry is kept before a sweep may drop it.
const defaultPriorityTTLMultiple = 3

// newIndexPriority returns a table, or nil when halflife is not positive — nil
// meaning "every version gets the base k", which is the pre-priority behaviour.
func newIndexPriority(halfLife time.Duration, now func() time.Time) *indexPriority {
	if halfLife <= 0 {
		return nil
	}
	if now == nil {
		now = time.Now
	}
	up := append([]float64(nil), DefaultPriorityTable...)
	down := make([]float64, len(up))
	for i, bar := range up {
		down[i] = bar * priorityHysteresis
	}
	return &indexPriority{
		halfLife:  halfLife,
		up:        up,
		down:      down,
		cleanupAt: 4096,
		ttl:       time.Duration(defaultPriorityTTLMultiple) * halfLife,
		now:       now,
		entries:   make(map[versionKey]priorityEntry),
	}
}

// Observe records that one query asked for this version, and returns its tier.
//
// The score is an exponentially weighted count: decay what was there, then add
// one. A version queried steadily converges to (queries per half-life) rather
// than growing without bound, which is what makes the thresholds meaningful
// across deployments of very different sizes.
func (p *indexPriority) Observe(kbID string, versionID int64) int {
	if p == nil {
		return 0
	}
	now := p.now()
	key := versionKey{kbID, versionID}

	p.mu.Lock()
	defer p.mu.Unlock()

	entry := p.entries[key]
	elapsed := now.Sub(entry.lastUpdate)
	if !entry.lastUpdate.IsZero() && elapsed > 0 {
		entry.score *= math.Exp(-elapsed.Seconds() / p.halfLife.Seconds())
	}
	entry.score++
	entry.tier = p.tierFor(entry.score, entry.tier)
	entry.lastUpdate = now
	p.entries[key] = entry

	if len(p.entries) > p.cleanupAt {
		p.sweepLocked(now)
	}
	return entry.tier
}

// Tier reports a version's current tier without recording a query. It is what the
// routing decision reads, so a read path never mutates the table by accident.
func (p *indexPriority) Tier(kbID string, versionID int64) int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tierLocked(kbID, versionID, p.now())
}

// tierLocked returns the tier a version is at NOW, discounting the decay since it
// was last touched.
//
// The decay has to be applied on read as well as on write: a version that was
// hammered an hour ago and never asked for again must not keep its tier until the
// next query arrives. Without this, "hot" would mean "was once hot".
func (p *indexPriority) tierLocked(kbID string, versionID int64, now time.Time) int {
	entry, ok := p.entries[versionKey{kbID, versionID}]
	if !ok {
		return 0
	}
	elapsed := now.Sub(entry.lastUpdate)
	if elapsed <= 0 {
		return entry.tier
	}
	decayed := entry.score * math.Exp(-elapsed.Seconds()/p.halfLife.Seconds())
	return p.tierFor(decayed, entry.tier)
}

// tierFor moves a tier up or down from where it was, using different bars in each
// direction.
func (p *indexPriority) tierFor(score float64, from int) int {
	tier := from
	if tier < 0 {
		tier = 0
	}
	if tier >= len(p.up) {
		tier = len(p.up) - 1
	}
	// Climb while the score clears the next tier's UP bar.
	for tier < len(p.up)-1 && score >= p.up[tier+1] {
		tier++
	}
	// Fall while the score is under this tier's DOWN bar.
	for tier > 0 && score < p.down[tier] {
		tier--
	}
	return tier
}

// sweepLocked drops entries that are at the bottom tier AND untouched for a ttl.
//
// Both conditions are required. An entry at the bottom tier contributes nothing —
// dropping it cannot change any k — and an entry touched recently is evidence
// about traffic even if its score happens to be low. Together they mean a sweep
// can only forget things nothing would have asked about anyway.
func (p *indexPriority) sweepLocked(now time.Time) {
	for key, entry := range p.entries {
		if entry.tier == 0 && now.Sub(entry.lastUpdate) > p.ttl {
			delete(p.entries, key)
		}
	}
}

// Len reports how many versions are tracked, for tests and diagnostics.
func (p *indexPriority) Len() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}
