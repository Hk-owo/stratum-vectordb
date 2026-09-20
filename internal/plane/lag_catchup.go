package plane

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"go.uber.org/zap"
)

// LagCatchup turns the chain tails the control leader carries back on this node's
// periodic cursor report into an occasional background catch-up
// (docs/active-lag-detection-design.md).
//
// The trade it is built around:
//
//   - Recovery is lazy today. EnsureIndex builds when someone asks, so a node that
//     nobody routes to can sit behind indefinitely, and the first query that does
//     reach it pays a cold fetch plus a build.
//   - Catching up eagerly on every signal would trade that for a storm: a node back
//     from a long outage would start every knowledge base at once, and the leader's
//     next interval would have every returning node do it together.
//
// Two things are therefore deliberate. A signal is followed by a random delay
// (Config.Jitter) before anything starts, which spreads "all at once" into a window.
// And it bounds how many knowledge bases it works on at once (Config.MaxConcurrentKBs)
// — jitter alone is not an upper bound once the number of lagging knowledge bases
// exceeds what anyone expected.
//
// There is no switch, deliberately. Catching up on its own is the node's ordinary
// behaviour rather than an operator's opt-in: a replica that is behind and stays behind
// is one nothing ever routes to, so "leave it lazy" reads as "leave it unserviceable".
// What an operator still tunes is the PACE (the jitter window and the concurrency bound
// above), which is where the real risk lives — a returning node starting every
// knowledge base at once.
//
// It schedules no builds of its own: it calls Config.Ensure, the same path a query
// takes, so the existing gates (the per-KB write limit, the build pool's backfill
// lane, the push semaphore) still apply. This component only decides *when* to start.
type LagCatchup struct {
	cfg LagCatchupConfig

	mu       sync.Mutex
	inflight map[string]bool // knowledge bases currently catching up

	// tails is the latest chain-tail mirror the leader carried back, per knowledge
	// base. Catch-up itself does not need to remember it (every report repeats it),
	// but §8.6(d)'s tombstone scan does: "the versions with nothing after them" is
	// exactly this map, and reading it here costs no RPC that is not already made.
	tails map[string]int64
}

// storeTails keeps the latest mirror under the same lock as inflight. A copy, not
// the caller's map: the caller is the sync layer's response handler and may reuse or
// mutate what it decoded.
func (l *LagCatchup) storeTails(tails map[string]int64) {
	cp := make(map[string]int64, len(tails))
	for kbID, versionID := range tails {
		cp[kbID] = versionID
	}
	l.mu.Lock()
	l.tails = cp
	l.mu.Unlock()
}

// ChainTails returns the chain tails the leader last carried back, per knowledge
// base — the versions with nothing after them.
//
// It exists for §8.6(d)'s tombstone scan, which needs those versions and would
// otherwise have to ask the control layer again for a fact this node is handed every
// report interval. A copy, for the same reason storeTails copies: the caller must not
// be able to mutate the mirror, and the next report may replace it at any moment.
//
// An empty result means "not told yet", never "no chains" — the same reading the
// chain_tails field documents, and the reason a caller must not treat absence as a
// negative fact about a knowledge base.
func (l *LagCatchup) ChainTails() map[string]int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.tails) == 0 {
		return nil
	}
	cp := make(map[string]int64, len(l.tails))
	for kbID, versionID := range l.tails {
		cp[kbID] = versionID
	}
	return cp
}

// LagCatchupConfig wires a LagCatchup.
type LagCatchupConfig struct {
	// MinLagVersions is how far behind the tail a knowledge base must be before it
	// counts as left behind; 1 means "any gap". Raising it trades a slower reaction
	// for fewer wake-ups on ordinary write races, where a report can overlap the
	// version being committed.
	MinLagVersions int64

	// Jitter is the exclusive upper bound of the random delay between receiving a
	// signal and acting on it.
	Jitter time.Duration

	// MaxConcurrentKBs bounds how many knowledge bases may catch up at once. <= 0
	// takes DefaultMaxConcurrentLagCatchups.
	MaxConcurrentKBs int

	// Ensure brings one version to a queryable state on this node — the same call a
	// query makes, so a missing version's data is fetched before its index is built.
	// *LocalDataPlane's EnsureIndex satisfies it. Required.
	Ensure func(ctx context.Context, kbID string, versionID int64) error

	// Cursor reports this node's own contiguous cursor per knowledge base. Required.
	Cursor func() map[string]int64

	// Logger is optional.
	Logger *zap.Logger

	// now and sleep are injectable so tests can drive the timing.
	now   func() time.Time
	sleep func(time.Duration)
}

// DefaultMaxConcurrentLagCatchups bounds concurrent catch-ups when nothing is
// configured. Small on purpose: a catch-up is background warm-up, and the bound
// exists precisely so a returning node cannot start everything at once.
const DefaultMaxConcurrentLagCatchups = 2

// LagCatchupTimeout bounds one catch-up. Like the other §10.4 numbers it is a
// placeholder: a large knowledge base may legitimately take longer, and being wrong
// costs a retry on the next signal rather than a stuck version.
const LagCatchupTimeout = 5 * time.Minute

// NewLagCatchup returns a LagCatchup. It satisfies sync.ChainTailSink.
func NewLagCatchup(cfg LagCatchupConfig) *LagCatchup {
	if cfg.now == nil {
		cfg.now = time.Now
	}
	if cfg.sleep == nil {
		cfg.sleep = time.Sleep
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	return &LagCatchup{cfg: cfg, inflight: make(map[string]bool)}
}

// SetChainTails implements sync.ChainTailSink: it is handed the tails the leader
// carried back on this node's latest cursor report.
//
// A knowledge base is picked up when its own cursor is at least MinLagVersions behind
// the tail, it is not already catching up, and there is room under the concurrency
// bound. Anything skipped is simply reconsidered on the next report — the signal
// repeats every interval by construction, so nothing needs a queue of its own.
//
// NOTE: this used to iterate in a sorted order (furthest behind first). That was
// reverted. The change was made on the theory that a knowledge base could starve
// behind the concurrency bound, and "nothing was scheduled" was read as starvation —
// but the real reason nothing was scheduled was that the signal never arrived at all
// (the leader never saw a report; see the AdminService registration fix). So the
// ordering solved a problem that did not exist. The starvation theory is not
// refuted by that alone — with a signal that does arrive, map order really is
// random — but unchanged code with a known history beats a fix whose premise was
// false, and nothing measured the sorted version.
func (l *LagCatchup) SetChainTails(tails map[string]int64) {
	// Kept before any early return: a node that cannot catch up (no Ensure/Cursor) is
	// still a node whose §8.6(d) scan wants to know where the chain ends.
	l.storeTails(tails)

	if l.cfg.Ensure == nil || l.cfg.Cursor == nil {
		return
	}
	// Kept from the diagnosis that found the wiring bug, and worth keeping: "the
	// signal carried nothing" and "this component is not wired" must not look alike
	// in the log.
	if len(tails) == 0 {
		l.cfg.Logger.Debug("plane: lag catch-up: signal carried no chain tails")
		return
	}
	cursor := l.cfg.Cursor()
	maxKBs := l.cfg.MaxConcurrentKBs
	if maxKBs <= 0 {
		maxKBs = DefaultMaxConcurrentLagCatchups
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	started := 0
	for kbID, tail := range tails {
		if len(l.inflight) >= maxKBs {
			// Out of room: the rest wait for the next report, which is a few
			// seconds away by construction.
			return
		}
		if l.inflight[kbID] || tail <= 0 {
			continue
		}
		if cursor[kbID]+l.lagThreshold() > tail {
			continue // not behind enough to act on
		}
		l.inflight[kbID] = true
		started++
		go l.catchUp(kbID, tail)
	}
	if started == 0 {
		// Same reason as the empty-signal line above: "nothing was behind" and
		// "nothing ran" must be distinguishable.
		l.cfg.Logger.Debug("plane: lag catch-up: nothing behind the chain tail",
			zap.Int("tails_received", len(tails)))
	}
}

// lagThreshold is the smallest gap that counts as left behind. With the default of 1
// it makes "cursor 5, tail 5" not a lag while "cursor 5, tail 6" is.
func (l *LagCatchup) lagThreshold() int64 {
	if l.cfg.MinLagVersions <= 0 {
		return 1
	}
	return l.cfg.MinLagVersions
}

// catchUp is one knowledge base's catch-up: wait out the jitter, then ask the data
// plane to bring the tail's version to a queryable state.
func (l *LagCatchup) catchUp(kbID string, target int64) {
	defer func() {
		l.mu.Lock()
		delete(l.inflight, kbID)
		l.mu.Unlock()
	}()

	if delay := l.jitterFor(kbID); delay > 0 {
		l.cfg.sleep(delay)
	}
	ctx, cancel := context.WithTimeout(context.Background(), LagCatchupTimeout)
	defer cancel()
	if err := l.cfg.Ensure(ctx, kbID, target); err != nil {
		l.cfg.Logger.Warn("plane: lag catch-up failed; the version will be built on demand instead",
			zap.String("kb_id", kbID), zap.Int64("version_id", target), zap.Error(err))
		return
	}
	l.cfg.Logger.Info("plane: caught up with the chain tail",
		zap.String("kb_id", kbID), zap.Int64("version_id", target))
}

// jitterFor returns this signal's delay. The knowledge base id goes into the hash so
// two nodes behind on the same knowledge base do not land in the same slot, and the
// current time goes in so a knowledge base that keeps signalling does not keep
// re-rolling into an identical delay.
func (l *LagCatchup) jitterFor(kbID string) time.Duration {
	span := l.cfg.Jitter.Nanoseconds()
	if span <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(kbID))
	seed := h.Sum32() ^ uint32(l.cfg.now().UnixNano())
	return time.Duration(int64(seed) % span)
}
