package plane

import (
	"context"
	"hash/fnv"
	"sort"
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
// Three things are therefore deliberate. It is opt-in (Config.Enabled). A signal is
// followed by a random delay (Config.Jitter) before anything starts, which spreads
// "all at once" into a window. And it bounds how many knowledge bases it works on at
// once (Config.MaxConcurrentKBs) — jitter alone is not an upper bound once the number
// of lagging knowledge bases exceeds what anyone expected.
//
// It schedules no builds of its own: it calls Config.Ensure, the same path a query
// takes, so the existing gates (the per-KB write limit, the build pool's backfill
// lane, the push semaphore) still apply. This component only decides *when* to start.
type LagCatchup struct {
	cfg LagCatchupConfig

	mu       sync.Mutex
	inflight map[string]bool // knowledge bases currently catching up
}

// LagCatchupConfig wires a LagCatchup.
type LagCatchupConfig struct {
	// Enabled turns the whole mechanism on. False is the default, and means the node
	// keeps the lazy recovery it has always had.
	Enabled bool

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
// bound. Anything not picked up is reconsidered on the next report — the signal
// repeats every interval by construction, so nothing needs a queue of its own.
//
// When more knowledge bases are behind than the bound allows, the ones furthest back
// go first. Iterating the map and stopping at the bound would be wrong twice over: Go
// randomises map order, so which knowledge bases got through would change every
// report, and a knowledge base could starve indefinitely behind a crowd of others that
// are only marginally behind.
func (l *LagCatchup) SetChainTails(tails map[string]int64) {
	if !l.cfg.Enabled || l.cfg.Ensure == nil || l.cfg.Cursor == nil {
		return
	}
	if len(tails) == 0 {
		l.cfg.Logger.Debug("plane: lag catch-up: signal carried no chain tails")
		return
	}
	cursor := l.cfg.Cursor()
	maxKBs := l.cfg.MaxConcurrentKBs
	if maxKBs <= 0 {
		maxKBs = DefaultMaxConcurrentLagCatchups
	}

	type lag struct {
		kbID  string
		tail  int64
		gapV  int64
	}
	var behind []lag
	for kbID, tail := range tails {
		if l.inflight[kbID] || tail <= 0 {
			continue
		}
		gapV := tail - cursor[kbID]
		if gapV < l.lagThreshold() {
			continue // not behind enough to act on
		}
		behind = append(behind, lag{kbID: kbID, tail: tail, gapV: gapV})
	}
	if len(behind) == 0 {
		l.cfg.Logger.Debug("plane: lag catch-up: nothing behind the chain tail",
			zap.Int("tails_received", len(tails)))
		return
	}
	sort.Slice(behind, func(i, j int) bool { return behind[i].gapV > behind[j].gapV })

	l.mu.Lock()
	defer l.mu.Unlock()
	room := maxKBs - len(l.inflight)
	started := 0
	for _, b := range behind {
		if room <= 0 {
			break
		}
		if l.inflight[b.kbID] {
			continue
		}
		l.inflight[b.kbID] = true
		room--
		started++
		go l.catchUp(b.kbID, b.tail)
	}
	l.cfg.Logger.Debug("plane: lag catch-up: scheduled",
		zap.Int("tails_received", len(tails)), zap.Int("behind", len(behind)),
		zap.Int("started", started), zap.Int("inflight", len(l.inflight)))
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
