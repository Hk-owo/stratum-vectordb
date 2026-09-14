package plane

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// DefaultReclaimInterval is how often a node tries to compact its WAL. It is
// deliberately slow: reclaiming rewrites a file, and the changes it drops are only
// ever a storage saving — nothing waits on them being dropped. §10.4's numbers are
// calibration starting points, and this is one of them.
const DefaultReclaimInterval = 5 * time.Minute

// ChangeReclaimer is the storage-layer capability the loop drives:
// *LocalDataPlane implements it. Narrow on purpose — the loop only needs "try once".
type ChangeReclaimer interface {
	ReclaimChanges(ctx context.Context) (map[string]int64, error)
}

// WALReclaimerConfig configures a WALReclaimer.
type WALReclaimerConfig struct {
	// Reclaimer is the storage layer whose WAL should be compacted. Required.
	Reclaimer ChangeReclaimer

	// Interval overrides the reclaim period. Zero uses the default.
	Interval time.Duration

	// Logger is optional. Failures are logged at info rather than error: a
	// compaction that could not run leaves the log as it was, which is the state
	// every node starts in — not a fault.
	Logger *zap.Logger
}

// WALReclaimer periodically compacts the WAL, dropping the recorded changes that
// every replica already holds (Stratum_设计文档v13.md §7.5). It is a background
// loop for the same reason index builds are: reclaiming rewrites a file, and nothing
// in the apply path may block on it.
type WALReclaimer struct {
	reclaimer ChangeReclaimer
	interval  time.Duration
	logger    *zap.Logger
}

// NewWALReclaimer returns a reclaimer ready to run.
func NewWALReclaimer(cfg WALReclaimerConfig) *WALReclaimer {
	interval := cfg.Interval
	if interval <= 0 {
		interval = DefaultReclaimInterval
	}
	return &WALReclaimer{reclaimer: cfg.Reclaimer, interval: interval, logger: cfg.Logger}
}

// Run reclaims until ctx is done. A failed pass never stops the loop: it says
// nothing about the data, and the next pass tries again with fresh watermarks.
func (r *WALReclaimer) Run(ctx context.Context) {
	if r.reclaimer == nil {
		return
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.ReclaimOnce(ctx); err != nil && r.logger != nil && ctx.Err() == nil {
				r.logger.Info("plane: WAL reclaim pass did not complete; it will be retried", zap.Error(err))
			}
		}
	}
}

// ReclaimOnce runs one reclaim pass. A nil error covers both "something was
// reclaimed" and "there was nothing to reclaim" — from the caller's side those are
// the same outcome, and the watermarks actually used are logged rather than returned.
func (r *WALReclaimer) ReclaimOnce(ctx context.Context) error {
	if r.reclaimer == nil {
		return nil
	}
	used, err := r.reclaimer.ReclaimChanges(ctx)
	if err != nil {
		return err
	}
	if len(used) > 0 && r.logger != nil {
		fields := make([]zap.Field, 0, len(used))
		for kbID, watermark := range used {
			fields = append(fields, zap.Int64(kbID, watermark))
		}
		r.logger.Info("plane: reclaimed recorded changes", fields...)
	}
	return nil
}
