package plane

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// cleanupRetryAttempts bounds how many times a failed cleanup broadcast is
// retried before it is given up on. A replica that has refused this many times
// needs an operator or a GC pass, not a seventh attempt — and an unbounded
// queue would quietly become its own leak.
const cleanupRetryAttempts = 5

// cleanupRetryInterval is how long the retry loop waits between passes. A var
// so tests can shorten it.
var cleanupRetryInterval = 30 * time.Second

// cleanupTask is one version whose cleanup did not reach every replica.
type cleanupTask struct {
	kbID      string
	versionID int64
	attempts  int
}

// scheduleCleanupRetry remembers a cleanup that did not finish, so a later pass
// can try again. It is called after a broadcast or a local drop failed
// (Stratum_设计文档v13.md §10.6).
//
// The queue is keyed like the failure counters and is deliberately small: one
// entry per version, replaced rather than appended, so repeated failures for
// the same version cannot grow it.
func (d *LocalDataPlane) scheduleCleanupRetry(kbID string, versionID int64) {
	key := failureKey(kbID, versionID)
	d.cleanupMu.Lock()
	if d.cleanupQueue == nil {
		d.cleanupQueue = make(map[string]*cleanupTask)
	}
	if _, ok := d.cleanupQueue[key]; !ok {
		d.cleanupQueue[key] = &cleanupTask{kbID: kbID, versionID: versionID}
	}
	d.cleanupMu.Unlock()
}

// StartCleanupRetries runs the cleanup retry loop until ctx ends. The node
// assembly starts one; tests that exercise the queue call retryCleanups
// directly instead.
func (d *LocalDataPlane) StartCleanupRetries(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(cleanupRetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.retryCleanups(ctx)
			}
		}
	}()
}

// retryCleanups makes one pass over the failed cleanups: it retries each and
// drops the ones that have run out of attempts.
func (d *LocalDataPlane) retryCleanups(ctx context.Context) {
	d.cleanupMu.Lock()
	pending := make([]*cleanupTask, 0, len(d.cleanupQueue))
	for _, task := range d.cleanupQueue {
		pending = append(pending, task)
	}
	d.cleanupMu.Unlock()

	for _, task := range pending {
		if err := d.DropVersionData(ctx, task.kbID, task.versionID); err != nil {
			task.attempts++
			if task.attempts >= cleanupRetryAttempts {
				d.cleanupMu.Lock()
				delete(d.cleanupQueue, failureKey(task.kbID, task.versionID))
				d.cleanupMu.Unlock()
				d.logger.Error("plane: giving up on a version cleanup after repeated failures; orphaned data needs an operator",
					zap.String("kb_id", task.kbID), zap.Int64("version_id", task.versionID),
					zap.Int("attempts", task.attempts), zap.Error(err))
				continue
			}
			d.logger.Warn("plane: version cleanup will be retried",
				zap.String("kb_id", task.kbID), zap.Int64("version_id", task.versionID),
				zap.Int("attempts", task.attempts), zap.Error(err))
			continue
		}
		d.cleanupMu.Lock()
		delete(d.cleanupQueue, failureKey(task.kbID, task.versionID))
		d.cleanupMu.Unlock()
	}
}

// PendingCleanups reports how many cleanups are waiting to be retried, for
// diagnostics and tests.
func (d *LocalDataPlane) PendingCleanups() int {
	d.cleanupMu.Lock()
	defer d.cleanupMu.Unlock()
	return len(d.cleanupQueue)
}
