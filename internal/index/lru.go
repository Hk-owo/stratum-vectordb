// The tiered cache (§5.2): residency accounting, the per-entry lock, LRU order,
// access protection, and eviction with its reload path.
package index

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	vecstorepb "stratum/api/proto/vecstore"
	stratumerrors "stratum/internal/errors"
	"time"
)

// acquire loads the index for key if not already in memory, blocking
// if a concurrent load is in progress, bounded by loadWaitTimeout and
// ctx.Done(). Returns nil once the index is loaded and ref-counted.
func (im *IndexManagerImpl) acquire(ctx context.Context, key indexKey) error {
	deadline := time.Now().Add(im.cfg.LoadWaitTimeout)

	im.mu.Lock()
	defer im.mu.Unlock()

	for {
		if idx, ok := im.loaded[key]; ok {
			idx.refCount++
			idx.lastAccess = time.Now()
			return nil
		}
		if im.loading[key] {
			// A build/load is in progress for this key; wait.
			if !im.waitLocked(ctx, deadline) {
				return im.waitTimeoutErr(ctx)
			}
			continue
		}
		// Not loaded and not being built.
		return fmt.Errorf("index: no built index for kbID=%s versionID=%d: %w", key.kbID, key.versionID, stratumerrors.ErrIndexNotReady)
	}
}

// waitLocked blocks until the loading condition is re-checkable, ctx is
// done, or deadline passes, returning false in the latter two cases. Must
// be called with im.mu held.
//
// Deliberately NOT implemented with cond.Wait: a lost wakeup (e.g. the
// build goroutine dies between setting loading and broadcasting) would
// park the caller forever — exactly the pressure-test Query hang where a
// goroutine sat in sync.Cond.Wait for minutes. Polling with a bounded
// deadline is slightly less efficient but cannot wedge.
func (im *IndexManagerImpl) waitLocked(ctx context.Context, deadline time.Time) bool {
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return false
		}
		// Drop the lock briefly so the build goroutine (which needs
		// im.mu to clear loading) can make progress, then re-check.
		im.mu.Unlock()
		select {
		case <-ctx.Done():
			im.mu.Lock()
			return false
		case <-time.After(50 * time.Millisecond):
		}
		im.mu.Lock()
	}
	return ctx.Err() == nil && time.Now().Before(deadline)
}

func (im *IndexManagerImpl) waitTimeoutErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return stratumerrors.ErrIndexLoadTimeout
}

func (im *IndexManagerImpl) release(key indexKey) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if idx, ok := im.loaded[key]; ok && idx.refCount > 0 {
		idx.refCount--
		im.cond.Broadcast()
	}
}

// recordSearch stamps key's most recent query time (§8.6a). Called at
// the top of Search, before any load/build/brute-force decision, so a
// version counts as "asked for" even when the request is answered by a
// scan or fails afterwards. Keys deleted by Discard/DeleteFilesByKB are
// revived only by a later search, which is the intended semantic.
func (im *IndexManagerImpl) recordSearch(key indexKey) {
	im.recordAccess(key)
}

// RecordInterest implements IndexManager. An operator asked for this version's
// index explicitly (RebuildIndex / WarmupVersion), which is the same evidence a
// query leaves — "this version is wanted here, now" — so it is recorded the
// same way and the retention policy shields it for the same window.
//
// Without it, the artifact such a request builds is protected only for the
// instant it completes: the post-build retention pass is handed the id being
// built, but the next build, or the next restart, has no reason to keep it — and
// an operator's explicit request then quietly expires.
func (im *IndexManagerImpl) RecordInterest(kbID string, versionID int64) {
	im.recordAccess(indexKey{kbID, versionID})
}

// recordAccess is where "something asked for this version" lands: the in-memory
// access table the cold policy reads, and — throttled — the on-disk .used
// sidecar the retention policy reads.
func (im *IndexManagerImpl) recordAccess(key indexKey) {
	now := time.Now()
	im.mu.Lock()
	im.lastSearch[key] = now
	// Persist the access time, throttled. This on-disk record is what lets the
	// retention policy shield a recently-queried version across a restart; a
	// write on every query would put a small file write on the hot path, while
	// the protection window is measured in hours, so a minute of granularity
	// costs nothing.
	persist := false
	if im.retentionProtectWindow() > 0 && im.cfg.IndexDataDir != "" {
		if last, ok := im.lastPersist[key]; !ok || now.Sub(last) >= im.cfg.AccessPersistInterval {
			im.lastPersist[key] = now
			persist = true
		}
	}
	im.mu.Unlock()
	if persist {
		// Outside the lock: it is a file write, and Search must not wait on it.
		im.persistAccessTime(key.kbID, key.versionID, now)
	}
}

// recordInterestNow records "this version is wanted here" and writes the on-disk
// .used shield immediately, bypassing recordAccess's throttle.
//
// The build-complete and install paths use it. The artifact they just produced
// or received is exactly the one a lagging replica is about to fetch (§8.4), and
// the retention pass that would drop it runs on every later build — so the shield
// has to be on disk before the next one, not up to a minute later. It maintains
// only lastPersist (the throttle's own bookkeeping); lastSearch is the cold
// policy's baseline and seedAccessLocked already set it, where a real query
// outranks a build as evidence.
func (im *IndexManagerImpl) recordInterestNow(kbID string, versionID int64) {
	if im.retentionProtectWindow() <= 0 || im.cfg.IndexDataDir == "" {
		return
	}
	now := time.Now()
	key := indexKey{kbID, versionID}
	im.mu.Lock()
	im.lastPersist[key] = now
	im.mu.Unlock()
	// Outside the lock: it is a file write, and a build or install must not wait
	// on it.
	im.persistAccessTime(kbID, versionID, now)
}

// LastAccess reports when (kbID, versionID) was last searched (or, if it
// was never searched here, when its index was last built), and whether
// this process knows the version at all. It is the fact the cold-version
// policy reads; false means "neither searched nor built here", in which
// case the cold policy never sees the version and leaves it alone.
func (im *IndexManagerImpl) LastAccess(kbID string, versionID int64) (time.Time, bool) {
	im.mu.Lock()
	defer im.mu.Unlock()
	t, ok := im.lastSearch[indexKey{kbID, versionID}]
	return t, ok
}

// seedAccessLocked gives the cold policy a baseline for key when it has
// none. A version this node just built or just received counts as "known
// here from now on": without a record the version would be invisible to
// the policy (coldShapeFor reads this access table, not the Raft state
// machine), so a version nobody ever queries could never age into cold.
// An existing record is left alone — a real query is better evidence than
// a build, and re-seeding on every build would keep a version that is
// being rebuilt for other reasons permanently hot. Must be called with
// im.mu held.
func (im *IndexManagerImpl) seedAccessLocked(key indexKey) {
	if _, ok := im.lastSearch[key]; !ok {
		im.lastSearch[key] = time.Now()
	}
}

// forgetSearch drops key's access record. Must be called with im.mu held.
func (im *IndexManagerImpl) forgetSearchLocked(key indexKey) {
	delete(im.lastSearch, key)
}

// forgetVersionLocked drops everything the §8.6a policy remembers about
// key: its access record and the shape of its index. Used when the
// version itself goes away (Discard); eviction is not a reason to forget
// either fact. Must be called with im.mu held.
func (im *IndexManagerImpl) forgetVersionLocked(key indexKey) {
	im.forgetSearchLocked(key)
	delete(im.builtGraphFree, key)
	delete(im.lastPersist, key)
}

// makeRoomLocked evicts least-recently-used, ref-count-zero indexes until
// there is room for one more entry: len(loaded) < LRUCapacity AND (if
// MemoryThresholdMB is set) loadedBytes <= threshold. If no evictable
// index remains (everything is pinned), it stops and returns what it has. Must
// be called with im.mu held; called BEFORE inserting the new entry so the
// brand-new index (refCount 0, nothing has acquired it yet) is never
// itself chosen as the eviction candidate.
//
// It RETURNS the keys it dropped instead of reclaiming them itself: the vecstore
// holds its own copy of every resident index, and the call that releases it is a
// network round trip, so it belongs outside im.mu (see dropEvicted).
func (im *IndexManagerImpl) makeRoomLocked() []indexKey {
	var threshold int64
	if im.cfg.MemoryThresholdMB > 0 {
		threshold = im.cfg.MemoryThresholdMB << 20 // MiB → bytes
	}
	var evicted []indexKey
	for (im.cfg.LRUCapacity > 0 && len(im.loaded) >= im.cfg.LRUCapacity) ||
		(threshold > 0 && im.loadedBytes > threshold) {
		var oldestKey indexKey
		var oldestTime time.Time
		found := false
		for k, idx := range im.loaded {
			if idx.refCount != 0 {
				continue
			}
			if !found || idx.lastAccess.Before(oldestTime) {
				oldestKey = k
				oldestTime = idx.lastAccess
				found = true
			}
		}
		if !found {
			// Everything resident is pinned by a caller in flight, so the overage
			// cannot be corrected right now. Say so, once in a while: the state is
			// the one M8 of docs/code-review-2026-09-24.md describes — the bound is
			// exceeded and looks enforced — and the fix for "silent" is a line, not
			// a refusal (the overage is transient by construction: the pins belong
			// to searches that are about to finish).
			im.warnOnPinnedOverflowLocked(threshold)
			return evicted
		}
		delete(im.loaded, oldestKey)
		evicted = append(evicted, oldestKey)
		if size, ok := im.sizeByKey[oldestKey]; ok {
			im.loadedBytes -= size
			delete(im.sizeByKey, oldestKey)
		}
	}
	return evicted
}

// warnOnPinnedOverflowLocked records "over capacity and nothing is evictable",
// rate-limited to one line per minute.
//
// Must be called with im.mu held. The numbers are the diagnosis: how far over the
// bound the manager is (loaded vs LRUCapacity, loadedBytes vs threshold) and how
// many of the resident indexes are the reason nothing could go (pinned). Without
// them an operator sees memory climb past a configured ceiling with nothing in the
// log to say the ceiling was reached rather than misconfigured.
func (im *IndexManagerImpl) warnOnPinnedOverflowLocked(threshold int64) {
	now := time.Now()
	if !im.lastPinWarn.IsZero() && now.Sub(im.lastPinWarn) < pinnedOverflowWarnInterval {
		return
	}
	im.lastPinWarn = now

	pinned := 0
	for _, idx := range im.loaded {
		if idx.refCount != 0 {
			pinned++
		}
	}
	im.logger.Warn("index: over capacity and nothing is evictable; the overage lasts until the in-flight searches release",
		zap.Int("loaded", len(im.loaded)),
		zap.Int("lru_capacity", im.cfg.LRUCapacity),
		zap.Int("pinned", pinned),
		zap.Int64("loaded_bytes", im.loadedBytes),
		zap.Int64("memory_threshold_bytes", threshold))
}

// pinnedOverflowWarnInterval is how often the pinned-overflow state may be logged.
// Long enough that a burst of concurrent searches produces one line rather than
// hundreds, short enough that a manager stuck over its bound keeps saying so.
const pinnedOverflowWarnInterval = time.Minute

// dropEvicted tells the vecstore to release the indexes this node has stopped
// tracking.
//
// H5 of docs/code-review-2026-09-24.md: the vecstore's index map only ever grew,
// because the Go side's view (loaded / sizeByKey) was the only one being pruned
// and a pruned version's object stayed resident for the life of the process — so
// RSS tracked how many versions had been touched, not how many were held.
//
// Best-effort by construction. What a failure costs is memory, not correctness:
// the version is already gone from this node's view, its artifact is still on
// disk, and a later Load rebuilds the object. So a failure is logged and never
// propagated — failing the caller's build/eviction over a leak would trade a
// slow leak for an outage. The RPC is idempotent, so the next eviction of the
// same key retries harmlessly.
//
// Eviction and dropping are not atomic with respect to a concurrent load, and
// that gap is the whole reason for the two checks below. A version evicted here
// can be loaded again — by a query, or by the catch-up that follows a snapshot
// install — before this call runs, and dropping it then leaves this node
// believing it holds an index the vecstore no longer has. The symptom is not an
// error at the point of the bug: the node's own state says READY and the query
// answers "index not ready", which is exactly what
// TestRealStack_ThreeNodeCluster_SnapshotPipeline caught.
func (im *IndexManagerImpl) dropEvicted(keys []indexKey) {
	if len(keys) == 0 || im.vectorIndexClient == nil {
		return
	}
	// Its own bounded context: this runs off the back of a build or an eviction,
	// whose own deadlines may already be gone, and it must not be able to hang
	// that worker.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, key := range keys {
		if im.IsLoaded(key.kbID, key.versionID) {
			// Loaded again since the eviction: that object is the one this node is
			// using, so it is not garbage.
			continue
		}
		if _, err := im.vectorIndexClient.Drop(ctx, &vecstorepb.DropIndexRequest{
			KbId:      key.kbID,
			VersionId: key.versionID,
		}); err != nil {
			im.logger.Warn("index: vecstore Drop failed; its memory for this version stays resident",
				zap.String("kb_id", key.kbID), zap.Int64("version_id", key.versionID), zap.Error(err))
			continue
		}
		// The load may also have raced the other way: its Load RPC reached the
		// vecstore BEFORE this Drop, so the object it created is the one just
		// deleted. Load is idempotent and the artifact is on disk, so asking again
		// is what makes the two sides agree.
		if im.IsLoaded(key.kbID, key.versionID) {
			im.reloadAfterDrop(ctx, key)
		}
	}
}

// reloadAfterDrop re-materialises the vecstore-side object for a version that was
// loaded again while its Drop was in flight (see dropEvicted). Best-effort: the
// next query's load path retries it anyway, and the alternative — leaving the
// node serving from an object the vecstore has dropped — is worse.
func (im *IndexManagerImpl) reloadAfterDrop(ctx context.Context, key indexKey) {
	if im.cfg.IndexDataDir == "" {
		return
	}
	if _, err := im.vectorIndexClient.Load(ctx, &vecstorepb.LoadIndexRequest{
		KbId:      key.kbID,
		VersionId: key.versionID,
		Path:      im.indexPath(key.kbID, key.versionID),
	}); err != nil {
		im.logger.Warn("index: could not reload an index a concurrent load re-added during a Drop",
			zap.String("kb_id", key.kbID), zap.Int64("version_id", key.versionID), zap.Error(err))
	}
}

// Evict implements IndexManager.
func (im *IndexManagerImpl) Evict(_ context.Context, kbID string, versionID int64) error {
	im.mu.Lock()
	key := indexKey{kbID, versionID}
	var evicted []indexKey
	if _, ok := im.loaded[key]; ok {
		delete(im.loaded, key)
		evicted = append(evicted, key)
	}
	if size, ok := im.sizeByKey[key]; ok {
		im.loadedBytes -= size
		delete(im.sizeByKey, key)
	}
	im.mu.Unlock()

	// Same release every other eviction path performs (L6 of
	// docs/code-review-2026-10-18.md): the vecstore keeps its own copy of each
	// resident index, so dropping only this side's view leaves that copy resident for
	// the life of the process. EvictByKB and Discard already do this; this entry
	// point was the one H5 of the 09-24 review missed.
	im.dropEvicted(evicted)
	return nil
}

// EvictByKB implements IndexManager.
func (im *IndexManagerImpl) EvictByKB(_ context.Context, kbID string) error {
	im.mu.Lock()
	var dropped []indexKey
	for k := range im.loaded {
		if k.kbID == kbID {
			delete(im.loaded, k)
			dropped = append(dropped, k)
			if size, ok := im.sizeByKey[k]; ok {
				im.loadedBytes -= size
				delete(im.sizeByKey, k)
			}
		}
	}
	im.mu.Unlock()
	// The vecstore keeps its own object per resident index, so pruning only this
	// node's view left the whole knowledge base resident in the vecstore process
	// for the rest of its life (H5 of docs/code-review-2026-09-24.md). Done after
	// the unlock, because it is a network call.
	im.dropEvicted(dropped)
	return nil
}
