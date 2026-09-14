package index

import (
	"bufio"
	"context"
	"os"
	"time"

	"go.uber.org/zap"
)

// §8.6(d) 已封印索引的事后（GC）清理 —— 第 1 期：估算与触发判定。
//
// The problem this closes is narrower than "a version has tombstones". A version
// WITH a successor is already covered: the next append sees the dead share and
// falls back to a full rebuild (§8.6c). What nothing covers is a version that
// STAYS active, is queried all along, and never gets a successor — its dead
// share is frozen at build time, so it sits in memory forever, costing recall
// and footprint, with no mechanism that would ever look at it again.
//
// Phase 1 (this file) is the estimate and the trigger decision. It reports; it
// does not clean. That is deliberate: the cleanup (reopening a sealed artifact)
// takes the version out of service on this node for a moment, so where that is
// allowed to happen is a design question of its own — see §8.6(d) for the
// rolling scheme (one replica at a time) and the preconditions it needs.
//
// Why the estimate cannot reuse the append path's numbers: appendBase() computes
// the dead share only while attempting an append. A version with no successor
// never attempts one, so nothing has ever measured it. Hence the low-frequency
// scan below, which asks the question directly: how many vectors does the
// artifact hold, and how many does the current document set justify?

const (
	// DefaultGCSweepInterval is how often the scanner re-estimates. Slow on
	// purpose: the thing it looks for only changes when documents are deleted,
	// and the action it feeds (reopening an artifact) is expensive enough that
	// acting on a stale estimate is worse than acting late. §8.6(d) explicitly
	// allows this to be imprecise.
	DefaultGCSweepInterval = 10 * time.Minute

	// DefaultGCRatioThreshold is the dead share above which an active version is
	// reported as a cleanup candidate. It matches DefaultAppendMaxDeadRatio:
	// both say "this much dead weight is not worth carrying", from two different
	// vantage points (may a build start from it, vs is a sealed artifact worth
	// reopening).
	DefaultGCRatioThreshold = 0.2

	// chunkIDLength is the length of a content-addressed chunk id: SHA-256 in
	// hex (internal/splitter), which is what the index sidecar records.
	chunkIDLength = 64
)

// SetActiveVersionsProvider wires the lookup §8.6(d) needs: which version is each
// knowledge base serving right now.
//
// Only active versions are scanned, for two reasons given in §8.6(d): a version
// with a successor is the append path's business, and a historical version is
// queried too rarely for the cleanup to be worth its service interruption.
func (im *IndexManagerImpl) SetActiveVersionsProvider(fn func(ctx context.Context) (map[string]int64, error)) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.activeVersions = fn
}

// StartGCScanner starts the background scan. Like the §8.6a evaluator and the
// §8.8 artifact sweeper, it is local and passive: it reads this node's own disk
// and reports, taking no part in consensus and talking to no peer.
//
// A negative GCSweepInterval disables it; <= 0 takes the default. It is a no-op
// when the active-version provider is unwired (there is nothing to scan) or when
// persistence is unconfigured (there is no artifact).
func (im *IndexManagerImpl) StartGCScanner() {
	if im.cfg.GCSweepInterval < 0 || im.cfg.IndexDataDir == "" {
		return
	}
	im.mu.Lock()
	if im.activeVersions == nil {
		im.mu.Unlock()
		return
	}
	if im.gcCancel != nil {
		im.mu.Unlock()
		return // already running
	}
	ctx, cancel := context.WithCancel(context.Background())
	im.gcCancel = cancel
	im.mu.Unlock()

	interval := im.cfg.GCSweepInterval
	if interval <= 0 {
		interval = DefaultGCSweepInterval
	}
	im.gcWG.Add(1)
	go func() {
		defer im.gcWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Scan first, collect second: the scan is what turns "this artifact
				// carries more dead weight than we tolerate" into candidates, and
				// the collection is what acts — only when it is enabled and only
				// after the control layer says enough replicas would remain serving
				// (§8.6(d)).
				im.collectCandidates(ctx, im.scanGCCandidates(ctx))
			}
		}
	}()
}

// StopGCScanner stops the scanner and waits for the running scan to return.
// Safe to call when it was never started.
func (im *IndexManagerImpl) StopGCScanner() {
	im.mu.Lock()
	cancel := im.gcCancel
	im.gcCancel = nil
	im.mu.Unlock()
	if cancel != nil {
		cancel()
		im.gcWG.Wait()
	}
}

// gcCandidate is one active version whose artifact carries more dead weight than
// the threshold allows. Phase 1 (§8.6(d)) only reports these; the cleanup itself
// is phase 2 (graph-free: reopen, remove, reseal) and phase 3 (graphed: a full
// rebuild), both of which need the rolling procedure the design describes.
type gcCandidate struct {
	KBID      string
	VersionID int64
	// DeadShare is the estimated share of the artifact's vectors the current
	// document set does not justify.
	DeadShare float64
}

// scanGCCandidates reports the active versions whose artifacts carry more dead
// weight than the threshold.
//
// Nothing is reported to the control layer: the version's state there is about
// data and index readiness, and "this node's artifact is carrying weight the
// document set no longer justifies" is a local, disposable observation — the
// same reason §7.13.5's health view stays inside the node.
func (im *IndexManagerImpl) scanGCCandidates(ctx context.Context) []gcCandidate {
	im.mu.Lock()
	provider := im.activeVersions
	im.mu.Unlock()
	if provider == nil || im.listDocIDs == nil || im.listChunkIDsByDocs == nil || im.cfg.IndexDataDir == "" {
		return nil
	}
	threshold := im.cfg.GCRatioThreshold
	if threshold <= 0 {
		threshold = DefaultGCRatioThreshold
	}

	active, err := provider(ctx)
	if err != nil {
		im.logger.Warn("index: gc scan could not read the active versions", zap.Error(err))
		return nil
	}
	// One line per scan, because the two ways this can find nothing — "the control
	// layer reports no active version" and "the candidate judgement rejected them
	// all" — look identical from the outside (nothing gets collected either way) and
	// have entirely different causes. One line per scan is affordable: production
	// scans every few minutes, and a test that shortens that to seconds is exactly
	// when you want to see this.
	im.logger.Info("index: gc scan read the active versions",
		zap.Int("active_versions", len(active)), zap.Float64("threshold", threshold))
	var candidates []gcCandidate
	for kbID, versionID := range active {
		// A SEALED artifact is the prerequisite: the unsealed remains of a dead
		// build are §8.8's business, and this node may not hold the version at
		// all (the active version is served by whoever was picked for it).
		if !fileExists(im.indexPath(kbID, versionID)) || !fileExists(im.sidecarPath(kbID, versionID)) {
			continue
		}
		share, ok := im.deadShare(ctx, kbID, versionID)
		if !ok || share <= threshold {
			continue
		}
		candidates = append(candidates, gcCandidate{KBID: kbID, VersionID: versionID, DeadShare: share})
	}
	for _, c := range candidates {
		im.logger.Info("index: an active version carries dead index weight and is a §8.6(d) cleanup candidate",
			zap.String("kb_id", c.KBID), zap.Int64("version_id", c.VersionID),
			zap.Float64("dead_share", c.DeadShare), zap.Float64("threshold", threshold))
	}
	return candidates
}

// deadShare estimates the share of an artifact's vectors that the version's
// current document set no longer justifies.
//
// It is an estimate, and allowed to be one: the artifact's size comes from the
// sealed sidecar, the live set from the version's current documents. Counting a
// chunk once however many live documents share it is correct for the question
// being asked, and a chunk whose vector came along from an ancestor is
// indistinguishable here from one whose document was deleted in this version —
// both are weight the current document set does not justify.
//
// ok=false means "cannot tell": no artifact, an unreadable sidecar, or a failed
// document lookup. A scan that cannot tell reports nothing rather than guessing,
// because the action downstream (reopening the artifact) is not free.
func (im *IndexManagerImpl) deadShare(ctx context.Context, kbID string, versionID int64) (float64, bool) {
	total, err := im.artifactChunkCount(kbID, versionID)
	if err != nil || total == 0 {
		return 0, false
	}
	docIDs, err := im.listDocIDs(ctx, kbID, versionID)
	if err != nil {
		return 0, false
	}
	live := 0
	if len(docIDs) > 0 {
		chunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, docIDs)
		if err != nil {
			return 0, false
		}
		distinct := make(map[string]struct{}, len(chunkIDs))
		for _, id := range chunkIDs {
			distinct[id] = struct{}{}
		}
		live = len(distinct)
	}
	dead := total - live
	if dead < 0 {
		// The sidecar does not cover every live chunk. Treating that as "no dead
		// weight" keeps the scan from reporting an artifact it does not
		// understand, which is the conservative direction: nothing gets cleaned.
		return 0, false
	}
	return float64(dead) / float64(total), true
}

// artifactChunkCount counts the chunk ids recorded in a version's sealed
// sidecar (.index.ids, written by vecstore's Save).
//
// The count is taken by recognising chunk-id lines rather than by skipping a
// fixed number of header lines: the header is the C++ writer's format (magic,
// dimension, metric, checksum) and has changed once already, while a chunk id
// has been "SHA-256 in hex" throughout. Recognising it keeps this side from
// breaking silently when the header grows a field.
func (im *IndexManagerImpl) artifactChunkCount(kbID string, versionID int64) (int, error) {
	chunkIDs, err := im.artifactChunkIDs(kbID, versionID)
	return len(chunkIDs), err
}

// artifactChunkIDs reads the chunk ids recorded in a version's sealed sidecar
// (.index.ids, written by vecstore's Save).
//
// The ids are recognised by SHAPE rather than by skipping a fixed number of
// header lines: the header is the C++ writer's format (magic, dimension, metric,
// checksum) and has changed once already, while a chunk id has been "SHA-256 in
// hex" throughout. Recognising them keeps this side from breaking silently when
// the header grows a field — and the caller needs the ids themselves, not just a
// count, to know WHICH vectors to drop.
func (im *IndexManagerImpl) artifactChunkIDs(kbID string, versionID int64) ([]string, error) {
	f, err := os.Open(im.sidecarPath(kbID, versionID))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, chunkIDLength+2), 4096)
	var ids []string
	for scanner.Scan() {
		if line := scanner.Text(); looksLikeChunkID(line) {
			ids = append(ids, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

// deadChunksOf returns the chunk ids the artifact holds that the version's
// current document set no longer justifies — the vectors a §8.6(d) collection
// would drop.
//
// The same set difference the estimate uses (artifact minus live), now materialised
// as ids because that is what RemoveChunks takes.
func (im *IndexManagerImpl) deadChunksOf(ctx context.Context, kbID string, versionID int64) ([]string, error) {
	artifact, err := im.artifactChunkIDs(kbID, versionID)
	if err != nil {
		return nil, err
	}
	docIDs, err := im.listDocIDs(ctx, kbID, versionID)
	if err != nil {
		return nil, err
	}
	live := make(map[string]struct{}, len(docIDs))
	if len(docIDs) > 0 {
		chunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, docIDs)
		if err != nil {
			return nil, err
		}
		for _, id := range chunkIDs {
			live[id] = struct{}{}
		}
	}
	var dead []string
	for _, id := range artifact {
		if _, ok := live[id]; !ok {
			dead = append(dead, id)
		}
	}
	return dead, nil
}

// looksLikeChunkID reports whether a sidecar line is a chunk id: SHA-256 hex,
// lowercase. The header's lines are a magic string and small decimal integers,
// so no header line matches.
func looksLikeChunkID(line string) bool {
	if len(line) != chunkIDLength {
		return false
	}
	for i := 0; i < len(line); i++ {
		c := line[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
