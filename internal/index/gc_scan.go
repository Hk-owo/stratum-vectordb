package index

import (
	"bufio"
	"context"
	"os"
	"strconv"
	"strings"
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

// SetActiveVersionsProvider wires one of §8.6(d)'s two target sources: which version
// is each knowledge base serving right now.
//
// Active versions are scanned because a historical version is queried too rarely for
// the cleanup to be worth its service interruption, and because a version WITH a
// successor is the append path's business. The other half of that reasoning — a
// version with no successor — is what SetChainTailVersionsProvider supplies, and it
// is not optional in practice: CreateVersion does not move the active pointer.
func (im *IndexManagerImpl) SetActiveVersionsProvider(fn func(ctx context.Context) (map[string]int64, error)) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.activeVersions = fn
}

// SetChainTailVersionsProvider wires §8.6(d)'s second target source: the version with
// nothing after it, per knowledge base.
//
// Without it the scan has a hole that is not hypothetical. CreateVersion does not move
// the active pointer (only RollbackVersion does), so on a knowledge base that was never
// rolled back the active map is EMPTY — the scan examines nothing at all — while the
// tail keeps whatever tombstones its writes left. The tail is also the only version
// that can be named for a knowledge base nobody has served yet, and it is the version
// the append path can never reclaim (there is no append to trigger a rebuild).
//
// Optional: unwired means "active versions only". A storage node can wire it from the
// chain-tail mirror its cursor report already brings back (the same signal §7.5's
// catch-up reads), so this needs no new RPC.
//
// An absent knowledge base means "the control layer said nothing about it", never
// "it has no tail" — the caller decides whether absence is worth a fallback.
func (im *IndexManagerImpl) SetChainTailVersionsProvider(fn func(ctx context.Context) (map[string]int64, error)) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.chainTailVersions = fn
}

// SetVersionsProvider wires the authoritative version set the §8.6a cold
// evaluator enumerates: per knowledge base, which versions exist, from the
// control layer's metadata.
//
// The evaluator needs this rather than the local access table because the table
// is what this process happens to have seen — it is empty after a restart, and
// an evaluator that walks it would therefore forget every version at exactly
// the moment it has the most reshaping to do. The table keeps its own job:
// supplying each version's last-query timestamp.
func (im *IndexManagerImpl) SetVersionsProvider(fn func(ctx context.Context) (map[string][]int64, error)) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.coldVersions = fn
}

// StartGCScanner starts the background scan. Like the §8.6a evaluator and the
// §8.8 artifact sweeper, it is local and passive: it reads this node's own disk
// and reports, taking no part in consensus and talking to no peer.
//
// A negative GCSweepInterval disables it; <= 0 takes the default. It is a no-op
// when BOTH target providers are unwired (there is nothing to scan) or when
// persistence is unconfigured (there is no artifact).
func (im *IndexManagerImpl) StartGCScanner() {
	if im.cfg.GCSweepInterval < 0 || im.cfg.IndexDataDir == "" {
		return
	}
	im.mu.Lock()
	if im.activeVersions == nil && im.chainTailVersions == nil {
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

// gcTarget is one (knowledge base, version) worth examining. A pair rather than two
// separate maps, so that a version named by both sources (the served version that is
// also the tail — the ordinary case for a fresh write) is examined ONCE: examining it
// twice would spend a second document scan and, worse, count it twice in the
// rejection tallies that exist to explain an empty scan.
type gcTarget struct {
	kbID      string
	versionID int64
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
	tailProvider := im.chainTailVersions
	im.mu.Unlock()
	if (provider == nil && tailProvider == nil) || im.listDocIDs == nil || im.listChunkIDsByDocs == nil || im.cfg.IndexDataDir == "" {
		return nil
	}
	threshold := im.cfg.GCRatioThreshold
	if threshold <= 0 {
		threshold = DefaultGCRatioThreshold
	}

	// Two sources, one target set (see gcTarget for why the pair is the key).
	targets := make(map[gcTarget]struct{})
	var activeCount, tailCount int
	if provider != nil {
		active, err := provider(ctx)
		if err != nil {
			// With no active set there is nothing authoritative to scan, and treating
			// "I could not read it" as "there is nothing" would silently drop every
			// served version: skip the scan and try next time.
			im.logger.Warn("index: gc scan could not read the active versions", zap.Error(err))
			return nil
		}
		activeCount = len(active)
		for kbID, versionID := range active {
			if versionID > 0 {
				targets[gcTarget{kbID: kbID, versionID: versionID}] = struct{}{}
			}
		}
	}
	if tailProvider != nil {
		tails, err := tailProvider(ctx)
		if err != nil {
			// Only the tails are lost, so this scan still has the active versions to
			// work with: report and carry on rather than dropping a scan that can
			// still find something.
			im.logger.Warn("index: gc scan could not read the chain tails", zap.Error(err))
		} else {
			tailCount = len(tails)
			for kbID, versionID := range tails {
				if versionID > 0 {
					targets[gcTarget{kbID: kbID, versionID: versionID}] = struct{}{}
				}
			}
		}
	}
	// One line per scan, because the ways this can find nothing — "the control layer
	// reported neither an active version nor a tail", "the two sources named the same
	// version", and "the candidate judgement rejected them all" — look identical from
	// the outside (nothing gets collected either way) and have entirely different
	// causes. One line per scan is affordable: production scans every few minutes, and
	// a test that shortens that to seconds is exactly when you want to see this.
	im.logger.Info("index: gc scan read its targets",
		zap.Int("active_versions", activeCount), zap.Int("chain_tails", tailCount),
		zap.Int("targets", len(targets)), zap.Float64("threshold", threshold))
	// Why each version was rejected, as counts.
	//
	// The line above says how many versions were examined; this one says which of
	// the four independent reasons rejected them. Without it they all look the
	// same from outside — and a run whose audit measured ~2,379 tombstoned
	// vectors in the ACTIVE artifact still reported no candidate, with nothing to
	// say which reason applied. Same level and same cadence as the line above:
	// production scans every few minutes, and when nothing is collected the only
	// question worth answering is "why nothing".
	var missingArtifact, missingSidecar, unestimable, belowThreshold int
	var maxShare float64
	var candidates []gcCandidate
	for t := range targets {
		kbID, versionID := t.kbID, t.versionID
		// A SEALED artifact is the prerequisite: the unsealed remains of a dead
		// build are §8.8's business, and this node may not hold the version at
		// all (the active version is served by whoever was picked for it).
		if !fileExists(im.indexPath(kbID, versionID)) {
			missingArtifact++
			continue
		}
		if !fileExists(im.sidecarPath(kbID, versionID)) {
			missingSidecar++
			continue
		}
		est, ok := im.deadShare(ctx, kbID, versionID)
		if !ok {
			unestimable++
			im.logger.Debug("index: gc scan: could not estimate a version's dead share",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Int("artifact_chunks", est.ArtifactChunks), zap.Int("live_chunks", est.LiveChunks),
				zap.Int("live_docs", est.LiveDocs))
			continue
		}
		if est.Share > maxShare {
			maxShare = est.Share
		}
		if est.Share <= threshold {
			belowThreshold++
			im.logger.Debug("index: gc scan: active version is under the dead-share threshold",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Float64("dead_share", est.Share),
				zap.Int("artifact_chunks", est.ArtifactChunks),
				zap.Int("live_chunks", est.LiveChunks),
				zap.Int("live_docs", est.LiveDocs),
				zap.Float64("threshold", threshold))
			continue
		}
		candidates = append(candidates, gcCandidate{KBID: kbID, VersionID: versionID, DeadShare: est.Share})
	}
	im.logger.Info("index: gc scan result",
		zap.Int("examined", len(targets)),
		zap.Int("no_artifact", missingArtifact),
		zap.Int("no_sidecar", missingSidecar),
		zap.Int("unestimable", unestimable),
		zap.Int("below_threshold", belowThreshold),
		zap.Int("candidates", len(candidates)),
		zap.Float64("max_dead_share", maxShare),
		zap.Float64("threshold", threshold))
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
func (im *IndexManagerImpl) deadShare(ctx context.Context, kbID string, versionID int64) (deadShareEstimate, bool) {
	est := deadShareEstimate{}
	artifactChunks, err := im.artifactChunkCount(kbID, versionID)
	if err != nil || artifactChunks == 0 {
		return est, false
	}
	est.ArtifactChunks = artifactChunks
	docIDs, err := im.listDocIDs(ctx, kbID, versionID)
	if err != nil {
		return est, false
	}
	est.LiveDocs = len(docIDs)
	if len(docIDs) > 0 {
		chunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, docIDs)
		if err != nil {
			return est, false
		}
		distinct := make(map[string]struct{}, len(chunkIDs))
		for _, id := range chunkIDs {
			distinct[id] = struct{}{}
		}
		est.LiveChunks = len(distinct)
	}
	dead := est.ArtifactChunks - est.LiveChunks
	if dead < 0 {
		// The sidecar does not cover every live chunk. Treating that as "no dead
		// weight" keeps the scan from reporting an artifact it does not
		// understand, which is the conservative direction: nothing gets cleaned.
		return est, false
	}
	est.Share = float64(dead) / float64(est.ArtifactChunks)
	return est, true
}

// deadShareEstimate is the measurement a candidate judgement is made from, kept
// together so a rejection can be explained rather than only reported.
//
// It is an estimate, and allowed to be one: the artifact's size comes from the
// sealed sidecar, the live set from the version's current documents. Counting a
// chunk once however many live documents share it is correct for the question
// being asked, and a chunk whose vector came along from an ancestor is
// indistinguishable here from one whose document was deleted in this version —
// both are weight the current document set does not justify.
type deadShareEstimate struct {
	// ArtifactChunks is how many chunk ids the sealed sidecar records.
	ArtifactChunks int
	// LiveChunks is how many distinct chunks the version's current documents
	// justify.
	LiveChunks int
	// LiveDocs is how many documents the version's current set holds. It is what
	// separates "this version really is small" from "this node believes the
	// version still holds documents that were deleted from it" — two states whose
	// dead share looks identical.
	LiveDocs int
	// Share is (ArtifactChunks - LiveChunks) / ArtifactChunks.
	Share float64
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
// artifactGraphFree reports whether the sealed artifact for (kbID, versionID)
// is the graph-free variant, by reading the shape line vecstore's Save writes
// (§8.6a). known is false when the sidecar has no such line — written before it
// existed — or cannot be read.
//
// Read from disk rather than from memory on purpose: the shape has to survive a
// restart and describe an artifact received from a peer, and the in-memory
// record does neither. The line is matched by prefix, like the chunk ids above,
// so the header growing another field cannot shift the read onto the wrong one.
func (im *IndexManagerImpl) artifactGraphFree(kbID string, versionID int64) (graphFree bool, known bool) {
	f, err := os.Open(im.sidecarPath(kbID, versionID))
	if err != nil {
		return false, false
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64), 4096)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, shapeLinePrefix) {
			continue
		}
		return strings.TrimSpace(strings.TrimPrefix(line, shapeLinePrefix)) == "1", true
	}
	return false, false
}

// shapeLinePrefix is the sidecar line vecstore's Save writes to record whether
// the artifact carries an HNSW graph (§8.6a). It must match hnsw_index.cpp's
// kShapePrefix.
const shapeLinePrefix = "graph_free "

// trainedLinePrefix is the sidecar line vecstore's Save writes to record the
// codebook baseline (docs/codebook-refresh-plan.md §3): how many vectors the
// quantizer was trained on, and how many append-reuses have happened since
// ("trained <ntotal> <appends>"). It must match hnsw_index.cpp's kTrainedPrefix.
//
// The values ride along with the artifact, so the baseline survives a restart
// and a §8.4 handoff without any Go-side state — the same reason the shape line
// lives there (§8.6a).
const trainedLinePrefix = "trained "

// artifactTrainedBaseline reads the codebook baseline from a version's sealed
// sidecar. known is false when the sidecar has no such line (written before this
// mechanism existed) or cannot be read.
//
// The caller must treat unknown as fail-safe — rebuild once, which writes a
// baseline — rather than as "trained just now": the latter would silently switch
// the whole mechanism off, which is the failure mode §7 risk 1 calls the most
// invisible one.
func (im *IndexManagerImpl) artifactTrainedBaseline(kbID string, versionID int64) (trainedNtotal, appendsSinceTrain int64, known bool) {
	f, err := os.Open(im.sidecarPath(kbID, versionID))
	if err != nil {
		return 0, 0, false
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64), 4096)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, trainedLinePrefix) {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, trainedLinePrefix))
		if len(fields) != 2 {
			return 0, 0, false
		}
		ntotal, nErr := strconv.ParseInt(fields[0], 10, 64)
		appends, aErr := strconv.ParseInt(fields[1], 10, 64)
		if nErr != nil || aErr != nil || ntotal < 0 || appends < 0 {
			return 0, 0, false
		}
		return ntotal, appends, true
	}
	return 0, 0, false
}

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
