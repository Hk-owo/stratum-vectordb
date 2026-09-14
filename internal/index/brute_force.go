package index

import (
	"context"
	"fmt"
	"math"
	"sort"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// DefaultBruteForceMaxChunks is the version size up to which a missing index is
// answered by scanning rather than by building (Stratum_设计文档v13.md §8.6b).
// Above it, scanning costs about as much as building, so making the caller wait
// is the better trade. Placeholder value, see §10.4.
const DefaultBruteForceMaxChunks = 50_000

// listChunkIDs collects every chunk of the version.
func (im *IndexManagerImpl) listChunkIDs(ctx context.Context, kbID string, versionID int64) ([]string, error) {
	if im.listDocIDs == nil || im.listChunkIDsByDocs == nil {
		return nil, fmt.Errorf("index: version %d of %s: chunk listing is not wired", versionID, kbID)
	}
	docIDs, err := im.listDocIDs(ctx, kbID, versionID)
	if err != nil {
		return nil, fmt.Errorf("index: list docs of %s/%d: %w", kbID, versionID, err)
	}
	if len(docIDs) == 0 {
		return nil, nil
	}
	chunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, docIDs)
	if err != nil {
		return nil, fmt.Errorf("index: list chunks of %s/%d: %w", kbID, versionID, err)
	}
	return chunkIDs, nil
}

// bruteForceMaxChunks is the size up to which scanning beats building.
func (im *IndexManagerImpl) bruteForceMaxChunks() int {
	if im.cfg.BruteForceMaxChunks > 0 {
		return im.cfg.BruteForceMaxChunks
	}
	return DefaultBruteForceMaxChunks
}

// tryBruteForce answers a query for a version that has no index, when scanning
// is the cheaper option (§8.6b). answered=false means the caller should build
// and wait instead.
func (im *IndexManagerImpl) tryBruteForce(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) (bool, []types.SearchResult, error) {
	// Deletion tombstones come first, exactly as they do on the load path: a
	// KB or version being deleted must not become queryable again just because
	// a scan can answer without an index.
	im.mu.Lock()
	deleted := im.deletedKBs[kbID] || im.deletedVersions[indexKey{kbID, versionID}]
	im.mu.Unlock()
	if deleted {
		return true, nil, stratumerrors.ErrIndexNotReady
	}

	chunkIDs, err := im.listChunkIDs(ctx, kbID, versionID)
	if err != nil {
		return false, nil, err
	}
	if len(chunkIDs) == 0 {
		// Nothing to scan and nothing to build. Reported the way an unbuilt
		// index always has been, so callers' existing handling of genuinely
		// empty versions still applies.
		return true, nil, stratumerrors.ErrIndexNotReady
	}
	if len(chunkIDs) > im.bruteForceMaxChunks() {
		return false, nil, nil // large: building is the better deal
	}

	// Schedule the build regardless: this version will be queried again, and
	// the scan is a bridge to it, not a replacement.
	if err := im.TriggerBuild(ctx, kbID, versionID); err != nil {
		im.logger.Warn("index: could not schedule a lazy build after a scan",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
	im.logger.Info("index: answering by scanning a version whose index is not built yet",
		zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Int("chunks", len(chunkIDs)))

	results, err := im.bruteForceSearch(ctx, kbID, versionID, chunkIDs, vector, topK)
	return true, results, err
}

// bruteForceSearch scores every chunk of the version against vector and returns
// the best topK.
//
// This is §8.6b's fallback for a version whose index does not exist yet. Lazy
// building means a query can legitimately arrive before one has been built, and
// for a small version answering it now is cheaper than making the caller wait
// for the whole build.
func (im *IndexManagerImpl) bruteForceSearch(ctx context.Context, kbID string, versionID int64, chunkIDs []string, vector []float32, topK int) ([]types.SearchResult, error) {
	if im.readChunkVector == nil {
		return nil, fmt.Errorf("index: version %d of %s: chunk read is not wired", versionID, kbID)
	}
	similarity := im.similarityOf(ctx, kbID)

	scored := make([]types.SearchResult, 0, len(chunkIDs))
	for _, chunkID := range chunkIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chunkVector, err := im.readChunkVector(ctx, kbID, chunkID)
		if err != nil {
			// A chunk that cannot be read is skipped rather than failing the
			// whole query — the same tolerance the vecstore search path shows.
			continue
		}
		score, ok := similarityScore(similarity, vector, chunkVector)
		if !ok {
			continue
		}
		scored = append(scored, types.SearchResult{ChunkID: chunkID, Score: score})
	}

	// Every metric is normalised so that higher is better (EUCLIDEAN is
	// negated distance), which keeps this sort metric-independent.
	sort.Slice(scored, func(i, j int) bool { return scored[i].Score > scored[j].Score })
	if len(scored) > topK {
		scored = scored[:topK]
	}
	return scored, nil
}

// similarityOf reads the KB's similarity metric, defaulting to COSINE when the
// metadata is unavailable — the same default used elsewhere.
func (im *IndexManagerImpl) similarityOf(ctx context.Context, kbID string) string {
	if im.kbMetaGetter == nil {
		return "COSINE"
	}
	kb, err := im.kbMetaGetter(ctx, kbID)
	if err != nil || kb.Similarity == "" {
		return "COSINE"
	}
	return kb.Similarity
}

// similarityScore computes a comparable "higher is better" score for the given
// metric. ok=false means the two vectors cannot be compared at all (a
// dimension mismatch or a zero-length vector under a cosine metric).
func similarityScore(similarity string, a, b []float32) (float32, bool) {
	if len(a) == 0 || len(a) != len(b) {
		return 0, false
	}
	switch similarity {
	case "EUCLIDEAN":
		var sum float64
		for i := range a {
			d := float64(a[i] - b[i])
			sum += d * d
		}
		return float32(-math.Sqrt(sum)), true
	case "INNER_PRODUCT":
		var sum float64
		for i := range a {
			sum += float64(a[i]) * float64(b[i])
		}
		return float32(sum), true
	default: // COSINE
		var dot, na, nb float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
			na += float64(a[i]) * float64(a[i])
			nb += float64(b[i]) * float64(b[i])
		}
		if na == 0 || nb == 0 {
			return 0, false
		}
		return float32(dot / (math.Sqrt(na) * math.Sqrt(nb))), true
	}
}
