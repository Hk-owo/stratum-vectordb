// Two-stage retrieval (§5.1): the quantized coarse pass in vecstore, then this
// node's own answers — the serviceable check, the readiness translations, and the
// "empty result may mean the data is still landing" disambiguation.
package index

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	vecstorepb "stratum/api/proto/vecstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"time"
)

// searchRequest builds the vector-store search request, applying the node's
// coarse-pass budget when one is configured (Stratum_设计文档v12.md §2.2).
//
// CandidateN stays unset at zero on purpose: the vector store already reads 0
// as "use my own default" (clamp(top_k × 8, 16, 4096)), and leaving the field
// out keeps a node that does not configure it identical on the wire to one
// built before this knob existed.
func (im *IndexManagerImpl) searchRequest(kbID string, versionID int64, vector []float32, topK int) *vecstorepb.SearchIndexRequest {
	req := &vecstorepb.SearchIndexRequest{
		KbId:      kbID,
		VersionId: versionID,
		Vector:    vector,
		TopK:      int32(topK),
	}
	if im.cfg.CandidateN > 0 {
		req.CandidateN = int32(im.cfg.CandidateN)
	}
	return req
}

// Search implements IndexManager.
func (im *IndexManagerImpl) Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	key := indexKey{kbID, versionID}

	// Per-stage timings of one search, at debug level — the read-path counterpart
	// of "index: build timings". The service layer's search_us wraps this whole
	// call; these fields say what it was made of, and where the process boundary
	// to vecstore sits:
	//
	//	load_us        — reopening the artifact from disk (only when not loaded)
	//	bruteforce_us  — the graph-free scan that answers a small unloaded version
	//	vecstore_us    — the gRPC round trip to the C++ HNSW search
	//	empty_check_us — the "documents are here but their chunks are not" probe,
	//	                 which runs ONLY on an empty result and costs two local reads
	searchStart := time.Now()
	var loadDur, bruteforceDur, vecstoreDur, emptyCheckDur time.Duration
	defer func() {
		im.loggerOrNop().Debug("index: search timings",
			zap.String("kb_id", kbID),
			zap.Int64("version_id", versionID),
			zap.Int("top_k", topK),
			zap.Int64("load_us", loadDur.Microseconds()),
			zap.Int64("bruteforce_us", bruteforceDur.Microseconds()),
			zap.Int64("vecstore_us", vecstoreDur.Microseconds()),
			zap.Int64("empty_check_us", emptyCheckDur.Microseconds()),
			zap.Int64("total_us", time.Since(searchStart).Microseconds()))
	}()

	// §8.6(d): while this node is collecting the version's artifact — it is
	// reopened, so it is BUILDING, so the vecstore cannot answer from it — say so
	// explicitly instead of letting the call fall through to a load that would
	// fail for a reason the caller cannot act on. The named sentinel is what lets
	// the station tell "this replica is briefly out" apart from "this version is
	// gone", and retry elsewhere.
	if im.inMaintenance(key) {
		return nil, stratumerrors.ErrIndexMaintenance
	}

	// §8.6a reheat: a graph-free artifact whose version has become hot again gets
	// its graph back.
	//
	// This runs BEFORE recordSearch, and that order is the whole trick: recordSearch
	// stamps "now", and the question is how long it had been since the PREVIOUS
	// query. With the stamp already written every version would look hot and the
	// reheat would fire on every query.
	if im.needsReshape(kbID, versionID) {
		im.loggerOrNop().Info("index: version's shape no longer matches its traffic; rebuilding (§8.6a)",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
		if err := im.TriggerBuild(ctx, kbID, versionID); err != nil {
			im.loggerOrNop().Warn("index: could not schedule the reshape",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		}
	}
	im.recordSearch(key)

	// Restore path: if the version's index is not already loaded (e.g.
	// this Go process restarted, or the index was LRU-evicted and the
	// vecstore side lost it), try loading it from the persisted file on
	// disk before falling back to ErrIndexNotReady. The load is
	// idempotent and concurrency-safe; failure here is non-fatal — a
	// truly unbuilt version still reports ErrIndexNotReady below.
	im.mu.Lock()
	_, loaded := im.loaded[key]
	_, loading := im.loading[key]
	im.mu.Unlock()
	// A build is already in flight for this version. Do NOT wait it out here:
	// §8.6b's rule is "a small version is answered by scanning", and what decides
	// that is the version's SIZE — not whether somebody happens to be building an
	// index for it. Falling through to acquire() in this state is what turned "a
	// build is in progress" into a load_wait_timeout stall followed by
	// ErrIndexLoadTimeout, for versions of a dozen chunks whose scan takes
	// microseconds (docs/stress-test-report.md §E.4).
	//
	// Deliberately no loadFromDisk on this path: loading==true means the artifact
	// for this key is being written right now, and reading a half-written one is
	// exactly what the build's own .tmp+rename discipline exists to prevent.
	if !loaded && loading {
		bruteStart := time.Now()
		answered, results, terr := im.tryBruteForce(ctx, kbID, versionID, vector, topK)
		bruteforceDur = time.Since(bruteStart)
		if terr != nil {
			return nil, terr
		}
		if answered {
			return results, nil
		}
		// Too large to scan: the build in flight is the only way to answer, so
		// fall through and wait for it (bounded by load_wait_timeout_ms).
	}
	if !loaded && !loading {
		loadStart := time.Now()
		loadErr := im.loadFromDisk(ctx, kbID, versionID)
		loadDur = time.Since(loadStart)
		if loadErr != nil {
			// §8.6b: indexes are built lazily now, so "no index on disk" is a
			// normal state rather than a failure. A small version is answered
			// by scanning; a large one falls through to the build below, which
			// acquire() then waits for.
			bruteStart := time.Now()
			answered, results, terr := im.tryBruteForce(ctx, kbID, versionID, vector, topK)
			bruteforceDur = time.Since(bruteStart)
			if terr != nil {
				return nil, terr
			}
			if answered {
				return results, nil
			}
			if err := im.TriggerBuild(ctx, kbID, versionID); err != nil {
				im.logger.Warn("index: could not schedule a lazy build",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			}
		}
	}

	// Acquire the index: load if needed, increment ref count.
	indexed, err := im.searchLoaded(ctx, key, vector, topK)
	vecstoreDur, emptyCheckDur = indexed.vecstoreDur, indexed.emptyCheckDur
	if err != nil {
		return nil, err
	}
	return indexed.results, nil
}

// indexedSearch is one completed search over an index this node holds in memory.
// The timings travel with the results because both callers report them: Search
// through its debug line, TrySearch through the service layer's stage timings.
type indexedSearch struct {
	results       []types.SearchResult
	vecstoreDur   time.Duration
	emptyCheckDur time.Duration
}

// searchLoaded runs the vector-store search over an index that has ALREADY been
// loaded (or is about to be, from disk), holding a reference for the duration of
// the call. The caller is responsible for having established that an index for
// key exists — acquire reports ErrIndexNotReady rather than triggering a build.
//
// Shared by Search and TrySearch so the two cannot drift on error translation or
// on the "an empty result may mean the data is still landing" probe.
func (im *IndexManagerImpl) searchLoaded(ctx context.Context, key indexKey, vector []float32, topK int) (indexedSearch, error) {
	kbID, versionID := key.kbID, key.versionID
	if err := im.acquire(ctx, key); err != nil {
		return indexedSearch{}, err
	}
	defer im.release(key)

	var out indexedSearch
	vecstoreStart := time.Now()
	resp, err := im.vectorIndexClient.Search(ctx, im.searchRequest(kbID, versionID, vector, topK))
	out.vecstoreDur = time.Since(vecstoreStart)
	if err != nil {
		// Translate the vector store's own classification rather than re-inventing it.
		// The C++ side already answers a search on a still-building index with
		// FAILED_PRECONDITION (hnsw_index.cpp → grpc_service.cpp maps absl's
		// FailedPrecondition straight through), and grpc-go hands us that as a
		// *status.Error — so the code is available here and is the right thing to read.
		//
		// Reading the code, not the message, is deliberate: the text ("index is still
		// building") is a wording detail that may change, while the status code is the
		// contract both sides already agreed on.
		//
		// Why this matters beyond tidiness: this error used to travel up wrapped in %w
		// only, and ToGRPCStatus' fallback then re-labelled a correctly-classified
		// error as Internal — the classification was not missing, it was overwritten.
		// Attaching the local sentinel keeps the meaning, and ToGRPCStatus maps it back
		// to FailedPrecondition.
		// Two codes mean the same thing ON THIS PATH: FAILED_PRECONDITION ("still
		// building") and NOT_FOUND ("no index built or loaded"). Both answer "the
		// version's index is not usable on this node yet" — whether a version exists is
		// the control layer's business, not the store's, so a NOT_FOUND here never means
		// "this version does not exist".
		switch status.Code(err) {
		case codes.FailedPrecondition, codes.NotFound:
			return indexedSearch{vecstoreDur: out.vecstoreDur}, fmt.Errorf("index: vector search (%s/%d): %w", kbID, versionID, stratumerrors.ErrIndexNotReady)
		}
		return indexedSearch{vecstoreDur: out.vecstoreDur}, fmt.Errorf("index: vector search (%s/%d): %w", kbID, versionID, err)
	}

	out.results = make([]types.SearchResult, len(resp.Results))
	for i, r := range resp.Results {
		out.results[i] = types.SearchResult{ChunkID: r.ChunkId, Score: r.Score}
	}
	if len(out.results) == 0 {
		emptyCheckStart := time.Now()
		emptyErr := im.emptySearchMeansTheDataIsStillLanding(ctx, kbID, versionID)
		out.emptyCheckDur = time.Since(emptyCheckStart)
		if emptyErr != nil {
			return indexedSearch{vecstoreDur: out.vecstoreDur, emptyCheckDur: out.emptyCheckDur}, emptyErr
		}
	}
	return out, nil
}

// TrySearch answers a query from whatever this node can serve RIGHT NOW, without
// waiting for anything: the in-memory index if there is one, the artifact on disk
// if it loads, and otherwise a scan of the version's chunks (§8.6b). It never
// blocks on a build and never triggers one just to satisfy the wait — the scan
// path schedules a build as its own bridge, which is the only build this asks
// for.
//
// It does NOT schedule a build. The two-step rule that decides "ask a peer, then build"
// (docs/index-owner-sharding-plan.md §4.2) belongs to the caller, which can run it off the
// request: doing it here as well would build twice, and a build for an artifact that is
// already finished is a full rebuild.
//
// The three outcomes, and what a caller does with each:
//
//   - (results, true, nil)   — answered now.
//   - (nil, false, nil)      — not now: the version is above BruteForceMaxChunks
//     (scanning would cost about what building costs) or none of its vectors
//     could be read. The caller decides; service/query.go refuses with
//     version_pending and lets the build answer a retry.
//   - (nil, true, err)       — a verdict, not an answer: maintenance, a deleted
//     version, or an index that exists but cannot serve. Errors are retryable
//     sentinels so a station moves to another replica instead of treating the
//     version as broken.
//
// answered=false therefore only ever travels with a nil error, the same contract
// tryBruteForce uses.
//
// Why this is a separate entry point rather than a flag on Search: Search's
// answer to "no index" is to WAIT for one — acquire blocks up to loadWaitTimeout —
// which is exactly right for a READY version and exactly wrong for a PENDING one.
// §8.6b says a small version needs no index at all, and making the caller wait
// turns a microsecond scan into a timeout. service/query.go refused every PENDING
// version outright before this existed, so the scan was unreachable from the
// query path no matter how small the version was.
func (im *IndexManagerImpl) TrySearch(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, bool, error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	default:
	}

	key := indexKey{kbID, versionID}
	if im.inMaintenance(key) {
		return nil, true, stratumerrors.ErrIndexMaintenance
	}
	// Recorded before any decision, for the same reason Search does it: the
	// version counts as asked-for even when the request is answered by a scan.
	im.recordSearch(key)

	im.mu.Lock()
	_, loaded := im.loaded[key]
	_, loading := im.loading[key]
	im.mu.Unlock()

	switch {
	case loaded:
		// The ordinary path, and the cheapest answer: this node already holds the
		// artifact, so it does not matter that the control layer has not been told
		// the build finished yet.

	case loading:
		// Somebody else's build is in flight. §8.6b decides by SIZE, not by
		// whether a build happens to be running (see Search's note on the same
		// branch). Too large to scan: the build in flight is the answer, and
		// waiting for it is the caller's decision rather than ours.
		//
		// No build is scheduled from here: a build is ALREADY in flight, and the caller
		// owns the "then build" step anyway (see the TrySearch doc comment).
		answered, results, err := im.tryBruteForceWith(ctx, kbID, versionID, vector, topK, false)
		return results, answered, err

	default:
		// Nothing in memory, nothing in flight: the artifact may still be on
		// disk. Reading a half-written one is impossible by the build's own
		// .tmp+rename discipline, and a missing one is the normal state for a
		// lazily-built version rather than an error.
		if loadErr := im.loadFromDisk(ctx, kbID, versionID); loadErr != nil {
			answered, results, err := im.tryBruteForceWith(ctx, kbID, versionID, vector, topK, false)
			return results, answered, err
		}
	}

	indexed, err := im.searchLoaded(ctx, key, vector, topK)
	if err != nil {
		return nil, true, err
	}
	return indexed.results, true, nil
}

// emptySearchMeansTheDataIsStillLanding reports an error when a search that found
// nothing is explained by this replica not having the version's chunks yet.
//
// Three ways a replica can answer a version's query with an EMPTY result while being
// unable to serve it at all are now all named: the document set is missing
// (service/query.go), every chunk vector is unreadable (§8.6b's scan), and — this one
// — the documents are here but the chunks that carry them are not, so the index this
// replica holds is empty and a search over it SUCCEEDS with nothing.
//
// Measured on the 3+3 cluster (2026-09-21, debug level), one replica 5 s before it
// finished catching up:
//
//	query: stage timings {candidates: 0, matched_docs: 0, chunkmap_calls: 0,
//	                      filter_us: 0, meta_us: 490, bloom_us: 2472, search_us: 493}
//	index: built from scratch … version 195, total_chunks: 11, status: READY   (t+5 s)
//
// The document set read fine AND non-empty, so the service layer's "document set is
// missing" test did not fire; the search returned successfully, so nothing upstream
// saw a retryable error; and the caller got `results=0, err=nil` — the one answer a
// caller cannot act on, because the replicas that DID hold the version were never
// asked (the station reads an empty result as an answer, by design).
//
// Only "documents here, chunks not" is reported. A version whose chunks are all
// present and simply do not match is a legitimate empty answer, and stays one.
func (im *IndexManagerImpl) emptySearchMeansTheDataIsStillLanding(ctx context.Context, kbID string, versionID int64) error {
	if im.listDocIDs == nil || im.listChunkIDsByDocs == nil {
		return nil
	}
	docIDs, err := im.listDocIDs(ctx, kbID, versionID)
	if err != nil || len(docIDs) == 0 {
		// Not this function's to name: "this replica has no document set" is already
		// its own retryable refusal in the service layer, and a version that really has
		// no documents is a legitimate empty answer.
		return nil
	}
	chunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, docIDs)
	if err != nil || len(chunkIDs) > 0 {
		return nil
	}
	return fmt.Errorf("index: vector search (%s/%d): %d documents are here but none of their chunks are, so this replica's index for the version is empty: %w",
		kbID, versionID, len(docIDs), stratumerrors.ErrIndexNotReady)
}

// IndexExists implements IndexManager: asks the vecstore side whether the
// persisted index files for (kbID, versionID) exist on disk. Stateless on
// the vecstore side, so the answer reflects disk facts even right after a
// vecstore restart. With an empty IndexDataDir (persistence unconfigured)
// it reports false — there is no on-disk index.
func (im *IndexManagerImpl) IndexExists(ctx context.Context, kbID string, versionID int64) (bool, error) {
	if im.cfg.IndexDataDir == "" {
		return false, nil
	}
	resp, err := im.vectorIndexClient.ExistsIndex(ctx, &vecstorepb.ExistsIndexRequest{
		KbId: kbID, VersionId: versionID, Path: im.indexPath(kbID, versionID),
	})
	if err != nil {
		return false, fmt.Errorf("index: ExistsIndex RPC: %w", err)
	}
	return resp.GetExists(), nil
}

// Resident implements IndexManager: loaded, or a build/load in flight.
func (im *IndexManagerImpl) Resident(kbID string, versionID int64) bool {
	key := indexKey{kbID, versionID}
	im.mu.Lock()
	defer im.mu.Unlock()
	_, loaded := im.loaded[key]
	return loaded || im.loading[key]
}
