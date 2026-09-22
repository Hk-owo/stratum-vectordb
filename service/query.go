package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/docstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
)

// QueryServiceImpl implements pb.QueryServiceServer.
type QueryServiceImpl struct {
	pb.UnimplementedQueryServiceServer

	raftNode       raft.RaftNode
	indexManager   index.IndexManager
	chunkDocMapper chunkdoc.ChunkDocMapper
	versionDocList versiondoc.VersionDocList
	docStore       docstore.DocStore
	vBloomStore    *bloom.VersionBloomStore

	// docSets memoizes version document-ID sets this replica has verified against
	// the control layer's committed digest, so the O(documents) read behind
	// doclist_us is paid once per version rather than once per query. See
	// docSetCache for why a verified set is safe to reuse.
	docSets docSetCache

	// logger carries the per-stage timings of a query. Debug level and silent by
	// default: one line per query at info would be noise, while the point is that
	// this path had no observability at all — localizing the O(candidates ×
	// documents) bug took a temporary C++ probe (v13 §5 #18). See SetLogger.
	logger *zap.Logger

	// localVersion is where §9.3(2)'s freshness check reads this node's
	// contiguous cursor. nil means "cannot verify a credential", which makes the
	// check skip rather than fail — an unconfigured node behaves as it did
	// before §9 rather than refusing everything.
	localVersion LocalVersionReporter

	// backfiller pulls this node's history up to a version on demand. It is what
	// turns §9.3(2)'s freshness refusal from a dead end into a repair: without it
	// a node that is behind refuses the query, never fetches what it is missing,
	// and therefore stays behind forever. See the freshness check in Query.
	backfiller Backfiller
}

// NewQueryService constructs a QueryServiceImpl.
// LocalVersionReporter reports this node's contiguous data cursor for a
// knowledge base — the highest version whose history it holds completely. It is
// what §9.3(2)'s freshness check compares against.
type LocalVersionReporter interface {
	LocalVersionOf(kbID string) int64
}

// SetLocalVersionReporter wires the cursor the freshness check reads. Optional:
// without it the node cannot verify a credential and therefore ignores one,
// which is the pre-§9 behaviour rather than a broken one.
func (s *QueryServiceImpl) SetLocalVersionReporter(r LocalVersionReporter) {
	s.localVersion = r
}

// Backfiller fetches a knowledge base's data on this node up to versionID — the
// storage layer's §7.5 pull, which plane.DataPlane.EnsureIndex already is.
type Backfiller interface {
	EnsureIndex(ctx context.Context, kbID string, versionID int64) error
}

// SetBackfiller wires the pull the freshness check uses to repair itself.
// Optional: without it the check refuses exactly as it did before and the node
// simply never catches up on demand, which is the pre-existing behaviour.
func (s *QueryServiceImpl) SetBackfiller(b Backfiller) {
	s.backfiller = b
}

func NewQueryService(
	rn raft.RaftNode,
	im index.IndexManager,
	cdm chunkdoc.ChunkDocMapper,
	vdl versiondoc.VersionDocList,
	ds docstore.DocStore,
	vBloomStore *bloom.VersionBloomStore,
) *QueryServiceImpl {
	return &QueryServiceImpl{
		raftNode:       rn,
		indexManager:   im,
		chunkDocMapper: cdm,
		versionDocList: vdl,
		docStore:       ds,
		vBloomStore:    vBloomStore,
		logger:         zap.NewNop(),
	}
}

// SetLogger wires the logger that carries the per-stage timings below. Optional:
// without it those timings go nowhere, which is the pre-existing behaviour
// (nothing about correctness depends on them).
func (s *QueryServiceImpl) SetLogger(l *zap.Logger) {
	if l != nil {
		s.logger = l
	}
}

// Query implements QueryServiceServer.
func (s *QueryServiceImpl) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	kbID := req.KnowledgeBaseId

	// Per-stage timings, emitted at debug level by the deferred function below.
	//
	// Why it exists: this path had NO observability, so locating the
	// O(candidates × documents) defect (v13 §5 #18) took an outside-in probe plus
	// a temporary C++ instrumentation. A slow query must be answerable from the
	// node's own log: search (vecstore), filter (chunk→doc mapping + version
	// membership), read (per-document content), total.
	//
	// Deferred rather than printed on the success path so that a query failing
	// *slowly* also reports where it spent the time.
	qStart := time.Now()
	var stageSearch, stageFilter, stageRead time.Duration
	var stageMeta, stageBloom, stageChunkMap time.Duration
	// Sub-stages added so every boundary on this path is attributable rather
	// than inferred by subtraction:
	//
	//	active_us      — resolving the active version when the caller names none
	//	                 (its own control-plane read, which used to run before
	//	                 metaStart and so was in NO stage)
	//	doclist_us      — reading the version's document-ID set (inside bloom_us)
	//	bloom_build_us  — rebuilding/caching the version filter (inside bloom_us)
	//	filter_cpu_us   — the filtering loop minus chunkmap_us: pure CPU
	//	agg_us          — aggregation + ranking between filter and read
	//	read_calls      — how many documents read_us paid for (its denominator)
	var stageActive, stageDocList, stageBloomBuild, stageAgg time.Duration
	var candidateCount, matched, chunkMapCalls, readCalls int
	defer func() {
		s.logger.Debug("query: stage timings",
			zap.String("kb_id", kbID),
			zap.Int("top_k", int(req.GetTopK())),
			zap.Int("candidates", candidateCount),
			zap.Int("matched_docs", matched),
			zap.Int("chunkmap_calls", chunkMapCalls),
			zap.Int("read_calls", readCalls),
			zap.Int64("active_us", stageActive.Microseconds()),
			zap.Int64("meta_us", stageMeta.Microseconds()),
			zap.Int64("bloom_us", stageBloom.Microseconds()),
			zap.Int64("doclist_us", stageDocList.Microseconds()),
			zap.Int64("bloom_build_us", stageBloomBuild.Microseconds()),
			zap.Int64("chunkmap_us", stageChunkMap.Microseconds()),
			zap.Int64("filter_cpu_us", (stageFilter-stageChunkMap).Microseconds()),
			zap.Int64("search_us", stageSearch.Microseconds()),
			zap.Int64("agg_us", stageAgg.Microseconds()),
			zap.Int64("filter_us", stageFilter.Microseconds()),
			zap.Int64("read_us", stageRead.Microseconds()),
			zap.Int64("total_us", time.Since(qStart).Microseconds()))
	}()

	// §9.3(2): honour the service station's freshness credential.
	//
	// A node whose contiguous history has not reached the version the caller
	// should be seeing can still answer — its data is complete, merely older —
	// and that is precisely the silent staleness §9.1 risk 1 describes: the
	// caller gets a well-formed result from the wrong point in time and has no
	// way to tell. Refusing makes it visible, and lets the station move to a
	// node that is current.
	//
	// Refusing is only half of it. A node that is behind has to be able to STOP
	// being behind, and nothing else in this path fetches what it is missing: the
	// write that would have pushed the data here reached quorum without it (§7.1),
	// and the confirmation that would have told it to pull is best-effort. Left to
	// refuse and nothing else, a replica stays behind forever while every query
	// for a version it could serve is answered with an error it can never clear —
	// measured on the 3+3 cluster, where one storage replica held none of a
	// knowledge base's versions and answered every query with "local history
	// reaches version 0".
	//
	// So the pull is asked for first, and the refusal is reserved for the case
	// where it genuinely did not help. The refusal carries a retryable wire name,
	// so a caller with another replica to ask (the station always has) moves on
	// instead of treating it as terminal — see router.retryableReasons.
	if want := req.GetMinVersion(); want > 0 && s.localVersion != nil {
		if have := s.localVersion.LocalVersionOf(kbID); have < want {
			if s.backfiller != nil {
				// Asynchronous, and on its own context. The pull has to outlive this
				// request: tied to the caller's ctx (as this was) it is cancelled by
				// the very deadline that made the caller ask — measured as
				// DeadlineExceeded on every single attempt, a fraction of the way in,
				// so the node never caught up and never answered. Off the request,
				// this caller is told to ask someone else (retryably, below) while
				// the pull runs to completion behind it, and the next query is
				// served from here.
				//
				// Re-triggering is harmless: EnsureIndex re-checks the cursor on every
				// pass and returns immediately once it is satisfied (see its pull loop).
				pullKB, pullWant := kbID, want
				go func() {
					// Bounded retry with backoff, because this is the node's ONLY way
					// back: the write that should have brought the data here reached
					// quorum without it (§7.1) and the announcement that would have
					// asked it to pull is best-effort, so if this attempt fails nothing
					// else will come along. A single attempt is not enough — it can
					// land in a window where the control tier is electing or a peer is
					// restarting, measured on an otherwise healthy cluster as
					// "resolve data source: GetClusterStatus: raft: remote: read at
					// control node 3". One unlucky attempt must not decide between a
					// node that catches up and one that stays behind for good.
					for attempt, backoff := 0, time.Second; attempt < 4; attempt++ {
						// Comfortably longer than EnsureIndex's own 30 s pull loop, so
						// what bounds a pull is the pull, not this wrapper.
						pullCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
						err := s.backfiller.EnsureIndex(pullCtx, pullKB, pullWant)
						cancel()
						if err == nil {
							return
						}
						s.logger.Debug("query: background pull did not complete; will retry",
							zap.String("kb_id", pullKB), zap.Int64("want", pullWant),
							zap.Int("attempt", attempt+1), zap.Duration("backoff", backoff), zap.Error(err))
						select {
						case <-time.After(backoff):
						case <-ctx.Done():
							// The caller gave up, but that is not a reason for this node
							// to stay behind: keep the retry schedule, drop the wait.
						}
						backoff *= 2
					}
				}()
			}
			if have = s.localVersion.LocalVersionOf(kbID); have < want {
				// Wrapped, not replaced: the wire name keeps it retryable for a
				// caller with another replica to ask, while the message keeps the
				// diagnosis — how far behind this node is and behind what. A bare
				// sentinel throws away the only information that makes the refusal
				// actionable (see TestQueryService_RefusesAStaleNodeForAFreshness…).
				return nil, stratumerrors.ToGRPCStatus(fmt.Errorf(
					"%w: %s: local history reaches version %d, below the required %d",
					stratumerrors.ErrIndexNotReady, kbID, have, want))
			}
		}
	}

	// Resolve version.
	var versionID int64
	if req.VersionId != nil {
		versionID = *req.VersionId
	} else {
		// active_us: this is a SECOND control-plane read, taken before meta_us
		// below and previously inside no stage at all. It only runs when the
		// caller does not pin a version — which is what the stress suite does —
		// so on that path the per-query control-plane cost is active_us + meta_us,
		// not meta_us alone.
		activeStart := time.Now()
		kb, err := s.raftNode.GetKB(ctx, kbID)
		stageActive = time.Since(activeStart)
		if err != nil {
			return nil, stratumerrors.ToGRPCStatus(err)
		}
		versionID = kb.ActiveVersionID
		if versionID == 0 {
			// A knowledge base with nothing written yet has NO active version, and
			// that is an empty result rather than a missing one
			// (docs/cursor-persistence-plan.md §5.1): a version now comes into
			// existence only when something is written, so refusing here would make
			// "create a knowledge base, then query it" fail against a knowledge
			// base that has done nothing wrong — it is simply unpopulated.
			//
			// Only the IMPLICIT lookup is answered this way. A caller that names a
			// version explicitly still gets version_not_found for an id that does
			// not exist, because it asked about that version in particular.
			return &pb.QueryResponse{Results: nil, VersionId: 0}, nil
		}
	}

	// Check version status.
	//
	// meta_us: on a storage node this is a CONTROL-PLANE read over gRPC
	// (RemoteRaftNode → the control tier), and it runs on EVERY query. The
	// per-stage timings showed search+filter+read summing to ~1.1 ms while
	// total_us was ~10.9 ms, so the missing time is exactly this kind of
	// per-query plumbing — measure it instead of assuming.
	metaStart := time.Now()
	// §F: ask for the one version this query is about — (versionID-1, versionID] —
	// rather than pulling the whole chain and filtering it here. On a storage node
	// this read goes to the control tier and runs on EVERY query (see meta_us
	// above), so a chain-sized answer was paid per query for one version's status.
	targetVersion, err := s.raftNode.GetVersion(ctx, kbID, versionID)
	stageMeta = time.Since(metaStart)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	if targetVersion.IndexStatus == types.IndexStatusPending {
		// A PENDING version is not a dead end: §8.6b builds its index lazily, and
		// THIS request is the "somebody is waiting for it" signal that the lazy
		// path exists for. Refusing without asking for the build is what made
		// PENDING permanent — the very query that would have triggered the build
		// was the one being refused, so a version whose eager build was slow,
		// queued or dropped never got a second chance. Measured: TestTwoTier's
		// `version is PENDING` never cleared inside its 180 s window, while the
		// same version was READY and servable minutes later.
		//
		// Triggering rather than waiting is deliberate: the build runs off this
		// request (the caller keeps its own deadline), and every retry the station
		// or the client makes gets a finished index.
		if err := s.indexManager.TriggerBuild(ctx, kbID, versionID); err != nil {
			s.logger.Debug("query: could not trigger a lazy build for a PENDING version",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		}
		// The refusal carries the index_not_ready sentinel, and that matters more
		// than the wording: a bare status.Error here has no wire name, so the
		// station's isRetryableErr reads it as terminal — it returns the error
		// WITHOUT trying the replica whose index IS ready, and records the replica
		// it just refused as failed. Measured: three rounds of that left every
		// storage node circuit-broken ("router: every node for this layer is
		// circuit-broken") and spent the test's whole 180 s window on it, while a
		// sibling replica was serving the same version the whole time.
		//
		// Same gRPC code as before (FailedPrecondition), so callers see no change;
		// what changes is that "another replica may have it" is now sayable.
		//
		// ErrVersionPending rather than ErrIndexNotReady: both are retryable wire
		// names, but this one names the actual state ("the version's write is still
		// in progress"), which is what someone reading a log wants to know.
		return nil, stratumerrors.ToGRPCStatus(stratumerrors.ErrVersionPending)
	}
	if targetVersion.IndexStatus.IsFailed() {
		return nil, status.Error(codes.FailedPrecondition, "version index is FAILED")
	}

	// Search: get multi × topK chunk results for aggregation headroom.
	searchTopK := int(req.TopK) * 3
	if searchTopK < 10 {
		searchTopK = 10
	}
	if searchTopK > 1000 {
		searchTopK = 1000
	}

	searchStart := time.Now()
	searchResults, err := s.indexManager.Search(ctx, kbID, versionID, req.Vector, searchTopK)
	stageSearch = time.Since(searchStart)
	candidateCount = len(searchResults)
	if err != nil {
		// An empty version (no documents) has no index entry, so Search
		// reports ErrIndexNotReady. Treat a genuinely empty version as an
		// empty result set rather than an error.
		// Only a version whose document set is PROVABLY empty is answered with an empty
		// result: the control layer records the empty set's digest for exactly that
		// state, so anything else means "this node cannot serve it YET" — which has to
		// travel as a retryable error. Answering empty here made a replica that had not
		// finished catching up indistinguishable from a version with no documents: the
		// caller gets a well-formed answer with nothing in it, and no reason to ask
		// another node.
		if errors.Is(err, stratumerrors.ErrIndexNotReady) && targetVersion.DocIDSetHash == types.EmptyDocIDSetHash {
			return &pb.QueryResponse{Results: nil, VersionId: versionID}, nil
		}
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Filter by threshold.
	threshold := float32(0)
	if req.Threshold != nil {
		threshold = *req.Threshold
	}

	// Map chunk results to document results.
	type docScore struct {
		docID  string
		scores []float32
	}
	docMap := make(map[string]*docScore)

	// Per-version document state, read ONCE for the whole query: the document-ID set is
	// both the filter's input and the authoritative membership test below.
	//
	// Derived from one snapshot on purpose. The filter decides which search hits are
	// dropped, so a filter that disagrees with the set it is meant to describe does not
	// merely lose precision — it deletes hits that are really there. The store rebuilds
	// a filter whenever that set changes (bloom.VersionBloomStore.GetForDocuments), and
	// handing the set over here is what keeps the two in step.
	//
	// bloom_us covers the set plus the filter: both are local reads, so this is what
	// "reading our own storage" costs.
	//
	// A failed read leaves both empty and filtering is then SKIPPED rather than applied
	// with an empty set: "the set is unavailable" must not read as "no document is in
	// this version", which would drop every hit and answer empty.
	bloomStart := time.Now()
	// The read behind doclist_us is O(documents) and used to run on every query.
	// A version's set is immutable once written, so it is memoized — but only
	// after it hashes to the digest the control layer committed for the version
	// (targetVersion.DocIDSetHash, already in hand from meta_us). A cache hit is
	// then equivalent to re-reading the store and verifying it, and a replica
	// whose data has not arrived is never cached. See docSetCache.
	ids, vDocs, idsErr := s.docSets.lookup(ctx, s.versionDocList, kbID, versionID, targetVersion.DocIDSetHash)
	stageDocList = time.Since(bloomStart)
	var (
		vBloom    bloom.BloomFilter
		vBloomErr error
	)
	if idsErr != nil {
		vBloomErr = idsErr
	}

	// An empty read is only an answer when the VERSION is empty. A replica that
	// has not received the version's documents yet reads the set successfully and
	// gets nothing back, and the two states must not collapse: "this version has
	// no documents" is a legitimate empty result, while "this replica does not
	// know the version's documents" cannot serve the query at all and has to say
	// so retryably, so the station asks a replica that can.
	//
	// The control layer already carries the distinction — the version's
	// document-set digest. The empty set has its own digest
	// (types.EmptyDocIDSetHash), so an empty read for a version that does NOT
	// carry that digest is this replica missing the data, not an empty version.
	// The Search-side branch below makes exactly this test for the same reason;
	// the filter path needs it too because search can succeed while the set does
	// not (the chunk mapping and the document set land separately).
	//
	// Measured on the 3+3 cluster, one replica having missed the write: its query
	// log read `candidates=1 matched_docs=0 chunkmap_calls=1` — the search found
	// the chunk, the filter dropped it because the set it was filtering against
	// was empty — and the caller got `results=0, err=nil` for ~14 s (t+6…+18 s)
	// until the replica caught up.
	if idsErr == nil && len(ids) == 0 && targetVersion.DocIDSetHash != types.EmptyDocIDSetHash {
		return nil, stratumerrors.ToGRPCStatus(fmt.Errorf(
			"%w: %s: version %d's document set is not on this replica yet (its committed digest is not the empty set's)",
			stratumerrors.ErrIndexNotReady, kbID, versionID))
	}

	// The version's document-ID set, materialized ONCE for the whole query.
	//
	// It used to be fetched INSIDE the loop below — once per candidate chunk,
	// again per docID — and every fetch was a prefix scan returning the version's
	// entire document list, which was then walked linearly to answer "is this
	// docID in the version?". One query therefore cost O(candidates × documents).
	// Measured on the 3+3 cluster: 2,000 documents → 207 ms, 8,000 → 2.73 s, while
	// the HNSW search inside vecstore took 16 µs (probe: VECPROBE search
	// ntotal=18 took_us=16). A single fetch plus a set makes membership O(1) and
	// the whole query O(documents) once.
	//
	// A failed fetch leaves vDocs nil, and every membership test then fails —
	// the same conservative "cannot confirm, so skip" the per-candidate fetch
	// had when its own call errored.
	if idsErr == nil {
		// bloom_build_us is inside bloom_us: the store's GetForDocuments, which
		// rebuilds and caches the version filter when the set has changed.
		// Separated from doclist_us because one is a read of our own storage and
		// the other is CPU plus its own bookkeeping. The membership map is no
		// longer built here — docSets.lookup returns it, from the cache when the
		// set has already been verified.
		bloomBuildStart := time.Now()
		vBloom = s.vBloomStore.GetForDocuments(kbID, versionID, ids)
		stageBloomBuild = time.Since(bloomBuildStart)
	}
	stageBloom = time.Since(bloomStart)

	filterStart := time.Now()
	for _, r := range searchResults {
		if r.Score < threshold {
			continue
		}
		// Two passes, and the first one is deliberate.
		//
		// This loop was rewritten to consume the mapping as it is decoded — an
		// IterateDocIDs callback instead of ListDocIDs plus a second walk — on the
		// theory that the slice and the extra pass were pure overhead. Measured on
		// the 3+3 cluster at 2,000 and 8,000 documents, it bought nothing: the
		// closure's captured variables are reached through an indirect load on
		// every one of the (candidates × documents) iterations, which cost as much
		// as the allocation it removed. The measurement could not even reproduce a
		// direction across runs — the same case's p50 moved ±40% between runs, far
		// more than the effect — so the version that is simpler and has no extra
		// interface surface is the one that stayed.
		chunkMapStart := time.Now()
		docIDs, err := s.chunkDocMapper.ListDocIDs(ctx, kbID, r.ChunkID)
		stageChunkMap += time.Since(chunkMapStart)
		chunkMapCalls++
		if err != nil {
			continue
		}
		for _, docID := range docIDs {
			// Version filter: a bloom miss means the document is
			// definitely not in this version's document set (no false
			// negatives), so it is skipped without an authoritative
			// check. A bloom hit may be a false positive, so it is
			// confirmed against the version doc list.
			if vBloomErr == nil {
				if !vBloom.Test(docID) {
					continue
				}
				if _, ok := vDocs[docID]; !ok {
					continue
				}
			}
			if ds, ok := docMap[docID]; ok {
				ds.scores = append(ds.scores, r.Score)
			} else {
				docMap[docID] = &docScore{docID: docID, scores: []float32{r.Score}}
			}
		}
	}
	stageFilter = time.Since(filterStart)
	matched = len(docMap)

	// Aggregate per-document scores.
	agg := req.Aggregation
	// 0 is the default (MEDIAN), so treat it as MEDIAN.

	type scoredDoc struct {
		docID   string
		score   float32
		content string
	}

	// Aggregate, rank, THEN read — in that order.
	//
	// Reading content used to happen while building the candidate list, so one
	// query read every matched document's text and threw almost all of it away
	// when it truncated to top_k. That is not a rare case: chunks are
	// content-addressed, so one chunk can belong to thousands of documents (a
	// corpus of near-identical documents collapses to a handful of chunks), and
	// the debug timings made it visible — `matched_docs: 1000`, `read_us: 8684`
	// out of `total_us: 22909` on a 2,000-document version, against
	// `search_us: 389`. Ranking first costs nothing (the scores are already in
	// memory) and turns O(matched) document reads into O(top_k).
	// agg_us: aggregation + ranking, between the filter loop and the reads. It was
	// inside no stage, so it silently inflated total_us − (all named stages).
	aggStart := time.Now()
	candidates := make([]scoredDoc, 0, len(docMap))
	for docID, ds := range docMap {
		candidates = append(candidates, scoredDoc{docID: docID, score: aggregate(ds.scores, agg)})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].score > candidates[j].score })
	stageAgg = time.Since(aggStart)

	// Read content for the winners, stopping as soon as top_k readable documents
	// are found. A candidate whose content cannot be read is skipped and the next
	// one takes its place, which is what the previous "read everything, then
	// truncate" ordering did as well.
	readStart := time.Now()
	results := make([]scoredDoc, 0, int(req.TopK))
	for _, cand := range candidates {
		if len(results) >= int(req.TopK) {
			break
		}
		readCalls++
		content, err := s.docStore.ReadAt(ctx, kbID, cand.docID, versionID)
		if err != nil {
			continue
		}
		results = append(results, scoredDoc{docID: cand.docID, score: cand.score, content: string(content)})
	}
	stageRead = time.Since(readStart)

	out := make([]*pb.QueryResult, len(results))
	for i, r := range results {
		out[i] = &pb.QueryResult{
			DocId:   r.docID,
			Content: r.content,
			Score:   r.score,
		}
	}

	return &pb.QueryResponse{Results: out, VersionId: versionID}, nil
}

// aggregate combines per-chunk scores into a per-document score using the
// specified aggregation method.
func aggregate(scores []float32, method pb.AggregationMethod) float32 {
	if len(scores) == 0 {
		return 0
	}
	if len(scores) == 1 {
		return scores[0]
	}

	sort.Slice(scores, func(i, j int) bool { return scores[i] < scores[j] })

	switch method {
	case pb.AggregationMethod_AGGREGATION_METHOD_MAX:
		return scores[len(scores)-1]
	case pb.AggregationMethod_AGGREGATION_METHOD_MEAN:
		var sum float32
		for _, s := range scores {
			sum += s
		}
		return sum / float32(len(scores))
	case pb.AggregationMethod_AGGREGATION_METHOD_MEDIAN:
		fallthrough
	default:
		mid := len(scores) / 2
		if len(scores)%2 == 0 {
			return (scores[mid-1] + scores[mid]) / 2
		}
		return scores[mid]
	}
}

var _ pb.QueryServiceServer = (*QueryServiceImpl)(nil)
