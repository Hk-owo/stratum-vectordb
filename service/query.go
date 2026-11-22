package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkdoc"
	"stratum/internal/docstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/versionview"
)

// QueryServiceImpl implements pb.QueryServiceServer.
type QueryServiceImpl struct {
	pb.UnimplementedQueryServiceServer

	raftNode       raft.MetaReader
	indexManager   index.IndexManager
	chunkDocMapper chunkdoc.ChunkDocMapper
	versionDocList versiondoc.VersionDocList
	docStore       docstore.DocStore
	versionViews   *versionview.Store

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

	// indexFetcher asks the other replicas for a version's built index. It is what
	// keeps a replica that does not build for itself (not the version's owner
	// head, docs/已实施/index-owner-sharding-plan.md §5.2) from staying cold forever
	// when §8.4's fire-and-forget push never reached it. Optional: without it the
	// PENDING path builds locally, which is the behaviour before on-demand fetch.
	indexFetcher IndexFetcher

	// pulls records the catch-up goroutines in flight, keyed by (kbID, versionID),
	// so a burst of queries on a lagging replica starts ONE pull rather than one
	// each (M1 of docs/参考/code-review-2026-09-24.md). Guarded by pullMu and created
	// lazily by beginPull: a node that never answers a lagging query never needs
	// the map at all.
	pullMu sync.Mutex
	pulls  map[pullKey]bool
}

// pullKey identifies one in-flight catch-up: this node's history for one
// knowledge base up to one version.
// pullKey is the de-duplication key for a background catch-up, and it holds the
// knowledge base ONLY (M2 of docs/参考/code-review-2026-10-18.md).
//
// It used to be (kbID, versionID), which made the de-duplication trivially
// bypassable: nothing bounds min_version, so a caller that alternated two values had
// two keys and got two goroutines, then three, then nine... each one a four-attempt
// pull with a 5-minute budget. The unit of work is not the version either — a pull
// always runs to the node's current cursor, and EnsureIndex returns as soon as it is
// satisfied — so "one catch-up per knowledge base at a time" is both the correct
// bound and the coarser one. A caller asking for a newer version while a pull runs
// needs nothing extra: the next query re-triggers, and by then the cursor has moved.
type pullKey struct {
	kbID string
}

// QueryOption configures optional capabilities of QueryServiceImpl at construction time.
type QueryOption func(*QueryServiceImpl)

// LocalVersionReporter reports this node's contiguous data cursor for a
// knowledge base — the highest version whose history it holds completely. It is
// what §9.3(2)'s freshness check compares against.
type LocalVersionReporter interface {
	LocalVersionOf(kbID string) int64
}

// WithLocalVersionReporter wires the cursor the freshness check reads. Optional:
// without it the node cannot verify a credential and therefore ignores one,
// which is the pre-§9 behaviour rather than a broken one.
func WithLocalVersionReporter(r LocalVersionReporter) QueryOption {
	return func(s *QueryServiceImpl) { s.localVersion = r }
}

// Backfiller fetches a knowledge base's data on this node up to versionID — the
// storage layer's §7.5 pull, which plane.DataPlane.EnsureIndex already is.
type Backfiller interface {
	EnsureIndex(ctx context.Context, kbID string, versionID int64) error
}

// WithBackfiller wires the pull the freshness check uses to repair itself.
// Optional: without it the check refuses exactly as it did before and the node
// simply never catches up on demand, which is the pre-existing behaviour.
func WithBackfiller(b Backfiller) QueryOption {
	return func(s *QueryServiceImpl) { s.backfiller = b }
}

// IndexFetcher asks the other replicas whether any holds a version's built index,
// and installs the first one that answers. plane.LocalDataPlane's
// FetchIndexFromPeers satisfies it.
//
// It reports installed=false with a nil error when nobody had it — an ordinary
// answer, not a failure, and the caller falls through to building.
type IndexFetcher interface {
	FetchIndexFromPeers(ctx context.Context, kbID string, versionID int64) (installed bool, err error)
}

// WithIndexFetcher wires the on-demand artifact fetch. Optional: without it a
// queried replica builds the index for itself, which is correct — it is the
// fallback — but it skips the cheap route of taking a peer's artifact.
func WithIndexFetcher(f IndexFetcher) QueryOption {
	return func(s *QueryServiceImpl) { s.indexFetcher = f }
}

// WithQueryLogger wires the logger that carries Query per-stage timings.
// Optional: without it those timings go nowhere.
func WithQueryLogger(l *zap.Logger) QueryOption {
	return func(s *QueryServiceImpl) {
		if l != nil {
			s.logger = l
		}
	}
}

// hasLocalIndexArtifact reports whether this node already has the version's index on
// disk, which makes asking a peer pointless.
//
// A failed probe answers false and is not fatal: the fetch it enables is harmless, and
// it is off the request anyway.
func (s *QueryServiceImpl) hasLocalIndexArtifact(ctx context.Context, kbID string, versionID int64) bool {
	if s.indexManager == nil {
		return false
	}
	exists, err := s.indexManager.IndexExists(ctx, kbID, versionID)
	return err == nil && exists
}

// warmIndexInBackground brings a version's index up to date BEHIND the request that
// asked for it: this node's artifact first, then a peer's, then building (§4.2's order,
// the same one plane.ensureOwnIndex uses).
//
// It is asynchronous because the alternative was measured into the query's latency.
// Doing the fetch inline traded a millisecond scan for a download plus a disk write plus
// a load into the vector store — hundreds of megabytes for a large version — to spare the
// NEXT query a cost it would have paid anyway. The bridge (§8.6b) exists precisely so the
// current request does not have to wait for the permanent answer.
//
// Resident comes first, before any goroutine is spawned. The two steps the warm-up ends
// in de-duplicate only work that OVERLAPS in time — plane.FetchIndexFromPeers shares one
// in-flight download, index.triggerBuild ignores a build already running — and the disk
// check cannot see a build until it finishes writing. So without it, a PENDING version
// queried throughout a minutes-long build asked every peer again on every query, and a
// peer that finished its own build meanwhile got its artifact installed over the build
// in flight here. "Loaded or loading" is the index manager's own record of "already in
// hand", which is the right granularity; no separate table is kept here.
//
// It runs on its own context, like startBackgroundPull and for the same reason: tied to
// the caller's ctx, the work is cancelled by the very deadline that made the caller ask.
func (s *QueryServiceImpl) warmIndexInBackground(kbID string, versionID int64) {
	if s.indexManager == nil || s.indexManager.Resident(kbID, versionID) {
		return
	}
	go func() {
		// Its own budget: long enough for a peer download and a build, and independent of
		// any single caller's deadline.
		ctx, cancel := context.WithTimeout(context.Background(), indexWarmupBudget)
		defer cancel()

		if s.hasLocalIndexArtifact(ctx, kbID, versionID) {
			return // nothing to do: TrySearch's loadFromDisk already uses it
		}
		if s.indexFetcher != nil {
			fetched, fetchErr := s.indexFetcher.FetchIndexFromPeers(ctx, kbID, versionID)
			if fetchErr != nil {
				s.logger.Debug("query: background ask for a peer's index artifact failed",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(fetchErr))
			} else if fetched {
				return // installed and loaded; the next query answers from it
			}
		}
		if err := s.indexManager.TriggerBuild(ctx, kbID, versionID); err != nil {
			s.logger.Debug("query: could not schedule a lazy build for a PENDING version",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		}
	}()
}

// indexWarmupBudget bounds one background warm-up. Five minutes: a peer download of a
// large artifact plus a build of the same version, which is the same order as the index
// manager's own build budget.
const indexWarmupBudget = 5 * time.Minute

// startBackgroundPull brings this node's history up to versionID, unless an
// identical pull is already running.
//
// It runs on a context of its own, and that part is deliberate and older than the
// de-duplication: tied to the caller's ctx (as this once was) the pull is
// cancelled by the very deadline that made the caller ask — measured as
// DeadlineExceeded on every attempt, a fraction of the way in, so the node never
// caught up and never answered. Off the request, the caller is told to ask
// someone else (retryably) while the pull runs to completion behind it, and the
// next query is served from here.
//
// What is new here (M1 of docs/参考/code-review-2026-09-24.md) is the de-duplication.
// The pull is idempotent — EnsureIndex re-checks the cursor on every pass and
// returns as soon as it is satisfied — so overlapping pulls bought nothing while
// costing a full transfer each: N concurrent queries on a lagging replica meant N
// goroutines each making a four-attempt pull of the same version, which is an
// amplification anyone able to send queries could ask for.
func (s *QueryServiceImpl) startBackgroundPull(kbID string, versionID int64) {
	if s.backfiller == nil || !s.beginPull(kbID) {
		return
	}
	go func() {
		defer s.endPull(kbID)
		// Bounded retry with backoff, because this is the node's ONLY way back: the
		// write that should have brought the data here reached quorum without it
		// (§7.1) and the announcement that would have asked it to pull is
		// best-effort, so if this attempt fails nothing else will come along. A
		// single attempt is not enough — it can land in a window where the control
		// tier is electing or a peer is restarting, measured on an otherwise healthy
		// cluster as "resolve data source: GetClusterStatus: raft: remote: read at
		// control node 3". One unlucky attempt must not decide between a node that
		// catches up and one that stays behind for good.
		for attempt, backoff := 0, time.Second; attempt < 4; attempt++ {
			// Comfortably longer than EnsureIndex's own 30 s pull loop, so what
			// bounds a pull is the pull, not this wrapper.
			pullCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			err := s.backfiller.EnsureIndex(pullCtx, kbID, versionID)
			cancel()
			if err == nil {
				return
			}
			s.logger.Debug("query: background pull did not complete; will retry",
				zap.String("kb_id", kbID), zap.Int64("want", versionID),
				zap.Int("attempt", attempt+1), zap.Duration("backoff", backoff), zap.Error(err))
			time.Sleep(backoff)
			backoff *= 2
		}
	}()
}

// beginPull registers a catch-up, reporting false when one for the same
// (kbID, versionID) is already in flight. The false case is the whole point: the
// caller then does nothing, which is exactly what the duplicate pull would have
// amounted to.
func (s *QueryServiceImpl) beginPull(kbID string) bool {
	key := pullKey{kbID: kbID}
	s.pullMu.Lock()
	defer s.pullMu.Unlock()
	if s.pulls == nil {
		s.pulls = make(map[pullKey]bool)
	}
	if s.pulls[key] {
		return false
	}
	s.pulls[key] = true
	return true
}

// endPull clears the registration, whichever way the pull finished. Called from a
// defer so a panic in the pull cannot pin the key forever and silence every
// later catch-up for that version.
func (s *QueryServiceImpl) endPull(kbID string) {
	key := pullKey{kbID: kbID}
	s.pullMu.Lock()
	defer s.pullMu.Unlock()
	delete(s.pulls, key)
}

// NewQueryService constructs a QueryServiceImpl with required storage/Raft
// dependencies and optional QueryOption capabilities.
func NewQueryService(
	rn raft.MetaReader,
	im index.IndexManager,
	cdm chunkdoc.ChunkDocMapper,
	vdl versiondoc.VersionDocList,
	ds docstore.DocStore,
	versionViews *versionview.Store,
	opts ...QueryOption,
) *QueryServiceImpl {
	if versionViews == nil {
		versionViews = versionview.NewStore(versionview.Config{
			VersionDocList: vdl,
			ChunkDocMapper: cdm,
		})
	}
	s := &QueryServiceImpl{
		raftNode:       rn,
		indexManager:   im,
		chunkDocMapper: cdm,
		versionDocList: vdl,
		docStore:       ds,
		versionViews:   versionViews,
		logger:         zap.NewNop(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// MaxQueryTopK bounds the caller-supplied top_k on Query.
//
// It is a request-size bound, not a product opinion: top_k arrives as an int32
// straight off the wire and is used to size allocations on this path
// (`make([]scoredDoc, 0, topK)` and the search's candidate count), so an
// unbounded value is a remote OOM — top_k = 2e9 asks for tens of GB in one
// make(), which panics and, past the runtime's patience, aborts the process.
// The ceiling sits far above anything the system can serve anyway: the search
// headroom is top_k × 3 clamped at 1000, so past ~333 the extra results cannot
// come from anywhere.
const MaxQueryTopK = 1024

// Query implements QueryServiceServer.
func (s *QueryServiceImpl) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	kbID := req.KnowledgeBaseId

	// Bound the caller's top_k before anything is sized from it. This is the
	// entry to a client-facing call, and every client-supplied number below is
	// either an allocation size or a loop bound, so the check belongs here and
	// not at the point of use: a value that is refused costs one error, a value
	// that reaches make() costs the node.
	//
	// top_k = 0 is refused rather than treated as "default": it is what a caller
	// that forgot the field sends, and answering it with an empty result — which
	// is what the old code did — hides the mistake instead of naming it.
	topK := int(req.GetTopK())
	if topK < 1 || topK > MaxQueryTopK {
		return nil, status.Errorf(codes.InvalidArgument,
			"query: top_k must be in [1, %d], got %d", MaxQueryTopK, topK)
	}

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
	vcmHit := versionview.HitNone
	defer func() {
		s.logger.Debug("query: stage timings",
			zap.String("kb_id", kbID),
			zap.Int("top_k", int(req.GetTopK())),
			zap.Int("candidates", candidateCount),
			zap.Int("matched_docs", matched),
			zap.Int("chunkmap_calls", chunkMapCalls),
			zap.Int("read_calls", readCalls),
			zap.String("vcm_hit", string(vcmHit)),
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
				//
				// M1 of docs/参考/code-review-2026-09-24.md: "harmless" was true of the
				// WORK but not of the cost. Every query that missed on the same
				// (kbID, version) started its own goroutine and its own four-attempt
				// full pull, so N concurrent queries meant N pulls of the same
				// version — RPC and IO amplification an unauthenticated caller could
				// ask for at will. startBackgroundPull keeps exactly one in flight.
				s.startBackgroundPull(kbID, want)
			}
			if have = s.localVersion.LocalVersionOf(kbID); have < want {
				// The wire name keeps it retryable for a caller with another replica
				// to ask, the message keeps the diagnosis, and the structured cursor
				// lets the station remember how far behind this node is. A bare
				// sentinel throws away the only information that makes the refusal
				// actionable (see TestQueryService_RefusesAStaleNodeForAFreshness…).
				return nil, stratumerrors.ToGRPCStatus(&stratumerrors.ReplicaBehindError{
					KBID: kbID, Have: have, Want: want,
				})
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
			// (docs/已实施/cursor-persistence-plan.md §5.1): a version now comes into
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

	// The data side's terminal verdict comes FIRST — before the PENDING branch and
	// before the index checks — because the two halves of a version settle
	// independently (§10.1b): a version whose data was declared lost is still
	// PENDING on the index side, so a reader that only looked at IndexStatus would
	// answer "still writing, ask again" for a write that is never coming. Every
	// retryable answer is a reason for the station to try another replica and for
	// the caller to keep polling, and none of that can ever help here.
	if targetVersion.DataStatus == types.DataStatusFailedPermanent {
		return nil, stratumerrors.ToGRPCStatus(fmt.Errorf(
			"%w: %s: version %d's data was declared lost, so it will never be queryable (write it again under a new client_request_id)",
			stratumerrors.ErrVersionDataFailed, kbID, versionID))
	}

	// Search: get multi × topK chunk results for aggregation headroom.
	//
	// Sized here rather than immediately before the search because §8.6b's scan
	// below spends the same budget: an answer from the chunks has to return the
	// candidate set the index would have, or one version's answers differ in
	// recall depending on whether its index happened to be built.
	searchTopK := topK * 3
	if searchTopK < 10 {
		searchTopK = 10
	}
	if searchTopK > 1000 {
		searchTopK = 1000
	}

	// searchResults carries this request's candidate set. It no longer comes from
	// exactly one call: a PENDING version can be answered by a scan (§8.6b), so
	// resolved records that the set is already decided — possibly by an error the
	// shared handling further down still has to classify.
	var (
		searchResults []types.SearchResult
		searchErr     error
		resolved      bool
	)

	if targetVersion.IndexStatus == types.IndexStatusPending {
		// The PENDING gate reads the DATA side, which is the point of §8.6(b): a
		// version is answerable when its data is there, not when an index nobody
		// has asked for has been built. IndexStatus stays PENDING from creation
		// until a build reports READY, so it covers two very different states —
		// "the write has not landed" and "the data is durable, the index is a
		// read-side product that does not exist yet" — and only the first one has
		// to be refused.
		//
		// The plan, in order:
		//
		//   1. Data not durable → refuse, and do NOT warm. A build reads whatever
		//      documents and chunks are here, and build() does not check the data
		//      side, so warming now could produce an index over part of the write,
		//      report it READY and persist it — after which IndexExists answers
		//      "present" and nothing ever repairs it (a peer could even fetch it).
		//      Nothing is lost by waiting: the first query after the data turns
		//      durable takes step 2 and warms then.
		//   2. TrySearch: the loaded index, the artifact on disk, or — at or below
		//      BruteForceMaxChunks — a scan (§8.6b). It never waits for a build.
		//      - A clean answer: serve it, and warm the index BEHIND it. The scan is
		//        a bridge, not a replacement.
		//      - A verdict (answered with an error: the version is deleted or in
		//        maintenance, it has no chunks, or this replica lacks its vectors):
		//        hand it to the shared classification below and do NOT warm. None
		//        of those is cured by a build; most would make one wrong (a
		//        tombstoned version marked BUILDING, a build that fails on vectors
		//        this replica does not have yet, a peer round per query for an
		//        empty version that will never have an artifact).
		//   3. Not answerable now (too large to scan): warm, and refuse retryably.
		//      The station moves on to the next candidate, which under owner
		//      sharding is another owner that may already hold the index; by the
		//      time it comes back here the warm-up has made progress.
		//
		// Why refusing still has to warm: THIS request is the "somebody is waiting
		// for it" signal §8.6b's lazy build exists for. Refusing without asking for
		// the build is what once made PENDING permanent (TestTwoTier's `version is
		// PENDING` never cleared inside its 180 s window).
		//
		// Why the warm-up is never on the request: it applies the index's two-step
		// rule (§4.2: this node's artifact, then a peer's, then building), and a
		// peer install is a download plus a disk write plus a load into the vector
		// store — seconds for a large version, against a millisecond scan.
		//
		// The refusal carries a named, retryable sentinel. A bare status.Error has no
		// wire name, so the station's isRetryableErr reads it as terminal and never
		// tries the replica whose index IS ready (measured: that left every storage
		// node circuit-broken while a sibling was serving the version).
		// ErrVersionPending rather than ErrIndexNotReady because it names the state.
		if targetVersion.DataStatus != types.DataStatusDurable {
			return nil, stratumerrors.ToGRPCStatus(stratumerrors.ErrVersionPending)
		}

		scanStart := time.Now()
		scanned, answered, scanErr := s.indexManager.TrySearch(ctx, kbID, versionID, req.Vector, searchTopK)
		stageSearch = time.Since(scanStart)
		switch {
		case answered && scanErr == nil:
			searchResults, resolved = scanned, true
			s.warmIndexInBackground(kbID, versionID)
		case answered:
			searchResults, searchErr, resolved = scanned, scanErr, true
		default:
			s.warmIndexInBackground(kbID, versionID)
			return nil, stratumerrors.ToGRPCStatus(stratumerrors.ErrVersionPending)
		}
	}
	if targetVersion.IndexStatus.IsFailed() {
		// Named, not a bare status.Error: the gRPC code was always
		// FailedPrecondition, but without a wire name the station cannot classify
		// it and a caller cannot branch on it — the same reason the PENDING branch
		// above spells out its sentinel. FAILED and FAILED_PERMANENT travel as the
		// same reason (both are "this version's index is not usable and no retry of
		// THIS node helps"); the status in the message says which.
		return nil, stratumerrors.ToGRPCStatus(fmt.Errorf("%w: %s: version %d's index is %s",
			stratumerrors.ErrVersionFailed, kbID, versionID, targetVersion.IndexStatus))
	}

	if !resolved {
		searchStart := time.Now()
		searchResults, searchErr = s.indexManager.Search(ctx, kbID, versionID, req.Vector, searchTopK)
		stageSearch = time.Since(searchStart)
	}
	candidateCount = len(searchResults)
	if searchErr != nil {
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
		if errors.Is(searchErr, stratumerrors.ErrIndexNotReady) && targetVersion.DocIDSetHash == types.EmptyDocIDSetHash {
			return &pb.QueryResponse{Results: nil, VersionId: versionID}, nil
		}
		return nil, stratumerrors.ToGRPCStatus(searchErr)
	}

	// Filter by threshold.
	threshold := float32(0)
	if req.Threshold != nil {
		threshold = *req.Threshold
	}

	viewStart := time.Now()
	view, hit, viewErr := s.versionViews.Lookup(ctx, kbID, versionID, targetVersion.DocIDSetHash)
	stageDocList = time.Since(viewStart)
	stageBloom = stageDocList
	vcmHit = hit
	if viewErr != nil && errors.Is(viewErr, stratumerrors.ErrIndexNotReady) {
		return nil, stratumerrors.ToGRPCStatus(viewErr)
	}
	if viewErr == nil && len(view.DocIDs) == 0 && targetVersion.DocIDSetHash != types.EmptyDocIDSetHash {
		return nil, stratumerrors.ToGRPCStatus(fmt.Errorf(
			"%w: %s: version %d's document set is not on this replica yet (its committed digest is not the empty set's)",
			stratumerrors.ErrIndexNotReady, kbID, versionID))
	}
	if viewErr == nil && len(view.DocIDs) == 0 {
		return &pb.QueryResponse{Results: nil, VersionId: versionID}, nil
	}

	agg := req.Aggregation
	type scoredDoc struct {
		docID   string
		score   float32
		content string
	}

	results := make([]scoredDoc, 0, topK)
	if viewErr == nil {
		var idxCandidates []scoredIdx
		idxCandidates, stageFilter, stageAgg = filterAndAggregateFromView(searchResults, threshold, agg, view)
		matched = len(idxCandidates)

		readStart := time.Now()
		for _, cand := range idxCandidates {
			if len(results) >= topK {
				break
			}
			docID := view.DocIDs[cand.docIdx]
			readCalls++
			content, err := s.docStore.ReadAt(ctx, kbID, docID, versionID)
			if err != nil {
				continue
			}
			results = append(results, scoredDoc{docID: docID, score: cand.score, content: string(content)})
		}
		stageRead = time.Since(readStart)
	} else {
		// Conservative fallback when VersionDocList itself fails with a real
		// underlying I/O error: skip version filtering and fall back to
		// ChunkDocMapper.ListDocIDs so transient store read issues do not drop
		// valid documents silently.
		var candidates []scoredFallback
		candidates, stageFilter, stageChunkMap, chunkMapCalls, stageAgg = s.filterAndAggregateFallback(ctx, kbID, searchResults, threshold, agg)
		matched = len(candidates)

		readStart := time.Now()
		for _, cand := range candidates {
			if len(results) >= topK {
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
	}

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

const denseDocIndexThreshold = 65536

type docHit struct {
	docIdx uint32
	first  float32
	extra  []float32
}

type scoredIdx struct {
	docIdx uint32
	score  float32
}

type scoredFallback struct {
	docID string
	score float32
}

func filterAndAggregateFromView(
	searchResults []types.SearchResult,
	threshold float32,
	agg pb.AggregationMethod,
	view *versionview.View,
) ([]scoredIdx, time.Duration, time.Duration) {
	filterStart := time.Now()
	numDocs := len(view.DocIDs)
	hits := make([]docHit, 0, min(len(searchResults)*2, numDocs))

	if numDocs <= denseDocIndexThreshold {
		// 1-based slot index so Go's zero-initialized slice means "unseen".
		slots := make([]int32, numDocs)
		for _, r := range searchResults {
			if r.Score < threshold {
				continue
			}
			for _, docIdx := range view.ChunkToDocs[r.ChunkID] {
				if int(docIdx) >= numDocs {
					continue
				}
				slot := slots[docIdx]
				if slot == 0 {
					hits = append(hits, docHit{docIdx: docIdx, first: r.Score})
					slots[docIdx] = int32(len(hits))
				} else {
					h := &hits[slot-1]
					h.extra = append(h.extra, r.Score)
				}
			}
		}
	} else {
		slots := make(map[uint32]int32, min(len(searchResults)*2, 1024))
		for _, r := range searchResults {
			if r.Score < threshold {
				continue
			}
			for _, docIdx := range view.ChunkToDocs[r.ChunkID] {
				if int(docIdx) >= numDocs {
					continue
				}
				slot := slots[docIdx]
				if slot == 0 {
					hits = append(hits, docHit{docIdx: docIdx, first: r.Score})
					slots[docIdx] = int32(len(hits))
				} else {
					h := &hits[slot-1]
					h.extra = append(h.extra, r.Score)
				}
			}
		}
	}
	filterDur := time.Since(filterStart)

	aggStart := time.Now()
	candidates := make([]scoredIdx, len(hits))
	var scratch []float32
	for i := range hits {
		h := &hits[i]
		var score float32
		if len(h.extra) == 0 {
			score = h.first
		} else {
			scratch = append(scratch[:0], h.first)
			scratch = append(scratch, h.extra...)
			score = aggregate(scratch, agg)
		}
		candidates[i] = scoredIdx{docIdx: h.docIdx, score: score}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].score > candidates[j].score })
	aggDur := time.Since(aggStart)
	return candidates, filterDur, aggDur
}

func (s *QueryServiceImpl) filterAndAggregateFallback(
	ctx context.Context,
	kbID string,
	searchResults []types.SearchResult,
	threshold float32,
	agg pb.AggregationMethod,
) ([]scoredFallback, time.Duration, time.Duration, int, time.Duration) {
	type docScore struct {
		first float32
		extra []float32
	}
	docMap := make(map[string]*docScore)
	var stageChunkMap time.Duration
	chunkMapCalls := 0

	filterStart := time.Now()
	for _, r := range searchResults {
		if r.Score < threshold {
			continue
		}
		chunkMapStart := time.Now()
		docIDs, err := s.chunkDocMapper.ListDocIDs(ctx, kbID, r.ChunkID)
		stageChunkMap += time.Since(chunkMapStart)
		chunkMapCalls++
		if err != nil {
			continue
		}
		for _, docID := range docIDs {
			if ds, ok := docMap[docID]; ok {
				ds.extra = append(ds.extra, r.Score)
			} else {
				docMap[docID] = &docScore{first: r.Score}
			}
		}
	}
	filterDur := time.Since(filterStart)

	aggStart := time.Now()
	candidates := make([]scoredFallback, 0, len(docMap))
	var scratch []float32
	for docID, ds := range docMap {
		var score float32
		if len(ds.extra) == 0 {
			score = ds.first
		} else {
			scratch = append(scratch[:0], ds.first)
			scratch = append(scratch, ds.extra...)
			score = aggregate(scratch, agg)
		}
		candidates = append(candidates, scoredFallback{docID: docID, score: score})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].score > candidates[j].score })
	aggDur := time.Since(aggStart)
	return candidates, filterDur, stageChunkMap, chunkMapCalls, aggDur
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
