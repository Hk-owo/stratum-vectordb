package service

import (
	"context"
	"errors"
	"sort"
	"testing"

	pb "stratum/api/proto/stratum"
	"stratum/internal/bloom"
	stratumerrors "stratum/internal/errors"
	stratinternalsync "stratum/internal/sync"
	"stratum/internal/versiondoc"
)

// countingVDL counts ListDocIDs calls on top of a real VersionDocList.
type countingVDL struct {
	versiondoc.VersionDocList
	calls int
}

func (c *countingVDL) ListDocIDs(ctx context.Context, kbID string, versionID int64) ([]string, error) {
	c.calls++
	return c.VersionDocList.ListDocIDs(ctx, kbID, versionID)
}

// stagedVDL answers successive ListDocIDs calls from a script, so a test can
// reproduce "the replica had not received the data yet, then it did".
type stagedVDL struct {
	stages [][]string
	calls  int
}

func (s *stagedVDL) Write(context.Context, string, int64, string) error { return nil }
func (s *stagedVDL) ListVersions(context.Context, string) ([]int64, error) {
	return nil, nil
}
func (s *stagedVDL) DeleteByVersion(context.Context, string, int64) error { return nil }
func (s *stagedVDL) DeleteByKB(context.Context, string) error             { return nil }
func (s *stagedVDL) WriteMany(context.Context, string, int64, []string) error {
	return nil
}

func (s *stagedVDL) ListDocIDs(context.Context, string, int64) ([]string, error) {
	i := s.calls
	if i >= len(s.stages) {
		i = len(s.stages) - 1
	}
	s.calls++
	return s.stages[i], nil
}

func seedVDL(t *testing.T, kbID string, versionID int64, ids []string) versiondoc.VersionDocList {
	t.Helper()
	vdl := versiondoc.NewMockVersionDocList()
	for _, id := range ids {
		if err := vdl.Write(context.Background(), kbID, versionID, id); err != nil {
			t.Fatalf("Write(%s,%d,%s): %v", kbID, versionID, id, err)
		}
	}
	return vdl
}

// TestDocSetCache_ReusesAVerifiedSet is the cache's whole point: a set that hashes
// to the committed digest is read from the store once and then served from memory.
//
// 撤掉即变红: without the cache in lookup, calls would be 2.
func TestDocSetCache_ReusesAVerifiedSet(t *testing.T) {
	ctx := context.Background()
	ids := []string{"d1", "d2", "d3"}
	vdl := seedVDL(t, "kb-1", 7, ids)
	digest := stratinternalsync.ComputeDocIDSetHash(ids)

	src := &countingVDL{VersionDocList: vdl}
	var c docSetCache

	first, err := c.lookup(ctx, src, "kb-1", 7, digest)
	if err != nil {
		t.Fatalf("first lookup: %v", err)
	}
	if src.calls != 1 {
		t.Fatalf("first lookup read the store %d times, want 1", src.calls)
	}
	assertSameSet(t, first.ids, ids)
	if _, ok := first.docs["d2"]; !ok {
		t.Fatalf("membership map is missing d2: %v", first.docs)
	}

	second, err := c.lookup(ctx, src, "kb-1", 7, digest)
	if err != nil {
		t.Fatalf("second lookup: %v", err)
	}
	if src.calls != 1 {
		t.Fatalf("a verified set must be served from the cache; the store was read %d times", src.calls)
	}
	assertSameSet(t, second.ids, ids)
	if len(second.docs) != len(first.docs) {
		t.Fatalf("cached membership map = %v, want %v", second.docs, first.docs)
	}
	// The fingerprint travels with the set: a hit must hand the bloom store the very
	// value a rebuild would produce, or the store would rebuild a correct filter.
	if second.fingerprint != first.fingerprint {
		t.Fatalf("cached fingerprint = %d, want %d", second.fingerprint, first.fingerprint)
	}
	if want := bloom.DocumentSetFingerprint(ids); second.fingerprint != want {
		t.Fatalf("fingerprint = %d, want DocumentSetFingerprint(ids) = %d", second.fingerprint, want)
	}
}

// TestDocSetCache_HitDoesNotRefingerprint is the other half of the cache's job: a hit
// must not re-derive the set's fingerprint. That derivation is O(documents) (a copy
// plus a sort of the whole set) and used to run on EVERY query, because the bloom
// store needed a fingerprint to check its cached filter against and was handed nothing
// but the IDs. Now it is derived once, on the miss, and cached next to the set.
//
// 撤掉即变红: deriving the fingerprint on the hit path would make calls 2.
func TestDocSetCache_HitDoesNotRefingerprint(t *testing.T) {
	ctx := context.Background()
	ids := []string{"d1", "d2", "d3"}
	vdl := seedVDL(t, "kb-1", 7, ids)
	digest := stratinternalsync.ComputeDocIDSetHash(ids)

	calls := 0
	restore := documentSetFingerprint
	documentSetFingerprint = func(got []string) uint64 {
		calls++
		return restore(got)
	}
	t.Cleanup(func() { documentSetFingerprint = restore })

	src := &countingVDL{VersionDocList: vdl}
	var c docSetCache

	first, err := c.lookup(ctx, src, "kb-1", 7, digest)
	if err != nil {
		t.Fatalf("first lookup: %v", err)
	}
	if calls != 1 {
		t.Fatalf("a miss derived the fingerprint %d times, want 1", calls)
	}

	for i := 0; i < 3; i++ {
		hit, err := c.lookup(ctx, src, "kb-1", 7, digest)
		if err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
		if hit.fingerprint != first.fingerprint {
			t.Fatalf("hit %d returned fingerprint %d, want %d", i, hit.fingerprint, first.fingerprint)
		}
	}
	if calls != 1 {
		t.Fatalf("cache hits re-derived the fingerprint; %d derivations for 1 miss and 3 hits, want 1", calls)
	}
	if src.calls != 1 {
		t.Fatalf("the store was read %d times, want 1", src.calls)
	}
}

// TestQueryService_WarmQueryDoesNotRefingerprintTheDocumentSet: the document-set cache
// must be REUSED across requests, not rebuilt per request — the same version asked three
// times derives the set's fingerprint once, on the first cold read. That cached set is
// what the second and third queries hand the bloom store, fingerprint included, which is
// what lets the store skip deriving it (pinned on the store's side by
// TestVersionBloomStore_FingerprintIsDerivedOnlyWhenTheCallerHasNone).
//
// 撤掉即变红: a per-request cache (a docSetCache built inside Query) would make calls 3.
func TestQueryService_WarmQueryDoesNotRefingerprintTheDocumentSet(t *testing.T) {
	ctx := context.Background()
	h := newQuerySvcHarness(t)
	kbID, vID := h.setupQueryableKB(t, map[string][]string{
		"doc-a": {"chunk-1"},
		"doc-b": {"chunk-2"},
	})

	// Commit the document-set digest. Without one the read is unverifiable, and an
	// unverifiable set is deliberately never cached (see docSetCache) — such a replica
	// keeps paying the read, exactly as it did before the cache existed.
	ids, err := h.versionDocs.ListDocIDs(ctx, kbID, vID)
	if err != nil {
		t.Fatalf("ListDocIDs: %v", err)
	}
	if err := h.raftNode.ProposeUpdateVersionSummary(ctx, vID, stratinternalsync.ComputeDocIDSetHash(ids)); err != nil {
		t.Fatalf("ProposeUpdateVersionSummary: %v", err)
	}

	calls := 0
	restore := documentSetFingerprint
	documentSetFingerprint = func(got []string) uint64 {
		calls++
		return restore(got)
	}
	t.Cleanup(func() { documentSetFingerprint = restore })

	for i := 0; i < 3; i++ {
		if _, err := h.svc.Query(ctx, &pb.QueryRequest{
			KnowledgeBaseId: kbID,
			VersionId:       &vID,
			Vector:          []float32{0.1, 0.2, 0.3, 0.4},
			TopK:            2,
		}); err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
	}
	if calls != 1 {
		t.Fatalf("3 queries derived the document-set fingerprint %d times, want 1 (the cold read)", calls)
	}
}

// TestDocSetCache_DoesNotCacheAnUnverifiedRead is the safety property: a read that
// does NOT hash to the committed digest is not the version's set (the replica is
// missing data, or has stale data). It is REFUSED retryably, and it is never cached —
// caching it would answer wrongly forever, and returning it would filter search hits
// against a set that is not the version's.
//
// 撤掉即变红: caching unconditionally would make calls 1 instead of 2.
func TestDocSetCache_DoesNotCacheAnUnverifiedRead(t *testing.T) {
	ctx := context.Background()
	vdl := seedVDL(t, "kb-1", 7, []string{"d1", "d2"})

	src := &countingVDL{VersionDocList: vdl}
	var c docSetCache

	for i := 0; i < 3; i++ {
		if _, err := c.lookup(ctx, src, "kb-1", 7, "digest-that-does-not-match"); !errors.Is(err, stratumerrors.ErrIndexNotReady) {
			t.Fatalf("lookup %d = %v, want ErrIndexNotReady", i, err)
		}
	}
	if src.calls != 3 {
		t.Fatalf("an unverified set must never be cached; the store was read %d times, want 3", src.calls)
	}
}

// TestDocSetCache_NoCommittedDigestIsNotCached pins the second uncacheable case:
// before the control layer commits a digest there is nothing to verify against, so
// the read is repeated (which is what the pre-cache code always did).
func TestDocSetCache_NoCommittedDigestIsNotCached(t *testing.T) {
	ctx := context.Background()
	vdl := seedVDL(t, "kb-1", 7, []string{"d1"})

	src := &countingVDL{VersionDocList: vdl}
	var c docSetCache

	for i := 0; i < 2; i++ {
		if _, err := c.lookup(ctx, src, "kb-1", 7, ""); err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
	}
	if src.calls != 2 {
		t.Fatalf("with no committed digest the store must be re-read; got %d reads, want 2", src.calls)
	}
}

// TestDocSetCache_AnEarlyEmptyReadNeverSticks is the §7.4 failure this cache must
// not reintroduce: a replica that reads the version's set before its data lands
// sees an empty (or partial) set. That read must not be memoized, so once the data
// arrives the replica serves the real set — the alternative is a replica answering
// "nothing matched" long after its data had arrived, with nothing to correct it.
//
// 撤掉即变红: caching the first read would leave the second lookup returning the
// empty set instead of the documents that had arrived.
func TestDocSetCache_AnEarlyEmptyReadNeverSticks(t *testing.T) {
	ctx := context.Background()
	full := []string{"d1", "d2", "d3"}
	digest := stratinternalsync.ComputeDocIDSetHash(full)

	// First call: nothing here yet. Second and later: the data has arrived.
	src := &stagedVDL{stages: [][]string{nil, full}}
	var c docSetCache

	// The early read is refused, not returned: an empty set that does not hash to the
	// committed digest is "this replica is incomplete", and a caller must not filter
	// against it.
	if _, err := c.lookup(ctx, src, "kb-1", 7, digest); !errors.Is(err, stratumerrors.ErrIndexNotReady) {
		t.Fatalf("lookup 1 = %v, want ErrIndexNotReady", err)
	}

	set, err := c.lookup(ctx, src, "kb-1", 7, digest)
	if err != nil {
		t.Fatalf("lookup 2: %v", err)
	}
	assertSameSet(t, set.ids, full)
	if len(set.docs) != len(full) {
		t.Fatalf("membership map = %v, want %d documents", set.docs, len(full))
	}

	// And now that it IS verified, it is cached: the store is not read again.
	before := src.calls
	if _, err := c.lookup(ctx, src, "kb-1", 7, digest); err != nil {
		t.Fatalf("lookup 3: %v", err)
	}
	if src.calls != before {
		t.Fatalf("the verified set must be cached; the store was read again (%d -> %d)", before, src.calls)
	}
}

// TestDocSetCache_ADifferentDigestMisses: a version whose set changed (or a
// different version reusing the slot) must not be served the old set.
func TestDocSetCache_ADifferentDigestMisses(t *testing.T) {
	ctx := context.Background()
	ids := []string{"d1"}
	vdl := seedVDL(t, "kb-1", 7, ids)

	src := &countingVDL{VersionDocList: vdl}
	var c docSetCache

	if _, err := c.lookup(ctx, src, "kb-1", 7, stratinternalsync.ComputeDocIDSetHash(ids)); err != nil {
		t.Fatalf("lookup: %v", err)
	}
	// A digest the local set does not hash to is a refusal (this replica cannot serve
	// the version), and it must not be cached either.
	if _, err := c.lookup(ctx, src, "kb-1", 7, "some-other-digest"); !errors.Is(err, stratumerrors.ErrIndexNotReady) {
		t.Fatalf("lookup with a different digest = %v, want ErrIndexNotReady", err)
	}
	if src.calls != 2 {
		t.Fatalf("a different digest must miss; got %d reads, want 2", src.calls)
	}
}

// TestDocSetCache_EvictsOldVersions pins the bound: the cache holds
// docSetCacheCap entries, oldest first.
func TestDocSetCache_EvictsOldVersions(t *testing.T) {
	ctx := context.Background()
	var c docSetCache
	for i := 0; i < docSetCacheCap+1; i++ {
		ids := []string{"d1"}
		vdl := seedVDL(t, "kb-1", 100+int64(i), ids)
		if _, err := c.lookup(ctx, &countingVDL{VersionDocList: vdl}, "kb-1", 100+int64(i),
			stratinternalsync.ComputeDocIDSetHash(ids)); err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
	}
	c.mu.Lock()
	size := len(c.entries)
	order := append([]docSetKey(nil), c.order...)
	c.mu.Unlock()
	if size != docSetCacheCap {
		t.Fatalf("cache holds %d entries, want %d", size, docSetCacheCap)
	}
	if len(order) == 0 {
		t.Fatal("cache eviction order is empty")
	}
	if order[0].versionID != 101 {
		t.Fatalf("oldest surviving entry is version %d, want 100 evicted and 101 the oldest left", order[0].versionID)
	}
}

func assertSameSet(t *testing.T, got, want []string) {
	t.Helper()
	g := append([]string(nil), got...)
	sort.Strings(g)
	w := append([]string(nil), want...)
	sort.Strings(w)
	if len(g) != len(w) {
		t.Fatalf("set = %v, want %v", g, w)
	}
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("set = %v, want %v", g, w)
		}
	}
}
