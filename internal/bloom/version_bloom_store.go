package bloom

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"stratum/internal/versiondoc"
)

// bloomHeaderLen is the size of the fingerprint prefix every persisted filter now
// carries: the document set the filter was built from, so a filter found on disk can
// be checked against the version's CURRENT set instead of being trusted because it
// exists. A file written before this prefix existed fails the length check and is
// treated as a miss — rebuilding is always safe, since the filter is derived data.
const bloomHeaderLen = 8

// bloomEntry is one cached filter plus the fingerprint of the document set it was
// built from. The fingerprint is what makes a stale filter DETECTABLE: the filter is
// only trusted while the version's document set still hashes to it.
type bloomEntry struct {
	filter BloomFilter
	fp     uint64
}

// documentSetFingerprint fingerprints a version's document set, order-insensitively:
// the set arrives from a prefix scan whose order is not part of any contract, and the
// write path forms it from its own changes, so two callers describing the same set
// must agree.
func documentSetFingerprint(docIDs []string) uint64 {
	ids := make([]string, len(docIDs))
	copy(ids, docIDs)
	sort.Strings(ids)

	h := fnv.New64a()
	for _, id := range ids {
		_, _ = h.Write([]byte(id))
		_, _ = h.Write([]byte{0}) // separator: "ab","c" must not hash like "a","bc"
	}
	return h.Sum64()
}

// versionKey identifies a single version's document bloom filter within a
// knowledge base.
type versionKey struct {
	kbID      string
	versionID int64
}

// VersionBloomStore manages the per-version document bloom filters
// (Stratum_设计文档v10.md "版本文档布隆过滤器": one filter per version,
// containing the full document ID set of that version).
//
// Lifecycle:
//   - Write path: WriteCoordinator builds the filter for the new version
//     from its full document-ID set and calls BuildAndPersist, which
//     caches it and writes it to disk.
//   - Read path: QueryService calls Get for the target version. On a
//     cache miss the filter is loaded from disk; if the on-disk copy is
//     missing or corrupt (e.g. a crash mid-write), it is rebuilt from the
//     authoritative source — the version's VersionDocList — and persisted
//     again, so a process restart never strands a version without its
//     filter.
//
// The filter is a pure accelerator: it is never the source of truth. A
// missing or stale filter degrades the read path to authoritative
// VersionDocList confirmations only, never to wrong results (Test has no
// false negatives).
type VersionBloomStore struct {
	dir           string
	expectedItems uint
	fpRate        float64
	vdl           versiondoc.VersionDocList

	mu    sync.Mutex
	cache map[versionKey]bloomEntry
}

// NewVersionBloomStore constructs a VersionBloomStore persisting filters
// under dir/bloom-version/<kbID>/<versionID>.bloom. vdl is used to rebuild
// a filter from its version's document list when no disk copy exists.
func NewVersionBloomStore(dir string, expectedItems uint, fpRate float64, vdl versiondoc.VersionDocList) *VersionBloomStore {
	return &VersionBloomStore{
		dir:           dir,
		expectedItems: expectedItems,
		fpRate:        fpRate,
		vdl:           vdl,
		cache:         make(map[versionKey]bloomEntry),
	}
}

// Get returns the document bloom filter for (kbID, versionID), correct for the
// version's CURRENT document set — from the cache when that set is unchanged, else
// rebuilt from the VersionDocList (the authoritative source) and persisted. The
// returned filter must not be mutated by the caller.
//
// Correct is the load-bearing word. A filter is only usable for the document set it
// was built from, and a filter built BEFORE a version's documents arrived holds
// nothing — an empty filter does not merely filter imprecisely, it rejects every
// hit. So a replica asked early (a restart catching up, a push racing a query) would
// answer "nothing matched" for a version it holds in full, with no error for the
// caller to retry on. That state used to be permanent: the filter was cached in
// memory and on disk and never re-derived, so the replica kept answering empty long
// after its data had arrived.
func (s *VersionBloomStore) Get(ctx context.Context, kbID string, versionID int64) (BloomFilter, error) {
	docIDs, err := s.vdl.ListDocIDs(ctx, kbID, versionID)
	if err != nil {
		return nil, fmt.Errorf("bloom: VersionBloomStore.Get(%s,%d): read the version's documents: %w", kbID, versionID, err)
	}
	return s.GetForDocuments(kbID, versionID, docIDs), nil
}

// GetForDocuments is Get for a caller that has already read the version's document
// set. The query path reads it anyway (it is the authoritative membership test that
// confirms a bloom hit), so handing the same list over keeps the filter and the
// membership test derived from one snapshot — and saves a second prefix scan.
func (s *VersionBloomStore) GetForDocuments(kbID string, versionID int64, docIDs []string) BloomFilter {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := versionKey{kbID: kbID, versionID: versionID}
	fp := documentSetFingerprint(docIDs)
	if e, ok := s.cache[key]; ok && e.fp == fp {
		return e.filter
	}

	// A disk copy is used only when it carries the SAME fingerprint. Anything else
	// is a filter for a document set this version no longer has, and using it would
	// drop hits that are really there.
	if f, diskFP, err := s.loadFromDisk(kbID, versionID); err == nil && diskFP == fp {
		s.cache[key] = bloomEntry{filter: f, fp: fp}
		return f
	}

	f := s.build(docIDs)
	if err := s.persist(kbID, versionID, fp, f); err != nil {
		// A persistence failure is non-fatal: the filter is still usable in memory,
		// and the next call retries the disk write.
		_ = err
	}
	s.cache[key] = bloomEntry{filter: f, fp: fp}
	return f
}

// BuildAndPersist builds a document bloom filter from docIDs, caches it
// and writes it to disk. Used by the write path once a version's full
// document-ID set is known. Idempotent: a repeated call for the same
// version overwrites the on-disk copy with an identical filter.
func (s *VersionBloomStore) BuildAndPersist(kbID string, versionID int64, docIDs []string) (BloomFilter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.build(docIDs)
	fp := documentSetFingerprint(docIDs)
	if err := s.persist(kbID, versionID, fp, f); err != nil {
		return nil, err
	}
	s.cache[versionKey{kbID: kbID, versionID: versionID}] = bloomEntry{filter: f, fp: fp}
	return f, nil
}

// DeleteByVersion removes (kbID, versionID)'s bloom filter: the cached
// entry and its on-disk file. Used by the DeleteVersion cleanup — until now
// only whole-KB deletion reclaimed these files, so one file per deleted
// version lingered forever.
//
// A missing file is not an error, so re-running the version-delete flow
// after a crash is safe. The filter is a pure accelerator (it is rebuilt
// from the version's VersionDocList on demand), so dropping it can never
// change query results.
//
// Note: a later Get on the same (kbID, versionID) would rebuild an empty
// filter from the by-then-deleted VersionDocList and persist it again. That
// is why the DeleteVersion cleanup runs this step only after the version's
// metadata has been removed: with the version gone from the state machine no
// request can reach it, and version IDs are never reused (the Raft counter is
// monotonic).
func (s *VersionBloomStore) DeleteByVersion(kbID string, versionID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.cache, versionKey{kbID: kbID, versionID: versionID})
	if s.dir == "" {
		return nil // persistence unconfigured: nothing on disk to remove
	}
	if err := os.Remove(s.filterPath(kbID, versionID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("bloom: VersionBloomStore.DeleteByVersion(%s,%d): %w", kbID, versionID, err)
	}
	return nil
}

// DeleteByKB removes kbID's on-disk version-bloom files entirely
// (dir/bloom-version/<kbID>/) and drops its filters from the cache. A
// missing directory is not an error, so re-running the knowledge-base
// delete flow after a crash is safe. Used by DeleteKnowledgeBase cleanup
// (Stratum_设计文档v10.md "删除知识库" 第 4 步).
func (s *VersionBloomStore) DeleteByKB(kbID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k := range s.cache {
		if k.kbID == kbID {
			delete(s.cache, k)
		}
	}
	if s.dir == "" {
		return nil // persistence unconfigured: nothing on disk to remove
	}
	if err := os.RemoveAll(filepath.Join(s.dir, "bloom-version", kbID)); err != nil {
		return fmt.Errorf("bloom: VersionBloomStore.DeleteByKB(%s): %w", kbID, err)
	}
	return nil
}

// build constructs a fresh BitsAndBloomsFilter and adds every docID.
func (s *VersionBloomStore) build(docIDs []string) *BitsAndBloomsFilter {
	f := NewBitsAndBloomsFilter(s.expectedItems, s.fpRate)
	for _, id := range docIDs {
		f.Add(id)
	}
	return f
}

// filterPath returns the on-disk path for (kbID, versionID)'s filter.
// kbID is generated by the system (a UUID), so it never contains path
// separators.
func (s *VersionBloomStore) filterPath(kbID string, versionID int64) string {
	return filepath.Join(s.dir, "bloom-version", kbID, fmt.Sprintf("%d.bloom", versionID))
}

// persist writes f's serialized form to disk (creating directories as
// needed). No-op when persistence is unconfigured (dir == ""), so the store
// stays a pure in-memory accelerator — matching DeleteByVersion/DeleteByKB,
// which already guard on the same condition.
func (s *VersionBloomStore) persist(kbID string, versionID int64, fp uint64, f BloomFilter) error {
	if s.dir == "" {
		return nil
	}
	body, err := f.Serialize()
	if err != nil {
		return fmt.Errorf("bloom: VersionBloomStore persist(%s,%d): serialize: %w", kbID, versionID, err)
	}
	// The fingerprint goes in front of the filter's own bytes, so a filter read back
	// from disk can say which document set it describes (see loadFromDisk).
	data := make([]byte, bloomHeaderLen+len(body))
	binary.BigEndian.PutUint64(data[:bloomHeaderLen], fp)
	copy(data[bloomHeaderLen:], body)

	path := s.filterPath(kbID, versionID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("bloom: VersionBloomStore persist(%s,%d): mkdir: %w", kbID, versionID, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("bloom: VersionBloomStore persist(%s,%d): write: %w", kbID, versionID, err)
	}
	return nil
}

// loadFromDisk reads and deserializes (kbID, versionID)'s filter. Any
// error (missing file, crash-truncated write, corrupt bytes) reports a
// miss; the caller falls back to rebuilding from the version doc list.
func (s *VersionBloomStore) loadFromDisk(kbID string, versionID int64) (BloomFilter, uint64, error) {
	data, err := os.ReadFile(s.filterPath(kbID, versionID))
	if err != nil {
		return nil, 0, err
	}
	if len(data) < bloomHeaderLen {
		return nil, 0, fmt.Errorf("bloom: %s/%d: persisted filter carries no document-set fingerprint (written by an older build)", kbID, versionID)
	}
	fp := binary.BigEndian.Uint64(data[:bloomHeaderLen])
	f := NewBitsAndBloomsFilter(s.expectedItems, s.fpRate)
	if err := f.Deserialize(data[bloomHeaderLen:]); err != nil {
		return nil, 0, err
	}
	return f, fp, nil
}
