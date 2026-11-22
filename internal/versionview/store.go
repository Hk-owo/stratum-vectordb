package versionview

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"stratum/internal/chunkdoc"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/fsutil"
	"stratum/internal/versiondoc"
)

const (
	defaultMaxEntries = 8
	diskShardCount    = 64
)

// Config configures a versionview Store.
type Config struct {
	// DataDir is the node's root data directory. When non-empty, L1 .vview
	// sidecars are persisted under <DataDir>/version-view/<kbID>/<versionID>.vview.
	DataDir string

	// MaxEntries bounds the L0 in-memory LRU cache. Non-positive defaults to 8.
	MaxEntries int

	// VersionDocList and ChunkDocMapper supply L2 derivation on cache misses.
	VersionDocList versiondoc.VersionDocList
	ChunkDocMapper chunkdoc.ChunkDocMapper
}

type viewKey struct {
	kbID      string
	versionID int64
}

type lruEntry struct {
	key  viewKey
	view *View
}

// Store manages the three-tier (L0 LRU, L1 disk .vview, L2 PebbleDB) cache of
// immutable per-version read views.
type Store struct {
	dataDir    string
	maxEntries int
	vdl        versiondoc.VersionDocList
	cdm        chunkdoc.ChunkDocMapper

	mu        sync.Mutex
	items     map[viewKey]*list.Element
	order     *list.List
	memBytes  int64
	l0Hits    uint64
	l1Hits    uint64
	l2Builds  uint64
	evictions uint64

	shards [diskShardCount]sync.Mutex
}

// NewStore constructs a Store with the given configuration.
func NewStore(cfg Config) *Store {
	maxEntries := cfg.MaxEntries
	if maxEntries <= 0 {
		maxEntries = defaultMaxEntries
	}
	return &Store{
		dataDir:    cfg.DataDir,
		maxEntries: maxEntries,
		vdl:        cfg.VersionDocList,
		cdm:        cfg.ChunkDocMapper,
		items:      make(map[viewKey]*list.Element),
		order:      list.New(),
	}
}

// Get returns the verified View for (kbID, versionID). See Lookup for hit-tier
// reporting.
func (s *Store) Get(ctx context.Context, kbID string, versionID int64, expectedDigest string) (*View, error) {
	v, _, err := s.Lookup(ctx, kbID, versionID, expectedDigest)
	return v, err
}

// Lookup returns the verified View for (kbID, versionID) and which cache tier
// satisfied the request.
//
// When expectedDigest is non-empty:
//   - L0 and L1 hits require an exact DocIDSetHash match;
//   - L2 verifies the derived document set against expectedDigest and returns
//     ErrIndexNotReady (without writing L0 or L1) if they disagree.
//
// When expectedDigest is empty, the view is derived from L2 for this call and
// never cached in L0 or L1.
func (s *Store) Lookup(ctx context.Context, kbID string, versionID int64, expectedDigest string) (*View, HitSource, error) {
	if err := fsutil.ValidatePathSegment(kbID); err != nil {
		return nil, HitNone, err
	}
	if expectedDigest != "" {
		if v := s.getL0(kbID, versionID, expectedDigest); v != nil {
			return v, HitL0, nil
		}
	}

	shard := s.shardFor(kbID, versionID)
	shard.Lock()
	defer shard.Unlock()

	if expectedDigest != "" {
		if v := s.getL0(kbID, versionID, expectedDigest); v != nil {
			return v, HitL0, nil
		}
		if s.dataDir != "" {
			if v, ok := s.readL1Locked(kbID, versionID, expectedDigest); ok {
				s.putL0(kbID, versionID, v)
				s.mu.Lock()
				s.l1Hits++
				s.mu.Unlock()
				return v, HitL1, nil
			}
		}
	}

	v, err := s.deriveL2(ctx, kbID, versionID, expectedDigest)
	if err != nil {
		return nil, HitNone, err
	}

	s.mu.Lock()
	s.l2Builds++
	s.mu.Unlock()

	if expectedDigest != "" {
		if s.dataDir != "" {
			_ = s.writeL1Locked(kbID, versionID, v)
		}
		s.putL0(kbID, versionID, v)
	}
	return v, HitL2, nil
}

// Warm seeds L1 (when DataDir is configured) and L0 from data already computed
// by an index build.
func (s *Store) Warm(kbID string, versionID int64, docIDs []string, chunkToDocs map[string][]uint32) error {
	if err := fsutil.ValidatePathSegment(kbID); err != nil {
		return err
	}
	sortedDocs, normalizedMap := normalizeWarmInputs(docIDs, chunkToDocs)
	digest := ComputeDocIDSetHash(sortedDocs)
	v := newView(digest, sortedDocs, normalizedMap)

	shard := s.shardFor(kbID, versionID)
	shard.Lock()
	defer shard.Unlock()

	var writeErr error
	if s.dataDir != "" {
		writeErr = s.writeL1Locked(kbID, versionID, v)
	}
	s.putL0(kbID, versionID, v)
	return writeErr
}

// Evict removes (kbID, versionID) from the L0 in-memory LRU while keeping any
// L1 disk sidecar intact.
func (s *Store) Evict(kbID string, versionID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem, ok := s.items[viewKey{kbID: kbID, versionID: versionID}]; ok {
		s.removeElementLocked(elem)
	}
}

// EvictByKB removes all entries belonging to kbID from the L0 in-memory LRU
// while keeping L1 disk sidecars intact.
func (s *Store) EvictByKB(kbID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, elem := range s.items {
		if k.kbID == kbID {
			s.removeElementLocked(elem)
		}
	}
}

// DeleteVersion removes (kbID, versionID) from L0 and deletes its L1 .vview file.
func (s *Store) DeleteVersion(kbID string, versionID int64) error {
	if err := fsutil.ValidatePathSegment(kbID); err != nil {
		return err
	}
	s.Evict(kbID, versionID)
	if s.dataDir == "" {
		return nil
	}

	shard := s.shardFor(kbID, versionID)
	shard.Lock()
	defer shard.Unlock()

	path := s.viewPath(kbID, versionID)
	err := os.Remove(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("versionview: remove %s: %w", path, err)
	}
	if serr := fsutil.SyncDir(filepath.Dir(path)); serr != nil && !errors.Is(serr, os.ErrNotExist) {
		return serr
	}
	return nil
}

// DeleteKB removes all L0 entries and the L1 directory for kbID.
func (s *Store) DeleteKB(kbID string) error {
	if err := fsutil.ValidatePathSegment(kbID); err != nil {
		return err
	}
	s.EvictByKB(kbID)
	if s.dataDir == "" {
		return nil
	}

	for i := range s.shards {
		s.shards[i].Lock()
	}
	defer func() {
		for i := len(s.shards) - 1; i >= 0; i-- {
			s.shards[i].Unlock()
		}
	}()

	dir := s.kbDir(kbID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("versionview: remove kb dir %s: %w", dir, err)
	}
	if serr := fsutil.SyncDir(filepath.Dir(dir)); serr != nil && !errors.Is(serr, os.ErrNotExist) {
		return serr
	}
	return nil
}

// Stats returns a snapshot of cache residency and cumulative hit counters.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Entries:   len(s.items),
		MemBytes:  s.memBytes,
		L0Hits:    s.l0Hits,
		L1Hits:    s.l1Hits,
		L2Builds:  s.l2Builds,
		Evictions: s.evictions,
	}
}

func (s *Store) getL0(kbID string, versionID int64, expectedDigest string) *View {
	s.mu.Lock()
	defer s.mu.Unlock()
	elem, ok := s.items[viewKey{kbID: kbID, versionID: versionID}]
	if !ok {
		return nil
	}
	entry := elem.Value.(*lruEntry)
	if entry.view.DocIDSetHash != expectedDigest {
		return nil
	}
	s.order.MoveToFront(elem)
	s.l0Hits++
	return entry.view
}

func (s *Store) putL0(kbID string, versionID int64, v *View) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := viewKey{kbID: kbID, versionID: versionID}
	if elem, ok := s.items[key]; ok {
		entry := elem.Value.(*lruEntry)
		s.memBytes -= entry.view.MemBytes
		entry.view = v
		s.memBytes += v.MemBytes
		s.order.MoveToFront(elem)
		return
	}
	for len(s.items) >= s.maxEntries {
		back := s.order.Back()
		if back == nil {
			break
		}
		s.removeElementLocked(back)
		s.evictions++
	}
	elem := s.order.PushFront(&lruEntry{key: key, view: v})
	s.items[key] = elem
	s.memBytes += v.MemBytes
}

func (s *Store) removeElementLocked(elem *list.Element) {
	entry := s.order.Remove(elem).(*lruEntry)
	delete(s.items, entry.key)
	s.memBytes -= entry.view.MemBytes
}

func (s *Store) deriveL2(ctx context.Context, kbID string, versionID int64, expectedDigest string) (*View, error) {
	if s.vdl == nil {
		return nil, fmt.Errorf("versionview: VersionDocList is not configured")
	}
	docIDs, err := s.vdl.ListDocIDs(ctx, kbID, versionID)
	if err != nil {
		return nil, err
	}
	sortedDocs := deduplicateAndSort(docIDs)
	gotDigest := ComputeDocIDSetHash(sortedDocs)
	if expectedDigest != "" && gotDigest != expectedDigest {
		return nil, fmt.Errorf(
			"%w: %s: version %d's document set does not match the committed digest on this replica",
			stratumerrors.ErrIndexNotReady, kbID, versionID)
	}

	chunkToDocs, err := s.mapChunks(ctx, kbID, sortedDocs)
	if err != nil {
		return nil, err
	}
	return newView(gotDigest, sortedDocs, chunkToDocs), nil
}

func (s *Store) mapChunks(ctx context.Context, kbID string, sortedDocs []string) (map[string][]uint32, error) {
	if len(sortedDocs) == 0 || s.cdm == nil {
		return map[string][]uint32{}, nil
	}
	if batch, ok := s.cdm.(chunkdoc.ChunkDocBatchMapper); ok {
		_, chunkToDocs, err := batch.MapChunksByDocs(ctx, kbID, sortedDocs)
		if err != nil {
			return nil, err
		}
		return chunkToDocs, nil
	}

	// Fallback for ChunkDocMapper implementations/stubs that do not implement
	// ChunkDocBatchMapper.
	chunkIDs, err := s.cdm.ListChunkIDsByDocs(ctx, kbID, sortedDocs)
	if err != nil {
		return nil, err
	}
	docIndex := make(map[string]uint32, len(sortedDocs))
	for i, id := range sortedDocs {
		docIndex[id] = uint32(i)
	}
	chunkToDocs := make(map[string][]uint32, len(chunkIDs))
	for _, chunkID := range chunkIDs {
		mappedDocs, err := s.cdm.ListDocIDs(ctx, kbID, chunkID)
		if err != nil {
			return nil, err
		}
		var indices []uint32
		for _, docID := range mappedDocs {
			if idx, ok := docIndex[docID]; ok {
				indices = append(indices, idx)
			}
		}
		if len(indices) > 0 {
			sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
			dedup := indices[:1]
			for _, idx := range indices[1:] {
				if idx != dedup[len(dedup)-1] {
					dedup = append(dedup, idx)
				}
			}
			chunkToDocs[chunkID] = dedup
		}
	}
	return chunkToDocs, nil
}

func (s *Store) readL1Locked(kbID string, versionID int64, expectedDigest string) (*View, bool) {
	path := s.viewPath(kbID, versionID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	v, err := decodeView(data)
	if err != nil || v.DocIDSetHash != expectedDigest {
		// Remove corrupt or stale L1 sidecar so the subsequent L2 build repairs it.
		_ = os.Remove(path)
		return nil, false
	}
	return v, true
}

func (s *Store) writeL1Locked(kbID string, versionID int64, v *View) error {
	data, err := encodeView(v)
	if err != nil {
		return err
	}
	return fsutil.AtomicWriteFile(
		s.viewPath(kbID, versionID),
		data,
		fsutil.WithMkdirAll(0o755),
		fsutil.WithTempPattern(fmt.Sprintf("%d.vview.tmp.*", versionID)),
	)
}

func (s *Store) kbDir(kbID string) string {
	return filepath.Join(s.dataDir, "version-view", kbID)
}

func (s *Store) viewPath(kbID string, versionID int64) string {
	return filepath.Join(s.kbDir(kbID), fmt.Sprintf("%d.vview", versionID))
}

func (s *Store) shardFor(kbID string, versionID int64) *sync.Mutex {
	h := uint32(2166136261)
	for i := 0; i < len(kbID); i++ {
		h ^= uint32(kbID[i])
		h *= 16777619
	}
	v := uint64(versionID)
	h ^= uint32(v) ^ uint32(v>>32)
	h *= 16777619
	return &s.shards[int(h%diskShardCount)]
}

func deduplicateAndSort(docIDs []string) []string {
	if len(docIDs) == 0 {
		return nil
	}
	sorted := make([]string, len(docIDs))
	copy(sorted, docIDs)
	if !sort.StringsAreSorted(sorted) {
		sort.Strings(sorted)
	}
	out := sorted[:1]
	for _, id := range sorted[1:] {
		if id != out[len(out)-1] {
			out = append(out, id)
		}
	}
	return out
}

// normalizeWarmInputs ensures DocIDs is sorted and ChunkToDocs indices point
// into the sorted slice and are strictly ascending.
func normalizeWarmInputs(docIDs []string, chunkToDocs map[string][]uint32) ([]string, map[string][]uint32) {
	if len(docIDs) == 0 {
		return nil, map[string][]uint32{}
	}
	if sort.StringsAreSorted(docIDs) {
		allUnique := true
		for i := 1; i < len(docIDs); i++ {
			if docIDs[i] == docIDs[i-1] {
				allUnique = false
				break
			}
		}
		if allUnique {
			return docIDs, chunkToDocs
		}
	}

	// Build remapping from original docIDs index -> sorted unique index.
	sorted := deduplicateAndSort(docIDs)
	posByID := make(map[string]uint32, len(sorted))
	for i, id := range sorted {
		posByID[id] = uint32(i)
	}
	remap := make([]uint32, len(docIDs))
	for i, id := range docIDs {
		remap[i] = posByID[id]
	}

	outMap := make(map[string][]uint32, len(chunkToDocs))
	for cid, list := range chunkToDocs {
		if len(list) == 0 {
			continue
		}
		mapped := make([]uint32, 0, len(list))
		for _, oldIdx := range list {
			if int(oldIdx) < len(remap) {
				mapped = append(mapped, remap[oldIdx])
			}
		}
		if len(mapped) == 0 {
			continue
		}
		sort.Slice(mapped, func(i, j int) bool { return mapped[i] < mapped[j] })
		dedup := mapped[:1]
		for _, idx := range mapped[1:] {
			if idx != dedup[len(dedup)-1] {
				dedup = append(dedup, idx)
			}
		}
		outMap[cid] = dedup
	}
	return sorted, outMap
}
