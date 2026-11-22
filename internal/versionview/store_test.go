package versionview_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"stratum/internal/chunkdoc"
	stratumerrors "stratum/internal/errors"
	stratinternalsync "stratum/internal/sync"
	"stratum/internal/versiondoc"
	"stratum/internal/versionview"
)

type countingVDL struct {
	versiondoc.VersionDocList
	calls int
}

func (c *countingVDL) ListDocIDs(ctx context.Context, kbID string, versionID int64) ([]string, error) {
	c.calls++
	return c.VersionDocList.ListDocIDs(ctx, kbID, versionID)
}

// legacyChunkDocMapper implements only chunkdoc.ChunkDocMapper (NOT ChunkDocBatchMapper)
// to verify the fallback path in Store.
type legacyChunkDocMapper struct {
	inner chunkdoc.ChunkDocMapper
}

func (l *legacyChunkDocMapper) Write(ctx context.Context, kbID, chunkID, docID string) error {
	return l.inner.Write(ctx, kbID, chunkID, docID)
}
func (l *legacyChunkDocMapper) ListDocIDs(ctx context.Context, kbID, chunkID string) ([]string, error) {
	return l.inner.ListDocIDs(ctx, kbID, chunkID)
}
func (l *legacyChunkDocMapper) WriteMany(ctx context.Context, kbID, docID string, chunkIDs []string) error {
	return l.inner.WriteMany(ctx, kbID, docID, chunkIDs)
}
func (l *legacyChunkDocMapper) ListChunkIDs(ctx context.Context, kbID string) ([]string, error) {
	return l.inner.ListChunkIDs(ctx, kbID)
}
func (l *legacyChunkDocMapper) ListChunkIDsByDocs(ctx context.Context, kbID string, docIDs []string) ([]string, error) {
	return l.inner.ListChunkIDsByDocs(ctx, kbID, docIDs)
}
func (l *legacyChunkDocMapper) DeleteByKB(ctx context.Context, kbID string) error {
	return l.inner.DeleteByKB(ctx, kbID)
}
func (l *legacyChunkDocMapper) DeleteByDoc(ctx context.Context, kbID, docID string) error {
	return l.inner.DeleteByDoc(ctx, kbID, docID)
}
func (l *legacyChunkDocMapper) DeleteMapping(ctx context.Context, kbID, chunkID, docID string) error {
	return l.inner.DeleteMapping(ctx, kbID, chunkID, docID)
}

func seedStores(t *testing.T, kbID string, versionID int64, docs map[string][]string) (*versiondoc.MockVersionDocList, *chunkdoc.MockChunkDocMapper, string) {
	t.Helper()
	ctx := context.Background()
	vdl := versiondoc.NewMockVersionDocList()
	cdm := chunkdoc.NewMockChunkDocMapper()
	var docIDs []string
	for docID, chunks := range docs {
		docIDs = append(docIDs, docID)
		if err := vdl.Write(ctx, kbID, versionID, docID); err != nil {
			t.Fatal(err)
		}
		if err := cdm.WriteMany(ctx, kbID, docID, chunks); err != nil {
			t.Fatal(err)
		}
	}
	return vdl, cdm, stratinternalsync.ComputeDocIDSetHash(docIDs)
}

func TestComputeDocIDSetHash_MatchesSyncPackage(t *testing.T) {
	cases := [][]string{
		nil,
		{},
		{"doc-a"},
		{"doc-c", "doc-a", "doc-b"},
		{"ab", "c"},
		{"a", "bc"},
	}
	for _, tc := range cases {
		got := versionview.ComputeDocIDSetHash(tc)
		want := stratinternalsync.ComputeDocIDSetHash(tc)
		if got != want {
			t.Fatalf("ComputeDocIDSetHash(%v) = %q, sync = %q", tc, got, want)
		}
	}
}

func TestStore_L2L1L0LifecycleAndSelfHealing(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	vdl, cdm, digest := seedStores(t, "kb-1", 5, map[string][]string{
		"doc-b": {"c-shared", "c-b"},
		"doc-a": {"c-shared", "c-a"},
		"doc-c": {"c-c"},
	})
	cvdl := &countingVDL{VersionDocList: vdl}

	store := versionview.NewStore(versionview.Config{
		DataDir:        dataDir,
		MaxEntries:     4,
		VersionDocList: cvdl,
		ChunkDocMapper: cdm,
	})

	// 1. Cold miss -> L2 build, populates L1 and L0.
	v, src, err := store.Lookup(ctx, "kb-1", 5, digest)
	if err != nil {
		t.Fatalf("Lookup L2: %v", err)
	}
	if src != versionview.HitL2 || cvdl.calls != 1 {
		t.Fatalf("src=%s calls=%d, want l2 / 1", src, cvdl.calls)
	}
	if !reflect.DeepEqual(v.DocIDs, []string{"doc-a", "doc-b", "doc-c"}) {
		t.Fatalf("DocIDs = %v", v.DocIDs)
	}
	if !reflect.DeepEqual(v.ChunkToDocs["c-shared"], []uint32{0, 1}) {
		t.Fatalf("c-shared postings = %v, want [0 1]", v.ChunkToDocs["c-shared"])
	}
	if v.MemBytes <= 0 {
		t.Fatalf("MemBytes = %d, want > 0", v.MemBytes)
	}

	vviewPath := filepath.Join(dataDir, "version-view", "kb-1", "5.vview")
	if _, err := os.Stat(vviewPath); err != nil {
		t.Fatalf("L1 sidecar missing: %v", err)
	}

	// 2. Warm -> L0 hit without touching VDL or disk.
	v2, src, err := store.Lookup(ctx, "kb-1", 5, digest)
	if err != nil {
		t.Fatalf("Lookup L0: %v", err)
	}
	if src != versionview.HitL0 || cvdl.calls != 1 {
		t.Fatalf("src=%s calls=%d, want l0 / 1", src, cvdl.calls)
	}
	if v2 != v {
		t.Fatalf("expected same immutable View pointer on L0 hit")
	}

	// 3. Evict from L0 -> L1 hit on next Lookup (still no L2 read).
	store.Evict("kb-1", 5)
	v3, src, err := store.Lookup(ctx, "kb-1", 5, digest)
	if err != nil {
		t.Fatalf("Lookup L1: %v", err)
	}
	if src != versionview.HitL1 || cvdl.calls != 1 {
		t.Fatalf("src=%s calls=%d, want l1 / 1", src, cvdl.calls)
	}
	if !reflect.DeepEqual(v3.ChunkToDocs, v.ChunkToDocs) || !reflect.DeepEqual(v3.DocIDs, v.DocIDs) {
		t.Fatalf("L1 decoded view disagrees with L2 original")
	}

	// 4. Corrupt L1 CRC -> evict L0 -> self-heals via L2 and rewrites L1.
	store.Evict("kb-1", 5)
	raw, err := os.ReadFile(vviewPath)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(vviewPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	v4, src, err := store.Lookup(ctx, "kb-1", 5, digest)
	if err != nil {
		t.Fatalf("Lookup after CRC corruption: %v", err)
	}
	if src != versionview.HitL2 || cvdl.calls != 2 {
		t.Fatalf("expected L2 self-heal after CRC corruption: src=%s calls=%d", src, cvdl.calls)
	}
	if !reflect.DeepEqual(v4.ChunkToDocs, v.ChunkToDocs) {
		t.Fatalf("self-healed view mismatch")
	}

	// Verify L1 was rewritten cleanly so another store can load it from L1.
	store2 := versionview.NewStore(versionview.Config{
		DataDir:        dataDir,
		MaxEntries:     4,
		VersionDocList: cvdl,
		ChunkDocMapper: cdm,
	})
	_, src, err = store2.Lookup(ctx, "kb-1", 5, digest)
	if err != nil || src != versionview.HitL1 {
		t.Fatalf("reopened store Lookup = (%s, %v), want l1", src, err)
	}
}

func TestStore_DigestRules(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	vdl, cdm, _ := seedStores(t, "kb-1", 1, map[string][]string{
		"doc-1": {"chunk-1"},
	})
	cvdl := &countingVDL{VersionDocList: vdl}
	store := versionview.NewStore(versionview.Config{
		DataDir:        dataDir,
		MaxEntries:     4,
		VersionDocList: cvdl,
		ChunkDocMapper: cdm,
	})

	// Mismatched digest -> ErrIndexNotReady, never cached in L0 or L1.
	for i := 0; i < 2; i++ {
		if _, err := store.Get(ctx, "kb-1", 1, "wrong-digest"); !errors.Is(err, stratumerrors.ErrIndexNotReady) {
			t.Fatalf("Get wrong digest = %v, want ErrIndexNotReady", err)
		}
	}
	if cvdl.calls != 2 {
		t.Fatalf("expected 2 L2 reads on mismatch, got %d", cvdl.calls)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "version-view", "kb-1", "1.vview")); !os.IsNotExist(err) {
		t.Fatalf("mismatch must not write L1 file, stat err = %v", err)
	}

	// Empty expected digest -> returns result, never cached in L0 or L1.
	for i := 0; i < 2; i++ {
		v, src, err := store.Lookup(ctx, "kb-1", 1, "")
		if err != nil {
			t.Fatalf("Lookup empty digest: %v", err)
		}
		if src != versionview.HitL2 || len(v.DocIDs) != 1 {
			t.Fatalf("Lookup empty digest = (%s, %v)", src, v.DocIDs)
		}
	}
	if cvdl.calls != 4 || store.Stats().Entries != 0 {
		t.Fatalf("empty digest must bypass cache: calls=%d entries=%d", cvdl.calls, store.Stats().Entries)
	}
}

func TestStore_LRUEvictionAndLifecycleDeletes(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	vdl := versiondoc.NewMockVersionDocList()
	cdm := chunkdoc.NewMockChunkDocMapper()
	digests := make(map[int64]string)
	for vid := int64(1); vid <= 4; vid++ {
		doc := fmt.Sprintf("doc-%d", vid)
		if err := vdl.Write(ctx, "kb-1", vid, doc); err != nil {
			t.Fatal(err)
		}
		if err := cdm.Write(ctx, "kb-1", "c1", doc); err != nil {
			t.Fatal(err)
		}
		digests[vid] = stratinternalsync.ComputeDocIDSetHash([]string{doc})
	}

	store := versionview.NewStore(versionview.Config{
		DataDir:        dataDir,
		MaxEntries:     2,
		VersionDocList: vdl,
		ChunkDocMapper: cdm,
	})

	if _, err := store.Get(ctx, "kb-1", 1, digests[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "kb-1", 2, digests[2]); err != nil {
		t.Fatal(err)
	}
	// Touch v1 so v2 becomes LRU.
	if _, src, err := store.Lookup(ctx, "kb-1", 1, digests[1]); err != nil || src != versionview.HitL0 {
		t.Fatalf("expected v1 L0 hit, got %s %v", src, err)
	}
	// Insert v3 -> evicts v2 from L0 (remains on L1).
	if _, err := store.Get(ctx, "kb-1", 3, digests[3]); err != nil {
		t.Fatal(err)
	}
	st := store.Stats()
	if st.Entries != 2 || st.Evictions != 1 {
		t.Fatalf("stats = %+v, want entries=2 evictions=1", st)
	}
	// v2 must come from L1 now (and evict v1).
	if _, src, err := store.Lookup(ctx, "kb-1", 2, digests[2]); err != nil || src != versionview.HitL1 {
		t.Fatalf("expected v2 L1 hit, got %s %v", src, err)
	}

	// Warm from unsorted inputs still produces canonical sorted view & hits L0.
	if err := store.Warm("kb-2", 9, []string{"z", "a"}, map[string][]uint32{"cz": {0}, "ca": {1}}); err != nil {
		t.Fatalf("Warm: %v", err)
	}
	wantKB2 := stratinternalsync.ComputeDocIDSetHash([]string{"a", "z"})
	vKB2, src, err := store.Lookup(ctx, "kb-2", 9, wantKB2)
	if err != nil || src != versionview.HitL0 {
		t.Fatalf("Warm Lookup = (%s, %v)", src, err)
	}
	if !reflect.DeepEqual(vKB2.DocIDs, []string{"a", "z"}) || !reflect.DeepEqual(vKB2.ChunkToDocs["cz"], []uint32{1}) || !reflect.DeepEqual(vKB2.ChunkToDocs["ca"], []uint32{0}) {
		t.Fatalf("remapped warm view = %+v", vKB2)
	}

	// DeleteVersion drops L0 and L1.
	if err := store.DeleteVersion("kb-1", 2); err != nil {
		t.Fatalf("DeleteVersion: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "version-view", "kb-1", "2.vview")); !os.IsNotExist(err) {
		t.Fatalf("vview still exists after DeleteVersion: %v", err)
	}

	// EvictByKB leaves L1 intact, DeleteKB removes L1 dir.
	store.EvictByKB("kb-1")
	if _, err := os.Stat(filepath.Join(dataDir, "version-view", "kb-1", "1.vview")); err != nil {
		t.Fatalf("EvictByKB must preserve L1 files: %v", err)
	}
	if err := store.DeleteKB("kb-1"); err != nil {
		t.Fatalf("DeleteKB: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "version-view", "kb-1")); !os.IsNotExist(err) {
		t.Fatalf("kb dir still exists after DeleteKB: %v", err)
	}

	// Path traversal kbID must be refused.
	if _, err := store.Get(ctx, "../escape", 1, "abc"); err == nil {
		t.Fatal("expected path traversal kbID to be rejected")
	}
	if err := store.DeleteKB("../escape"); err == nil {
		t.Fatal("expected DeleteKB traversal kbID to be rejected")
	}
}

func TestStore_LegacyChunkDocMapperFallbackAndConcurrency(t *testing.T) {
	ctx := context.Background()
	vdl, cdm, digest := seedStores(t, "kb-legacy", 11, map[string][]string{
		"doc-1": {"c1", "c2"},
		"doc-2": {"c2"},
	})
	store := versionview.NewStore(versionview.Config{
		DataDir:        t.TempDir(),
		MaxEntries:     8,
		VersionDocList: vdl,
		ChunkDocMapper: &legacyChunkDocMapper{inner: cdm},
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := store.Get(ctx, "kb-legacy", 11, digest)
			if err != nil {
				t.Errorf("concurrent Get: %v", err)
				return
			}
			if !reflect.DeepEqual(v.ChunkToDocs["c2"], []uint32{0, 1}) {
				t.Errorf("c2 postings = %v, want [0 1]", v.ChunkToDocs["c2"])
			}
		}()
	}
	wg.Wait()
}
