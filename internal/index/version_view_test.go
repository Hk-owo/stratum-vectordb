package index

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type warmCall struct {
	kbID        string
	versionID   int64
	docIDs      []string
	chunkToDocs map[string][]uint32
}

type recordingViewSink struct {
	mu             sync.Mutex
	warms          []warmCall
	evicts         []indexKey
	evictKBs       []string
	deleteVersions []indexKey
	deleteKBs      []string
}

func (r *recordingViewSink) Warm(kbID string, versionID int64, docIDs []string, chunkToDocs map[string][]uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	docsCopy := append([]string(nil), docIDs...)
	mapCopy := make(map[string][]uint32, len(chunkToDocs))
	for k, v := range chunkToDocs {
		mapCopy[k] = append([]uint32(nil), v...)
	}
	r.warms = append(r.warms, warmCall{kbID: kbID, versionID: versionID, docIDs: docsCopy, chunkToDocs: mapCopy})
	return nil
}

func (r *recordingViewSink) Evict(kbID string, versionID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evicts = append(r.evicts, indexKey{kbID, versionID})
}

func (r *recordingViewSink) EvictByKB(kbID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evictKBs = append(r.evictKBs, kbID)
}

func (r *recordingViewSink) DeleteVersion(kbID string, versionID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleteVersions = append(r.deleteVersions, indexKey{kbID, versionID})
	return nil
}

func (r *recordingViewSink) DeleteKB(kbID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleteKBs = append(r.deleteKBs, kbID)
	return nil
}

func TestIndexManager_VersionViewLifecycleHooks(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-b", []string{"chunk-1"}, map[string][]float32{"chunk-1": {1, 0}})
	ds.addDoc(1, "doc-a", []string{"chunk-1", "chunk-2"}, map[string][]float32{"chunk-1": {1, 0}, "chunk-2": {0, 1}})
	ds.addDoc(2, "doc-a", []string{"chunk-1", "chunk-2"}, map[string][]float32{"chunk-1": {1, 0}, "chunk-2": {0, 1}})
	ds.addDoc(2, "doc-c", []string{"chunk-3"}, map[string][]float32{"chunk-3": {0.5, 0.5}})

	sink := &recordingViewSink{}
	dataDir := t.TempDir()
	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:            1,
		LoadWaitTimeout:        5 * time.Second,
		IndexDataDir:           dataDir,
		IndexRetentionCount:    1,
		RetentionProtectWindow: -1,
		VersionView:            sink,
		MapChunksByDocs: func(_ context.Context, _ string, docIDs []string) ([]string, map[string][]uint32, error) {
			ds.mu.Lock()
			defer ds.mu.Unlock()
			seen := make(map[string]struct{})
			var chunks []string
			chunkToDocs := make(map[string][]uint32)
			for i, docID := range docIDs {
				idx := uint32(i)
				for _, cid := range ds.chunks[docID] {
					if _, ok := seen[cid]; !ok {
						seen[cid] = struct{}{}
						chunks = append(chunks, cid)
					}
					chunkToDocs[cid] = append(chunkToDocs[cid], idx)
				}
			}
			return chunks, chunkToDocs, nil
		},
	})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector
	im.versionParent = func(_ context.Context, _ string, versionID int64) (int64, error) {
		if versionID == 2 {
			return 1, nil
		}
		return 0, nil
	}
	t.Cleanup(func() { _ = im.Close() })

	ctx := context.Background()
	if err := im.TriggerBuild(ctx, "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild v1: %v", err)
	}
	waitLoaded(t, im, "kb-1", 1)

	sink.mu.Lock()
	if len(sink.warms) != 1 || sink.warms[0].versionID != 1 {
		t.Fatalf("expected Warm for v1, got %+v", sink.warms)
	}
	if !reflect.DeepEqual(sink.warms[0].docIDs, []string{"doc-a", "doc-b"}) {
		t.Fatalf("v1 Warm docIDs = %v, want sorted [doc-a doc-b]", sink.warms[0].docIDs)
	}
	if !reflect.DeepEqual(sink.warms[0].chunkToDocs["chunk-1"], []uint32{0, 1}) {
		t.Fatalf("v1 Warm chunk-1 postings = %v, want [0 1]", sink.warms[0].chunkToDocs["chunk-1"])
	}
	sink.mu.Unlock()

	// Building v2 (append from v1) with LRUCapacity=1 warms v2 and evicts v1.
	if err := im.TriggerBuild(ctx, "kb-1", 2); err != nil {
		t.Fatalf("TriggerBuild v2: %v", err)
	}
	waitLoaded(t, im, "kb-1", 2)

	sink.mu.Lock()
	if len(sink.warms) != 2 || sink.warms[1].versionID != 2 {
		t.Fatalf("expected Warm for v2 (append path), got %+v", sink.warms)
	}
	if len(sink.evicts) != 1 || sink.evicts[0] != (indexKey{"kb-1", 1}) {
		t.Fatalf("expected LRU Evict for v1, got %v", sink.evicts)
	}
	sink.mu.Unlock()

	// EnforceDiskRetention(count=1) drops v1 on disk -> DeleteVersion(kb-1, 1).
	idxDir := filepath.Join(dataDir, "index", "kb-1")
	if err := os.MkdirAll(idxDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"1.index", "1.index.ids", "2.index", "2.index.ids"} {
		if err := os.WriteFile(filepath.Join(idxDir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := im.EnforceDiskRetention(ctx, "kb-1", nil); err != nil {
		t.Fatalf("EnforceDiskRetention: %v", err)
	}
	sink.mu.Lock()
	if len(sink.deleteVersions) != 1 || sink.deleteVersions[0] != (indexKey{"kb-1", 1}) {
		t.Fatalf("expected DeleteVersion for retention-dropped v1, got %v", sink.deleteVersions)
	}
	sink.mu.Unlock()

	// Explicit Evict & Discard on v2.
	if err := im.Evict(ctx, "kb-1", 2); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if err := im.Discard(ctx, "kb-1", 2); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	// EvictByKB & DeleteFilesByKB.
	if err := im.EvictByKB(ctx, "kb-1"); err != nil {
		t.Fatalf("EvictByKB: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "index", "kb-1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := im.DeleteFilesByKB(ctx, "kb-1"); err != nil {
		t.Fatalf("DeleteFilesByKB: %v", err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.evictKBs) != 1 || sink.evictKBs[0] != "kb-1" {
		t.Fatalf("evictKBs = %v", sink.evictKBs)
	}
	if len(sink.deleteKBs) != 1 || sink.deleteKBs[0] != "kb-1" {
		t.Fatalf("deleteKBs = %v", sink.deleteKBs)
	}
	if len(sink.deleteVersions) != 2 || sink.deleteVersions[1] != (indexKey{"kb-1", 2}) {
		t.Fatalf("deleteVersions = %v", sink.deleteVersions)
	}
}
