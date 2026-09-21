package index

import (
	"context"
	"errors"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

func newSearchOnlyManager(ds *docSource, vc *mockVectorIndexClient) *IndexManagerImpl {
	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		VecstoreAddr:    "unused", // the client is injected below
	})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector
	return im
}

// TestIndexManager_EmptySearchOnAReplicaThatHasNoChunksYetIsRetryable is the third of
// the three "empty answer that is really a refusal": the documents are here, the
// chunks are not, so this replica's index for the version is EMPTY and a search over
// it succeeds with nothing. Without emptySearchMeansTheDataIsStillLanding the caller
// gets `results=0, err=nil`, the station reads that as an answer, and the replicas
// that do hold the version are never asked (measured on the 3+3 cluster, see the
// function's comment).
//
// The index entry is faked as loaded on purpose: that is what takes the query past the
// lazy-build path, whose "no chunks" refusal already exists, and onto the search
// itself — the path that answered empty in the measurement.
func TestIndexManager_EmptySearchOnAReplicaThatHasNoChunksYetIsRetryable(t *testing.T) {
	vc := newMockVectorIndexClient()
	// The search runs, and succeeds: it simply has nothing to find.
	vc.searchFn = func(string, int64, []float32, int) ([]types.SearchResult, error) { return nil, nil }

	ds := newDocSource()
	ds.addDoc(1, "doc-1", nil, nil) // documents present, chunks not yet pulled

	im := newSearchOnlyManager(ds, vc)
	im.loaded[indexKey{"kb-1", 1}] = &loadedIndex{lastAccess: time.Now()}

	_, err := im.Search(context.Background(), "kb-1", 1, []float32{0.1, 0.2, 0.3}, 5)
	if !errors.Is(err, stratumerrors.ErrIndexNotReady) {
		t.Fatalf("an empty search with no chunks on this replica must be retryable, got: %v", err)
	}
}

// TestIndexManager_EmptySearchWithChunksPresentIsAnAnswer is the other direction, so
// the new test cannot turn every empty answer into a refusal: when the chunks ARE
// here and none of them matches, empty is the correct answer.
func TestIndexManager_EmptySearchWithChunksPresentIsAnAnswer(t *testing.T) {
	vc := newMockVectorIndexClient()
	vc.searchFn = func(string, int64, []float32, int) ([]types.SearchResult, error) { return nil, nil }

	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.1, 0.2, 0.3}})

	im := newSearchOnlyManager(ds, vc)
	im.loaded[indexKey{"kb-1", 1}] = &loadedIndex{lastAccess: time.Now()}

	results, err := im.Search(context.Background(), "kb-1", 1, []float32{0.1, 0.2, 0.3}, 5)
	if err != nil {
		t.Fatalf("a search whose chunks are present must stay an empty answer: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("results = %d, want 0", len(results))
	}
}
