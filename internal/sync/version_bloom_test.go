package sync

import (
	"context"
	"sync"
	"testing"

	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/versiondoc"
)

// recordingBloom records the document-filter rebuilds a transfer performs.
type recordingBloom struct {
	mu    sync.Mutex
	calls []bloomRebuild
	err   error
}

type bloomRebuild struct {
	kbID      string
	versionID int64
	docIDs    []string
}

func (r *recordingBloom) BuildAndPersist(kbID string, versionID int64, docIDs []string) (bloom.BloomFilter, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, bloomRebuild{kbID: kbID, versionID: versionID, docIDs: append([]string(nil), docIDs...)})
	if r.err != nil {
		return nil, r.err
	}
	return nil, nil
}

func (r *recordingBloom) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// TestFollower_RebuildsTheVersionBloomFromTheDocumentsThatLanded pins the replica's half
// of the per-version document filter.
//
// The writer builds it inside its write transaction. A replica that receives the same
// data has to build it too, and that is not a nicety: the filter drops the search hits
// that are not in the version, so a replica whose filter was built before the data
// arrived (an early query, a push racing a read) holds an EMPTY one — and an empty
// filter rejects every hit, making the replica answer "nothing matched", with no error,
// for a version it holds in full. See Follower.SetVersionBloom.
func TestFollower_RebuildsTheVersionBloomFromTheDocumentsThatLanded(t *testing.T) {
	ctx := context.Background()
	vd := versiondoc.NewMockVersionDocList()
	for _, doc := range []string{"doc-a", "doc-b"} {
		if err := vd.Write(ctx, "kb-1", 3, doc); err != nil {
			t.Fatalf("Write(%s): %v", doc, err)
		}
	}

	f := NewFollower(docstore.NewMockDocStore(), chunkdoc.NewMockChunkDocMapper(), vd,
		chunkstore.NewMockChunkStore(), &recordingTrigger{})
	rebuilt := &recordingBloom{}
	f.SetVersionBloom(rebuilt)

	f.rebuildVersionBloom(ctx, "kb-1", 3)

	if got := rebuilt.count(); got != 1 {
		t.Fatalf("BuildAndPersist calls = %d, want exactly 1", got)
	}
	call := rebuilt.calls[0]
	if call.kbID != "kb-1" || call.versionID != 3 {
		t.Errorf("rebuilt %s v%d, want kb-1 v3", call.kbID, call.versionID)
	}
	if len(call.docIDs) != 2 {
		t.Errorf("rebuilt from %v, want the version's full document set", call.docIDs)
	}
}

// A store failure is not a data failure: the filter is an accelerator, so the transfer
// that carried the data stays successful.
func TestFollower_RebuildVersionBloomToleratesAFailure(t *testing.T) {
	ctx := context.Background()
	vd := versiondoc.NewMockVersionDocList()
	if err := vd.Write(ctx, "kb-1", 3, "doc-a"); err != nil {
		t.Fatalf("Write: %v", err)
	}

	f := NewFollower(nil, nil, vd, nil, nil)
	f.SetVersionBloom(&recordingBloom{err: errTriggerFailed})

	f.rebuildVersionBloom(ctx, "kb-1", 3) // must not panic or block
}

// Not wiring the store is allowed (an older assembly, a focused test): the step is
// skipped, exactly as the cursor advancer is when it is absent.
func TestFollower_RebuildVersionBloomWithoutAStoreIsSilent(t *testing.T) {
	f := NewFollower(nil, nil, versiondoc.NewMockVersionDocList(), nil, nil)
	f.rebuildVersionBloom(context.Background(), "kb-1", 3)
}
