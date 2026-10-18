package coordinator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"stratum/internal/bloom"
	stratumerrors "stratum/internal/errors"
	stratumync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// docSetHashRaftNode answers GetVersion with a committed document-set digest, so
// the write path's parent check has something to compare against.
type docSetHashRaftNode struct {
	*testRaftNode
	hashes map[int64]string
}

func (r *docSetHashRaftNode) GetVersion(_ context.Context, _ string, versionID int64) (types.VersionMeta, error) {
	return types.VersionMeta{VersionID: versionID, DocIDSetHash: r.hashes[versionID]}, nil
}

// parentDocSetFixture wires a write coordinator whose parent version (v1) has a
// committed digest for {doc-1, doc-2}.
func parentDocSetFixture(t *testing.T) (*WriteCoordinatorImpl, *testVersionDocList) {
	t.Helper()
	want := []string{"doc-1", "doc-2"}
	rn := &docSetHashRaftNode{
		testRaftNode: newTestRaftNode(),
		hashes:       map[int64]string{1: stratumync.ComputeDocIDSetHash(want)},
	}
	rn.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 100, ChunkOverlapSize: 20,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})

	vdl := newTestVersionDocList()
	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 wal.NewMockWAL(),
		RaftNode:            rn,
		Splitter:            &mockSplitter{windowSize: 100, overlapSize: 20},
		EmbedClient:         &testEmbedClient{},
		ChunkBloom:          bloom.NewMockBloomFilter(),
		ChunkStore:          newTestChunkStore(),
		ChunkDocMapper:      newTestChunkDocMapper(),
		DocStore:            newTestDocStore(),
		VersionDocList:      vdl,
		IndexManager:        newTestIndexManager(),
	})
	return coord, vdl
}

// TestWriteCoordinator_RefusesChildWhenParentDataIsMissing pins H1 of
// docs/code-review-2026-10-18.md.
//
// A child version's document set is the parent's set plus this version's changes,
// and the parent's set is read from THIS node's store. Under the dispatch topology
// the candidate set is "every replica of the KB" with no check that the candidate
// holds the parent, so a replica whose fan-out failed (or that restarted, or just
// joined) reads an empty parent set — indistinguishable from "the parent has no
// documents" — and every inherited document disappears from the new version. Worse,
// the wrong set is what gets hashed into DocIDSetHash and fanned out, so the whole
// cluster agrees on it.
//
// The committed digest is what tells the two apart, so a mismatch has to stop the
// write.
func TestWriteCoordinator_RefusesChildWhenParentDataIsMissing(t *testing.T) {
	coord, _ := parentDocSetFixture(t)

	changes := []types.DocChange{{Op: types.ChangeOpUpdate, DocID: "doc-1", Content: "new content"}}
	_, err := coord.Execute(context.Background(), "kb-1", 1, changes, "")
	if err == nil {
		t.Fatal("Execute must refuse to derive a child version from a parent whose data this node does not hold")
	}
	if !strings.Contains(err.Error(), "does not match its committed digest") {
		t.Errorf("error should name the digest mismatch, got: %v", err)
	}
	if !errors.Is(err, stratumerrors.ErrIndexNotReady) {
		t.Errorf("the refusal must be retryable (so another replica can take the write), got: %v", err)
	}
}

// TestWriteCoordinator_AcceptsChildWhenParentDigestMatches is the control: with the
// parent's data actually present, the same write goes through and the child's set
// really is the parent's plus the change.
func TestWriteCoordinator_AcceptsChildWhenParentDigestMatches(t *testing.T) {
	coord, vdl := parentDocSetFixture(t)
	ctx := context.Background()
	for _, docID := range []string{"doc-1", "doc-2"} {
		if err := vdl.Write(ctx, "kb-1", 1, docID); err != nil {
			t.Fatalf("seed parent doc list: %v", err)
		}
	}

	changes := []types.DocChange{{Op: types.ChangeOpUpdate, DocID: "doc-1", Content: "new content"}}
	versionID, err := coord.Execute(ctx, "kb-1", 1, changes, "")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	got, err := vdl.ListDocIDs(ctx, "kb-1", versionID)
	if err != nil {
		t.Fatalf("ListDocIDs(child): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("child document set = %v, want both inherited documents", got)
	}
}
