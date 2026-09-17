package coordinator

import (
	"context"
	"strings"
	"sync"
	"testing"

	"stratum/internal/bloom"
	"stratum/internal/splitter"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// These tests cover the two changes the CDC plan adds to the write path
// (docs/content-defined-chunking-plan.md §4 and §3.8). They run the production
// splitter — both modes — over the in-memory doubles, so what they exercise is
// the real decision path, not a stub of it.

// recordingSplitter wraps a splitter and remembers the parameters it was handed,
// so a test can see what the write path asked for. Split carries no knowledge
// base id, so the parameters are what identifies the caller's intent.
type recordingSplitter struct {
	inner splitter.ChunkSplitter

	mu     sync.Mutex
	params []types.ChunkParams
}

func (s *recordingSplitter) Split(content string, params types.ChunkParams, embedConfigID string) []types.Chunk {
	s.mu.Lock()
	s.params = append(s.params, params)
	s.mu.Unlock()
	return s.inner.Split(content, params, embedConfigID)
}

func (s *recordingSplitter) recorded() []types.ChunkParams {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]types.ChunkParams(nil), s.params...)
}

// reuseTestStack is the assembled coordinator plus the doubles the assertions
// need to look at.
type reuseTestStack struct {
	coord    *WriteCoordinatorImpl
	chunks   *testChunkStore
	embed    *testEmbedClient
	mapper   *testChunkDocMapper
	splitter *recordingSplitter
}

func newReuseTestStack(t *testing.T, kbs ...types.KnowledgeBaseMeta) *reuseTestStack {
	t.Helper()
	ds := newTestDocStore()
	cdm := newTestChunkDocMapper()
	vdl := newTestVersionDocList()
	cs := newTestChunkStore()
	ec := &testEmbedClient{}
	im := newTestIndexManager()
	rn := newTestRaftNode()
	w := wal.NewMockWAL()
	sp := &recordingSplitter{inner: splitter.NewDefault()}

	for _, kb := range kbs {
		if err := rn.ProposeCreateKB(context.Background(), kb); err != nil {
			t.Fatalf("ProposeCreateKB(%s): %v", kb.KBID, err)
		}
	}

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		Splitter:            sp,
		EmbedClient:         ec,
		ChunkBloom:          bloom.NewMockBloomFilter(),
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vdl,
		IndexManager:        im,
	})
	return &reuseTestStack{coord: coord, chunks: cs, embed: ec, mapper: cdm, splitter: sp}
}

// reuseTestContent is long enough to split into several chunks.
func reuseTestContent() string {
	return strings.Repeat("内容复用的用例：同一份文档写两次，第二次它的每个块都已经在库里了。", 30)
}

// TestWriteCoordinator_SkipsEmbedForChunksAlreadyStored covers §4: the point of
// the pre-embed filter is that a chunk the KB already holds costs no embed call.
// The realistic shape of that is writing the same document again — a retry, or a
// later version that re-adds it — which is what this does.
func TestWriteCoordinator_SkipsEmbedForChunksAlreadyStored(t *testing.T) {
	stack := newReuseTestStack(t, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})
	ctx := context.Background()
	content := reuseTestContent()
	changes := []types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc-1", Content: content}}

	if _, err := stack.coord.Execute(ctx, "kb-1", 0, changes, ""); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	firstCalls := stack.embed.calls
	firstStored := stack.chunks.writtenCount()
	if firstCalls == 0 {
		t.Fatalf("the first write embedded nothing; the fixture cannot show a saving")
	}
	if firstStored == 0 {
		t.Fatalf("the first write stored no chunks")
	}

	// Second version, same document: every chunk is already in the store, so
	// the embed service must not be asked for any of them.
	if _, err := stack.coord.Execute(ctx, "kb-1", 1, changes, ""); err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if got := stack.embed.calls - firstCalls; got != 0 {
		t.Errorf("embed was called %d extra time(s) for chunks the KB already held", got)
	}
	if got := stack.chunks.writtenCount(); got != firstStored {
		t.Errorf("chunk store grew from %d to %d; a fully reused document must write no new chunk", firstStored, got)
	}
}

// TestWriteCoordinator_KeepsMappingForReusedChunks: a reused chunk still has to
// be mapped to the document that now contains it. Dropping the mapping because
// the vector was not rewritten would silently break retrieval for the new
// version.
func TestWriteCoordinator_KeepsMappingForReusedChunks(t *testing.T) {
	stack := newReuseTestStack(t, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 512, ChunkOverlapSize: 64,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})
	ctx := context.Background()
	content := reuseTestContent()

	if _, err := stack.coord.Execute(ctx, "kb-1", 0, []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: content},
	}, ""); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	// A second document whose content is the same: every chunk is reused, so no
	// vector is written — but each one still has to be mapped to doc-2. The
	// forward map is chunk -> set of documents, so its size does not move here;
	// that is exactly why this checks both directions instead of a counter.
	if _, err := stack.coord.Execute(ctx, "kb-1", 1, []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-2", Content: content},
	}, ""); err != nil {
		t.Fatalf("second Execute: %v", err)
	}

	doc2Chunks, err := stack.mapper.ListChunkIDsByDocs(ctx, "kb-1", []string{"doc-2"})
	if err != nil {
		t.Fatalf("ListChunkIDsByDocs(doc-2): %v", err)
	}
	if len(doc2Chunks) == 0 {
		t.Fatalf("doc-2 has no chunks mapped; the reused chunks lost their mapping")
	}
	for _, chunkID := range doc2Chunks {
		docs, err := stack.mapper.ListDocIDs(ctx, "kb-1", chunkID)
		if err != nil {
			t.Fatalf("ListDocIDs(%s): %v", chunkID, err)
		}
		has := make(map[string]bool, len(docs))
		for _, docID := range docs {
			has[docID] = true
		}
		if !has["doc-1"] || !has["doc-2"] {
			t.Errorf("chunk %s maps to %v, want both doc-1 and doc-2", chunkID, docs)
		}
	}
}

// TestWriteCoordinator_SplitsPerKnowledgeBaseMode covers §3.8: a single splitter
// serves every knowledge base on the node, so the mode has to come from each
// KB's metadata, per write. Before this, the splitter was chosen once at
// assembly time and a CDC knowledge base could not exist.
func TestWriteCoordinator_SplitsPerKnowledgeBaseMode(t *testing.T) {
	stack := newReuseTestStack(t,
		types.KnowledgeBaseMeta{
			KBID: "kb-window", Name: "window",
			// Only code asks for the window; the zero value would be
			// content-defined chunking, like every knowledge base the API can
			// create (docs/content-defined-chunking-plan.md §3.6).
			ChunkMode:       types.ChunkModeWindow,
			ChunkWindowSize: 512, ChunkOverlapSize: 64,
			EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
		},
		types.KnowledgeBaseMeta{
			KBID: "kb-cdc", Name: "cdc",
			ChunkMode:    types.ChunkModeCDC,
			ChunkMinSize: types.DefaultChunkMinSize,
			ChunkMaxSize: types.DefaultChunkMaxSize,
			ChunkAvgBits: types.DefaultChunkAvgBits,
			EmbedConfig:  types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
		},
	)
	ctx := context.Background()
	content := reuseTestContent()

	for _, kbID := range []string{"kb-window", "kb-cdc"} {
		if _, err := stack.coord.Execute(ctx, kbID, 0, []types.DocChange{
			{Op: types.ChangeOpAdd, DocID: "doc-1", Content: content},
		}, ""); err != nil {
			t.Fatalf("%s: Execute: %v", kbID, err)
		}
	}

	modes := map[types.ChunkMode]int{}
	for _, params := range stack.splitter.recorded() {
		modes[params.Mode]++
	}
	if modes[types.ChunkModeWindow] == 0 {
		t.Errorf("no write asked for the sliding window; recorded modes: %v", modes)
	}
	if modes[types.ChunkModeCDC] == 0 {
		t.Errorf("the CDC knowledge base did not reach the content-defined splitter; recorded modes: %v", modes)
	}

	// The metadata's sizes have to travel with the mode, not just the mode
	// name: a CDC write with the window's defaults would silently cut
	// differently from what the KB was created with.
	for _, params := range stack.splitter.recorded() {
		if params.Mode != types.ChunkModeCDC {
			continue
		}
		if params.MinSize != types.DefaultChunkMinSize || params.MaxSize != types.DefaultChunkMaxSize || params.AvgBits != types.DefaultChunkAvgBits {
			t.Errorf("CDC parameters lost on the way to the splitter: %+v", params)
		}
	}
}
