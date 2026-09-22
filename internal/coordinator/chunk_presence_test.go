package coordinator

import (
	"context"
	"testing"

	"stratum/internal/bloom"
	"stratum/internal/types"
)

// The write path used to ask the vecstore about every (document, chunk) pair
// separately. On a repetitive corpus that means asking the same question about the
// same chunk over and over inside ONE version's write: measured on the 3+3 cluster
// with a 1,000-document batch, chunkPresent was 51.4% of the per-document step,
// and the chunk WRITES in that same step were only 38 calls. chunkPresenceCache is
// what makes the answer reusable for the life of that version's write, and it cut
// that step's share to 0.6%.
//
// 撤掉缓存即变红: pass nil instead of cache below, and the Exists count is 3.
func TestChunkPresent_ConfirmsEachChunkOnceWithTheCache(t *testing.T) {
	ctx := context.Background()
	cs := newTestChunkStore()
	if err := cs.Write(ctx, "kb-1", "chunk-1", []float32{1}); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	bf := bloom.NewMockBloomFilter()
	bf.Add("chunk-1") // a bloom miss never reaches the store, so the fixture needs a hit

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{ChunkBloom: bf, ChunkStore: cs})
	cache := &chunkPresenceCache{}

	for i := 0; i < 3; i++ {
		present, err := coord.chunkPresent(ctx, "kb-1", "chunk-1", cache)
		if err != nil {
			t.Fatalf("chunkPresent #%d: %v", i+1, err)
		}
		if !present {
			t.Fatalf("chunkPresent #%d = false, want true: the chunk was seeded", i+1)
		}
	}
	if got := cs.existsCallsFor("chunk-1"); got != 1 {
		t.Fatalf("ChunkStore.Exists was called %d time(s) for one chunk in one write, want 1", got)
	}
}

// The cache belongs to one version's write, not to the coordinator. A caller that
// has no cache must keep asking every time — that is the pre-cache behaviour, and
// it is what every path outside writeDocumentsConcurrently still gets.
func TestChunkPresent_WithoutACacheAsksEveryTime(t *testing.T) {
	ctx := context.Background()
	cs := newTestChunkStore()
	if err := cs.Write(ctx, "kb-1", "chunk-1", []float32{1}); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	bf := bloom.NewMockBloomFilter()
	bf.Add("chunk-1")

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{ChunkBloom: bf, ChunkStore: cs})

	for i := 0; i < 3; i++ {
		if _, err := coord.chunkPresent(ctx, "kb-1", "chunk-1", nil); err != nil {
			t.Fatalf("chunkPresent #%d: %v", i+1, err)
		}
	}
	if got := cs.existsCallsFor("chunk-1"); got != 3 {
		t.Fatalf("with no cache the store must be asked every time; got %d call(s), want 3", got)
	}
}

// Only PRESENT is remembered, and that is a correctness property rather than a
// tuning choice: a chunk another document stores later in the same write becomes
// present, and a remembered "absent" would make the next document embed and store
// it all over again. This pins both halves of it — a bloom miss never reaches the
// store, and nothing about the miss is cached.
func TestChunkPresent_DoesNotRememberAbsence(t *testing.T) {
	ctx := context.Background()
	cs := newTestChunkStore()
	bf := bloom.NewMockBloomFilter() // empty on purpose

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{ChunkBloom: bf, ChunkStore: cs})
	cache := &chunkPresenceCache{}

	present, err := coord.chunkPresent(ctx, "kb-1", "chunk-absent", cache)
	if err != nil {
		t.Fatalf("chunkPresent: %v", err)
	}
	if present {
		t.Fatal("a bloom miss must answer absent")
	}
	if got := cs.existsCallsFor("chunk-absent"); got != 0 {
		t.Fatalf("a bloom miss must not reach the store; got %d call(s)", got)
	}
	if cache.isPresent("chunk-absent") {
		t.Fatal("absence must not be remembered: the chunk can become present later in the same write")
	}

	// Once this write stores it, the same cache must report it present without
	// asking the store again.
	if _, err := coord.writeChunk(ctx, "kb-1", types.Chunk{ChunkID: "chunk-absent"}, []float32{1}, cache); err != nil {
		t.Fatalf("writeChunk: %v", err)
	}
	before := cs.existsCallsFor("chunk-absent")
	if present, err = coord.chunkPresent(ctx, "kb-1", "chunk-absent", cache); err != nil {
		t.Fatalf("chunkPresent after write: %v", err)
	}
	if !present {
		t.Fatal("a chunk this write just stored must read as present")
	}
	if got := cs.existsCallsFor("chunk-absent"); got != before {
		t.Fatalf("the chunk this write stored must be remembered; the store was asked %d extra time(s)", got-before)
	}
}
