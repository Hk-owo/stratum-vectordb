package chunkdoc

import (
	"context"
	"testing"
)

// WriteMany is how the write path records one document's chunk mappings now: a
// document maps to several chunks, and those mappings used to go in one Write per
// chunk — one durable commit each. Both directions must still land together, which
// is the invariant Write's own batch exists for ("so they can never diverge").
//
// Both implementations run, because the write path is written against the
// interface: a divergence between pebble and mock is a bug in whichever one the
// test did not exercise.
//
// 撤掉批量即变红: implement WriteMany as a no-op and every read-back below is empty.
func TestWriteMany_RecordsBothDirections(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		open func(t *testing.T) ChunkDocMapper
	}{
		{"pebble", func(t *testing.T) ChunkDocMapper { return newTestPebbleChunkDocMapper(t) }},
		{"mock", func(t *testing.T) ChunkDocMapper { return NewMockChunkDocMapper() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.open(t)

			// An empty batch is a no-op, not an error: a document can split into no
			// chunks at all.
			if err := m.WriteMany(ctx, "kb-1", "doc-1", nil); err != nil {
				t.Fatalf("WriteMany(empty): %v", err)
			}
			if got, err := m.ListDocIDs(ctx, "kb-1", "chunk-1"); err != nil || len(got) != 0 {
				t.Fatalf("an empty batch wrote (%v, %v), want nothing", got, err)
			}

			// One document, several chunks, in one commit.
			chunkIDs := []string{"chunk-1", "chunk-2", "chunk-3"}
			if err := m.WriteMany(ctx, "kb-1", "doc-1", chunkIDs); err != nil {
				t.Fatalf("WriteMany: %v", err)
			}

			// Forward direction: every chunk maps back to the document.
			for _, chunkID := range chunkIDs {
				got, err := m.ListDocIDs(ctx, "kb-1", chunkID)
				if err != nil {
					t.Fatalf("ListDocIDs(%s): %v", chunkID, err)
				}
				assertSetEqualPebble(t, got, []string{"doc-1"})
			}

			// Reverse direction: the document maps back to every chunk. This is the
			// half a partial write would silently lose, and the half the index build
			// reads.
			got, err := m.ListChunkIDsByDocs(ctx, "kb-1", []string{"doc-1"})
			if err != nil {
				t.Fatalf("ListChunkIDsByDocs: %v", err)
			}
			assertSetEqualPebble(t, got, chunkIDs)

			// Knowledge-base scoping holds.
			if got, err := m.ListChunkIDs(ctx, "kb-2"); err != nil || len(got) != 0 {
				t.Fatalf("knowledge bases leaked: %v (%v)", got, err)
			}

			// Idempotent: the same batch again changes nothing.
			if err := m.WriteMany(ctx, "kb-1", "doc-1", chunkIDs); err != nil {
				t.Fatalf("WriteMany(repeat): %v", err)
			}
			if got, _ = m.ListChunkIDsByDocs(ctx, "kb-1", []string{"doc-1"}); len(got) != len(chunkIDs) {
				t.Fatalf("after a repeat the document has %d chunks, want %d", len(got), len(chunkIDs))
			}

			// And it composes with the single-mapping Write: a second document
			// sharing one chunk must show up in that chunk's forward list.
			mustWritePebbleOrMock(t, m, "kb-1", "chunk-1", "doc-2")
			got, _ = m.ListDocIDs(ctx, "kb-1", "chunk-1")
			assertSetEqualPebble(t, got, []string{"doc-1", "doc-2"})
		})
	}
}

// mustWritePebbleOrMock writes one mapping through the interface, so the case
// above runs unchanged against either implementation.
func mustWritePebbleOrMock(t *testing.T, m ChunkDocMapper, kbID, chunkID, docID string) {
	t.Helper()
	if err := m.Write(context.Background(), kbID, chunkID, docID); err != nil {
		t.Fatalf("Write(%s,%s,%s): %v", kbID, chunkID, docID, err)
	}
}
