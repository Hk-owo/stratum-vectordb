package chunkdoc

import (
	"context"
	"reflect"
	"sort"
	"testing"
)

// M9 of docs/code-review-2026-09-24.md turned ListChunkIDsByDocs from "one Pebble
// iterator (and one seek) per document" into "one iterator, SeekGE per document,
// stop at the prefix". These are the cases that shape exercises and the old shape
// did not: the scan must move FORWARD on the shared iterator without carrying
// anything over, and each document's prefix is its own boundary.
func TestPebbleChunkDocMapper_ListChunkIDsByDocsAcrossDocuments(t *testing.T) {
	ctx := context.Background()

	t.Run("documents asked for out of order still get their own chunks", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk-a", "doc-1")
		mustWritePebble(t, m, "kb1", "chunk-b", "doc-2")
		mustWritePebble(t, m, "kb1", "chunk-c", "doc-3")

		// Reverse order: every SeekGE jumps FORWARD past the previous documents'
		// entries, and nothing from them may leak into the result.
		got, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{"doc-3", "doc-1"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		assertChunkSet(t, got, []string{"chunk-a", "chunk-c"})
	})

	t.Run("a chunk shared by several documents appears once", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "shared", "doc-1")
		mustWritePebble(t, m, "kb1", "shared", "doc-2")
		mustWritePebble(t, m, "kb1", "own", "doc-2")

		got, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{"doc-1", "doc-2", "doc-1"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		assertChunkSet(t, got, []string{"shared", "own"})
	})

	t.Run("a document id that is a prefix of another does not absorb it", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		// The reverse index encodes each id with its length, so "doc-1" cannot
		// match "doc-10" — and the hand-rolled HasPrefix bound in the new scan has
		// to agree with that.
		mustWritePebble(t, m, "kb1", "chunk-1", "doc-1")
		mustWritePebble(t, m, "kb1", "chunk-10", "doc-10")

		got, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{"doc-1"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		assertChunkSet(t, got, []string{"chunk-1"})
	})

	t.Run("another knowledge base's entries are never scanned", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "mine", "doc-1")
		mustWritePebble(t, m, "kb2", "theirs", "doc-1")

		got, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{"doc-1"})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		assertChunkSet(t, got, []string{"mine"})
	})

	t.Run("no documents at all is an empty result, not an error", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk-a", "doc-1")

		got, err := m.ListChunkIDsByDocs(ctx, "kb1", nil)
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("ListChunkIDsByDocs(nil) = %v, want empty", got)
		}
	})

	t.Run("an empty document id does not sweep the knowledge base", func(t *testing.T) {
		m := newTestPebbleChunkDocMapper(t)
		mustWritePebble(t, m, "kb1", "chunk-a", "doc-1")

		// encodeReversePrefix("kb1", "") is a valid prefix (the length-encoded empty
		// string), so this must return nothing rather than every entry.
		got, err := m.ListChunkIDsByDocs(ctx, "kb1", []string{""})
		if err != nil {
			t.Fatalf("ListChunkIDsByDocs: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("ListChunkIDsByDocs(\"\") = %v, want empty", got)
		}
	})
}

// assertChunkSet compares as a set: the chunk order is whatever the scan produced.
func assertChunkSet(t *testing.T, got, want []string) {
	t.Helper()
	gotSorted := append([]string(nil), got...)
	wantSorted := append([]string(nil), want...)
	sort.Strings(gotSorted)
	sort.Strings(wantSorted)
	if !reflect.DeepEqual(gotSorted, wantSorted) {
		t.Fatalf("chunks = %v, want %v", got, want)
	}
}
