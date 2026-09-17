package splitter

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"stratum/internal/types"
)

// The measurements below implement the acceptance criteria of
// docs/content-defined-chunking-plan.md §8. Two numbers matter, and the second
// is the one that reflects cost:
//
//   - tail retention: of the chunks the original document had AFTER the
//     insertion point, what share still hashes to the same ChunkID in the
//     edited document;
//   - recomputed share: of the edited document's bytes, what share sits in
//     chunks whose ID the original did not contain. Counting chunks instead
//     would hide the fact that the two algorithms cut different sizes.
const cdcTestEmbedConfigID = "m1"

// cdcTestDocument builds a document of distinct paragraphs, each with sentence
// punctuation. Distinctness matters: the measurements compare chunk ID sets, so
// repeated text would collapse them.
func cdcTestDocument(paragraphs int) string {
	var b strings.Builder
	for i := 0; i < paragraphs; i++ {
		fmt.Fprintf(&b, "第 %d 段：内容定义分块把边界交给滚动指纹决定，因此一次中部的增删只影响编辑点附近的若干块。", i)
		b.WriteString("后续段落的指纹序列与先前完全一致，于是它们切出的块也逐字一致，块 ID 得以复用。")
		b.WriteString("这段说明本身也是一个句子，句末标点给边界提供了吸附候选。\n")
	}
	return b.String()
}

// insertedText is what the tests splice into the middle of a document.
const insertedText = "插入的新内容，用来模拟文档中部的增删改动，它本身也是几个完整的句子。"

// midDocumentEdit returns doc and doc-with-insertedText-in-the-middle, plus the
// byte offset of the insertion point in both.
//
// The offset is pulled back to a rune boundary. Splitting mid-character would
// leave the edited document holding an invalid byte sequence, and the sliding
// window decodes to runes — so it would substitute U+FFFD and quietly stop
// producing chunks that appear in the document at all.
func midDocumentEdit(doc string) (before, after string, insertAt int) {
	insertAt = len(doc) / 2
	for insertAt > 0 && !utf8.RuneStart(doc[insertAt]) {
		insertAt--
	}
	return doc, doc[:insertAt] + insertedText + doc[insertAt:], insertAt
}

// tailRetention is the plan's §8 criterion. A chunk's position is recovered
// from the document by searching for its text, which is unambiguous for the
// documents these tests build (every paragraph is distinct).
func tailRetention(t *testing.T, sp ChunkSplitter, before, after string, insertAt int, params types.ChunkParams) float64 {
	t.Helper()
	beforeIDs := chunkIDsAfter(t, sp, before, insertAt, params)
	afterIDs := chunkIDsAfter(t, sp, after, insertAt, params)
	if len(beforeIDs) == 0 {
		t.Fatalf("no chunks after the insertion point; the document is too short for this test")
	}
	kept := 0
	for id := range beforeIDs {
		if afterIDs[id] {
			kept++
		}
	}
	return float64(kept) / float64(len(beforeIDs))
}

func chunkIDsAfter(t *testing.T, sp ChunkSplitter, doc string, from int, params types.ChunkParams) map[string]bool {
	t.Helper()
	ids := map[string]bool{}
	for _, chunk := range sp.Split(doc, params, cdcTestEmbedConfigID) {
		pos := strings.Index(doc, chunk.Content)
		if pos < 0 {
			t.Fatalf("chunk %q does not appear in the document it came from", chunk.Content)
		}
		if pos >= from {
			ids[chunk.ChunkID] = true
		}
	}
	return ids
}

// recomputedShare is the cost yardstick: the share of the edited document's
// bytes that end up in chunks the original document did not have.
func recomputedShare(t *testing.T, sp ChunkSplitter, before, after string, params types.ChunkParams) float64 {
	t.Helper()
	known := map[string]bool{}
	for _, chunk := range sp.Split(before, params, cdcTestEmbedConfigID) {
		known[chunk.ChunkID] = true
	}
	total := 0
	for _, chunk := range sp.Split(after, params, cdcTestEmbedConfigID) {
		if !known[chunk.ChunkID] {
			total += len(chunk.Content)
		}
	}
	return float64(total) / float64(len(after))
}

// TestCDCSplitter_ReuseAfterMidDocumentInsert is the case the whole plan turns
// on. The sliding window is measured alongside, because the number that matters
// is the gap between the two, not either one on its own: a window with 100%
// reuse after an insertion would mean the plan is unnecessary.
func TestCDCSplitter_ReuseAfterMidDocumentInsert(t *testing.T) {
	const (
		minTailRetention = 0.90
		maxRecomputed    = 0.20
	)
	cdcParams := types.CDCChunkParams(256, 1536, 9)
	windowParams := types.WindowChunkParams(512, 64)

	doc := cdcTestDocument(200)
	before, after, insertAt := midDocumentEdit(doc)

	t.Run("content-defined boundaries keep the tail's chunk IDs", func(t *testing.T) {
		got := tailRetention(t, NewCDCSplitter(), before, after, insertAt, cdcParams)
		if got < minTailRetention {
			t.Errorf("tail retention = %.1f%%, want >= %.1f%%", 100*got, 100*minTailRetention)
		}
		t.Logf("cdc tail retention = %.1f%%", 100*got)

		share := recomputedShare(t, NewCDCSplitter(), before, after, cdcParams)
		if share > maxRecomputed {
			t.Errorf("recomputed share = %.1f%%, want <= %.1f%%", 100*share, 100*maxRecomputed)
		}
		t.Logf("cdc recomputed share = %.1f%%", 100*share)
	})

	t.Run("the sliding window loses the whole tail, as the plan assumes", func(t *testing.T) {
		// This is the control, not a defect report: it pins down that the
		// criterion above actually distinguishes the two algorithms. If the
		// window ever scores well here, the fixture stopped exercising the
		// mid-document edit and the assertion above became meaningless.
		got := tailRetention(t, NewSlidingWindowSplitter(), before, after, insertAt, windowParams)
		if got > 0.05 {
			t.Errorf("window tail retention = %.1f%%, want <= 5%% (fixture no longer tests a mid-document edit)", 100*got)
		}
		t.Logf("window tail retention = %.1f%%", 100*got)

		share := recomputedShare(t, NewSlidingWindowSplitter(), before, after, windowParams)
		if share < 0.40 {
			t.Errorf("window recomputed share = %.1f%%, want >= 40%%", 100*share)
		}
		t.Logf("window recomputed share = %.1f%%", 100*share)
	})
}

// TestCDCSplitter_ChunksAreValidUTF8 is the §3.3 red line. The chunker cuts on
// bytes, so a boundary can land inside a multi-byte character unless it is
// snapped to a rune start.
func TestCDCSplitter_ChunksAreValidUTF8(t *testing.T) {
	contents := map[string]string{
		"chinese":       strings.Repeat("中文段落，用于测试多字节字符的边界。", 200),
		"emoji":         strings.Repeat("表情 🙂🙃🎉 与四字节汉字 𠮷 混排。", 150),
		"mixed":         strings.Repeat("English text mixed with 中文 and emoji 🌟 here. ", 200),
		"boundary-risk": strings.Repeat("a一", 700),
	}
	params := types.CDCChunkParams(256, 1536, 9)

	for name, content := range contents {
		t.Run(name, func(t *testing.T) {
			chunks := NewCDCSplitter().Split(content, params, cdcTestEmbedConfigID)
			if len(chunks) == 0 {
				t.Fatalf("no chunks")
			}
			for i, chunk := range chunks {
				if !utf8.ValidString(chunk.Content) {
					t.Fatalf("chunk %d is not valid UTF-8: %q", i, chunk.Content)
				}
			}
		})
	}
}

// TestCDCSplitter_ChunksCoverEveryByte pins the boundary bookkeeping: the
// boundaries only ever move forward, so reassembling the chunks must reproduce
// the document exactly — no lost byte, no duplicated one, no empty chunk.
func TestCDCSplitter_ChunksCoverEveryByte(t *testing.T) {
	doc := cdcTestDocument(60)
	chunks := NewCDCSplitter().Split(doc, types.CDCChunkParams(256, 1536, 9), cdcTestEmbedConfigID)
	if len(chunks) < 2 {
		t.Fatalf("expected several chunks, got %d", len(chunks))
	}

	var rebuilt strings.Builder
	for i, chunk := range chunks {
		if chunk.Content == "" {
			t.Fatalf("chunk %d is empty", i)
		}
		rebuilt.WriteString(chunk.Content)
	}
	if rebuilt.String() != doc {
		t.Fatalf("chunks do not reassemble the document: %d bytes vs %d", rebuilt.Len(), len(doc))
	}
}

// TestCDCSplitter_SnapsToSentenceEnds checks that the semantic adjustment is
// actually applied: most chunks should end at a sentence boundary, which is
// what keeps a chunk from straddling two unrelated sentences (§3.4).
func TestCDCSplitter_SnapsToSentenceEnds(t *testing.T) {
	doc := cdcTestDocument(60)
	chunks := NewCDCSplitter().Split(doc, types.CDCChunkParams(256, 1536, 9), cdcTestEmbedConfigID)
	if len(chunks) < 4 {
		t.Fatalf("expected several chunks, got %d", len(chunks))
	}

	// The last chunk ends at the end of the document, which is not a candidate
	// boundary, so it is excluded.
	endsAtSentence := 0
	for _, chunk := range chunks[:len(chunks)-1] {
		r, _ := utf8.DecodeLastRuneInString(chunk.Content)
		if isSentenceEnd(r) {
			endsAtSentence++
		}
	}
	if endsAtSentence == 0 {
		t.Fatalf("no chunk ends at a sentence boundary; the snapping step is not running")
	}
	t.Logf("%d of %d non-final chunks end at a sentence boundary", endsAtSentence, len(chunks)-1)
}

// TestCDCSplitter_ChunkLengthsStayWithinBounds covers the min/max contract and
// the degenerate documents the plan calls out (§8): arbitrarily short text, and
// long text with nothing to snap to.
func TestCDCSplitter_ChunkLengthsStayWithinBounds(t *testing.T) {
	const minSize, maxSize, avgBits = 256, 1536, 9
	params := types.CDCChunkParams(minSize, maxSize, avgBits)

	t.Run("ordinary document", func(t *testing.T) {
		chunks := NewCDCSplitter().Split(cdcTestDocument(60), params, cdcTestEmbedConfigID)
		for i, chunk := range chunks {
			// Only the final chunk may be short: it is the document's tail.
			if i < len(chunks)-1 && len(chunk.Content) < minSize {
				t.Errorf("chunk %d is %d bytes, below the minimum %d", i, len(chunk.Content), minSize)
			}
			// A snapped boundary can add the slack, and rune alignment up to
			// one character.
			if limit := maxSize + cdcSentenceSlack + utf8.UTFMax; len(chunk.Content) > limit {
				t.Errorf("chunk %d is %d bytes, above %d", i, len(chunk.Content), limit)
			}
		}
	})

	t.Run("document shorter than the minimum is one chunk", func(t *testing.T) {
		content := "短文档，一个句子就结束了。"
		chunks := NewCDCSplitter().Split(content, params, cdcTestEmbedConfigID)
		if len(chunks) != 1 {
			t.Fatalf("chunk count = %d, want 1", len(chunks))
		}
		if chunks[0].Content != content {
			t.Fatalf("chunk content = %q, want the whole document", chunks[0].Content)
		}
	})

	t.Run("long document with no punctuation still respects the maximum", func(t *testing.T) {
		content := strings.Repeat("abcdefghij", 2000) // 20 000 bytes, no candidate
		chunks := NewCDCSplitter().Split(content, params, cdcTestEmbedConfigID)
		if len(chunks) < 2 {
			t.Fatalf("expected the maximum to force several chunks, got %d", len(chunks))
		}
		for i, chunk := range chunks {
			if i < len(chunks)-1 && len(chunk.Content) < minSize {
				t.Errorf("chunk %d is %d bytes, below the minimum %d", i, len(chunk.Content), minSize)
			}
			if len(chunk.Content) > maxSize+utf8.UTFMax {
				t.Errorf("chunk %d is %d bytes, above the maximum %d", i, len(chunk.Content), maxSize)
			}
		}
	})
}

// TestCDCSplitter_DegenerateConfig covers the configurations that would make
// the chunker misbehave — most sharply a MinSize below its 64-byte rolling
// window, which underflows inside the library instead of reporting anything
// (§3.2).
func TestCDCSplitter_DegenerateConfig(t *testing.T) {
	doc := cdcTestDocument(20)

	t.Run("empty document", func(t *testing.T) {
		chunks := NewCDCSplitter().Split("", types.CDCChunkParams(256, 1536, 9), cdcTestEmbedConfigID)
		if len(chunks) != 0 {
			t.Fatalf("empty document produced %d chunks, want 0", len(chunks))
		}
	})

	t.Run("min below the rolling window does not panic or stall", func(t *testing.T) {
		chunks := NewCDCSplitter().Split(doc, types.CDCChunkParams(1, 16, 9), cdcTestEmbedConfigID)
		if len(chunks) == 0 {
			t.Fatalf("expected at least one chunk")
		}
		for i, chunk := range chunks {
			if !utf8.ValidString(chunk.Content) {
				t.Fatalf("chunk %d is not valid UTF-8", i)
			}
		}
	})

	t.Run("max not above min falls back to one whole-document chunk", func(t *testing.T) {
		chunks := NewCDCSplitter().Split(doc, types.CDCChunkParams(1024, 512, 9), cdcTestEmbedConfigID)
		if len(chunks) != 1 || chunks[0].Content != doc {
			t.Fatalf("expected a single whole-document chunk, got %d chunks", len(chunks))
		}
	})

	t.Run("unset parameters fall back to the defaults", func(t *testing.T) {
		fallback := NewCDCSplitter().Split(doc, types.ChunkParams{Mode: types.ChunkModeCDC}, cdcTestEmbedConfigID)
		explicit := NewCDCSplitter().Split(doc,
			types.CDCChunkParams(types.DefaultChunkMinSize, types.DefaultChunkMaxSize, types.DefaultChunkAvgBits),
			cdcTestEmbedConfigID)
		if !sameChunkIDs(fallback, explicit) {
			t.Fatalf("unset parameters did not fall back to the documented defaults")
		}
	})
}

// TestCDCSplitter_Deterministic: the same input must always cut the same way,
// or nothing downstream can reuse anything.
func TestCDCSplitter_Deterministic(t *testing.T) {
	doc := cdcTestDocument(50)
	params := types.CDCChunkParams(256, 1536, 9)

	first := NewCDCSplitter().Split(doc, params, cdcTestEmbedConfigID)
	second := NewCDCSplitter().Split(doc, params, cdcTestEmbedConfigID)
	if !sameChunkIDs(first, second) || len(first) != len(second) {
		t.Fatalf("two splits of the same document disagree: %d chunks vs %d", len(first), len(second))
	}
}

// TestDefaultSplitter_DispatchByMode covers §3.8: one splitter instance serves
// every knowledge base, so the mode has to select the algorithm per call.
func TestDefaultSplitter_DispatchByMode(t *testing.T) {
	doc := cdcTestDocument(30)
	sp := NewDefault()

	window := sp.Split(doc, types.WindowChunkParams(512, 64), cdcTestEmbedConfigID)
	cdc := sp.Split(doc, types.CDCChunkParams(256, 1536, 9), cdcTestEmbedConfigID)
	if len(window) == 0 || len(cdc) == 0 {
		t.Fatalf("both modes must produce chunks: window=%d cdc=%d", len(window), len(cdc))
	}
	if sameChunkIDs(window, cdc) {
		t.Fatalf("cdc mode returned the window's chunks; the dispatch is not reaching CDCSplitter")
	}

	// The zero mode is what an older snapshot, or a knowledge base read back
	// through the control plane, carries — and it has to land on the same
	// algorithm as an explicit cdc, or the two GetKB paths would cut one
	// knowledge base differently from each other.
	zero := sp.Split(doc, types.ChunkParams{}, cdcTestEmbedConfigID)
	if !sameChunkIDs(zero, cdc) {
		t.Fatalf("an unset mode must behave as content-defined chunking")
	}
}

// TestSnapToRuneStart locks the boundary arithmetic: a byte offset inside a
// multi-byte character moves forward to the next character, never backward
// (a backward step would drop the byte into no chunk at all).
func TestSnapToRuneStart(t *testing.T) {
	data := []byte("a中b") // rune starts: 0, 1, 4; then len(data) = 5
	offsets := runeStartOffsets(data)
	want := []int{0, 1, 4, 5}
	if len(offsets) != len(want) {
		t.Fatalf("runeStartOffsets = %v, want %v", offsets, want)
	}
	for i := range want {
		if offsets[i] != want[i] {
			t.Fatalf("runeStartOffsets = %v, want %v", offsets, want)
		}
	}

	for _, tc := range []struct{ in, out int }{
		{0, 0}, {1, 1}, {2, 4}, {3, 4}, {4, 4}, {5, 5},
	} {
		if got := snapToRuneStart(offsets, tc.in); got != tc.out {
			t.Errorf("snapToRuneStart(%d) = %d, want %d", tc.in, got, tc.out)
		}
	}
}

// TestSnapToSentenceEnd locks the semantic adjustment, including the rule that
// a punctuation mark straddling the limit is not a candidate — without that
// bound a snapped boundary could overtake the next one.
func TestSnapToSentenceEnd(t *testing.T) {
	data := []byte("第一句。第二句。") // "第一句。" is 12 bytes
	const firstEnd = 12

	if got := snapToSentenceEnd(data, 0, len(data)); got != firstEnd {
		t.Errorf("snapToSentenceEnd(0) = %d, want %d", got, firstEnd)
	}
	if got := snapToSentenceEnd(data, firstEnd, len(data)); got != len(data) {
		t.Errorf("snapToSentenceEnd(%d) = %d, want %d", firstEnd, got, len(data))
	}
	// A limit inside the punctuation mark must not yield a boundary past it.
	if got := snapToSentenceEnd(data, 0, firstEnd-1); got != 0 {
		t.Errorf("snapToSentenceEnd with a tight limit = %d, want 0 (no candidate)", got)
	}
	// Text with nothing to snap to stays where it was.
	plain := []byte(strings.Repeat("abcdef", 8))
	if got := snapToSentenceEnd(plain, 5, len(plain)); got != 5 {
		t.Errorf("snapToSentenceEnd on punctuation-free text = %d, want 5", got)
	}
}

// sameChunkIDs reports whether two chunk sets have the same IDs. The IDs are
// content-addressed, so this is a comparison of what would be reused.
func sameChunkIDs(a, b []types.Chunk) bool {
	if len(a) != len(b) {
		return false
	}
	ids := make(map[string]bool, len(a))
	for _, chunk := range a {
		ids[chunk.ChunkID] = true
	}
	for _, chunk := range b {
		if !ids[chunk.ChunkID] {
			return false
		}
	}
	return true
}
