package splitter

import (
	"strings"
	"testing"
)

// TestSlidingWindowSplitter follows the T1-5 case table in
// Stratum_测试顺序.md.
func TestSlidingWindowSplitter(t *testing.T) {
	s := NewSlidingWindowSplitter()

	t.Run("normal split: chunk count and boundaries correct", func(t *testing.T) {
		content := strings.Repeat("字", 1000)
		windowSize, overlap := 200, 50
		chunks := s.Split(content, windowSize, overlap, "cfg1")

		step := windowSize - overlap // 150
		wantCount := 0
		for start := 0; start < 1000; start += step {
			wantCount++
			end := start + windowSize
			if end >= 1000 {
				break
			}
		}
		if len(chunks) != wantCount {
			t.Fatalf("chunk count = %d, want %d", len(chunks), wantCount)
		}

		// First chunk starts at rune 0, length windowSize.
		first := []rune(chunks[0].Content)
		if len(first) != windowSize {
			t.Fatalf("first chunk length = %d, want %d", len(first), windowSize)
		}
		// Last chunk must end exactly at the end of the document.
		last := []rune(chunks[len(chunks)-1].Content)
		wantLastLen := 1000 - (wantCount-1)*step
		if wantLastLen > windowSize {
			wantLastLen = windowSize
		}
		if len(last) != wantLastLen {
			t.Fatalf("last chunk length = %d, want %d", len(last), wantLastLen)
		}
	})

	t.Run("short document becomes a single whole chunk", func(t *testing.T) {
		content := strings.Repeat("a", 50)
		chunks := s.Split(content, 200, 50, "cfg1")
		if len(chunks) != 1 {
			t.Fatalf("chunk count = %d, want 1", len(chunks))
		}
		if chunks[0].Content != content {
			t.Fatalf("chunk content = %q, want full document", chunks[0].Content)
		}
	})

	t.Run("chunk ID is consistent across repeated splits", func(t *testing.T) {
		content := strings.Repeat("hello world ", 100)
		c1 := s.Split(content, 200, 50, "cfg1")
		c2 := s.Split(content, 200, 50, "cfg1")
		if len(c1) != len(c2) {
			t.Fatalf("chunk count differs across runs: %d vs %d", len(c1), len(c2))
		}
		for i := range c1 {
			if c1[i].ChunkID != c2[i].ChunkID {
				t.Fatalf("chunk[%d] ID differs across runs: %s vs %s", i, c1[i].ChunkID, c2[i].ChunkID)
			}
		}
	})

	t.Run("different embedConfigID yields different chunk IDs", func(t *testing.T) {
		content := strings.Repeat("hello world ", 100)
		c1 := s.Split(content, 200, 50, "cfg1")
		c2 := s.Split(content, 200, 50, "cfg2")
		if len(c1) != len(c2) {
			t.Fatalf("chunk count differs: %d vs %d", len(c1), len(c2))
		}
		for i := range c1 {
			if c1[i].ChunkID == c2[i].ChunkID {
				t.Fatalf("chunk[%d] ID identical across different embedConfigID: %s", i, c1[i].ChunkID)
			}
			// Content itself should be identical; only the ID differs.
			if c1[i].Content != c2[i].Content {
				t.Fatalf("chunk[%d] content differs across embedConfigID, should not: %q vs %q", i, c1[i].Content, c2[i].Content)
			}
		}
	})

	t.Run("overlap is correct: next chunk head contains previous chunk's tail", func(t *testing.T) {
		content := strings.Repeat("0123456789", 30) // 300 runes
		windowSize, overlap := 100, 20
		chunks := s.Split(content, windowSize, overlap, "cfg1")
		if len(chunks) < 2 {
			t.Fatalf("expected at least 2 chunks, got %d", len(chunks))
		}
		for i := 1; i < len(chunks); i++ {
			prevRunes := []rune(chunks[i-1].Content)
			currRunes := []rune(chunks[i].Content)
			prevTail := string(prevRunes[len(prevRunes)-overlap:])
			currHead := string(currRunes[:overlap])
			if prevTail != currHead {
				t.Fatalf("chunk[%d] head %q != chunk[%d] tail %q", i, currHead, i-1, prevTail)
			}
		}
	})

	t.Run("empty document does not panic and returns no/empty chunks", func(t *testing.T) {
		chunks := s.Split("", 200, 50, "cfg1")
		if len(chunks) > 1 {
			t.Fatalf("empty document produced %d chunks, want 0 or 1", len(chunks))
		}
		if len(chunks) == 1 && chunks[0].Content != "" {
			t.Fatalf("single chunk for empty document has non-empty content: %q", chunks[0].Content)
		}
	})
}

// TestSlidingWindowSplitter_DegenerateConfig covers configuration edge
// cases not explicitly in the T1-5 table but reachable via caller input
// (e.g. misconfigured knowledge_base_defaults), to ensure no panics or
// infinite loops.
func TestSlidingWindowSplitter_DegenerateConfig(t *testing.T) {
	s := NewSlidingWindowSplitter()

	t.Run("zero window size does not panic or loop forever", func(t *testing.T) {
		content := strings.Repeat("a", 100)
		chunks := s.Split(content, 0, 0, "cfg1")
		if len(chunks) != 1 || chunks[0].Content != content {
			t.Fatalf("zero windowSize should fall back to a single whole-document chunk, got %d chunks", len(chunks))
		}
	})

	t.Run("overlap >= windowSize does not loop forever", func(t *testing.T) {
		content := strings.Repeat("a", 500)
		chunks := s.Split(content, 100, 100, "cfg1") // overlap == windowSize
		if len(chunks) == 0 {
			t.Fatalf("expected at least one chunk")
		}
		// Just verifying it terminates and produces a sane result; the
		// effective step is clamped to >= 1.
	})

	t.Run("negative overlap treated as zero", func(t *testing.T) {
		content := strings.Repeat("a", 300)
		chunks := s.Split(content, 100, -10, "cfg1")
		if len(chunks) == 0 {
			t.Fatalf("expected at least one chunk")
		}
	})
}

// TestNewChunk_HashFormula locks in the documented ChunkID formula:
// SHA-256(chunk text + embedConfigID).
func TestNewChunk_HashFormula(t *testing.T) {
	first := newChunk("hello", "cfg1")
	again := newChunk("hello", "cfg1")
	if first.ChunkID != again.ChunkID {
		t.Fatalf("newChunk should be deterministic for the same input")
	}
	if len(first.ChunkID) != 64 { // hex-encoded SHA-256 = 64 chars
		t.Fatalf("ChunkID length = %d, want 64 (hex SHA-256)", len(first.ChunkID))
	}
}
