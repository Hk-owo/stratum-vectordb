package splitter

import (
	"crypto/sha256"
	"encoding/hex"

	"stratum/internal/types"
)

// SlidingWindowSplitter is the default ChunkSplitter implementation: it
// slides a fixed-size window over the document, advancing by
// (windowSize - overlapSize) runes each step, so consecutive chunks share
// overlapSize runes of content.
//
// Splitting operates on runes, not bytes, so that windowSize and
// overlapSize counts behave correctly for multi-byte text (e.g. Chinese).
type SlidingWindowSplitter struct{}

// NewSlidingWindowSplitter constructs the default splitter. It holds no
// state; a single instance can be shared across knowledge bases and goroutines.
func NewSlidingWindowSplitter() *SlidingWindowSplitter {
	return &SlidingWindowSplitter{}
}

// Split implements ChunkSplitter.
func (s *SlidingWindowSplitter) Split(content string, windowSize int, overlapSize int, embedConfigID string) []types.Chunk {
	runes := []rune(content)
	n := len(runes)

	if n == 0 {
		// Empty document: no chunks. Returning an empty (possibly nil)
		// slice rather than panicking or returning a chunk with empty
		// content lets callers treat "no changes" uniformly without a
		// special case.
		return []types.Chunk{}
	}

	if windowSize <= 0 {
		// Degenerate configuration: treat the whole document as one chunk
		// rather than looping forever or dividing by a non-positive step.
		return []types.Chunk{newChunk(content, embedConfigID)}
	}

	if n <= windowSize {
		// Document shorter than (or exactly) the window: the entire
		// document is a single chunk.
		return []types.Chunk{newChunk(content, embedConfigID)}
	}

	// Step size is how far the window advances between chunks. Clamp
	// overlapSize so it never reaches or exceeds windowSize, which would
	// make the step zero or negative and loop forever.
	effectiveOverlap := overlapSize
	if effectiveOverlap < 0 {
		effectiveOverlap = 0
	}
	if effectiveOverlap >= windowSize {
		effectiveOverlap = windowSize - 1
	}
	step := windowSize - effectiveOverlap

	var chunks []types.Chunk
	for start := 0; start < n; start += step {
		end := start + windowSize
		if end > n {
			end = n
		}
		chunkText := string(runes[start:end])
		chunks = append(chunks, newChunk(chunkText, embedConfigID))
		if end == n {
			break
		}
	}
	return chunks
}

// newChunk computes ChunkID = SHA-256(chunk text + embedConfigID) and
// packages it with the chunk text.
func newChunk(text string, embedConfigID string) types.Chunk {
	h := sha256.Sum256([]byte(text + embedConfigID))
	return types.Chunk{
		ChunkID: hex.EncodeToString(h[:]),
		Content: text,
	}
}

var _ ChunkSplitter = (*SlidingWindowSplitter)(nil)
