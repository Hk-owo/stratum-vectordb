package splitter

import "stratum/internal/types"

// SlidingWindowSplitter is the fixed-offset ChunkSplitter implementation: it
// slides a window of params.WindowSize runes over the document, advancing by
// (WindowSize - OverlapSize) runes each step, so consecutive chunks share
// OverlapSize runes of content.
//
// Splitting operates on runes, not bytes, so that windowSize and overlapSize
// counts behave correctly for multi-byte text (e.g. Chinese).
//
// The boundaries depend on nothing but the offset from the start of the
// document, which is exactly why a mid-document insertion or deletion shifts
// every chunk after it and invalidates their IDs
// (docs/content-defined-chunking-plan.md §1.2). CDCSplitter is the
// content-defined alternative; this one stays as the default mode and as the
// baseline the CDC tests compare against.
type SlidingWindowSplitter struct{}

// NewSlidingWindowSplitter constructs the sliding-window splitter. It holds no
// state; a single instance can be shared across knowledge bases and goroutines.
func NewSlidingWindowSplitter() *SlidingWindowSplitter {
	return &SlidingWindowSplitter{}
}

// Split implements ChunkSplitter.
func (s *SlidingWindowSplitter) Split(content string, params types.ChunkParams, embedConfigID string) []types.Chunk {
	windowSize, overlapSize := params.WindowSize, params.OverlapSize
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

var _ ChunkSplitter = (*SlidingWindowSplitter)(nil)
