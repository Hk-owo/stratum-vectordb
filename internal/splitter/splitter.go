// Package splitter defines the ChunkSplitter interface — the document
// splitting strategy used on the write path — along with the default
// sliding-window implementation.
//
// See Stratum_接口设计v9.md "ChunkSplitter" and Stratum_设计文档v10.md
// "文档切割与向量生成" for the authoritative design.
package splitter

import "stratum/internal/types"

// ChunkSplitter splits document content into chunks. The default
// implementation (SlidingWindowSplitter) uses a sliding window; future
// strategies (e.g. semantic splitting) can implement this interface as a
// drop-in replacement.
//
// Split also computes each chunk's ChunkID = SHA-256(chunk text +
// embedConfigID), so the returned []types.Chunk is immediately usable by
// callers without a separate hashing step.
type ChunkSplitter interface {
	// Split splits content into chunks using a window of windowSize runes
	// with overlapSize runes of overlap between consecutive chunks. If
	// content is shorter than windowSize, the entire document is returned
	// as a single chunk. embedConfigID is folded into each chunk's
	// ChunkID computation so that the same text under a different embed
	// configuration produces a different ChunkID.
	Split(content string, windowSize int, overlapSize int, embedConfigID string) []types.Chunk
}
