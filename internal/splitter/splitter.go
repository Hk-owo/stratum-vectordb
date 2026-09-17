// Package splitter defines the ChunkSplitter interface — the document
// splitting strategy used on the write path — and the two implementations:
// SlidingWindowSplitter (fixed offsets, the default) and CDCSplitter
// (content-defined boundaries).
//
// See Stratum_接口设计v9.md "ChunkSplitter" and Stratum_设计文档v10.md
// "文档切割与向量生成" for the authoritative design, and
// docs/content-defined-chunking-plan.md for why the second implementation
// exists and what it is worth.
package splitter

import (
	"crypto/sha256"
	"encoding/hex"

	"stratum/internal/types"
)

// ChunkSplitter splits document content into chunks. The implementation is
// chosen per call from params.Mode, not per instance: the mode is an attribute
// of each knowledge base, while the node wires a single splitter for all of
// them (docs/content-defined-chunking-plan.md §3.8). NewDefault builds the
// complete set.
//
// Split also computes each chunk's ChunkID = SHA-256(chunk text +
// embedConfigID), so the returned []types.Chunk is immediately usable by
// callers without a separate hashing step.
type ChunkSplitter interface {
	// Split splits content into chunks according to params. embedConfigID is
	// folded into each chunk's ChunkID computation so that the same text
	// under a different embed configuration produces a different ChunkID.
	Split(content string, params types.ChunkParams, embedConfigID string) []types.Chunk
}

// DefaultSplitter implements ChunkSplitter by dispatching on params.Mode. It
// holds no state of its own; a single instance can be shared across knowledge
// bases and goroutines.
type DefaultSplitter struct {
	window *SlidingWindowSplitter
	cdc    *CDCSplitter
}

// NewDefault constructs the full splitter set — every mode this build knows
// how to apply. The write path is wired with one of these rather than with a
// single algorithm, because which algorithm applies is read from the knowledge
// base's metadata at write time.
func NewDefault() *DefaultSplitter {
	return &DefaultSplitter{window: NewSlidingWindowSplitter(), cdc: NewCDCSplitter()}
}

// Split implements ChunkSplitter. Only an explicit ChunkModeWindow picks the
// sliding window; everything else — including the zero value, which is what
// metadata decoded from an older snapshot, or read back through the control
// plane, carries — is chunked content-defined.
func (d *DefaultSplitter) Split(content string, params types.ChunkParams, embedConfigID string) []types.Chunk {
	if params.Mode == types.ChunkModeWindow {
		return d.window.Split(content, params, embedConfigID)
	}
	return d.cdc.Split(content, params, embedConfigID)
}

// newChunk computes ChunkID = SHA-256(chunk text + embedConfigID) and packages
// it with the chunk text. Both implementations hash through here, so a chunk's
// ID depends only on its text and the embed configuration — never on which
// algorithm produced it, or on where it sat in the document. That is the
// property the whole reuse story rests on
// (docs/content-defined-chunking-plan.md §3.7).
func newChunk(text string, embedConfigID string) types.Chunk {
	h := sha256.Sum256([]byte(text + embedConfigID))
	return types.Chunk{
		ChunkID: hex.EncodeToString(h[:]),
		Content: text,
	}
}

var _ ChunkSplitter = (*DefaultSplitter)(nil)
