package splitter

import (
	"bytes"
	"sort"
	"unicode/utf8"

	"github.com/restic/chunker"

	"stratum/internal/types"
)

const (
	// cdcPolynomial is the Rabin fingerprint polynomial the chunker rolls: the
	// value restic's own examples use. It only has to be a fixed irreducible
	// polynomial — but it is part of what a boundary is, so changing it re-cuts
	// every document.
	cdcPolynomial = 0x3DA3358B4DC173

	// cdcRollingWindow is the chunker's rolling window, in bytes. It is a hard
	// floor on MinSize rather than a tuning knob: the chunker computes
	// `pre = MinSize - cdcRollingWindow` in uint, so anything smaller
	// underflows silently (docs/content-defined-chunking-plan.md §3.2).
	cdcRollingWindow = 64

	// cdcSentenceSlack bounds how far past a content-defined boundary we look
	// for a sentence end to snap to (§3.4). 64 bytes is roughly one Chinese
	// sentence's tail, and it is far short of a chunk, so a snapped boundary
	// can never reach its neighbour.
	cdcSentenceSlack = 64
)

// CDCSplitter is the content-defined ChunkSplitter implementation: boundaries
// come from a rolling Rabin fingerprint over the document's bytes, so they are
// decided by content instead of by offset from the start. A mid-document
// insertion or deletion therefore disturbs only the chunks around it; every
// boundary after the edit is recomputed from the same bytes as before and lands
// in the same place, which is what keeps the chunk IDs — and with them the
// stored vectors and their embeddings — reusable
// (docs/content-defined-chunking-plan.md §1.2b).
//
// Three adjustments turn the raw fingerprint boundaries into chunks a retrieval
// index wants:
//
//   - each boundary is snapped to a rune start, so a chunk never ends inside a
//     multi-byte character (§3.3). That one is a correctness line, not a
//     tuning choice: without it roughly 58% of chunks on Chinese text are
//     invalid UTF-8;
//   - a boundary is nudged forward to just after a sentence-ending punctuation
//     mark or a newline when one is within reach, so chunks stay semantically
//     whole (§3.4);
//   - both examine only the bytes near the boundary, never its absolute
//     offset. A rule that read the offset would shift every boundary after an
//     edit and give up the entire point of going content-defined.
//
// The chunk text is hashed exactly as the sliding window hashes it (newChunk),
// so switching a knowledge base between the two modes produces different chunk
// sets — which is why the mode has to be pinned per knowledge base (§3.6) — but
// never two different IDs for the same text (§3.7).
type CDCSplitter struct{}

// NewCDCSplitter constructs the content-defined splitter. It holds no state; a
// single instance can be shared across knowledge bases and goroutines.
func NewCDCSplitter() *CDCSplitter {
	return &CDCSplitter{}
}

// Split implements ChunkSplitter.
func (s *CDCSplitter) Split(content string, params types.ChunkParams, embedConfigID string) []types.Chunk {
	data := []byte(content)
	if len(data) == 0 {
		// Same contract as the sliding window: an empty document has no
		// chunks, so callers can treat "no changes" uniformly.
		return []types.Chunk{}
	}

	minSize, maxSize, avgBits := params.MinSize, params.MaxSize, params.AvgBits
	if minSize <= 0 {
		minSize = types.DefaultChunkMinSize
	}
	if minSize < cdcRollingWindow {
		minSize = cdcRollingWindow
	}
	if maxSize <= 0 {
		maxSize = types.DefaultChunkMaxSize
	}
	if maxSize <= minSize {
		// Degenerate configuration: the chunker would have no interval to
		// search for a boundary in. Falling back to one whole-document chunk
		// beats panicking or looping — the same trade the sliding window makes
		// for windowSize <= 0.
		return []types.Chunk{newChunk(content, embedConfigID)}
	}
	if avgBits <= 0 || avgBits > 30 {
		avgBits = types.DefaultChunkAvgBits
	}

	edges := cdcEdges(data, minSize, maxSize, avgBits)
	chunks := make([]types.Chunk, 0, len(edges)-1)
	for i := 0; i+1 < len(edges); i++ {
		if edges[i+1] <= edges[i] {
			continue // two boundaries snapped onto the same byte: no chunk here
		}
		chunks = append(chunks, newChunk(string(data[edges[i]:edges[i+1]]), embedConfigID))
	}
	if len(chunks) == 0 {
		return []types.Chunk{newChunk(content, embedConfigID)}
	}
	return chunks
}

// cdcEdges returns the chunk boundaries of data as byte offsets, first and last
// included: the raw content-defined boundaries, snapped to rune starts (§3.3)
// and then, where one is close enough, to a sentence end (§3.4). The result is
// non-decreasing, starts at 0 and ends at len(data), so every byte belongs to at
// least one chunk and none is lost.
func cdcEdges(data []byte, minSize, maxSize, avgBits int) []int {
	raw := cdcRawBounds(data, minSize, maxSize, avgBits)
	runeStarts := runeStartOffsets(data)

	edges := make([]int, len(raw))
	for i, b := range raw {
		edges[i] = snapToRuneStart(runeStarts, b)
	}

	// Nudge the interior boundaries to a sentence end where one is within
	// reach. The search stops at the NEXT boundary as well as at the slack, so
	// a snapped boundary cannot overtake its neighbour — that is what keeps
	// "every byte in at least one chunk" true without a second pass.
	for i := 1; i+1 < len(edges); i++ {
		limit := edges[i] + cdcSentenceSlack
		if edges[i+1] < limit {
			limit = edges[i+1]
		}
		edges[i] = snapToSentenceEnd(data, edges[i], limit)
	}
	return edges
}

// cdcRawBounds runs the chunker over data and returns its boundaries as byte
// offsets, including 0 and len(data).
func cdcRawBounds(data []byte, minSize, maxSize, avgBits int) []int {
	c := chunker.NewWithBoundaries(bytes.NewReader(data), chunker.Pol(cdcPolynomial), uint(minSize), uint(maxSize))
	c.SetAverageBits(avgBits)

	// The buffer only spares an allocation per chunk — Start and Length are
	// read, never Data.
	buf := make([]byte, 0, maxSize)

	bounds := []int{0}
	for {
		chunk, err := c.Next(buf)
		if err != nil {
			// io.EOF is the documented end of the stream, and a bytes.Reader
			// cannot fail any other way, so the boundaries collected so far are
			// the ones we keep.
			break
		}
		end := int(chunk.Start + chunk.Length)
		if end >= len(data) {
			break // this is the tail; len(data) is appended below
		}
		bounds = append(bounds, end)
	}
	return append(bounds, len(data))
}

// runeStartOffsets returns the byte offset of every rune start in data,
// followed by len(data).
func runeStartOffsets(data []byte) []int {
	offsets := make([]int, 0, utf8.RuneCount(data)+1)
	for i := 0; i < len(data); {
		offsets = append(offsets, i)
		_, size := utf8.DecodeRune(data[i:])
		if size <= 0 {
			size = 1 // does not happen; keeps the loop moving if it ever did
		}
		i += size
	}
	return append(offsets, len(data))
}

// snapToRuneStart returns the first rune start at or after b, so a boundary
// never lands inside a multi-byte character (§3.3).
func snapToRuneStart(offsets []int, b int) int {
	i := sort.SearchInts(offsets, b)
	if i == len(offsets) {
		return offsets[len(offsets)-1]
	}
	return offsets[i]
}

// snapToSentenceEnd returns the first byte boundary in (from, limit] that
// follows a sentence-ending punctuation mark or a newline, or from when there
// is none. Only data between from and limit is examined: the decision depends
// on local content alone, never on where from sits in the document (§3.4).
func snapToSentenceEnd(data []byte, from, limit int) int {
	for i := from; i < limit; {
		r, size := utf8.DecodeRune(data[i:])
		if size <= 0 {
			return from
		}
		next := i + size
		if next > limit {
			return from // the punctuation straddles the limit: not a candidate
		}
		if isSentenceEnd(r) {
			return next
		}
		i = next
	}
	return from
}

// isSentenceEnd reports whether r closes a sentence in the text this project
// indexes: the full-width punctuation Chinese prose uses, its ASCII
// equivalents, and the newline that closes a line (§3.4).
func isSentenceEnd(r rune) bool {
	switch r {
	case '。', '！', '？', '；', '\n', '.', '!', '?', ';':
		return true
	}
	return false
}

var _ ChunkSplitter = (*CDCSplitter)(nil)
