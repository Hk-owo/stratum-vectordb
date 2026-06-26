package embed

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"sync"

	"stratum/internal/types"
)

// MockEmbedClient is a deterministic, in-process stand-in for EmbedClient,
// for use in unit tests of modules that depend on EmbedClient
// (WriteCoordinator, etc.) and in integration tests that need a fast,
// network-free embed service (see Stratum_测试顺序.md's "mock embed HTTP
// server" for the over-the-wire equivalent used in later test batches).
//
// Vectors are derived deterministically from each chunk's ChunkID via a
// hash, so the same chunk always embeds to the same vector across calls —
// this matters for tests asserting chunk dedup behavior, and lets HNSW
// build/search tests get distinguishable (if not semantically meaningful)
// vectors. The mock supports a configurable fixed call latency and
// injectable failures for exercising WriteCoordinator's retry and
// timeout-handling paths.
type MockEmbedClient struct {
	dim int

	mu       sync.Mutex
	failNext int // if > 0, the next N calls to Embed return Err and decrement this counter
	err      error
	callLog  []int // records len(chunks) for each Embed call, for assertions
}

// NewMockEmbedClient constructs a MockEmbedClient that produces vectors of
// dimension dim.
func NewMockEmbedClient(dim int) *MockEmbedClient {
	return &MockEmbedClient{dim: dim}
}

// Embed implements EmbedClient. It also honors ctx cancellation, since the
// code style document requires all waiting operations to respect
// ctx.Done() — a real HTTP-backed client would too.
func (c *MockEmbedClient) Embed(ctx context.Context, chunks []types.Chunk) (map[string][]float32, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c.mu.Lock()
	c.callLog = append(c.callLog, len(chunks))
	if c.failNext > 0 {
		c.failNext--
		err := c.err
		c.mu.Unlock()
		if err == nil {
			err = fmt.Errorf("embed: mock injected failure")
		}
		return nil, err
	}
	c.mu.Unlock()

	out := make(map[string][]float32, len(chunks))
	for _, chunk := range chunks {
		out[chunk.ChunkID] = deterministicVector(chunk.ChunkID, c.dim)
	}
	return out, nil
}

// FailNextCalls configures the next n calls to Embed to return err (or a
// generic error if err is nil). Used by tests exercising retry behavior.
func (c *MockEmbedClient) FailNextCalls(n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failNext = n
	c.err = err
}

// CallCount returns how many times Embed has been called so far. Used by
// tests asserting batching/retry call counts.
func (c *MockEmbedClient) CallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.callLog)
}

// Reset clears call history and any pending injected failure. Convenience
// for tests; not part of the EmbedClient interface.
func (c *MockEmbedClient) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failNext = 0
	c.err = nil
	c.callLog = nil
}

// deterministicVector derives a fixed-dimension unit vector from a
// ChunkID's hash so that the same input always produces the same output,
// and different inputs are very likely to produce different vectors.
func deterministicVector(chunkID string, dim int) []float32 {
	h := sha256.Sum256([]byte(chunkID))
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		// Cycle through the 32 hash bytes, mixing in the index so that
		// dimensions beyond 32 don't simply repeat.
		b := h[(i+i/32)%len(h)]
		vec[i] = float32(b) / 255.0
	}
	normalize(vec)
	return vec
}

func normalize(vec []float32) {
	var sumSq float64
	for _, v := range vec {
		sumSq += float64(v) * float64(v)
	}
	norm := math.Sqrt(sumSq)
	if norm == 0 {
		return
	}
	for i := range vec {
		vec[i] = float32(float64(vec[i]) / norm)
	}
}

var _ EmbedClient = (*MockEmbedClient)(nil)
