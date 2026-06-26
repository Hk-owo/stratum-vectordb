// Package embed defines the EmbedClient interface — the client used to
// call an external embed service to generate vectors for chunks.
//
// See Stratum_接口设计v9.md "EmbedClient" and Stratum_设计文档v10.md
// "文档切割与向量生成" for the authoritative design. This file contains
// only the interface definition; the HTTP-backed implementation
// (HTTPEmbedClient) is built in Phase 4 (4-A), against a mock embed HTTP
// server.
package embed

import (
	"context"

	"stratum/internal/types"
)

// EmbedClient calls an external embed service to generate vectors for a
// batch of chunks. A knowledge base is bound to one EmbedClient
// configuration (types.EmbedConfig) at creation time; the binding is
// immutable.
type EmbedClient interface {
	// Embed generates a vector for each chunk in chunks and returns a map
	// from ChunkID to vector. Implementations should treat partial
	// failures (some chunks succeed, some fail) according to their own
	// retry/timeout policy; the contract here is "either return a
	// complete map covering every input ChunkID, or return an error" —
	// callers must not have to special-case a partially populated map.
	Embed(ctx context.Context, chunks []types.Chunk) (map[string][]float32, error)
}
