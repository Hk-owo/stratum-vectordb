package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"stratum/internal/types"
)

// HTTPEmbedClient is the real EmbedClient implementation: an HTTP client
// that calls an external embed service to generate vectors for chunks, per
// Stratum_接口设计v9.md "EmbedClient".
//
// The external service is expected to accept POST requests with a JSON body
// of the form:
//
//	{"chunks": [{"chunk_id": "...", "content": "..."}, ...]}
//
// and return a JSON response of the form:
//
//	{"vectors": {"<chunk_id>": [0.1, 0.2, ...], ...}}
type HTTPEmbedClient struct {
	addr       string
	httpClient *http.Client
}

// NewHTTPEmbedClient constructs an HTTPEmbedClient that calls the embed
// service at addr with the given timeout. Zero or negative timeout means
// no overall client-level timeout (per-request deadlines should then be
// driven by ctx).
func NewHTTPEmbedClient(addr string, timeout time.Duration) *HTTPEmbedClient {
	c := &HTTPEmbedClient{addr: addr}
	if timeout > 0 {
		c.httpClient = &http.Client{Timeout: timeout}
	} else {
		c.httpClient = &http.Client{}
	}
	return c
}

type embedRequest struct {
	Chunks []embedRequestChunk `json:"chunks"`
}

type embedRequestChunk struct {
	ChunkID string `json:"chunk_id"`
	Content string `json:"content"`
}

type embedResponse struct {
	Vectors map[string][]float32 `json:"vectors"`
}

// Embed implements EmbedClient. It sends all chunks in a single HTTP POST
// to the configured embed service and returns the complete vector map, or
// an error if any chunk failed. Partial results are not returned.
func (c *HTTPEmbedClient) Embed(ctx context.Context, chunks []types.Chunk) (map[string][]float32, error) {
	if len(chunks) == 0 {
		return make(map[string][]float32), nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	reqBody := embedRequest{Chunks: make([]embedRequestChunk, len(chunks))}
	for i, ch := range chunks {
		reqBody.Chunks[i] = embedRequestChunk{ChunkID: ch.ChunkID, Content: ch.Content}
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("embed: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("embed: create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("embed: call %s: %w", c.addr, err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20)) // 32 MiB limit
	if err != nil {
		return nil, fmt.Errorf("embed: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// Limit the error body to prevent leaking large error pages into logs.
		bodyPreview := string(respBytes)
		if len(bodyPreview) > 512 {
			bodyPreview = bodyPreview[:512] + "..."
		}
		return nil, fmt.Errorf("embed: %s returned status %d: %s", c.addr, resp.StatusCode, bodyPreview)
	}

	var embedResp embedResponse
	if err := json.Unmarshal(respBytes, &embedResp); err != nil {
		return nil, fmt.Errorf("embed: unmarshal response: %w", err)
	}
	if len(embedResp.Vectors) != len(chunks) {
		return nil, fmt.Errorf("embed: expected %d vectors, got %d", len(chunks), len(embedResp.Vectors))
	}

	return embedResp.Vectors, nil
}

var _ EmbedClient = (*HTTPEmbedClient)(nil)
