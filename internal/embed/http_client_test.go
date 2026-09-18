package embed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"stratum/internal/types"
)

// mockEmbedServer is a test HTTP server that implements the embed service
// contract: POST with JSON body of chunk IDs and texts, returns vectors.
// Configurable latency and failure injection for exercising timeout/retry.
type mockEmbedServer struct {
	dim       int
	latency   time.Duration
	failCount int
	reqCount  int
}

func (s *mockEmbedServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.reqCount++

	if s.failCount > 0 {
		s.failCount--
		http.Error(w, "injected failure", http.StatusInternalServerError)
		return
	}

	if s.latency > 0 {
		time.Sleep(s.latency)
	}

	var req struct {
		Chunks []struct {
			ChunkID string `json:"chunk_id"`
			Content string `json:"content"`
		} `json:"chunks"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := struct {
		Vectors map[string][]float32 `json:"vectors"`
	}{Vectors: make(map[string][]float32, len(req.Chunks))}

	for _, ch := range req.Chunks {
		resp.Vectors[ch.ChunkID] = deterministicVector(ch.ChunkID, s.dim)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func startMockEmbedServer(dim int, latency time.Duration) (*httptest.Server, *mockEmbedServer) {
	handler := &mockEmbedServer{dim: dim, latency: latency}
	return httptest.NewServer(handler), handler
}

func TestHTTPEmbedClient_Success(t *testing.T) {
	srv, mock := startMockEmbedServer(128, 0)
	defer srv.Close()

	client := NewHTTPEmbedClient(srv.URL, 5*time.Second)

	chunks := []types.Chunk{
		{ChunkID: "chunk-1", Content: "hello"},
		{ChunkID: "chunk-2", Content: "world"},
	}

	vectors, err := client.Embed(context.Background(), chunks)
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}

	if len(vectors) != 2 {
		t.Fatalf("expected 2 vectors, got %d", len(vectors))
	}
	if _, ok := vectors["chunk-1"]; !ok {
		t.Error("missing vector for chunk-1")
	}
	if _, ok := vectors["chunk-2"]; !ok {
		t.Error("missing vector for chunk-2")
	}
	if mock.reqCount != 1 {
		t.Errorf("expected 1 request, got %d", mock.reqCount)
	}
}

func TestHTTPEmbedClient_Timeout(t *testing.T) {
	srv, _ := startMockEmbedServer(128, 200*time.Millisecond)
	defer srv.Close()

	client := NewHTTPEmbedClient(srv.URL, 10*time.Millisecond) // very short timeout

	chunks := []types.Chunk{
		{ChunkID: "chunk-1", Content: "hello"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	_, err := client.Embed(ctx, chunks)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestHTTPEmbedClient_ContextCancelled(t *testing.T) {
	srv, _ := startMockEmbedServer(128, 500*time.Millisecond)
	defer srv.Close()

	client := NewHTTPEmbedClient(srv.URL, 5*time.Second)
	chunks := []types.Chunk{{ChunkID: "chunk-1", Content: "hello"}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := client.Embed(ctx, chunks)
	if err == nil {
		t.Fatal("expected context cancelled error, got nil")
	}
}

func TestHTTPEmbedClient_EmptyChunks(t *testing.T) {
	srv, _ := startMockEmbedServer(128, 0)
	defer srv.Close()

	client := NewHTTPEmbedClient(srv.URL, 5*time.Second)

	vectors, err := client.Embed(context.Background(), nil)
	if err != nil {
		t.Fatalf("Embed with nil chunks failed: %v", err)
	}
	if len(vectors) != 0 {
		t.Errorf("expected 0 vectors for nil chunks, got %d", len(vectors))
	}

	vectors, err = client.Embed(context.Background(), []types.Chunk{})
	if err != nil {
		t.Fatalf("Embed with empty chunks failed: %v", err)
	}
	if len(vectors) != 0 {
		t.Errorf("expected 0 vectors, got %d", len(vectors))
	}
}

func TestHTTPEmbedClient_ServerError(t *testing.T) {
	srv, mock := startMockEmbedServer(128, 0)
	defer srv.Close()
	mock.failCount = 1

	client := NewHTTPEmbedClient(srv.URL, 5*time.Second)
	chunks := []types.Chunk{{ChunkID: "chunk-1", Content: "hello"}}

	_, err := client.Embed(context.Background(), chunks)
	if err == nil {
		t.Fatal("expected error from server failure, got nil")
	}
}

func TestHTTPEmbedClient_DeterministicVectors(t *testing.T) {
	srv, _ := startMockEmbedServer(128, 0)
	defer srv.Close()

	client := NewHTTPEmbedClient(srv.URL, 5*time.Second)
	chunks := []types.Chunk{{ChunkID: "same-id", Content: "same content"}}

	vectors1, err := client.Embed(context.Background(), chunks)
	if err != nil {
		t.Fatalf("first Embed failed: %v", err)
	}
	vectors2, err := client.Embed(context.Background(), chunks)
	if err != nil {
		t.Fatalf("second Embed failed: %v", err)
	}

	// Same ChunkID should produce same vectors (mocked server does this deterministically)
	if len(vectors1["same-id"]) != len(vectors2["same-id"]) {
		t.Fatal("vector dimensions differ between calls")
	}
	for i, v := range vectors1["same-id"] {
		if v != vectors2["same-id"][i] {
			t.Fatalf("vectors differ at index %d: %f vs %f", i, v, vectors2["same-id"][i])
		}
	}
}

// A request that names the same chunk twice must NOT be treated as a shortfall:
// the response is keyed by ChunkID, and identical content is the same chunk with
// the same vector. Comparing lengths rejected this — and since ChunkID is
// content-addressed, every write whose documents share text hit it. That is
// exactly how the docker integration's datavolume case stalled: a document
// sliced into 5 chunks whose texts repeat yielded 3 distinct ids, and all three
// storage nodes refused the write ("expected 5 vectors, got 3").
func TestHTTPEmbedClient_AcceptsDuplicateChunksInRequest(t *testing.T) {
	srv, _ := startMockEmbedServer(8, 0)
	defer srv.Close()

	client := NewHTTPEmbedClient(srv.URL, 5*time.Second)
	vectors, err := client.Embed(context.Background(), []types.Chunk{
		{ChunkID: "dup", Content: "same"},
		{ChunkID: "other", Content: "different"},
		{ChunkID: "dup", Content: "same"}, // same content ⇒ same ChunkID ⇒ one entry
	})
	if err != nil {
		t.Fatalf("Embed with a repeated chunk failed: %v", err)
	}
	if len(vectors) != 2 {
		t.Fatalf("vectors = %d, want 2 (one per distinct ChunkID)", len(vectors))
	}
	if _, ok := vectors["dup"]; !ok {
		t.Error("missing vector for dup")
	}
	if _, ok := vectors["other"]; !ok {
		t.Error("missing vector for other")
	}
}

// Dropping a chunk the request actually asked for is still an error: the check
// is about coverage of the distinct ids, not about counting entries.
func TestHTTPEmbedClient_RejectsResponseMissingAChunk(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"vectors":{"only-one":[]}}`))
	}))
	defer srv.Close()

	client := NewHTTPEmbedClient(srv.URL, 5*time.Second)
	if _, err := client.Embed(context.Background(), []types.Chunk{
		{ChunkID: "only-one", Content: "a"},
		{ChunkID: "missing", Content: "b"},
	}); err == nil {
		t.Fatal("Embed accepted a response that omitted a requested chunk")
	}
}
