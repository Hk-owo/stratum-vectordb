//go:build ignore

// Mock embed HTTP server for Stratum integration testing.
// Returns deterministic vectors from chunk IDs so tests can assert on results.
// Run: go run mock_embed_server.go
package main

import (
	"crypto/sha256"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

const defaultDim = 768

func main() {
	dim := defaultDim
	if v := os.Getenv("VEC_DIM"); v != "" {
		if d, err := strconv.Atoi(v); err == nil && d > 0 {
			dim = d
		}
	}
	latency := 10 * time.Millisecond
	if v := os.Getenv("LATENCY_MS"); v != "" {
		if d, err := strconv.Atoi(v); err == nil {
			latency = time.Duration(d) * time.Millisecond
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleEmbed(w, r, dim, latency)
	})

	addr := ":8080"
	log.Printf("mock embed server listening on %s (dim=%d, latency=%v)", addr, dim, latency)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func handleEmbed(w http.ResponseWriter, r *http.Request, dim int, latency time.Duration) {
	if latency > 0 {
		time.Sleep(latency)
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
		resp.Vectors[ch.ChunkID] = deterministicVector(ch.ChunkID, dim)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func deterministicVector(chunkID string, dim int) []float32 {
	h := sha256.Sum256([]byte(chunkID))
	vec := make([]float32, dim)
	for i := range vec {
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
	if sumSq == 0 {
		return
	}
	norm := float32(1.0 / math.Sqrt(sumSq))
	for i := range vec {
		vec[i] *= norm
	}
}
