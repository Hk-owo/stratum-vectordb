package embed

import (
	"context"
	"errors"
	"testing"

	"stratum/internal/types"
)

func TestMockEmbedClient_Deterministic(t *testing.T) {
	c := NewMockEmbedClient(128)
	chunks := []types.Chunk{{ChunkID: "a", Content: "hello"}, {ChunkID: "b", Content: "world"}}

	v1, err := c.Embed(context.Background(), chunks)
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	v2, err := c.Embed(context.Background(), chunks)
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}

	for _, chunk := range chunks {
		if len(v1[chunk.ChunkID]) != 128 {
			t.Fatalf("vector dim = %d, want 128", len(v1[chunk.ChunkID]))
		}
		for i := range v1[chunk.ChunkID] {
			if v1[chunk.ChunkID][i] != v2[chunk.ChunkID][i] {
				t.Fatalf("vector for %s not deterministic across calls", chunk.ChunkID)
			}
		}
	}

	a := v1["a"]
	b := v1["b"]
	identical := true
	for i := range a {
		if a[i] != b[i] {
			identical = false
			break
		}
	}
	if identical {
		t.Fatalf("different chunks produced identical vectors")
	}
}

func TestMockEmbedClient_ReturnsAllInputChunkIDs(t *testing.T) {
	c := NewMockEmbedClient(8)
	chunks := []types.Chunk{{ChunkID: "x"}, {ChunkID: "y"}, {ChunkID: "z"}}
	vecs, err := c.Embed(context.Background(), chunks)
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	for _, chunk := range chunks {
		if _, ok := vecs[chunk.ChunkID]; !ok {
			t.Fatalf("missing vector for chunk %s", chunk.ChunkID)
		}
	}
}

func TestMockEmbedClient_FailNextCalls(t *testing.T) {
	c := NewMockEmbedClient(8)
	injected := errors.New("injected failure")
	c.FailNextCalls(2, injected)

	chunks := []types.Chunk{{ChunkID: "x"}}

	if _, err := c.Embed(context.Background(), chunks); !errors.Is(err, injected) {
		t.Fatalf("call 1 err = %v, want injected", err)
	}
	if _, err := c.Embed(context.Background(), chunks); !errors.Is(err, injected) {
		t.Fatalf("call 2 err = %v, want injected", err)
	}
	if _, err := c.Embed(context.Background(), chunks); err != nil {
		t.Fatalf("call 3 err = %v, want nil (failure budget exhausted)", err)
	}
}

func TestMockEmbedClient_RespectsContextCancellation(t *testing.T) {
	c := NewMockEmbedClient(8)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Embed(ctx, []types.Chunk{{ChunkID: "x"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestMockEmbedClient_CallCount(t *testing.T) {
	c := NewMockEmbedClient(8)
	chunks := []types.Chunk{{ChunkID: "x"}}
	if _, err := c.Embed(context.Background(), chunks); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if _, err := c.Embed(context.Background(), chunks); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if got := c.CallCount(); got != 2 {
		t.Fatalf("CallCount = %d, want 2", got)
	}
	c.Reset()
	if got := c.CallCount(); got != 0 {
		t.Fatalf("CallCount after Reset = %d, want 0", got)
	}
}
