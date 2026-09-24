package service

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
)

// blockingBackfiller holds EnsureIndex open until released, so a burst of queries
// provably overlaps inside one pull.
type blockingBackfiller struct {
	mu      sync.Mutex
	n       int
	entered chan struct{}
	release chan struct{}
}

func newBlockingBackfiller() *blockingBackfiller {
	return &blockingBackfiller{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (b *blockingBackfiller) EnsureIndex(ctx context.Context, _ string, _ int64) error {
	b.mu.Lock()
	b.n++
	b.mu.Unlock()
	select {
	case b.entered <- struct{}{}:
	default:
	}
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blockingBackfiller) calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.n
}

// M1 of docs/code-review-2026-09-24.md: every query that missed on the same
// (kbID, version) used to start its own goroutine and its own four-attempt full
// pull, so N concurrent queries on a lagging replica were N full transfers of one
// version — amplification anyone able to send queries could ask for, and the
// refused caller learns nothing from the duplicates (they were already being
// refused by the freshness check).
func TestQueryService_DedupesConcurrentBackgroundPulls(t *testing.T) {
	h := newQuerySvcHarness(t)
	h.svc.SetLocalVersionReporter(stubCursor(5)) // this node's history reaches version 5
	puller := newBlockingBackfiller()
	h.svc.SetBackfiller(puller)

	required := int64(7)
	ask := func() {
		_, _ = h.svc.Query(context.Background(), &pb.QueryRequest{
			KnowledgeBaseId: "kb-1",
			Vector:          make([]float32, 8),
			TopK:            5,
			MinVersion:      &required,
		})
	}

	// A burst that all misses before any of them has started a pull.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ask()
		}()
	}
	wg.Wait()

	select {
	case <-puller.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the background pull never started")
	}
	// More misses while that pull is still in flight.
	for i := 0; i < 8; i++ {
		ask()
	}
	if got := puller.calls(); got != 1 {
		t.Fatalf("EnsureIndex calls while a pull is in flight = %d, want 1", got)
	}

	// Once it finishes, a later miss must be able to start a new one: the
	// de-duplication is "one at a time", not "one ever".
	close(puller.release)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && puller.calls() < 2 {
		ask()
		time.Sleep(20 * time.Millisecond)
	}
	if got := puller.calls(); got < 2 {
		t.Fatalf("a later query must be able to start a new pull after the first finished, calls=%d", got)
	}
}
