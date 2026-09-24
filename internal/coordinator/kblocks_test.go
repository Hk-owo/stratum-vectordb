package coordinator

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"stratum/internal/bloom"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// M7 of docs/code-review-2026-09-24.md: one global write lock serialized every
// knowledge base behind every other one's network round trip. What has to stay
// serialized is per knowledge base — that is the granularity the WAL's
// BEGIN/VERSION_ID pairing needs — and these tests pin both halves of that.
func TestKBLockSet_SerializesPerKnowledgeBase(t *testing.T) {
	locks := NewKBLockSet()

	unlockA := locks.Lock("kb-a")
	acquiredB := make(chan func(), 1)
	go func() { acquiredB <- locks.Lock("kb-b") }()

	select {
	case unlockB := <-acquiredB:
		unlockB()
	case <-time.After(2 * time.Second):
		t.Fatal("locking kb-b waited on kb-a's lock: the set is not per knowledge base")
	}

	// Same knowledge base still serializes — that is the property the WAL relies on.
	acquiredA2 := make(chan func(), 1)
	go func() { acquiredA2 <- locks.Lock("kb-a") }()
	select {
	case unlockA2 := <-acquiredA2:
		unlockA2()
		t.Fatal("kb-a's lock was acquired twice concurrently")
	case <-time.After(200 * time.Millisecond):
		// Expected: the second holder waits.
	}
	unlockA()
	select {
	case unlockA2 := <-acquiredA2:
		unlockA2()
	case <-time.After(2 * time.Second):
		t.Fatal("kb-a's lock was never released to the waiter")
	}
}

// The set must forget knowledge bases that are no longer writing: a deployment can
// create them forever, and a map that only grows is the same unbounded bookkeeping
// this review keeps finding.
func TestKBLockSet_ForgetsIdleKnowledgeBases(t *testing.T) {
	locks := NewKBLockSet()

	for i := 0; i < 100; i++ {
		unlock := locks.Lock("kb-1")
		unlock()
	}
	if got := locks.Len(); got != 0 {
		t.Fatalf("idle entries kept: Len() = %d, want 0", got)
	}

	// While one holder (or waiter) remains, the entry stays.
	unlock := locks.Lock("kb-1")
	if got := locks.Len(); got != 1 {
		t.Fatalf("Len() = %d with one holder, want 1", got)
	}
	unlock()
	if got := locks.Len(); got != 0 {
		t.Fatalf("Len() = %d after release, want 0", got)
	}
}

// A nil set is what a directly-constructed coordinator can hold, so the calls have to
// be safe on it rather than panicking deep inside a write.
func TestKBLockSet_NilIsSafe(t *testing.T) {
	var s *KBLockSet
	unlock := s.Lock("kb-1")
	unlock() // must not panic: there is nothing to unlock
	if got := s.Len(); got != 0 {
		t.Fatalf("Len() on a nil set = %d, want 0", got)
	}
}

// And the coordinator uses it that way: a write to one knowledge base proceeds
// while another knowledge base's write is in flight, while a second write to the
// SAME knowledge base still queues.
func TestWriteCoordinator_OnlyWaitsForTheSameKnowledgeBase(t *testing.T) {
	locks := NewKBLockSet()
	rn := newTestRaftNode()
	for _, kbID := range []string{"kb-a", "kb-b"} {
		if err := rn.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{
			KBID: kbID, Name: kbID, ChunkWindowSize: 100, ChunkOverlapSize: 20,
			EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
		}); err != nil {
			t.Fatalf("ProposeCreateKB(%s): %v", kbID, err)
		}
	}

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		Locks:               locks,
		WAL:                 wal.NewMockWAL(),
		RaftNode:            rn,
		Splitter:            &mockSplitter{windowSize: 100, overlapSize: 20},
		EmbedClient:         &testEmbedClient{},
		ChunkBloom:          bloom.NewMockBloomFilter(),
		ChunkStore:          newTestChunkStore(),
		ChunkDocMapper:      newTestChunkDocMapper(),
		DocStore:            newTestDocStore(),
		VersionDocList:      newTestVersionDocList(),
		IndexManager:        newTestIndexManager(),
		Logger:              zap.NewNop(),
	})

	changes := []types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "body"}}
	ctx := context.Background()

	// Stand in for "kb-a's write is in flight": its lock is held. Released
	// explicitly below rather than deferred: a double release is a panic, and the
	// release is part of what this test asserts.
	unlockA := locks.Lock("kb-a")

	other := make(chan error, 1)
	go func() {
		_, err := coord.Execute(ctx, "kb-b", 0, changes, "")
		other <- err
	}()
	select {
	case err := <-other:
		if err != nil {
			t.Fatalf("Execute(kb-b): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a write to kb-b waited behind kb-a: the write lock is global again")
	}

	same := make(chan error, 1)
	go func() {
		_, err := coord.Execute(ctx, "kb-a", 0, changes, "")
		same <- err
	}()
	select {
	case <-same:
		t.Fatal("a write to kb-a ran while kb-a's lock was held: the same knowledge base is no longer serialized")
	case <-time.After(300 * time.Millisecond):
		// Expected.
	}

	unlockA()
	select {
	case err := <-same:
		if err != nil {
			t.Fatalf("Execute(kb-a) after release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a write to kb-a never proceeded after its lock was released")
	}
}
