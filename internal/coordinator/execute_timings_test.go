package coordinator

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"stratum/internal/bloom"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// TestWriteCoordinator_ExecuteReportsStageTimings pins stage 3's line — the half
// of a write the CLIENT waits for.
//
// It has to report on its own, because nothing downstream can: the response
// carries the version id and the storage transaction (stage 5) and the index
// build (stage 6) happen after the client is answered. txn_wait_us is the part
// with no other measurement at all — §7.7 serializes writes to one knowledge
// base on txnMu, so a burst of concurrent versions is visible here and nowhere
// else.
func TestWriteCoordinator_ExecuteReportsStageTimings(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	coord := newTimedWriteCoordinator(t, zap.New(core), nil)

	changes := []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "hello world this is a test document"},
		{Op: types.ChangeOpAdd, DocID: "doc-2", Content: "another document with different content"},
	}
	versionID, err := coord.Execute(context.Background(), "kb-1", 0, changes, "")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	entry, ok := executeTimingsEntry(logs, "coordinator: write execute timings")
	if !ok {
		t.Fatalf("no execute-timings line was emitted; got %v", executeLogMessages(logs))
	}
	fields := entry.ContextMap()
	if got := fields["kb_id"]; got != "kb-1" {
		t.Errorf("kb_id = %v, want kb-1", got)
	}
	if got := fields["version_id"]; got != versionID {
		t.Errorf("version_id = %v, want the id that was returned (%d)", got, versionID)
	}
	if got := fields["changes"]; got != int64(2) {
		t.Errorf("changes = %v, want 2", got)
	}
	// No dispatcher is wired in this stack, so the storage transaction ran
	// inline — which is the case storage_us exists for.
	if got := fields["dispatched"]; got != false {
		t.Errorf("dispatched = %v, want false on the un-dispatched path", got)
	}
	if _, present := fields["txn_wait_us"]; !present {
		t.Error("txn_wait_us is missing")
	}
	if _, present := fields["register_us"]; !present {
		t.Error("register_us is missing")
	}
	if proposeUs, ok := fields["propose_us"].(int64); !ok || proposeUs < 0 {
		t.Errorf("propose_us = %v, want the Raft round trip's time", fields["propose_us"])
	}
	if total, ok := fields["total_us"].(int64); !ok || total <= 0 {
		t.Errorf("total_us = %v, want a positive total", fields["total_us"])
	}
}

// The dispatched path reports the hand-off instead of the storage transaction:
// the storage work has not happened yet when this line is written, so reporting
// a 0 for it is honest and reporting the hand-off's cost is what the caller
// actually paid.
func TestWriteCoordinator_ExecuteReportsDispatchHandoff(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)

	var dispatched int
	coord := newTimedWriteCoordinator(t, zap.New(core), func(context.Context, string, int64, int64, []types.DocChange) error {
		dispatched++
		return nil
	})

	changes := []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "hello world this is a test document"},
	}
	if _, err := coord.Execute(context.Background(), "kb-1", 0, changes, "req-1"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	coord.dispatchWG.Wait()

	entry, ok := executeTimingsEntry(logs, "coordinator: write execute timings")
	if !ok {
		t.Fatalf("no execute-timings line was emitted; got %v", executeLogMessages(logs))
	}
	fields := entry.ContextMap()
	if got := fields["dispatched"]; got != true {
		t.Errorf("dispatched = %v, want true", got)
	}
	if got := fields["storage_us"]; got != int64(0) {
		t.Errorf("storage_us = %v, want 0 on the dispatched path (the work was handed off)", got)
	}
	if _, present := fields["handoff_us"]; !present {
		t.Error("handoff_us is missing")
	}
	if dispatched != 1 {
		t.Errorf("the dispatcher was called %d times, want 1", dispatched)
	}
}

// newTimedWriteCoordinator builds the same minimal write stack the other tests
// in this package use, with a logger attached and an optional dispatcher.
func newTimedWriteCoordinator(t *testing.T, logger *zap.Logger, dispatch func(context.Context, string, int64, int64, []types.DocChange) error) *WriteCoordinatorImpl {
	t.Helper()
	rn := newTestRaftNode()
	rn.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 100, ChunkOverlapSize: 20,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})
	return NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
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
		Dispatch:            dispatch,
		Logger:              logger,
	})
}

func executeTimingsEntry(logs *observer.ObservedLogs, message string) (observer.LoggedEntry, bool) {
	for _, entry := range logs.All() {
		if entry.Message == message {
			return entry, true
		}
	}
	return observer.LoggedEntry{}, false
}

func executeLogMessages(logs *observer.ObservedLogs) []string {
	var out []string
	for _, entry := range logs.All() {
		out = append(out, entry.Message)
	}
	return out
}
