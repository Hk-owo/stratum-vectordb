package index

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"stratum/internal/types"
)

// buildStateSpy records the transitions the manager reports to the node state
// machine, so the wiring can be pinned without standing one up.
type buildStateSpy struct {
	mu     sync.Mutex
	events []string
}

func (s *buildStateSpy) record(e string) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
}

func (s *buildStateSpy) BuildScheduled(string, int64) error { s.record("scheduled"); return nil }

func (s *buildStateSpy) BuildSucceeded(string, int64) error { s.record("succeeded"); return nil }

func (s *buildStateSpy) BuildFailed(_ string, _ int64, detail string) error {
	s.record("failed: " + detail)
	return nil
}

func (s *buildStateSpy) IndexInstalled(string, int64) error { s.record("installed"); return nil }

func (s *buildStateSpy) IndexDropped(string, int64) error { s.record("dropped"); return nil }

func (s *buildStateSpy) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.events))
	copy(out, s.events)
	return out
}

// TestIndexManager_ReportsBuildTransitionsToTheStateMachine: the manager is the
// single entry point a build has, so it — not its callers, and not the
// completion callback — is where the version layer's BUILDING -> READY
// transition is recorded (docs/storage-node-state-machine-plan.md §7, item 4).
func TestIndexManager_ReportsBuildTransitionsToTheStateMachine(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-a", "chunk-b"}, map[string][]float32{
		"chunk-a": {0.1, 0.2, 0.3},
		"chunk-b": {0.4, 0.5, 0.6},
	})
	spy := &buildStateSpy{}

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		VecstoreAddr:    "unused", // the client is injected below
		BuildState:      spy,
	})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	done := make(chan struct{})
	im.RegisterBuildCallback(func(string, int64, types.IndexStatus) error {
		close(done)
		return nil
	})

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the build never completed")
	}

	// The completion callback fires from inside doBuild's cleanup, AFTER the
	// state machine has been told, so both transitions are present here.
	events := spy.snapshot()
	if len(events) != 2 || events[0] != "scheduled" || events[1] != "succeeded" {
		t.Fatalf("transitions = %v, want [scheduled succeeded]", events)
	}
}

// TestIndexManager_ReportsABuildThatNeverStarts: with the pool closed nothing
// runs, so no completion callback ever fires — and the version would read
// BUILDING forever to anyone asking the state machine. That case is the whole
// reason these transitions live in the manager rather than in a callback.
func TestIndexManager_ReportsABuildThatNeverStarts(t *testing.T) {
	spy := &buildStateSpy{}
	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		VecstoreAddr:    "unused",
		BuildState:      spy,
	})
	// The pool is created lazily, so build it and shut it down before anything
	// can be queued on it.
	im.buildPoolOnce.Do(func() { im.buildPool = newBuildPool(1, func(buildRequest) {}, nil) })
	im.buildPool.Close()

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err == nil {
		t.Fatal("TriggerBuild with a closed pool succeeded, want an error")
	}

	events := spy.snapshot()
	if len(events) != 2 {
		t.Fatalf("transitions = %v, want the schedule and its correction", events)
	}
	if events[0] != "scheduled" {
		t.Errorf("first transition = %q, want scheduled", events[0])
	}
	if !strings.HasPrefix(events[1], "failed: ") {
		t.Errorf("second transition = %q, want a failure: no build ever ran", events[1])
	}
}

// TestIndexManager_BuildStateIsOptional: without a recorder the transitions are
// simply not reported, which is the unit-test default — and the build itself
// must be unaffected by its absence.
func TestIndexManager_BuildStateIsOptional(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.1, 0.2, 0.3}})

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		VecstoreAddr:    "unused",
	})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	done := make(chan types.IndexStatus, 1)
	im.RegisterBuildCallback(func(_ string, _ int64, status types.IndexStatus) error {
		done <- status
		return nil
	})

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	select {
	case status := <-done:
		if status != types.IndexStatusReady {
			t.Fatalf("build status = %v, want READY", status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the build never completed without a state recorder")
	}
}
