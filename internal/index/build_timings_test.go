package index

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"stratum/internal/types"
)

// TestIndexManager_ReportsBuildStageTimings pins stage 6's line — the version's
// wait for READY, split into the time it spent in the pool's queue and the time
// it spent actually building.
//
// The split is the point: a build that took 90 s behind four others is a capacity
// problem and the fix is more workers, while the same 90 s inside build() is a
// vecstore or batch problem. Everything downstream of a write sees only "PENDING
// for 90 s", which cannot tell the two apart.
func TestIndexManager_ReportsBuildStageTimings(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.1, 0.2, 0.3}})

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		VecstoreAddr:    "unused", // the client is injected directly
	})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	core, logs := observer.New(zap.DebugLevel)
	im.SetLogger(zap.New(core))

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
	case <-time.After(5 * time.Second):
		t.Fatal("the build never completed")
	}

	entry, ok := buildTimingsEntry(logs, "index: build timings")
	if !ok {
		t.Fatalf("no build-timings line was emitted; got %v", buildLogMessages(logs))
	}
	fields := entry.ContextMap()
	if got := fields["kb_id"]; got != "kb-1" {
		t.Errorf("kb_id = %v, want kb-1", got)
	}
	if got := fields["version_id"]; got != int64(1) {
		t.Errorf("version_id = %v, want 1", got)
	}
	if got := fields["status"]; got != types.IndexStatusReady.String() {
		t.Errorf("status = %v, want %s", got, types.IndexStatusReady.String())
	}
	if got := fields["priority"]; got != "interactive" {
		t.Errorf("priority = %v, want interactive (a writer is waiting on this build)", got)
	}
	for _, name := range []string{"queue_us", "build_us", "total_us", "size_bytes"} {
		if _, present := fields[name]; !present {
			t.Errorf("field %s is missing", name)
		}
	}
	// The two parts are measured inside the whole, so neither can exceed it.
	total, _ := fields["total_us"].(int64)
	if queue, ok := fields["queue_us"].(int64); ok && queue > total {
		t.Errorf("queue_us = %d exceeds total_us = %d", queue, total)
	}
	if build, ok := fields["build_us"].(int64); ok && build > total {
		t.Errorf("build_us = %d exceeds total_us = %d", build, total)
	}
}

// A failed build reports the same line, with the FAILED status. That is the
// case an operator is actually looking for: a version that never reaches READY
// has no other measurement of where its time went.
func TestIndexManager_ReportsBuildStageTimingsOnFailure(t *testing.T) {
	vc := newMockVectorIndexClient()
	vc.buildErr = context.DeadlineExceeded

	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.1, 0.2, 0.3}})

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 2 * time.Second,
		VecstoreAddr:    "unused",
	})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	core, logs := observer.New(zap.DebugLevel)
	im.SetLogger(zap.New(core))

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
		if !status.IsFailed() {
			t.Fatalf("build status = %v, want a failed status", status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the build never reported an outcome")
	}

	entry, ok := buildTimingsEntry(logs, "index: build timings")
	if !ok {
		t.Fatalf("no build-timings line was emitted for a failed build; got %v", buildLogMessages(logs))
	}
	fields := entry.ContextMap()
	if got, ok := fields["build_us"].(int64); !ok || got <= 0 {
		t.Errorf("build_us = %v, want the failed attempt's time", fields["build_us"])
	}
	if got := fields["status"]; got == types.IndexStatusReady.String() {
		t.Errorf("status = %v, want a failed status", got)
	}
}

func buildTimingsEntry(logs *observer.ObservedLogs, message string) (observer.LoggedEntry, bool) {
	for _, entry := range logs.All() {
		if entry.Message == message {
			return entry, true
		}
	}
	return observer.LoggedEntry{}, false
}

func buildLogMessages(logs *observer.ObservedLogs) []string {
	var out []string
	for _, entry := range logs.All() {
		out = append(out, entry.Message)
	}
	return out
}
