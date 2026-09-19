package router

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	pb "stratum/api/proto/stratum"
)

// TestForwardWrite_ReportsStageTimings pins the station's share of the write
// path's staged timings: what the station decided on its own (admission), how
// long finding the leader took, what the forwarded attempt took, and the version
// the response named — the join key back to the control and storage nodes.
//
// The numbers are not the point (they are a few ms of a fake call); the point is
// that the line exists, carries every field, and reports the version id it was
// answered with. Without the version id this line cannot be tied to the two
// behind it, which is the whole reason it exists.
func TestForwardWrite_ReportsStageTimings(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	r := &Router{
		controlAddrs: []string{"a", "b", "c"},
		discoverer:   &fakeResolver{order: []int{1}, ok: true},
		logger:       zap.New(core),
	}

	const admission = 2 * time.Millisecond
	const callTook = 5 * time.Millisecond
	_, err := forwardWrite(r, context.Background(), "kb-1", admission,
		func(idx int, ctx context.Context) (*pb.CreateVersionResponse, error) {
			time.Sleep(callTook)
			return &pb.CreateVersionResponse{VersionId: 7}, nil
		})
	if err != nil {
		t.Fatalf("forwardWrite: %v", err)
	}

	entry, ok := routerTimingsEntry(logs, "router: write forward timings")
	if !ok {
		t.Fatalf("no stage-timings line was emitted; got %v", routerLogMessages(logs))
	}
	fields := entry.ContextMap()
	if got := fields["kb_id"]; got != "kb-1" {
		t.Errorf("kb_id = %v, want kb-1", got)
	}
	if got := fields["version_id"]; got != int64(7) {
		t.Errorf("version_id = %v, want 7 (read out of the response)", got)
	}
	if got := fields["attempts"]; got != int64(1) {
		t.Errorf("attempts = %v, want 1", got)
	}
	if got := fields["leader_index"]; got != int64(1) {
		t.Errorf("leader_index = %v, want 1 (the scripted leader)", got)
	}
	if got := fields["admission_us"]; got != admission.Microseconds() {
		t.Errorf("admission_us = %v, want %d", got, admission.Microseconds())
	}
	total, ok := fields["total_us"].(int64)
	if !ok || total < callTook.Microseconds() {
		t.Errorf("total_us = %v, want a time at least as long as the call (%d us)", fields["total_us"], callTook.Microseconds())
	}
	if attempt, ok := fields["attempt_us"].(int64); !ok || attempt < callTook.Microseconds() {
		t.Errorf("attempt_us = %v, want the forwarded call's time (%d us at least)", fields["attempt_us"], callTook.Microseconds())
	}
	for _, name := range []string{"leader_lookup_us", "admission_us", "attempt_us", "total_us"} {
		if _, present := fields[name]; !present {
			t.Errorf("field %s is missing", name)
		}
	}
}

// A write that never reaches a version reports the line anyway, with no version
// id. Reporting only on success would hide exactly the writes an operator asks
// about — and the deferred emit is what makes the failure path measurable.
func TestForwardWrite_ReportsStageTimingsWithoutAVersion(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	r := &Router{
		controlAddrs: []string{"a", "b", "c"},
		discoverer:   &fakeResolver{order: []int{0}, ok: true},
		logger:       zap.New(core),
	}

	_, err := forwardWrite(r, context.Background(), "kb-1", 0,
		func(idx int, ctx context.Context) (*pb.CreateVersionResponse, error) {
			return nil, notLeaderErr()
		})
	if err == nil {
		t.Fatal("a not-leader answer from every node must fail the call")
	}

	entry, ok := routerTimingsEntry(logs, "router: write forward timings")
	if !ok {
		t.Fatalf("no stage-timings line was emitted; got %v", routerLogMessages(logs))
	}
	fields := entry.ContextMap()
	if got := fields["version_id"]; got != int64(0) {
		t.Errorf("version_id = %v, want 0 when the write produced no version", got)
	}
	if got, ok := fields["attempts"].(int64); !ok || got < 1 {
		t.Errorf("attempts = %v, want the retries to be counted", fields["attempts"])
	}
}

func routerTimingsEntry(logs *observer.ObservedLogs, message string) (observer.LoggedEntry, bool) {
	for _, entry := range logs.All() {
		if entry.Message == message {
			return entry, true
		}
	}
	return observer.LoggedEntry{}, false
}

func routerLogMessages(logs *observer.ObservedLogs) []string {
	var out []string
	for _, entry := range logs.All() {
		out = append(out, entry.Message)
	}
	return out
}
