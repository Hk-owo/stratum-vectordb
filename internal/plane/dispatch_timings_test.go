package plane

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// TestCoordinatorDispatcher_ReportsStageTimings pins stage 4's line: resolving
// the candidate list, each candidate's turn at the write, and which one took it.
//
// Why the per-candidate split matters: the client has already been answered when
// this runs, so the only symptom of a bad dispatch is a version that stays
// PENDING. "The first candidate was gone and absorbed its whole budget" and "the
// candidate really did write the batch" produce the same total time, and only the
// per-attempt breakdown tells them apart — which is what decides whether the fix
// is the candidate list or the write itself.
func TestCoordinatorDispatcher_ReportsStageTimings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The refusing candidate's address is picked for the SORT, not for realism:
	// the dispatcher tries candidates in a stable order (candidates() sorts them),
	// so an address that compares below any real ephemeral port is what guarantees
	// it is attempted first. What this test is about is that a failed attempt is
	// recorded and the next candidate still gets its turn.
	refusingAddr := "127.0.0.1:1"
	accepting := &fakeDataSyncServer{}
	acceptingAddr := startFakeDataSyncServer(t, accepting)

	core, logs := observer.New(zap.DebugLevel)
	d := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{
		// Order here does not decide the order of attempts (see refusingAddr), and
		// that is the point: the per-attempt breakdown must not depend on it.
		Replicas: func(context.Context) ([]string, error) {
			return []string{acceptingAddr, refusingAddr}, nil
		},
		Logger: zap.New(core),
	})

	if _, err := d.Dispatch(ctx, "kb-1", 7, 6, dispatchChanges()); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	entry, ok := dispatchTimingsEntry(logs, "plane: dispatch timings")
	if !ok {
		t.Fatalf("no dispatch-timings line was emitted; got %v", dispatchLogMessages(logs))
	}
	fields := entry.ContextMap()
	if got := fields["kb_id"]; got != "kb-1" {
		t.Errorf("kb_id = %v, want kb-1", got)
	}
	if got := fields["version_id"]; got != int64(7) {
		t.Errorf("version_id = %v, want 7", got)
	}
	if got := fields["changes"]; got != int64(1) {
		t.Errorf("changes = %v, want 1", got)
	}
	if got := fields["candidates"]; got != int64(2) {
		t.Errorf("candidates = %v, want 2 (both replicas)", got)
	}
	// The accepting replica is the one that must be named, whichever order the
	// dispatcher tried the two in.
	if got := fields["chosen"]; got != acceptingAddr {
		t.Errorf("chosen = %v, want %q", got, acceptingAddr)
	}
	tried, ok := fields["tried"].([]interface{})
	if !ok || len(tried) != 2 {
		t.Fatalf("tried = %v, want both candidates recorded", fields["tried"])
	}
	triedUs, ok := fields["tried_us"].([]interface{})
	if !ok || len(triedUs) != len(tried) {
		t.Fatalf("tried_us = %v, want one duration per attempt", fields["tried_us"])
	}
	for i, v := range triedUs {
		if us, ok := v.(int64); !ok || us < 0 {
			t.Errorf("tried_us[%d] = %v, want a non-negative duration", i, v)
		}
	}
	attemptsUs, ok := fields["attempts_us"].(int64)
	if !ok || attemptsUs <= 0 {
		t.Errorf("attempts_us = %v, want the attempts' summed time", fields["attempts_us"])
	}
	if _, present := fields["resolve_us"]; !present {
		t.Error("resolve_us is missing")
	}
	if budget, ok := fields["budget_ms"].(int64); !ok || budget <= 0 {
		t.Errorf("budget_ms = %v, want the per-candidate budget that was in force", fields["budget_ms"])
	}
}

func dispatchTimingsEntry(logs *observer.ObservedLogs, message string) (observer.LoggedEntry, bool) {
	for _, entry := range logs.All() {
		if entry.Message == message {
			return entry, true
		}
	}
	return observer.LoggedEntry{}, false
}

func dispatchLogMessages(logs *observer.ObservedLogs) []string {
	var out []string
	for _, entry := range logs.All() {
		out = append(out, entry.Message)
	}
	return out
}
