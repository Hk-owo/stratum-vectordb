package sync

import (
	"context"
	"testing"
	"time"

	"stratum/internal/types"
	"stratum/internal/wal"
)

// TestVersionChangesPuller_RoundTripsDeltas drives the §7.5 delta path end to end
// over real gRPC: a peer's recorded changes come back as VersionDeltas keyed by
// version, parent included.
func TestVersionChangesPuller_RoundTripsDeltas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reader := &stubVersionChanges{deltas: map[int64]wal.VersionDelta{
		2: {VersionID: 2, ParentVersionID: 1, Changes: []types.DocChange{{Op: types.ChangeOpAdd, DocID: "d2", Content: "two"}}},
		3: {VersionID: 3, ParentVersionID: 2, Changes: []types.DocChange{{Op: types.ChangeOpDelete, DocID: "d3"}}},
	}}
	_, _, addr := startPushServer(t, 4, WithVersionChangesReader(reader))

	puller := NewVersionChangesPuller(PresenceCheckerConfig{})
	got, err := puller.ChangesInRange(ctx, addr, "kb-1", 1, 3)
	if err != nil {
		t.Fatalf("ChangesInRange: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("deltas = %v, want versions 2 and 3", got)
	}
	if got[2].ParentVersionID != 1 || len(got[2].Changes) != 1 || got[2].Changes[0].Content != "two" {
		t.Errorf("v2 delta = %+v, want parent 1 and the recorded change", got[2])
	}
	if got[3].ParentVersionID != 2 || len(got[3].Changes) != 1 || got[3].Changes[0].Op != types.ChangeOpDelete {
		t.Errorf("v3 delta = %+v, want parent 2 and the recorded delete", got[3])
	}
}

// A peer that cannot serve deltas must produce an ERROR, not an empty map: an
// empty range would tell the caller "that gap contains no changes", and it would
// then advance its cursor over history it never received.
func TestVersionChangesPuller_RefusalIsAnErrorNotAnEmptyRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, addr := startPushServer(t, 4) // no WithVersionChangesReader

	puller := NewVersionChangesPuller(PresenceCheckerConfig{})
	got, err := puller.ChangesInRange(ctx, addr, "kb-1", 1, 3)
	if err == nil {
		t.Fatalf("a peer with no changes reader must produce an error, got %v", got)
	}
	if got != nil {
		t.Errorf("the map must be nil on error, got %v", got)
	}
}

// An unreachable peer is an error too — so the caller falls back to full records
// rather than concluding there is nothing to fetch.
func TestVersionChangesPuller_UnreachablePeerIsAnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	puller := NewVersionChangesPuller(PresenceCheckerConfig{})
	if _, err := puller.ChangesInRange(ctx, "127.0.0.1:1", "kb-1", 1, 3); err == nil {
		t.Fatal("an unreachable peer must produce an error")
	}
}
