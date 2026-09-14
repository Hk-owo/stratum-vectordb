package plane

import (
	"context"
	"errors"
	"testing"
)

// stubQuerier answers fixed cursors per peer and can mark peers unreachable.
type stubQuerier struct {
	cursors map[string]int64
	down    map[string]bool
}

func (q *stubQuerier) LocalVersionOf(_ context.Context, peerAddr, _ string) (int64, error) {
	if q.down[peerAddr] {
		return 0, errors.New("peer unreachable")
	}
	return q.cursors[peerAddr], nil
}

var _ CursorQuerier = (*stubQuerier)(nil)

// sourceRecordingPuller records which source each version was pulled from.
type sourceRecordingPuller struct {
	sources  []string
	versions []int64
}

// PullVersionData satisfies VersionPuller; the data-only flavour records the
// same way, since these tests care about which source was chosen.
func (p *sourceRecordingPuller) PullVersionData(ctx context.Context, sourceAddr, kbID string, versionID int64) error {
	return p.PullVersion(ctx, sourceAddr, kbID, versionID)
}

func (p *sourceRecordingPuller) PullVersion(_ context.Context, sourceAddr, _ string, versionID int64) error {
	p.sources = append(p.sources, sourceAddr)
	p.versions = append(p.versions, versionID)
	return nil
}

var _ VersionPuller = (*sourceRecordingPuller)(nil)

func newSourcePlane(puller VersionPuller, replicas []string, querier CursorQuerier) *LocalDataPlane {
	tr := &tracer{}
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
		Puller:       puller,
		Verify:       func(context.Context, string, int64) bool { return true },
		Resolve:      func(context.Context, string, int64) (string, bool, error) { return "leader", true, nil },
		ResolveReplicas: func(context.Context) ([]string, error) {
			return replicas, nil
		},
		CursorQuerier: querier,
	})
}

// A backfill must pull from a peer that actually holds the gap — the whole
// point of exchanging cursors (Stratum_设计文档v13.md §7.6).
func TestLocalDataPlane_BackfillPicksAPeerHoldingTheGap(t *testing.T) {
	puller := &sourceRecordingPuller{}
	dp := newSourcePlane(puller,
		[]string{"peer-a", "peer-b"},
		&stubQuerier{cursors: map[string]int64{"peer-a": 2, "peer-b": 9}},
	)
	dp.advanceLocalVersion("kb-1", 3) // gap is 4..5, so the source needs >= 5

	if err := dp.EnsureIndex(context.Background(), "kb-1", 6); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if len(puller.versions) != 3 || puller.versions[0] != 4 || puller.versions[1] != 5 || puller.versions[2] != 6 {
		t.Fatalf("pulled versions = %v, want the gap (4, 5) then the target (6)", puller.versions)
	}
	// The gap must come from the peer whose cursor reaches it...
	for i, source := range puller.sources[:2] {
		if source != "peer-b" {
			t.Fatalf("gap pull %d came from %q, want peer-b (the only peer whose cursor reaches it)", i, source)
		}
	}
	// ...while the target version itself is pulled from the resolver's source,
	// which by definition holds the newest history.
	if puller.sources[2] != "leader" {
		t.Errorf("target pull came from %q, want the resolver's source", puller.sources[2])
	}
}

// When no peer holds the gap the node still tries its default source rather
// than failing outright: best effort beats refusing to fetch anything.
func TestLocalDataPlane_BackfillFallsBackWhenNoPeerHoldsTheGap(t *testing.T) {
	puller := &sourceRecordingPuller{}
	dp := newSourcePlane(puller,
		[]string{"peer-a", "peer-b"},
		&stubQuerier{cursors: map[string]int64{"peer-a": 1, "peer-b": 2}},
	)
	dp.advanceLocalVersion("kb-1", 3)

	if err := dp.EnsureIndex(context.Background(), "kb-1", 6); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	for i, source := range puller.sources {
		if source != "leader" {
			t.Fatalf("pull %d came from %q, want the default source when no peer has the gap", i, source)
		}
	}
}

// Without a cursor querier (and hence without cursor knowledge) the source
// stays the resolver's answer.
func TestLocalDataPlane_BackfillWithoutQuerierUsesDefaultSource(t *testing.T) {
	puller := &sourceRecordingPuller{}
	dp := newSourcePlane(puller, []string{"peer-a"}, nil)
	dp.advanceLocalVersion("kb-1", 3)

	if err := dp.EnsureIndex(context.Background(), "kb-1", 5); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	for i, source := range puller.sources {
		if source != "leader" {
			t.Fatalf("pull %d came from %q, want leader", i, source)
		}
	}
}

// An unreachable peer is skipped, not treated as holding nothing: its silence
// says nothing about the peers that do answer.
func TestLocalDataPlane_BackfillSkipsUnreachablePeers(t *testing.T) {
	puller := &sourceRecordingPuller{}
	dp := newSourcePlane(puller,
		[]string{"peer-a", "peer-b"},
		&stubQuerier{
			down:    map[string]bool{"peer-a": true},
			cursors: map[string]int64{"peer-b": 7},
		},
	)
	dp.advanceLocalVersion("kb-1", 3)

	if err := dp.EnsureIndex(context.Background(), "kb-1", 5); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if len(puller.versions) != 2 || puller.versions[0] != 4 || puller.versions[1] != 5 {
		t.Fatalf("pulled versions = %v, want the gap (4) then the target (5)", puller.versions)
	}
	if puller.sources[0] != "peer-b" {
		t.Errorf("gap pull came from %q, want peer-b (peer-a is unreachable)", puller.sources[0])
	}
}
