package plane

import (
	"context"
	"errors"
	"testing"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// TestReportIndexReady_CarriesTheReporter pins the identity half of the §8.6(d)
// addition: the report says WHO is serving, which is what makes a serving count
// possible at all — "READY" alone cannot be subtracted from.
func TestReportIndexReady_CarriesTheReporter(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta, WithNodeID(4))

	if err := cp.ReportIndexReady(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("ReportIndexReady: %v", err)
	}
	if len(meta.statusCalls) != 1 {
		t.Fatalf("status proposals = %+v, want exactly one", meta.statusCalls)
	}
	if got := meta.statusCalls[0].nodeID; got != 4 {
		t.Fatalf("reported nodeID = %d, want 4 (the node this plane speaks for)", got)
	}
}

// TestReportIndexReady_WithoutNodeIDRecordsNothing: an unwired node is the safe
// direction — the report then records no replica, which under-states the serving
// count, and under-stating can only make cleanup more cautious.
func TestReportIndexReady_WithoutNodeIDRecordsNothing(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta)

	if err := cp.ReportIndexReady(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("ReportIndexReady: %v", err)
	}
	if got := meta.statusCalls[0].nodeID; got != 0 {
		t.Fatalf("reported nodeID = %d, want 0 for an unwired plane", got)
	}
}

// TestIndexReadyReplicaCount_ExcludesTheAsker is the exact question §8.6(d)'s
// rolling cleanup asks before it takes a replica out of service.
func TestIndexReadyReplicaCount_ExcludesTheAsker(t *testing.T) {
	meta := &stubMeta{versions: map[string][]types.VersionMeta{
		"kb-1": {{VersionID: 7, IndexReadyNodes: []int64{1, 2, 3}}},
	}}
	cp := NewLocalControlPlane(meta)

	got, err := cp.IndexReadyReplicaCount(context.Background(), "kb-1", 7, 2)
	if err != nil {
		t.Fatalf("IndexReadyReplicaCount: %v", err)
	}
	if got != 2 {
		t.Fatalf("count = %d, want 2 (three are serving, one of them is the asker)", got)
	}

	// A node that is not in the set changes nothing.
	got, err = cp.IndexReadyReplicaCount(context.Background(), "kb-1", 7, 99)
	if err != nil {
		t.Fatalf("IndexReadyReplicaCount: %v", err)
	}
	if got != 3 {
		t.Fatalf("count = %d, want 3 when the asker is not among the servers", got)
	}
}

// TestIndexReadyReplicaCount_UnknownVersionIsAnError: "this version is gone" and
// "nobody is serving it" call for opposite actions — the first means stop, the
// second means wait — so the caller has to be able to tell them apart.
func TestIndexReadyReplicaCount_UnknownVersionIsAnError(t *testing.T) {
	meta := &stubMeta{versions: map[string][]types.VersionMeta{
		"kb-1": {{VersionID: 7}},
	}}
	cp := NewLocalControlPlane(meta)

	_, err := cp.IndexReadyReplicaCount(context.Background(), "kb-1", 8, 1)
	if !errors.Is(err, stratumerrors.ErrVersionNotFound) {
		t.Fatalf("error = %v, want ErrVersionNotFound", err)
	}
}
