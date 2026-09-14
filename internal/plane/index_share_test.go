package plane

import (
	"context"
	"errors"
	"testing"
)

// stubIndexReader returns fixed index bytes.
type stubIndexReader struct {
	index   []byte
	sidecar []byte
	err     error
}

func (r *stubIndexReader) ReadIndexFiles(string, int64) ([]byte, []byte, error) {
	if r.err != nil {
		return nil, nil, r.err
	}
	return r.index, r.sidecar, nil
}

var _ IndexReader = (*stubIndexReader)(nil)

// stubIndexShipper records who an index was shipped to.
type stubIndexShipper struct {
	targets  []string
	versions []int64
	fail     map[string]bool
}

func (s *stubIndexShipper) PushIndex(_ context.Context, targetAddr, _ string, versionID int64, _, _ []byte) error {
	s.targets = append(s.targets, targetAddr)
	s.versions = append(s.versions, versionID)
	if s.fail[targetAddr] {
		return errors.New("replica unreachable")
	}
	return nil
}

var _ IndexShipper = (*stubIndexShipper)(nil)

func newIndexPlane(reader IndexReader, shipper IndexShipper, peers []string) *LocalDataPlane {
	tr := &tracer{}
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr},
		IndexReader:  reader,
		IndexShipper: shipper,
		ResolveReplicas: func(context.Context) ([]string, error) {
			return peers, nil
		},
	})
}

// "建一次、分发 N 份": the built index goes to every candidate replica.
func TestLocalDataPlane_PushIndexToReplicasCoversEveryCandidate(t *testing.T) {
	shipper := &stubIndexShipper{}
	dp := newIndexPlane(&stubIndexReader{index: []byte("idx"), sidecar: []byte("side")}, shipper,
		[]string{"peer-a", "peer-b", "peer-c"})

	if err := dp.PushIndexToReplicas(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("PushIndexToReplicas: %v", err)
	}
	if len(shipper.targets) != 3 {
		t.Fatalf("shipped to %v, want all three candidates", shipper.targets)
	}
	for i, v := range shipper.versions {
		if v != 7 {
			t.Errorf("ship %d carried version %d, want 7", i, v)
		}
	}
}

// A replica that cannot be reached is logged, not fatal: it falls back to
// building for itself, which is the behaviour the cluster had before
// distribution existed.
func TestLocalDataPlane_PushIndexToReplicasKeepsGoingOnPeerFailure(t *testing.T) {
	shipper := &stubIndexShipper{fail: map[string]bool{"peer-a": true}}
	dp := newIndexPlane(&stubIndexReader{index: []byte("idx"), sidecar: []byte("s")}, shipper,
		[]string{"peer-a", "peer-b"})

	err := dp.PushIndexToReplicas(context.Background(), "kb-1", 7)
	if err != nil {
		t.Fatalf("a per-replica failure must not fail the distribution: %v", err)
	}
	if len(shipper.targets) != 2 {
		t.Fatalf("shipped to %v, want both candidates attempted", shipper.targets)
	}
}

// Without the wiring the call is a no-op, not an error: a node that does not
// distribute is the single-node and test default.
func TestLocalDataPlane_PushIndexToReplicasWithoutWiring(t *testing.T) {
	dp := newIndexPlane(nil, nil, []string{"peer-a"})
	if err := dp.PushIndexToReplicas(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("want a no-op without the wiring, got %v", err)
	}
}

// A failure to read the local index is the builder's own problem and must be
// surfaced — there is nothing to ship, so pretending otherwise would hide a
// broken disk.
func TestLocalDataPlane_PushIndexToReplicasSurfacesReadFailure(t *testing.T) {
	shipper := &stubIndexShipper{}
	dp := newIndexPlane(&stubIndexReader{err: errors.New("no such file")}, shipper, []string{"peer-a"})

	if err := dp.PushIndexToReplicas(context.Background(), "kb-1", 7); err == nil {
		t.Fatal("want the read failure surfaced")
	}
	if len(shipper.targets) != 0 {
		t.Errorf("shipped to %v despite having nothing to ship", shipper.targets)
	}
}
