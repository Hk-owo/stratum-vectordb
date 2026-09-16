package plane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
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

// stubIndexShipper records who an index was shipped to. The mutex is not
// decoration: the concurrency test below drives PushIndex from several
// goroutines at once.
type stubIndexShipper struct {
	mu       sync.Mutex
	targets  []string
	versions []int64
	fail     map[string]bool
	// block, when non-nil, is received from inside PushIndex — the hook that
	// holds a distribution open long enough for the ceiling to be observed.
	block chan struct{}
	// inflight counts calls currently inside PushIndex; peak is their high-water
	// mark, which is what the concurrency test asserts on.
	inflight int
	peak     int
}

func (s *stubIndexShipper) PushIndex(_ context.Context, targetAddr, _ string, versionID int64, _, _ []byte) error {
	s.mu.Lock()
	s.inflight++
	if s.inflight > s.peak {
		s.peak = s.inflight
	}
	s.targets = append(s.targets, targetAddr)
	s.versions = append(s.versions, versionID)
	block, fail := s.block, s.fail[targetAddr]
	s.mu.Unlock()

	if block != nil {
		<-block
	}

	s.mu.Lock()
	s.inflight--
	s.mu.Unlock()

	if fail {
		return errors.New("replica unreachable")
	}
	return nil
}

func (s *stubIndexShipper) inflightCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight
}

func (s *stubIndexShipper) peakSeen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

var _ IndexShipper = (*stubIndexShipper)(nil)

func newIndexPlane(reader IndexReader, shipper IndexShipper, peers []string) *LocalDataPlane {
	return newIndexPlaneWithPushLimit(reader, shipper, peers, 0)
}

// newIndexPlaneWithPushLimit is newIndexPlane with an explicit distribution
// ceiling, so the concurrency tests do not have to reason about the default.
// A non-positive limit takes the default, exactly as production does.
func newIndexPlaneWithPushLimit(reader IndexReader, shipper IndexShipper, peers []string, limit int) *LocalDataPlane {
	tr := &tracer{}
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:           &stubIndexStore{},
		WAL:                    &stubWAL{t: tr},
		Executor:               &stubExecutor{t: tr},
		IndexReader:            reader,
		IndexShipper:           shipper,
		MaxConcurrentIndexPush: limit,
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

// §8.4(a): distribution is bounded. Without the gate, a batch of version switches
// — or a set of replicas catching up at once — would open one whole-file read
// (plus N outbound streams) per build at the same moment.
func TestLocalDataPlane_PushIndexToReplicasRespectsConcurrencyLimit(t *testing.T) {
	const limit = 2
	const callers = limit + 3

	block := make(chan struct{})
	shipper := &stubIndexShipper{block: block}
	dp := newIndexPlaneWithPushLimit(
		&stubIndexReader{index: []byte("idx"), sidecar: []byte("side")}, shipper,
		[]string{"peer-a"}, limit)

	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- dp.PushIndexToReplicas(context.Background(), "kb-1", 7)
		}()
	}

	// Hold every slot open before releasing: the assertion is about a real
	// overlap, not about how fast the goroutines happened to be scheduled.
	waitForInflight(t, shipper, limit)
	close(block)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("PushIndexToReplicas: %v", err)
		}
	}
	if got := shipper.peakSeen(); got > limit {
		t.Errorf("peak concurrent distributions = %d, want <= %d", got, limit)
	}
}

// A distribution queued behind a full gate must still return when its caller
// gives up. The callers are background goroutines, and one that could not be
// cancelled would sit here for as long as the storm lasts.
func TestLocalDataPlane_PushIndexToReplicasHonoursContextWhileQueued(t *testing.T) {
	const limit = 1

	block := make(chan struct{})
	shipper := &stubIndexShipper{block: block}
	dp := newIndexPlaneWithPushLimit(
		&stubIndexReader{index: []byte("idx"), sidecar: []byte("side")}, shipper,
		[]string{"peer-a"}, limit)

	// Occupy the only slot.
	holding := make(chan struct{})
	go func() {
		defer close(holding)
		_ = dp.PushIndexToReplicas(context.Background(), "kb-1", 7)
	}()
	waitForInflight(t, shipper, limit)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := dp.PushIndexToReplicas(ctx, "kb-1", 7); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled while queued, got %v", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("the queued call waited %v before honouring the cancelled context", waited)
	}

	close(block)
	<-holding
}

// waitForInflight blocks until want distributions are inside the stub's
// PushIndex, so a test can release them all at once.
func waitForInflight(t *testing.T, s *stubIndexShipper, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := s.inflightCount(); got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d distributions reached PushIndex, want %d", s.inflightCount(), want)
		}
		time.Sleep(time.Millisecond)
	}
}
