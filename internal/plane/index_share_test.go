package plane

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	stratuminternalsync "stratum/internal/sync"
)

// stubIndexReader returns fixed index bytes, and counts the calls: the §8.4(a)
// pre-flight's whole point is that a fully-skipped distribution never reads the
// file, so the count is the evidence for it.
type stubIndexReader struct {
	index   []byte
	sidecar []byte
	err     error

	mu    sync.Mutex
	reads int
}

func (r *stubIndexReader) ReadIndexFiles(string, int64) ([]byte, []byte, error) {
	r.mu.Lock()
	r.reads++
	r.mu.Unlock()
	if r.err != nil {
		return nil, nil, r.err
	}
	return r.index, r.sidecar, nil
}

func (r *stubIndexReader) readCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
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
	// holds names the peers whose §8.4(a) probe answers "already held";
	// probeErr names the ones whose probe fails; alreadyPresent names the ones
	// whose SHIP answers "already held". probes records every target probed, so
	// a test can tell a skip apart from a peer never asked.
	holds          map[string]bool
	probeErr       map[string]bool
	alreadyPresent map[string]bool
	probes         []string
	// block, when non-nil, is received from inside PushIndex — the hook that
	// holds a distribution open long enough for the ceiling to be observed.
	block chan struct{}
	// inflight counts calls currently inside PushIndex; peak is their high-water
	// mark, which is what the concurrency test asserts on.
	inflight int
	peak     int
}

func (s *stubIndexShipper) ProbeIndex(_ context.Context, targetAddr, _ string, _ int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probes = append(s.probes, targetAddr)
	if s.probeErr[targetAddr] {
		return false, errors.New("probe unreachable")
	}
	return s.holds[targetAddr], nil
}

func (s *stubIndexShipper) probedTargets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.probes...)
}

func (s *stubIndexShipper) shippedTargets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.targets...)
}

func (s *stubIndexShipper) PushIndex(_ context.Context, targetAddr, _ string, versionID int64, _, _ []byte) error {
	s.mu.Lock()
	s.inflight++
	if s.inflight > s.peak {
		s.peak = s.inflight
	}
	s.targets = append(s.targets, targetAddr)
	s.versions = append(s.versions, versionID)
	block, fail, already := s.block, s.fail[targetAddr], s.alreadyPresent[targetAddr]
	s.mu.Unlock()

	if block != nil {
		<-block
	}

	s.mu.Lock()
	s.inflight--
	s.mu.Unlock()

	switch {
	case already:
		// The ship opens with the same probe the pre-flight sent, so a peer can
		// answer "already held" here too. It is a skip, not a failure.
		return stratuminternalsync.ErrIndexAlreadyPresent
	case fail:
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
	return newIndexPlaneFor(reader, shipper, peers, limit, nil)
}

// newIndexPlaneWithLogger is newIndexPlane with an observer attached: the
// §8.4(a) skip is only worth having if it is reported, and reporting is what
// these tests assert on.
func newIndexPlaneWithLogger(reader IndexReader, shipper IndexShipper, peers []string, logger *zap.Logger) *LocalDataPlane {
	return newIndexPlaneFor(reader, shipper, peers, 0, logger)
}

func newIndexPlaneFor(reader IndexReader, shipper IndexShipper, peers []string, limit int, logger *zap.Logger) *LocalDataPlane {
	tr := &tracer{}
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:           &stubIndexStore{},
		WAL:                    &stubWAL{t: tr},
		Executor:               &stubExecutor{t: tr},
		IndexReader:            reader,
		IndexShipper:           shipper,
		MaxConcurrentIndexPush: limit,
		Logger:                 logger,
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

// §8.4(a): a peer that already holds the artifact is dropped at the probe, and
// the peers that do NOT hold it are still served — the pre-flight narrows the
// fan-out, it does not replace it.
func TestLocalDataPlane_PushIndexToReplicasSkipsPeersThatHoldTheArtifact(t *testing.T) {
	shipper := &stubIndexShipper{holds: map[string]bool{"peer-a": true}}
	reader := &stubIndexReader{index: []byte("idx"), sidecar: []byte("side")}
	dp := newIndexPlane(reader, shipper, []string{"peer-a", "peer-b", "peer-c"})

	if err := dp.PushIndexToReplicas(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("PushIndexToReplicas: %v", err)
	}
	if got := shipper.probedTargets(); !reflect.DeepEqual(got, []string{"peer-a", "peer-b", "peer-c"}) {
		t.Errorf("probed %v, want every candidate", got)
	}
	if got := shipper.shippedTargets(); !reflect.DeepEqual(got, []string{"peer-b", "peer-c"}) {
		t.Errorf("shipped to %v, want only the peers that did not hold it", got)
	}
	if got := reader.readCount(); got != 1 {
		t.Errorf("read the index %d times, want 1: something still had to be shipped", got)
	}
}

// Every candidate already holds it: neither the read nor the fan-out may
// happen, because that is where the memory peak and the outbound bytes were.
func TestLocalDataPlane_PushIndexToReplicasReadsNothingWhenEveryPeerHoldsIt(t *testing.T) {
	shipper := &stubIndexShipper{holds: map[string]bool{"peer-a": true, "peer-b": true}}
	reader := &stubIndexReader{index: []byte("idx"), sidecar: []byte("side")}
	dp := newIndexPlane(reader, shipper, []string{"peer-a", "peer-b"})

	if err := dp.PushIndexToReplicas(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("PushIndexToReplicas: %v", err)
	}
	if got := reader.readCount(); got != 0 {
		t.Errorf("read the index %d times, want 0: every peer already had it", got)
	}
	if got := shipper.shippedTargets(); len(got) != 0 {
		t.Errorf("shipped to %v, want nobody", got)
	}
	if got := shipper.peakSeen(); got != 0 {
		t.Errorf("opened %d distributions, want 0", got)
	}
}

// A probe that cannot be answered must NOT become a skip: not knowing is not
// the same as knowing the peer has it, and guessing that way would leave the
// replica to build for itself while the log called it an optimisation.
func TestLocalDataPlane_PushIndexToReplicasShipsWhenTheProbeFails(t *testing.T) {
	logger, logs := testIndexLogger()
	shipper := &stubIndexShipper{probeErr: map[string]bool{"peer-a": true}}
	dp := newIndexPlaneWithLogger(
		&stubIndexReader{index: []byte("idx"), sidecar: []byte("side")}, shipper,
		[]string{"peer-a"}, logger)

	if err := dp.PushIndexToReplicas(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("PushIndexToReplicas: %v", err)
	}
	if got := shipper.shippedTargets(); !reflect.DeepEqual(got, []string{"peer-a"}) {
		t.Errorf("shipped to %v, want the peer whose probe failed", got)
	}
	if got := countMessages(logs, "plane: index presence probe failed; shipping anyway"); got != 1 {
		t.Errorf("saw %d probe-failure reports, want 1", got)
	}
}

// The skip is invisible without the report, and a skip is not a failure: the
// log has to say how many replicas were spared and warn about none of them.
func TestLocalDataPlane_PushIndexToReplicasReportsSkipsAsSkips(t *testing.T) {
	logger, logs := testIndexLogger()
	shipper := &stubIndexShipper{holds: map[string]bool{"peer-a": true}}
	dp := newIndexPlaneWithLogger(
		&stubIndexReader{index: []byte("idx"), sidecar: []byte("side")}, shipper,
		[]string{"peer-a", "peer-b"}, logger)

	if err := dp.PushIndexToReplicas(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("PushIndexToReplicas: %v", err)
	}
	for _, entry := range logs.All() {
		if entry.Level >= zapcore.WarnLevel {
			t.Errorf("%s warned: %s", entry.Level, entry.Message)
		}
	}
	entry, ok := findMessage(logs, "plane: index distribution skipped replicas that already hold the artifact")
	if !ok {
		t.Fatal("no skip report; the optimisation is invisible without it")
	}
	if got := entry.ContextMap()["skipped"]; got != int64(1) {
		t.Errorf("skipped = %v, want 1", got)
	}
}

// One skip, one failure, one success: each path is counted where it belongs,
// rather than collapsing into "the distribution had problems".
func TestLocalDataPlane_PushIndexToReplicasSeparatesSkipFromFailure(t *testing.T) {
	logger, logs := testIndexLogger()
	shipper := &stubIndexShipper{
		holds: map[string]bool{"peer-a": true},
		fail:  map[string]bool{"peer-b": true},
	}
	dp := newIndexPlaneWithLogger(
		&stubIndexReader{index: []byte("idx"), sidecar: []byte("side")}, shipper,
		[]string{"peer-a", "peer-b", "peer-c"}, logger)

	if err := dp.PushIndexToReplicas(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("a per-replica failure must not fail the distribution: %v", err)
	}
	if got := shipper.shippedTargets(); !reflect.DeepEqual(got, []string{"peer-b", "peer-c"}) {
		t.Errorf("shipped to %v, want the two that had to be served", got)
	}
	if got := countMessages(logs, "plane: index distribution failed; that replica will build its own"); got != 1 {
		t.Errorf("saw %d failure warnings, want 1 (peer-b)", got)
	}
	if got := countMessages(logs, "plane: index distribution skipped replicas that already hold the artifact"); got != 1 {
		t.Errorf("saw %d skip reports, want 1", got)
	}
}

// A peer can also answer "already held" at the SHIP's own first frame, which is
// the same probe arriving in between the pre-flight and the transfer. That
// answer is the same skip, not a failure.
func TestLocalDataPlane_PushIndexToReplicasCountsAnInStreamSkipAsASkip(t *testing.T) {
	logger, logs := testIndexLogger()
	shipper := &stubIndexShipper{alreadyPresent: map[string]bool{"peer-a": true}}
	dp := newIndexPlaneWithLogger(
		&stubIndexReader{index: []byte("idx"), sidecar: []byte("side")}, shipper,
		[]string{"peer-a", "peer-b"}, logger)

	if err := dp.PushIndexToReplicas(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("PushIndexToReplicas: %v", err)
	}
	if got := countMessages(logs, "plane: index distribution failed; that replica will build its own"); got != 0 {
		t.Errorf("saw %d failure warnings for an in-stream skip, want 0", got)
	}
	entry, ok := findMessage(logs, "plane: index distribution skipped replicas that already hold the artifact")
	if !ok {
		t.Fatal("no skip report for an in-stream skip")
	}
	if got := entry.ContextMap()["skipped"]; got != int64(1) {
		t.Errorf("skipped = %v, want 1", got)
	}
}

// testIndexLogger returns a logger that records what was written, so the
// §8.4(a) report can be asserted on instead of merely hoped for.
func testIndexLogger() (*zap.Logger, *observer.ObservedLogs) {
	core, logs := observer.New(zap.DebugLevel)
	return zap.New(core), logs
}

func countMessages(logs *observer.ObservedLogs, message string) int {
	n := 0
	for _, entry := range logs.All() {
		if entry.Message == message {
			n++
		}
	}
	return n
}

func findMessage(logs *observer.ObservedLogs, message string) (observer.LoggedEntry, bool) {
	for _, entry := range logs.All() {
		if entry.Message == message {
			return entry, true
		}
	}
	return observer.LoggedEntry{}, false
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
