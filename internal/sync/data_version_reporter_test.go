package sync

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"

	pb "stratum/api/proto/stratum"
)

// recordingRecorder stands in for the leader-side aggregate (§7.13.4).
type recordingRecorder struct {
	nodes   []int64
	reports []map[string]int64
}

func (r *recordingRecorder) Record(nodeID int64, address string, dataVersions map[string]int64) {
	r.nodes = append(r.nodes, nodeID)
	r.reports = append(r.reports, dataVersions)
}

// KnowledgeBases is the union across reports, which is exactly what the leader
// answers from — and the reason a node that named nothing still hears about its
// chains.
func (r *recordingRecorder) KnowledgeBases() []string {
	seen := make(map[string]struct{})
	for _, report := range r.reports {
		for kbID := range report {
			seen[kbID] = struct{}{}
		}
	}
	kbs := make([]string, 0, len(seen))
	for kbID := range seen {
		kbs = append(kbs, kbID)
	}
	sort.Strings(kbs)
	return kbs
}

// The leader folds the report into its aggregate, keyed by the reporting node.
func TestPushHandler_ReportDataVersions_LeaderRecordsTheReport(t *testing.T) {
	rec := &recordingRecorder{}
	_, _, addr := startPushServer(t, 7, WithDataVersionAggregator(rec, func() bool { return true }))

	resp, err := dialSyncServer(t, addr).ReportDataVersions(context.Background(), &pb.ReportDataVersionsRequest{
		NodeId:       3,
		DataVersions: map[string]int64{"kb-1": 4, "kb-2": 9},
	})
	if err != nil {
		t.Fatalf("ReportDataVersions: %v", err)
	}
	if !resp.GetAccepted() {
		t.Error("a leader must accept the report")
	}
	if resp.GetNodeId() != 7 {
		t.Errorf("responder node_id = %d, want 7", resp.GetNodeId())
	}
	if len(rec.nodes) != 1 || rec.nodes[0] != 3 {
		t.Errorf("recorded nodes = %v, want [3] (the reporter's own id)", rec.nodes)
	}
	if got := rec.reports[0]; got["kb-1"] != 4 || got["kb-2"] != 9 {
		t.Errorf("recorded cursors = %v, want kb-1:4 kb-2:9", got)
	}
}

// A non-leader declines without an error: "you reached the wrong node" is not a
// transport failure, and the reporter's next interval re-resolves the leader.
func TestPushHandler_ReportDataVersions_NonLeaderDeclinesWithoutError(t *testing.T) {
	rec := &recordingRecorder{}
	_, _, addr := startPushServer(t, 7, WithDataVersionAggregator(rec, func() bool { return false }))

	resp, err := dialSyncServer(t, addr).ReportDataVersions(context.Background(), &pb.ReportDataVersionsRequest{
		NodeId:       3,
		DataVersions: map[string]int64{"kb-1": 4},
	})
	if err != nil {
		t.Fatalf("a decline must not be an RPC error: %v", err)
	}
	if resp.GetAccepted() {
		t.Error("a non-leader must not report the write as accepted")
	}
	if resp.GetNodeId() != 7 {
		t.Errorf("responder node_id = %d, want 7 (so the reporter can tell a moved leader from an intermediary)", resp.GetNodeId())
	}
	if len(rec.nodes) != 0 {
		t.Errorf("a non-leader recorded %v; only the leader may keep the aggregate", rec.nodes)
	}
}

// With no aggregate wired the node still answers, and answers "not me" — a
// follower-only deployment must not look like a leader that dropped the report.
func TestPushHandler_ReportDataVersions_UnwiredDeclines(t *testing.T) {
	_, _, addr := startPushServer(t, 7)

	resp, err := dialSyncServer(t, addr).ReportDataVersions(context.Background(), &pb.ReportDataVersionsRequest{
		NodeId:       3,
		DataVersions: map[string]int64{"kb-1": 4},
	})
	if err != nil {
		t.Fatalf("ReportDataVersions: %v", err)
	}
	if resp.GetAccepted() {
		t.Error("a node with no aggregate must not claim to have accepted")
	}
}

// reporterConfig builds a reporter aimed at addr via a fixed leader resolver.
func reporterConfig(addr string, cursors map[string]int64) DataVersionReporterConfig {
	return DataVersionReporterConfig{
		NodeID:       3,
		DataVersions: func() map[string]int64 { return cursors },
		ResolveLeader: func(context.Context) (string, bool, error) {
			return addr, true, nil
		},
	}
}

// A whole report round-trips and lands in the aggregate.
func TestDataVersionReporter_ReportLandsOnTheLeader(t *testing.T) {
	rec := &recordingRecorder{}
	_, _, addr := startPushServer(t, 7, WithDataVersionAggregator(rec, func() bool { return true }))

	if err := NewDataVersionReporter(reporterConfig(addr, map[string]int64{"kb-1": 5})).ReportOnce(context.Background()); err != nil {
		t.Fatalf("ReportOnce: %v", err)
	}
	if len(rec.nodes) != 1 || rec.nodes[0] != 3 {
		t.Fatalf("recorded nodes = %v, want [3]", rec.nodes)
	}
	if got := rec.reports[0]["kb-1"]; got != 5 {
		t.Errorf("recorded cursor = %d, want 5", got)
	}
}

// An empty view is still sent: it is how a node that lost its data says so. If it
// were suppressed, the leader would keep describing versions this node no longer
// has.
func TestDataVersionReporter_EmptyViewIsStillReported(t *testing.T) {
	rec := &recordingRecorder{}
	_, _, addr := startPushServer(t, 7, WithDataVersionAggregator(rec, func() bool { return true }))

	if err := NewDataVersionReporter(reporterConfig(addr, map[string]int64{})).ReportOnce(context.Background()); err != nil {
		t.Fatalf("ReportOnce: %v", err)
	}
	if len(rec.reports) != 1 {
		t.Fatalf("reports = %d, want 1 (an empty view is a statement, not silence)", len(rec.reports))
	}
	if len(rec.reports[0]) != 0 {
		t.Errorf("reported cursors = %v, want empty", rec.reports[0])
	}
}

// A decline from a non-leader means the report did not land, so the reporter must
// treat it as a failure and try again — but nothing may be recorded.
func TestDataVersionReporter_DeclineIsNotALandedReport(t *testing.T) {
	rec := &recordingRecorder{}
	_, _, addr := startPushServer(t, 7, WithDataVersionAggregator(rec, func() bool { return false }))

	err := NewDataVersionReporter(reporterConfig(addr, map[string]int64{"kb-1": 5})).ReportOnce(context.Background())
	if err == nil {
		t.Error("a non-leader's answer must not count as a landed report")
	}
	if len(rec.nodes) != 0 {
		t.Errorf("a non-leader recorded %v", rec.nodes)
	}
}

// A leader election is an ordinary state: with no leader known the interval is
// simply skipped, and the caller is not told something failed.
func TestDataVersionReporter_NoLeaderSkipsQuietly(t *testing.T) {
	r := NewDataVersionReporter(DataVersionReporterConfig{
		NodeID:       3,
		DataVersions: func() map[string]int64 { return map[string]int64{"kb-1": 5} },
		ResolveLeader: func(context.Context) (string, bool, error) {
			return "", false, nil
		},
	})
	if err := r.ReportOnce(context.Background()); err != nil {
		t.Errorf("ReportOnce during an election = %v, want nil (nothing to report to, nothing broken)", err)
	}
}

// A resolver that cannot answer is a failure to retry, not a silent no-op — the
// two cases lead to different operator conclusions.
func TestDataVersionReporter_ResolverFailureIsRetryable(t *testing.T) {
	r := NewDataVersionReporter(DataVersionReporterConfig{
		NodeID:       3,
		DataVersions: func() map[string]int64 { return map[string]int64{"kb-1": 5} },
		ResolveLeader: func(context.Context) (string, bool, error) {
			return "", false, errors.New("cluster status unavailable")
		},
	})
	if err := r.ReportOnce(context.Background()); err == nil {
		t.Error("a resolver error must surface as a retryable failure")
	}
}

// fixedChainTails is a ChainTailSource under a test's control.
type fixedChainTails struct {
	mu         sync.Mutex
	term, seq  uint64
	noSignal   bool
	tails      map[string]int64
	err        error
	tailsCalls int
}

func (s *fixedChainTails) ChainGeneration() (uint64, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.seq, !s.noSignal
}

func (s *fixedChainTails) ChainTails(context.Context) (map[string]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tailsCalls++
	return s.tails, s.err
}

func (s *fixedChainTails) set(mutate func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mutate()
}

// recordingChainTails captures what reached the sink.
type recordingChainTails struct {
	calls   int
	lastOne map[string]int64
}

func (s *recordingChainTails) SetChainTails(tails map[string]int64) {
	s.calls++
	s.lastOne = tails
}

func chainTailReporter(addr string, sink ChainTailSink) *DataVersionReporter {
	return NewDataVersionReporter(DataVersionReporterConfig{
		NodeID:       3,
		DataVersions: func() map[string]int64 { return map[string]int64{"kb-1": 40} },
		ResolveLeader: func(context.Context) (string, bool, error) {
			return addr, true, nil
		},
		ChainTails: sink,
	})
}

// The heartbeat response carries the generation and never the tails, so it stays the same
// size however many knowledge bases there are.
func TestPushHandler_ReportDataVersions_CarriesTheGenerationNotTheTails(t *testing.T) {
	src := &fixedChainTails{term: 2, seq: 9, tails: map[string]int64{"kb-1": 42}}
	_, _, addr := startPushServer(t, 7,
		WithDataVersionAggregator(&recordingRecorder{}, func() bool { return true }),
		WithChainTails(src))

	resp, err := dialSyncServer(t, addr).ReportDataVersions(context.Background(), &pb.ReportDataVersionsRequest{NodeId: 3})
	if err != nil {
		t.Fatalf("ReportDataVersions: %v", err)
	}
	if g := resp.GetChainGeneration(); g.GetTerm() != 2 || g.GetSeq() != 9 {
		t.Errorf("generation = %v, want term 2 seq 9", g)
	}
	if len(resp.GetChainTails()) != 0 {
		t.Errorf("the heartbeat carried tails: %v", resp.GetChainTails())
	}
	if src.tailsCalls != 0 {
		t.Errorf("the heartbeat read the tails %d times, want 0", src.tailsCalls)
	}

	// A leader with no tail source offers no signal.
	_, _, addr2 := startPushServer(t, 8, WithDataVersionAggregator(&recordingRecorder{}, func() bool { return true }))
	resp2, err := dialSyncServer(t, addr2).ReportDataVersions(context.Background(), &pb.ReportDataVersionsRequest{NodeId: 3})
	if err != nil {
		t.Fatalf("ReportDataVersions (unwired): %v", err)
	}
	if resp2.GetChainGeneration() != nil {
		t.Errorf("an unwired leader sent a generation: %v", resp2.GetChainGeneration())
	}
}

func TestPushHandler_GetChainTails(t *testing.T) {
	src := &fixedChainTails{term: 2, seq: 9, tails: map[string]int64{"kb-1": 42}}
	_, _, addr := startPushServer(t, 7,
		WithDataVersionAggregator(&recordingRecorder{}, func() bool { return true }),
		WithChainTails(src))

	resp, err := dialSyncServer(t, addr).GetChainTails(context.Background(), &pb.GetChainTailsRequest{})
	if err != nil {
		t.Fatalf("GetChainTails: %v", err)
	}
	if !resp.GetAccepted() || resp.GetChainTails()["kb-1"] != 42 || resp.GetGeneration().GetSeq() != 9 {
		t.Errorf("resp = %v, want the leader's tails and generation", resp)
	}

	// A follower's metadata can lag, so it does not answer.
	_, _, follower := startPushServer(t, 8,
		WithDataVersionAggregator(&recordingRecorder{}, func() bool { return false }),
		WithChainTails(src))
	resp, err = dialSyncServer(t, follower).GetChainTails(context.Background(), &pb.GetChainTailsRequest{})
	if err != nil {
		t.Fatalf("GetChainTails (follower): %v", err)
	}
	if resp.GetAccepted() || len(resp.GetChainTails()) != 0 {
		t.Errorf("resp = %v, want a decline", resp)
	}

	// An unreadable list is a failure to retry, not an empty answer.
	src.set(func() { src.err = errors.New("no quorum") })
	if _, err := dialSyncServer(t, addr).GetChainTails(context.Background(), &pb.GetChainTailsRequest{}); err == nil {
		t.Error("want the read failure")
	}
}

// Tails are pulled only when the generation moves, while the sink hears them on every
// heartbeat: it retries what it skipped on the next signal, so the signal has to keep
// coming whether or not anything changed.
func TestDataVersionReporter_PullsTailsOnlyWhenTheGenerationMoves(t *testing.T) {
	src := &fixedChainTails{term: 1, seq: 1, tails: map[string]int64{"kb-1": 42}}
	_, _, addr := startPushServer(t, 7,
		WithDataVersionAggregator(&recordingRecorder{}, func() bool { return true }),
		WithChainTails(src))
	sink := &recordingChainTails{}
	r := chainTailReporter(addr, sink)

	report := func() {
		t.Helper()
		if err := r.ReportOnce(context.Background()); err != nil {
			t.Fatalf("ReportOnce: %v", err)
		}
	}

	report()
	report()
	report()
	if src.tailsCalls != 1 {
		t.Errorf("pulled %d times over three unchanged heartbeats, want 1", src.tailsCalls)
	}
	if sink.calls != 3 || sink.lastOne["kb-1"] != 42 {
		t.Errorf("sink saw %d calls ending in %v, want 3 ending in kb-1:42", sink.calls, sink.lastOne)
	}

	src.set(func() { src.seq, src.tails = 2, map[string]int64{"kb-1": 43} })
	report()
	if src.tailsCalls != 2 || sink.lastOne["kb-1"] != 43 {
		t.Errorf("after a tail change: %d pulls, sink has %v; want 2 pulls and kb-1:43", src.tailsCalls, sink.lastOne)
	}

	// A new leader's count may land on the same number. The term is what tells them apart.
	src.set(func() { src.term, src.tails = 2, map[string]int64{"kb-1": 44} })
	report()
	if src.tailsCalls != 3 || sink.lastOne["kb-1"] != 44 {
		t.Errorf("after a new term: %d pulls, sink has %v; want 3 pulls and kb-1:44", src.tailsCalls, sink.lastOne)
	}
}

// A failed pull keeps the old generation, so the next heartbeat tries again and the node
// cannot miss the change. Until the first pull lands the sink hears nothing: "not told"
// is not "no chains".
func TestDataVersionReporter_FailedPullIsRetried(t *testing.T) {
	src := &fixedChainTails{term: 1, seq: 1, err: errors.New("no quorum")}
	_, _, addr := startPushServer(t, 7,
		WithDataVersionAggregator(&recordingRecorder{}, func() bool { return true }),
		WithChainTails(src))
	sink := &recordingChainTails{}
	r := chainTailReporter(addr, sink)

	if err := r.ReportOnce(context.Background()); err != nil {
		t.Fatalf("ReportOnce: a failed pull does not mean the heartbeat failed: %v", err)
	}
	if sink.calls != 0 {
		t.Errorf("sink calls = %d, want 0 before anything was pulled", sink.calls)
	}

	src.set(func() { src.err, src.tails = nil, map[string]int64{"kb-1": 42} })
	if err := r.ReportOnce(context.Background()); err != nil {
		t.Fatalf("ReportOnce: %v", err)
	}
	if src.tailsCalls != 2 || sink.lastOne["kb-1"] != 42 {
		t.Errorf("%d pulls, sink has %v; want a retry that delivered kb-1:42", src.tailsCalls, sink.lastOne)
	}
}

// A leader offering no signal triggers no pull.
func TestDataVersionReporter_NoGenerationNoPull(t *testing.T) {
	src := &fixedChainTails{noSignal: true, tails: map[string]int64{"kb-1": 42}}
	_, _, addr := startPushServer(t, 7,
		WithDataVersionAggregator(&recordingRecorder{}, func() bool { return true }),
		WithChainTails(src))
	sink := &recordingChainTails{}

	if err := chainTailReporter(addr, sink).ReportOnce(context.Background()); err != nil {
		t.Fatalf("ReportOnce: %v", err)
	}
	if src.tailsCalls != 0 || sink.calls != 0 {
		t.Errorf("%d pulls, %d sink calls; want neither", src.tailsCalls, sink.calls)
	}
}

// A refused report must not feed the sink either: a stale or wrong-node answer says
// nothing about where the chain is, and acting on it would start catch-ups that are
// not warranted.
func TestDataVersionReporter_RefusedReportDoesNotFeedChainTails(t *testing.T) {
	src := &fixedChainTails{term: 1, seq: 1, tails: map[string]int64{"kb-1": 42}}
	// A non-leader: it declines.
	_, _, addr := startPushServer(t, 7,
		WithDataVersionAggregator(&recordingRecorder{}, func() bool { return false }),
		WithChainTails(src))
	sink := &recordingChainTails{}

	if err := chainTailReporter(addr, sink).ReportOnce(context.Background()); err == nil {
		t.Fatal("a declined report must be reported as not landed")
	}
	if sink.calls != 0 || src.tailsCalls != 0 {
		t.Errorf("sink calls = %d, pulls = %d, want neither from a refused answer", sink.calls, src.tailsCalls)
	}
}
