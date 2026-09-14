package sync

import (
	"context"
	"errors"
	"testing"

	pb "stratum/api/proto/stratum"
)

// recordingRecorder stands in for the leader-side aggregate (§7.13.4).
type recordingRecorder struct {
	nodes   []int64
	reports []map[string]int64
}

// fixedWatermarks is a ReclaimWatermarkSource with a fixed table.
type fixedWatermarks struct {
	watermarks map[string]int64
}

func (w *fixedWatermarks) ReclaimableChangesThrough(kbID string) (int64, bool) {
	v, ok := w.watermarks[kbID]
	return v, ok
}

// recordingSink captures what a leader carried back.
type recordingSink struct {
	calls   int
	lastOne map[string]int64
}

func (s *recordingSink) SetLeaderWatermarks(watermarks map[string]int64) {
	s.calls++
	s.lastOne = watermarks
}

func (r *recordingRecorder) Record(nodeID int64, address string, dataVersions map[string]int64) {
	r.nodes = append(r.nodes, nodeID)
	r.reports = append(r.reports, dataVersions)
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

// The leader carries back a watermark for each knowledge base the reporter mentioned —
// those are the ones whose changes could be sitting in the reporter's WAL.
func TestPushHandler_ReportDataVersions_CarriesWatermarksBack(t *testing.T) {
	rec := &recordingRecorder{}
	marks := &fixedWatermarks{watermarks: map[string]int64{"kb-1": 8}}
	_, _, addr := startPushServer(t, 7,
		WithDataVersionAggregator(rec, func() bool { return true }),
		WithReclaimWatermarks(marks))

	resp, err := dialSyncServer(t, addr).ReportDataVersions(context.Background(), &pb.ReportDataVersionsRequest{
		NodeId:       3,
		DataVersions: map[string]int64{"kb-1": 8, "kb-2": 4},
	})
	if err != nil {
		t.Fatalf("ReportDataVersions: %v", err)
	}
	if got := resp.GetReclaimable()["kb-1"]; got != 8 {
		t.Errorf("reclaimable[kb-1] = %d, want 8", got)
	}
	// kb-2 has no watermark, so it must be absent — an unknown must not travel as a
	// default value, or the reporter would reclaim changes it cannot justify.
	if _, present := resp.GetReclaimable()["kb-2"]; present {
		t.Error("kb-2 has no watermark and must not be sent")
	}
}

// An accepted report stores what the leader carried back; that is what lets a node
// which writes data — and therefore does not lead — learn the watermark.
func TestDataVersionReporter_StoresWatermarksFromAnAcceptedReport(t *testing.T) {
	rec := &recordingRecorder{}
	marks := &fixedWatermarks{watermarks: map[string]int64{"kb-1": 8}}
	_, _, addr := startPushServer(t, 7,
		WithDataVersionAggregator(rec, func() bool { return true }),
		WithReclaimWatermarks(marks))

	sink := &recordingSink{}
	r := NewDataVersionReporter(DataVersionReporterConfig{
		NodeID:       3,
		DataVersions: func() map[string]int64 { return map[string]int64{"kb-1": 8} },
		ResolveLeader: func(context.Context) (string, bool, error) {
			return addr, true, nil
		},
		Watermarks: sink,
	})
	if err := r.ReportOnce(context.Background()); err != nil {
		t.Fatalf("ReportOnce: %v", err)
	}
	if sink.calls != 1 {
		t.Fatalf("sink calls = %d, want 1", sink.calls)
	}
	if sink.lastOne["kb-1"] != 8 {
		t.Errorf("stored watermarks = %v, want kb-1:8", sink.lastOne)
	}
}

// A refused report must NOT move the watermarks: a stale or wrong-node answer cannot
// be allowed to decide what may be discarded.
func TestDataVersionReporter_RefusedReportDoesNotMoveWatermarks(t *testing.T) {
	rec := &recordingRecorder{}
	// A non-leader: it declines.
	_, _, addr := startPushServer(t, 7,
		WithDataVersionAggregator(rec, func() bool { return false }),
		WithReclaimWatermarks(&fixedWatermarks{watermarks: map[string]int64{"kb-1": 8}}))

	sink := &recordingSink{}
	r := NewDataVersionReporter(DataVersionReporterConfig{
		NodeID:       3,
		DataVersions: func() map[string]int64 { return map[string]int64{"kb-1": 8} },
		ResolveLeader: func(context.Context) (string, bool, error) {
			return addr, true, nil
		},
		Watermarks: sink,
	})
	if err := r.ReportOnce(context.Background()); err == nil {
		t.Fatal("a declined report must be reported as not landed")
	}
	if sink.calls != 0 {
		t.Errorf("sink calls = %d, want 0: a refused answer must not move the watermarks", sink.calls)
	}
}
