package sync

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
)

// DefaultDataVersionReportInterval is how often a node reposts its cursors. It is
// a freshness/latency trade: the report is soft state (§7.13.4), so nothing
// breaks if one is lost, and a shorter interval only costs heartbeats.
const DefaultDataVersionReportInterval = 5 * time.Second

// DataVersionSource reports this node's contiguous cursors, keyed by knowledge
// base — the storage layer's answer to "what do I actually hold". An empty map is
// meaningful and is still sent: it is how a node that lost its data says so, and
// suppressing it would leave the leader's aggregate describing versions this node
// no longer has.
type DataVersionSource func() map[string]int64

// LeaderAddrResolver returns the current control leader's address, and whether one
// is known. It is called every interval rather than cached: a leader change must
// not leave the reporter talking to the old one forever (§7.13.1's pattern of
// re-resolving instead of growing a forwarding path).
type LeaderAddrResolver func(ctx context.Context) (string, bool, error)

// DataVersionReporterConfig configures a DataVersionReporter.
type DataVersionReporterConfig struct {
	// NodeID identifies this node in the report. The receiver records on this
	// value, so it must be the node's own ID, never one taken from elsewhere.
	NodeID int64

	// SelfAddr is this node's storage-layer gRPC address, reported alongside the
	// cursors. The leader's aggregate hands it back to consumers (the station's
	// route table) so they can dial the holder directly; without it every consumer
	// would need its own node-id→address map. Optional: an empty value means
	// "unknown", and holders are then reported without one.
	SelfAddr string

	// DataVersions supplies the cursors to report. Required.
	DataVersions DataVersionSource

	// ResolveLeader finds the control leader. Required: with no leader known the
	// report is simply skipped for this interval.
	ResolveLeader LeaderAddrResolver

	// Interval overrides the report period. Zero uses the default.
	Interval time.Duration

	// Logger is optional: reporting failures are expected (leader elections,
	// restarts) and are logged at debug so they cannot be mistaken for faults.
	Logger *zap.Logger

	// Dial overrides the gRPC dialer, so tests can point at an in-process
	// listener. Optional.
	Dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)

	// Watermarks receives the §7.5 reclaim watermarks the leader carries back on each
	// response. Optional: without it this node never learns them and simply keeps all
	// its recorded changes. *plane.LocalControlPlane implements it.
	Watermarks LeaderWatermarkSink
}

// LeaderWatermarkSink receives the watermarks a control leader published on a report's
// response (v13 §7.5): per knowledge base, the highest version whose recorded changes
// may be discarded. Declared here, rather than importing plane, because plane imports
// this package.
type LeaderWatermarkSink interface {
	SetLeaderWatermarks(watermarks map[string]int64)
}

// DataVersionReporter periodically tells the control leader which versions this
// node holds (§7.13.4). It owns no state: each report is a complete statement read
// fresh from the storage layer, so a missed report costs nothing and the next one
// corrects everything.
type DataVersionReporter struct {
	nodeID        int64
	selfAddr      string
	dataVersions  DataVersionSource
	resolveLeader LeaderAddrResolver
	interval      time.Duration
	logger        *zap.Logger
	dial          func(ctx context.Context, addr string) (*grpc.ClientConn, error)
	watermarks    LeaderWatermarkSink
}

// NewDataVersionReporter returns a reporter that dials leaders directly.
func NewDataVersionReporter(cfg DataVersionReporterConfig) *DataVersionReporter {
	dial := cfg.Dial
	if dial == nil {
		dial = NewPresenceChecker(PresenceCheckerConfig{}).dial
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = DefaultDataVersionReportInterval
	}
	return &DataVersionReporter{
		nodeID:        cfg.NodeID,
		selfAddr:      cfg.SelfAddr,
		dataVersions:  cfg.DataVersions,
		resolveLeader: cfg.ResolveLeader,
		interval:      interval,
		logger:        cfg.Logger,
		dial:          dial,
		watermarks:    cfg.Watermarks,
	}
}

// Run reports until ctx is done. Failures never stop the loop: a report that did
// not land says nothing about the data, and the next interval sends the whole
// view again.
func (r *DataVersionReporter) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.ReportOnce(ctx); err != nil && r.logger != nil && ctx.Err() == nil {
				r.logger.Debug("sync: data-version report did not land; retrying next interval", zap.Error(err))
			}
		}
	}
}

// ReportOnce sends one report. A nil return means either "the leader recorded
// it" or "there is no leader to report to right now" — both are ordinary states,
// not failures. An error means the report did not land and the next interval
// should try again.
func (r *DataVersionReporter) ReportOnce(ctx context.Context) error {
	if r.dataVersions == nil || r.resolveLeader == nil {
		return nil
	}
	addr, ok, err := r.resolveLeader(ctx)
	if err != nil {
		return fmt.Errorf("sync: resolve control leader for data-version report: %w", err)
	}
	if !ok || addr == "" {
		// No leader during an election: nothing to report to, and nothing broken.
		return nil
	}

	conn, err := r.dial(ctx, addr)
	if err != nil {
		return fmt.Errorf("sync: dial leader %s for data-version report: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := pb.NewDataSyncServiceClient(conn).ReportDataVersions(ctx, &pb.ReportDataVersionsRequest{
		NodeId:       r.nodeID,
		Address:      r.selfAddr,
		DataVersions: r.dataVersions(),
	})
	if err != nil {
		return fmt.Errorf("sync: ReportDataVersions at %s: %w", addr, err)
	}
	if !resp.GetAccepted() {
		// The node we reached is not the leader. Its answer is not an error on the
		// wire, but the report did not land: the next interval re-resolves.
		return fmt.Errorf("sync: data-version report at %s was not accepted (node %d is not the control leader)",
			addr, resp.GetNodeId())
	}

	// An accepted report carries the leader's reclaim watermarks back (§7.5). Storing
	// them is what lets a node that WRITES data — which under §7.13.2 need not be the
	// leader — learn how far it may discard its recorded changes. Only an accepted
	// response updates them: a stale or refused answer must not move the watermarks.
	if r.watermarks != nil {
		r.watermarks.SetLeaderWatermarks(resp.GetReclaimable())
	}
	return nil
}
