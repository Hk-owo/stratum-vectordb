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

	// ChainTails receives the chain tails the leader carries back on each response:
	// per knowledge base, the newest version the control layer has accepted. It is
	// the signal a node uses to notice it has fallen behind
	// (docs/active-lag-detection-design.md). Optional: without it this node keeps the
	// lazy recovery it has always had. *plane.LagCatchup implements it.
	ChainTails ChainTailSink

	// Holders receives the control leader's answer to "which nodes reported holding
	// this version", per knowledge base (§7.13.4). It is the same aggregate the
	// service station's route table reads; this node mirrors it so its data-source
	// lookup can answer without dialling anyone — that lookup may run on the Raft
	// apply path (see §8.5's history). Optional: without it the lookup keeps
	// answering from its last layer. *plane.HoldersCache implements it.
	//
	// It rides this report because the response is already being sent every
	// interval to the only node that has the aggregate. There is nothing to ask
	// for: the answer comes back with everything else.
	Holders HoldersSink
}

// LeaderWatermarkSink receives the watermarks a control leader published on a report's
// response (v13 §7.5): per knowledge base, the highest version whose recorded changes
// may be discarded. Declared here, rather than importing plane, because plane imports
// this package.
type LeaderWatermarkSink interface {
	SetLeaderWatermarks(watermarks map[string]int64)
}

// ChainTailSink receives the chain tails a control leader published on a report's
// response: per knowledge base, the version at the tail of the replicated chain
// (docs/active-lag-detection-design.md). What it does with them — and whether it does
// anything at all — is its own decision. Declared here for the same reason
// LeaderWatermarkSink is: plane imports this package.
type ChainTailSink interface {
	SetChainTails(tails map[string]int64)
}

// HoldersSink receives the per-knowledge-base holder lists a control leader published
// on a report's response (§7.13.4): which nodes reported holding that knowledge base,
// and the address each of them reported for itself.
//
// through carries, per knowledge base, the version the answer is good for; on this
// response it is the chain tail. A sink needs it because holder sets only narrow as
// the version rises — a node whose cursor reached 20 holds everything below 20 too —
// so an answer fetched with a higher version may serve lower asks, and never the other
// way round.
//
// A knowledge base ABSENT from holders means "the leader said nothing about it" —
// never "nobody holds it". The two lead to opposite actions, so a sink must not turn
// an absence into a negative fact. Declared here for the same reason the sinks above
// are: plane imports this package.
type HoldersSink interface {
	StoreHolders(holders map[string][]string, through map[string]int64)
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

	// chainTails receives the chain tails the leader carries back: per knowledge
	// base, the newest version the control layer has accepted
	// (docs/active-lag-detection-design.md). A sink decides for itself what to do
	// with them — including nothing.
	chainTails ChainTailSink

	// holders mirrors the leader's §7.13.4 aggregate. It is filled from this loop's
	// own response, which is why it needs no thread of its own.
	holders HoldersSink
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
		chainTails:    cfg.ChainTails,
		holders:       cfg.Holders,
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
	// The same accepted response carries the chain tails back. Like the watermarks,
	// only an accepted answer may act on this node: a stale or refused response says
	// nothing about where the chain is. What the sink does with them is its decision
	// — an unwired or disabled sink simply does nothing
	// (docs/active-lag-detection-design.md).
	if r.chainTails != nil {
		// What actually crossed the wire. "The report did not land" and "it landed
		// carrying nothing" look identical from outside and mean opposite things: the
		// first says the leader never heard us, the second says it did and had no tail
		// to give. Both counts are needed to tell them apart, because a report naming
		// zero knowledge bases cannot carry a tail at all — the leader only fills
		// tails for the knowledge bases the reporter names.
		//
		// dataVersions is read again here rather than kept from the request: this is
		// diagnostic, and a second read of the same node-local map is close enough.
		if r.logger != nil {
			r.logger.Debug("sync: data-version report landed",
				zap.Int("reported_kbs", len(r.dataVersions())),
				zap.Int("chain_tails", len(resp.GetChainTails())),
				zap.Int("reclaimable", len(resp.GetReclaimable())))
		}
		r.chainTails.SetChainTails(resp.GetChainTails())
	}
	// The same response answers "who holds this version" for the data-source
	// lookup, which mirrors it because that lookup may run on the Raft apply path
	// and cannot dial anyone (see §8.5's history). Nothing is asked for here: the
	// leader fills this for every knowledge base it knows, not only the ones named
	// above, so a node that missed a whole chain still learns where that chain is.
	if r.holders != nil {
		byKB := make(map[string][]string, len(resp.GetHolders()))
		for kbID, list := range resp.GetHolders() {
			byKB[kbID] = list.GetAddresses()
		}
		// The tail is what these answers are good through: a node holding the tail
		// holds every version below it, so the cache may reuse these holders for
		// lower asks — and only for lower ones.
		r.holders.StoreHolders(byKB, resp.GetChainTails())
	}
	return nil
}
