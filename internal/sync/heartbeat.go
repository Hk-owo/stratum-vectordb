package sync

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
)

// DefaultHeartbeatInterval is how often a node tells the control leader it is alive. The
// heartbeat is soft state, so nothing breaks if one is lost.
const DefaultHeartbeatInterval = 5 * time.Second

// LeaderAddrResolver returns the current control leader's address, and whether one
// is known. It is called every interval rather than cached: a leader change must
// not leave the reporter talking to the old one forever (§7.13.1's pattern of
// re-resolving instead of growing a forwarding path).
type LeaderAddrResolver func(ctx context.Context) (string, bool, error)

// HeartbeaterConfig configures a Heartbeater.
type HeartbeaterConfig struct {
	// NodeID identifies this node in the heartbeat. The receiver records on this
	// value, so it must be the node's own ID, never one taken from elsewhere.
	NodeID int64

	// ResolveLeader finds the control leader. Required: with no leader known the
	// heartbeat is simply skipped for this interval.
	ResolveLeader LeaderAddrResolver

	// Interval overrides the heartbeat period. Zero uses the default.
	Interval time.Duration

	// Logger is optional: failed heartbeats are expected (leader elections,
	// restarts) and are logged at debug so they cannot be mistaken for faults.
	Logger *zap.Logger

	// Dial overrides the gRPC dialer, so tests can point at an in-process
	// listener. Optional.
	Dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)

	// ChainTails receives the chain tails on every accepted heartbeat: per knowledge
	// base, the newest version the control layer has accepted. It is the signal a node uses to notice it has fallen behind
	// (docs/active-lag-detection-design.md). Optional: without it this node keeps the
	// lazy recovery it has always had. *plane.LagCatchup implements it.
	ChainTails ChainTailSink
}

// ChainTailSink receives the chain tails: per knowledge base, the version at the tail of
// the replicated chain (docs/active-lag-detection-design.md). What it does with them — and
// whether it does anything at all — is its own decision. Declared here because plane
// imports this package.
type ChainTailSink interface {
	SetChainTails(tails map[string]int64)
}

// Heartbeater periodically tells the control leader this node is alive, and learns from
// the answer when the chain tails have moved.
type Heartbeater struct {
	nodeID        int64
	resolveLeader LeaderAddrResolver
	interval      time.Duration
	logger        *zap.Logger
	dial          func(ctx context.Context, addr string) (*grpc.ClientConn, error)

	// conn is the connection to the leader last resolved, reused between heartbeats so a
	// beat every few seconds does not cost a new connection each time. Only Run's
	// goroutine touches it.
	conn     *grpc.ClientConn
	connAddr string

	// chainTails receives the chain tails on every accepted heartbeat: per knowledge
	// base, the newest version the control layer has accepted
	// (docs/active-lag-detection-design.md). A sink decides for itself what to do
	// with them — including nothing.
	chainTails ChainTailSink
	// tails and tailsGeneration are the last pull. Only Run's goroutine touches them.
	tails           map[string]int64
	tailsGeneration *pb.ChainGeneration
}

// NewHeartbeater returns a heartbeater that dials leaders directly.
func NewHeartbeater(cfg HeartbeaterConfig) *Heartbeater {
	dial := cfg.Dial
	if dial == nil {
		dial = NewPresenceChecker(PresenceCheckerConfig{}).dial
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = DefaultHeartbeatInterval
	}
	return &Heartbeater{
		nodeID:        cfg.NodeID,
		resolveLeader: cfg.ResolveLeader,
		interval:      interval,
		logger:        cfg.Logger,
		dial:          dial,
		chainTails:    cfg.ChainTails,
	}
}

// Run sends heartbeats until ctx is done. Failures never stop the loop: a heartbeat that
// did not land says nothing about the data, and the next interval tries again.
func (r *Heartbeater) Run(ctx context.Context) {
	defer r.closeConn()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.ReportOnce(ctx); err != nil && r.logger != nil && ctx.Err() == nil {
				r.logger.Debug("sync: heartbeat did not land; retrying next interval", zap.Error(err))
			}
		}
	}
}

// ReportOnce sends one heartbeat. A nil return means either "the leader recorded it" or
// "there is no leader to report to right now" — both are ordinary states, not failures.
// An error means it did not land and the next interval should try again.
//
// The whole beat is bounded by one interval, so a leader that stops answering cannot
// stall the loop.
func (r *Heartbeater) ReportOnce(ctx context.Context) error {
	if r.resolveLeader == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, r.interval)
	defer cancel()

	addr, ok, err := r.resolveLeader(ctx)
	if err != nil {
		return fmt.Errorf("sync: resolve control leader for heartbeat: %w", err)
	}
	if !ok || addr == "" {
		// No leader during an election: nothing to report to, and nothing broken.
		return nil
	}

	conn, err := r.connTo(ctx, addr)
	if err != nil {
		return fmt.Errorf("sync: dial leader %s for heartbeat: %w", addr, err)
	}

	client := pb.NewDataSyncServiceClient(conn)
	resp, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: r.nodeID})
	if err != nil {
		return fmt.Errorf("sync: Heartbeat at %s: %w", addr, err)
	}
	if !resp.GetAccepted() {
		// The node we reached is not the leader. Its answer is not an error on the
		// wire, but the heartbeat did not land: the next interval re-resolves.
		return fmt.Errorf("sync: heartbeat at %s was not accepted (node %d is not the control leader)",
			addr, resp.GetNodeId())
	}

	// Only an accepted answer may act on this node: a stale or refused response says
	// nothing about where the chain is.
	if r.chainTails != nil {
		r.syncChainTails(ctx, client, resp.GetChainGeneration())
	}
	return nil
}

// connTo returns the connection to addr, reusing the last one when the leader has not
// moved. gRPC reconnects it on its own, so a failed call does not need a new one.
func (r *Heartbeater) connTo(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	if r.conn != nil && r.connAddr == addr {
		return r.conn, nil
	}
	r.closeConn()
	conn, err := r.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	r.conn, r.connAddr = conn, addr
	return conn, nil
}

func (r *Heartbeater) closeConn() {
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn, r.connAddr = nil, ""
	}
}

// syncChainTails pulls the tails when the leader's generation differs from the one this
// node last pulled at, and hands the sink the latest tails either way.
//
// Handing them over on every accepted heartbeat is deliberate. The sink skips what it has
// no room for, and retries what failed, on the next signal, so the signal has to keep
// coming whether or not anything changed. It costs a local comparison, not a network trip.
//
// A failed pull keeps the old generation, so the next heartbeat tries again.
func (r *Heartbeater) syncChainTails(ctx context.Context, client pb.DataSyncServiceClient, generation *pb.ChainGeneration) {
	if generation != nil && !sameGeneration(generation, r.tailsGeneration) {
		resp, err := client.GetChainTails(ctx, &pb.GetChainTailsRequest{})
		switch {
		case err != nil:
			if r.logger != nil {
				r.logger.Debug("sync: chain tails not pulled; retrying next interval", zap.Error(err))
			}
		case resp.GetAccepted() && resp.GetGeneration() != nil:
			r.tails = resp.GetChainTails()
			r.tailsGeneration = resp.GetGeneration()
			if r.logger != nil {
				r.logger.Debug("sync: chain tails pulled", zap.Int("chain_tails", len(r.tails)))
			}
		}
	}
	// Nothing pulled yet means "not told", which is not "no chains": the sink hears
	// nothing rather than an empty answer.
	if r.tailsGeneration != nil {
		r.chainTails.SetChainTails(r.tails)
	}
}

func sameGeneration(a, b *pb.ChainGeneration) bool {
	return b != nil && a.GetTerm() == b.GetTerm() && a.GetSeq() == b.GetSeq()
}
