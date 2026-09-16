package router

import (
	"context"
	"time"

	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
)

// statusClient is the narrow slice of AdminServiceClient the leader
// discoverer needs. Kept as an interface so tests can inject fakes
// without implementing the whole AdminServiceClient surface.
type statusClient interface {
	GetClusterStatus(context.Context, *pb.GetClusterStatusRequest, ...grpc.CallOption) (*pb.GetClusterStatusResponse, error)
}

// leaderResolver abstracts how the router learns which node index is the
// current Raft leader. *LeaderDiscoverer is the production implementation;
// tests inject fakes.
type leaderResolver interface {
	// LeaderNow returns the node index of the current leader by re-polling
	// the cluster, or false if no leader is known right now (election in
	// progress, or every node unreachable). There is deliberately no
	// TTL-cached variant: the only consumer is the write path, and writes
	// must always land on the true leader.
	LeaderNow(ctx context.Context) (int, bool)
}

// LeaderDiscoverer polls the cluster via AdminService.GetClusterStatus and
// returns the current leader. Discovery uses majority voting: each
// reachable node reports the leader it currently believes in, and the node
// ID reported by the most nodes wins. This tolerates partial partitions
// and converges to the true leader under normal Raft operation.
type LeaderDiscoverer struct {
	admins []statusClient
}

// NewLeaderDiscoverer constructs a discoverer over the given admin
// clients.
func NewLeaderDiscoverer(admins []statusClient) *LeaderDiscoverer {
	return &LeaderDiscoverer{admins: admins}
}

// LeaderNow polls every node's GetClusterStatus and returns the majority-
// voted leader index. Unreachable nodes are skipped; ok is false when no
// node is reported as leader (election in progress, or the leader itself
// is unreachable so its ID cannot be resolved to an index).
// leaderProbeTimeout bounds ONE node's GetClusterStatus probe.
//
// The question is "which node do you believe leads?", which a node answers from
// its own Raft state — no consensus round, no peer traffic. A node that cannot
// answer within this window is therefore far more likely to be gone than slow.
//
// The bound is what keeps one dead node from pinning the whole discovery: each
// probe gets its own budget instead of sharing the caller's, so an
// unresponsive node costs one probe and no more. Before this, a single dead
// node held discovery until the caller's deadline expired — and every write
// went with it, because LeaderNow is forwardWrite's first step.
const leaderProbeTimeout = time.Second

func (d *LeaderDiscoverer) LeaderNow(ctx context.Context) (int, bool) {
	votes := map[int64]int{}
	nodeByID := map[int64]int{}
	for idx, admin := range d.admins {
		// WithTimeout keeps whichever deadline is earlier, so a caller with
		// less budget left is never given more than it has.
		probeCtx, cancel := context.WithTimeout(ctx, leaderProbeTimeout)
		resp, err := admin.GetClusterStatus(probeCtx, &pb.GetClusterStatusRequest{})
		cancel()
		if err != nil {
			continue // node unreachable; rely on the others' reports
		}
		nodeByID[resp.NodeId] = idx
		if resp.HasLeader {
			votes[resp.LeaderId]++
		}
	}
	bestID, best := int64(0), 0
	for id, n := range votes {
		if n > best {
			bestID, best = id, n
		}
	}
	idx, ok := nodeByID[bestID]
	return idx, ok && best > 0
}
