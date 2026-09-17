package sync

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
)

// PresenceChecker asks a replica whether it holds a version's data — the
// control layer's side of DataSyncService.VersionPresence. A version that no
// candidate replica holds is one whose data never landed anywhere, as opposed
// to one that is merely waiting for its index build
// (Stratum_设计文档v13.md §7.12 step ①).
type PresenceChecker struct {
	dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// PresenceCheckerConfig wires a PresenceChecker.
type PresenceCheckerConfig struct {
	// Dial overrides the default gRPC dialer, so tests can point at an
	// in-process listener. Optional.
	Dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// NewPresenceChecker returns a checker that dials replicas directly.
func NewPresenceChecker(cfg PresenceCheckerConfig) *PresenceChecker {
	dial := cfg.Dial
	if dial == nil {
		dial = func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
			return grpc.DialContext(ctx, addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
		}
	}
	return &PresenceChecker{dial: dial}
}

// LocalVersionQuerier asks a peer how far its contiguous history reaches —
// the query a lagging node uses to pick a backfill source instead of always
// asking the leader (Stratum_设计文档v13.md §7.6). It shares
// PresenceCheckerConfig: both are just "how to dial a peer".
type LocalVersionQuerier struct {
	dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// NewLocalVersionQuerier returns a querier that dials peers directly.
func NewLocalVersionQuerier(cfg PresenceCheckerConfig) *LocalVersionQuerier {
	checker := NewPresenceChecker(cfg)
	return &LocalVersionQuerier{dial: checker.dial}
}

// LocalVersionOf asks the peer at addr for the highest version it holds
// contiguously for kbID. A dial or RPC failure is returned as an error: an
// unreachable peer is not a usable backfill source, but it is also not
// evidence that the version is missing anywhere.
func (q *LocalVersionQuerier) LocalVersionOf(ctx context.Context, addr, kbID string) (int64, error) {
	conn, err := q.dial(ctx, addr)
	if err != nil {
		return 0, fmt.Errorf("sync: dial replica %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := pb.NewDataSyncServiceClient(conn).LocalVersion(ctx, &pb.LocalVersionRequest{
		KnowledgeBaseId: kbID,
	})
	if err != nil {
		return 0, fmt.Errorf("sync: LocalVersion(%s) at %s: %w", kbID, addr, err)
	}
	return resp.GetVersion(), nil
}

// VersionDataCleaner broadcasts a version's cleanup to a peer
// (DataSyncService.DeleteVersionData). It implements plane.VersionDataCleaner
// and shares PresenceCheckerConfig: all of these are just "how to dial a peer".
type VersionDataCleaner struct {
	dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// NewVersionDataCleaner returns a cleaner that dials peers directly.
func NewVersionDataCleaner(cfg PresenceCheckerConfig) *VersionDataCleaner {
	checker := NewPresenceChecker(cfg)
	return &VersionDataCleaner{dial: checker.dial}
}

// DeleteVersionData asks the peer at addr to reclaim its copy of the version.
// A dial or RPC failure is returned as an error so the caller can log which
// replica could not be reached — the cleanup is best effort by design, but a
// replica that keeps refusing to answer is worth knowing about.
func (c *VersionDataCleaner) DeleteVersionData(ctx context.Context, peerAddr, kbID string, versionID int64, reason string) error {
	conn, err := c.dial(ctx, peerAddr)
	if err != nil {
		return fmt.Errorf("sync: dial replica %s: %w", peerAddr, err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := pb.NewDataSyncServiceClient(conn).DeleteVersionData(ctx, &pb.DeleteVersionDataRequest{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
		Reason:          reason,
	}); err != nil {
		return fmt.Errorf("sync: DeleteVersionData(%s v%d) at %s: %w", kbID, versionID, peerAddr, err)
	}
	return nil
}

// ConfirmBroadcaster tells a peer that a version it received reached quorum,
// so its §7.3 takeover timer can stand down. It implements
// plane.WriteConfirmer and shares PresenceCheckerConfig: like the others, it
// only needs to know how to dial a peer.
type ConfirmBroadcaster struct {
	dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// NewConfirmBroadcaster returns a broadcaster that dials peers directly.
func NewConfirmBroadcaster(cfg PresenceCheckerConfig) *ConfirmBroadcaster {
	checker := NewPresenceChecker(cfg)
	return &ConfirmBroadcaster{dial: checker.dial}
}

// ConfirmVersionWrite asks the peer at addr to stand down its takeover timer.
// A dial or RPC failure is returned so the caller can log which replica stayed
// in the dark — it will simply check for itself later, which is the safe
// direction (an extra announcement, never a missing one).
func (c *ConfirmBroadcaster) ConfirmVersionWrite(ctx context.Context, peerAddr, kbID string, versionID int64, sourceAddr string) error {
	conn, err := c.dial(ctx, peerAddr)
	if err != nil {
		return fmt.Errorf("sync: dial replica %s: %w", peerAddr, err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := pb.NewDataSyncServiceClient(conn).ConfirmVersionWrite(ctx, &pb.ConfirmVersionWriteRequest{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
		// sourceAddr is the writer's own address: the confirmation is also the
		// §8.5 announcement of where this version's data lives.
		SourceAddr: sourceAddr,
	}); err != nil {
		return fmt.Errorf("sync: ConfirmVersionWrite(%s v%d) at %s: %w", kbID, versionID, peerAddr, err)
	}
	return nil
}

// HasVersion reports whether the replica at addr holds (kbID, versionID).
// A dial or RPC failure is returned as an error so the caller can treat an
// unreachable replica as "unknown" rather than as "does not hold it".
func (c *PresenceChecker) HasVersion(ctx context.Context, addr, kbID string, versionID int64) (bool, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return false, fmt.Errorf("sync: dial replica %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := pb.NewDataSyncServiceClient(conn).VersionPresence(ctx, &pb.VersionPresenceRequest{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
	})
	if err != nil {
		return false, fmt.Errorf("sync: VersionPresence(%s v%d) at %s: %w", kbID, versionID, addr, err)
	}
	return resp.GetPresent(), nil
}
