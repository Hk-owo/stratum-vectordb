package sync

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
	"stratum/internal/wal"
)

// VersionChangesPuller fetches the changes a peer recorded over a version range —
// the client side of §7.5's delta backfill. It shares PresenceCheckerConfig with
// the other peer-facing helpers: all of them are just "how to dial a peer".
type VersionChangesPuller struct {
	dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// NewVersionChangesPuller returns a puller that dials peers directly.
func NewVersionChangesPuller(cfg PresenceCheckerConfig) *VersionChangesPuller {
	checker := NewPresenceChecker(cfg)
	return &VersionChangesPuller{dial: checker.dial}
}

// ChangesInRange asks the peer at addr for the changes it recorded for
// (fromExclusive, toInclusive], keyed by version ID. Versions the peer has no
// record for are absent from the map — the caller has to detect the gap and fall
// back to full records (§7.5).
//
// A peer that refuses (no reader wired, unreachable) or that fails mid-stream is
// reported as an error, never as a short or empty map: a truncated history that
// looks complete is exactly the failure this path must not produce.
func (p *VersionChangesPuller) ChangesInRange(ctx context.Context, peerAddr, kbID string, fromExclusive, toInclusive int64) (map[int64]wal.VersionDelta, error) {
	conn, err := p.dial(ctx, peerAddr)
	if err != nil {
		return nil, fmt.Errorf("sync: dial replica %s: %w", peerAddr, err)
	}
	defer func() { _ = conn.Close() }()

	stream, err := pb.NewDataSyncServiceClient(conn).PullVersionChanges(ctx, &pb.PullVersionChangesRequest{
		KnowledgeBaseId: kbID,
		FromExclusive:   fromExclusive,
		ToInclusive:     toInclusive,
	})
	if err != nil {
		return nil, fmt.Errorf("sync: PullVersionChanges(%s, (%d,%d]) at %s: %w",
			kbID, fromExclusive, toInclusive, peerAddr, err)
	}

	out := make(map[int64]wal.VersionDelta)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("sync: PullVersionChanges(%s, (%d,%d]) at %s: recv: %w",
				kbID, fromExclusive, toInclusive, peerAddr, err)
		}
		out[msg.GetVersionId()] = wal.VersionDelta{
			VersionID:       msg.GetVersionId(),
			ParentVersionID: msg.GetParentVersionId(),
			Changes:         docChangesFromProto(msg.GetChanges()),
		}
	}
}
