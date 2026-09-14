package sync

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
)

// indexChunkSize is how much of an index file travels per stream message,
// comfortably under gRPC's default 4 MiB message limit so a large index never
// forces that limit to be reconfigured.
const indexChunkSize = 1 << 20 // 1 MiB

// IndexPusher ships one version's built index to another node
// (DataSyncService.PushIndexData), so that node loads the artifact instead of
// rebuilding it (Stratum_设计文档v13.md §8.4).
type IndexPusher struct {
	// Dial is injectable for tests; nil means a plain insecure dial.
	Dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// NewIndexPusher returns a pusher that dials peers directly.
func NewIndexPusher() *IndexPusher {
	return &IndexPusher{}
}

func (p *IndexPusher) dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	if p.Dial != nil {
		return p.Dial(ctx, addr)
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// PushIndex streams the file to targetAddr.
//
// The sidecar goes first and the index second. That ordering is the point: if
// the transfer breaks midway, the replica is left with a checksum file and no
// index (harmless, and the next attempt overwrites it) rather than an index it
// cannot authenticate.
func (p *IndexPusher) PushIndex(ctx context.Context, targetAddr, kbID string, versionID int64, indexData, sidecarData []byte) error {
	if len(indexData) == 0 {
		return fmt.Errorf("sync: PushIndex(%s v%d): empty index payload", kbID, versionID)
	}
	conn, err := p.dial(ctx, targetAddr)
	if err != nil {
		return fmt.Errorf("sync: PushIndex: dial %s: %w", targetAddr, err)
	}
	defer func() { _ = conn.Close() }()

	stream, err := pb.NewDataSyncServiceClient(conn).PushIndexData(ctx)
	if err != nil {
		return fmt.Errorf("sync: PushIndex(%s v%d) to %s: open stream: %w", kbID, versionID, targetAddr, err)
	}

	send := func(sidecar bool, data []byte, last bool) error {
		return stream.Send(&pb.PushIndexChunk{
			KnowledgeBaseId: kbID,
			VersionId:       versionID,
			Sidecar:         sidecar,
			Data:            data,
			Last:            last,
		})
	}

	for off := 0; off < len(sidecarData); off += indexChunkSize {
		end := off + indexChunkSize
		if end > len(sidecarData) {
			end = len(sidecarData)
		}
		if err := send(true, sidecarData[off:end], false); err != nil {
			return fmt.Errorf("sync: PushIndex(%s v%d) to %s: sidecar chunk at %d: %w", kbID, versionID, targetAddr, off, err)
		}
	}
	for off := 0; off < len(indexData); off += indexChunkSize {
		end := off + indexChunkSize
		if end > len(indexData) {
			end = len(indexData)
		}
		if err := send(false, indexData[off:end], end == len(indexData)); err != nil {
			return fmt.Errorf("sync: PushIndex(%s v%d) to %s: index chunk at %d: %w", kbID, versionID, targetAddr, off, err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("sync: PushIndex(%s v%d) to %s: %w", kbID, versionID, targetAddr, err)
	}
	_ = resp.GetNodeId() // the acceptor's identity is informational here
	return nil
}
