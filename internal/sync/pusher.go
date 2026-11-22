package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "stratum/api/proto/stratum"
	"stratum/internal/rpcpool"
)

// VersionExporter is the storage layer's "hand one version's records to a
// sink" capability. *LeaderHandler implements it (it reads the local stores);
// keeping it an interface lets the Pusher be tested against a fake exporter.
type VersionExporter interface {
	ExportVersion(ctx context.Context, kbID string, versionID int64, send func(*pb.SyncEntry) error) error
}

// Pusher replicates one version's records to a target replica by driving that
// replica's PushVersionData stream. It is the write coordinator's side of the
// push direction (docs/参考/历史/Stratum_设计文档v13.md §7.2): the coordinator exports from
// its own stores and the target applies, so a version that was just written
// reaches its replicas instead of waiting for them to poll for it.
type Pusher struct {
	exporter VersionExporter
	nodeID   int64
	pool     *rpcpool.Pool
	dial     func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// PusherConfig wires a Pusher.
type PusherConfig struct {
	// Exporter supplies the version's records (see VersionExporter).
	Exporter VersionExporter
	// NodeID identifies this coordinator; echoed in diagnostics.
	NodeID int64
	// Pool supplies long-lived outbound storage-peer channels when Dial is nil.
	Pool *rpcpool.Pool
	// Dial overrides the default gRPC dialer, so tests can target an
	// in-process listener. Optional.
	Dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// NewPusher returns a Pusher over the given exporter.
func NewPusher(cfg PusherConfig) *Pusher {
	return &Pusher{exporter: cfg.Exporter, nodeID: cfg.NodeID, pool: cfg.Pool, dial: cfg.Dial}
}

// PushVersion hands (kbID, versionID) to the replica at targetAddr and waits
// for its acknowledgement. The receiving side applies records idempotently, so
// retrying after a lost acknowledgement is safe.
func (p *Pusher) PushVersion(ctx context.Context, targetAddr, kbID string, versionID int64) (*pb.PushVersionDataResponse, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set("x-stratum-push-kb", kbID)
	md.Set("x-stratum-push-version", strconv.FormatInt(versionID, 10))
	ctx = metadata.NewOutgoingContext(ctx, md)
	conn, release, err := rpcpool.Acquire(ctx, p.pool, p.dial, nil, targetAddr)
	if err != nil {
		return nil, fmt.Errorf("sync: dial replica %s: %w", targetAddr, err)
	}
	defer release()

	stream, err := pb.NewDataSyncServiceClient(conn).PushVersionData(ctx)
	if err != nil {
		return nil, fmt.Errorf("sync: open PushVersionData to %s: %w", targetAddr, err)
	}

	// A client stream's Send only ever says io.EOF once the stream has ended, whatever ended
	// it: the receiver refusing, or this push's own budget running out. The status that
	// says which comes from CloseAndRecv. Without it a timed-out push and a broken replica
	// read the same, as `stream ChunkDocMapper forward: EOF`.
	streamEnded := false
	send := func(entry *pb.SyncEntry) error {
		err := stream.Send(entry)
		if errors.Is(err, io.EOF) {
			streamEnded = true
		}
		return err
	}
	if err := p.exporter.ExportVersion(ctx, kbID, versionID, send); err != nil {
		if streamEnded {
			if _, recvErr := stream.CloseAndRecv(); recvErr != nil {
				return nil, fmt.Errorf("sync: export version %d to %s stopped at %q: %w", versionID, targetAddr, err.Error(), recvErr)
			}
		}
		return nil, fmt.Errorf("sync: export version %d to %s: %w", versionID, targetAddr, err)
	}

	ack, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("sync: close PushVersionData to %s: %w", targetAddr, err)
	}
	if ack.GetKnowledgeBaseId() != kbID || ack.GetVersionId() != versionID || ack.GetAcceptorId() <= 0 {
		return nil, fmt.Errorf("sync: replica %s acknowledged a different KB, version or node", targetAddr)
	}
	return ack, nil
}
