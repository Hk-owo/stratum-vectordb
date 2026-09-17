package sync

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

// indexChunkSize is how much of an index file travels per stream message,
// comfortably under gRPC's default 4 MiB message limit so a large index never
// forces that limit to be reconfigured.
const indexChunkSize = 1 << 20 // 1 MiB

// ErrIndexAlreadyPresent reports that the target already holds this version's
// artifact, so the ship was skipped rather than lost (§8.4(a)).
//
// It carries its own identity because "not needed" and "failed" look identical
// on the wire — both are a non-nil error out of PushIndex — while meaning
// opposite things: one is the optimisation working, the other is a replica that
// will have to build for itself and a line in the log worth reading.
var ErrIndexAlreadyPresent = errors.New("sync: the target already holds this version's index")

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

// indexShipError classifies a stream error the way every caller needs it: "the
// target already holds this version" is a SKIP, not a failure (§8.4(a)), so it
// is wrapped in ErrIndexAlreadyPresent; anything else passes through as the
// failure it is.
//
// The classification has to happen at more than one point in the stream's
// lifecycle. The receiver answers a probe by returning AlreadyExists while the
// sender may still be inside Send, but the code is only guaranteed to surface at
// CloseAndRecv — checking one site alone would let the other degrade into io.EOF
// and be read as a completed transfer.
func indexShipError(prefix string, err error) error {
	if status.Code(err) == codes.AlreadyExists {
		return fmt.Errorf("%s: %w", prefix, ErrIndexAlreadyPresent)
	}
	return fmt.Errorf("%s: %w", prefix, err)
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

	stage := func(what string) string {
		return fmt.Sprintf("sync: PushIndex(%s v%d) to %s: %s", kbID, versionID, targetAddr, what)
	}
	// sendFrame sends one frame and, when the stream has already ended, recovers
	// the receiver's real status. gRPC reports a server-side termination to the
	// SENDER as a bare io.EOF out of Send, and the code saying WHY is only
	// available from the receive side — so a sender that returned io.EOF here
	// would turn "the target already holds it" into a transport error.
	sendFrame := func(sidecar bool, data []byte, last bool) error {
		err := stream.Send(&pb.PushIndexChunk{
			KnowledgeBaseId: kbID,
			VersionId:       versionID,
			Sidecar:         sidecar,
			Data:            data,
			Last:            last,
		})
		if err == io.EOF {
			_, err = stream.CloseAndRecv()
		}
		return err
	}

	// §8.4(a) probe: one zero-length frame asks the receiver whether it already
	// holds this artifact, before a single megabyte moves.
	//
	// It has to be the FIRST frame, and it is the only one with no payload, no
	// sidecar flag and no last flag — a real transfer opens with a sidecar
	// chunk, and an empty index payload is refused above, so the receiver can
	// tell a probe from data without any protocol change. An old receiver sees
	// a zero-length chunk (it writes nothing) and the transfer proceeds
	// unchanged; a new one that holds the artifact answers AlreadyExists.
	if err := sendFrame(false, nil, false); err != nil {
		return indexShipError(stage("probe"), err)
	}

	for off := 0; off < len(sidecarData); off += indexChunkSize {
		end := off + indexChunkSize
		if end > len(sidecarData) {
			end = len(sidecarData)
		}
		if err := sendFrame(true, sidecarData[off:end], false); err != nil {
			return indexShipError(stage(fmt.Sprintf("sidecar chunk at %d", off)), err)
		}
	}
	for off := 0; off < len(indexData); off += indexChunkSize {
		end := off + indexChunkSize
		if end > len(indexData) {
			end = len(indexData)
		}
		if err := sendFrame(false, indexData[off:end], end == len(indexData)); err != nil {
			return indexShipError(stage(fmt.Sprintf("index chunk at %d", off)), err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		// The other place the probe's answer arrives: a receiver that answered
		// while this side was still sending surfaces the code HERE, not in Send.
		return indexShipError(stage("close"), err)
	}
	_ = resp.GetNodeId() // the acceptor's identity is informational here
	return nil
}

// ProbeIndex asks targetAddr whether it already holds (kbID, versionID)'s
// artifact, without shipping it (§8.4(a)).
//
// It is one zero-length frame on a PushIndexData stream. AlreadyExists is "yes,
// skip the ship". NotFound is the explicit "no" that a bare probe is answered
// with. Anything else — an unreachable peer, a timeout, an older receiver with
// no probe path — comes back as an error.
//
// The caller decides what an error means, and the only safe answer is "ship
// anyway": not knowing is not the same as knowing it is absent, and guessing
// wrong in that direction costs the replica a build, while guessing wrong the
// other way costs one redundant transfer.
func (p *IndexPusher) ProbeIndex(ctx context.Context, targetAddr, kbID string, versionID int64) (bool, error) {
	conn, err := p.dial(ctx, targetAddr)
	if err != nil {
		return false, fmt.Errorf("sync: ProbeIndex: dial %s: %w", targetAddr, err)
	}
	defer func() { _ = conn.Close() }()

	stream, err := pb.NewDataSyncServiceClient(conn).PushIndexData(ctx)
	if err != nil {
		return false, fmt.Errorf("sync: ProbeIndex(%s v%d) to %s: open stream: %w", kbID, versionID, targetAddr, err)
	}
	// io.EOF here means the receiver has already answered, not that the send
	// failed: the status comes back from CloseAndRecv below.
	err = stream.Send(&pb.PushIndexChunk{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
		Sidecar:         false,
		Data:            nil,
		Last:            false,
	})
	if err != nil && err != io.EOF {
		return false, fmt.Errorf("sync: ProbeIndex(%s v%d) to %s: probe frame: %w", kbID, versionID, targetAddr, err)
	}
	_, err = stream.CloseAndRecv()
	switch status.Code(err) {
	case codes.AlreadyExists:
		return true, nil
	case codes.NotFound:
		return false, nil
	case codes.OK:
		// A nil error means the receiver installed something, which a probe is
		// not. Rather than read that as an answer, report it as the anomaly it
		// is and let the caller ship.
		return false, fmt.Errorf("sync: ProbeIndex(%s v%d) to %s: receiver installed a payload for a probe", kbID, versionID, targetAddr)
	default:
		return false, fmt.Errorf("sync: ProbeIndex(%s v%d) to %s: %w", kbID, versionID, targetAddr, err)
	}
}
