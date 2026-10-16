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

// ErrIndexNotExported reports that a peer asked for an artifact answered "I do
// not hold it".
//
// The caller's next move is "ask someone else, or build" — not "retry this
// peer" — and it needs a distinct identity to act on that.
var ErrIndexNotExported = errors.New("sync: the peer does not hold this version's index")

// IndexFetcher pulls one version's built index FROM a peer that holds it
// (DataSyncService.FetchIndexData). This is §8.4's "建一次、分发 N 份" in its
// only direction.
//
// The push half (PushIndexData / PushIndexToReplicas) was removed. Two reasons,
// and the second is the one that decided it:
//
//   - A push is fire-and-forget. PushIndexToReplicas warned and returned nil for
//     a replica it could not reach, so nothing on the receiving side ever learned
//     that an artifact had been meant for it — the replica simply stayed without
//     an index. As long as the receiving side asks, that gap cannot exist.
//   - The pusher and the fetcher had to be gated separately, and only one of them
//     was: PushIndexToReplicas ran under indexPushSem, while the fetch service had
//     no gate at all. Two half-gates on the same payload is strictly worse than
//     one gate on the surviving path (see NodeHandler.FetchIndexData).
//
// It is not the ONLY way an artifact arrives. A node that asks and finds nobody
// holding the artifact builds it itself (see LocalDataPlane.EnsureIndex): the
// fetch is an optimisation, and a version that cannot be fetched must still
// become servable.
type IndexFetcher struct {
	// Dial is injectable for tests; nil means a plain insecure dial.
	Dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// NewIndexFetcher returns a fetcher that dials peers directly.
func NewIndexFetcher() *IndexFetcher {
	return &IndexFetcher{}
}

func (f *IndexFetcher) dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	if f.Dial != nil {
		return f.Dial(ctx, addr)
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// FetchIndex pulls one version's built index from a peer that holds it.
//
// The framing is the same one the (removed) push used, in the same order: the
// sidecar arrives first, then the index, in indexChunkSize slices, terminated by
// a chunk with last set. Message framing is deliberately byte-identical between
// directions so the two never drift; nothing here validates the CONTENT — the
// installer does, against the checksum the sidecar carries.
func (f *IndexFetcher) FetchIndex(ctx context.Context, sourceAddr, kbID string, versionID int64) (indexData, sidecarData []byte, err error) {
	conn, err := f.dial(ctx, sourceAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("sync: FetchIndex: dial %s: %w", sourceAddr, err)
	}
	defer func() { _ = conn.Close() }()

	stream, err := pb.NewDataSyncServiceClient(conn).FetchIndexData(ctx, &pb.FetchIndexDataRequest{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
	})
	if err != nil {
		return nil, nil, fetchIndexError(kbID, versionID, sourceAddr, err)
	}

	for {
		chunk, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			return nil, nil, fetchIndexError(kbID, versionID, sourceAddr, recvErr)
		}
		data := chunk.GetData()
		if len(data) == 0 {
			continue // the terminating frame
		}
		if chunk.GetSidecar() {
			sidecarData = append(sidecarData, data...)
			continue
		}
		indexData = append(indexData, data...)
	}

	if len(indexData) == 0 {
		// A stream that ended without an index is not "the peer has nothing" —
		// that is NotFound, and it maps to ErrIndexNotExported below. This is a
		// peer that answered OK and then sent nothing, which the caller must not
		// read as "no artifact exists".
		return nil, nil, fmt.Errorf("sync: FetchIndex(%s v%d) from %s: peer answered but sent no index payload",
			kbID, versionID, sourceAddr)
	}
	return indexData, sidecarData, nil
}

// fetchIndexError maps the wire status onto the identities the caller acts on.
// NotFound is the one that means "ask someone else"; everything else is a
// transport or server problem the caller reports as a failure.
func fetchIndexError(kbID string, versionID int64, sourceAddr string, err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("sync: FetchIndex(%s v%d) from %s: %w", kbID, versionID, sourceAddr, ErrIndexNotExported)
	}
	return fmt.Errorf("sync: FetchIndex(%s v%d) from %s: %w", kbID, versionID, sourceAddr, err)
}

// sendIndexArtifact writes an artifact through a stream: the sidecar FIRST (it
// carries the checksum the index is validated against, so an interrupted
// transfer never leaves an index whose authenticating metadata is missing), then
// the index, in indexChunkSize slices, with a terminating frame.
func sendIndexArtifact(send func(*pb.IndexChunk) error, kbID string, versionID int64, indexData, sidecarData []byte) error {
	for _, part := range []struct {
		data    []byte
		sidecar bool
	}{{sidecarData, true}, {indexData, false}} {
		for off := 0; off < len(part.data); off += indexChunkSize {
			end := off + indexChunkSize
			if end > len(part.data) {
				end = len(part.data)
			}
			if err := send(&pb.IndexChunk{
				KnowledgeBaseId: kbID,
				VersionId:       versionID,
				Sidecar:         part.sidecar,
				Data:            part.data[off:end],
			}); err != nil {
				return err
			}
		}
	}
	return send(&pb.IndexChunk{KnowledgeBaseId: kbID, VersionId: versionID, Last: true})
}
