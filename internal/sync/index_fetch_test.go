package sync

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

// stubArtifactReader is the export side of FetchIndexData: it answers with fixed
// bytes, or with an error when the version is meant to be absent.
type stubArtifactReader struct {
	indexData   []byte
	sidecarData []byte
	err         error
}

func (s stubArtifactReader) ReadIndexFiles(string, int64) ([]byte, []byte, error) {
	return s.indexData, s.sidecarData, s.err
}

// startFetchServer hosts a NodeHandler whose artifact export is the given
// reader. Nothing else on the service is wired, because FetchIndexData is the
// only method under test — and reading it that way keeps the failure obvious if
// the handler ever starts depending on something else.
func startFetchServer(t *testing.T, reader IndexArtifactReader) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterDataSyncServiceServer(srv, NewNodeHandler(nil, nil, WithNodeIndexReader(reader)))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// TestIndexPusher_FetchIndexRoundTrip: an artifact that a peer holds comes back
// intact, sidecar included, so the receiving side can hand both straight to
// InstallIndex.
func TestIndexPusher_FetchIndexRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Larger than one chunk, so the framing (not just a single Send) is exercised.
	indexData := make([]byte, indexChunkSize+1234)
	for i := range indexData {
		indexData[i] = byte(i)
	}
	sidecarData := []byte("checksum-sidecar")

	addr := startFetchServer(t, stubArtifactReader{indexData: indexData, sidecarData: sidecarData})

	gotIndex, gotSidecar, err := NewIndexFetcher().FetchIndex(ctx, addr, "kb-1", 7)
	if err != nil {
		t.Fatalf("FetchIndex: %v", err)
	}
	if len(gotIndex) != len(indexData) {
		t.Fatalf("fetched %d index bytes, want %d", len(gotIndex), len(indexData))
	}
	for i := range indexData {
		if gotIndex[i] != indexData[i] {
			t.Fatalf("index byte %d = %d, want %d — the payload was corrupted in transit", i, gotIndex[i], indexData[i])
		}
	}
	if string(gotSidecar) != string(sidecarData) {
		t.Fatalf("sidecar = %q, want %q", gotSidecar, sidecarData)
	}
}

// TestIndexPusher_FetchIndex_PeerDoesNotHoldIt: the answer that means "ask
// someone else, or build" has to be distinguishable from "the call failed",
// because those two lead the caller in opposite directions.
func TestIndexPusher_FetchIndex_PeerDoesNotHoldIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr := startFetchServer(t, stubArtifactReader{err: errors.New("no artifact on this node")})

	_, _, err := NewIndexFetcher().FetchIndex(ctx, addr, "kb-1", 7)
	if err == nil {
		t.Fatal("FetchIndex succeeded against a peer that does not hold the artifact")
	}
	if !errors.Is(err, ErrIndexNotExported) {
		t.Fatalf("err = %v, want it to wrap ErrIndexNotExported so the caller moves on", err)
	}
}

// TestIndexPusher_FetchIndex_ExportNotWired: a node that does not export answers
// Unimplemented, which must NOT read as "nobody has it" — the caller cannot
// conclude anything about other peers from one that cannot answer.
func TestIndexPusher_FetchIndex_ExportNotWired(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr := startFetchServer(t, nil)

	_, _, err := NewIndexFetcher().FetchIndex(ctx, addr, "kb-1", 7)
	if err == nil {
		t.Fatal("FetchIndex succeeded against a node with no artifact export")
	}
	if errors.Is(err, ErrIndexNotExported) {
		t.Fatalf("err = %v; an Unimplemented export must not be read as \"this peer has nothing\"", err)
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("err code = %v, want Unimplemented", status.Code(err))
	}
}

// TestIndexPusher_FetchIndex_EmptyArtifactReadsAsAbsent: a zero-byte index file
// is not an artifact. The export side answers NotFound for it, which is the
// answer the caller must move on from — installing nothing would look like a
// successful fetch and leave the version just as cold.
func TestIndexPusher_FetchIndex_EmptyArtifactReadsAsAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr := startFetchServer(t, stubArtifactReader{indexData: nil})

	_, _, err := NewIndexFetcher().FetchIndex(ctx, addr, "kb-1", 7)
	if err == nil {
		t.Fatal("FetchIndex accepted an empty artifact")
	}
	if !errors.Is(err, ErrIndexNotExported) {
		t.Fatalf("err = %v, want ErrIndexNotExported: a zero-byte artifact is "+
			"\"this peer does not have one\", not a transport failure", err)
	}
}
