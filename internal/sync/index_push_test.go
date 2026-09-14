package sync

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
)

// recordingInstaller captures what a received index was installed as.
type recordingInstaller struct {
	kbID      string
	versionID int64
	index     []byte
	sidecar   []byte
	err       error
}

func (i *recordingInstaller) InstallIndex(_ context.Context, kbID string, versionID int64, indexData, sidecarData []byte) error {
	if i.err != nil {
		return i.err
	}
	i.kbID = kbID
	i.versionID = versionID
	i.index = indexData
	i.sidecar = sidecarData
	return nil
}

var _ IndexInstaller = (*recordingInstaller)(nil)

// startIndexTarget registers a PushHandler carrying installer.
func startIndexTarget(t *testing.T, installer IndexInstaller) string {
	t.Helper()
	_, _, addr := startPushServer(t, 7, WithIndexInstaller(installer))
	return addr
}

func dialIndex(t *testing.T, addr string) pb.DataSyncServiceClient {
	t.Helper()
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewDataSyncServiceClient(conn)
}

// The transfer must reassemble both files byte-for-byte, across chunk
// boundaries — the point of streaming rather than one big message.
func TestIndexPusher_StreamsBothFilesIntact(t *testing.T) {
	ctx := context.Background()
	installer := &recordingInstaller{}
	addr := startIndexTarget(t, installer)

	// Larger than one chunk, so the reassembly is actually exercised.
	indexData := bytes.Repeat([]byte("I"), indexChunkSize+1234)
	sidecarData := []byte("stratum-index-1\n4\n0\ncrc\n")

	p := NewIndexPusher()
	if err := p.PushIndex(ctx, addr, "kb-1", 7, indexData, sidecarData); err != nil {
		t.Fatalf("PushIndex: %v", err)
	}

	if installer.kbID != "kb-1" || installer.versionID != 7 {
		t.Errorf("installed %s v%d, want kb-1 v7", installer.kbID, installer.versionID)
	}
	if !bytes.Equal(installer.index, indexData) {
		t.Errorf("index reassembled to %d bytes, want %d", len(installer.index), len(indexData))
	}
	if !bytes.Equal(installer.sidecar, sidecarData) {
		t.Errorf("sidecar = %q, want %q", installer.sidecar, sidecarData)
	}
}

// The sidecar carries the checksum the index is validated against, so it has
// to arrive first: an interrupted transfer must not leave an index that cannot
// be authenticated.
func TestIndexPusher_SendsTheSidecarFirst(t *testing.T) {
	ctx := context.Background()
	var order []bool
	srv := &orderRecordingServer{order: &order}
	addr := startOrderServer(t, srv)

	indexData := bytes.Repeat([]byte("I"), indexChunkSize+1)
	p := NewIndexPusher()
	if err := p.PushIndex(ctx, addr, "kb-1", 7, indexData, []byte("sidecar")); err != nil {
		t.Fatalf("PushIndex: %v", err)
	}

	if len(order) < 2 {
		t.Fatalf("saw %d chunks, want several", len(order))
	}
	if !order[0] {
		t.Error("the first chunk was not the sidecar")
	}
	if order[len(order)-1] {
		t.Error("the last chunk was a sidecar, want the index tail")
	}
}

// An empty index is refused locally rather than shipped as a stream that would
// install nothing.
func TestIndexPusher_RefusesAnEmptyIndex(t *testing.T) {
	p := NewIndexPusher()
	err := p.PushIndex(context.Background(), "127.0.0.1:1", "kb-1", 7, nil, []byte("s"))
	if err == nil {
		t.Fatal("want an error for an empty index payload")
	}
	if !strings.Contains(err.Error(), "empty index") {
		t.Errorf("err = %v, want it to name the empty payload", err)
	}
}

// A node with nothing to install with says so, instead of reporting success
// for data it dropped.
func TestPushHandler_PushIndexDataWithoutInstallerFails(t *testing.T) {
	ctx := context.Background()
	_, _, addr := startPushServer(t, 7) // no installer wired
	client := dialIndex(t, addr)

	stream, err := client.PushIndexData(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.Send(&pb.PushIndexChunk{
		KnowledgeBaseId: "kb-1", VersionId: 7, Data: []byte("x"), Last: true,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Fatal("want an error when the node has no index installer")
	}
}

// An install failure surfaces to the sender, so the builder can log which
// replica will have to build for itself.
func TestPushHandler_PushIndexDataSurfacesInstallFailure(t *testing.T) {
	ctx := context.Background()
	addr := startIndexTarget(t, &recordingInstaller{err: errors.New("disk full")})
	client := dialIndex(t, addr)

	stream, err := client.PushIndexData(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.Send(&pb.PushIndexChunk{
		KnowledgeBaseId: "kb-1", VersionId: 7, Data: []byte("x"), Last: true,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Fatal("want the install failure surfaced")
	}
}

// orderRecordingServer notes which kind of chunk arrived, in order.
type orderRecordingServer struct {
	pb.UnimplementedDataSyncServiceServer
	order *[]bool
}

func (s *orderRecordingServer) PushIndexData(stream pb.DataSyncService_PushIndexDataServer) error {
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break
		}
		*s.order = append(*s.order, chunk.GetSidecar())
	}
	return stream.SendAndClose(&pb.PushIndexDataResponse{NodeId: 7})
}

func startOrderServer(t *testing.T, srv *orderRecordingServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	g := grpc.NewServer()
	pb.RegisterDataSyncServiceServer(g, srv)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	return lis.Addr().String()
}
