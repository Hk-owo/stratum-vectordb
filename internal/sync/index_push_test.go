package sync

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

// recordingInstaller captures what a received index was installed as, and
// answers the §8.4(a) presence probe. The counters are the evidence for the
// probe's whole point: a skip leaves InstallIndex untouched, and a skip is the
// only outcome that should even ask.
type recordingInstaller struct {
	kbID      string
	versionID int64
	index     []byte
	sidecar   []byte
	err       error

	// held is what HasIndex answers; hasErr, when set, is what it fails with.
	held   bool
	hasErr error

	mu       sync.Mutex
	hasSeen  int
	installs int
}

func (i *recordingInstaller) HasIndex(_ context.Context, _ string, _ int64) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.hasSeen++
	if i.hasErr != nil {
		return false, i.hasErr
	}
	return i.held, nil
}

func (i *recordingInstaller) InstallIndex(_ context.Context, kbID string, versionID int64, indexData, sidecarData []byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.installs++
	if i.err != nil {
		return i.err
	}
	i.kbID = kbID
	i.versionID = versionID
	i.index = indexData
	i.sidecar = sidecarData
	return nil
}

func (i *recordingInstaller) counts() (hasSeen, installs int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.hasSeen, i.installs
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
// to arrive before the index: an interrupted transfer must not leave an index
// that cannot be authenticated. §8.4(a) puts a zero-length presence probe ahead
// of both, so "first" is asserted of the data frames.
func TestIndexPusher_SendsTheSidecarFirst(t *testing.T) {
	ctx := context.Background()
	srv := &orderRecordingServer{}
	addr := startOrderServer(t, srv)

	indexData := bytes.Repeat([]byte("I"), indexChunkSize+1)
	p := NewIndexPusher()
	if err := p.PushIndex(ctx, addr, "kb-1", 7, indexData, []byte("sidecar")); err != nil {
		t.Fatalf("PushIndex: %v", err)
	}

	chunks := srv.seen()
	if len(chunks) < 3 {
		t.Fatalf("saw %d chunks, want a probe plus both files", len(chunks))
	}
	if chunks[0].sidecar || chunks[0].size != 0 {
		t.Errorf("first chunk = %+v, want the zero-length presence probe", chunks[0])
	}
	data := chunks[1:]
	if !data[0].sidecar || data[0].size == 0 {
		t.Errorf("first data chunk = %+v, want a non-empty sidecar chunk", data[0])
	}
	if last := data[len(data)-1]; last.sidecar {
		t.Errorf("last chunk = %+v, want the index tail", last)
	}
	for i, c := range data {
		if c.size == 0 {
			t.Errorf("data chunk %d is empty: only the probe may be", i)
		}
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

// §8.4(a): a target that answers the probe with "already held" is a SKIP, and
// the caller has to be able to tell that apart from a failure — the whole
// point of shipping the probe is that the caller stops treating a redundant
// transfer as an error.
func TestIndexPusher_PushIndexReportsAnAlreadyPresentTarget(t *testing.T) {
	ctx := context.Background()
	installer := &recordingInstaller{held: true}
	addr := startIndexTarget(t, installer)

	err := NewIndexPusher().PushIndex(ctx, addr, "kb-1", 7, []byte("idx"), []byte("side"))
	if !errors.Is(err, ErrIndexAlreadyPresent) {
		t.Fatalf("err = %v, want ErrIndexAlreadyPresent", err)
	}
	if _, installs := installer.counts(); installs != 0 {
		t.Errorf("InstallIndex ran %d times for a skipped ship, want 0", installs)
	}
}

// ProbeIndex is the pre-flight: it must report the receiver's answer without
// shipping anything, in both directions.
func TestIndexPusher_ProbeIndexHitAndMiss(t *testing.T) {
	ctx := context.Background()

	held := startIndexTarget(t, &recordingInstaller{held: true})
	got, err := NewIndexPusher().ProbeIndex(ctx, held, "kb-1", 7)
	if err != nil {
		t.Fatalf("ProbeIndex(held): %v", err)
	}
	if !got {
		t.Error("ProbeIndex(held) = false, want true")
	}

	absent := startIndexTarget(t, &recordingInstaller{})
	got, err = NewIndexPusher().ProbeIndex(ctx, absent, "kb-1", 7)
	if err != nil {
		t.Fatalf("ProbeIndex(absent): %v", err)
	}
	if got {
		t.Error("ProbeIndex(absent) = true, want false")
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

// §8.4(a): a probe frame against a node that already holds the artifact ends
// the stream with AlreadyExists — and must not install, which is the assertion
// that proves no tens of megabytes were buffered first.
func TestPushHandler_PushIndexDataProbeSkipsWhenHeld(t *testing.T) {
	ctx := context.Background()
	installer := &recordingInstaller{held: true}
	client := dialIndex(t, startIndexTarget(t, installer))

	stream, err := client.PushIndexData(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.Send(probeFrame("kb-1", 7)); err != nil {
		t.Fatalf("send probe: %v", err)
	}
	_, err = stream.CloseAndRecv()
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("err = %v, want AlreadyExists", err)
	}
	hasSeen, installs := installer.counts()
	if hasSeen != 1 {
		t.Errorf("HasIndex ran %d times, want 1", hasSeen)
	}
	if installs != 0 {
		t.Errorf("InstallIndex ran %d times for a skipped probe, want 0", installs)
	}
}

// The same probe against a node that does NOT hold the artifact must be
// side-effect-free: the transfer that follows has to install as before.
func TestPushHandler_PushIndexDataProbeAcceptsWhenAbsent(t *testing.T) {
	ctx := context.Background()
	installer := &recordingInstaller{}
	client := dialIndex(t, startIndexTarget(t, installer))

	stream, err := client.PushIndexData(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	for _, chunk := range []*pb.PushIndexChunk{
		probeFrame("kb-1", 7),
		{KnowledgeBaseId: "kb-1", VersionId: 7, Sidecar: true, Data: []byte("side")},
		{KnowledgeBaseId: "kb-1", VersionId: 7, Data: []byte("idx"), Last: true},
	} {
		if err := stream.Send(chunk); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("install: %v", err)
	}
	if installer.index == nil || string(installer.index) != "idx" {
		t.Errorf("installed index = %q, want idx", installer.index)
	}
	if string(installer.sidecar) != "side" {
		t.Errorf("installed sidecar = %q, want side", installer.sidecar)
	}
}

// A bare probe — the pre-flight's own shape — is answered with NotFound, which
// is what lets ProbeIndex tell "not held" apart from an error.
func TestPushHandler_PushIndexDataBareProbeAnswersNotFound(t *testing.T) {
	ctx := context.Background()
	installer := &recordingInstaller{}
	client := dialIndex(t, startIndexTarget(t, installer))

	stream, err := client.PushIndexData(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.Send(probeFrame("kb-1", 7)); err != nil {
		t.Fatalf("send probe: %v", err)
	}
	_, err = stream.CloseAndRecv()
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v, want NotFound", err)
	}
	if _, installs := installer.counts(); installs != 0 {
		t.Errorf("InstallIndex ran %d times for a bare probe, want 0", installs)
	}
}

// Rolling upgrade, other direction: an older sender opens with a sidecar chunk
// and never probes, so the receiver must install exactly as it did before.
func TestPushHandler_PushIndexDataInstallsTheOldSequence(t *testing.T) {
	ctx := context.Background()
	installer := &recordingInstaller{}
	client := dialIndex(t, startIndexTarget(t, installer))

	stream, err := client.PushIndexData(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	for _, chunk := range []*pb.PushIndexChunk{
		{KnowledgeBaseId: "kb-1", VersionId: 7, Sidecar: true, Data: []byte("side")},
		{KnowledgeBaseId: "kb-1", VersionId: 7, Data: []byte("idx"), Last: true},
	} {
		if err := stream.Send(chunk); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("install: %v", err)
	}
	if _, installs := installer.counts(); installs != 1 {
		t.Errorf("InstallIndex ran %d times, want 1", installs)
	}
	if string(installer.index) != "idx" {
		t.Errorf("installed index = %q, want idx", installer.index)
	}
}

// A probe that fails is not an answer: the receiver must accept the push rather
// than skip a ship that was actually needed.
func TestPushHandler_PushIndexDataProbeFailureStillAccepts(t *testing.T) {
	ctx := context.Background()
	installer := &recordingInstaller{hasErr: errors.New("vecstore down")}
	client := dialIndex(t, startIndexTarget(t, installer))

	stream, err := client.PushIndexData(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	for _, chunk := range []*pb.PushIndexChunk{
		probeFrame("kb-1", 7),
		{KnowledgeBaseId: "kb-1", VersionId: 7, Sidecar: true, Data: []byte("side")},
		{KnowledgeBaseId: "kb-1", VersionId: 7, Data: []byte("idx"), Last: true},
	} {
		if err := stream.Send(chunk); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("a failed probe must not stop the install: %v", err)
	}
	if _, installs := installer.counts(); installs != 1 {
		t.Errorf("InstallIndex ran %d times, want 1", installs)
	}
}

// §8 风险 7: only the FIRST frame can be a probe. A zero-length frame in the
// middle of a real transfer is a data frame — treating it as a question would
// race with data that is already being written.
func TestPushHandler_PushIndexDataZeroLengthFrameMidStreamIsData(t *testing.T) {
	ctx := context.Background()
	installer := &recordingInstaller{}
	client := dialIndex(t, startIndexTarget(t, installer))

	stream, err := client.PushIndexData(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	for _, chunk := range []*pb.PushIndexChunk{
		{KnowledgeBaseId: "kb-1", VersionId: 7, Sidecar: true, Data: []byte("side")},
		{KnowledgeBaseId: "kb-1", VersionId: 7}, // empty, but NOT the first frame
		{KnowledgeBaseId: "kb-1", VersionId: 7, Data: []byte("idx"), Last: true},
	} {
		if err := stream.Send(chunk); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("install: %v", err)
	}
	if hasSeen, _ := installer.counts(); hasSeen != 0 {
		t.Errorf("HasIndex ran %d times; a mid-stream empty frame is data, not a probe", hasSeen)
	}
	if string(installer.index) != "idx" || string(installer.sidecar) != "side" {
		t.Errorf("installed index=%q sidecar=%q, want idx/side", installer.index, installer.sidecar)
	}
}

// probeFrame is the §8.4(a) presence question: the first frame of a stream, no
// payload, no sidecar flag, no last flag.
func probeFrame(kbID string, versionID int64) *pb.PushIndexChunk {
	return &pb.PushIndexChunk{KnowledgeBaseId: kbID, VersionId: versionID}
}

// recordedChunk is one frame as the receiving side saw it.
type recordedChunk struct {
	sidecar bool
	size    int
}

// orderRecordingServer notes the shape of every frame that arrived, in order.
type orderRecordingServer struct {
	pb.UnimplementedDataSyncServiceServer

	mu     sync.Mutex
	chunks []recordedChunk
}

func (s *orderRecordingServer) PushIndexData(stream pb.DataSyncService_PushIndexDataServer) error {
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break
		}
		s.mu.Lock()
		s.chunks = append(s.chunks, recordedChunk{sidecar: chunk.GetSidecar(), size: len(chunk.GetData())})
		s.mu.Unlock()
	}
	return stream.SendAndClose(&pb.PushIndexDataResponse{NodeId: 7})
}

func (s *orderRecordingServer) seen() []recordedChunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedChunk(nil), s.chunks...)
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
