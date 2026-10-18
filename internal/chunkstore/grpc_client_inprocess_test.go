package chunkstore

import (
	"context"

	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	vecstorepb "stratum/api/proto/vecstore"
)

// The tests in grpc_client_test.go talk to the real C++ vecstore_server, so they
// skip wherever that binary has not been built (CI included). The client's own
// contract — key encoding, error wrapping, and above all "FlushWAL reports the
// server's failure instead of swallowing it" — is testable without it: the wire
// protocol is the generated gRPC stub, and a Go implementation of the same service
// speaks it just as well over an in-process listener.
//
// What this cannot cover is the C++ side's actual semantics (RocksDB prefix
// delete, Faiss); those stay with the real-server test and vecstore's ctest.

// fakeChunkStorageService is an in-memory ChunkStorageService. Keys are compared
// byte-for-byte, exactly like the C++ side's RocksDB keys, so key encoding bugs
// (prefix collisions between "kb" and "kb-extended") show up here too.
type fakeChunkStorageService struct {
	vecstorepb.UnimplementedChunkStorageServiceServer

	mu      sync.Mutex
	vectors map[string][]float32
	writes  int
	flushes int

	// Injected failures: the client must surface these, never paper over them.
	writeErr  error
	readErr   error
	existsErr error
	flushErr  error
	usageErr  error
	lastFlush []string // keys that existed at each FlushWAL, for ordering assertions
}

func newFakeChunkStorageService() *fakeChunkStorageService {
	return &fakeChunkStorageService{vectors: make(map[string][]float32)}
}

func (s *fakeChunkStorageService) Write(_ context.Context, req *vecstorepb.WriteChunkRequest) (*vecstorepb.WriteChunkResponse, error) {
	if s.writeErr != nil {
		return nil, s.writeErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := make([]float32, len(req.GetVector()))
	copy(stored, req.GetVector())
	s.vectors[req.GetKey()] = stored
	s.writes++
	return &vecstorepb.WriteChunkResponse{}, nil
}

func (s *fakeChunkStorageService) Read(_ context.Context, req *vecstorepb.ReadChunkRequest) (*vecstorepb.ReadChunkResponse, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.vectors[req.GetKey()]
	if !ok {
		return nil, status.Error(codes.NotFound, "chunk not found")
	}
	return &vecstorepb.ReadChunkResponse{Vector: v}, nil
}

func (s *fakeChunkStorageService) Exists(_ context.Context, req *vecstorepb.ExistsChunkRequest) (*vecstorepb.ExistsChunkResponse, error) {
	if s.existsErr != nil {
		return nil, s.existsErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.vectors[req.GetKey()]
	return &vecstorepb.ExistsChunkResponse{Exists: ok}, nil
}

func (s *fakeChunkStorageService) Delete(_ context.Context, req *vecstorepb.DeleteChunkRequest) (*vecstorepb.DeleteChunkResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vectors, req.GetKey())
	return &vecstorepb.DeleteChunkResponse{}, nil
}

func (s *fakeChunkStorageService) DeleteByPrefix(_ context.Context, req *vecstorepb.DeleteByPrefixRequest) (*vecstorepb.DeleteByPrefixResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.vectors {
		if strings.HasPrefix(k, req.GetPrefix()) {
			delete(s.vectors, k)
		}
	}
	return &vecstorepb.DeleteByPrefixResponse{}, nil
}

func (s *fakeChunkStorageService) DiskUsage(context.Context, *vecstorepb.DiskUsageRequest) (*vecstorepb.DiskUsageResponse, error) {
	if s.usageErr != nil {
		return nil, s.usageErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var bytes uint64
	for k, v := range s.vectors {
		bytes += uint64(len(k)) + 4*uint64(len(v))
	}
	return &vecstorepb.DiskUsageResponse{Bytes: bytes}, nil
}

func (s *fakeChunkStorageService) FlushWAL(context.Context, *vecstorepb.FlushWALRequest) (*vecstorepb.FlushWALResponse, error) {
	if s.flushErr != nil {
		return nil, s.flushErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes++
	snapshot := make([]string, 0, len(s.vectors))
	for k := range s.vectors {
		snapshot = append(snapshot, k)
	}
	s.lastFlush = snapshot
	return &vecstorepb.FlushWALResponse{}, nil
}

// newInProcessChunkStore wires a VecstoreChunkStore to srv over bufconn, the same
// way NewVecstoreChunkStore wires it to a TCP address — the client code under test
// is unchanged, only the transport differs.
func newInProcessChunkStore(t *testing.T, srv *fakeChunkStorageService) *VecstoreChunkStore {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	vecstorepb.RegisterChunkStorageServiceServer(server, srv)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &VecstoreChunkStore{conn: conn, client: vecstorepb.NewChunkStorageServiceClient(conn)}
}

// TestVecstoreChunkStore_InProcess covers every ChunkStore method over the real
// gRPC stub: the requests the client builds (keys included) and the answers it
// maps back to Go values.
func TestVecstoreChunkStore_InProcess(t *testing.T) {
	srv := newFakeChunkStorageService()
	store := newInProcessChunkStore(t, srv)
	ctx := context.Background()

	if err := store.Write(ctx, "kb1", "chunk1", []float32{1, 2, 3}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	ok, err := store.Exists(ctx, "kb1", "chunk1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !ok {
		t.Fatal("Exists = false after Write")
	}
	if ok, err := store.Exists(ctx, "kb1", "never-written"); err != nil || ok {
		t.Fatalf("Exists(never-written) = %v, %v; want false, nil", ok, err)
	}

	// The client must send the length-prefixed key, not kbID+chunkID: the server
	// stores whatever bytes it is given, so reading them back is the assertion.
	if _, found := srv.vectors[encodeKey("kb1", "chunk1")]; !found {
		t.Fatalf("the server did not receive the encoded key %q; got keys %v",
			encodeKey("kb1", "chunk1"), keysOf(srv.vectors))
	}

	// VecstoreClient is the data-sync leader's read path; it has to reach the same
	// vectors the write path stored.
	vec, err := store.VecstoreClient().Read(ctx, &vecstorepb.ReadChunkRequest{Key: encodeKey("kb1", "chunk1")})
	if err != nil {
		t.Fatalf("Read through VecstoreClient: %v", err)
	}
	for i, want := range []float32{1, 2, 3} {
		if got := vec.GetVector()[i]; got != want {
			t.Fatalf("Read vector[%d] = %v, want %v", i, got, want)
		}
	}

	// DeleteByKB must remove one knowledge base and leave prefix-sharing ones
	// alone ("kb" vs "kb-extended").
	for _, kb := range []string{"kb", "kb-extended"} {
		if err := store.Write(ctx, kb, "c1", []float32{1}); err != nil {
			t.Fatalf("Write(%s): %v", kb, err)
		}
	}
	if err := store.DeleteByKB(ctx, "kb"); err != nil {
		t.Fatalf("DeleteByKB: %v", err)
	}
	if ok, err := store.Exists(ctx, "kb", "c1"); err != nil || ok {
		t.Fatalf("Exists(kb,c1) after DeleteByKB = %v, %v; want false, nil", ok, err)
	}
	if ok, err := store.Exists(ctx, "kb-extended", "c1"); err != nil || !ok {
		t.Fatalf("Exists(kb-extended,c1) after DeleteByKB(kb) = %v, %v; want true, nil", ok, err)
	}

	if err := store.Delete(ctx, "kb-extended", "c1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ok, _ := store.Exists(ctx, "kb-extended", "c1"); ok {
		t.Fatal("Exists = true after Delete")
	}

	usage, err := store.DiskUsage(ctx)
	if err != nil {
		t.Fatalf("DiskUsage: %v", err)
	}
	if want := uint64(len(encodeKey("kb1", "chunk1"))) + 4*3; usage != want {
		t.Fatalf("DiskUsage = %d, want %d", usage, want)
	}

	// FlushWAL must reach the server, and only after the writes it is vouching
	// for: a flush that runs first flushes nothing.
	if err := store.Write(ctx, "kb1", "chunk2", []float32{4}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := store.FlushWAL(ctx); err != nil {
		t.Fatalf("FlushWAL: %v", err)
	}
	if srv.flushes != 1 {
		t.Fatalf("the server saw %d FlushWAL call(s), want 1", srv.flushes)
	}
	if len(srv.lastFlush) != 2 {
		t.Fatalf("the flush covered %d chunk(s), want 2 (kb1/chunk1, kb1/chunk2)", len(srv.lastFlush))
	}
}

// TestVecstoreChunkStore_SurfacesServerFailures pins the direction the durability
// barrier depends on: these errors reach the caller, they are not logged away. The
// write path fails the transaction on a FlushWAL error precisely because the
// alternative is a COMMIT vouching for data still in the page cache.
func TestVecstoreChunkStore_SurfacesServerFailures(t *testing.T) {
	srv := newFakeChunkStorageService()
	store := newInProcessChunkStore(t, srv)
	ctx := context.Background()

	// gRPC turns a server error into a status, so what survives is the code and the
	// message; the assertion is that the client neither drops it nor turns it into
	// success.
	srv.flushErr = status.Error(codes.Internal, "rocksdb: flush failed")
	err := store.FlushWAL(ctx)
	if status.Code(err) != codes.Internal {
		t.Fatalf("FlushWAL = %v, want an Internal status", err)
	}
	if !strings.Contains(err.Error(), "FlushWAL") || !strings.Contains(err.Error(), "rocksdb: flush failed") {
		t.Fatalf("FlushWAL error %q should name the call and carry the server message", err)
	}

	srv.writeErr = status.Error(codes.Internal, "rocksdb: write failed")
	if err := store.Write(ctx, "kb1", "c1", []float32{1}); status.Code(err) != codes.Internal {
		t.Fatalf("Write = %v, want an Internal status", err)
	}
	srv.writeErr = nil

	srv.existsErr = status.Error(codes.Internal, "rocksdb: exists failed")
	if _, err := store.Exists(ctx, "kb1", "c1"); status.Code(err) != codes.Internal {
		t.Fatalf("Exists = %v, want an Internal status", err)
	}
	srv.existsErr = nil

	// The data-sync leader's read path uses VecstoreClient() directly; its failures
	// travel back untouched.
	srv.readErr = status.Error(codes.Internal, "rocksdb: read failed")
	if _, err := store.VecstoreClient().Read(ctx, &vecstorepb.ReadChunkRequest{Key: encodeKey("kb1", "c1")}); status.Code(err) != codes.Internal {
		t.Fatalf("Read = %v, want an Internal status", err)
	}
	srv.readErr = nil

	srv.usageErr = status.Error(codes.Internal, "rocksdb: property failed")
	if _, err := store.DiskUsage(ctx); status.Code(err) != codes.Internal {
		t.Fatalf("DiskUsage = %v, want an Internal status", err)
	}
}

// TestEncodeKey_DistinguishesPrefixSharingKnowledgeBases pins the encoding the
// prefix delete relies on. Without the length prefix, DeleteByKB("kb") would also
// wipe "kb-extended"'s vectors, and nothing would ever notice.
func TestEncodeKey_DistinguishesPrefixSharingKnowledgeBases(t *testing.T) {
	if got, want := EncodeKey("kb", "c1"), encodeKey("kb", "c1"); got != want {
		t.Fatalf("EncodeKey = %q, want %q", got, want)
	}

	short := encodeKBPrefix("kb")
	long := encodeKBPrefix("kb-extended")
	if !strings.HasPrefix("kb-extended", "kb") {
		t.Fatalf("test premise broken: \"kb-extended\" no longer starts with \"kb\"")
	}
	if strings.HasPrefix(string(long), string(short)) {
		t.Fatalf("encodeKBPrefix(%q) = %q is a prefix of encodeKBPrefix(%q) = %q; "+
			"DeleteByKB would delete both knowledge bases", "kb-extended", long, "kb", short)
	}
	if strings.HasPrefix(encodeKey("kb-extended", "c1"), string(short)) {
		t.Fatalf("encodeKey(kb-extended, c1) = %q collides with the delete prefix of \"kb\" (%q)",
			encodeKey("kb-extended", "c1"), short)
	}

	// And the length prefix has to survive non-ASCII IDs byte-for-byte.
	for _, kb := range []string{"", "kb", "知识库", "a\x00b"} {
		if got, want := string(encodeKBPrefix(kb)), EncodeKey(kb, "")[:len(encodeKBPrefix(kb))]; got != want {
			t.Fatalf("encodeKBPrefix(%q) = %q, want the leading bytes of the encoded key %q", kb, got, want)
		}
	}
}

func keysOf(m map[string][]float32) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
