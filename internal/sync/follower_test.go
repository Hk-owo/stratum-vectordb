package sync

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/versiondoc"
)

// recordingTrigger is an IndexBuildTrigger that records TriggerBuild calls.
type recordingTrigger struct {
	mu       sync.Mutex
	calls    []triggerCall
	failNext bool
}

type triggerCall struct {
	kbID      string
	versionID int64
}

func (r *recordingTrigger) TriggerBuild(_ context.Context, kbID string, versionID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failNext {
		r.failNext = false
		return errTriggerFailed
	}
	r.calls = append(r.calls, triggerCall{kbID: kbID, versionID: versionID})
	return nil
}

func (r *recordingTrigger) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

var errTriggerFailed = context.DeadlineExceeded // reuse a sentinel error

// newFollowerWithMocks builds a Follower wired to mock stores, suitable
// for focused applyEntry and error-path tests.
func newFollowerWithMocks(indexManager IndexBuildTrigger) *Follower {
	return NewFollower(
		docstore.NewMockDocStore(),
		chunkdoc.NewMockChunkDocMapper(),
		versiondoc.NewMockVersionDocList(),
		chunkstore.NewMockChunkStore(),
		indexManager,
	)
}

// TestFollower_applyEntry_AllTypes verifies each SyncEntryType is applied
// to the correct store with the documented semantics.
func TestFollower_applyEntry_AllTypes(t *testing.T) {
	f := newFollowerWithMocks(nil)
	ctx := context.Background()

	// VERSION_DOC_LIST → versiondoc.
	if err := f.applyEntry(ctx, &pb.SyncEntry{
		EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST,
		KbId:      "kb1", VersionId: 2, DocId: "doc-1",
	}); err != nil {
		t.Fatalf("VERSION_DOC_LIST: %v", err)
	}

	// DOC_STORE → docstore (payload carries the content tag, mirroring
	// the leader's wire format).
	if err := f.applyEntry(ctx, &pb.SyncEntry{
		EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE,
		KbId:      "kb1", VersionId: 2, DocId: "doc-1", Payload: append([]byte{tagContent}, []byte("content")...),
	}); err != nil {
		t.Fatalf("DOC_STORE: %v", err)
	}

	// CHUNK_DOC_FORWARD → chunkdoc (writes both directions).
	if err := f.applyEntry(ctx, &pb.SyncEntry{
		EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_FORWARD,
		KbId:      "kb1", ChunkId: "c1", DocId: "doc-1",
	}); err != nil {
		t.Fatalf("CHUNK_DOC_FORWARD: %v", err)
	}

	// CHUNK_VECTOR → chunkstore.
	if err := f.applyEntry(ctx, &pb.SyncEntry{
		EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_VECTOR,
		KbId:      "kb1", ChunkId: "c1", Payload: float32sToBytes([]float32{0.5, 1.5}),
	}); err != nil {
		t.Fatalf("CHUNK_VECTOR: %v", err)
	}

	// Assert the results landed in the right stores.
	ds, ok := f.docStore.(*docstore.MockDocStore)
	if !ok {
		t.Fatal("expected mock docstore")
	}
	if got, err := ds.ReadAt(ctx, "kb1", "doc-1", 2); err != nil || string(got) != "content" {
		t.Errorf("docstore ReadAt = (%q, %v), want (content, nil)", got, err)
	}

	cdm, ok := f.chunkDoc.(*chunkdoc.MockChunkDocMapper)
	if !ok {
		t.Fatal("expected mock chunkdoc mapper")
	}
	docIDs, err := cdm.ListDocIDs(ctx, "kb1", "c1")
	if err != nil || len(docIDs) != 1 || docIDs[0] != "doc-1" {
		t.Errorf("forward ListDocIDs(c1) = %v, %v; want [doc-1]", docIDs, err)
	}
	chunkIDs, err := cdm.ListChunkIDsByDocs(ctx, "kb1", []string{"doc-1"})
	if err != nil || len(chunkIDs) != 1 || chunkIDs[0] != "c1" {
		t.Errorf("reverse ListChunkIDsByDocs(doc-1) = %v, %v; want [c1]", chunkIDs, err)
	}

	vd, ok := f.versionDoc.(*versiondoc.MockVersionDocList)
	if !ok {
		t.Fatal("expected mock versiondoc list")
	}
	docs, err := vd.ListDocIDs(ctx, "kb1", 2)
	if err != nil || len(docs) != 1 || docs[0] != "doc-1" {
		t.Errorf("versiondoc ListDocIDs = %v, %v; want [doc-1]", docs, err)
	}

	cs, ok := f.chunkStore.(*chunkstore.MockChunkStore)
	if !ok {
		t.Fatal("expected mock chunkstore")
	}
	if got, err := cs.Read("kb1", "c1"); err != nil || len(got) != 2 || got[0] != 0.5 {
		t.Errorf("chunkstore Read = %v, %v; want [0.5 1.5]", got, err)
	}
}

// TestFollower_applyEntry_ReverseEntry verifies a standalone reverse
// entry still lands (idempotent Write path) per the documented behavior.
func TestFollower_applyEntry_ReverseEntry(t *testing.T) {
	f := newFollowerWithMocks(nil)
	ctx := context.Background()

	if err := f.applyEntry(ctx, &pb.SyncEntry{
		EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_REVERSE,
		KbId:      "kb1", ChunkId: "c9", DocId: "doc-9",
	}); err != nil {
		t.Fatalf("CHUNK_DOC_REVERSE: %v", err)
	}

	cdm := f.chunkDoc.(*chunkdoc.MockChunkDocMapper)
	chunkIDs, err := cdm.ListChunkIDsByDocs(ctx, "kb1", []string{"doc-9"})
	if err != nil || len(chunkIDs) != 1 || chunkIDs[0] != "c9" {
		t.Errorf("reverse-only entry did not land: %v, %v", chunkIDs, err)
	}
}

// TestFollower_applyEntry_UnknownType verifies unknown entry types error
// out (the stream is then aborted by the caller).
func TestFollower_applyEntry_UnknownType(t *testing.T) {
	f := newFollowerWithMocks(nil)
	err := f.applyEntry(context.Background(), &pb.SyncEntry{
		EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_UNSPECIFIED,
	})
	if err == nil {
		t.Fatal("expected error for unknown entry type")
	}
	if !strings.Contains(err.Error(), "unknown SyncEntryType") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestFollower_applyEntry_Tombstone verifies a tombstone-tagged entry
// ({0x00}) produces a deleted document in the docstore.
func TestFollower_applyEntry_Tombstone(t *testing.T) {
	f := newFollowerWithMocks(nil)
	ctx := context.Background()

	if err := f.applyEntry(ctx, &pb.SyncEntry{
		EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE,
		KbId:      "kb1", VersionId: 3, DocId: "doc-gone", Payload: []byte{tagTombstone},
	}); err != nil {
		t.Fatalf("DOC_STORE tombstone: %v", err)
	}

	ds := f.docStore.(*docstore.MockDocStore)
	if _, err := ds.ReadAt(ctx, "kb1", "doc-gone", 3); err == nil {
		t.Error("expected ReadAt of a tombstoned doc to error (not found)")
	}
}

// TestFollower_applyEntry_EmptyContent verifies a content-tagged entry
// with empty content ({0x01}) yields a LIVE document whose content is the
// empty string — distinct from a tombstone.
func TestFollower_applyEntry_EmptyContent(t *testing.T) {
	f := newFollowerWithMocks(nil)
	ctx := context.Background()

	if err := f.applyEntry(ctx, &pb.SyncEntry{
		EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE,
		KbId:      "kb1", VersionId: 3, DocId: "doc-empty", Payload: []byte{tagContent},
	}); err != nil {
		t.Fatalf("DOC_STORE empty content: %v", err)
	}

	ds := f.docStore.(*docstore.MockDocStore)
	got, err := ds.ReadAt(ctx, "kb1", "doc-empty", 3)
	if err != nil {
		t.Fatalf("ReadAt of an empty-content doc must succeed (not a tombstone): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("content = %q, want empty", got)
	}
}

// TestFollower_applyEntry_DocStoreMissingTag verifies a DOC_STORE entry
// without the tag byte is rejected as corrupt.
func TestFollower_applyEntry_DocStoreMissingTag(t *testing.T) {
	f := newFollowerWithMocks(nil)
	err := f.applyEntry(context.Background(), &pb.SyncEntry{
		EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE,
		KbId:      "kb1", VersionId: 1, DocId: "doc-1",
	})
	if err == nil || !strings.Contains(err.Error(), "missing tag byte") {
		t.Fatalf("expected 'missing tag byte' error, got %v", err)
	}
}

// TestFollower_applyEntry_DocStoreUnknownTag verifies an unrecognized tag
// byte is rejected as corrupt.
func TestFollower_applyEntry_DocStoreUnknownTag(t *testing.T) {
	f := newFollowerWithMocks(nil)
	err := f.applyEntry(context.Background(), &pb.SyncEntry{
		EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE,
		KbId:      "kb1", VersionId: 1, DocId: "doc-1", Payload: []byte{0x99, 'x'},
	})
	if err == nil || !strings.Contains(err.Error(), "unknown tag") {
		t.Fatalf("expected 'unknown tag' error, got %v", err)
	}
}

// startLeaderServer starts a real gRPC server hosting a LeaderHandler over
// real Pebble stores and the given fake vecstore, returning the server,
// the fixture stores (so the test can seed the leader's data), and the
// listener address.
func startLeaderServer(t *testing.T, fv *fakeVecstore) (*leaderFixture, *grpc.Server, string) {
	t.Helper()
	f := newLeaderFixture(t)
	if fv != nil {
		f.vecstore = fv
		f.handler = NewLeaderHandler(f.docStore.DB(), f.chunkDoc.DB(), f.versionDoc.DB(), fv)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterDataSyncServiceServer(srv, f.handler)
	go srv.Serve(lis)

	t.Cleanup(srv.Stop)
	return f, srv, lis.Addr().String()
}

// TestFollower_PullVersion_EndToEnd is the module-interaction test for
// sync: a real LeaderHandler streaming over real gRPC into a real
// Follower whose target stores are real PebbleDB instances. After
// PullVersion the follower's stores must contain exactly the leader's
// dataset, and TriggerBuild must have been invoked.
func TestFollower_PullVersion_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Leader side: real Pebble stores + fake vecstore with one vector.
	leader, _, addr := startLeaderServer(t, newFakeVecstore())
	kbID, versionID := "kb-sync", int64(7)
	if err := leader.docStore.Write(ctx, kbID, "doc-1", 7, []byte("synced content")); err != nil {
		t.Fatal(err)
	}
	if err := leader.chunkDoc.Write(ctx, kbID, "c-1", "doc-1"); err != nil {
		t.Fatal(err)
	}
	if err := leader.versionDoc.Write(ctx, kbID, versionID, "doc-1"); err != nil {
		t.Fatal(err)
	}
	leader.vecstore.vectors[encodeVecstoreKey(kbID, "c-1")] = []float32{1, 2, 3, 4}

	// Follower side: real Pebble stores + recording trigger.
	ds, err := docstore.NewPebbleDocStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	vd, err := versiondoc.NewPebbleVersionDocList(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	follower := NewFollower(ds, cdm, vd, chunkstore.NewMockChunkStore(), &recordingTrigger{})
	t.Cleanup(func() {
		ds.Close()
		cdm.Close()
		vd.Close()
	})

	if err := follower.PullVersion(ctx, addr, kbID, versionID); err != nil {
		t.Fatalf("PullVersion: %v", err)
	}

	// DocStore: visible content matches the leader's.
	if got, err := ds.ReadAt(ctx, kbID, "doc-1", versionID); err != nil || string(got) != "synced content" {
		t.Errorf("follower docstore = (%q, %v), want (synced content, nil)", got, err)
	}

	// ChunkDocMapper: forward and reverse both present.
	docIDs, err := cdm.ListDocIDs(ctx, kbID, "c-1")
	if err != nil || len(docIDs) != 1 || docIDs[0] != "doc-1" {
		t.Errorf("follower forward map = %v, %v", docIDs, err)
	}
	chunkIDs, err := cdm.ListChunkIDsByDocs(ctx, kbID, []string{"doc-1"})
	if err != nil || len(chunkIDs) != 1 || chunkIDs[0] != "c-1" {
		t.Errorf("follower reverse map = %v, %v", chunkIDs, err)
	}

	// VersionDocList: full doc set present.
	docs, err := vd.ListDocIDs(ctx, kbID, versionID)
	if err != nil || len(docs) != 1 || docs[0] != "doc-1" {
		t.Errorf("follower versiondoc = %v, %v", docs, err)
	}

	// ChunkStore: vector present via the mock's Read.
	csMock := follower.chunkStore.(*chunkstore.MockChunkStore)
	vec, err := csMock.Read(kbID, "c-1")
	if err != nil || len(vec) != 4 || vec[3] != 4 {
		t.Errorf("follower chunkstore vector = %v, %v", vec, err)
	}

	// TriggerBuild must have fired with the right arguments.
	trig := follower.indexManager.(*recordingTrigger)
	if trig.len() != 1 {
		t.Fatalf("TriggerBuild calls = %d, want 1", trig.len())
	}
}

// TestFollower_PullVersion_Idempotent verifies that pulling the same
// version twice converges (all stores idempotent-by-key) and triggers two
// builds.
func TestFollower_PullVersion_Idempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leader, _, addr := startLeaderServer(t, newFakeVecstore())
	kbID := "kb-dup"
	if err := leader.docStore.Write(ctx, kbID, "doc-1", 5, []byte("content")); err != nil {
		t.Fatal(err)
	}
	if err := leader.chunkDoc.Write(ctx, kbID, "c-1", "doc-1"); err != nil {
		t.Fatal(err)
	}
	if err := leader.versionDoc.Write(ctx, kbID, 5, "doc-1"); err != nil {
		t.Fatal(err)
	}
	leader.vecstore.vectors[encodeVecstoreKey(kbID, "c-1")] = []float32{1}

	ds, err := docstore.NewPebbleDocStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	vd, err := versiondoc.NewPebbleVersionDocList(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ds.Close()
		cdm.Close()
		vd.Close()
	})
	trig := &recordingTrigger{}
	follower := NewFollower(ds, cdm, vd, chunkstore.NewMockChunkStore(), trig)

	for i := 0; i < 2; i++ {
		if err := follower.PullVersion(ctx, addr, kbID, 5); err != nil {
			t.Fatalf("PullVersion #%d: %v", i+1, err)
		}
	}

	// No duplicate mappings from re-application.
	docIDs, err := cdm.ListDocIDs(ctx, kbID, "c-1")
	if err != nil || len(docIDs) != 1 {
		t.Errorf("after double pull forward map = %v, %v; want exactly [doc-1]", docIDs, err)
	}
	if trig.len() != 2 {
		t.Errorf("TriggerBuild calls = %d, want 2", trig.len())
	}
}

// TestFollower_PullVersion_DialError verifies an unreachable leader
// produces a wrapped error and no TriggerBuild.
func TestFollower_PullVersion_DialError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	// Grab a port with no listener behind it.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := lis.Addr().String()
	lis.Close() // now nothing is listening

	trig := &recordingTrigger{}
	follower := newFollowerWithMocks(trig)
	err = follower.PullVersion(ctx, deadAddr, "kb", 1)
	if err == nil {
		t.Fatal("expected PullVersion to fail when the leader is unreachable")
	}
	if trig.len() != 0 {
		t.Errorf("TriggerBuild should not be called on dial failure")
	}
}

// TestFollower_PullVersion_ServerError verifies that a mid-stream failure
// on the leader side surfaces as a PullVersion error and skips
// TriggerBuild.
func TestFollower_PullVersion_ServerError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fv := newFakeVecstore()
	fv.err = status.Error(codes.Unavailable, "vecstore down") // fail the vector read at the end of the stream
	leader, _, addr := startLeaderServer(t, fv)
	kbID := "kb-fail"
	if err := leader.docStore.Write(ctx, kbID, "doc-1", 1, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := leader.chunkDoc.Write(ctx, kbID, "c-1", "doc-1"); err != nil {
		t.Fatal(err)
	}
	if err := leader.versionDoc.Write(ctx, kbID, 1, "doc-1"); err != nil {
		t.Fatal(err)
	}

	trig := &recordingTrigger{}
	follower := newFollowerWithMocks(trig)
	err := follower.PullVersion(ctx, addr, kbID, 1)
	if err == nil {
		t.Fatal("expected PullVersion to fail when the leader's vecstore read fails")
	}
	if trig.len() != 0 {
		t.Errorf("TriggerBuild should not be called after a failed pull")
	}
}

// TestFollower_PullVersion_TriggerBuildFailure verifies that a failing
// TriggerBuild surfaces as a PullVersion error (the Raft apply loop then
// retries).
func TestFollower_PullVersion_TriggerBuildFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leader, _, addr := startLeaderServer(t, newFakeVecstore())
	kbID := "kb-trigfail"
	if err := leader.docStore.Write(ctx, kbID, "doc-1", 1, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := leader.versionDoc.Write(ctx, kbID, 1, "doc-1"); err != nil {
		t.Fatal(err)
	}

	trig := &recordingTrigger{failNext: true}
	follower := newFollowerWithMocks(trig)
	err := follower.PullVersion(ctx, addr, kbID, 1)
	if err == nil {
		t.Fatal("expected PullVersion to fail when TriggerBuild fails")
	}
	if !strings.Contains(err.Error(), "TriggerBuild") {
		t.Errorf("unexpected error: %v", err)
	}
}
