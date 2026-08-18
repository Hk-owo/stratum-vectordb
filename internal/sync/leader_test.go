package sync

import (
	"context"
	"encoding/binary"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	vecstorepb "stratum/api/proto/vecstore"
	"stratum/internal/chunkdoc"
	"stratum/internal/docstore"
	"stratum/internal/versiondoc"
)

// fakeVecstore is an in-memory ChunkStorageServiceClient stand-in that
// records the keys Read is called with and can be configured to fail.
type fakeVecstore struct {
	vectors  map[string][]float32
	err      error // if set, Read returns this error for every key
	readKeys []string
}

var _ vecstorepb.ChunkStorageServiceClient = (*fakeVecstore)(nil)

func newFakeVecstore() *fakeVecstore {
	return &fakeVecstore{vectors: make(map[string][]float32)}
}

func (f *fakeVecstore) Read(_ context.Context, in *vecstorepb.ReadChunkRequest, _ ...grpc.CallOption) (*vecstorepb.ReadChunkResponse, error) {
	f.readKeys = append(f.readKeys, in.GetKey())
	if f.err != nil {
		return nil, f.err
	}
	v, ok := f.vectors[in.GetKey()]
	if !ok {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return &vecstorepb.ReadChunkResponse{Vector: v}, nil
}

func (f *fakeVecstore) Write(context.Context, *vecstorepb.WriteChunkRequest, ...grpc.CallOption) (*vecstorepb.WriteChunkResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used in leader tests")
}
func (f *fakeVecstore) Delete(context.Context, *vecstorepb.DeleteChunkRequest, ...grpc.CallOption) (*vecstorepb.DeleteChunkResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used in leader tests")
}
func (f *fakeVecstore) DeleteByPrefix(context.Context, *vecstorepb.DeleteByPrefixRequest, ...grpc.CallOption) (*vecstorepb.DeleteByPrefixResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used in leader tests")
}
func (f *fakeVecstore) Exists(context.Context, *vecstorepb.ExistsChunkRequest, ...grpc.CallOption) (*vecstorepb.ExistsChunkResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used in leader tests")
}
func (f *fakeVecstore) DiskUsage(context.Context, *vecstorepb.DiskUsageRequest, ...grpc.CallOption) (*vecstorepb.DiskUsageResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used in leader tests")
}

// captureStream is a DataSyncService_PullVersionDataServer that records
// every sent SyncEntry instead of transmitting them.
type captureStream struct {
	grpc.ServerStream // embed for the default no-op implementations
	ctx               context.Context
	entries           []*pb.SyncEntry
	sendErr           error // if set, Send returns this error
}

var _ pb.DataSyncService_PullVersionDataServer = (*captureStream)(nil)

func (s *captureStream) Send(e *pb.SyncEntry) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.entries = append(s.entries, e)
	return nil
}

func (s *captureStream) Context() context.Context { return s.ctx }

// leaderFixture wires a LeaderHandler over real PebbleDB-backed stores.
type leaderFixture struct {
	docStore   *docstore.PebbleDocStore
	chunkDoc   *chunkdoc.PebbleChunkDocMapper
	versionDoc *versiondoc.PebbleVersionDocList
	vecstore   *fakeVecstore
	handler    *LeaderHandler
	ctx        context.Context
}

func newLeaderFixture(t *testing.T) *leaderFixture {
	t.Helper()
	ds, err := docstore.NewPebbleDocStore(t.TempDir())
	if err != nil {
		t.Fatalf("docstore: %v", err)
	}
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	vd, err := versiondoc.NewPebbleVersionDocList(t.TempDir())
	if err != nil {
		t.Fatalf("versiondoc: %v", err)
	}
	t.Cleanup(func() {
		ds.Close()
		cdm.Close()
		vd.Close()
	})

	fv := newFakeVecstore()
	return &leaderFixture{
		docStore:   ds,
		chunkDoc:   cdm,
		versionDoc: vd,
		vecstore:   fv,
		handler:    NewLeaderHandler(ds.DB(), cdm.DB(), vd.DB(), fv),
		ctx:        context.Background(),
	}
}

// pull runs PullVersionData and returns the captured entries, failing the
// test on error.
func (f *leaderFixture) pull(t *testing.T, kbID string, versionID int64) []*pb.SyncEntry {
	t.Helper()
	stream := &captureStream{ctx: f.ctx}
	err := f.handler.PullVersionData(&pb.PullVersionDataRequest{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
	}, stream)
	if err != nil {
		t.Fatalf("PullVersionData: %v", err)
	}
	return stream.entries
}

// docContent returns the semantic content of a DOC_STORE entry (tag byte
// stripped), failing the test if the payload isn't content-tagged.
func docContent(t *testing.T, e *pb.SyncEntry) string {
	t.Helper()
	if len(e.Payload) == 0 || e.Payload[0] != tagContent {
		t.Fatalf("DOC_STORE entry for %s lacks content tag: %x", e.DocId, e.Payload)
	}
	return string(e.Payload[1:])
}

// entriesByType groups captured entries by their type.
func entriesByType(entries []*pb.SyncEntry) map[pb.SyncEntryType][]*pb.SyncEntry {
	out := make(map[pb.SyncEntryType][]*pb.SyncEntry)
	for _, e := range entries {
		out[e.EntryType] = append(out[e.EntryType], e)
	}
	return out
}

// TestLeaderHandler_StreamsFullDataset populates all three PebbleDB stores
// plus the vecstore and verifies PullVersionData emits the complete,
// ordered dataset: VersionDocList, DocStore (MVCC-visible entry only),
// ChunkDocMapper reverse, ChunkDocMapper forward, ChunkStore vectors.
func TestLeaderHandler_StreamsFullDataset(t *testing.T) {
	f := newLeaderFixture(t)
	ctx := f.ctx
	kbID, versionID := "kb-1", int64(3)

	// Two documents at version 3; doc-1 was also written at version 2
	// (must NOT be streamed — only the entry visible at version 3).
	if err := f.docStore.Write(ctx, kbID, "doc-1", 2, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := f.docStore.Write(ctx, kbID, "doc-1", 3, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := f.docStore.Write(ctx, kbID, "doc-2", 3, []byte("second doc")); err != nil {
		t.Fatal(err)
	}

	// doc-1 has two chunks, doc-2 has one; chunk c2-1 is shared-style
	// reverse (doc-2 → chunk) — write both directions through the mapper.
	chunks := map[string][]string{
		"doc-1": {"c1-1", "c1-2"},
		"doc-2": {"c2-1"},
	}
	for docID, chunkIDs := range chunks {
		for _, c := range chunkIDs {
			if err := f.chunkDoc.Write(ctx, kbID, c, docID); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.versionDoc.Write(ctx, kbID, versionID, docID); err != nil {
			t.Fatal(err)
		}
	}

	// Vecstore holds vectors for all chunks.
	for _, c := range []string{"c1-1", "c1-2", "c2-1"} {
		f.vecstore.vectors[encodeVecstoreKey(kbID, c)] = []float32{1, 2, 3}
	}

	entries := f.pull(t, kbID, versionID)
	byType := entriesByType(entries)

	// 1. VersionDocList: exactly doc-1 and doc-2.
	vdEntries := byType[pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST]
	if len(vdEntries) != 2 {
		t.Fatalf("expected 2 VERSION_DOC_LIST entries, got %d: %+v", len(vdEntries), entries)
	}

	// 2. DocStore: exactly 2 entries (doc-1's visible content at v3).
	dsEntries := byType[pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE]
	if len(dsEntries) != 2 {
		t.Fatalf("expected 2 DOC_STORE entries, got %d", len(dsEntries))
	}
	for _, e := range dsEntries {
		switch e.DocId {
		case "doc-1":
			if docContent(t, e) != "new" || e.VersionId != 3 {
				t.Errorf("doc-1 entry = (v%d, %q), want (v3, %q)", e.VersionId, docContent(t, e), "new")
			}
		case "doc-2":
			if docContent(t, e) != "second doc" {
				t.Errorf("doc-2 payload = %q", docContent(t, e))
			}
		default:
			t.Errorf("unexpected doc store entry: %+v", e)
		}
	}

	// 3. Reverse: 3 entries (one per chunk-doc pair).
	rev := byType[pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_REVERSE]
	if len(rev) != 3 {
		t.Fatalf("expected 3 CHUNK_DOC_REVERSE entries, got %d", len(rev))
	}

	// 4. Forward: 3 entries.
	fwd := byType[pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_FORWARD]
	if len(fwd) != 3 {
		t.Fatalf("expected 3 CHUNK_DOC_FORWARD entries, got %d", len(fwd))
	}

	// 5. Vectors: 3 entries with round-tripped payloads.
	vecEntries := byType[pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_VECTOR]
	if len(vecEntries) != 3 {
		t.Fatalf("expected 3 CHUNK_VECTOR entries, got %d", len(vecEntries))
	}
	for _, e := range vecEntries {
		got := bytesToFloat32s(e.Payload)
		want := []float32{1, 2, 3}
		if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
			t.Errorf("vector for %s = %v, want %v", e.ChunkId, got, want)
		}
	}

	// Ordering contract: versiondoc first, then docstore, then reverse,
	// then forward, then vectors — check the overall order of the types.
	order := make([]pb.SyncEntryType, 0, len(entries))
	for _, e := range entries {
		order = append(order, e.EntryType)
	}
	wantOrder := []pb.SyncEntryType{
		pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_REVERSE,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_REVERSE,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_REVERSE,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_FORWARD,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_FORWARD,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_FORWARD,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_VECTOR,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_VECTOR,
		pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_VECTOR,
	}
	for i, want := range wantOrder {
		if order[i] != want {
			t.Fatalf("entry %d type = %v, want %v (full order: %v)", i, order[i], want, order)
		}
	}
}

// TestLeaderHandler_EmptyVersion verifies that a version with no
// documents produces zero entries (nothing to sync) instead of erroring.
func TestLeaderHandler_EmptyVersion(t *testing.T) {
	f := newLeaderFixture(t)
	entries := f.pull(t, "kb-empty", 1)
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries for empty version, got %d: %+v", len(entries), entries)
	}
}

// TestLeaderHandler_TombstoneStreamed verifies that a deleted document
// (empty payload in the leader's docstore) is streamed as an empty-payload
// DOC_STORE entry so the follower can reconstruct the MVCC tombstone.
func TestLeaderHandler_TombstoneStreamed(t *testing.T) {
	f := newLeaderFixture(t)
	ctx := f.ctx
	kbID, versionID := "kb-tomb", int64(4)

	if err := f.docStore.Write(ctx, kbID, "doc-1", 3, []byte("alive at v3")); err != nil {
		t.Fatal(err)
	}
	if err := f.docStore.Write(ctx, kbID, "doc-1", 4, nil); err != nil { // tombstone
		t.Fatal(err)
	}
	if err := f.versionDoc.Write(ctx, kbID, versionID, "doc-1"); err != nil {
		t.Fatal(err)
	}

	entries := f.pull(t, kbID, versionID)
	var dsEntry *pb.SyncEntry
	for _, e := range entries {
		if e.EntryType == pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE {
			dsEntry = e
		}
	}
	if dsEntry == nil {
		t.Fatal("expected a DOC_STORE entry for the tombstoned document")
	}
	// Tombstone payload is the single tagTombstone byte ({0x00}).
	if dsEntry.VersionId != 4 || len(dsEntry.Payload) != 1 || dsEntry.Payload[0] != tagTombstone {
		t.Errorf("tombstone entry = (v%d, %x), want (v4, [0x00])", dsEntry.VersionId, dsEntry.Payload)
	}
}

// TestLeaderHandler_EmptyContentPreserved verifies a live document with
// empty content streams as {tagContent} — NOT as a tombstone — so the
// follower can reconstruct the distinction.
func TestLeaderHandler_EmptyContentPreserved(t *testing.T) {
	f := newLeaderFixture(t)
	ctx := f.ctx
	kbID, versionID := "kb-empty-doc", int64(5)

	if err := f.docStore.Write(ctx, kbID, "doc-empty", versionID, []byte{}); err != nil {
		t.Fatal(err)
	}
	if err := f.versionDoc.Write(ctx, kbID, versionID, "doc-empty"); err != nil {
		t.Fatal(err)
	}

	entries := f.pull(t, kbID, versionID)
	var dsEntry *pb.SyncEntry
	for _, e := range entries {
		if e.EntryType == pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE {
			dsEntry = e
		}
	}
	if dsEntry == nil {
		t.Fatal("expected a DOC_STORE entry for the empty-content document")
	}
	if len(dsEntry.Payload) != 1 || dsEntry.Payload[0] != tagContent {
		t.Errorf("empty-content payload = %x, want [0x01] (content tag, not tombstone)", dsEntry.Payload)
	}
	if got := docContent(t, dsEntry); got != "" {
		t.Errorf("content = %q, want empty string", got)
	}
}

// TestLeaderHandler_KBPrefixIsolation verifies that syncing one KB does
// not bleed entries from another KB whose ID shares a string prefix.
func TestLeaderHandler_KBPrefixIsolation(t *testing.T) {
	f := newLeaderFixture(t)
	ctx := f.ctx

	if err := f.docStore.Write(ctx, "kb", "doc-shared", 1, []byte("short kb")); err != nil {
		t.Fatal(err)
	}
	if err := f.docStore.Write(ctx, "kb-extended", "doc-shared", 1, []byte("extended kb")); err != nil {
		t.Fatal(err)
	}
	if err := f.versionDoc.Write(ctx, "kb", 1, "doc-shared"); err != nil {
		t.Fatal(err)
	}
	if err := f.versionDoc.Write(ctx, "kb-extended", 1, "doc-shared"); err != nil {
		t.Fatal(err)
	}

	entries := f.pull(t, "kb", 1)
	if len(entries) != 2 { // 1 versiondoc + 1 docstore, no chunks
		t.Fatalf("expected 2 entries for kb, got %d: %+v", len(entries), entries)
	}
	for _, e := range entries {
		if e.KbId != "kb" {
			t.Errorf("entry leaked from another KB: %+v", e)
		}
		if e.EntryType == pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE && docContent(t, e) != "short kb" {
			t.Errorf("docstore payload = %q, want %q", docContent(t, e), "short kb")
		}
	}
}

// TestLeaderHandler_VecstoreNotFoundSkipped verifies that a chunk whose
// vector was GC'd on the leader (vecstore returns NotFound) is skipped
// rather than failing the whole stream.
func TestLeaderHandler_VecstoreNotFoundSkipped(t *testing.T) {
	f := newLeaderFixture(t)
	ctx := f.ctx
	kbID := "kb-gc"

	if err := f.docStore.Write(ctx, kbID, "doc-1", 1, []byte("content")); err != nil {
		t.Fatal(err)
	}
	if err := f.chunkDoc.Write(ctx, kbID, "c-gc", "doc-1"); err != nil {
		t.Fatal(err)
	}
	if err := f.versionDoc.Write(ctx, kbID, 1, "doc-1"); err != nil {
		t.Fatal(err)
	}
	// No vector in vecstore → NotFound → chunk skipped.

	entries := f.pull(t, kbID, 1)
	for _, e := range entries {
		if e.EntryType == pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_VECTOR {
			t.Fatalf("expected no CHUNK_VECTOR entries for a GC'd chunk, got %+v", e)
		}
	}
}

// TestLeaderHandler_VecstoreErrorFails verifies that a genuine vecstore
// failure (not NotFound) aborts the stream with an error.
func TestLeaderHandler_VecstoreErrorFails(t *testing.T) {
	f := newLeaderFixture(t)
	ctx := f.ctx
	kbID := "kb-err"

	if err := f.docStore.Write(ctx, kbID, "doc-1", 1, []byte("content")); err != nil {
		t.Fatal(err)
	}
	if err := f.chunkDoc.Write(ctx, kbID, "c1", "doc-1"); err != nil {
		t.Fatal(err)
	}
	if err := f.versionDoc.Write(ctx, kbID, 1, "doc-1"); err != nil {
		t.Fatal(err)
	}
	f.vecstore.err = status.Error(codes.Unavailable, "vecstore down")

	stream := &captureStream{ctx: f.ctx}
	err := f.handler.PullVersionData(&pb.PullVersionDataRequest{
		KnowledgeBaseId: kbID,
		VersionId:       1,
	}, stream)
	if err == nil {
		t.Fatal("expected PullVersionData to fail when vecstore is unavailable")
	}
}

// TestLeaderHandler_SendErrorAborts verifies a Send failure surfaces from
// PullVersionData immediately.
func TestLeaderHandler_SendErrorAborts(t *testing.T) {
	f := newLeaderFixture(t)
	ctx := f.ctx
	kbID := "kb-senderr"

	if err := f.versionDoc.Write(ctx, kbID, 1, "doc-1"); err != nil {
		t.Fatal(err)
	}

	stream := &captureStream{ctx: f.ctx, sendErr: status.Error(codes.Aborted, "stream closed")}
	err := f.handler.PullVersionData(&pb.PullVersionDataRequest{
		KnowledgeBaseId: kbID,
		VersionId:       1,
	}, stream)
	if err == nil {
		t.Fatal("expected PullVersionData to propagate a Send error")
	}
}

// TestEncodeVecstoreKey verifies the vecstore key encoding matches the
// chunkstore convention: 4-byte big-endian kbID length + kbID + chunkID.
func TestEncodeVecstoreKey(t *testing.T) {
	got := encodeVecstoreKey("kb", "chunk")
	n := len("kb")
	wantPrefix := []byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
	want := append(wantPrefix, []byte("kbchunk")...)
	if got != string(want) {
		t.Errorf("encodeVecstoreKey = %x, want %x", got, want)
	}
}

// TestFloat32RoundTrip verifies float32 slice → bytes → float32 slice is
// lossless, including negative and fractional values.
func TestFloat32RoundTrip(t *testing.T) {
	in := []float32{0.5, -1.25, 3.0, 0.0, -0.0001, 12345.678}
	out := bytesToFloat32s(float32sToBytes(in))
	if len(out) != len(in) {
		t.Fatalf("length = %d, want %d", len(out), len(in))
	}
	for i := range in {
		if out[i] != in[i] {
			t.Errorf("index %d: %v != %v", i, out[i], in[i])
		}
	}
}

// TestFloat32sToBytesEncoding sanity-checks the little-endian wire format.
func TestFloat32sToBytesEncoding(t *testing.T) {
	b := float32sToBytes([]float32{1.0})
	if len(b) != 4 {
		t.Fatalf("expected 4 bytes for one float32, got %d", len(b))
	}
	if got := binary.LittleEndian.Uint32(b); got != 0x3f800000 {
		t.Errorf("bits = %08x, want 0x3f800000 (1.0f)", got)
	}
}

// TestLeaderHandler_MetadataEmptyVectors verifies an empty vector payload
// round-trips without panic (defensive: never expected in practice).
func TestLeaderHandler_MetadataEmptyVectors(t *testing.T) {
	b := float32sToBytes(nil)
	if len(b) != 0 {
		t.Errorf("empty float32 slice should serialize to 0 bytes, got %d", len(b))
	}
	if got := bytesToFloat32s(b); len(got) != 0 {
		t.Errorf("expected empty deserialization, got %v", got)
	}
}
