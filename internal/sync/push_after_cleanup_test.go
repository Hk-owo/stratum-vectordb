package sync

import (
	"context"
	"testing"

	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/versiondoc"
)

// reclaimDropper reclaims from the real stores with the same prefix deletes
// WriteCoordinatorImpl.DropVersionStorage performs (docstore + versiondoc; the
// index and bloom halves do not matter to what is pinned here).
type reclaimDropper struct {
	ds *docstore.PebbleDocStore
	vd *versiondoc.PebbleVersionDocList
}

func (d reclaimDropper) DropVersionStorage(ctx context.Context, kbID string, versionID int64) error {
	if err := d.ds.DeleteByVersion(ctx, kbID, versionID); err != nil {
		return err
	}
	return d.vd.DeleteByVersion(ctx, kbID, versionID)
}

// TestPushHandler_PushAfterACleanupWritesTheVersionBack pins the source of the
// leftover window docs/known-gaps.md §B/§C describes, and it is the one that needs
// NO failure at all:
//
//   - fanOut returns the instant QUORUM is satisfied and lets the remaining pushes
//     keep running ("Quorum is in hand; the remaining pushes keep running in the
//     background with their own budget", local_data_plane.go). The version is
//     DURABLE by then, and delete admission only refuses an ACTIVE or PENDING one —
//     so it can be deleted right away.
//   - The cleanup broadcast reaches a slow replica BEFORE that replica's own push
//     has landed, so it drops what is there (usually an empty prefix).
//   - The late push then lands, and the receiving side has NO "does this version
//     still exist?" check anywhere on the write path (grep ExistingVersions /
//     VersionExists finds nothing in push.go or follower.go): the records are
//     written back, and PushVersionData's tail even moves the cursor up over the
//     version it just re-created.
//
// So the line between "rare" and "routine" is "did the slowest push outlive the
// delete?", not "did something fail".
//
// ⚠️ This test asserts CURRENT behaviour, and that behaviour is a KNOWN DEFECT —
// it is here so nobody assumes the window is closed. Whoever adds the existence
// check should flip these assertions (a late push must be refused and the version
// must stay gone) rather than delete the case.
func TestPushHandler_PushAfterACleanupWritesTheVersionBack(t *testing.T) {
	ctx := context.Background()

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
	t.Cleanup(func() { _ = ds.Close(); _ = cdm.Close(); _ = vd.Close() })

	f := NewFollower(ds, cdm, vd, chunkstore.NewMockChunkStore(), &recordingTrigger{})
	h := NewPushHandler(f, 3, WithVersionDataDropper(reclaimDropper{ds: ds, vd: vd}))

	const (
		kbID  = "kb-late-push"
		docID = "doc-1"
		body  = "content"
		vID   = int64(7)
	)

	// A push is a stream of entries: the version→docID list and the document record
	// itself (mirroring what sync.LeaderHandler exports). Fresh slices each time —
	// the payload is handed to the store.
	entries := func() []*pb.SyncEntry {
		return []*pb.SyncEntry{
			{
				EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST,
				KbId:      kbID, VersionId: vID, DocId: docID,
			},
			{
				EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE,
				KbId:      kbID, VersionId: vID, DocId: docID,
				Payload: append([]byte{tagContent}, []byte(body)...),
			},
		}
	}
	apply := func(what string) {
		t.Helper()
		for _, e := range entries() {
			if err := f.applyEntry(ctx, e); err != nil {
				t.Fatalf("%s: applyEntry(%v): %v", what, e.GetEntryType(), err)
			}
		}
	}

	// The version landed once — this is the quorum half of the fan-out.
	apply("first push")
	if got := listDocIDs(t, vd, kbID, vID); len(got) != 1 || got[0] != docID {
		t.Fatalf("precondition: docIDs = %v, want [%s]", got, docID)
	}

	// The delete flow's cleanup reaches this replica first.
	if _, err := h.DeleteVersionData(ctx, &pb.DeleteVersionDataRequest{
		KnowledgeBaseId: kbID,
		VersionId:       vID,
		Reason:          "deleted by the client",
	}); err != nil {
		t.Fatalf("DeleteVersionData: %v", err)
	}
	if got := listDocIDs(t, vd, kbID, vID); len(got) != 0 {
		t.Fatalf("docIDs = %v, want none after the cleanup", got)
	}
	if _, err := ds.ReadAt(ctx, kbID, docID, vID); err == nil {
		t.Fatal("precondition: the cleanup must have removed the record")
	}

	// The slow push lands afterwards. Nothing rejects it, so the version comes back
	// on a replica whose metadata row is already gone: this is the leftover.
	apply("late push")
	if got := listDocIDs(t, vd, kbID, vID); len(got) != 1 || got[0] != docID {
		t.Fatalf("docIDs = %v, want the late push to have written [%s] back", got, docID)
	}
	content, err := ds.ReadAt(ctx, kbID, docID, vID)
	if err != nil {
		t.Fatalf("ReadAt after the late push: %v", err)
	}
	if string(content) != body {
		t.Errorf("content = %q, want %q", content, body)
	}
}

func listDocIDs(t *testing.T, vd *versiondoc.PebbleVersionDocList, kbID string, versionID int64) []string {
	t.Helper()
	ids, err := vd.ListDocIDs(context.Background(), kbID, versionID)
	if err != nil {
		t.Fatalf("ListDocIDs: %v", err)
	}
	return ids
}
