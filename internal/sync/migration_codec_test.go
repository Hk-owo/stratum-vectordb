package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "stratum/api/proto/stratum"
	"stratum/internal/storage"
	"stratum/internal/types"
)

func migrationCodecFixture() (*pb.MigrationTransferRequest, storage.MigrationManifest, []*pb.MigrationDataFrame) {
	req := &pb.MigrationTransferRequest{KnowledgeBaseId: "kb", OperationId: "move", ExpectedEpoch: 1, SourceNodeId: 1}
	manifest, _ := storage.BuildMigrationManifest("kb", 1, []types.VersionMeta{{KBID: "kb", VersionID: 1, DataStatus: types.DataStatusDurable, DocIDSetHash: ComputeDocIDSetHash([]string{"doc"})}})
	entry := func(kind pb.SyncEntryType, doc, chunk string, v int64, payload []byte) *pb.SyncEntry {
		return &pb.SyncEntry{EntryType: kind, KbId: "kb", DocId: doc, ChunkId: chunk, VersionId: v, Payload: payload}
	}
	frames := []*pb.MigrationDataFrame{
		{Manifest: manifest.Bytes(), SourceNodeId: 1, OperationId: "move", KnowledgeBaseId: "kb", PlacementEpoch: 1},
		{Entry: entry(pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE, "doc", "", 1, []byte{tagContent, 'b'})},
		{VersionId: 1, Entry: entry(pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST, "doc", "", 1, nil)},
		{VersionId: 1, Entry: entry(pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE, "doc", "", 1, []byte{tagContent, 'b'})},
		{VersionId: 1, Entry: entry(pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_REVERSE, "doc", "chunk", 0, nil)},
		{VersionId: 1, Entry: entry(pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_FORWARD, "doc", "chunk", 0, nil)},
		{VersionId: 1, Entry: entry(pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_VECTOR, "", "chunk", 0, float32sToBytes([]float32{1, 2}))},
		{VersionId: 1, VersionEnd: true},
	}
	return req, manifest, frames
}

// Recompute a valid transport checksum even for malformed semantic records.
// This ensures negative tests are not passing solely because of a stale hash.
func withMigrationFooter(t *testing.T, frames []*pb.MigrationDataFrame) []*pb.MigrationDataFrame {
	t.Helper()
	digest := sha256.New()
	var count uint64
	for _, f := range frames {
		b, err := encodeMigrationFrame(f)
		if err != nil {
			t.Fatal(err)
		}
		hashMigrationFrame(digest, b)
		if f.Entry != nil {
			count++
		}
	}
	return append(frames, &pb.MigrationDataFrame{Complete: true, PayloadSha256: hex.EncodeToString(digest.Sum(nil)), EntryCount: count})
}
func TestMigrationCodecRejectsIncompleteOrOutOfScopeDataDespiteValidChecksum(t *testing.T) {
	tests := map[string]func([]*pb.MigrationDataFrame) []*pb.MigrationDataFrame{
		"missing-vector":           func(f []*pb.MigrationDataFrame) []*pb.MigrationDataFrame { return append(f[:6], f[7:]...) },
		"missing-visible-body":     func(f []*pb.MigrationDataFrame) []*pb.MigrationDataFrame { return append(f[:3], f[4:]...) },
		"missing-forward-relation": func(f []*pb.MigrationDataFrame) []*pb.MigrationDataFrame { return append(f[:5], f[6:]...) },
		"wrong-kb":                 func(f []*pb.MigrationDataFrame) []*pb.MigrationDataFrame { f[3].Entry.KbId = "other"; return f },
		"future-mvcc":              func(f []*pb.MigrationDataFrame) []*pb.MigrationDataFrame { f[3].Entry.VersionId = 2; return f },
		"duplicate-document": func(f []*pb.MigrationDataFrame) []*pb.MigrationDataFrame {
			return append(append(f[:3:3], proto.Clone(f[2]).(*pb.MigrationDataFrame)), f[3:]...)
		},
		"wrong-source": func(f []*pb.MigrationDataFrame) []*pb.MigrationDataFrame { f[0].SourceNodeId = 9; return f },
		"tombstone-as-live-document": func(f []*pb.MigrationDataFrame) []*pb.MigrationDataFrame {
			f[3].Entry.Payload = []byte{tagTombstone}
			return f
		},
		"invalid-raw-tombstone": func(f []*pb.MigrationDataFrame) []*pb.MigrationDataFrame {
			f[1].Entry.Payload = []byte{tagTombstone, 1}
			return f
		},
		"reordered-version": func(f []*pb.MigrationDataFrame) []*pb.MigrationDataFrame { f[2].VersionId = 2; return f },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			req, m, frames := migrationCodecFixture()
			frames = withMigrationFooter(t, mutate(frames))
			check := newMigrationStreamCheck(req, m)
			var err error
			for _, f := range frames {
				if _, err = check.consume(f); err != nil {
					break
				}
			}
			if err == nil {
				t.Fatal("invalid semantic data accepted")
			}
		})
	}
}
func TestMigrationCodecRequiresExplicitEndAndNoTrailingFrames(t *testing.T) {
	req, m, frames := migrationCodecFixture()
	check := newMigrationStreamCheck(req, m)
	for _, f := range frames {
		if _, err := check.consume(f); err != nil {
			t.Fatal(err)
		}
	}
	if check.complete {
		t.Fatal("EOF without completion accepted")
	}
	if _, err := check.consume(check.footer()); err != nil {
		t.Fatal(err)
	}
	if !check.complete {
		t.Fatal("valid transfer incomplete")
	}
	if _, err := check.consume(&pb.MigrationDataFrame{VersionId: 1, VersionEnd: true}); err == nil {
		t.Fatal("trailing frame accepted")
	}
}
func TestMigrationCodecRequiresEmptyAndFailedVersionMarkers(t *testing.T) {
	req := &pb.MigrationTransferRequest{KnowledgeBaseId: "kb", OperationId: "move", ExpectedEpoch: 1, SourceNodeId: 1}
	m, err := storage.BuildMigrationManifest("kb", 1, []types.VersionMeta{{KBID: "kb", VersionID: 1, DataStatus: types.DataStatusDurable, DocIDSetHash: types.EmptyDocIDSetHash}, {KBID: "kb", VersionID: 2, ParentVersionID: 1, DataStatus: types.DataStatusFailedPermanent}})
	if err != nil {
		t.Fatal(err)
	}
	frames := []*pb.MigrationDataFrame{{Manifest: m.Bytes(), KnowledgeBaseId: "kb", OperationId: "move", PlacementEpoch: 1, SourceNodeId: 1}, {VersionId: 1, VersionEnd: true}, {VersionId: 2, VersionEnd: true}}
	check := newMigrationStreamCheck(req, m)
	for _, f := range withMigrationFooter(t, frames) {
		if _, err := check.consume(f); err != nil {
			t.Fatal(err)
		}
	}
	if !check.complete {
		t.Fatal("empty/failed markers lost")
	}
}
