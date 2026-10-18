package sync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	vecstorepb "stratum/api/proto/vecstore"
	"stratum/internal/pebbleutil"
)

// Naming note: "Leader" here is historical. What this type actually is, is the
// node's *export* side — the half of DataSyncService that hands this node's
// data to a peer — and it works the same whether or not this node leads. The
// name survives because it appears in a lot of call sites; renaming it is a
// pure cleanup, tracked in Stratum_设计文档v13.md §7.10.
//
// LeaderHandler implements the server side of DataSyncService. It is
// registered on the leader's gRPC server so followers can pull
// per-version storage-layer data. It reads directly from the local
// PebbleDB instances (docstore, chunkdoc, versiondoc) and the local
// vecstore gRPC client.
// entrySink accepts one version's records as they stream out. Both the gRPC
// pull stream (server side) and the push stream (client side) satisfy it,
// which is what lets the pull and push paths share the export logic below.
type entrySink interface {
	Send(*pb.SyncEntry) error
}

// entrySinkFunc adapts a plain function to entrySink.
type entrySinkFunc func(*pb.SyncEntry) error

func (f entrySinkFunc) Send(entry *pb.SyncEntry) error { return f(entry) }

type LeaderHandler struct {
	pb.UnimplementedDataSyncServiceServer

	docstoreDB   *pebble.DB
	chunkdocDB   *pebble.DB
	versiondocDB *pebble.DB
	vecstore     vecstorepb.ChunkStorageServiceClient
}

// NewLeaderHandler constructs a LeaderHandler. The caller retains
// ownership of the DB handles and the vecstore client; LeaderHandler
// does not close them.
func NewLeaderHandler(
	docstoreDB, chunkdocDB, versiondocDB *pebble.DB,
	vecstoreClient vecstorepb.ChunkStorageServiceClient,
) *LeaderHandler {
	return &LeaderHandler{
		docstoreDB:   docstoreDB,
		chunkdocDB:   chunkdocDB,
		versiondocDB: versiondocDB,
		vecstore:     vecstoreClient,
	}
}

// PullVersionData streams all storage-layer data for (kbID, versionID)
// to the follower via server-side streaming. The stream covers, in
// order: VersionDocList, DocStore, ChunkDocMapper (forward),
// ChunkDocMapper (reverse), ChunkStore vectors.
func (h *LeaderHandler) PullVersionData(
	req *pb.PullVersionDataRequest,
	stream pb.DataSyncService_PullVersionDataServer,
) error {
	return h.ExportVersion(stream.Context(), req.GetKnowledgeBaseId(), req.GetVersionId(), stream.Send)
}

// ExportVersion hands one version's records to send, in exactly the order and
// shape PullVersionData produces. The push path drives it with a sink that
// forwards to the target replica's PushVersionData stream, so both directions
// export through identical logic.
func (h *LeaderHandler) ExportVersion(ctx context.Context, kbID string, versionID int64, send func(*pb.SyncEntry) error) error {
	return h.exportVersion(kbID, versionID, entrySinkFunc(send))
}

func (h *LeaderHandler) exportVersion(kbID string, versionID int64, stream entrySink) error {
	// 1. VersionDocList: stream all (kbID, versionID, docID) entries.
	docIDs, err := h.streamVersionDocList(kbID, versionID, stream)
	if err != nil {
		return fmt.Errorf("sync: stream VersionDocList: %w", err)
	}
	if len(docIDs) == 0 {
		return nil // empty version, nothing else to sync
	}

	// 2. DocStore: for each docID, find the entry with largest
	//    versionID <= versionID and stream it (including tombstone
	//    markers so the follower can reconstruct MVCC state).
	if err := h.streamDocStore(kbID, versionID, docIDs, stream); err != nil {
		return fmt.Errorf("sync: stream DocStore: %w", err)
	}

	// 3. ChunkDocMapper reverse: docID → chunkID list.
	chunkSet, err := h.streamChunkDocReverse(kbID, docIDs, stream)
	if err != nil {
		return fmt.Errorf("sync: stream ChunkDocMapper reverse: %w", err)
	}

	// Convert set to slice for the next steps.
	chunkIDs := make([]string, 0, len(chunkSet))
	for chunkID := range chunkSet {
		chunkIDs = append(chunkIDs, chunkID)
	}

	// 4. ChunkDocMapper forward: chunkID → docID.
	if err := h.streamChunkDocForward(kbID, chunkIDs, stream); err != nil {
		return fmt.Errorf("sync: stream ChunkDocMapper forward: %w", err)
	}

	// 5. ChunkStore vectors.
	if err := h.streamChunkVectors(kbID, chunkIDs, stream); err != nil {
		return fmt.Errorf("sync: stream ChunkStore vectors: %w", err)
	}

	return nil
}

// streamVersionDocList scans the versiondoc PebbleDB for all docIDs
// belonging to (kbID, versionID). Returns the collected docIDs for
// downstream steps.
func (h *LeaderHandler) streamVersionDocList(
	kbID string, versionID int64,
	stream entrySink,
) ([]string, error) {
	prefix := append(
		pebbleutil.EncodeString(kbID),
		pebbleutil.EncodeVersionID(versionID)...,
	)
	upper := pebbleutil.PrefixSuccessor(prefix)

	iter, err := h.versiondocDB.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var docIDs []string
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		// key = EncodeString(kbID) + EncodeVersionID(versionID) + EncodeString(docID)
		kbLen := 4 + len(kbID)
		prefixLen := kbLen + 8 // 8 bytes for versionID
		if len(key) <= prefixLen {
			continue
		}
		docID := pebbleutil.DecodeString(key[prefixLen:])

		docIDs = append(docIDs, docID)

		if err := stream.Send(&pb.SyncEntry{
			EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST,
			KbId:      kbID,
			DocId:     docID,
			VersionId: versionID,
		}); err != nil {
			return nil, err
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return docIDs, nil
}

// versionScopedUpperBound returns the exclusive upper bound for one document's
// keys restricted to versionIDs ≤ versionID: the document prefix followed by the
// encoded versionID+1. This is the same bound docstore.ReadAt uses, and the
// reason both need it is the same — the keyspace orders by (kb, doc, version), so
// only a version-suffixed bound excludes later versions of the SAME document.
func versionScopedUpperBound(docPrefix []byte, versionID int64) []byte {
	if versionID == math.MaxInt64 {
		// Unreachable in practice (version ids come from a Raft counter that
		// starts at 1), and versionID+1 would overflow into a bound that admits
		// everything. Degrading to the document prefix keeps that theoretical case
		// from turning into a wrong answer.
		return pebbleutil.PrefixSuccessor(docPrefix)
	}
	upper := make([]byte, 0, len(docPrefix)+8)
	upper = append(upper, docPrefix...)
	return append(upper, pebbleutil.EncodeVersionID(versionID+1)...)
}

// streamDocStore scans the leader's docstore for each docID and streams
// the visible entry at versionID (largest version ≤ versionID).
func (h *LeaderHandler) streamDocStore(
	kbID string, versionID int64, docIDs []string,
	stream entrySink,
) error {
	kbPrefix := pebbleutil.EncodeString(kbID)

	for _, docID := range docIDs {
		docPrefix := append(kbPrefix, pebbleutil.EncodeString(docID)...)
		// The bound has to be "the first key past this version", not "the
		// successor of the document prefix". Bounded by the prefix successor,
		// SeekLT lands on the document's NEWEST entry; when the document was
		// updated after versionID, the version check that used to sit below then
		// dropped it and the export shipped no record for it at all (C4 of
		// docs/code-review-2026-10-18.md). Pulling a middle version — which the
		// gap backfill does for every version between this node's cursor and the
		// one being applied — therefore produced a version whose document list
		// was complete and whose bodies were missing, and the follower advanced
		// its cursor over it, so nothing ever asked again.
		upper := versionScopedUpperBound(docPrefix, versionID)

		iter, err := h.docstoreDB.NewIter(&pebble.IterOptions{
			LowerBound: docPrefix,
			UpperBound: upper,
		})
		if err != nil {
			return err
		}

		// The bound already restricts the range to versions ≤ versionID, so the
		// last key inside it IS the entry to send.
		var foundKey []byte
		var foundVal []byte
		var foundVersion int64
		if iter.SeekLT(upper) {
			k := iter.Key()
			if ver, ok := pebbleutil.DecodeVersionID(k[len(docPrefix):]); ok {
				v, err := iter.ValueAndErr()
				if err != nil {
					iter.Close()
					return err
				}
				foundVersion = ver
				foundKey = append([]byte(nil), k...)
				foundVal = append([]byte(nil), v...)
			}
		}
		iter.Close()

		if foundKey == nil {
			// The version's document list names this document, so having no entry
			// for it at or before versionID means the two stores disagree — not
			// that the document is absent. Saying so is the point: what used to
			// happen here was shipping the version without the document, which the
			// follower could not tell apart from a version that really lacks it.
			return fmt.Errorf(
				"sync: stream DocStore: %s/%s has no entry at or before version %d, but the version's document list includes it",
				kbID, docID, versionID)
		}

		// The raw value carries docstore's internal tag byte
		// (tagContent 0x01 / tagTombstone 0x00). The wire format
		// PRESERVES the tag verbatim so the follower can reconstruct the
		// MVCC state losslessly — critically, the distinction between a
		// live document with empty content ({0x01}) and a tombstone
		// ({0x00}), which proto3 bytes fields cannot express with a bare
		// semantic payload (empty and absent both decode to nil). The
		// follower strips the tag before writing to its own store.
		if len(foundVal) == 0 {
			return fmt.Errorf("sync: stream DocStore: empty stored value for %s/%s (missing tag byte)", kbID, docID)
		}
		switch foundVal[0] {
		case tagTombstone, tagContent:
			// pass through verbatim
		default:
			return fmt.Errorf("sync: stream DocStore: unknown value tag 0x%02x for %s/%s", foundVal[0], kbID, docID)
		}
		payload := foundVal

		if err := stream.Send(&pb.SyncEntry{
			EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE,
			KbId:      kbID,
			DocId:     docID,
			VersionId: foundVersion,
			Payload:   payload,
		}); err != nil {
			return err
		}
	}
	return nil
}

// streamChunkDocReverse streams the reverse ChunkDocMapper entries
// (docID → chunkID) for the given docIDs. Returns the set of all unique
// chunkIDs encountered.
func (h *LeaderHandler) streamChunkDocReverse(
	kbID string, docIDs []string,
	stream entrySink,
) (map[string]bool, error) {
	chunkSet := make(map[string]bool)
	kbPrefix := pebbleutil.EncodeString(kbID)

	for _, docID := range docIDs {
		prefix := append(append([]byte{'R'},
			kbPrefix...),
			pebbleutil.EncodeString(docID)...)
		upper := pebbleutil.PrefixSuccessor(prefix)

		iter, err := h.chunkdocDB.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upper,
		})
		if err != nil {
			return nil, err
		}

		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			// key = 'R' + EncodeString(kbID) + EncodeString(docID) + EncodeString(chunkID)
			kbLen := 4 + len(kbID)
			docLen := 4 + len(docID)
			chunkStart := 1 + kbLen + docLen // 1 for 'R'
			if len(key) <= chunkStart {
				continue
			}
			chunkID := pebbleutil.DecodeString(key[chunkStart:])
			chunkSet[chunkID] = true

			if err := stream.Send(&pb.SyncEntry{
				EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_REVERSE,
				KbId:      kbID,
				DocId:     docID,
				ChunkId:   chunkID,
			}); err != nil {
				return nil, err
			}
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return nil, err
		}
		iter.Close()
	}
	return chunkSet, nil
}

// streamChunkDocForward streams the forward ChunkDocMapper entries
// (chunkID → docID) extracted from the chunkdoc PebbleDB for each
// chunkID.
func (h *LeaderHandler) streamChunkDocForward(
	kbID string, chunkIDs []string,
	stream entrySink,
) error {
	kbPrefix := pebbleutil.EncodeString(kbID)

	for _, chunkID := range chunkIDs {
		prefix := append(append([]byte{'F'},
			kbPrefix...),
			pebbleutil.EncodeString(chunkID)...)
		upper := pebbleutil.PrefixSuccessor(prefix)

		iter, err := h.chunkdocDB.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upper,
		})
		if err != nil {
			return err
		}

		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			kbLen := 4 + len(kbID)
			chunkLen := 4 + len(chunkID)
			docStart := 1 + kbLen + chunkLen // 1 for 'F'
			if len(key) <= docStart {
				continue
			}
			docID := pebbleutil.DecodeString(key[docStart:])

			if err := stream.Send(&pb.SyncEntry{
				EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_FORWARD,
				KbId:      kbID,
				DocId:     docID,
				ChunkId:   chunkID,
			}); err != nil {
				return err
			}
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return err
		}
		iter.Close()
	}
	return nil
}

// streamChunkVectors reads each chunk vector from the vecstore and
// streams it.
func (h *LeaderHandler) streamChunkVectors(
	kbID string, chunkIDs []string,
	stream entrySink,
) error {
	for _, chunkID := range chunkIDs {
		resp, err := h.vecstore.Read(context.Background(), &vecstorepb.ReadChunkRequest{
			Key: encodeVecstoreKey(kbID, chunkID),
		})
		if err != nil {
			if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
				continue // chunk might have been GC'd on leader
			}
			return fmt.Errorf("vecstore Read(%s, %s): %w", kbID, chunkID, err)
		}

		// Serialize []float32 as raw bytes (little-endian).
		vector := resp.GetVector()
		payload := float32sToBytes(vector)

		if err := stream.Send(&pb.SyncEntry{
			EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_VECTOR,
			KbId:      kbID,
			ChunkId:   chunkID,
			Payload:   payload,
		}); err != nil {
			return err
		}
	}
	return nil
}

// Vecstore key encoding mirrors chunkstore.encodeKey. Must stay in sync.
func encodeVecstoreKey(kbID, chunkID string) string {
	b := make([]byte, 4+len(kbID))
	n := len(kbID)
	b[0] = byte(n >> 24)
	b[1] = byte(n >> 16)
	b[2] = byte(n >> 8)
	b[3] = byte(n)
	copy(b[4:], kbID)
	return string(b) + chunkID
}

// docstore value tags, mirroring internal/docstore's private
// tagTombstone/tagContent. The sync wire format for a DOC_STORE payload
// is the *semantic* content (raw doc text, or empty for a tombstone), so
// the leader must strip the tag byte before streaming. Must stay in sync
// with docstore.encodeValue.
const (
	tagTombstone byte = 0x00
	tagContent   byte = 0x01
)

// float32sToBytes serializes []float32 as raw IEEE-754 bytes
// (little-endian bit patterns, one float32 per 4 bytes).
func float32sToBytes(v []float32) []byte {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

// bytesToFloat32s deserializes raw bytes back to []float32.
func bytesToFloat32s(b []byte) []float32 {
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return v
}

// Compile-time check.
var _ pb.DataSyncServiceServer = (*LeaderHandler)(nil)
