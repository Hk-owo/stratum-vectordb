package sync

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"math"

	"google.golang.org/protobuf/proto"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/pebbleutil"
	"stratum/internal/storage"
	"stratum/internal/types"
)

const maxMigrationFrame = 64 << 20

func migrationDataError(detail string) error {
	return fmt.Errorf("%w: migration data: %s", serrors.ErrMigrationConflict, detail)
}
func encodeMigrationFrame(f *pb.MigrationDataFrame) ([]byte, error) {
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(f)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 || len(b) > maxMigrationFrame {
		return nil, migrationDataError("frame exceeds protocol limit")
	}
	return b, nil
}
func hashMigrationFrame(h hash.Hash, b []byte) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(b)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(b)
}

type migrationPair struct{ doc, chunk string }
type migrationVersionCheck struct {
	ref     storage.MigrationVersion
	docs    map[string]bool
	bodies  map[string]bool
	reverse map[migrationPair]bool
	forward map[migrationPair]bool
	chunks  map[string]bool
	vectors map[string]bool
}

func newMigrationVersionCheck(ref storage.MigrationVersion) *migrationVersionCheck {
	return &migrationVersionCheck{ref: ref, docs: map[string]bool{}, bodies: map[string]bool{}, reverse: map[migrationPair]bool{}, forward: map[migrationPair]bool{}, chunks: map[string]bool{}, vectors: map[string]bool{}}
}
func (v *migrationVersionCheck) entry(e *pb.SyncEntry) error {
	bad := func() error { return migrationDataError("invalid, duplicate or out-of-scope record") }
	switch e.GetEntryType() {
	case pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST:
		if e.GetDocId() == "" || e.GetVersionId() != v.ref.ID || e.GetChunkId() != "" || len(e.GetPayload()) != 0 || v.docs[e.DocId] {
			return bad()
		}
		v.docs[e.DocId] = true
	case pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE:
		if !v.docs[e.GetDocId()] || v.bodies[e.DocId] || e.GetVersionId() <= 0 || e.GetVersionId() > v.ref.ID || e.GetChunkId() != "" || len(e.GetPayload()) == 0 || e.Payload[0] != tagContent {
			return bad()
		}
		v.bodies[e.DocId] = true
	case pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_REVERSE, pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_FORWARD:
		if e.GetDocId() == "" || e.GetChunkId() == "" || e.GetVersionId() != 0 || len(e.GetPayload()) != 0 {
			return bad()
		}
		pair := migrationPair{e.DocId, e.ChunkId}
		if e.EntryType == pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_REVERSE {
			if !v.docs[e.DocId] || v.reverse[pair] {
				return bad()
			}
			v.reverse[pair] = true
			v.chunks[e.ChunkId] = true
		} else {
			if !v.chunks[e.ChunkId] || v.forward[pair] {
				return bad()
			}
			v.forward[pair] = true
		}
	case pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_VECTOR:
		if e.GetChunkId() == "" || !v.chunks[e.ChunkId] || v.vectors[e.ChunkId] || e.GetDocId() != "" || e.GetVersionId() != 0 || len(e.Payload) == 0 || len(e.Payload)%4 != 0 {
			return bad()
		}
		for i := 0; i < len(e.Payload); i += 4 {
			f := math.Float32frombits(binary.LittleEndian.Uint32(e.Payload[i:]))
			if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
				return bad()
			}
		}
		v.vectors[e.ChunkId] = true
	default:
		return bad()
	}
	return nil
}
func (v *migrationVersionCheck) finish() error {
	if v.ref.DataStatus == types.DataStatusFailedPermanent {
		if len(v.docs)+len(v.bodies)+len(v.chunks) != 0 {
			return migrationDataError("failed DATA version has records")
		}
		return nil
	}
	docs := make([]string, 0, len(v.docs))
	for doc := range v.docs {
		docs = append(docs, doc)
		if !v.bodies[doc] {
			return migrationDataError("missing document body")
		}
	}
	if ComputeDocIDSetHash(docs) != v.ref.DocIDSetHash {
		return migrationDataError("document set does not match committed digest")
	}
	for pair := range v.reverse {
		if !v.forward[pair] {
			return migrationDataError("missing forward relation")
		}
	}
	for chunk := range v.chunks {
		if !v.vectors[chunk] {
			return migrationDataError("missing required vector")
		}
	}
	return nil
}

type migrationStreamCheck struct {
	request                     *pb.MigrationTransferRequest
	manifest                    storage.MigrationManifest
	digest                      hash.Hash
	header, complete, rawClosed bool
	index                       int
	current                     *migrationVersionCheck
	entries                     uint64
	lastRaw                     []byte
}

func newMigrationStreamCheck(req *pb.MigrationTransferRequest, m storage.MigrationManifest) *migrationStreamCheck {
	return &migrationStreamCheck{request: req, manifest: m, digest: sha256.New()}
}
func (c *migrationStreamCheck) consume(f *pb.MigrationDataFrame) ([]byte, error) {
	if f == nil || c.complete {
		return nil, migrationDataError("record after completion")
	}
	b, err := encodeMigrationFrame(f)
	if err != nil {
		return nil, err
	}
	if !c.header {
		if !bytes.Equal(f.Manifest, c.manifest.Bytes()) || f.GetSourceNodeId() != c.request.SourceNodeId || f.GetOperationId() != c.request.OperationId || f.GetKnowledgeBaseId() != c.request.KnowledgeBaseId || f.GetPlacementEpoch() != c.request.ExpectedEpoch || f.Entry != nil || f.VersionId != 0 || f.VersionEnd || f.Complete || f.PayloadSha256 != "" || f.EntryCount != 0 {
			return nil, migrationDataError("invalid manifest/identity header")
		}
		c.header = true
		hashMigrationFrame(c.digest, b)
		return b, nil
	}
	if len(f.Manifest) != 0 || f.SourceNodeId != 0 || f.OperationId != "" || f.KnowledgeBaseId != "" || f.PlacementEpoch != 0 {
		return nil, migrationDataError("identity changed inside stream")
	}
	if f.Complete {
		if f.Entry != nil || f.VersionId != 0 || f.VersionEnd || c.current != nil || c.index != len(c.manifest.Versions) || f.EntryCount != c.entries || f.PayloadSha256 != hex.EncodeToString(c.digest.Sum(nil)) {
			return nil, migrationDataError("truncated or mismatched completion")
		}
		c.complete = true
		return b, nil
	}
	if f.PayloadSha256 != "" || f.EntryCount != 0 || (f.Entry == nil) != f.VersionEnd {
		return nil, migrationDataError("invalid data frame")
	}
	if f.VersionId == 0 {
		e := f.Entry
		if c.rawClosed || f.VersionEnd || e == nil || e.KbId != c.manifest.KBID || e.EntryType != pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE || e.DocId == "" || e.VersionId <= 0 || e.ChunkId != "" || len(e.Payload) == 0 || (e.Payload[0] != tagContent && e.Payload[0] != tagTombstone) || (e.Payload[0] == tagTombstone && len(e.Payload) != 1) {
			return nil, migrationDataError("invalid raw MVCC record")
		}
		key := append(pebbleutil.EncodeString(e.DocId), pebbleutil.EncodeVersionID(e.VersionId)...)
		if c.lastRaw != nil && bytes.Compare(c.lastRaw, key) >= 0 {
			return nil, migrationDataError("unordered/duplicate raw MVCC record")
		}
		c.lastRaw = key
	} else {
		c.rawClosed = true
		if c.index >= len(c.manifest.Versions) || f.VersionId != c.manifest.Versions[c.index].ID {
			return nil, migrationDataError("missing or reordered retained version")
		}
		if c.current == nil {
			c.current = newMigrationVersionCheck(c.manifest.Versions[c.index])
		}
		if f.VersionEnd {
			if err := c.current.finish(); err != nil {
				return nil, err
			}
			c.current = nil
			c.index++
		} else {
			if f.Entry.KbId != c.manifest.KBID || c.current.ref.DataStatus != types.DataStatusDurable {
				return nil, migrationDataError("cross-KB or failed-version record")
			}
			if err := c.current.entry(f.Entry); err != nil {
				return nil, err
			}
		}
	}
	if f.Entry != nil {
		c.entries++
	}
	hashMigrationFrame(c.digest, b)
	return b, nil
}
func (c *migrationStreamCheck) footer() *pb.MigrationDataFrame {
	return &pb.MigrationDataFrame{Complete: true, PayloadSha256: hex.EncodeToString(c.digest.Sum(nil)), EntryCount: c.entries}
}
