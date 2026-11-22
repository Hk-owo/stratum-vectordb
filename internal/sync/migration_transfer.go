package sync

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/fsutil"
	"stratum/internal/pebbleutil"
	"stratum/internal/rpcpool"
	"stratum/internal/storage"
	"stratum/internal/types"
)

type MigrationMetadata interface {
	storage.MigrationTransferReader
}
type migrationStager struct {
	syncDir   func(string) error
	metadata  MigrationMetadata
	dir       string
	sem       chan struct{}
	serveWait time.Duration
	pool      *rpcpool.Pool
	dial      func(context.Context, string) (*grpc.ClientConn, error)
}

func WithMigrationStaging(metadata MigrationMetadata, dir string) NodeHandlerOption {
	return func(h *NodeHandler) {
		if h.migration == nil {
			h.migration = &migrationStager{sem: make(chan struct{}, 1)}
		}
		h.migration.metadata = metadata
		h.migration.dir = dir
	}
}

// WithMigrationPool binds a shared outbound storage-peer connection pool for
// streaming KB migration exports from peer storage nodes.
func WithMigrationPool(pool *rpcpool.Pool) NodeHandlerOption {
	return func(h *NodeHandler) {
		if h.migration == nil {
			h.migration = &migrationStager{sem: make(chan struct{}, 1)}
		}
		h.migration.pool = pool
	}
}

func (h *NodeHandler) migrationView(ctx context.Context, req *pb.MigrationTransferRequest, target bool) (storage.Directory, storage.Migration, storage.MigrationManifest, error) {
	return h.migrationViewFor(ctx, req, target, false)
}

// migrationViewFor is migrationView with an explicit phase window. Installation
// needs the INSTALLED phase as well, because a repeated request must be answered
// from the committed task rather than refused as stale.
func (h *NodeHandler) migrationViewFor(ctx context.Context, req *pb.MigrationTransferRequest, target, allowInstalled bool) (storage.Directory, storage.Migration, storage.MigrationManifest, error) {
	var d storage.Directory
	var m storage.Migration
	var manifest storage.MigrationManifest
	if h.migration == nil || h.migration.metadata == nil || h.receiver == nil {
		return d, m, manifest, serrors.ErrPlacementChangeUnsafe
	}
	if req == nil || req.KnowledgeBaseId == "" || req.OperationId == "" || req.ExpectedEpoch == 0 || req.SourceNodeId <= 0 {
		return d, m, manifest, serrors.ErrInvalidArgument
	}
	view, err := h.migration.metadata.GetKBMigrationTransfer(ctx, req.OperationId)
	d = view.Directory
	if err != nil {
		return d, m, manifest, err
	}
	m, ok := d.Migrations[req.OperationId]
	if !ok {
		return d, m, manifest, serrors.ErrMigrationNotFound
	}
	if m.Request.KBID != req.KnowledgeBaseId || m.Request.ExpectedEpoch != req.ExpectedEpoch {
		return d, m, manifest, serrors.ErrPlacementStale
	}
	if m.Phase != storage.MigrationFenced && m.Phase != storage.MigrationStaged && !(allowInstalled && m.Phase == storage.MigrationInstalled) {
		return d, m, manifest, serrors.ErrMigrationConflict
	}
	if err := d.ValidateFailedBlock(m); err != nil {
		return d, m, manifest, err
	}
	snap, err := d.Resolve(req.KnowledgeBaseId)
	if err != nil {
		return d, m, manifest, err
	}
	if snap.Placement.Epoch != req.ExpectedEpoch || !slices.Equal(snap.Placement.ReplicaIDs, m.Old.ReplicaIDs) || snap.Placement.RequiredReplicas != m.Old.RequiredReplicas {
		return d, m, manifest, serrors.ErrPlacementStale
	}
	if !m.Old.Contains(req.SourceNodeId) || !slices.Contains(m.FencedIDs, req.SourceNodeId) || (m.Request.Fault && m.Request.FromID == req.SourceNodeId) {
		return d, m, manifest, serrors.ErrNotKBReplica
	}
	for _, id := range m.RequiredFenceIDs() {
		if !slices.Contains(m.FencedIDs, id) {
			return d, m, manifest, serrors.ErrMigrationBusy
		}
	}
	if target {
		if h.receiver.nodeID != m.Request.ToID {
			return d, m, manifest, serrors.ErrNotKBReplica
		}
	} else if h.receiver.nodeID != req.SourceNodeId {
		return d, m, manifest, serrors.ErrNotKBReplica
	}
	manifest = view.Manifest
	if manifest.KBID != req.KnowledgeBaseId || manifest.ActiveVersion != snap.ActiveVersion {
		return d, m, manifest, serrors.ErrMigrationConflict
	}
	return d, m, manifest, err
}

// ExportKBMigration is intentionally stricter than ordinary catch-up. Missing
// bodies, relations or required vectors prevent a completion frame and receipt.
func (h *NodeHandler) ExportKBMigration(req *pb.MigrationTransferRequest, stream pb.DataSyncService_ExportKBMigrationServer) error {
	ctx := stream.Context()
	_, _, manifest, err := h.migrationView(ctx, req, false)
	if err != nil {
		return serrors.ToGRPCStatus(err)
	}
	if h.exporter == nil {
		return serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
	}
	check := newMigrationStreamCheck(req, manifest)
	send := func(f *pb.MigrationDataFrame) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := check.consume(f); err != nil {
			return err
		}
		return stream.Send(f)
	}
	if err := send(&pb.MigrationDataFrame{Manifest: manifest.Bytes(), SourceNodeId: h.receiver.nodeID, OperationId: req.OperationId, KnowledgeBaseId: req.KnowledgeBaseId, PlacementEpoch: req.ExpectedEpoch}); err != nil {
		return serrors.ToGRPCStatus(err)
	}
	if err := h.exporter.exportMigrationMVCC(ctx, req.KnowledgeBaseId, func(e *pb.SyncEntry) error { return send(&pb.MigrationDataFrame{Entry: e}) }); err != nil {
		return serrors.ToGRPCStatus(err)
	}
	requiredDocs := map[string]bool{}
	requiredReverse := map[migrationPair]bool{}
	for _, version := range manifest.Versions {
		if version.DataStatus == types.DataStatusDurable {
			if err := h.exporter.ExportVersion(ctx, manifest.KBID, version.ID, func(e *pb.SyncEntry) error { return send(&pb.MigrationDataFrame{VersionId: version.ID, Entry: e}) }); err != nil {
				return serrors.ToGRPCStatus(err)
			}
		}
		if check.current != nil {
			for doc := range check.current.docs {
				requiredDocs[doc] = true
			}
			for pair := range check.current.reverse {
				requiredReverse[pair] = true
			}
		}
		if err := send(&pb.MigrationDataFrame{VersionId: version.ID, VersionEnd: true}); err != nil {
			return serrors.ToGRPCStatus(err)
		}
	}
	if err := h.exporter.verifyMigrationForwardRelations(ctx, manifest.KBID, requiredDocs, requiredReverse); err != nil {
		return serrors.ToGRPCStatus(err)
	}
	_, _, latest, err := h.migrationView(ctx, req, false)
	if err != nil {
		return serrors.ToGRPCStatus(err)
	}
	if latest.Hash() != manifest.Hash() {
		return serrors.ToGRPCStatus(serrors.ErrPlacementStale)
	}
	return serrors.ToGRPCStatus(send(check.footer()))
}

func (h *LeaderHandler) exportMigrationMVCC(ctx context.Context, kb string, send func(*pb.SyncEntry) error) error {
	prefix := pebbleutil.EncodeString(kb)
	iter, err := h.docstoreDB.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: pebbleutil.PrefixSuccessor(prefix)})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := iter.Key()
		offset := len(prefix)
		if len(key) < offset+4+8 {
			return migrationDataError("malformed MVCC key")
		}
		length := uint64(binary.BigEndian.Uint32(key[offset : offset+4]))
		offset += 4
		if length == 0 || length > uint64(len(key)-offset-8) || offset+int(length)+8 != len(key) {
			return migrationDataError("malformed MVCC document identity")
		}
		doc := string(key[offset : offset+int(length)])
		offset += int(length)
		id := int64(binary.BigEndian.Uint64(key[offset:]))
		if err := send(&pb.SyncEntry{EntryType: pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE, KbId: kb, DocId: doc, VersionId: id, Payload: append([]byte(nil), iter.Value()...)}); err != nil {
			return err
		}
	}
	return iter.Error()
}

func migrationStagePath(root, kb, op string) string {
	sum := sha256.Sum256(append(pebbleutil.EncodeString(kb), pebbleutil.EncodeString(op)...))
	return filepath.Join(root, hex.EncodeToString(sum[:])+".stage")
}
func writeMigrationFrame(w io.Writer, b []byte) error {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(b)))
	if _, err := w.Write(n[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}
func readMigrationFrame(r io.Reader) (*pb.MigrationDataFrame, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(n[:])
	if length == 0 || length > maxMigrationFrame {
		return nil, migrationDataError("invalid artifact frame length")
	}
	b := make([]byte, int(length))
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	frame := new(pb.MigrationDataFrame)
	if err := proto.Unmarshal(b, frame); err != nil {
		return nil, err
	}
	return frame, nil
}
func validateMigrationStage(ctx context.Context, path string, req *pb.MigrationTransferRequest, manifest storage.MigrationManifest, target int64) (storage.MigrationStageReceipt, error) {
	var receipt storage.MigrationStageReceipt
	stat, err := os.Lstat(path)
	if err != nil {
		return receipt, err
	}
	if !stat.Mode().IsRegular() {
		return receipt, migrationDataError("staging artifact is not a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return receipt, err
	}
	defer f.Close()
	return validateMigrationFile(ctx, f, req, manifest, target, func() error { return ctx.Err() })
}

// The installer verifies and rewinds this same open descriptor before replaying
// it, so a pathname replacement cannot substitute an unverified artifact.
func validateMigrationFile(ctx context.Context, f *os.File, req *pb.MigrationTransferRequest, manifest storage.MigrationManifest, target int64, progress func() error) (storage.MigrationStageReceipt, error) {
	var receipt storage.MigrationStageReceipt
	stat, err := f.Stat()
	if err != nil {
		return receipt, err
	}
	if !stat.Mode().IsRegular() {
		return receipt, migrationDataError("staging artifact is not a regular file")
	}
	check := newMigrationStreamCheck(req, manifest)
	reader := bufio.NewReader(f)
	for {
		if err := ctx.Err(); err != nil {
			return receipt, err
		}
		if err := progress(); err != nil {
			return receipt, err
		}
		frame, err := readMigrationFrame(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return receipt, err
		}
		if _, err := check.consume(frame); err != nil {
			return receipt, err
		}
	}
	if !check.complete {
		return receipt, migrationDataError("incomplete artifact")
	}
	return storage.MigrationStageReceipt{SourceID: req.SourceNodeId, TargetID: target, ManifestHash: manifest.Hash(), PayloadHash: hex.EncodeToString(check.digest.Sum(nil)), Entries: check.entries, Bytes: uint64(stat.Size())}, nil
}

func (h *NodeHandler) StageKBMigration(ctx context.Context, req *pb.MigrationTransferRequest) (*pb.MigrationStageReceipt, error) {
	if h.migration == nil || h.migration.dir == "" || h.exporter == nil {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
	}
	select {
	case h.migration.sem <- struct{}{}:
		defer func() { <-h.migration.sem }()
	case <-ctx.Done():
		return nil, serrors.ToGRPCStatus(ctx.Err())
	}
	if err := h.migration.prepareDirectory(); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	d, _, manifest, err := h.migrationView(ctx, req, true)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	path := migrationStagePath(h.migration.dir, req.KnowledgeBaseId, req.OperationId)
	receipt, err := validateMigrationStage(ctx, path, req, manifest, h.receiver.nodeID)
	if err == nil {
		// A prior rename may have succeeded while directory fsync failed.
		// Revalidate durability on retries rather than turning that failure
		// into a successful acknowledgement merely because a file exists.
		file, e := os.OpenFile(path, os.O_RDWR, 0600)
		if e != nil {
			return nil, e
		}
		e = file.Sync()
		closeErr := file.Close()
		if e != nil {
			return nil, e
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if e := h.migration.syncDirectory(filepath.Dir(h.migration.dir)); e != nil {
			return nil, e
		}
		if e := h.migration.syncDirectory(h.migration.dir); e != nil {
			return nil, e
		}
		_, _, latest, e := h.migrationView(ctx, req, true)
		if e != nil {
			return nil, serrors.ToGRPCStatus(e)
		}
		if latest.Hash() != manifest.Hash() {
			return nil, serrors.ToGRPCStatus(serrors.ErrPlacementStale)
		}
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		return stageReceiptToProto(receipt), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, serrors.ToGRPCStatus(fmt.Errorf("%w: existing staging artifact: %v", serrors.ErrMigrationConflict, err))
	}
	if err := os.MkdirAll(h.migration.dir, 0700); err != nil {
		return nil, err
	}
	if err := h.migration.syncDirectory(filepath.Dir(h.migration.dir)); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(h.migration.dir, ".transfer-*.tmp")
	if err != nil {
		return nil, err
	}
	temporary := f.Name()
	defer os.Remove(temporary)
	defer f.Close()
	writer := bufio.NewWriterSize(f, 64<<10)
	source, ok := d.Nodes[req.SourceNodeId]
	if !ok || source.Addr == "" {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementUnknown)
	}
	conn, release, err := rpcpool.Acquire(ctx, h.migration.pool, h.migration.dial, nil, source.Addr)
	if err != nil {
		return nil, err
	}
	defer release()
	stream, err := pb.NewDataSyncServiceClient(conn).ExportKBMigration(ctx, req, grpc.MaxCallRecvMsgSize(maxMigrationFrame))
	if err != nil {
		return nil, err
	}
	check := newMigrationStreamCheck(req, manifest)
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, err := check.consume(frame)
		if err != nil {
			return nil, serrors.ToGRPCStatus(err)
		}
		if err := writeMigrationFrame(writer, b); err != nil {
			return nil, err
		}
	}
	if !check.complete {
		return nil, serrors.ToGRPCStatus(migrationDataError("source ended without completion"))
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	_, _, latest, err := h.migrationView(ctx, req, true)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if latest.Hash() != manifest.Hash() {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementStale)
	}
	// Verify the durable file itself, not just the received in-memory stream.
	receipt, err = validateMigrationStage(ctx, temporary, req, manifest, h.receiver.nodeID)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.Rename(temporary, path); err != nil {
		return nil, err
	}
	if err := h.migration.syncDirectory(h.migration.dir); err != nil {
		return nil, err
	}
	return stageReceiptToProto(receipt), nil
}
func stageReceiptToProto(r storage.MigrationStageReceipt) *pb.MigrationStageReceipt {
	return &pb.MigrationStageReceipt{SourceNodeId: r.SourceID, TargetNodeId: r.TargetID, ManifestSha256: r.ManifestHash, PayloadSha256: r.PayloadHash, EntryCount: r.Entries, ByteCount: r.Bytes}
}
func stageReceiptFromProto(r *pb.MigrationStageReceipt) storage.MigrationStageReceipt {
	return storage.MigrationStageReceipt{SourceID: r.GetSourceNodeId(), TargetID: r.GetTargetNodeId(), ManifestHash: r.GetManifestSha256(), PayloadHash: r.GetPayloadSha256(), Entries: r.GetEntryCount(), Bytes: r.GetByteCount()}
}

func (s *migrationStager) syncDirectory(path string) error {
	if s.syncDir != nil {
		return s.syncDir(path)
	}
	return fsutil.SyncDir(path)
}

// Ordinary export starts from reverse mappings. Independently scan forward
// mappings too, so losing a reverse row cannot silently hide a required chunk.
func (h *LeaderHandler) verifyMigrationForwardRelations(ctx context.Context, kb string, docs map[string]bool, reverse map[migrationPair]bool) error {
	prefix := append([]byte{'F'}, pebbleutil.EncodeString(kb)...)
	iter, err := h.chunkdocDB.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: pebbleutil.PrefixSuccessor(prefix)})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := iter.Key()
		offset := len(prefix)
		decode := func() (string, error) {
			if len(key)-offset < 4 {
				return "", migrationDataError("malformed forward relation")
			}
			n := uint64(binary.BigEndian.Uint32(key[offset:]))
			offset += 4
			if n == 0 || n > uint64(len(key)-offset) {
				return "", migrationDataError("malformed forward relation identity")
			}
			s := string(key[offset : offset+int(n)])
			offset += int(n)
			return s, nil
		}
		chunk, err := decode()
		if err != nil {
			return err
		}
		doc, err := decode()
		if err != nil {
			return err
		}
		if offset != len(key) {
			return migrationDataError("trailing forward relation key bytes")
		}
		if docs[doc] && !reverse[migrationPair{doc: doc, chunk: chunk}] {
			return migrationDataError("missing reverse relation for a retained document")
		}
	}
	return iter.Error()
}
