package sync

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/fsutil"
	"stratum/internal/storage"
	"stratum/internal/types"
)

// Installing a staged artifact is the target's own work: it replays the verified
// frame sequence into its ordinary DocStore/VersionDoc/ChunkDoc/ChunkStore, then
// re-reads those stores and proves the result matches the frozen DATA manifest.
//
// Three boundaries stay where the earlier batches put them:
//
//   - Installation never changes placement. The effective replica set and epoch
//     are the same before and after; the target admits no ordinary data write.
//   - A committed install receipt is not a serving condition. Index readiness,
//     cursor/bloom state and query admission are separate decisions.
//   - Installation is driven by the control leader (like staging), so a target
//     cannot install a task the control layer has cancelled or replaced.
//
// The local record below is what makes a repeated request idempotent across
// process restarts: replays are harmless (every store write is idempotent), but
// the record is the durable evidence that the verification actually ran.

const installRecordSuffix = ".installed"

// installCheckpoint is how often a long installation re-reads the task from the
// control layer, so a cancellation or a replaced epoch stops the work instead of
// finishing an artifact nobody will use.
const installCheckpoint = 5 * time.Second

type migrationInstallRecord struct {
	Receipt     storage.MigrationInstallReceipt `json:"receipt"`
	OperationID string                          `json:"operation_id"`
	KBID        string                          `json:"kb_id"`
	Epoch       uint64                          `json:"epoch"`
}

func migrationInstallPath(root, kb, op string) string {
	return migrationStagePath(root, kb, op) + installRecordSuffix
}

func readInstallRecord(path string) (migrationInstallRecord, error) {
	var record migrationInstallRecord
	data, err := os.ReadFile(path)
	if err != nil {
		return record, err
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return record, migrationDataError("corrupt installation record")
	}
	if record.OperationID == "" || record.KBID == "" || record.Epoch == 0 || record.Receipt.TargetID <= 0 {
		return record, migrationDataError("invalid installation record")
	}
	return record, nil
}

// InstallKBMigration writes the staged artifact into this node's own stores.
// Only the task's target may call it; only the control leader drives it.
func (h *NodeHandler) InstallKBMigration(ctx context.Context, req *pb.MigrationTransferRequest) (*pb.MigrationInstallReceipt, error) {
	if h.migration == nil || h.migration.dir == "" || h.receiver == nil || h.receiver.follower == nil || h.exporter == nil {
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
	d, task, manifest, err := h.migrationViewFor(ctx, req, true, true)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	target, ok := d.Nodes[task.Request.ToID]
	if !ok || target.ID != h.receiver.nodeID {
		return nil, serrors.ToGRPCStatus(serrors.ErrNotKBReplica)
	}
	if task.Stage == nil {
		return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
	}
	path := migrationInstallPath(h.migration.dir, req.KnowledgeBaseId, req.OperationId)
	record, recordErr := readInstallRecord(path)
	if recordErr != nil && !errors.Is(recordErr, os.ErrNotExist) {
		return nil, serrors.ToGRPCStatus(recordErr)
	}

	// An already-recorded installation is still replayed and re-verified: the
	// record alone cannot prove the stores were not damaged afterwards, and every
	// write it performs is idempotent. A receipt that disagrees with the control
	// layer's committed one is a conflict, never an upgrade.
	if task.Phase == storage.MigrationInstalled && task.Install == nil {
		return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
	}
	if recordErr == nil {
		if record.OperationID != req.OperationId || record.KBID != req.KnowledgeBaseId || record.Epoch != req.ExpectedEpoch || record.Receipt.TargetID != target.ID {
			return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
		}
	}

	// The gate below is the ONLY way a non-member enters a data mutation: it
	// checks the local service authorization and the durable fence floor, and it
	// drains against a concurrent fence/GC of the same knowledge base. Membership
	// is checked by the task itself, not by the ordinary replica predicate.
	importCtx, release, err := h.receiver.enterMigrationImport(ctx, req.KnowledgeBaseId, req.ExpectedEpoch, req.OperationId)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	defer release()
	ctx = importCtx
	// Admission may have waited behind a cancelled/replaced task's work.
	_, currentTask, currentManifest, err := h.migrationViewFor(ctx, req, true, true)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if (currentTask.Phase != storage.MigrationStaged && currentTask.Phase != storage.MigrationInstalled) || currentManifest.Hash() != manifest.Hash() || currentTask.Stage == nil || *currentTask.Stage != *task.Stage {
		return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
	}
	task = currentTask
	if indexes, ok := h.receiver.serving.(MigrationIndexMaintenance); ok {
		indexes.ResumeKnowledgeBase(req.KnowledgeBaseId)
	}

	stagePath := migrationStagePath(h.migration.dir, req.KnowledgeBaseId, req.OperationId)
	stat, err := os.Lstat(stagePath)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if !stat.Mode().IsRegular() {
		return nil, serrors.ToGRPCStatus(migrationDataError("staging artifact is not a regular file"))
	}
	follow, err := os.Open(stagePath)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	defer follow.Close()

	lastCheck := time.Now()
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Since(lastCheck) < installCheckpoint {
			return nil
		}
		lastCheck = time.Now()
		_, current, latest, err := h.migrationViewFor(ctx, req, true, true)
		if err != nil {
			return err
		}
		if (current.Phase != storage.MigrationStaged && current.Phase != storage.MigrationInstalled) || latest.Hash() != manifest.Hash() {
			return serrors.ErrMigrationConflict
		}
		return nil
	}
	// Complete preflight precedes EVERY ordinary-store write, including footer,
	// EOF and the exact payload committed in Raft (self-consistency is not enough).
	verified, err := validateMigrationFile(ctx, follow, req, manifest, target.ID, check)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if verified != *task.Stage {
		return nil, serrors.ToGRPCStatus(migrationDataError("artifact does not match committed staging receipt"))
	}
	if _, err := follow.Seek(0, io.SeekStart); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	// Before touching ordinary stores, refuse a target that already holds data
	// this task did not put there: installation may complete an interrupted
	// attempt, never absorb or overwrite an unrelated replica's data.
	if err := h.receiver.follower.checkMigrationTarget(ctx, manifest); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	checker := newMigrationStreamCheck(req, manifest)
	reader := bufio.NewReader(follow)
	var entries uint64
	for {
		frame, err := readMigrationFrame(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, serrors.ToGRPCStatus(err)
		}
		if _, err := checker.consume(frame); err != nil {
			return nil, serrors.ToGRPCStatus(err)
		}
		if frame.Entry == nil {
			continue
		}
		if err := h.receiver.follower.applyEntry(ctx, frame.Entry); err != nil {
			return nil, serrors.ToGRPCStatus(err)
		}
		entries++
		if entries%512 == 0 {
			if err := check(); err != nil {
				return nil, serrors.ToGRPCStatus(err)
			}
		}
	}
	if !checker.complete {
		return nil, serrors.ToGRPCStatus(migrationDataError("incomplete staging artifact"))
	}
	if fmt.Sprintf("%x", checker.digest.Sum(nil)) != verified.PayloadHash || checker.entries != verified.Entries {
		return nil, serrors.ToGRPCStatus(migrationDataError("artifact changed during installation"))
	}
	if err := check(); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	// Vectors live in the vecstore's RocksDB (sync=false); the verification below
	// must never vouch for writes still sitting in the page cache.
	if err := h.receiver.follower.flushReceivedData(ctx); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	receipt, err := h.receiver.follower.verifyMigrationInstall(ctx, manifest, check)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	receipt.SourceID = task.Stage.SourceID
	receipt.TargetID = target.ID
	receipt.ManifestHash = manifest.Hash()
	receipt.StageHash = task.Stage.PayloadHash
	// Late work is harmless locally but must never be recorded as the task's
	// result: the control layer may have cancelled the task or the epoch moved.
	_, current, latest, err := h.migrationViewFor(ctx, req, true, true)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if current.Phase == storage.MigrationInstalled && current.Install != nil {
		if *current.Install != receipt {
			return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
		}
	} else if current.Phase != storage.MigrationStaged || latest.Hash() != manifest.Hash() {
		return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
	}
	if err := h.migration.writeInstallRecord(path, migrationInstallRecord{Receipt: receipt, OperationID: req.OperationId, KBID: req.KnowledgeBaseId, Epoch: req.ExpectedEpoch}); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	return installReceiptToProto(receipt), nil
}

// writeInstallRecord persists the receipt with the same durability sequence the
// staging artifact uses: content fsync, atomic rename, directory fsync. A failed
// directory fsync leaves no acknowledged receipt, so a retry repeats it.
func (s *migrationStager) writeInstallRecord(path string, record migrationInstallRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return fsutil.AtomicWriteFile(
		path,
		append(data, '\n'),
		fsutil.WithTempPattern(".install-*.tmp"),
		fsutil.WithSyncDir(s.syncDirectory),
	)
}

func installReceiptToProto(r storage.MigrationInstallReceipt) *pb.MigrationInstallReceipt {
	return &pb.MigrationInstallReceipt{SourceNodeId: r.SourceID, TargetNodeId: r.TargetID, ManifestSha256: r.ManifestHash, StageSha256: r.StageHash, VersionCount: r.Versions, DocumentCount: r.Documents, ChunkCount: r.Chunks}
}
func installReceiptFromProto(r *pb.MigrationInstallReceipt) storage.MigrationInstallReceipt {
	return storage.MigrationInstallReceipt{SourceID: r.GetSourceNodeId(), TargetID: r.GetTargetNodeId(), ManifestHash: r.GetManifestSha256(), StageHash: r.GetStageSha256(), Versions: r.GetVersionCount(), Documents: r.GetDocumentCount(), Chunks: r.GetChunkCount()}
}

// checkMigrationTarget bounds what an installation may find on the target: only
// versions this task carries. Rows of another version would survive the replay
// and later become indistinguishable from imported data.
func (f *Follower) checkMigrationTarget(ctx context.Context, manifest storage.MigrationManifest) error {
	if f == nil || f.versionDoc == nil || f.docStore == nil {
		return serrors.ErrPlacementChangeUnsafe
	}
	allowed := make(map[int64]bool, len(manifest.Versions))
	for _, v := range manifest.Versions {
		allowed[v.ID] = true
	}
	local, err := f.versionDoc.ListVersions(ctx, manifest.KBID)
	if err != nil {
		return err
	}
	for _, v := range local {
		if allowed[v] {
			continue
		}
		return fmt.Errorf("%w: target holds version %d outside the migration manifest", serrors.ErrMigrationConflict, v)
	}
	return nil
}

// verifyMigrationInstall re-reads the ordinary stores after the replay. It checks
// the document-set digest of every DURABLE version against the committed manifest,
// that every one of those documents is visible at its version, and that every
// referenced chunk vector exists. Index state is deliberately NOT part of this
// proof; it is a serving condition, decided by the query/index protocol.
func (f *Follower) verifyMigrationInstall(ctx context.Context, manifest storage.MigrationManifest, progress func() error) (storage.MigrationInstallReceipt, error) {
	var receipt storage.MigrationInstallReceipt
	if f == nil || f.versionDoc == nil || f.docStore == nil || f.chunkDoc == nil || f.chunkStore == nil {
		return receipt, serrors.ErrPlacementChangeUnsafe
	}
	const batch = 256
	seenVectors := map[string]bool{}
	for _, version := range manifest.Versions {
		if version.DataStatus != types.DataStatusDurable {
			continue
		}
		docs, err := f.versionDoc.ListDocIDs(ctx, manifest.KBID, version.ID)
		if err != nil {
			return receipt, err
		}
		if ComputeDocIDSetHash(docs) != version.DocIDSetHash {
			return receipt, fmt.Errorf("%w: version %d document set does not match the frozen manifest", serrors.ErrMigrationConflict, version.ID)
		}
		sort.Strings(docs)
		for i, doc := range docs {
			if i%batch == 0 {
				if err := progress(); err != nil {
					return receipt, err
				}
			}
			if _, err := f.docStore.ReadAt(ctx, manifest.KBID, doc, version.ID); err != nil {
				return receipt, fmt.Errorf("%w: version %d document %q is not readable after install: %v", serrors.ErrMigrationConflict, version.ID, doc, err)
			}
			receipt.Documents++
		}
		for start := 0; start < len(docs); start += batch {
			if err := progress(); err != nil {
				return receipt, err
			}
			end := min(start+batch, len(docs))
			chunks, err := f.chunkDoc.ListChunkIDsByDocs(ctx, manifest.KBID, docs[start:end])
			if err != nil {
				return receipt, err
			}
			for _, chunk := range chunks {
				if chunk == "" || seenVectors[chunk] {
					continue
				}
				exists, err := f.chunkStore.Exists(ctx, manifest.KBID, chunk)
				if err != nil {
					return receipt, err
				}
				if !exists {
					return receipt, fmt.Errorf("%w: chunk %s has no vector after install", serrors.ErrMigrationConflict, chunk)
				}
				seenVectors[chunk] = true
				receipt.Chunks++
			}
		}
		receipt.Versions++
	}
	return receipt, nil
}
