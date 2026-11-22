package sync

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
)

// PrepareKBMigrationServing is the target's second, separate step after
// installation: it restores the LOCAL state a query would rely on and proves the
// active version's index artifact is durable here.
//
// It is deliberately not part of the install receipt and not part of the DATA
// manifest: index readiness is a serving condition, never a data-durability
// proof. It also reports nothing to the control layer's READY counters — this
// node is not a member yet, and READY on a non-member is not a fact the query
// model may use.
//
// The three checks it produces are exactly the query model's prerequisites:
//   - the data cursor covers the manifest's active version (a station refuses a
//     replica whose history stops short of what it is asked to serve),
//   - every DURABLE version has its document filter rebuilt from installed data
//     (a filter built before the data arrived silently drops every hit),
//   - the active version's index artifact exists on this node, building it when
//     it does not and waiting within a bounded budget.

// MigrationServing is the narrow index surface a migration target needs. Declared
// here, by the consumer, so this package does not depend on internal/index.
type MigrationServing interface {
	// IndexExists reports whether the persisted index artifact for the version
	// exists on this node's disk.
	IndexExists(ctx context.Context, kbID string, versionID int64) (bool, error)
	// TriggerBuildBackfill schedules a build at BACKFILL priority: nobody is
	// waiting on this version yet (the task is not activated), so the build must
	// not outrank a live write's index.
	TriggerBuildBackfill(ctx context.Context, kbID string, versionID int64) error
}

// WithMigrationServing wires the index surface used by the serving step.
func WithMigrationServing(s MigrationServing) PushHandlerOption {
	return func(h *PushHandler) { h.serving = s }
}

// defaultServeIndexWait bounds one serving step's wait for the index build. A
// later step retries: an over-budget build stays scheduled, and the task remains
// INSTALLED (not serviceable) in the meantime.
const defaultServeIndexWait = 2 * time.Minute

// serveIndexPoll is the index-existence polling interval inside that budget.
const serveIndexPoll = 200 * time.Millisecond

// serveCheckpoint bounds how often the wait re-reads the task, so a cancelled or
// replaced task stops the work instead of preparing a target nobody will use.
const serveCheckpoint = 5 * time.Second

func serveReceiptToProto(r storage.MigrationServeReceipt) *pb.MigrationServeReceipt {
	return &pb.MigrationServeReceipt{TargetNodeId: r.TargetID, ManifestSha256: r.ManifestHash, ActiveVersion: r.ActiveVersion, CursorVersion: r.CursorVersion, BloomVersionCount: r.BloomVersions, IndexReady: r.IndexReady}
}
func serveReceiptFromProto(r *pb.MigrationServeReceipt) storage.MigrationServeReceipt {
	return storage.MigrationServeReceipt{TargetID: r.GetTargetNodeId(), ManifestHash: r.GetManifestSha256(), ActiveVersion: r.GetActiveVersion(), CursorVersion: r.GetCursorVersion(), BloomVersions: r.GetBloomVersionCount(), IndexReady: r.GetIndexReady()}
}

// PrepareKBMigrationServing restores this target's local serving state and reports
// the evidence. Only the task's target may call it.
func (h *NodeHandler) PrepareKBMigrationServing(ctx context.Context, req *pb.MigrationTransferRequest) (*pb.MigrationServeReceipt, error) {
	return h.prepareMigrationServing(ctx, req, false)
}
func (h *NodeHandler) ActivateKBMigration(ctx context.Context, req *pb.MigrationTransferRequest) (*pb.MigrationServeReceipt, error) {
	if h.migration == nil || h.receiver == nil {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
	}
	manager, ok := h.migration.metadata.(storage.MaintenanceManager)
	if !ok {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
	}
	d, err := manager.GetStorageMaintenance(ctx)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if m, ok := d.Migrations[req.GetOperationId()]; ok && m.Phase == storage.MigrationActivated {
		if m.Request.KBID != req.GetKnowledgeBaseId() || m.Old.Epoch != req.GetExpectedEpoch() || m.Request.ToID != h.receiver.nodeID || m.Stage == nil || m.Stage.SourceID != req.GetSourceNodeId() || m.Serve == nil {
			return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
		}
		return serveReceiptToProto(*m.Serve), nil
	}
	return h.prepareMigrationServing(ctx, req, true)
}
func (h *NodeHandler) prepareMigrationServing(ctx context.Context, req *pb.MigrationTransferRequest, activate bool) (*pb.MigrationServeReceipt, error) {
	if h.migration == nil || h.migration.dir == "" || h.receiver == nil || h.receiver.follower == nil || h.exporter == nil {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
	}
	if h.receiver.serving == nil || h.receiver.localVersion == nil {
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
	if task.Phase != storage.MigrationInstalled || task.Install == nil {
		return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
	}
	// Serving evidence is only meaningful for data this task actually installed;
	// a missing or foreign record means the local stores and the task disagree.
	record, err := readInstallRecord(migrationInstallPath(h.migration.dir, req.KnowledgeBaseId, req.OperationId))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
		}
		return nil, serrors.ToGRPCStatus(err)
	}
	if record.OperationID != req.OperationId || record.KBID != req.KnowledgeBaseId || record.Epoch != req.ExpectedEpoch || record.Receipt != *task.Install {
		return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
	}

	importCtx, release, err := h.receiver.enterMigrationImport(ctx, req.KnowledgeBaseId, req.ExpectedEpoch, req.OperationId)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	defer release()
	ctx = importCtx
	if err := h.checkMigrationServingTask(ctx, req, task, manifest); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}

	if activate {
		// Re-read formal DATA on this live target, even after restart. A historical
		// receipt alone must not turn a node with lost stores into a new replica.
		if _, err := h.receiver.follower.verifyMigrationInstall(ctx, manifest, func() error { return h.checkMigrationServingTask(ctx, req, task, manifest) }); err != nil {
			return nil, serrors.ToGRPCStatus(err)
		}
	}
	if h.receiver.follower.versionDoc == nil {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
	}
	durable := make([]storage.MigrationVersion, 0, len(manifest.Versions))
	for _, v := range manifest.Versions {
		if v.DataStatus == types.DataStatusDurable {
			durable = append(durable, v)
		}
	}
	sort.Slice(durable, func(i, j int) bool { return durable[i].ID < durable[j].ID })
	for _, v := range durable {
		h.receiver.follower.markVersionContiguous(manifest.KBID, v.ID)
	}
	cursor := h.receiver.localVersion.LocalVersionOf(manifest.KBID)
	if cursor < manifest.ActiveVersion {
		return nil, serrors.ToGRPCStatus(fmt.Errorf("%w: target cursor %d does not reach active version %d", serrors.ErrMigrationBusy, cursor, manifest.ActiveVersion))
	}

	indexReady := manifest.ActiveVersion == 0
	// Empty committed versions are queryable without a vecstore index artifact.
	// The real index manager deliberately never persists an empty index.
	for _, v := range durable {
		if v.ID == manifest.ActiveVersion && v.DocIDSetHash == types.EmptyDocIDSetHash {
			indexReady = true
		}
	}
	if !indexReady {
		if indexReady, err = h.waitMigrationIndex(ctx, req, manifest); err != nil {
			return nil, serrors.ToGRPCStatus(err)
		}
	}
	receipt := storage.MigrationServeReceipt{TargetID: target.ID, ManifestHash: manifest.Hash(), ActiveVersion: manifest.ActiveVersion, CursorVersion: cursor, BloomVersions: uint64(len(durable)), IndexReady: indexReady}
	if err := ctx.Err(); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if err := h.checkMigrationServingTask(ctx, req, task, manifest); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if activate {
		manager, ok := h.migration.metadata.(storage.MaintenanceManager)
		if !ok {
			return nil, serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
		}
		if err := manager.ProposeActivateKBMigration(ctx, req.OperationId, storage.MigrationActivation{Epoch: req.ExpectedEpoch, Install: *task.Install, Serve: receipt}); err != nil {
			return nil, serrors.ToGRPCStatus(err)
		}
	}
	return serveReceiptToProto(receipt), nil
}

func (h *NodeHandler) checkMigrationServingTask(ctx context.Context, req *pb.MigrationTransferRequest, task storage.Migration, manifest storage.MigrationManifest) error {
	_, current, latest, err := h.migrationViewFor(ctx, req, true, true)
	if err != nil {
		return err
	}
	if current.Phase != storage.MigrationInstalled || current.Install == nil || *current.Install != *task.Install || latest.Hash() != manifest.Hash() {
		return serrors.ErrMigrationConflict
	}
	return nil
}

// waitMigrationIndex makes the active version's index artifact durable here. It
// builds once (BACKFILL priority — no caller waits on this version yet) and waits
// within one bounded budget; a still-running build simply leaves the task
// INSTALLED for the next scheduling pass.
func (h *NodeHandler) waitMigrationIndex(ctx context.Context, req *pb.MigrationTransferRequest, manifest storage.MigrationManifest) (bool, error) {
	budget := h.migration.serveWait
	if budget <= 0 {
		budget = defaultServeIndexWait
	}
	wait, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	exists, err := h.receiver.serving.IndexExists(wait, manifest.KBID, manifest.ActiveVersion)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	if err := h.receiver.serving.TriggerBuildBackfill(wait, manifest.KBID, manifest.ActiveVersion); err != nil {
		return false, err
	}
	lastCheck := time.Now()
	ticker := time.NewTicker(serveIndexPoll)
	defer ticker.Stop()
	for {
		select {
		case <-wait.Done():
			return false, fmt.Errorf("%w: active version index is not durable on the target yet", serrors.ErrMigrationBusy)
		case <-ticker.C:
		}
		if time.Since(lastCheck) >= serveCheckpoint {
			lastCheck = time.Now()
			if _, current, latest, err := h.migrationViewFor(wait, req, true, true); err != nil || current.Phase != storage.MigrationInstalled || latest.Hash() != manifest.Hash() {
				return false, serrors.ErrMigrationConflict
			}
		}
		exists, err := h.receiver.serving.IndexExists(wait, manifest.KBID, manifest.ActiveVersion)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
	}
}
