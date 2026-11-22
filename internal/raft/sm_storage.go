package raft

import (
	"slices"

	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
)

// applyStorage applies storage-directory, placement, migration, and service
// lease commands. The caller holds sm.mu.
func (sm *stateMachine) applyStorage(cmd command) (applyResult, bool) {
	switch cmd.Type {
	case cmdGetStorageMaintenance:
		return applyResult{MigrationRead: true}, true
	case cmdGetKBMigrationTransfer:
		// A small committed read barrier; build the detached view once on
		// the reader, not on every follower or in the settled-result cache.
		return applyResult{MigrationRead: true, ServiceRevision: sm.directory.Revision}, true
	case cmdRenewStorageService, cmdBlockStorageService, cmdResumeStorageService, cmdConfirmStorageServiceRevoked:
		return sm.applyService(cmd), true
	case cmdDrainStorageNode, cmdActivateKBMigration, cmdAckStorageCleanup, cmdManageStorageNode, cmdBeginKBMigration, cmdFenceKBMigration, cmdAckKBMigrationFence, cmdCancelKBMigration, cmdRecordKBMigrationStage, cmdRecordKBMigrationInstall, cmdRecordKBMigrationServe:
		return applyResult{Err: sm.applyMigration(cmd)}, true
	case cmdRegisterStorageNodes:
		if cmd.StorageNodes == nil {
			return applyResult{Err: serrors.ErrInvalidArgument}, true
		}
		return applyResult{Err: sm.directory.RegisterNodes(cmd.DirectoryRevision, cmd.StorageNodes.Nodes)}, true
	case cmdAssignKBPlacement:
		if cmd.Placement == nil {
			return applyResult{Err: serrors.ErrInvalidArgument}, true
		}
		if _, deleted := sm.directory.Tombstones[cmd.Placement.KBID]; deleted {
			return applyResult{Err: serrors.ErrKnowledgeBaseDeleted}, true
		}
		kb, ok := sm.kbs[cmd.Placement.KBID]
		if !ok {
			return applyResult{Err: serrors.ErrKnowledgeBaseNotFound}, true
		}
		if kb.Status != types.KBStatusActive {
			return applyResult{Err: serrors.ErrKnowledgeBaseDeleted}, true
		}
		return applyResult{Err: sm.directory.AssignInitial(*cmd.Placement)}, true
	default:
		return applyResult{}, false
	}
}

func (sm *stateMachine) applyService(c command) applyResult {
	if c.Service == nil {
		return applyResult{Err: serrors.ErrInvalidArgument}
	}
	var err error
	switch c.Type {
	case cmdRenewStorageService:
		err = sm.directory.AuthorizeService(*c.Service)
	case cmdBlockStorageService:
		err = sm.directory.BlockService(*c.Service)
	case cmdConfirmStorageServiceRevoked:
		err = sm.directory.ConfirmServiceRevoked(*c.Service)
	case cmdResumeStorageService:
		err = sm.directory.ResumeService(*c.Service)
	default:
		err = serrors.ErrInvalidArgument
	}
	result := applyResult{Err: err, ServiceRevision: sm.directory.Revision}
	if b, ok := sm.directory.ServiceBlocks[c.Service.NodeID]; ok {
		result.ServiceBlockRevision = b.Revision
	}
	return result
}

func (sm *stateMachine) migrationBlocksCommand(c command) bool {
	kbID := c.KBID
	switch c.Type {
	case cmdMarkKBDeleting, cmdMarkKBDeleteFailed, cmdRemoveKBMeta, cmdRollback, cmdMarkVersionDeleting, cmdRemoveVersionMeta:
	case cmdRetryVersion:
		kbID = sm.versions[c.VersionID].KBID
	case cmdMarkVersionFailedPermanent:
		v, exists := sm.versions[c.VersionID]
		if c.FailureSide == types.FailureSideIndex || !exists || v.DataStatus == types.DataStatusFailedPermanent {
			return false
		}
		m, ok := sm.directory.ActiveMigration(kbID)
		return ok && m.BlocksDataWrites()
	case cmdDiscardVersion:
		m, ok := sm.directory.ActiveMigration(kbID)
		return ok && m.BlocksDataWrites()
	default:
		return false
	}
	_, active := sm.directory.ActiveMigration(kbID)
	return active
}

// The caller holds the state-machine lock. This is metadata-only; it never
// performs a storage RPC or waits for node-local work to drain.
func (sm *stateMachine) applyMigration(c command) error {
	switch c.Type {
	case cmdDrainStorageNode:
		if c.Drain == nil {
			return serrors.ErrInvalidArgument
		}
		for kb, p := range sm.directory.Placements {
			if !p.Contains(c.Drain.NodeID) {
				continue
			}
			meta, ok := sm.kbs[kb]
			if !ok || meta.Status != types.KBStatusActive {
				return serrors.ErrKnowledgeBaseDeleted
			}
			for _, id := range sm.versionsByKB[kb] {
				if sm.versions[id].Deleting {
					return serrors.ErrMigrationBusy
				}
			}
		}
		return sm.directory.DrainNode(*c.Drain)
	case cmdAckStorageCleanup:
		return sm.directory.AckCleanup(c.OperationID)
	case cmdManageStorageNode:
		if c.NodeChange == nil {
			return serrors.ErrInvalidArgument
		}
		return sm.directory.ManageNode(*c.NodeChange)
	case cmdActivateKBMigration:
		if c.Activation == nil {
			return serrors.ErrInvalidArgument
		}
		m, ok := sm.directory.Migrations[c.OperationID]
		if !ok {
			return serrors.ErrMigrationNotFound
		}
		var manifest storage.MigrationManifest
		if m.Phase != storage.MigrationActivated {
			_, read, e := sm.migrationManifest(c.OperationID)
			if e != nil {
				return e
			}
			manifest = read
		}
		wasActivated := m.Phase == storage.MigrationActivated
		if err := sm.directory.ActivateMigration(c.OperationID, *c.Activation, manifest); err != nil {
			return err
		}
		if !wasActivated {
			for _, id := range sm.versionsByKB[m.Request.KBID] {
				v := sm.versions[id]
				v.IndexReadyNodes = slices.DeleteFunc(slices.Clone(v.IndexReadyNodes), func(n int64) bool { return n == m.Request.FromID })
				sm.versions[id] = v
			}
		}
		return nil
	case cmdRecordKBMigrationStage:
		if c.StageReceipt == nil {
			return serrors.ErrInvalidArgument
		}
		_, manifest, err := sm.migrationManifest(c.OperationID)
		if err != nil {
			return err
		}
		return sm.directory.RecordMigrationStage(c.OperationID, *c.StageReceipt, manifest)
	case cmdRecordKBMigrationInstall:
		if c.InstallReceipt == nil {
			return serrors.ErrInvalidArgument
		}
		_, manifest, err := sm.migrationManifest(c.OperationID)
		if err != nil {
			return err
		}
		return sm.directory.RecordMigrationInstall(c.OperationID, *c.InstallReceipt, manifest)
	case cmdRecordKBMigrationServe:
		if c.ServeReceipt == nil {
			return serrors.ErrInvalidArgument
		}
		_, manifest, err := sm.migrationManifest(c.OperationID)
		if err != nil {
			return err
		}
		return sm.directory.RecordMigrationServe(c.OperationID, *c.ServeReceipt, manifest)
	case cmdBeginKBMigration:
		if c.Migration == nil {
			return serrors.ErrInvalidArgument
		}
		req := *c.Migration
		if _, exists := sm.directory.Migrations[req.OperationID]; exists {
			return sm.directory.BeginMigration(req)
		}
		kb, ok := sm.kbs[req.KBID]
		if !ok {
			return serrors.ErrKnowledgeBaseNotFound
		}
		if kb.Status != types.KBStatusActive {
			return serrors.ErrKnowledgeBaseDeleted
		}
		for _, id := range sm.versionsByKB[req.KBID] {
			if sm.versions[id].Deleting {
				return serrors.ErrMigrationBusy
			}
		}
		return sm.directory.BeginMigration(req)
	case cmdFenceKBMigration:
		m, ok := sm.directory.Migrations[c.OperationID]
		if !ok {
			return serrors.ErrMigrationNotFound
		}
		if m.Phase == storage.MigrationDraining {
			for _, id := range sm.versionsByKB[m.Request.KBID] {
				v := sm.versions[id]
				if v.Deleting || v.DataStatus == types.DataStatusPending {
					return serrors.ErrMigrationBusy
				}
			}
		}
		return sm.directory.StartFencing(c.OperationID)
	case cmdAckKBMigrationFence:
		return sm.directory.AckFence(c.OperationID, c.PlacementEpoch, c.NodeID)
	case cmdCancelKBMigration:
		return sm.directory.CancelMigration(c.OperationID)
	default:
		return serrors.ErrInvalidArgument
	}
}

// migrationDataPhase is the set of phases whose frozen DATA manifest is still the
// authority: prepared, staged and installed tasks all describe the same bytes.
func migrationDataPhase(phase storage.MigrationPhase) bool {
	return phase == storage.MigrationFenced || phase == storage.MigrationStaged || phase == storage.MigrationInstalled
}

func (sm *stateMachine) migrationManifest(op string) (storage.Migration, storage.MigrationManifest, error) {
	var manifest storage.MigrationManifest
	m, ok := sm.directory.Migrations[op]
	if !ok {
		return m, manifest, serrors.ErrMigrationNotFound
	}
	if !migrationDataPhase(m.Phase) {
		return m, manifest, serrors.ErrMigrationConflict
	}
	kb, ok := sm.kbs[m.Request.KBID]
	if !ok || kb.Status != types.KBStatusActive {
		return m, manifest, serrors.ErrKnowledgeBaseDeleted
	}
	versions := make([]types.VersionMeta, 0, len(sm.versionsByKB[kb.KBID]))
	for _, id := range sm.versionsByKB[kb.KBID] {
		versions = append(versions, sm.versions[id])
	}
	manifest, err := storage.BuildMigrationManifest(kb.KBID, kb.ActiveVersionID, versions)
	return m, manifest, err
}

// Called only with the state-machine lock held (including from Raft apply).
func (sm *stateMachine) migrationTransferView(op string) (storage.MigrationTransferView, error) {
	var view storage.MigrationTransferView
	m, manifest, err := sm.migrationManifest(op)
	if err != nil {
		return view, err
	}
	if !migrationDataPhase(m.Phase) {
		return view, serrors.ErrMigrationConflict
	}
	if err := sm.directory.ValidateFailedBlock(m); err != nil {
		return view, err
	}
	placement, err := sm.directory.Lookup(m.Request.KBID)
	if err != nil {
		return view, err
	}
	d := storage.NewDirectory()
	d.Revision = sm.directory.Revision
	d.Placements[m.Request.KBID] = placement
	d.Migrations = map[string]storage.Migration{op: m.Clone()}
	d.ActiveVersions = map[string]int64{m.Request.KBID: manifest.ActiveVersion}
	for _, id := range m.FenceIDs() {
		if n, ok := sm.directory.Nodes[id]; ok {
			d.Nodes[id] = n
		}
		if b, ok := sm.directory.ServiceBlocks[id]; ok {
			if d.ServiceBlocks == nil {
				d.ServiceBlocks = map[int64]storage.ServiceBlock{}
			}
			d.ServiceBlocks[id] = b
		}
	}
	view.Directory, view.Manifest = d, manifest
	return view, nil
}
