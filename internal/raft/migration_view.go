package raft

import (
	"bytes"
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wire"
)

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
func (r *RaftNodeImpl) GetKBMigrationTransfer(ctx context.Context, op string) (storage.MigrationTransferView, error) {
	var zero storage.MigrationTransferView
	if op == "" {
		return zero, serrors.ErrInvalidArgument
	}
	if !r.IsLeader() {
		return zero, serrors.ErrNoLeaderYet
	}
	term := r.raft.Term()
	result, err := r.proposeAndWait(ctx, command{Type: cmdGetKBMigrationTransfer, OperationID: op})
	if err != nil {
		return zero, err
	}
	if result.Err != nil {
		return zero, result.Err
	}
	if !r.IsLeader() || term != r.raft.Term() || !result.MigrationRead {
		return zero, serrors.ErrNoLeaderYet
	}
	r.sm.mu.RLock()
	view, err := r.sm.migrationTransferView(op)
	r.sm.mu.RUnlock()
	if err != nil {
		return zero, err
	}
	if !r.IsLeader() || term != r.raft.Term() {
		return zero, serrors.ErrNoLeaderYet
	}
	return view, nil
}
func (s *InternalServiceServer) GetKBMigrationTransfer(ctx context.Context, req *pb.GetKBMigrationTransferRequest) (*pb.GetKBMigrationTransferResponse, error) {
	view, err := s.node.GetKBMigrationTransfer(ctx, req.GetOperationId())
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	return &pb.GetKBMigrationTransferResponse{Directory: wire.StorageDirectoryToProto(view.Directory), Manifest: view.Manifest.Bytes()}, nil
}
func (r *RemoteRaftNode) GetKBMigrationTransfer(ctx context.Context, op string) (storage.MigrationTransferView, error) {
	var view storage.MigrationTransferView
	err := r.readAtAnyControl(ctx, func(c context.Context, conn *grpc.ClientConn) error {
		response, err := pb.NewInternalServiceClient(conn).GetKBMigrationTransfer(c, &pb.GetKBMigrationTransferRequest{OperationId: op})
		if err != nil {
			return err
		}
		var manifest storage.MigrationManifest
		if err := json.Unmarshal(response.GetManifest(), &manifest); err != nil {
			return err
		}
		if response.GetDirectory() == nil || !bytes.Equal(response.Manifest, manifest.Bytes()) {
			return serrors.ErrMigrationConflict
		}
		view = storage.MigrationTransferView{Directory: wire.StorageDirectoryFromProto(response.Directory), Manifest: manifest}
		return nil
	})
	return view, err
}
func (r *MockRaftNode) GetKBMigrationTransfer(ctx context.Context, op string) (storage.MigrationTransferView, error) {
	if err := ctx.Err(); err != nil {
		return storage.MigrationTransferView{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	sm := stateMachine{directory: r.directory, kbs: r.kbs, versions: r.versions, versionsByKB: r.versionsByKB}
	return sm.migrationTransferView(op)
}

var _ storage.MigrationTransferReader = (*RaftNodeImpl)(nil)
var _ storage.MigrationTransferReader = (*RemoteRaftNode)(nil)
var _ storage.MigrationTransferReader = (*MockRaftNode)(nil)
