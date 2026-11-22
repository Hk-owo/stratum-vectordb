package raft

import (
	"bytes"
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/wire"
)

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
