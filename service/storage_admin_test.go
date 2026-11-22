package service

import (
	"context"
	pb "stratum/api/proto/stratum"
	"stratum/internal/raft"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wal"
	"testing"
)

type storageAdminWithRevoke struct {
	*raft.MockRaftNode
}

func (w *storageAdminWithRevoke) WaitStorageServiceRevoked(_ context.Context, _ storage.ServiceRequest) error {
	return nil
}

func TestStorageAdminRegistersAndPlansWholeNodeDrain(t *testing.T) {
	ctx := context.Background()
	backend := raft.NewMockRaftNode(wal.NewMockWAL())
	s := NewStorageAdminService(backend)
	if _, err := s.RegisterNodes(ctx, &pb.RegisterStorageNodesRequest{Nodes: []*pb.StorageNodeEntry{{NodeId: 1, Address: "s1"}, {NodeId: 2, Address: "s2"}, {NodeId: 3, Address: "s3"}}}); err != nil {
		t.Fatal(err)
	}
	for _, kb := range []string{"A", "B"} {
		if err := backend.ProposeCreateKBWithPlacement(ctx, types.KnowledgeBaseMeta{KBID: kb}, []int64{1, 2}); err != nil {
			t.Fatal(err)
		}
	}
	before, err := s.GetDirectory(ctx, &pb.GetStorageDirectoryRequest{})
	if err != nil {
		t.Fatal(err)
	}
	req := &pb.DrainStorageNodeRequest{OperationId: "whole", NodeId: 2, ExpectedRevision: before.Revision, TargetNodeIds: []int64{3}}
	if _, err := s.DrainNode(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DrainNode(ctx, req); err != nil {
		t.Fatal("drain retry failed", err)
	}
	d, err := s.GetDirectory(ctx, &pb.GetStorageDirectoryRequest{})
	if err != nil || len(d.Migrations) != 2 {
		t.Fatal("whole-node operation lost a KB", d, err)
	}
	for _, p := range d.Placements {
		if p.PlacementEpoch != 1 || len(p.ReplicaIds) != 2 {
			t.Fatal("drain changed placement before transfer", p)
		}
	}
	if _, err := s.ManageNode(ctx, &pb.ManageStorageNodeRequest{NodeId: 2, ExpectedRevision: d.Revision, Action: "retire"}); err == nil {
		t.Fatal("retired live node")
	}
	if !ClientFacing(pb.StorageAdminService_DrainNode_FullMethodName) {
		t.Fatal("new public API bypasses station verification")
	}
}

func TestStorageAdminMigrationAndServiceActionsAndUnsupportedBackend(t *testing.T) {
	ctx := context.Background()

	// Backend without storage interfaces returns FailedPrecondition/Unavailable mapped status.
	noCap := NewStorageAdminService(struct{}{})
	if _, err := noCap.GetDirectory(ctx, &pb.GetStorageDirectoryRequest{}); err == nil {
		t.Fatal("expected GetDirectory error on non-capable backend")
	}
	if _, err := noCap.RegisterNodes(ctx, &pb.RegisterStorageNodesRequest{}); err == nil {
		t.Fatal("expected RegisterNodes error on non-capable backend")
	}
	if _, err := noCap.ManageNode(ctx, &pb.ManageStorageNodeRequest{Action: "drain"}); err == nil {
		t.Fatal("expected ManageNode error on non-capable backend")
	}
	if _, err := noCap.MigrateKB(ctx, &pb.MigrateStorageKBRequest{}); err == nil {
		t.Fatal("expected MigrateKB error on non-capable backend")
	}
	if _, err := noCap.CancelMigration(ctx, &pb.CancelStorageMigrationRequest{}); err == nil {
		t.Fatal("expected CancelMigration error on non-capable backend")
	}
	if _, err := noCap.DrainNode(ctx, &pb.DrainStorageNodeRequest{}); err == nil {
		t.Fatal("expected DrainNode error on non-capable backend")
	}

	// Capable mock backend: MigrateKB, CancelMigration, ManageNode(block/confirm_revoked/resume).
	backend := raft.NewMockRaftNode(wal.NewMockWAL())
	s := NewStorageAdminService(backend)
	if _, err := s.RegisterNodes(ctx, &pb.RegisterStorageNodesRequest{Nodes: []*pb.StorageNodeEntry{
		{NodeId: 1, Address: "s1"},
		{NodeId: 2, Address: "s2"},
		{NodeId: 3, Address: "s3"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := backend.ProposeCreateKBWithPlacement(ctx, types.KnowledgeBaseMeta{KBID: "kb-m"}, []int64{1, 2}); err != nil {
		t.Fatal(err)
	}
	dir, err := s.GetDirectory(ctx, &pb.GetStorageDirectoryRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MigrateKB(ctx, &pb.MigrateStorageKBRequest{
		OperationId:     "mig-1",
		KnowledgeBaseId: "kb-m",
		ExpectedEpoch:   1,
		FromNodeId:      2,
		ToNodeId:        3,
	}); err != nil {
		t.Fatalf("MigrateKB: %v", err)
	}
	if _, err := s.CancelMigration(ctx, &pb.CancelStorageMigrationRequest{OperationId: "mig-1"}); err != nil {
		t.Fatalf("CancelMigration: %v", err)
	}
	dir, err = s.GetDirectory(ctx, &pb.GetStorageDirectoryRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ManageNode(ctx, &pb.ManageStorageNodeRequest{
		NodeId:           2,
		ExpectedRevision: dir.Revision,
		Action:           "block",
		OperationId:      "svc-1",
	}); err != nil {
		t.Fatalf("ManageNode block: %v", err)
	}
	dirAfterBlock, err := s.GetDirectory(ctx, &pb.GetStorageDirectoryRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var blockRev uint64
	if b := dirAfterBlock.GetServiceBlocks()[2]; b != nil {
		blockRev = b.GetRevision()
	}
	if _, err := s.ManageNode(ctx, &pb.ManageStorageNodeRequest{
		NodeId:        2,
		Action:        "confirm_revoked",
		OperationId:   "svc-1",
		BlockRevision: blockRev,
	}); err == nil {
		t.Fatal("expected confirm_revoked error when backend lacks WaitStorageServiceRevoked")
	}
	sRevoke := NewStorageAdminService(&storageAdminWithRevoke{MockRaftNode: backend})
	if _, err := sRevoke.ManageNode(ctx, &pb.ManageStorageNodeRequest{
		NodeId:        2,
		Action:        "confirm_revoked",
		OperationId:   "svc-1",
		BlockRevision: blockRev,
	}); err != nil {
		t.Fatalf("ManageNode confirm_revoked: %v", err)
	}
	if _, err := s.ManageNode(ctx, &pb.ManageStorageNodeRequest{
		NodeId:           2,
		ExpectedRevision: dirAfterBlock.Revision,
		Action:           "resume",
		OperationId:      "svc-1",
		BlockRevision:    blockRev,
	}); err != nil {
		t.Fatalf("ManageNode resume: %v", err)
	}
}
