package service

import (
	"context"
	pb "stratum/api/proto/stratum"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/wal"
	"testing"
)

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
