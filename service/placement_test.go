package service

import (
	"context"
	"reflect"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

func TestPlacementCreateExplicitMembersAndRejectInvalidAtomically(t *testing.T) {
	h := newKBSvcTestHarness()
	ctx := context.Background()
	if err := h.raftNode.ProposeRegisterStorageNodes(ctx, 0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}}); err != nil {
		t.Fatal(err)
	}
	resp, err := h.svc.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{Name: "selected", EmbedConfig: &pb.EmbedConfig{ServiceAddr: "embed", ModelId: "m"}, StorageReplicaIds: []int64{3, 1}})
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.raftNode.GetKBPlacement(ctx, resp.GetKnowledgeBaseId())
	if err != nil || !reflect.DeepEqual(s.Placement.ReplicaIDs, []int64{1, 3}) {
		t.Fatalf("%+v %v", s, err)
	}
	_, err = h.svc.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{Name: "invalid", EmbedConfig: &pb.EmbedConfig{ServiceAddr: "embed", ModelId: "m"}, StorageReplicaIds: []int64{1, 1}})
	if serrors.ReasonOf(err) != "invalid_argument" {
		t.Fatal(err)
	}
	kbs, err := h.raftNode.ListKnowledgeBases(ctx)
	if err != nil || len(kbs) != 1 {
		t.Fatal("failed creation left metadata:", kbs, err)
	}
}

func TestPlacementStorageInterceptorRejectsNonmemberAndStaleBeforeWork(t *testing.T) {
	d := storage.NewDirectory()
	if err := d.RegisterNodes(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.AssignNew(storage.Placement{KBID: "A", Epoch: 1, ReplicaIDs: []int64{1}, RequiredReplicas: 1, OperationID: "op"}); err != nil {
		t.Fatal(err)
	}
	resolve := func(_ context.Context, kb string) (storage.Snapshot, error) { return d.Resolve(kb) }
	next := func(context.Context, any) (any, error) {
		t.Fatal("rejected request reached local storage")
		return nil, nil
	}
	_, err := UnaryStoragePlacement(resolve, 2)(context.Background(), &pb.QueryRequest{KnowledgeBaseId: "A"}, &grpc.UnaryServerInfo{FullMethod: pb.QueryService_Query_FullMethodName}, next)
	if serrors.ReasonOf(err) != "not_kb_replica" {
		t.Fatal(err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-stratum-placement-epoch", "2"))
	_, err = UnaryStoragePlacement(resolve, 1)(ctx, &pb.QueryRequest{KnowledgeBaseId: "A"}, &grpc.UnaryServerInfo{FullMethod: pb.QueryService_Query_FullMethodName}, next)
	if serrors.ReasonOf(err) != "placement_stale" {
		t.Fatal(err)
	}
}
