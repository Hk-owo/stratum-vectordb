package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

func TestServiceAuthorizationRequiresCommittedCurrentDirectory(t *testing.T) {
	sm, w := newTestSM(t)
	apply := func(c command) applyResult { return sm.apply(context.Background(), c, w, zap.NewNop()) }
	if err := apply(registerStorageNodesCommand(0, directoryTestNodes)).Err; err != nil {
		t.Fatal(err)
	}
	req := storage.ServiceRequest{NodeID: 1, Address: "s1"}
	grant := apply(command{Type: cmdRenewStorageService, Service: &req})
	if grant.Err != nil || grant.ServiceRevision != sm.directory.Revision {
		t.Fatal(grant)
	}
	req.OperationID = "fault"
	req.ExpectedRevision = sm.directory.Revision
	block := apply(command{Type: cmdBlockStorageService, Service: &req})
	if block.Err != nil || block.ServiceBlockRevision == 0 {
		t.Fatal(block)
	}
	if err := apply(command{Type: cmdRenewStorageService, Service: &req}).Err; !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Fatal(err)
	}
	data, err := sm.serialize()
	if err != nil {
		t.Fatal(err)
	}
	restored := newStateMachine()
	if err := restored.restore(data); err != nil {
		t.Fatal(err)
	}
	if restored.directory.ServiceBlocks[1] != sm.directory.ServiceBlocks[1] {
		t.Fatal("revocation lost on restart")
	}
	if err := restored.applyService(command{Type: cmdRenewStorageService, Service: &req}).Err; !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Fatal("restart reopened grant:", err)
	}
}

func TestServiceGrantRealRaftAndRevocationWaitFailsClosed(t *testing.T) {
	r, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()
	if err := r.ProposeRegisterStorageNodes(ctx, 0, directoryTestNodes); err != nil {
		t.Fatal(err)
	}
	server := NewInternalServiceServer(r)
	request := &pb.RenewStorageServiceRequest{NodeId: 1, Address: "s1", RequestId: "request"}
	response, err := server.RenewStorageService(ctx, request)
	if err != nil || response.GetDurationMs() != storage.ServiceLeaseDuration.Milliseconds() || response.GetRequestId() != "request" {
		t.Fatalf("%v %v", response, err)
	}
	d, _ := r.StorageDirectory(ctx)
	block := storage.ServiceRequest{NodeID: 1, OperationID: "fault", ExpectedRevision: d.Revision}
	if err := r.ProposeBlockStorageService(ctx, block); err != nil {
		t.Fatal(err)
	}
	if _, err := server.RenewStorageService(ctx, request); serrors.ReasonOf(err) != "storage_service_paused" {
		t.Fatal(err)
	}
	// A new wait never treats the committed block as already expired. Restart
	// and leader-change callers must perform the same full wait.
	c, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := r.WaitStorageServiceRevoked(c, block); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("revocation wait skipped:", err)
	}
	projected, err := server.GetStorageDirectory(ctx, &pb.GetStorageDirectoryRequest{NodeId: 1})
	if err != nil || projected.GetServiceBlocks()[1].GetOperationId() != "fault" {
		t.Fatal(projected, err)
	}
}
