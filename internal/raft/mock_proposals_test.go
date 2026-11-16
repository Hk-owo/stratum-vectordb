package raft

import (
	"context"
	"errors"
	"testing"

	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// mockStorageNode is a MockRaftNode holding one KB "kb" on {1,2}: the smallest
// directory the maintenance proposals can act on.
func mockStorageNode(t *testing.T) *MockRaftNode {
	t.Helper()
	node := NewMockRaftNode(wal.NewMockWAL())
	ctx := context.Background()
	if err := node.ProposeRegisterStorageNodes(ctx, 0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}}); err != nil {
		t.Fatal(err)
	}
	if err := node.ProposeCreateKBWithPlacement(ctx, types.KnowledgeBaseMeta{KBID: "kb"}, []int64{1, 2}); err != nil {
		t.Fatal(err)
	}
	return node
}

// Every maintenance proposal is a thin wrapper around one command, and the wrapper
// is where a cancelled context has to stop the work: a proposal that outlived its
// caller would change the directory on nobody's behalf.
func TestMockRaftNode_MaintenanceProposals(t *testing.T) {
	ctx := context.Background()
	node := mockStorageNode(t)

	if err := node.ProposeManageStorageNode(ctx, storage.NodeChange{NodeID: 3, ExpectedRevision: node.directory.Revision, Action: "drain"}); err != nil {
		t.Fatalf("manage node: %v", err)
	}
	if node.directory.Nodes[3].State != storage.NodeDraining {
		t.Errorf("node 3 = %+v, want draining", node.directory.Nodes[3])
	}
	if err := node.ProposeManageStorageNode(ctx, storage.NodeChange{NodeID: 9, Action: "drain"}); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Errorf("unknown node = %v, want ErrInvalidArgument", err)
	}

	// Node 2 holds data and the only replacement offered is its own peer, so the
	// drain must be refused as a whole rather than planned partially.
	if err := node.ProposeDrainStorageNode(ctx, storage.DrainRequest{OperationID: "drain", NodeID: 2, ExpectedRevision: node.directory.Revision, TargetIDs: []int64{1}}); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("drain with an inadmissible target = %v, want ErrPlacementChangeUnsafe", err)
	}
	if err := node.ProposeAckStorageCleanup(ctx, "missing"); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Errorf("unknown cleanup = %v, want ErrInvalidArgument", err)
	}
	if err := node.ProposeActivateKBMigration(ctx, "missing", storage.MigrationActivation{Epoch: 1}); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown activation = %v, want ErrMigrationNotFound", err)
	}

	// The receipt proposals go through the same wrapper, so a task that does not
	// exist is refused before anything is recorded.
	if err := node.ProposeRecordKBMigrationStage(ctx, "missing", storage.MigrationStageReceipt{}); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown stage = %v, want ErrMigrationNotFound", err)
	}
	if err := node.ProposeRecordKBMigrationInstall(ctx, "missing", storage.MigrationInstallReceipt{}); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown install = %v, want ErrMigrationNotFound", err)
	}
	if err := node.ProposeRecordKBMigrationServe(ctx, "missing", storage.MigrationServeReceipt{}); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown serve = %v, want ErrMigrationNotFound", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := node.ProposeManageStorageNode(cancelled, storage.NodeChange{NodeID: 1}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled manage = %v, want context.Canceled", err)
	}
	if err := node.ProposeAckStorageCleanup(cancelled, "c"); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled ack = %v, want context.Canceled", err)
	}
	if err := node.ProposeActivateKBMigration(cancelled, "op", storage.MigrationActivation{}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled activation = %v, want context.Canceled", err)
	}

	// The maintenance read is the same directory the node serves, not a copy the
	// caller could mutate.
	dir, err := node.GetStorageMaintenance(ctx)
	if err != nil {
		t.Fatalf("maintenance read: %v", err)
	}
	if len(dir.Nodes) != 3 || !dir.Placements["kb"].Contains(1) {
		t.Errorf("maintenance directory = %+v", dir)
	}
}

// Service authorization proposals share one implementation; a resumed or blocked
// service is recorded in the same directory the transfer paths read.
func TestMockRaftNode_ServiceProposals(t *testing.T) {
	ctx := context.Background()
	node := mockStorageNode(t)

	if err := node.ProposeBlockStorageService(ctx, storage.ServiceRequest{NodeID: 3, OperationID: "block", ExpectedRevision: node.directory.Revision}); err != nil {
		t.Fatalf("block: %v", err)
	}
	block, ok := node.directory.ServiceBlocks[3]
	if !ok || block.OperationID != "block" {
		t.Fatalf("service block = %+v", block)
	}
	if err := node.ProposeResumeStorageService(ctx, storage.ServiceRequest{NodeID: 3, OperationID: "block", BlockRevision: block.Revision, ExpectedRevision: node.directory.Revision}); err != nil {
		t.Fatalf("resume: %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := node.ProposeBlockStorageService(cancelled, storage.ServiceRequest{NodeID: 1, OperationID: "x"}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled block = %v, want context.Canceled", err)
	}
	if err := node.ProposeResumeStorageService(cancelled, storage.ServiceRequest{NodeID: 1}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled resume = %v, want context.Canceled", err)
	}
}

// The maintenance read and the drain proposal both work through the mock's own
// directory, so a node with an unreferenced node can drain it and read its own
// obligations back.
func TestMockRaftNode_MaintenanceReadAndDrain(t *testing.T) {
	ctx := context.Background()
	node := mockStorageNode(t)

	// Node 3 holds no data, so draining it plans nothing and must succeed.
	if err := node.ProposeDrainStorageNode(ctx, storage.DrainRequest{OperationID: "drain", NodeID: 3, ExpectedRevision: node.directory.Revision, TargetIDs: []int64{1}}); err != nil {
		t.Fatalf("drain an unused node: %v", err)
	}
	if node.directory.Nodes[3].State != storage.NodeDraining {
		t.Errorf("node 3 = %+v, want draining", node.directory.Nodes[3])
	}

	dir, err := node.GetStorageMaintenance(ctx)
	if err != nil {
		t.Fatalf("maintenance read: %v", err)
	}
	if dir.Nodes[3].State != storage.NodeDraining {
		t.Errorf("maintenance read = %+v, want the same directory the proposals change", dir.Nodes[3])
	}

	// A cleanup obligation is acknowledged through the same path the scheduler uses.
	node.directory.Cleanups = map[string]storage.Cleanup{"c": {ID: "c", KBID: "kb", NodeID: 1}}
	if err := node.ProposeAckStorageCleanup(ctx, "c"); err != nil {
		t.Fatalf("ack cleanup: %v", err)
	}
	if !node.directory.Cleanups["c"].Done {
		t.Error("cleanup was not acknowledged")
	}
}
