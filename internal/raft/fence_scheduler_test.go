package raft

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"stratum/internal/storage"
)

type lostFenceAck struct {
	*MockRaftNode
	lose bool
}

func (r *lostFenceAck) ProposeAckKBMigrationFence(ctx context.Context, op string, epoch uint64, id int64) error {
	if err := r.MockRaftNode.ProposeAckKBMigrationFence(ctx, op, epoch, id); err != nil {
		return err
	}
	if r.lose {
		r.lose = false
		return context.DeadlineExceeded
	}
	return nil
}

func TestMigrationFenceSchedulerResumesPartialAcksAndNeverShrinksRequirement(t *testing.T) {
	r, req := migrationNode(t)
	ctx := context.Background()
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	manager := &lostFenceAck{MockRaftNode: r, lose: true}
	offline := true
	counts := map[int64]int{}
	fence := func(_ context.Context, node storage.Node, m storage.Migration) error {
		counts[node.ID]++
		if m.Phase != storage.MigrationFencing {
			t.Fatal("task RPC issued before fencing intent was committed")
		}
		if node.ID == 4 && offline {
			return errors.New("offline")
		}
		return nil
	}
	s := &storage.FenceScheduler{Manager: manager, IsLeader: func() bool { return true }, Versions: r.ListVersions, Fence: fence}
	if err := s.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Step(ctx); err == nil {
		t.Fatal("lost ACK/offline node not surfaced")
	}
	d, err := r.StorageDirectory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m := d.Migrations[req.OperationID]
	if m.Phase != storage.MigrationFencing || !reflect.DeepEqual(m.FencedIDs, []int64{1, 2, 3}) {
		t.Fatal("offline member disappeared or stored ACK was lost:", m)
	}
	offline = false
	// A fresh scheduler represents a control leader change. It only reissues
	// missing work; response loss must not discard the already committed ACK.
	s = &storage.FenceScheduler{Manager: r, IsLeader: func() bool { return true }, Versions: r.ListVersions, Fence: fence}
	if err := s.Step(ctx); err != nil {
		t.Fatal(err)
	}
	d, _ = r.StorageDirectory(ctx)
	if d.Migrations[req.OperationID].Phase != storage.MigrationFenced {
		t.Fatal("preparation did not resume")
	}
	if !reflect.DeepEqual(counts, map[int64]int{1: 1, 2: 1, 3: 1, 4: 2}) {
		t.Fatal("resumed finished work:", counts)
	}
	if d.Placements["A"].Contains(4) {
		t.Fatal("scheduler switched without any data proof")
	}
	if err := s.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if counts[4] != 2 {
		t.Fatal("prepared task issued more RPCs")
	}
}

func TestMigrationFenceSchedulerStopsIssuingTasksAfterLeadershipLoss(t *testing.T) {
	r, req := migrationNode(t)
	ctx := context.Background()
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	leader := true
	calls := 0
	s := &storage.FenceScheduler{Manager: r, IsLeader: func() bool { return leader }, Versions: r.ListVersions, Fence: func(context.Context, storage.Node, storage.Migration) error { calls++; leader = false; return nil }}
	_ = s.Step(ctx)
	d, _ := r.StorageDirectory(ctx)
	if calls != 1 || len(d.Migrations[req.OperationID].FencedIDs) != 0 {
		t.Fatal("old leader kept acknowledging/issuing work:", calls, d.Migrations[req.OperationID])
	}
}
