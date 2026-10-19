package raft

import (
	"errors"
	"testing"
	"time"

	"stratum/internal/kvraft"
)

// A proposal that has been appended but never applied must not outlive the node.
// Every build-complete callback reports with context.Background(), so a waiter with
// no deadline is the NORMAL case — and Stop used to leave it parked forever, which
// made "stop a node" wait on the index manager's build pool until the test binary's
// own timeout fired (measured: 9 minutes, integration
// TestRealStack_ThreeNodeCluster_FaultTolerance).
func TestRaftNodeImpl_StopSettlesAWaitingProposal(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)

	// The state a lost apply leaves behind: a registered waiter and no entry left to
	// deliver. Registering it by hand is the only way to hold this state still — a
	// single-node cluster applies everything it appends.
	resultCh := make(chan applyResult, 1)
	impl.pendingMu.Lock()
	impl.pending[42] = &pendingProposal{term: impl.raft.Term(), resultCh: resultCh}
	impl.pendingMu.Unlock()

	impl.Stop()

	select {
	case res := <-resultCh:
		if !errors.Is(res.Err, kvraft.ErrStopped) {
			t.Fatalf("waiter settled with %v, want kvraft.ErrStopped", res.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop left a waiting proposal unsettled: a caller with no deadline blocks for the rest of the process")
	}

	impl.pendingMu.Lock()
	left := len(impl.pending)
	impl.pendingMu.Unlock()
	if left != 0 {
		t.Errorf("pending proposals after Stop = %d, want 0", left)
	}
}
