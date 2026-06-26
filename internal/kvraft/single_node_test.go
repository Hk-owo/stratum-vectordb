package kvraft

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"stratum/internal/kvstorage"
)

func newTestRaft(t *testing.T, id int64, opts ...Option) (*Raft, *Transport, chan ApplyMsg) {
	t.Helper()
	transport := NewTransport(nil)
	applyCh := make(chan ApplyMsg, 16)
	persister := kvstorage.NewPersister(filepath.Join(t.TempDir(), "raft"))
	allOpts := append([]Option{WithElectionTimeoutRange(30*time.Millisecond, 60*time.Millisecond), WithHeartbeatInterval(15 * time.Millisecond)}, opts...)
	rf := NewRaft(id, transport, applyCh, persister, allOpts...)
	t.Cleanup(rf.Stop)
	return rf, transport, applyCh
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// readUntilIndex reads from applyCh, discarding any messages (such as
// kvraft's automatic no-op entry proposed on election — see
// SendVoteRequest's doc comment) that don't carry the expected index, and
// returns the message once that index arrives. Fails the test if timeout
// elapses first.
func readUntilIndex(t *testing.T, applyCh chan ApplyMsg, wantIndex uint64, timeout time.Duration) ApplyMsg {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case msg := <-applyCh:
			if msg.Index == wantIndex {
				return msg
			}
		case <-deadline:
			t.Fatalf("timed out waiting for ApplyMsg with index %d", wantIndex)
			return ApplyMsg{}
		}
	}
}

func TestSingleNode_BecomesLeader(t *testing.T) {
	rf, _, _ := newTestRaft(t, 1)
	rf.Run()

	if !waitFor(t, 2*time.Second, rf.IsLeader) {
		t.Fatalf("single node never became leader")
	}
	id, known := rf.LeaderID()
	if !known || id != 1 {
		t.Fatalf("LeaderID() = (%d, %v), want (1, true)", id, known)
	}
}

func TestSingleNode_ProposeCommitsAndApplies(t *testing.T) {
	rf, _, applyCh := newTestRaft(t, 1)
	rf.Run()

	if !waitFor(t, 2*time.Second, rf.IsLeader) {
		t.Fatalf("never became leader")
	}

	index, term, err := rf.Propose(context.Background(), []byte("hello"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if index == 0 {
		t.Fatalf("Propose returned index 0, want a positive index")
	}
	if term == 0 {
		t.Fatalf("Propose returned term 0, want a positive term")
	}

	msg := readUntilIndex(t, applyCh, index, 2*time.Second)
	if msg.IsSnapshot {
		t.Fatalf("got snapshot ApplyMsg, want normal entry")
	}
	if string(msg.Command) != "hello" {
		t.Fatalf("ApplyMsg.Command = %q, want %q", msg.Command, "hello")
	}
}

func TestSingleNode_ProposeOnNonLeaderFails(t *testing.T) {
	rf, _, _ := newTestRaft(t, 1)
	// Deliberately do not call Run(): the node stays a fresh Follower
	// forever (no election timer running), so Propose must reject it.
	_, _, err := rf.Propose(context.Background(), []byte("x"))
	if err != ErrNotLeader {
		t.Fatalf("Propose on a non-leader = %v, want ErrNotLeader", err)
	}
}

func TestSingleNode_ProposeRespectsCancelledContext(t *testing.T) {
	rf, _, _ := newTestRaft(t, 1)
	rf.Run()
	waitFor(t, 2*time.Second, rf.IsLeader)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := rf.Propose(ctx, []byte("x"))
	if err == nil {
		t.Fatalf("Propose with cancelled context returned nil error")
	}
}

func TestSingleNode_PersistsAndRecoversStateAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	persisterPath := filepath.Join(dir, "raft")

	transport1 := NewTransport(nil)
	applyCh1 := make(chan ApplyMsg, 16)
	persister1 := kvstorage.NewPersister(persisterPath)
	rf1 := NewRaft(1, transport1, applyCh1, persister1,
		WithElectionTimeoutRange(30*time.Millisecond, 60*time.Millisecond),
		WithHeartbeatInterval(15*time.Millisecond))
	rf1.Run()
	if !waitFor(t, 2*time.Second, rf1.IsLeader) {
		t.Fatalf("never became leader")
	}
	if _, _, err := rf1.Propose(context.Background(), []byte("persisted-entry")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	// Give the apply loop a moment to actually apply it before we tear
	// down, so LastApplied reflects it.
	waitFor(t, time.Second, func() bool { return rf1.LastApplied() >= 1 })
	rf1.Stop()

	// Reopen against the same persisted state. The node ID need not match
	// the previous process's ID (a node's identity is this process's
	// configured role, not something baked into the persisted Raft log).
	transport2 := NewTransport(nil)
	applyCh2 := make(chan ApplyMsg, 16)
	persister2 := kvstorage.NewPersister(persisterPath)
	rf2 := NewRaft(1, transport2, applyCh2, persister2,
		WithElectionTimeoutRange(30*time.Millisecond, 60*time.Millisecond),
		WithHeartbeatInterval(15*time.Millisecond))
	t.Cleanup(rf2.Stop)

	if rf2.Term() < 1 {
		t.Fatalf("Term() after reopen = %d, want >= 1 (persisted term must survive restart)", rf2.Term())
	}
}

func TestSingleNode_StopIsGraceful(t *testing.T) {
	rf, _, _ := newTestRaft(t, 1)
	rf.Run()
	waitFor(t, 2*time.Second, rf.IsLeader)

	done := make(chan struct{})
	go func() {
		rf.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Stop() did not return within 5s")
	}

	select {
	case <-rf.Done():
	default:
		t.Fatalf("Done() channel not closed after Stop() returned")
	}
}

func TestSingleNode_StopIsIdempotent(t *testing.T) {
	rf, _, _ := newTestRaft(t, 1)
	rf.Run()
	waitFor(t, 2*time.Second, rf.IsLeader)

	rf.Stop()
	rf.Stop() // must not panic or block
}

func TestSingleNode_ClusterSizeIsOne(t *testing.T) {
	rf, _, _ := newTestRaft(t, 1)
	if got := rf.ClusterSize(); got != 1 {
		t.Fatalf("ClusterSize() = %d, want 1", got)
	}
}

func TestSingleNode_LeaderIDUnknownBeforeRun(t *testing.T) {
	rf, _, _ := newTestRaft(t, 1)
	_, known := rf.LeaderID()
	if known {
		t.Fatalf("LeaderID() known = true before Run(), want false")
	}
}
