package kvraft

import (
	"testing"

	kvraftpb "stratum/api/proto/kvraft"
)

// TestRaft_ReplicatedThrough_IsTheOldestPeerPosition pins the bound's meaning: it is
// the minimum over the peers' acknowledged positions, because that is the point up
// to which nothing can still be in flight. Callers prune state on it (a tombstone may
// be dropped only once every replica has certainly seen it).
func TestRaft_ReplicatedThrough_IsTheOldestPeerPosition(t *testing.T) {
	rf := &Raft{
		state:      Leader,
		log:        []*kvraftpb.Entry{{Index: 1}, {Index: 2}, {Index: 3}},
		matchIndex: map[int64]uint64{2: 3, 3: 2},
	}
	through, ok := rf.ReplicatedThrough()
	if !ok {
		t.Fatal("ReplicatedThrough on a leader must be known")
	}
	if through != 2 {
		t.Errorf("ReplicatedThrough = %d, want 2 (the slowest peer's position)", through)
	}
}

// TestRaft_ReplicatedThrough_IsUnknownOnAFollower: a follower does not track other
// nodes' progress, and a guessed or stale answer would let a caller drop state a
// replica has not seen yet. "Unknown" has to be sayable.
func TestRaft_ReplicatedThrough_IsUnknownOnAFollower(t *testing.T) {
	rf := &Raft{state: Follower}
	through, ok := rf.ReplicatedThrough()
	if ok {
		t.Errorf("ReplicatedThrough on a follower = (%d, true), want (_, false)", through)
	}
}
