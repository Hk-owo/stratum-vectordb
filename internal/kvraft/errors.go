package kvraft

import "errors"

// ErrNotLeader is returned by Propose when called on a node that does not
// currently believe itself to be the Raft leader. Callers (the future
// Stratum RaftNodeImpl layer) are expected to either reject the request
// or, in a future iteration, redirect it to the current leader (see
// LeaderID).
var ErrNotLeader = errors.New("kvraft: not leader")

// ErrStopped is returned by operations attempted after Stop has been
// called on this node.
var ErrStopped = errors.New("kvraft: node stopped")
