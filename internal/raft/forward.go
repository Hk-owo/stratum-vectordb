package raft

import "context"

// ProposeForwarder carries a proposal to the node that can actually append it.
//
// Raft only appends on the leader — kvraft.Raft.Propose answers ErrNotLeader
// otherwise. But a node that is not the leader may still have a fact to report:
// a replica that just finished writing a version, or any node running its
// startup reconcile. This interface is that missing path
// (Stratum_设计文档v13.md §7.3/§7.8).
//
// It lives here as an interface (not an implementation) so internal/raft keeps
// no transport dependency; the node assembly wires the gRPC client.
type ProposeForwarder interface {
	// ForwardPropose runs the encoded command on the node identified by
	// leaderID and returns its apply outcome.
	ForwardPropose(ctx context.Context, leaderID int64, cmd []byte) (ForwardedResult, error)
}

// ForwardedResult is what a forwarded proposal yields, in exported form so the
// interface can be implemented outside this package (applyResult itself is
// package-private).
type ForwardedResult struct {
	VersionID         int64
	DeletedVersionIDs []int64
	// Err is the leader's apply error, rebuilt from its wire name so
	// errors.Is keeps working across the forward.
	Err error
}
