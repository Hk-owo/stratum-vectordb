package kvraft

// ApplyMsg is delivered on the channel passed to NewRaft once a log entry
// has been committed by a majority of the cluster (a normal entry), or
// when the state machine needs to take or install a snapshot.
//
// For a normal entry, Index, Term, and Command are set and IsSnapshot is
// false. The receiver (the Stratum-specific state-machine layer built on
// top of this package) is responsible for decoding Command and applying
// it; this package has no opinion on Command's encoding. Term is the term
// under which the entry was originally proposed — a caller that proposed
// an entry and is waiting to see it applied (correlating by the index
// Propose returned) should also check Term against the term Propose
// returned: if they differ, the original entry was never actually
// committed (it was overwritten by a different leader's entry at the
// same index, a normal and expected Raft occurrence after a leadership
// change), and what's being delivered now is a different command that
// happens to share the same index.
//
// For a snapshot, IsSnapshot is true. SnapshotData == nil means "the local
// log has grown past its size threshold; serialize your own state and
// call Raft.Snapshot" (a locally-triggered snapshot). SnapshotData != nil
// means "here is a snapshot received from the leader via InstallSnapshot;
// replace your state with it" (a remotely-installed snapshot).
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte

	IsSnapshot    bool
	SnapshotData  []byte
	SnapshotIndex uint64
}
