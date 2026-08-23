package coordinator

import "context"

// DeleteVersionCoordinator owns the asynchronous cleanup orchestration that
// runs after DeleteVersion marks a version (and its recursive descendants)
// as Deleting in the Raft state machine.
//
// The full cleanup flow, executed for every version of kbID currently
// marked Deleting (discovered via RaftNode.ListVersions — the mark step
// already stamped the whole subtree):
//
//  1. WAL.WriteVersionDeleteMark (idempotent per version; records that the
//     cleanup started so Recover can resume it after a crash)
//  2. IndexManager.Discard (evict in-memory + reset the vecstore-side
//     index for the version)
//  3. VersionDocList.DeleteByVersion
//  4. DocStore.DeleteByVersion (physical MVCC record cleanup, full scan)
//  5. RaftNode.ProposeRemoveVersionMeta (idempotent)
//  6. WAL.WriteVersionDeleteComplete
//
// Idempotency: every storage step is a filter/prefix delete, the metadata
// removal is idempotent, and the WAL writes are idempotent per version — so
// the whole flow can safely re-run from scratch after a crash at any step
// (or when the operator re-triggers DeleteVersion for a still-Deleting
// version). Permanent errors: each step retries with exponential backoff;
// once retries are exhausted, Execute returns the error and leaves the
// version Deleting so it stays visible via GetSystemStatus for operator
// intervention.
type DeleteVersionCoordinator interface {
	// Execute runs the full cleanup for every Deleting version of kbID.
	Execute(ctx context.Context, kbID string) error
}
