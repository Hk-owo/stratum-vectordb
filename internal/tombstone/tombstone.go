// Package tombstone keeps this node's own record of versions the control layer
// has TOLD it are deleted — a local, durable "already gone" list.
//
// Why a local list at all, when the control layer already has the metadata: the
// two answer different questions at different moments.
//
//   - "is this version deleted?" is answered by the control layer, from CURRENT
//     state (the live chain plus the highest allocated id — see
//     plane.DeletedVersionLeftovers). It has no retention and no gaps, and it is
//     what the reverse reconciliation uses.
//   - "may this data land here?" has to be answered by the RECEIVING node, on the
//     landing path, without a round trip. After a deletion completes, its metadata
//     row is gone; a late push (a retry still in flight, a backfill running behind)
//     then arrives with nothing at the control layer that could name it. Without a
//     local record the node writes the data back and the only thing that will ever
//     notice is the next reverse reconciliation.
//
// So this is not a replacement for that reconciliation, and the distinction is
// deliberate: a tombstone is written only when this node has been CONFIRMED the
// deletion (DataSyncService.DeleteVersionData), and a node that never received
// that message has nothing here — which is exactly why the reconciliation stays.
// What this buys is the two windows the reconciliation cannot cover cheaply:
//
//   - the crash/restart window: the entry is on disk, so a node that dies between
//     "told" and "cleaned up" resumes by itself;
//   - the late-push window: the landing path refuses a version it knows is gone,
//     instead of waiting for a scan.
//
// An entry is Pending until this node's own reclaim of the version's bytes
// finishes, and Done afterwards. BOTH refuse data: "the notification arrived but
// the bytes are not gone yet" is not a reason to accept more of them. Done entries
// are kept for a grace window (see DefaultGrace) and then dropped, so the table
// stays bounded by (deletions in the window), not by the history of the cluster.
package tombstone

import (
	"context"
	"time"
)

// DefaultGrace is how long a DONE entry is kept before it is pruned.
//
// It exists to cover the last window: a reclaim that finished is not a promise
// that no copy of the data is still in flight — a push that was retrying, or a
// backfill that started before the deletion, can arrive afterwards. One hour is
// generous for that (the same order as the reconciliation's own patience) and it
// is a bound on the table's size, not a correctness boundary: an arrival after the
// window is the reverse reconciliation's job, and it is why that pass is kept.
const DefaultGrace = time.Hour

// State is where one version's local reclaim stands.
type State uint8

const (
	// Pending means this node knows the version is deleted and has not finished
	// reclaiming its bytes.
	Pending State = iota
	// Done means the reclaim finished; the entry stays for the grace window so a
	// late arrival is still refused.
	Done
)

// String is for logs and tests.
func (s State) String() string {
	switch s {
	case Pending:
		return "PENDING"
	case Done:
		return "DONE"
	default:
		return "UNKNOWN"
	}
}

// Entry is one deletion this node has been told about.
//
// VersionID == 0 names the knowledge base itself: a whole-KB deletion removes the KB
// and every version in one apply, so it is recorded as one entry and its reclaim is
// the KB-wide prefix delete. Without it, a replica that was unreachable while the KB
// was deleted would hold bytes nothing could ever name again — the KB is gone from the
// metadata, so the live-chain judgement cannot see it either.
type Entry struct {
	KBID      string
	VersionID int64
	State     State
	NotedAt   time.Time
	DoneAt    time.Time
}

// SyncState is this node's watermark into the control layer's deletion log. The
// epoch identifies the state machine that minted the sequence numbers: a rebuilt
// cluster mints a new one, and a watermark from another epoch must be treated as
// absent rather than as "I am ahead".
type SyncState struct {
	Epoch string
	Seq   int64
}

// DeletedVersion is one entry of the control layer's deletion log, in the shape this
// table takes it. It mirrors types.DeletedVersion; declaring it here keeps the
// contract readable without making the callers convert.
type DeletedVersion struct {
	Seq       int64
	KBID      string
	VersionID int64
}

// Store is the table. Every method is safe for concurrent use.
//
// The narrow surface is the point: writers are the deletion path (Note, MarkDone),
// readers are the landing paths (Contains) and this node's own retry loop
// (Pending, Prune). Nothing here decides anything — a tombstone is a fact someone
// else established.
type Store interface {
	// Note records that (kbID, versionID) was deleted on this node's behalf.
	// Idempotent: a repeated notification (the coordinator retries) must not reset a
	// DONE entry back to PENDING, or the retry would resurrect work that finished.
	Note(ctx context.Context, kbID string, versionID int64, at time.Time) error

	// MarkDone records that this node finished reclaiming the version's bytes.
	// Idempotent, and a no-op for a version that was never noted — "cleaned up
	// something I was never told about" is the reconciliation's outcome, not an
	// error.
	MarkDone(ctx context.Context, kbID string, versionID int64, at time.Time) error

	// Contains reports whether this node knows the version is deleted, in EITHER
	// state. A Pending entry counts: the notification arrived, so more of its data
	// must not land here.
	Contains(ctx context.Context, kbID string, versionID int64) (bool, error)

	// ContainsKB reports whether this node knows the whole knowledge base is
	// deleted. Separate from Contains because a version of a deleted KB has no entry
	// of its own — the KB-level entry is what refuses it.
	ContainsKB(ctx context.Context, kbID string) (bool, error)

	// Pending lists the entries whose reclaim has not finished, so this node can
	// retry them on its own cadence.
	Pending(ctx context.Context) ([]Entry, error)

	// Prune drops DONE entries older than grace and reports how many went.
	Prune(ctx context.Context, now time.Time, grace time.Duration) (int, error)

	// SyncSeq returns this node's watermark into the control layer's deletion log.
	// ok is false when there is none — a node that has never synced, or one whose
	// table was rebuilt — which the caller must treat as "re-judge from the live
	// chain", never as "start from zero and hope".
	SyncSeq(ctx context.Context) (state SyncState, ok bool, err error)

	// NoteBatch records a page of deletions AND advances the watermark in ONE atomic
	// write. The atomicity is the contract, not an optimisation: a watermark that
	// moved before the entries landed would lose those deletions permanently (the
	// control layer only serves the range once), while entries that landed before the
	// watermark only cost a re-read (Note is idempotent).
	NoteBatch(ctx context.Context, state SyncState, entries []DeletedVersion) error

	// Close releases the underlying storage.
	Close() error
}
