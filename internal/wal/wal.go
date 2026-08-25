// Package wal defines the WAL interface — Stratum's crash-consistency
// mechanism for both the write path (CreateVersion) and the delete path
// (DeleteKnowledgeBase).
//
// See Stratum_接口设计v9.md "WAL" and Stratum_设计文档v10.md "事务保证（WAL）"
// and "删除知识库" for the authoritative design. This file contains only
// the interface definition; the on-disk implementation (WALImpl) is built
// in Phase 2 (2-A).
package wal

import (
	"context"

	"stratum/internal/types"
)

// WAL provides crash-consistency for two independent flows:
//
//  1. CreateVersion: WriteBegin -> [inside Raft apply: WriteVersionID] ->
//     ... storage writes ... -> WriteCommit.
//  2. DeleteKnowledgeBase: WriteDeleteMark -> ... cleanup steps ... ->
//     WriteDeleteComplete.
//
// Record semantics:
//   - WriteBegin: transaction-start marker carrying the transaction's full
//     replay input — kbID, parentVersionID and the complete changes list.
//     The recovery path for a VERSION_ID-without-COMMIT record replays
//     steps 3-6 of the write path from scratch, which requires the
//     original changes; persisting them here is what makes a full replay
//     possible (a crash before WriteCommit may have left only a subset of
//     the storage writes on disk, and that subset alone cannot be
//     inferred back into the missing documents).
//   - WriteVersionID: called by the Raft state machine's apply phase,
//     written before the version ID is assigned into the state machine.
//     Idempotent: writing the same versionID again returns success. On
//     replay, finding a VERSION_ID record means Raft apply already
//     completed for that version; the recovery path skips re-proposing
//     and resumes storage writes directly using the recorded versionID.
//   - WriteCommit: marks that all storage-layer writes for a version
//     completed successfully; carries the versionID.
//
// Crash recovery logic (see Recover):
//   - BEGIN with no VERSION_ID: Raft apply never completed; the state
//     machine has no corresponding version, so the caller simply
//     re-proposes from scratch. Recover returns no PendingRecord for this
//     case — there is nothing to resume.
//   - VERSION_ID with no COMMIT: Raft apply completed; Recover returns a
//     PendingRecord{Type: PendingRecordTypeVersionWrite, VersionID: ...,
//     ParentVersionID: ..., Changes: ...} so the caller can skip
//     re-proposing and replay storage writes from the start using the
//     recorded versionID and the transaction's persisted changes. Every
//     storage write is idempotent, so a full replay is always safe.
//     A PendingRecord with nil Changes was written by an older WAL format
//     (BEGIN carried an empty payload); it cannot be replayed
//     automatically and requires operator intervention.
//   - COMMIT present but the version's IndexStatus is still PENDING: the
//     caller checks whether the index file already exists on disk — if
//     so, propose READY directly; if not, trigger an async build. (This
//     case is detected by comparing Raft state machine status, not via
//     Recover — the WAL alone cannot know the version's IndexStatus.)
//   - DELETE_MARK with no DELETE_COMPLETE: Recover returns a
//     PendingRecord{Type: PendingRecordTypeDeleteMark, KBID: ...} so the
//     caller can resume the DeleteKnowledgeBase cleanup flow.
//   - VERSION_DELETE_MARK with no VERSION_DELETE_COMPLETE: Recover
//     returns a PendingRecord{Type: PendingRecordTypeVersionDelete,
//     KBID: ..., VersionID: ...} so the caller can resume the DeleteVersion
//     cleanup flow.
type WAL interface {
	// WriteBegin writes a transaction-start marker for a new CreateVersion
	// flow, persisting the replay input (kbID, parentVersionID, changes)
	// inside the record so crash recovery can replay the storage writes.
	WriteBegin(ctx context.Context, kbID string, parentVersionID int64, changes []types.DocChange) error

	// WriteVersionID records that Raft apply has assigned versionID to
	// the in-flight transaction. Idempotent: repeated calls with the same
	// versionID return success without producing duplicate records.
	WriteVersionID(ctx context.Context, versionID int64) error

	// WriteCommit marks that all storage-layer writes for versionID have
	// completed successfully.
	WriteCommit(ctx context.Context, versionID int64) error

	// WriteDeleteMark records that a DeleteKnowledgeBase flow has started
	// for kbID.
	WriteDeleteMark(ctx context.Context, kbID string) error

	// WriteDeleteComplete records that the DeleteKnowledgeBase cleanup for
	// kbID has finished.
	WriteDeleteComplete(ctx context.Context, kbID string) error

	// WriteVersionDeleteMark records that a DeleteVersion flow has started
	// for versionID within kbID. Idempotent per versionID.
	WriteVersionDeleteMark(ctx context.Context, kbID string, versionID int64) error

	// WriteVersionDeleteComplete records that the DeleteVersion cleanup
	// for versionID has finished. Idempotent per versionID.
	WriteVersionDeleteComplete(ctx context.Context, kbID string, versionID int64) error

	// Recover scans the WAL at startup and returns every PendingRecord
	// requiring crash-recovery handling. An empty slice means there is
	// nothing to recover.
	Recover(ctx context.Context) ([]types.PendingRecord, error)

	// GetReplayCounters returns the current in-memory replay-failure
	// counters (reset to zero on every process restart). Exposed to
	// GetSystemStatus so operators can see WAL records whose replay keeps
	// failing.
	GetReplayCounters() []types.ReplayCounter

	// IncrementReplayCounter records a replay failure against rec. Called
	// by the crash-recovery path when a PendingRecord cannot be replayed
	// (transient error or missing replay input). In-memory only.
	IncrementReplayCounter(rec types.PendingRecord)
}
