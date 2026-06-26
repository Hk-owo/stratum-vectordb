// Package raft defines the RaftNode interface — strongly-consistent
// read/write access to knowledge base and version metadata, backed by the
// existing kvserver Raft implementation.
//
// See Stratum_接口设计v9.md "RaftNode" and Stratum_设计文档v10.md "版本元数据
// （Raft 状态机）" for the authoritative design. This file contains only the
// interface definition; the real implementation (RaftNodeImpl) is built in
// Phase 3, on top of the existing kvserver Raft.
package raft

import (
	"context"

	"stratum/internal/types"
)

// RaftNode encapsulates all Raft-backed operations on knowledge base and
// version metadata.
//
// Version ID allocation: ProposeCreateVersion does not accept a versionID
// parameter. During the state machine's apply phase, the implementation
// must, in order: (1) synchronously call WAL.WriteVersionID(versionID)
// (idempotent — a repeat write for the same versionID returns success
// immediately), and only after that succeeds (2) allocate the new version
// ID and write it into the state machine. This ordering guarantees that
// whenever the WAL has a VERSION_ID record, the state machine already has
// a corresponding version — eliminating orphan versions.
//
// Parent version constraint: ProposeCreateVersion validates, during apply,
// that the parent version belongs to the same knowledge base and is not in
// PENDING status. Forking (multiple child versions sharing one parent) is
// allowed.
//
// ProposeRemoveKBMeta idempotency: if the knowledge base's metadata no
// longer exists, this returns success rather than
// errors.ErrKnowledgeBaseNotFound, so the delete flow's crash-recovery
// path can safely re-execute this step any number of times.
//
// Knowledge base deletion has three state transitions: ProposeMarkKBDeleting
// sets KBStatusDeleting; ProposeRemoveKBMeta removes the metadata once
// cleanup has finished; ProposeMarkKBDeleteFailed sets KBStatusDeleteFailed
// once retries are exhausted, surfacing the failure to GetSystemStatus.
type RaftNode interface {
	// ProposeCreateKB commits new knowledge base metadata.
	ProposeCreateKB(ctx context.Context, kb types.KnowledgeBaseMeta) error

	// ProposeMarkKBDeleting marks kbID as KBStatusDeleting, rejecting new
	// queries and writes against it going forward.
	ProposeMarkKBDeleting(ctx context.Context, kbID string) error

	// ProposeMarkKBDeleteFailed marks kbID as KBStatusDeleteFailed after
	// the delete flow's retries are exhausted.
	ProposeMarkKBDeleteFailed(ctx context.Context, kbID string) error

	// ProposeRemoveKBMeta removes kbID's metadata entirely. Idempotent:
	// returns success if the metadata is already gone.
	ProposeRemoveKBMeta(ctx context.Context, kbID string) error

	// ProposeCreateVersion allocates and commits a new version for kbID,
	// with parentVersionID as its parent. Returns the newly allocated
	// version ID. See the type-level doc comment for the WAL ordering and
	// parent-version constraints enforced during apply.
	ProposeCreateVersion(ctx context.Context, kbID string, parentVersionID int64) (int64, error)

	// ProposeUpdateVersionStatus updates a version's IndexStatus (e.g. to
	// READY after a successful index build, or FAILED after a failed one).
	ProposeUpdateVersionStatus(ctx context.Context, versionID int64, status types.IndexStatus) error

	// ProposeRollback switches kbID's active version to targetVersionID.
	ProposeRollback(ctx context.Context, kbID string, targetVersionID int64) error

	// GetKB returns kbID's current metadata.
	GetKB(ctx context.Context, kbID string) (types.KnowledgeBaseMeta, error)

	// ListVersions returns the full version list for kbID.
	ListVersions(ctx context.Context, kbID string) ([]types.VersionMeta, error)

	// GetClusterStatus returns Raft cluster connectivity information,
	// independent of any specific knowledge base. Used by HealthCheck's
	// Raft connectivity probe.
	GetClusterStatus(ctx context.Context) (types.ClusterStatus, error)
}
