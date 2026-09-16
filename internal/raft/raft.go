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
	// IsLeader reports whether this node currently believes it leads the
	// cluster. The §7.13.2 dispatch hook reads it at APPLY time: that is when
	// the answer matters, since a forwarded proposal is applied by whoever leads
	// then, and a since-deposed leader must not dispatch.
	IsLeader() bool

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
	ProposeCreateVersion(ctx context.Context, kbID string, parentVersionID int64, opts ...ProposeOption) (int64, error)

	// ProposeUpdateVersionStatus updates a version's IndexStatus (e.g. to
	// READY after a successful index build, or FAILED after a failed one).
	//
	// nodeID names the replica whose own index became serviceable, and is
	// recorded so the control layer can answer "how many replicas are serving
	// this version" (§8.6(d)'s rolling cleanup asks exactly that before taking
	// one out of service). 0 means the control layer is setting the status
	// itself — a reconcile promotion or an availability verdict — and no replica
	// is recorded.
	ProposeUpdateVersionStatus(ctx context.Context, versionID int64, status types.IndexStatus, nodeID int64) error

	// ProposeMarkVersionFailedPermanent records the terminal verdict for one
	// side of a version (Stratum_设计文档v13.md §10.1, §10.1b): the control
	// layer has decided that side's retry budget is spent and nothing will
	// retry it automatically. side says which state the verdict lands in —
	// DataStatusFailedPermanent or IndexStatusFailedPermanent — and reason and
	// count form the auditable cause chain an operator needs.
	ProposeMarkVersionFailedPermanent(ctx context.Context, kbID string, versionID int64, side types.FailureSide, reason string, count int32) error

	// ProposeUpdateVersionSummary records the version's full document-ID
	// set hash (VersionMeta.DocIDSetHash). The leader calls this after its
	// storage-layer writes for the version have completed; followers use
	// the committed digest to verify DataSync pulls are complete. No-op
	// when the version does not exist (returns ErrVersionNotFound).
	ProposeUpdateVersionSummary(ctx context.Context, versionID int64, docIDSetHash string) error

	// ProposeMarkVersionDataDurable records that versionID's DATA is durable,
	// on the control layer's own authority rather than a writer's report
	// (Stratum_设计文档v13.md §10.1b). It is how the startup reconcile turns the
	// storage layer's reported cursor into replicated state (§7.9). Only a
	// PENDING data side is promoted: an already settled one — durable, or a
	// terminal verdict — is left exactly as it is.
	ProposeMarkVersionDataDurable(ctx context.Context, versionID int64) error

	// ProposeRollback switches kbID's active version to targetVersionID.
	// Rejected (ErrVersionDeleting) if the target version is being deleted.
	ProposeRollback(ctx context.Context, kbID string, targetVersionID int64) error

	// ProposeMarkVersionDeleting marks the version set selected by mode
	// relative to versionID (SUBTREE: versionID + descendants; SINGLE:
	// versionID alone, its children spliced onto its parent; ANCESTORS:
	// every ancestor of versionID, making versionID the new base) within
	// kbID as Deleting, kicking off the asynchronous DeleteVersion cleanup.
	// Returns the IDs actually marked. Rejected with ErrVersionIsActive if
	// the active version is in that set, or ErrVersionPending if any
	// version in it is still PENDING. Idempotent for an already-Deleting
	// set; ANCESTORS on a version that is already the base is a no-op and
	// returns an empty slice.
	ProposeMarkVersionDeleting(ctx context.Context, kbID string, versionID int64, mode types.VersionDeleteMode) ([]int64, error)

	// ProposeRemoveVersionMeta removes a single version's metadata from the
	// state machine. Idempotent: returns success if the version is already
	// gone, so the DeleteVersion cleanup's crash-recovery path can safely
	// re-propose this any number of times.
	ProposeRemoveVersionMeta(ctx context.Context, kbID string, versionID int64) error

	// GetKB returns kbID's current metadata.
	GetKB(ctx context.Context, kbID string) (types.KnowledgeBaseMeta, error)

	// ListVersions returns the full version list for kbID.
	ListVersions(ctx context.Context, kbID string) ([]types.VersionMeta, error)

	// ListKnowledgeBases returns metadata for every knowledge base known to
	// the Raft state machine. Used by the console (ListKnowledgeBases RPC)
	// and by GetSystemStatus to scan for stuck versions / delete-failed KBs.
	ListKnowledgeBases(ctx context.Context) ([]types.KnowledgeBaseMeta, error)

	// GetClusterStatus returns Raft cluster connectivity information,
	// independent of any specific knowledge base. Used by HealthCheck's
	// Raft connectivity probe.
	GetClusterStatus(ctx context.Context) (types.ClusterStatus, error)
}

// ProposeOption tweaks a ProposeCreateVersion call without changing the
// signature every caller uses.
type ProposeOption func(*proposeOptions)

type proposeOptions struct {
	clientRequestID string
}

// WithClientRequestID attaches the client's idempotency key to the call: a
// retry carrying the same key (for the same knowledge base) reuses the version
// the first attempt allocated instead of allocating another one — what lets a
// client re-send the changes for a version whose data never landed
// (Stratum_设计文档v13.md §7.12). An empty key means "no idempotency".
func WithClientRequestID(id string) ProposeOption {
	return func(o *proposeOptions) { o.clientRequestID = id }
}

func resolveProposeOptions(opts []ProposeOption) proposeOptions {
	var o proposeOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}
