package raft

import (
	"encoding/json"
	"fmt"

	"stratum/internal/types"
)

// commandType identifies the kind of state-machine mutation a command
// encodes.
type commandType string

const (
	cmdCreateKB             commandType = "CreateKB"
	cmdMarkKBDeleting       commandType = "MarkKBDeleting"
	cmdMarkKBDeleteFailed   commandType = "MarkKBDeleteFailed"
	cmdRemoveKBMeta         commandType = "RemoveKBMeta"
	cmdCreateVersion        commandType = "CreateVersion"
	cmdUpdateVersionStatus  commandType = "UpdateVersionStatus"
	cmdUpdateVersionSummary commandType = "UpdateVersionSummary"
	cmdRollback             commandType = "Rollback"
	cmdMarkVersionDeleting  commandType = "MarkVersionDeleting"
	cmdRemoveVersionMeta    commandType = "RemoveVersionMeta"
	cmdPruneTombstones      commandType = "PruneTombstones"

	// cmdMarkVersionFailedPermanent records the control layer's verdict that a
	// version has spent its retry budget and will not be retried again
	// (Stratum_设计文档v13.md §10.1).
	cmdMarkVersionFailedPermanent commandType = "MarkVersionFailedPermanent"

	// cmdRetryVersion is an OPERATOR's revocation of one side's terminal verdict
	// (Stratum_设计文档v13.md §10.1): it puts that side back to PENDING and drops the
	// recorded cause chain once neither side is terminal any more.
	//
	// It exists because "nothing re-triggers it automatically" is a statement about
	// automatic paths, not a prohibition on humans: an operator who decides the
	// verdict was wrong has to be able to say so. Before this the only way to act on
	// such a decision was RebuildIndex's bare status overwrite, which left the old
	// cause chain (`FailureReason` / `FailureCount` / `FailureSide`) in place — so
	// GetSystemStatus kept describing a failure that had just been revoked.
	cmdRetryVersion commandType = "RetryVersion"

	// cmdMarkDataDurable records the DATA side as durable on the control layer's
	// own authority rather than a writer's report: the startup reconcile knows a
	// version's data sits on a quorum's worth of disks, and this is how that fact
	// becomes replicated state (Stratum_设计文档v13.md §10.1b, §7.9).
	cmdMarkDataDurable commandType = "MarkDataDurable"

	// cmdDiscardVersion is the CALLER's declaration that it is abandoning a
	// version whose write never landed — the counterpart of §7.12's re-send,
	// for a caller that no longer holds the changes
	// (docs/await-version-plan.md §7 Step 6). It is deliberately not a
	// DeleteVersion: discarding has nothing to reclaim, so the whole operation
	// is removing metadata that never came with data.
	cmdDiscardVersion commandType = "DiscardVersion"
)

// command is the JSON-encoded payload carried inside each kvraft log
// entry. JSON (rather than gob or protobuf) was chosen for this
// admittedly low-traffic, low-volume control-plane command stream purely
// for debuggability: log entries dumped during troubleshooting are
// human-readable without needing a decoder.
type command struct {
	Type commandType `json:"type"`

	// cmdCreateKB
	KB *types.KnowledgeBaseMeta `json:"kb,omitempty"`

	// cmdMarkKBDeleting / cmdMarkKBDeleteFailed / cmdRemoveKBMeta /
	// cmdCreateVersion / cmdRollback / cmdMarkVersionDeleting /
	// cmdRemoveVersionMeta
	KBID string `json:"kb_id,omitempty"`

	// cmdCreateVersion
	ParentVersionID int64 `json:"parent_version_id,omitempty"`
	// cmdCreateVersion: optional client idempotency key. Re-applying a command
	// with the same (KBID, ClientRequestID) returns the version the first
	// apply allocated instead of allocating another one
	// (Stratum_设计文档v13.md §7.12).
	ClientRequestID string `json:"client_request_id,omitempty"`

	// cmdUpdateVersionStatus / cmdMarkVersionDeleting / cmdRemoveVersionMeta
	VersionID int64             `json:"version_id,omitempty"`
	Status    types.IndexStatus `json:"status,omitempty"`

	// cmdUpdateVersionStatus: which NODE is reporting, when the command carries a
	// replica's own fact rather than the control layer's verdict. Zero means "no
	// replica is being recorded" — the control layer setting a status itself
	// (a reconcile promotion, an availability verdict). Recording a node for
	// those would make the §8.6(d) service-capacity count lie.
	NodeID int64 `json:"node_id,omitempty"`

	// cmdPruneTombstones: tombstones recorded at or below this log index may be
	// dropped (see applyPruneTombstones). It is a LOG POSITION rather than a
	// version number because tombstones are not created in version order — a
	// middle version can be deleted long after a later one — so only the log
	// order can say "every replica has certainly seen this one".
	ThroughIndex uint64 `json:"through_index,omitempty"`

	// Index is the log position this entry was applied at, filled by the apply
	// loop from the ApplyMsg. It is never encoded and never proposed: a command
	// cannot know its own future position. A tombstone records it so pruning can
	// wait until every replica holds that position. Zero means "unknown", which
	// only ever keeps a tombstone LONGER — the safe direction.
	Index uint64 `json:"-"`

	// cmdMarkVersionDeleting: which versions to remove relative to
	// VersionID (subtree / single-with-splice / ancestors). Zero value
	// (VersionDeleteSubtree) keeps the historical behavior.
	Mode types.VersionDeleteMode `json:"mode,omitempty"`

	// cmdUpdateVersionSummary
	DocIDSetHash string `json:"doc_id_set_hash,omitempty"`

	// cmdMarkVersionFailedPermanent: the cause chain an operator needs.
	FailureReason string `json:"failure_reason,omitempty"`
	FailureCount  int32  `json:"failure_count,omitempty"`
	// cmdMarkVersionFailedPermanent / cmdRetryVersion: which side the command
	// settles (Stratum_设计文档v13.md §10.1b). Omitted means the data side, which is
	// what every command written before the two sides were separated meant — and for
	// the retry command the same zero value keeps that reading rather than falling
	// through to a convenient default: the state machine refuses a data-side retry
	// outright instead of treating an unset field as "the index side, then".
	FailureSide types.FailureSide `json:"failure_side,omitempty"`

	// cmdRollback
	TargetVersionID int64 `json:"target_version_id,omitempty"`
}

func encodeCommand(c command) ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("raft: encode command: %w", err)
	}
	return data, nil
}

func decodeCommand(data []byte) (command, error) {
	var c command
	if err := json.Unmarshal(data, &c); err != nil {
		return command{}, fmt.Errorf("raft: decode command: %w", err)
	}
	return c, nil
}

// Command constructors.
//
// Both the local RaftNodeImpl (which appends to this node's own Raft) and
// RemoteRaftNode (which forwards the command to whichever control node leads)
// build their proposals through these. Sharing them is what keeps the two
// paths from drifting into encoding different commands for one logical
// operation: such a drift would not fail a build, it would surface as a
// storage node reporting something subtly different from what a same-process
// node reports — and only under the split topology.

func newCreateKBCommand(kb types.KnowledgeBaseMeta) command {
	return command{Type: cmdCreateKB, KB: &kb}
}

func newMarkKBDeletingCommand(kbID string) command {
	return command{Type: cmdMarkKBDeleting, KBID: kbID}
}

func newMarkKBDeleteFailedCommand(kbID string) command {
	return command{Type: cmdMarkKBDeleteFailed, KBID: kbID}
}

func newRemoveKBMetaCommand(kbID string) command {
	return command{Type: cmdRemoveKBMeta, KBID: kbID}
}

func newCreateVersionCommand(kbID string, parentVersionID int64, clientRequestID string) command {
	return command{
		Type:            cmdCreateVersion,
		KBID:            kbID,
		ParentVersionID: parentVersionID,
		ClientRequestID: clientRequestID,
	}
}

// newUpdateVersionStatusCommand builds the status update. nodeID is the replica
// whose own index just became serviceable, or 0 when the control layer is setting
// the status itself (see command.NodeID).
func newUpdateVersionStatusCommand(versionID int64, status types.IndexStatus, nodeID int64) command {
	return command{Type: cmdUpdateVersionStatus, VersionID: versionID, Status: status, NodeID: nodeID}
}

func newUpdateVersionSummaryCommand(versionID int64, docIDSetHash string) command {
	return command{Type: cmdUpdateVersionSummary, VersionID: versionID, DocIDSetHash: docIDSetHash}
}

func newRollbackCommand(kbID string, targetVersionID int64) command {
	return command{Type: cmdRollback, KBID: kbID, TargetVersionID: targetVersionID}
}

func newMarkVersionDeletingCommand(kbID string, versionID int64, mode types.VersionDeleteMode) command {
	return command{Type: cmdMarkVersionDeleting, KBID: kbID, VersionID: versionID, Mode: mode}
}

func newPruneTombstonesCommand(throughIndex uint64) command {
	return command{Type: cmdPruneTombstones, ThroughIndex: throughIndex}
}

func newRemoveVersionMetaCommand(kbID string, versionID int64) command {
	return command{Type: cmdRemoveVersionMeta, KBID: kbID, VersionID: versionID}
}

func newDiscardVersionCommand(kbID string, versionID int64) command {
	return command{Type: cmdDiscardVersion, KBID: kbID, VersionID: versionID}
}

func newMarkVersionFailedPermanentCommand(kbID string, versionID int64, side types.FailureSide, reason string, count int32) command {
	return command{
		Type:          cmdMarkVersionFailedPermanent,
		KBID:          kbID,
		VersionID:     versionID,
		FailureSide:   side,
		FailureReason: reason,
		FailureCount:  count,
	}
}

// newRetryVersionCommand builds the operator's revocation of one side's terminal
// verdict (see cmdRetryVersion).
func newRetryVersionCommand(kbID string, versionID int64, side types.FailureSide) command {
	return command{Type: cmdRetryVersion, KBID: kbID, VersionID: versionID, FailureSide: side}
}

// newMarkDataDurableCommand is the control layer's own data-side promotion
// (Stratum_设计文档v13.md §10.1b): no KB ID, because the version ID is globally
// unique and the state machine looks the version up by it.
func newMarkDataDurableCommand(versionID int64) command {
	return command{Type: cmdMarkDataDurable, VersionID: versionID}
}
