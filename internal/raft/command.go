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

	// cmdMarkVersionFailedPermanent records the control layer's verdict that a
	// version has spent its retry budget and will not be retried again
	// (Stratum_设计文档v13.md §10.1).
	cmdMarkVersionFailedPermanent commandType = "MarkVersionFailedPermanent"

	// cmdMarkDataDurable records the DATA side as durable on the control layer's
	// own authority rather than a writer's report: the startup reconcile knows a
	// version's data sits on a quorum's worth of disks, and this is how that fact
	// becomes replicated state (Stratum_设计文档v13.md §10.1b, §7.9).
	cmdMarkDataDurable commandType = "MarkDataDurable"
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

	// cmdMarkVersionDeleting: which versions to remove relative to
	// VersionID (subtree / single-with-splice / ancestors). Zero value
	// (VersionDeleteSubtree) keeps the historical behavior.
	Mode types.VersionDeleteMode `json:"mode,omitempty"`

	// cmdUpdateVersionSummary
	DocIDSetHash string `json:"doc_id_set_hash,omitempty"`

	// cmdMarkVersionFailedPermanent: the cause chain an operator needs.
	FailureReason string `json:"failure_reason,omitempty"`
	FailureCount  int32  `json:"failure_count,omitempty"`
	// cmdMarkVersionFailedPermanent: which side the verdict lands on
	// (Stratum_设计文档v13.md §10.1b). Omitted means the data side, which is
	// what every command written before the two sides were separated meant.
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

func newRemoveVersionMetaCommand(kbID string, versionID int64) command {
	return command{Type: cmdRemoveVersionMeta, KBID: kbID, VersionID: versionID}
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

// newMarkDataDurableCommand is the control layer's own data-side promotion
// (Stratum_设计文档v13.md §10.1b): no KB ID, because the version ID is globally
// unique and the state machine looks the version up by it.
func newMarkDataDurableCommand(versionID int64) command {
	return command{Type: cmdMarkDataDurable, VersionID: versionID}
}
