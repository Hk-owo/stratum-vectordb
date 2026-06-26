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
	cmdCreateKB            commandType = "CreateKB"
	cmdMarkKBDeleting      commandType = "MarkKBDeleting"
	cmdMarkKBDeleteFailed  commandType = "MarkKBDeleteFailed"
	cmdRemoveKBMeta        commandType = "RemoveKBMeta"
	cmdCreateVersion       commandType = "CreateVersion"
	cmdUpdateVersionStatus commandType = "UpdateVersionStatus"
	cmdRollback            commandType = "Rollback"
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
	// cmdCreateVersion / cmdRollback
	KBID string `json:"kb_id,omitempty"`

	// cmdCreateVersion
	ParentVersionID int64 `json:"parent_version_id,omitempty"`

	// cmdUpdateVersionStatus
	VersionID int64             `json:"version_id,omitempty"`
	Status    types.IndexStatus `json:"status,omitempty"`

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
