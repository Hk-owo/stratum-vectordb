package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// applyResult is what a single command application produces: an error if
// the command was rejected by a deterministic validation check (e.g. an
// invalid parent version), or, for cmdCreateVersion specifically, the
// newly assigned VersionID.
type applyResult struct {
	VersionID int64
	Err       error
}

// stateMachine holds the in-memory knowledge base and version metadata
// replicated via Raft. It is mutated exclusively by stateMachine.apply,
// called from RaftNodeImpl's single apply-dispatch loop — so apply itself
// needs no internal synchronization against concurrent writers, but reads
// (GetKB, ListVersions) can happen concurrently from other goroutines and
// so still need the mutex.
type stateMachine struct {
	mu            sync.RWMutex
	kbs           map[string]types.KnowledgeBaseMeta
	versions      map[int64]types.VersionMeta
	versionsByKB  map[string][]int64
	nextVersionID int64
}

func newStateMachine() *stateMachine {
	return &stateMachine{
		kbs:           make(map[string]types.KnowledgeBaseMeta),
		versions:      make(map[int64]types.VersionMeta),
		versionsByKB:  make(map[string][]int64),
		nextVersionID: 1,
	}
}

// apply deterministically applies cmd to the state machine. Called from
// the single apply-dispatch loop, identically (same input, same code
// path) on every node that applies this log entry — followers included,
// not just whichever node originally proposed it. w is the Stratum WAL
// (used only by cmdCreateVersion, for the WAL-before-state-machine
// ordering described on RaftNodeImpl).
func (sm *stateMachine) apply(ctx context.Context, cmd command, w wal.WAL, logger *zap.Logger) applyResult {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	switch cmd.Type {
	case cmdCreateKB:
		kb := *cmd.KB
		if kb.Status == types.KBStatusActive || kb.Status == 0 {
			kb.Status = types.KBStatusActive
		}
		sm.kbs[kb.KBID] = kb
		return applyResult{}

	case cmdMarkKBDeleting:
		kb, ok := sm.kbs[cmd.KBID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}
		}
		kb.Status = types.KBStatusDeleting
		sm.kbs[cmd.KBID] = kb
		return applyResult{}

	case cmdMarkKBDeleteFailed:
		kb, ok := sm.kbs[cmd.KBID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}
		}
		kb.Status = types.KBStatusDeleteFailed
		sm.kbs[cmd.KBID] = kb
		return applyResult{}

	case cmdRemoveKBMeta:
		// Idempotent: deleting an already-absent key is a no-op, not an
		// error, so the delete flow's crash-recovery path can safely
		// re-propose this any number of times.
		delete(sm.kbs, cmd.KBID)
		for _, versionID := range sm.versionsByKB[cmd.KBID] {
			delete(sm.versions, versionID)
		}
		delete(sm.versionsByKB, cmd.KBID)
		return applyResult{}

	case cmdCreateVersion:
		return sm.applyCreateVersion(ctx, cmd, w, logger)

	case cmdUpdateVersionStatus:
		v, ok := sm.versions[cmd.VersionID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrVersionNotFound}
		}
		v.IndexStatus = cmd.Status
		sm.versions[cmd.VersionID] = v
		return applyResult{}

	case cmdUpdateVersionSummary:
		v, ok := sm.versions[cmd.VersionID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrVersionNotFound}
		}
		v.DocIDSetHash = cmd.DocIDSetHash
		sm.versions[cmd.VersionID] = v
		return applyResult{}

	case cmdRollback:
		kb, ok := sm.kbs[cmd.KBID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}
		}
		v, ok := sm.versions[cmd.TargetVersionID]
		if !ok || v.KBID != cmd.KBID {
			return applyResult{Err: stratumerrors.ErrVersionNotFound}
		}
		kb.ActiveVersionID = cmd.TargetVersionID
		sm.kbs[cmd.KBID] = kb
		return applyResult{}

	default:
		return applyResult{Err: fmt.Errorf("raft: unknown command type %q", cmd.Type)}
	}
}

// applyCreateVersion handles cmdCreateVersion: validates the parent-version
// constraints, deterministically assigns the next version ID from the
// replicated counter, durably records the pending write in the local WAL
// (per the critical WAL-before-state-machine ordering — see the
// RaftNodeImpl doc comment), and finally writes the new version into the
// state machine. Must be called with sm.mu held.
func (sm *stateMachine) applyCreateVersion(ctx context.Context, cmd command, w wal.WAL, logger *zap.Logger) applyResult {
	if _, ok := sm.kbs[cmd.KBID]; !ok {
		return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}
	}

	if cmd.ParentVersionID != 0 {
		parent, ok := sm.versions[cmd.ParentVersionID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrInvalidParentVersion}
		}
		if parent.KBID != cmd.KBID {
			return applyResult{Err: fmt.Errorf("parent version %d belongs to a different knowledge base: %w", cmd.ParentVersionID, stratumerrors.ErrInvalidParentVersion)}
		}
		if parent.IndexStatus == types.IndexStatusPending {
			return applyResult{Err: fmt.Errorf("parent version %d is PENDING: %w", cmd.ParentVersionID, stratumerrors.ErrInvalidParentVersion)}
		}
	}

	versionID := sm.nextVersionID
	sm.nextVersionID++

	// Critical ordering: WAL.WriteVersionID before the version is written
	// into the state machine. This runs identically on every node that
	// applies this entry — not just the leader — because any node could
	// become leader after a restart and would then need its OWN WAL to
	// correctly detect and resume an incomplete write (see
	// Stratum_设计文档v10.md "关键时序约束").
	//
	// A WAL write failure here is treated as a local durability problem,
	// not a reason to diverge this node's replicated state from its
	// peers: the state machine update proceeds regardless, with the
	// failure logged loudly for operator attention. Aborting the state
	// machine update on a local WAL failure would make this node's
	// applied state disagree with every other node that succeeded,
	// breaking Raft's core replication invariant.
	if err := w.WriteVersionID(ctx, versionID); err != nil {
		logger.Error("WAL.WriteVersionID failed during apply; continuing to avoid replicated-state divergence, but this node's crash recovery for this version is now at risk and needs operator attention",
			zap.Int64("version_id", versionID), zap.String("kb_id", cmd.KBID), zap.Error(err))
	}

	sm.versions[versionID] = types.VersionMeta{
		VersionID:       versionID,
		ParentVersionID: cmd.ParentVersionID,
		KBID:            cmd.KBID,
		CreatedAt:       time.Now().Unix(),
		IndexStatus:     types.IndexStatusPending,
	}
	sm.versionsByKB[cmd.KBID] = append(sm.versionsByKB[cmd.KBID], versionID)

	return applyResult{VersionID: versionID}
}

// snapshotState is the gob-serializable form of the full state machine,
// used by serialize/restore for Raft log compaction.
type snapshotState struct {
	KBs           map[string]types.KnowledgeBaseMeta
	Versions      map[int64]types.VersionMeta
	VersionsByKB  map[string][]int64
	NextVersionID int64
}

// serialize encodes the full current state machine for a Raft snapshot.
func (sm *stateMachine) serialize() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	snap := snapshotState{
		KBs:           sm.kbs,
		Versions:      sm.versions,
		VersionsByKB:  sm.versionsByKB,
		NextVersionID: sm.nextVersionID,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snap); err != nil {
		return nil, fmt.Errorf("raft: serialize state machine snapshot: %w", err)
	}
	return buf.Bytes(), nil
}

// restore replaces the state machine's contents with a previously
// serialized snapshot.
func (sm *stateMachine) restore(data []byte) error {
	var snap snapshotState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		return fmt.Errorf("raft: restore state machine snapshot: %w", err)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.kbs = snap.KBs
	sm.versions = snap.Versions
	sm.versionsByKB = snap.VersionsByKB
	sm.nextVersionID = snap.NextVersionID
	if sm.kbs == nil {
		sm.kbs = make(map[string]types.KnowledgeBaseMeta)
	}
	if sm.versions == nil {
		sm.versions = make(map[int64]types.VersionMeta)
	}
	if sm.versionsByKB == nil {
		sm.versionsByKB = make(map[string][]int64)
	}
	return nil
}
