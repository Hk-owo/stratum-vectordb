package raft

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// newDeletionEpoch mints the identity of one state machine's deletion log. Random
// rather than derived: the whole point is to be different in a rebuilt cluster, and
// a counter or a timestamp can collide with the previous life.
func newDeletionEpoch() string {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		// No randomness means no way to tell "another cluster" apart from "a lagging
		// replica", which is the difference between a full re-judge and a skip. A
		// fixed value would make it silently wrong, so refuse to guess.
		panic(fmt.Sprintf("raft: no randomness for the deletion epoch: %v", err))
	}
	return hex.EncodeToString(suffix[:])
}

// recordDeletion appends one completed deletion to the bounded log. Must be called
// with sm.mu held.
func (sm *stateMachine) recordDeletion(kbID string, versionID int64) {
	sm.deletedSeq++
	sm.deletedLog = append(sm.deletedLog, types.DeletedVersion{
		Seq:       sm.deletedSeq,
		KBID:      kbID,
		VersionID: versionID,
	})
	limit := sm.deletedLogLimitOrDefault()
	if over := len(sm.deletedLog) - limit; over > 0 {
		// Copy rather than reslice: reslicing would keep the trimmed entries (and
		// their strings) reachable from the backing array for as long as the log
		// lives.
		kept := make([]types.DeletedVersion, limit)
		copy(kept, sm.deletedLog[over:])
		sm.deletedLog = kept
	}
}

// applyKB applies knowledge-base metadata lifecycle commands.
// The caller holds sm.mu.
func (sm *stateMachine) applyKB(cmd command) (applyResult, bool) {
	switch cmd.Type {
	case cmdCreateKB:
		if cmd.KB == nil || cmd.KB.KBID == "" {
			return applyResult{Err: stratumerrors.ErrInvalidArgument}, true
		}
		kb := *cmd.KB
		if cmd.Placement != nil {
			if _, exists := sm.kbs[kb.KBID]; exists {
				return applyResult{Err: stratumerrors.ErrPlacementChangeUnsafe}, true
			}
			if cmd.Placement.KBID != kb.KBID {
				return applyResult{Err: stratumerrors.ErrInvalidArgument}, true
			}
			if len(sm.directory.Nodes) == 0 {
				return applyResult{Err: stratumerrors.ErrPlacementUnknown}, true
			}
		}
		if _, deleted := sm.directory.Tombstones[kb.KBID]; deleted {
			return applyResult{Err: stratumerrors.ErrKnowledgeBaseDeleted}, true
		}
		// Never overwrite business metadata under an already registered identity.
		if _, assigned := sm.directory.Placements[kb.KBID]; assigned {
			return applyResult{Err: stratumerrors.ErrInvalidArgument}, true
		}
		if kb.Status == types.KBStatusActive || kb.Status == 0 {
			kb.Status = types.KBStatusActive
		}
		if cmd.Placement != nil && kb.Status != types.KBStatusActive {
			return applyResult{Err: stratumerrors.ErrInvalidArgument}, true
		}
		// Creation and the original full placement share one committed command.
		// Scheduling and RPCs remain outside apply.
		if len(sm.directory.Nodes) > 0 && kb.Status == types.KBStatusActive {
			p := sm.directory.InitialPlacement(kb.KBID, "create/"+kb.KBID)
			if cmd.Placement != nil {
				p = cmd.Placement.Clone()
			}
			if err := sm.directory.AssignNew(p); err != nil {
				return applyResult{Err: err}, true
			}
		}
		sm.kbs[kb.KBID] = kb
		return applyResult{}, true

	case cmdMarkKBDeleting:
		kb, ok := sm.kbs[cmd.KBID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}, true
		}
		kb.Status = types.KBStatusDeleting
		sm.directory.Delete(cmd.KBID)
		sm.kbs[cmd.KBID] = kb
		return applyResult{}, true

	case cmdMarkKBDeleteFailed:
		kb, ok := sm.kbs[cmd.KBID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}, true
		}
		kb.Status = types.KBStatusDeleteFailed
		sm.kbs[cmd.KBID] = kb
		return applyResult{}, true

	case cmdRemoveKBMeta:
		if _, exists := sm.kbs[cmd.KBID]; exists {
			sm.directory.Delete(cmd.KBID)
		}
		// Idempotent: deleting an already-absent key is a no-op, not an
		// error, so the delete flow's crash-recovery path can safely
		// re-propose this any number of times.
		delete(sm.kbs, cmd.KBID)
		for _, versionID := range sm.versionsByKB[cmd.KBID] {
			delete(sm.versions, versionID)
		}
		delete(sm.versionsByKB, cmd.KBID)
		sm.chainSeq++
		sm.dropRequestMappings(cmd.KBID, 0)
		// ONE entry for the whole knowledge base, never one per version: a KB can hold
		// hundreds of thousands of versions, and appending them would flush the bounded
		// log (and everything a consumer had not yet read) in a single apply. The entry
		// names the KB itself — VersionID 0 — and the consumer's reclaim is the KB-wide
		// prefix delete.
		sm.recordDeletion(cmd.KBID, 0)
		return applyResult{}, true

	default:
		return applyResult{}, false
	}
}
