package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// applyResult is what a single command application produces: an error if
// the command was rejected by a deterministic validation check (e.g. an
// invalid parent version), or, for cmdCreateVersion specifically, the
// newly assigned VersionID.
type applyResult struct {
	MigrationRead        bool
	ServiceRevision      uint64
	ServiceBlockRevision uint64
	VersionID            int64
	// DeletedVersionIDs is set by cmdMarkVersionDeleting: every version
	// that command marked Deleting, so the caller can report the exact
	// impact set (for ANCESTORS that is every swept-up 前置版本).
	// Empty for every other command.
	DeletedVersionIDs []int64
	Err               error
}

// stateMachine holds the in-memory knowledge base and version metadata
// replicated via Raft. It is mutated exclusively by stateMachine.apply,
// called from RaftNodeImpl's single apply-dispatch loop — so apply itself
// needs no internal synchronization against concurrent writers, but reads
// (GetKB, ListVersions) can happen concurrently from other goroutines and
// so still need the mutex.
type stateMachine struct {
	mu            sync.RWMutex
	directory     storage.Directory
	kbs           map[string]types.KnowledgeBaseMeta
	versions      map[int64]types.VersionMeta
	versionsByKB  map[string][]int64
	nextVersionID int64
	// versionsByRequest maps a client idempotency key (see requestKey) to the
	// version ID the first apply of that request allocated. It is what lets a
	// retried CreateVersion reuse its version instead of allocating another
	// one, so a client can re-send the changes for a version whose data never
	// landed (docs/参考/历史/Stratum_设计文档v13.md §7.12). Entries live exactly as long as
	// the version metadata they point at.
	versionsByRequest map[string]int64

	// deletedEpoch / deletedSeq / deletedLog are the BOUNDED deletion log a
	// storage node consumes incrementally (see RaftNodeImpl.DeletionsSince).
	//
	// It is a removal record, which this system deliberately removed once before
	// (docs/未实施/known-gaps.md §B: a record is history, pruning can drop it, and a
	// leftover whose evidence was pruned is invisible forever). What makes it
	// acceptable now is that it is NOT the judgement: the live-chain read
	// (VersionLiveness) stays and remains the authority, so a consumer that falls
	// behind the log, or a node that never had a watermark at all, falls back to it
	// and loses nothing. The log only spares a consumer from re-reading a whole
	// range when nothing was deleted.
	//
	// deletedEpoch changes when the state machine is created anew (a rebuilt
	// cluster). A consumer's watermark is (epoch, seq): the epoch is what tells
	// "your sequence numbers are from another cluster" apart from "you are reading a
	// replica that is behind" — without it a rebuilt cluster's seq=0 would look like
	// an ordinary lagging read forever.
	deletedEpoch    string
	deletedSeq      int64
	deletedLog      []types.DeletedVersion
	deletedLogLimit int

	// chainSeq counts the applies that may have moved a knowledge base's chain tail. It
	// is local to this replica and never persisted: only the leader's value is read, and
	// the term it travels with is what separates one leader's count from another's.
	chainSeq uint64
}

// deletedLogLimit bounds the deletion log. A thousand deletions an hour for ten
// hours, i.e. enough that a consumer polling on any sane cadence never falls behind
// it — and small enough that a snapshot carries ~300 KB of it. A consumer that DOES
// fall behind is told so (truncated) and re-judges from the live chain.
const defaultDeletedLogLimit = 10_000

// deletedLogLimit returns the effective bound.
func (sm *stateMachine) deletedLogLimitOrDefault() int {
	if sm.deletedLogLimit > 0 {
		return sm.deletedLogLimit
	}
	return defaultDeletedLogLimit
}

func newStateMachine() *stateMachine {
	return newStateMachineWithDeletedLogLimit(0)
}

// newStateMachineWithDeletedLogLimit builds a state machine whose deletion log keeps
// at most limit entries (0 = defaultDeletedLogLimit).
func newStateMachineWithDeletedLogLimit(limit int) *stateMachine {
	return &stateMachine{
		directory:         storage.NewDirectory(),
		kbs:               make(map[string]types.KnowledgeBaseMeta),
		versions:          make(map[int64]types.VersionMeta),
		versionsByKB:      make(map[string][]int64),
		nextVersionID:     1,
		deletedEpoch:      newDeletionEpoch(),
		deletedLogLimit:   limit,
		versionsByRequest: make(map[string]int64),
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
	if sm.migrationBlocksCommand(cmd) {
		return applyResult{Err: stratumerrors.ErrMigrationInProgress}
	}

	if res, ok := sm.applyStorage(cmd); ok {
		return res
	}
	if res, ok := sm.applyKB(cmd); ok {
		return res
	}
	if res, ok := sm.applyVersion(ctx, cmd, w, logger); ok {
		return res
	}
	return applyResult{Err: fmt.Errorf("raft: unknown command type %q", cmd.Type)}
}

// snapshotState is the gob-serializable form of the full state machine,
// used by serialize/restore for Raft log compaction.
type snapshotState struct {
	Directory         storage.Directory
	KBs               map[string]types.KnowledgeBaseMeta
	Versions          map[int64]types.VersionMeta
	VersionsByKB      map[string][]int64
	NextVersionID     int64
	VersionsByRequest map[string]int64
	// The deletion log travels with the snapshot: a node restored from it must be
	// able to answer a consumer's incremental read. Losing it is survivable (an empty
	// log with a live sequence reads as "truncated", which sends the consumer back to
	// the live chain), which is why it is a plain field rather than a durability
	// mechanism of its own.
	DeletedEpoch string
	DeletedSeq   int64
	DeletedLog   []types.DeletedVersion
}

// deepCopy returns a stable copy of the current state machine under RLock.
// It is the fast, lock-bounded step of the async snapshot path: the copy
// is cheap (KB/version metadata only), so apply is blocked for a moment;
// the slow gob encoding and disk write happen later on the copy without
// holding any lock, so a slow snapshot can never stall the apply loop.
func (sm *stateMachine) deepCopy() snapshotState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	kbs := make(map[string]types.KnowledgeBaseMeta, len(sm.kbs))
	for k, v := range sm.kbs {
		kbs[k] = v
	}
	versions := make(map[int64]types.VersionMeta, len(sm.versions))
	for k, v := range sm.versions {
		versions[k] = v
	}
	versionsByKB := make(map[string][]int64, len(sm.versionsByKB))
	for k, v := range sm.versionsByKB {
		versionsByKB[k] = append([]int64(nil), v...)
	}
	versionsByRequest := make(map[string]int64, len(sm.versionsByRequest))
	for k, v := range sm.versionsByRequest {
		versionsByRequest[k] = v
	}
	return snapshotState{
		Directory:         sm.directory.Clone(),
		KBs:               kbs,
		Versions:          versions,
		VersionsByKB:      versionsByKB,
		NextVersionID:     sm.nextVersionID,
		VersionsByRequest: versionsByRequest,
		DeletedEpoch:      sm.deletedEpoch,
		DeletedSeq:        sm.deletedSeq,
		DeletedLog:        append([]types.DeletedVersion(nil), sm.deletedLog...),
	}
}

// serialize encodes the full current state machine for a Raft snapshot.
func (sm *stateMachine) serialize() ([]byte, error) {
	return encodeSnapshot(sm.deepCopy())
}

// encodeSnapshot gob-encodes a (copied) snapshotState without touching the
// live state machine, so it can run off the state-machine lock.
func encodeSnapshot(snap snapshotState) ([]byte, error) {
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
	sm.directory = snap.Directory.Clone()
	sm.versions = snap.Versions
	sm.versionsByKB = snap.VersionsByKB
	sm.nextVersionID = snap.NextVersionID
	sm.versionsByRequest = snap.VersionsByRequest
	sm.deletedEpoch = snap.DeletedEpoch
	sm.deletedSeq = snap.DeletedSeq
	sm.deletedLog = snap.DeletedLog
	sm.chainSeq++
	if sm.deletedEpoch == "" {
		// A snapshot written before the log existed. Mint a fresh epoch rather than
		// reusing "": consumers compare epochs, and an empty one must not read as "the
		// same cluster".
		sm.deletedEpoch = newDeletionEpoch()
	}
	if sm.kbs == nil {
		sm.kbs = make(map[string]types.KnowledgeBaseMeta)
	}
	if sm.versions == nil {
		sm.versions = make(map[int64]types.VersionMeta)
	}
	if sm.versionsByKB == nil {
		sm.versionsByKB = make(map[string][]int64)
	}
	// A snapshot written before the idempotency map existed decodes to nil.
	if sm.versionsByRequest == nil {
		sm.versionsByRequest = make(map[string]int64)
	}
	return nil
}

// listAllVersions returns a snapshot of every version that still needs
// storage-layer data sync. Index readiness says nothing about whether data
// needs recovery: lazy indexes can be PENDING for durable versions. Announce
// pending data too, just as applying CreateVersion would have done; the data
// plane waits for actual data rather than treating the hook as a durability
// proof. Deleted and permanently lost DATA are never restored.
//
// Used after installing a leader-pushed snapshot: the snapshot covers log
// entries that are never applied individually on this node, so the normal
// per-version onVersionCreated hook would never fire for them — the caller
// triggers it explicitly for every version returned here.
func (sm *stateMachine) listAllVersions() []types.VersionMeta {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	out := make([]types.VersionMeta, 0, len(sm.versions))
	for _, v := range sm.versions {
		if v.DataStatus == types.DataStatusFailedPermanent || v.Deleting {
			continue
		}
		out = append(out, v)
	}
	return out
}
