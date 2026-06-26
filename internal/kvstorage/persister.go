// Package kvstorage provides Persister, the on-disk persistence layer for
// the Raft consensus implementation in internal/kvraft: durable storage of
// Raft's hard state (current term, vote, log entries) and of state-machine
// snapshots.
//
// Adapted from the KVServer project's internal/storage/persistent.go (an
// MIT 6.5840-style teaching implementation), which Stratum's RaftNode
// builds on per Stratum_实现顺序.md 阶段 3. The original used gob encoding
// and an atomic-rename write pattern; both are preserved here as sound,
// simple choices — only documentation, naming, and a couple of minor
// safety details (directory creation error handling) were adjusted to
// match this repository's conventions.
package kvstorage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"

	kvraftpb "stratum/api/proto/kvraft"
)

// persistState is the on-disk representation of Raft's hard state: the
// fields that must survive a restart for the safety properties of Raft to
// hold (current term, the candidate voted for in that term, and the full
// log).
type persistState struct {
	Term    uint64
	VoteFor int64
	Log     []*kvraftpb.Entry
}

// snapshotState is the on-disk representation of a state-machine
// snapshot: opaque serialized state-machine data plus the Raft log index
// it covers up to (inclusive).
type snapshotState struct {
	Data      []byte
	LastIndex uint64
}

// Persister durably stores Raft hard state and state-machine snapshots
// under two files derived from a single base path: path (hard state) and
// path+".snapshot" (snapshot).
type Persister struct {
	statePath    string
	snapshotPath string
}

// NewPersister returns a Persister rooted at path. Neither file is
// created until the first Save call; LoadState/LoadSnapshot on a fresh
// path return zero values rather than an error, since "no prior state"
// is the expected condition on a node's first-ever startup.
func NewPersister(path string) *Persister {
	return &Persister{
		statePath:    path,
		snapshotPath: path + ".snapshot",
	}
}

// SaveState durably persists Raft's current term, vote, and full log.
func (p *Persister) SaveState(term uint64, voteFor int64, log []*kvraftpb.Entry) error {
	state := persistState{Term: term, VoteFor: voteFor, Log: log}
	if err := atomicWriteGob(p.statePath, state); err != nil {
		return fmt.Errorf("kvstorage: SaveState: %w", err)
	}
	return nil
}

// LoadState reads back the most recently saved Raft hard state. On a
// fresh path (no prior SaveState call ever succeeded), it returns zero
// values and a nil error — this is the expected condition on first
// startup, not an error condition.
func (p *Persister) LoadState() (term uint64, voteFor int64, log []*kvraftpb.Entry, err error) {
	data, err := os.ReadFile(p.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil, nil
		}
		return 0, 0, nil, fmt.Errorf("kvstorage: LoadState: read %s: %w", p.statePath, err)
	}
	var state persistState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		return 0, 0, nil, fmt.Errorf("kvstorage: LoadState: decode %s: %w", p.statePath, err)
	}
	return state.Term, state.VoteFor, state.Log, nil
}

// SaveSnapshot durably persists a state-machine snapshot and the Raft log
// index it covers up to (inclusive). Independent of SaveState: saving a
// snapshot does not alter the separately persisted hard state, and vice
// versa.
func (p *Persister) SaveSnapshot(data []byte, lastIndex uint64) error {
	snap := snapshotState{Data: data, LastIndex: lastIndex}
	if err := atomicWriteGob(p.snapshotPath, snap); err != nil {
		return fmt.Errorf("kvstorage: SaveSnapshot: %w", err)
	}
	return nil
}

// LoadSnapshot reads back the most recently saved snapshot. On a fresh
// path (no prior SaveSnapshot call ever succeeded), it returns nil data,
// a zero index, and a nil error — this is the expected condition when a
// node has never taken a snapshot, not an error condition.
func (p *Persister) LoadSnapshot() (data []byte, lastIndex uint64, err error) {
	raw, err := os.ReadFile(p.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("kvstorage: LoadSnapshot: read %s: %w", p.snapshotPath, err)
	}
	var snap snapshotState
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&snap); err != nil {
		return nil, 0, fmt.Errorf("kvstorage: LoadSnapshot: decode %s: %w", p.snapshotPath, err)
	}
	return snap.Data, snap.LastIndex, nil
}

// atomicWriteGob gob-encodes val and writes it to path via a write-to-
// temp-file-then-rename sequence, so a crash mid-write can never leave a
// half-written, corrupt file at path — readers always see either the
// previous complete version or the new complete version, never a partial
// one.
func atomicWriteGob(path string, val any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(val); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmpPath, path, err)
	}
	return nil
}
