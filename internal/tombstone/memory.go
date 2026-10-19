package tombstone

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryStore is the in-process Store. Two uses:
//
//   - tests, where opening a database to assert "the notification was recorded"
//     would be noise;
//   - a node with no persistent store wired, where the table still refuses late
//     pushes for as long as the process lives. That is strictly better than
//     nothing and strictly worse than PebbleStore — it does not survive a
//     restart, which is one of the two windows this table exists for.
//
// It is a map behind a mutex: the table is small (deletions in the grace window)
// and every operation is a single lookup or a full walk of it.
type MemoryStore struct {
	mu      sync.Mutex
	entries map[memKey]Entry
	// sync is the watermark, with the same meaning (and the same "not a judgement"
	// role) as the durable implementation's.
	sync    SyncState
	hasSync bool
}

type memKey struct {
	kbID      string
	versionID int64
}

// NewMemoryStore returns an empty in-process table.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[memKey]Entry)}
}

// Note implements Store.
func (m *MemoryStore) Note(_ context.Context, kbID string, versionID int64, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := memKey{kbID: kbID, versionID: versionID}
	entry := Entry{KBID: kbID, VersionID: versionID, State: Pending, NotedAt: at}
	if existing, ok := m.entries[key]; ok {
		// A repeated notification must not resurrect a finished reclaim.
		entry.State = existing.State
		entry.DoneAt = existing.DoneAt
	}
	m.entries[key] = entry
	return nil
}

// MarkDone implements Store.
func (m *MemoryStore) MarkDone(_ context.Context, kbID string, versionID int64, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := memKey{kbID: kbID, versionID: versionID}
	entry, ok := m.entries[key]
	if !ok {
		return nil // never noted: the reconciliation's business, not ours
	}
	entry.State = Done
	entry.DoneAt = at
	m.entries[key] = entry
	return nil
}

// Contains implements Store.
func (m *MemoryStore) Contains(_ context.Context, kbID string, versionID int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.entries[memKey{kbID: kbID, versionID: versionID}]
	return ok, nil
}

// ContainsKB implements Store.
func (m *MemoryStore) ContainsKB(_ context.Context, kbID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.entries[memKey{kbID: kbID, versionID: 0}]
	return ok, nil
}

// Pending implements Store.
func (m *MemoryStore) Pending(_ context.Context) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Entry
	for _, entry := range m.entries {
		if entry.State == Pending {
			out = append(out, entry)
		}
	}
	sortEntries(out)
	return out, nil
}

// Prune implements Store.
func (m *MemoryStore) Prune(_ context.Context, now time.Time, grace time.Duration) (int, error) {
	if grace < 0 {
		grace = DefaultGrace
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	dropped := 0
	for key, entry := range m.entries {
		if entry.State == Done && now.Sub(entry.DoneAt) > grace {
			delete(m.entries, key)
			dropped++
		}
	}
	return dropped, nil
}

// SyncSeq implements Store.
func (m *MemoryStore) SyncSeq(_ context.Context) (SyncState, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sync, m.hasSync, nil
}

// NoteBatch implements Store.
func (m *MemoryStore) NoteBatch(_ context.Context, state SyncState, entries []DeletedVersion) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	at := time.Now()
	for _, entry := range entries {
		key := memKey{kbID: entry.KBID, versionID: entry.VersionID}
		record := Entry{KBID: entry.KBID, VersionID: entry.VersionID, State: Pending, NotedAt: at}
		if existing, ok := m.entries[key]; ok {
			record.State = existing.State
			record.DoneAt = existing.DoneAt
		}
		m.entries[key] = record
	}
	// The map cannot be torn halfway: both the entries and the watermark land here or
	// neither does, which is all the atomicity the caller needs.
	m.sync, m.hasSync = state, true
	return nil
}

// Close implements Store.
func (m *MemoryStore) Close() error { return nil }

// sortEntries gives Pending a deterministic order, so a caller's log lines and any
// test asserting on them do not depend on map iteration order.
func sortEntries(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].KBID != entries[j].KBID {
			return entries[i].KBID < entries[j].KBID
		}
		return entries[i].VersionID < entries[j].VersionID
	})
}
