package coordinator

import "sync"

// KBLockSet hands out one write lock per knowledge base.
//
// Why per knowledge base and not one global lock (M7 of
// docs/code-review-2026-09-24.md): the lock is held across the whole CreateVersion
// transaction, which includes a Raft round trip (ProposeCreateVersion) and, on the
// non-dispatch path, the storage layer's fan-out. With a single lock that is a
// cluster-wide serialization: every write to every knowledge base waits behind the
// slowest network hop of an unrelated KB, and the orphan-chunk GC's reclaim phase —
// which takes the same lock — stops all writes while it deletes one chunk.
//
// What must stay serialized is per knowledge base, and that is what the WAL actually
// needs: a transaction's BEGIN and VERSION_ID must be adjacent *within its own
// knowledge base's records*, or a crash-replay would bind the wrong replay input
// (see internal/wal/file.go, which now pairs per KB for exactly this reason).
//
// Locks are reference-counted and dropped when idle: a deployment can create
// knowledge bases forever, and a map that only grows would be the same unbounded
// bookkeeping this review keeps finding elsewhere.
type KBLockSet struct {
	mu    sync.Mutex
	locks map[string]*kbLock
}

type kbLock struct {
	mu   sync.Mutex
	refs int // holders + waiters; the entry goes away at zero
}

// NewKBLockSet returns an empty set.
func NewKBLockSet() *KBLockSet {
	return &KBLockSet{locks: make(map[string]*kbLock)}
}

// Lock acquires kbID's lock and returns the function that releases it.
//
// The returned function is idempotent-ish in the usual Go sense (calling it twice
// unlocks twice and panics) — callers use it as `defer unlock()`.
func (s *KBLockSet) Lock(kbID string) func() {
	if s == nil {
		// A nil set means "no configured write lock": nothing to serialize against.
		// Returning a no-op keeps the call sites free of nil checks, and the single
		// caller that can hit this (a test constructing a coordinator directly)
		// passes a set anyway.
		return func() {}
	}
	s.mu.Lock()
	if s.locks == nil {
		s.locks = make(map[string]*kbLock)
	}
	entry := s.locks[kbID]
	if entry == nil {
		entry = &kbLock{}
		s.locks[kbID] = entry
	}
	entry.refs++
	s.mu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		s.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(s.locks, kbID)
		}
		s.mu.Unlock()
	}
}

// Len reports how many knowledge bases currently hold or are waiting for a lock.
// For tests and diagnostics: it is the visible half of the reference counting.
func (s *KBLockSet) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.locks)
}
