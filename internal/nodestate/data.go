package nodestate

import "sort"

// This file is the DATA side of the machine: the contiguous cursor and the two
// accounts it advances over.
//
// These used to live on the storage layer (LocalDataPlane's localVersion and the
// two above-the-cursor accounts it kept beside it), and they are here now because
// they are state
// with exactly the same standing as the version entries above them — the last
// thing about this node that was tracked somewhere the state machine could not
// see. Moving them is what makes the machine answerable for the whole data
// layer's local position, rather than only for the versions that happen to have
// an entry.
//
// What is deliberately NOT changed is the cursor's MEANING. It is a contiguous
// account, not a "highest version I have seen": it may only step over a version
// this node holds or a version that no longer exists, and a version the control
// layer says exists but this node does not hold stops it (§7.5). Collapsing it
// into a maximum would let one failed push followed by a successful later push
// claim an unbroken history the node does not have.

// Cursor reports kbID's contiguous data cursor: the highest version this node
// holds without a gap. Zero means "nothing known yet" — a node that has applied
// nothing for that knowledge base.
func (m *Machine) Cursor(kbID string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cursors[kbID]
}

// Cursors returns every knowledge base's cursor as a copy — the whole view a
// node reports to the control leader (§7.13.4).
//
// A copy, not the live map: the caller may encode it, retry it, or hold it
// across a tick, and a concurrent write must not mutate what was already
// reported. Knowledge bases with no local data are absent rather than zero, so
// the receiver can tell "reported 0" from "never reported" — a distinction
// §7.13.4's aggregate depends on.
func (m *Machine) Cursors() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.cursors) == 0 {
		return nil
	}
	out := make(map[string]int64, len(m.cursors))
	for kbID, versionID := range m.cursors {
		out[kbID] = versionID
	}
	return out
}

// InstallCursor sets kbID's cursor from a persisted record, WITHOUT the accounts
// a normal advance maintains.
//
// It is the recovery path (docs/cursor-persistence-plan.md): a restarted node
// reads the value it wrote before the crash rather than replaying the events
// that produced it. Monotone for the same reason the cursor itself is — memory
// may already have moved past the record (a write that landed before this ran),
// and moving back would forget versions this node holds.
func (m *Machine) InstallCursor(kbID string, versionID int64) {
	if kbID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if versionID > m.cursors[kbID] {
		m.cursors[kbID] = versionID
	}
}

// Advance records that this node has accounted for versionID — it holds the
// version's data (written here, pulled, pushed, or covered by a full-state
// transfer), or the version no longer exists (dropped as deleted) — and returns
// the cursor afterwards.
//
// The returned value is the cursor AFTER the step, not versionID: the cursor
// only moves over a contiguous run, so an advance may have stepped over several
// versions at once, and it is that boundary the caller persists.
func (m *Machine) Advance(kbID string, versionID int64) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if versionID > m.cursors[kbID] {
		above := m.heldAbove[kbID]
		if above == nil {
			above = make(map[int64]struct{})
			m.heldAbove[kbID] = above
		}
		above[versionID] = struct{}{}
	}
	m.advanceCursorLocked(kbID)
	return m.cursors[kbID]
}

// MarkHandled accounts for every version in [from, to] in one step, and returns
// the cursor afterwards.
//
// It is for the paths that obtain a whole RANGE's state at once — §6.4's
// full-state transfer is the only one — where advancing version by version
// would re-scan the pending accounts once per version.
func (m *Machine) MarkHandled(kbID string, from, to int64) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	above := m.heldAbove[kbID]
	if above == nil {
		above = make(map[int64]struct{})
		m.heldAbove[kbID] = above
	}
	for v := from; v <= to; v++ {
		if v > m.cursors[kbID] {
			above[v] = struct{}{}
		}
	}
	m.advanceCursorLocked(kbID)
	return m.cursors[kbID]
}

// Announce records that the control layer says versionID exists.
//
// It is the "known gap" half of the §7.5 invariant: a version this node has been
// told about but does not hold must block the cursor from stepping over it.
// Announcing is idempotent and cheap, and it deliberately does NOT move the
// cursor — existence is not possession.
func (m *Machine) Announce(kbID string, versionID int64) {
	if kbID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if versionID <= m.cursors[kbID] {
		return
	}
	announced := m.announcedAbove[kbID]
	if announced == nil {
		announced = make(map[int64]struct{})
		m.announcedAbove[kbID] = announced
	}
	announced[versionID] = struct{}{}
}

// advanceCursorLocked walks the cursor up over every accounted-for version,
// stopping at the first KNOWN version that is not accounted for (or when nothing
// above the cursor is known).
//
// The work is proportional to the number of versions above the cursor still
// being tracked, which is one in the healthy case — an advance immediately
// consumes its own entry — and grows only while a gap is genuinely missing.
func (m *Machine) advanceCursorLocked(kbID string) {
	held := m.heldAbove[kbID]
	announced := m.announcedAbove[kbID]
	for {
		next, ok := m.lowestAboveLocked(kbID)
		if !ok {
			break
		}
		if _, done := held[next]; !done {
			// A version the control layer says exists and this node does not
			// hold: the cursor stops below it rather than claiming a history
			// with a hole in it.
			break
		}
		delete(held, next)
		delete(announced, next)
		m.cursors[kbID] = next
	}
	if len(held) == 0 {
		delete(m.heldAbove, kbID)
	}
	if len(announced) == 0 {
		delete(m.announcedAbove, kbID)
	}
}

// lowestAboveLocked returns the smallest version above the cursor that this node
// knows of — accounted for or merely announced. The second return value is false
// when nothing above the cursor is known, i.e. there is nothing left to step over.
func (m *Machine) lowestAboveLocked(kbID string) (int64, bool) {
	cursor := m.cursors[kbID]
	found := false
	var lowest int64
	for _, set := range []map[int64]struct{}{m.heldAbove[kbID], m.announcedAbove[kbID]} {
		for v := range set {
			if v <= cursor {
				continue
			}
			if !found || v < lowest {
				lowest, found = v, true
			}
		}
	}
	return lowest, found
}

// KnowledgeBases reports the knowledge bases this node has a cursor for — the
// ones it holds data for, and therefore the ones whose recorded changes could be
// sitting in its WAL.
//
// It is the cursor's key set, which is the same statement: a cursor exists
// exactly when something landed for that knowledge base. Sorted, so a caller
// iterating it produces a reproducible sequence.
func (m *Machine) KnowledgeBases() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.cursors) == 0 {
		return nil
	}
	out := make([]string, 0, len(m.cursors))
	for kbID := range m.cursors {
		out = append(out, kbID)
	}
	sort.Strings(out)
	return out
}
