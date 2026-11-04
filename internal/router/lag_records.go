package router

import (
	"sync"
	"time"
)

// defaultLagRecordTTL is how long a "this node is behind" record keeps the node out
// of the front of the line. Long enough for the pull its refusal started to make
// progress, short enough that a node that has caught up is not left idle for long.
const defaultLagRecordTTL = 3 * time.Second

// maxLagRecords bounds the table. Records are small and expire on their own, so this
// only exists to keep a burst of knowledge bases from growing it without limit.
const maxLagRecords = 4096

type lagKey struct {
	kbID string
	node int
}

type lagRecord struct {
	cursor int64
	until  time.Time
}

// lagRecords is what this station has learned from refusals: "node N's history for
// this knowledge base reached version C when it last refused me".
//
// It is an optimisation and nothing else. The freshness credential on the forwarded
// query is what keeps an answer correct; a record only moves a node that would refuse
// to the back of the line, so a lost, stale or missing record costs at most one extra
// round trip. It is local to the station for the same reason the breakers are: stations
// scale by sharing nothing, and each learns a node's lag by asking it.
type lagRecords struct {
	ttl time.Duration

	mu      sync.RWMutex
	entries map[lagKey]lagRecord
}

func newLagRecords(ttl time.Duration) *lagRecords {
	return &lagRecords{ttl: ttl, entries: make(map[lagKey]lagRecord)}
}

// note records a refusal. A nil table records nothing.
func (l *lagRecords) note(kbID string, node int, cursor int64, now time.Time) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) >= maxLagRecords {
		for key, rec := range l.entries {
			if !now.Before(rec.until) {
				delete(l.entries, key)
			}
		}
		// Every record is still live: forgetting them all costs one refusal each,
		// which is the price of the bound and nothing worse.
		if len(l.entries) >= maxLagRecords {
			l.entries = make(map[lagKey]lagRecord)
		}
	}
	l.entries[lagKey{kbID: kbID, node: node}] = lagRecord{cursor: cursor, until: now.Add(l.ttl)}
}

// behind reports whether a live record says node cannot serve versionID yet. Only a
// version above the recorded cursor counts: a node that is behind on the newest
// version still serves everything below it.
func (l *lagRecords) behind(kbID string, node int, versionID int64, now time.Time) bool {
	if l == nil {
		return false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	rec, ok := l.entries[lagKey{kbID: kbID, node: node}]
	return ok && now.Before(rec.until) && rec.cursor < versionID
}
