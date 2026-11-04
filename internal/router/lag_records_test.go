package router

import (
	"testing"
	"time"
)

func TestLagRecords_NilTableRemembersNothing(t *testing.T) {
	var l *lagRecords
	l.note("kb-1", 0, 3, time.Now())
	if l.behind("kb-1", 0, 9, time.Now()) {
		t.Fatal("a nil table must say nobody is behind")
	}
}

func TestLagRecords_NewerRefusalReplacesTheOlder(t *testing.T) {
	l := newLagRecords(time.Minute)
	now := time.Now()
	l.note("kb-1", 0, 3, now)
	l.note("kb-1", 0, 8, now)

	if l.behind("kb-1", 0, 8, now) {
		t.Error("the latest refusal says the node holds version 8")
	}
	if !l.behind("kb-1", 0, 9, now) {
		t.Error("the latest refusal says the node does not hold version 9")
	}
}

// TestLagRecords_Bounded: a full table drops what expired, and failing that starts over.
// Forgetting costs one refusal each, so the bound can never cause a wrong answer.
func TestLagRecords_Bounded(t *testing.T) {
	l := newLagRecords(time.Minute)
	now := time.Now()
	for i := 0; i < maxLagRecords+10; i++ {
		l.note("kb-1", i, 1, now)
	}
	if len(l.entries) > maxLagRecords {
		t.Fatalf("table holds %d records, want at most %d", len(l.entries), maxLagRecords)
	}

	later := now.Add(2 * time.Minute)
	l.note("kb-2", 0, 1, later)
	if len(l.entries) > maxLagRecords {
		t.Fatalf("table holds %d records after expiry, want at most %d", len(l.entries), maxLagRecords)
	}
	if !l.behind("kb-2", 0, 2, later) {
		t.Error("the newest record must survive the sweep")
	}
}
