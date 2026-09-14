package plane

import (
	"reflect"
	"testing"
)

// It answers "who could serve version V" from the reported cursors — the one
// question cursors can actually answer.
func TestDataVersionRegistry_HoldersComeFromReportedCursors(t *testing.T) {
	reg := NewDataVersionRegistry()
	reg.Record(1, map[string]int64{"kb-1": 5, "kb-2": 2})
	reg.Record(2, map[string]int64{"kb-1": 3})
	reg.Record(3, map[string]int64{"kb-1": 10})

	// A node holds every version up to and including its cursor (the cursor means
	// "contiguous up to here"), so v3 is held by 1, 2 and 3.
	if got := reg.Holders("kb-1", 3); !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Errorf("Holders(kb-1, 3) = %v, want [1 2 3]", got)
	}
	// v4 is past node 2's cursor.
	if got := reg.Holders("kb-1", 4); !reflect.DeepEqual(got, []int64{1, 3}) {
		t.Errorf("Holders(kb-1, 4) = %v, want [1 3]", got)
	}
	// v10 only reaches node 3's cursor.
	if got := reg.Holders("kb-1", 10); !reflect.DeepEqual(got, []int64{3}) {
		t.Errorf("Holders(kb-1, 10) = %v, want [3]", got)
	}
	// Node 2 never reported kb-2 at all, and that is not knowledge about kb-2's
	// data — so only node 1 shows up.
	if got := reg.Holders("kb-2", 1); !reflect.DeepEqual(got, []int64{1}) {
		t.Errorf("Holders(kb-2, 1) = %v, want [1]", got)
	}
	// An unknown knowledge base has no holders.
	if got := reg.Holders("kb-404", 1); len(got) != 0 {
		t.Errorf("Holders(kb-404, 1) = %v, want empty", got)
	}
}

// A report replaces the previous one wholesale rather than merging: each report
// is a complete statement, and merging would resurrect knowledge bases the node
// has since dropped.
func TestDataVersionRegistry_RecordReplacesRatherThanMerges(t *testing.T) {
	reg := NewDataVersionRegistry()
	reg.Record(1, map[string]int64{"kb-1": 5, "kb-2": 7})
	reg.Record(1, map[string]int64{"kb-1": 9})

	if cursor, ok := reg.Cursor(1, "kb-1"); !ok || cursor != 9 {
		t.Errorf("Cursor(1, kb-1) = (%d, %v), want (9, true)", cursor, ok)
	}
	if _, ok := reg.Cursor(1, "kb-2"); ok {
		t.Error("kb-2 survived a later report that did not mention it; a report is a complete statement")
	}
}

// The caller must be able to tell "never reported" from "reported a low cursor":
// the former says nothing about the node's data, so no one may read it as "it
// does not have it".
func TestDataVersionRegistry_AbsentNodeIsNotAZeroCursor(t *testing.T) {
	reg := NewDataVersionRegistry()
	if cursor, ok := reg.Cursor(7, "kb-1"); ok {
		t.Errorf("Cursor for a node that never reported = (%d, true), want false", cursor)
	}
	reg.Record(7, map[string]int64{"kb-1": 0})
	if cursor, ok := reg.Cursor(7, "kb-1"); !ok || cursor != 0 {
		t.Errorf("Cursor(7, kb-1) = (%d, %v), want (0, true) — a reported zero is a fact", cursor, ok)
	}
	if _, ok := reg.ReportedAt(7); !ok {
		t.Error("ReportedAt must be set for a node that reported")
	}
	if _, ok := reg.ReportedAt(8); ok {
		t.Error("ReportedAt must be absent for a node that never reported")
	}
}

// A departed node must stop vouching for versions, and a leadership change must
// discard the predecessor's soft state rather than inherit it.
func TestDataVersionRegistry_ForgetAndResetDropTheView(t *testing.T) {
	reg := NewDataVersionRegistry()
	reg.Record(1, map[string]int64{"kb-1": 5})
	reg.Record(2, map[string]int64{"kb-1": 5})
	if got := reg.Nodes(); !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Errorf("Nodes() = %v, want [1 2]", got)
	}

	reg.Forget(1)
	if got := reg.Holders("kb-1", 5); !reflect.DeepEqual(got, []int64{2}) {
		t.Errorf("after Forget(1), Holders = %v, want [2]", got)
	}

	reg.Reset()
	if got := reg.Holders("kb-1", 5); len(got) != 0 {
		t.Errorf("after Reset, Holders = %v, want empty", got)
	}
	if got := reg.Nodes(); len(got) != 0 {
		t.Errorf("after Reset, Nodes() = %v, want empty", got)
	}
}

// A copy is taken on Record: a caller that keeps mutating its map must not be
// able to change what the registry reported (it is shared across goroutines).
func TestDataVersionRegistry_RecordCopiesTheMap(t *testing.T) {
	reg := NewDataVersionRegistry()
	cursors := map[string]int64{"kb-1": 5}
	reg.Record(1, cursors)
	cursors["kb-1"] = 99

	if cursor, _ := reg.Cursor(1, "kb-1"); cursor != 5 {
		t.Errorf("Cursor(1, kb-1) = %d, want 5 — the report must be a snapshot", cursor)
	}
}
