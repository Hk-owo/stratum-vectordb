package coordinator

import (
	"testing"

	"stratum/internal/types"
)

// TestWriteCoordinatorImpl_PendingDispatch_RegisteredChangesComeBack pins the
// out-of-band handover of §7.13.2: the changes the dispatcher needs are not in
// the Raft command, so Execute registers them and the apply-time hook reads them
// back by (kbID, clientRequestID).
func TestWriteCoordinatorImpl_PendingDispatch_RegisteredChangesComeBack(t *testing.T) {
	c := NewWriteCoordinatorImpl(WriteCoordinatorConfig{})

	changes := []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "alpha"},
		{Op: types.ChangeOpDelete, DocID: "doc-2"},
	}
	c.RegisterPendingDispatch("kb-1", "req-1", 6, changes)

	parentID, got, ok := c.TakePendingDispatch("kb-1", "req-1")
	if !ok {
		t.Fatal("a registered write must be found")
	}
	if parentID != 6 {
		t.Errorf("parent version = %d, want 6", parentID)
	}
	if len(got) != 2 || got[0].DocID != "doc-1" || got[1].Op != types.ChangeOpDelete {
		t.Fatalf("changes = %+v, want the registered ones", got)
	}
}

// Taking is what makes a repeated dispatch a no-op: the same version must not be
// written twice because two dispatch attempts raced.
func TestWriteCoordinatorImpl_PendingDispatch_TakeConsumesTheRegistration(t *testing.T) {
	c := NewWriteCoordinatorImpl(WriteCoordinatorConfig{})
	c.RegisterPendingDispatch("kb-1", "req-1", 6, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "d"}})

	if _, _, ok := c.TakePendingDispatch("kb-1", "req-1"); !ok {
		t.Fatal("the first take must succeed")
	}
	if _, changes, ok := c.TakePendingDispatch("kb-1", "req-1"); ok {
		t.Fatalf("the second take must find nothing, got %+v", changes)
	}
}

// An unknown key is "nothing to dispatch", not an empty write.
func TestWriteCoordinatorImpl_PendingDispatch_UnknownKeyIsAbsent(t *testing.T) {
	c := NewWriteCoordinatorImpl(WriteCoordinatorConfig{})

	if _, changes, ok := c.TakePendingDispatch("kb-1", "never-registered"); ok || changes != nil {
		t.Fatalf("TakePendingDispatch(unknown) = (%+v, %v), want (nil, false)", changes, ok)
	}
}

// The registration survives until taken, but can be withdrawn when the proposal
// never landed — otherwise it would later be dispatched against a version that
// does not exist.
func TestWriteCoordinatorImpl_PendingDispatch_ForgetDropsTheRegistration(t *testing.T) {
	c := NewWriteCoordinatorImpl(WriteCoordinatorConfig{})
	c.RegisterPendingDispatch("kb-1", "req-1", 6, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "d"}})

	c.ForgetPendingDispatch("kb-1", "req-1")
	if _, _, ok := c.TakePendingDispatch("kb-1", "req-1"); ok {
		t.Fatal("a forgotten registration must not be dispatchable")
	}
}

// Registrations are per (kbID, clientRequestID): one write's changes must never
// be handed to another write's dispatch.
func TestWriteCoordinatorImpl_PendingDispatch_KeysAreIsolated(t *testing.T) {
	c := NewWriteCoordinatorImpl(WriteCoordinatorConfig{})
	c.RegisterPendingDispatch("kb-1", "req-1", 6, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "one"}})
	c.RegisterPendingDispatch("kb-2", "req-1", 9, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "two"}})
	c.RegisterPendingDispatch("kb-1", "req-2", 7, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "three"}})

	for _, tc := range []struct {
		kbID, reqID, wantDoc string
		wantParent           int64
	}{
		{"kb-1", "req-1", "one", 6},
		{"kb-2", "req-1", "two", 9},
		{"kb-1", "req-2", "three", 7},
	} {
		parent, changes, ok := c.TakePendingDispatch(tc.kbID, tc.reqID)
		if !ok || parent != tc.wantParent || len(changes) != 1 || changes[0].DocID != tc.wantDoc {
			t.Errorf("TakePendingDispatch(%s, %s) = (%d, %+v, %v), want (%d, %s, true)",
				tc.kbID, tc.reqID, parent, changes, ok, tc.wantParent, tc.wantDoc)
		}
	}
}
