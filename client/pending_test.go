package client

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
	"stratum/service"
)

func newStoreForTest(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

// The record has to survive a crash, which is the failure it exists for: a state
// file that can be half-written would lose exactly the records that matter.
func TestStore_RoundTripsAndReplacesAtomically(t *testing.T) {
	s := newStoreForTest(t)

	older := PendingWrite{ID: "a", KnowledgeBaseID: "kb-1", VersionID: 7, ClientRequestID: "key-a",
		Changes: []Change{{Op: "ADD", DocID: "d1", Content: "x"}}, SubmittedAt: time.Now().Add(-time.Minute)}
	newer := PendingWrite{ID: "b", KnowledgeBaseID: "kb-1", VersionID: 8, ClientRequestID: "key-b",
		Changes: []Change{{Op: "DELETE", DocID: "d2"}}, SubmittedAt: time.Now()}
	for _, w := range []PendingWrite{older, newer} {
		if err := s.Upsert(w); err != nil {
			t.Fatalf("Upsert(%s): %v", w.ID, err)
		}
	}

	records, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
	if records[0].ID != "b" {
		t.Errorf("records[0] = %s, want b (newest first)", records[0].ID)
	}
	if len(records[0].Changes) != 1 || records[0].Changes[0].DocID != "d2" {
		t.Errorf("changes did not survive the round trip: %+v", records[0].Changes)
	}

	// Same ID replaces rather than appending: a settled batch is the same batch.
	settled := newer
	settled.Settled = true
	if err := s.Upsert(settled); err != nil {
		t.Fatalf("Upsert(settled): %v", err)
	}
	got, ok, err := s.Get("b")
	if err != nil || !ok {
		t.Fatalf("Get(b) = (%+v, %v, %v)", got, ok, err)
	}
	if !got.Settled {
		t.Error("the replacement was not recorded")
	}
	if records, _ := s.Load(); len(records) != 2 {
		t.Errorf("records after replacing = %d, want 2", len(records))
	}

	// Nothing half-written is left behind.
	if _, err := os.Stat(s.Path() + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("a temporary file was left at %s", s.Path()+".tmp")
	}
}

func TestStore_MissingFileIsAnEmptyStore(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	records, err := s.Load()
	if err != nil {
		t.Fatalf("Load on a fresh store: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("records = %d, want 0", len(records))
	}
}

// A corrupt state file is an error, not an empty store: silently forgetting the
// caller's only copy of its changes is the one outcome worth refusing.
func TestStore_CorruptFileIsAnError(t *testing.T) {
	s := newStoreForTest(t)
	if err := os.WriteFile(s.Path(), []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write corrupt state: %v", err)
	}
	if _, err := s.Load(); err == nil {
		t.Fatal("Load on a corrupt store = nil error, want a refusal")
	}
}

func TestNewStore_CreatesTheStateDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "state")
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := os.Stat(s.Path()); !os.IsNotExist(err) {
		t.Fatalf("state file should not exist before the first write")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("state dir was not created: %v", err)
	}
}

func TestChange_ToProto(t *testing.T) {
	cases := []struct {
		name    string
		change  Change
		wantOp  pb.ChangeOp
		wantErr bool
	}{
		{"add", Change{Op: "ADD", DocID: "d", Content: "x"}, pb.ChangeOp_CHANGE_OP_ADD, false},
		{"update (lowercase is the ops scripts' habit)", Change{Op: "update", DocID: "d", Content: "x"}, pb.ChangeOp_CHANGE_OP_UPDATE, false},
		{"delete needs no content", Change{Op: "DELETE", DocID: "d"}, pb.ChangeOp_CHANGE_OP_DELETE, false},
		{"unknown op is refused", Change{Op: "UPSERT", DocID: "d", Content: "x"}, 0, true},
		{"add without content is refused", Change{Op: "ADD", DocID: "d"}, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.change.ToProto()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ToProto(%+v) = %+v, want an error", tc.change, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ToProto: %v", err)
			}
			if got.GetOp() != tc.wantOp || got.GetDocId() != tc.change.DocID {
				t.Errorf("ToProto = %+v, want op %v on %s", got, tc.wantOp, tc.change.DocID)
			}
		})
	}
}

// TestDecide_EveryStageAndBothOwnershipStates is the branch that makes this
// package worth having: the server reports facts, the caller owns the decision
// only it can make — whether its changes still exist.
func TestDecide_EveryStageAndBothOwnershipStates(t *testing.T) {
	cases := []struct {
		name        string
		stage       string
		dataMissing bool
		haveChanges bool
		want        Decision
	}{
		{"ready is done", stageIndexReady, false, true, DecisionDone},
		{"ready is done without changes too", stageIndexReady, false, false, DecisionDone},
		{"still writing: wait", stageDataPending, false, true, DecisionWait},
		{"durable but not ready: keep waiting for the target", stageDataDurable, false, true, DecisionWait},
		{"index failed: rebuildable, wait", stageIndexFailed, false, true, DecisionWait},
		{"deleting: give up on it", stageDeleting, false, true, DecisionDiscard},
		{"data missing, changes kept: re-send", stageDataPending, true, true, DecisionResend},
		{"data missing, changes gone: discard", stageDataPending, true, false, DecisionDiscard},
		{"data side retired, changes kept: start over", stageDataFailedPermanent, false, true, DecisionResend},
		{"data side retired, changes gone: discard", stageDataFailedPermanent, false, false, DecisionDiscard},
		{"index side retired, changes kept: start over", stageIndexFailedPermanent, false, true, DecisionResend},
		{"unknown stage is a newer server: never destroy anything", "SOMETHING_NEW", false, true, DecisionWait},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &pb.AwaitVersionResponse{Stage: tc.stage, DataMissing: tc.dataMissing}
			if got := Decide(resp, tc.haveChanges); got != tc.want {
				t.Errorf("Decide(stage=%s, data_missing=%v, haveChanges=%v) = %s, want %s",
					tc.stage, tc.dataMissing, tc.haveChanges, got, tc.want)
			}
		})
	}
}

// TestStagesMatchTheServiceConstants keeps this package's literals in step with
// the server's. They are duplicated on purpose (a client should not link the
// server implementation); this test is what makes the duplication safe.
func TestStagesMatchTheServiceConstants(t *testing.T) {
	pairs := map[string]string{
		stageDataPending:          service.StageDataPending,
		stageDataDurable:          service.StageDataDurable,
		stageIndexReady:           service.StageIndexReady,
		stageIndexFailed:          service.StageIndexFailed,
		stageDataFailedPermanent:  service.StageDataFailedPermanent,
		stageIndexFailedPermanent: service.StageIndexFailedPermanent,
		stageDeleting:             service.StageDeleting,
	}
	for mine, theirs := range pairs {
		if mine != theirs {
			t.Errorf("stage %q does not match service's %q", mine, theirs)
		}
	}
}
