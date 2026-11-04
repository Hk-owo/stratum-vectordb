package plane

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// recordingCompactorWAL is a WAL that both writes and records what compaction was
// asked to do — the seam ReclaimChanges talks to.
type recordingCompactorWAL struct {
	stubWAL
	compactions []map[string]int64
	compactErr  error
}

func (w *recordingCompactorWAL) Compact(_ context.Context, keepThrough map[string]int64) error {
	if w.compactErr != nil {
		return w.compactErr
	}
	kept := make(map[string]int64, len(keepThrough))
	for kbID, v := range keepThrough {
		kept[kbID] = v
	}
	w.compactions = append(w.compactions, kept)
	return nil
}

// peerCursors is a CursorQuerier answering from a fixed table: peer → kb → cursor. A
// peer absent from the table cannot be reached.
type peerCursors struct {
	cursors map[string]map[string]int64
	asked   map[string]int
}

func (p *peerCursors) LocalVersionOf(_ context.Context, peer, kbID string) (int64, error) {
	if p.asked == nil {
		p.asked = map[string]int{}
	}
	p.asked[peer]++
	byKB, ok := p.cursors[peer]
	if !ok {
		return 0, errors.New("unreachable")
	}
	return byKB[kbID], nil
}

// reclaimPlane builds a plane whose own cursor is 10 for each of kbs, over the given
// peers. A nil querier leaves the cursor query unwired.
func reclaimPlane(t *testing.T, wal TransactionWAL, querier CursorQuerier, peers []string, kbs ...string) *LocalDataPlane {
	t.Helper()
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		WAL:             wal,
		Executor:        &stubExecutor{t: &tracer{}},
		CursorQuerier:   querier,
		ResolveReplicas: func(context.Context) ([]string, error) { return peers, nil },
	})
	for _, kbID := range kbs {
		dp.advanceLocalVersion(kbID, 10)
	}
	return dp
}

// The slowest replica decides: a version is only safe to forget once every peer that
// needs its delta has it. This node's own cursor is one of the inputs.
func TestLocalDataPlane_ReclaimChangesUsesTheSlowestReplica(t *testing.T) {
	wal := &recordingCompactorWAL{}
	peers := &peerCursors{cursors: map[string]map[string]int64{
		"p1": {"kb-1": 7},
		"p2": {"kb-1": 12},
	}}
	dp := reclaimPlane(t, wal, peers, []string{"p1", "p2"}, "kb-1")

	used, err := dp.ReclaimChanges(context.Background())
	if err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if !reflect.DeepEqual(used, map[string]int64{"kb-1": 7}) {
		t.Errorf("watermarks used = %v, want kb-1:7", used)
	}
	if len(wal.compactions) != 1 || !reflect.DeepEqual(wal.compactions[0], map[string]int64{"kb-1": 7}) {
		t.Errorf("compactions = %v, want one call with kb-1:7", wal.compactions)
	}

	// This node being the slowest counts too.
	wal = &recordingCompactorWAL{}
	peers = &peerCursors{cursors: map[string]map[string]int64{"p1": {"kb-1": 20}}}
	dp = reclaimPlane(t, wal, peers, []string{"p1"}, "kb-1")
	if used, _ := dp.ReclaimChanges(context.Background()); used["kb-1"] != 10 {
		t.Errorf("watermarks used = %v, want this node's own 10", used)
	}
}

// With no other replica this node's own cursor is the whole answer.
func TestLocalDataPlane_ReclaimChangesSingleReplica(t *testing.T) {
	wal := &recordingCompactorWAL{}
	dp := reclaimPlane(t, wal, &peerCursors{}, nil, "kb-1")

	if used, _ := dp.ReclaimChanges(context.Background()); used["kb-1"] != 10 {
		t.Errorf("watermarks used = %v, want kb-1:10", used)
	}
}

// A replica that holds nothing for a knowledge base keeps it: this is not a reclaimable
// zero, since the replica may still need the delta. Knowledge bases are independent, so
// a partial answer is used as far as it goes.
func TestLocalDataPlane_ReclaimChangesSkipsKnowledgeBasesWithNoWatermark(t *testing.T) {
	wal := &recordingCompactorWAL{}
	peers := &peerCursors{cursors: map[string]map[string]int64{"p1": {"kb-1": 3}}}
	dp := reclaimPlane(t, wal, peers, []string{"p1"}, "kb-1", "kb-2")

	used, err := dp.ReclaimChanges(context.Background())
	if err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if !reflect.DeepEqual(used, map[string]int64{"kb-1": 3}) {
		t.Errorf("watermarks used = %v, want only kb-1", used)
	}
	if _, present := wal.compactions[0]["kb-2"]; present {
		t.Error("kb-2 was given a watermark it does not have; an unknown must not become a default")
	}
	blockers, ok := dp.ReclaimBlockers("kb-2")
	if !ok || !reflect.DeepEqual(blockers, []ReclaimBlocker{{Address: "p1", Reason: ReclaimBlockerNoCursor}}) {
		t.Errorf("blockers = %v/%v, want p1 with no cursor", blockers, ok)
	}
}

// A replica that cannot be asked keeps everything, and is named. It is asked once per
// pass, so a dead peer costs one timeout rather than one per knowledge base.
func TestLocalDataPlane_ReclaimChangesUnreachableReplicaBlocksEverything(t *testing.T) {
	wal := &recordingCompactorWAL{}
	peers := &peerCursors{cursors: map[string]map[string]int64{"p1": {"kb-1": 9, "kb-2": 9}}}
	dp := reclaimPlane(t, wal, peers, []string{"p1", "dead"}, "kb-1", "kb-2")

	used, err := dp.ReclaimChanges(context.Background())
	if err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if len(used) != 0 || len(wal.compactions) != 0 {
		t.Errorf("used = %v, compactions = %v, want nothing reclaimed", used, wal.compactions)
	}
	if peers.asked["dead"] != 1 {
		t.Errorf("dead peer asked %d times, want once per pass", peers.asked["dead"])
	}
	for _, kbID := range []string{"kb-1", "kb-2"} {
		blockers, ok := dp.ReclaimBlockers(kbID)
		if !ok || !reflect.DeepEqual(blockers, []ReclaimBlocker{{Address: "dead", Reason: ReclaimBlockerUnreachable}}) {
			t.Errorf("%s blockers = %v/%v, want the dead peer", kbID, blockers, ok)
		}
	}
}

// This node holding nothing for a knowledge base in its WAL (its data was dropped, say)
// is not a watermark of zero either.
func TestLocalDataPlane_ReclaimChangesLocalNodeWithNoCursorBlocks(t *testing.T) {
	wal := &scopedCompactorWAL{kbs: []string{"kb-dropped"}}
	dp := reclaimPlane(t, wal, &peerCursors{}, nil)

	if used, _ := dp.ReclaimChanges(context.Background()); len(used) != 0 {
		t.Errorf("used = %v, want nothing", used)
	}
	blockers, ok := dp.ReclaimBlockers("kb-dropped")
	if !ok || !reflect.DeepEqual(blockers, []ReclaimBlocker{{Address: ReclaimBlockerLocalNode, Reason: ReclaimBlockerNoCursor}}) {
		t.Errorf("blockers = %v/%v, want this node", blockers, ok)
	}
}

type scopedCompactorWAL struct {
	recordingCompactorWAL
	kbs []string
}

func (w *scopedCompactorWAL) KnowledgeBases() []string { return w.kbs }

// "No pass yet" and "nothing blocked" lead an operator to opposite conclusions.
func TestLocalDataPlane_ReclaimBlockersUnknownBeforeTheFirstPass(t *testing.T) {
	wal := &recordingCompactorWAL{}
	peers := &peerCursors{cursors: map[string]map[string]int64{"p1": {"kb-1": 9}}}
	dp := reclaimPlane(t, wal, peers, []string{"p1"}, "kb-1")

	if _, ok := dp.ReclaimBlockers("kb-1"); ok {
		t.Fatal("no pass has run, so nothing can be judged yet")
	}
	if _, err := dp.ReclaimChanges(context.Background()); err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if blockers, ok := dp.ReclaimBlockers("kb-1"); !ok || len(blockers) != 0 {
		t.Errorf("blockers = %v/%v, want an empty judged answer", blockers, ok)
	}
}

// Nothing to reclaim is not a failure: no knowledge base means no compaction call.
func TestLocalDataPlane_ReclaimChangesDoesNothingWithoutWatermarks(t *testing.T) {
	wal := &recordingCompactorWAL{}
	dp := reclaimPlane(t, wal, &peerCursors{}, nil)

	used, err := dp.ReclaimChanges(context.Background())
	if err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if len(used) != 0 || len(wal.compactions) != 0 {
		t.Errorf("used = %v, compactions = %v, want neither", used, wal.compactions)
	}
}

// A WAL that cannot compact is not an error either — the in-memory one just grows.
func TestLocalDataPlane_ReclaimChangesIsQuietWhenTheWALCannotCompact(t *testing.T) {
	dp := reclaimPlane(t, &stubWAL{t: &tracer{}}, &peerCursors{}, nil, "kb-1")

	used, err := dp.ReclaimChanges(context.Background())
	if err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if len(used) != 0 {
		t.Errorf("used = %v, want empty", used)
	}
}

// With no way to ask the replicas there is no judgement, so nothing is reclaimed.
func TestLocalDataPlane_ReclaimChangesNeedsACursorQuery(t *testing.T) {
	wal := &recordingCompactorWAL{}
	dp := reclaimPlane(t, wal, nil, nil, "kb-1")

	if _, err := dp.ReclaimChanges(context.Background()); err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if len(wal.compactions) != 0 {
		t.Errorf("compactions = %v, want none without a cursor query", wal.compactions)
	}
}

// A compaction failure must surface: silently believing the WAL shrank would leave
// the operator with an unbounded log and no signal.
func TestLocalDataPlane_ReclaimChangesReportsCompactionFailure(t *testing.T) {
	wal := &recordingCompactorWAL{compactErr: errors.New("disk full")}
	dp := reclaimPlane(t, wal, &peerCursors{}, nil, "kb-1")

	if _, err := dp.ReclaimChanges(context.Background()); err == nil {
		t.Fatal("a failed compaction must be reported")
	}
}

// The furthest cursor wins, and everyone who reached it is named: they hold every version
// below it too. A silent peer is skipped rather than held against the answer.
func TestLocalDataPlane_PeerHolders(t *testing.T) {
	peers := &peerCursors{cursors: map[string]map[string]int64{
		"p3": {"kb-1": 9},
		"p1": {"kb-1": 9},
		"p2": {"kb-1": 4},
		"p4": {},
	}}
	dp := reclaimPlane(t, &stubWAL{t: &tracer{}}, peers, []string{"p3", "p1", "p2", "p4", "dead"})

	version, addrs, ok := dp.PeerHolders(context.Background(), "kb-1")
	if !ok || version != 9 || !reflect.DeepEqual(addrs, []string{"p1", "p3"}) {
		t.Fatalf("PeerHolders = %d, %v, %v; want 9, [p1 p3], true", version, addrs, ok)
	}

	if _, _, ok := dp.PeerHolders(context.Background(), "kb-unknown"); ok {
		t.Error("nobody holds it, so there is no answer to cache")
	}
}
