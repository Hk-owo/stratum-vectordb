package integration_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"

	pb "stratum/api/proto/stratum"
	"stratum/internal/plane"
	"stratum/internal/raft"
	stratumsync "stratum/internal/sync"
	"stratum/internal/types"
)

// d2DeletedStub answers the liveness read from a fixed "these ids are gone" list — the
// metadata's CURRENT state (docs/known-gaps.md §B). "Gone" is what makes a gap unfillable,
// and unlike the removal record this replaced, nothing can prune it away.
type d2DeletedStub struct {
	deleted []int64
}

func (s *d2DeletedStub) VersionLiveness(_ context.Context, _ string, fromExclusive, toInclusive *int64) ([]int64, int64, error) {
	bound := int64(0)
	removed := make(map[int64]bool, len(s.deleted))
	for _, id := range s.deleted {
		removed[id] = true
		if id > bound {
			bound = id
		}
	}
	if toInclusive != nil && *toInclusive > bound {
		bound = *toInclusive
	}
	alive := make([]int64, 0, bound)
	for id := int64(1); id <= bound; id++ {
		if fromExclusive != nil && id <= *fromExclusive {
			continue
		}
		if toInclusive != nil && id > *toInclusive {
			break
		}
		if !removed[id] {
			alive = append(alive, id)
		}
	}
	return alive, bound, nil
}

// d2Executor flags it if the incremental replay path ever runs. This case is about
// the full-state fallback, which reaches the stores through the puller, not through
// the write transaction.
type d2Executor struct {
	calls atomic.Int32
}

func (e *d2Executor) WriteVersionStorage(context.Context, string, int64, int64, []types.DocChange) ([]string, error) {
	e.calls.Add(1)
	return nil, errors.New("the incremental replay path must not run in the full-state fallback case")
}

// d2PullRecorder wraps the real puller and records which versions the plane asked
// for. It is the case's evidence that the full-state path ran: a version that was
// confirmed deleted must never appear in the per-version pulls.
type d2PullRecorder struct {
	inner plane.VersionPuller
	mu    sync.Mutex
	byVer []int64
	byAll []int64
}

func (p *d2PullRecorder) PullVersion(ctx context.Context, addr, kbID string, versionID int64) error {
	p.mu.Lock()
	p.byVer = append(p.byVer, versionID)
	p.mu.Unlock()
	return p.inner.PullVersion(ctx, addr, kbID, versionID)
}

func (p *d2PullRecorder) PullVersionData(ctx context.Context, addr, kbID string, versionID int64) error {
	p.mu.Lock()
	p.byAll = append(p.byAll, versionID)
	p.mu.Unlock()
	return p.inner.PullVersionData(ctx, addr, kbID, versionID)
}

func (p *d2PullRecorder) pulledVersionByVersion(versionID int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range p.byVer {
		if v == versionID {
			return true
		}
	}
	return false
}

func (p *d2PullRecorder) pulledFullState(versionID int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range p.byAll {
		if v == versionID {
			return true
		}
	}
	return false
}

// TestRealStack_BackfillFallsBackToFullStateTransfer is the §6.4 end-to-end case: a
// node that cannot fill a gap version by version — because the metadata says one of
// those versions is gone — transfers ONE whole version's state and lands that
// version's real document set.
//
// What is real here: both nodes, their Pebble stores, the vecstore chunk store, the
// gRPC pull, and the data the follower ends up holding. What is simulated: the "v3
// was deleted" signal, which comes from a stub rather than from actually deleting a
// middle version. Simulating it keeps the case independent of the
// delete-middle-version implementation, while exercising the exact decision this
// path makes — "metadata says gone, so do not walk the gap".
func TestRealStack_BackfillFallsBackToFullStateTransfer(t *testing.T) {
	vecLeader := startVecstoreServerForTest(t)
	vecFollower := startVecstoreServerForTest(t)
	embedURL := startMockEmbedForTest(t, 4)

	raft1, raft2 := freeLoopbackAddr(t), freeLoopbackAddr(t)
	grpc1, grpc2 := freeLoopbackAddr(t), freeLoopbackAddr(t)
	peersFor := func(int64) []raft.PeerConfig {
		return []raft.PeerConfig{
			{ID: 1, RaftAddr: raft1, ServiceAddr: grpc1},
			{ID: 2, RaftAddr: raft2, ServiceAddr: grpc2},
		}
	}
	n1 := newRealNodeWithAddrs(t, 1, peersFor(1), vecLeader, embedURL, raft1, grpc1)
	n2 := newRealNodeWithAddrs(t, 2, peersFor(2), vecFollower, embedURL, raft2, grpc2)

	leader := waitForLeader(t, n1, n2)
	var follower *realNode
	if leader == n1 {
		follower = n2
	} else {
		follower = n1
	}

	ctx := context.Background()
	kbID, v1 := leader.createTestKB(ctx, "d2-full-state")

	// The follower is deliberately NOT wired to pull, so our own plane below is what
	// brings its data across.
	create := func(parent int64, docs ...string) int64 {
		t.Helper()
		changes := make([]*pb.DocChange, 0, len(docs))
		for _, doc := range docs {
			changes = append(changes, &pb.DocChange{
				Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: doc, Content: doc + "-content",
			})
		}
		resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
			KnowledgeBaseId: kbID, ParentVersionId: parent, Changes: changes,
		})
		if err != nil {
			t.Fatalf("leader CreateVersion(parent=%d): %v", parent, err)
		}
		leader.waitVersionReady(ctx, kbID, resp.VersionId)
		return resp.VersionId
	}
	v2 := create(v1, "doc-a", "doc-b")
	v3 := create(v2, "doc-c")
	v4 := create(v3, "doc-d")

	leaderAddr := grpc1
	if leader.nodeID == 2 {
		leaderAddr = grpc2
	}

	// The follower's own plane, over its real stores, pulling from the leader. The
	// recorder is what makes "the fallback really ran" checkable rather than assumed.
	realPuller := stratumsync.NewFollower(
		follower.docStore, follower.chunkDoc, follower.versionDoc, follower.chunkStore, follower.indexMgr)
	puller := &d2PullRecorder{inner: realPuller}
	executor := &d2Executor{}
	dp := plane.NewLocalDataPlane(plane.LocalDataPlaneConfig{
		IndexManager: follower.indexMgr,
		WAL:          follower.fileWAL,
		Executor:     executor,
		Puller:       puller,
		// The source is the leader, and it is known: this case is about the gap, not
		// about source discovery.
		Resolve: func(context.Context, string, int64) (string, bool, error) {
			return leaderAddr, true, nil
		},
		// Accept whatever a pull produced: the digest that would normally gate this
		// is committed by the writer, and asserting on it here would test the write
		// path rather than the fallback.
		Verify: func(context.Context, string, int64) bool { return true },
		// The metadata records v3 as removed.
		Liveness: &d2DeletedStub{deleted: []int64{v3}},
		Logger:   zap.NewNop(),
	})

	// Step 1: bring the follower's cursor to v2 so the gap that follows is a real
	// one. With no cursor yet, the plane simply learns v2.
	if err := dp.EnsureIndex(ctx, kbID, v2); err != nil {
		t.Fatalf("EnsureIndex(%d): %v", v2, err)
	}
	if got := dp.LocalVersionOf(kbID); got != v2 {
		t.Fatalf("localVersion after the first pull = %d, want %d", got, v2)
	}

	// Step 2: apply v4. The gap (v2, v3] contains v3, which the metadata says is
	// gone — so the plane must take a full-state transfer instead of walking it.
	if err := dp.EnsureIndex(ctx, kbID, v4); err != nil {
		t.Fatalf("EnsureIndex(%d): %v (the fallback must not fail)", v4, err)
	}
	if got := dp.LocalVersionOf(kbID); got != v4 {
		t.Errorf("localVersion = %d, want %d (the cursor moves to the transferred version)", got, v4)
	}

	// Evidence, not assumption: the deleted version was never fetched version by
	// version, and the full-state transfer of v4 did happen. Without the first of
	// these, a green test would be equally consistent with the older behaviour of
	// walking the gap and quietly pulling a version that no longer exists.
	if puller.pulledVersionByVersion(v3) {
		t.Errorf("v%d was fetched version by version although the metadata says it is gone", v3)
	}
	if !puller.pulledFullState(v4) {
		t.Errorf("v%d was not transferred as a full state; the fallback did not run", v4)
	}

	// The real check: the follower must hold v4's document set, identical to the
	// leader's. A cursor alone would not prove the data landed.
	leaderDocs, err := leader.versionDoc.ListDocIDs(ctx, kbID, v4)
	if err != nil {
		t.Fatalf("leader ListDocIDs(%d): %v", v4, err)
	}
	followerDocs, err := follower.versionDoc.ListDocIDs(ctx, kbID, v4)
	if err != nil {
		t.Fatalf("follower ListDocIDs(%d): %v", v4, err)
	}
	if len(leaderDocs) == 0 {
		t.Fatal("the leader reports no documents for v4; the case would prove nothing")
	}
	if len(followerDocs) != len(leaderDocs) {
		t.Fatalf("follower v4 docs = %v, want the leader's %v", followerDocs, leaderDocs)
	}
	for i := range leaderDocs {
		if followerDocs[i] != leaderDocs[i] {
			t.Fatalf("follower v4 docs = %v, want the leader's %v", followerDocs, leaderDocs)
		}
	}

	if n := executor.calls.Load(); n != 0 {
		t.Errorf("the incremental replay path ran %d time(s); this case is about the full-state fallback", n)
	}
}
