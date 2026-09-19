package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/coordinator"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// --- The derivation, pinned directly -------------------------------------
//
// deriveStage/awaitReached/awaitTerminal carry the semantics the whole RPC
// rests on ("not yet is not an error", "durable satisfies a durable target",
// "terminal stops the wait"), so they are tested as functions rather than only
// through a wait loop that would hide which branch answered.

func TestDeriveStage_CoversEveryCombination(t *testing.T) {
	cases := []struct {
		name string
		v    types.VersionMeta
		want string
	}{
		{
			"deleting wins",
			types.VersionMeta{Deleting: true, IndexStatus: types.IndexStatusReady, DataStatus: types.DataStatusDurable},
			StageDeleting,
		},
		{
			"data side terminal",
			types.VersionMeta{DataStatus: types.DataStatusFailedPermanent},
			StageDataFailedPermanent,
		},
		{
			"index side terminal",
			types.VersionMeta{IndexStatus: types.IndexStatusFailedPermanent},
			StageIndexFailedPermanent,
		},
		{
			"index ready",
			types.VersionMeta{IndexStatus: types.IndexStatusReady, DataStatus: types.DataStatusDurable},
			StageIndexReady,
		},
		{
			"index failed once the data settled",
			types.VersionMeta{IndexStatus: types.IndexStatusFailed, DataStatus: types.DataStatusDurable},
			StageIndexFailed,
		},
		{
			"index failed while the data is still arriving (the data side is the story)",
			types.VersionMeta{IndexStatus: types.IndexStatusFailed, DataStatus: types.DataStatusPending},
			StageDataPending,
		},
		{
			"data durable, index still building",
			types.VersionMeta{IndexStatus: types.IndexStatusPending, DataStatus: types.DataStatusDurable},
			StageDataDurable,
		},
		{
			"fresh version",
			types.VersionMeta{IndexStatus: types.IndexStatusPending, DataStatus: types.DataStatusPending},
			StageDataPending,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := deriveStage(tc.v); got != tc.want {
				t.Errorf("deriveStage(%+v) = %s, want %s", tc.v, got, tc.want)
			}
		})
	}
}

func TestAwaitReached_DataDurableTargetIsInclusive(t *testing.T) {
	reached := []string{StageDataDurable, StageIndexReady}
	for _, stage := range reached {
		if !awaitReached(stage, pb.AwaitTarget_AWAIT_TARGET_DATA_DURABLE) {
			t.Errorf("awaitReached(%s, DATA_DURABLE) = false, want true", stage)
		}
	}
	for _, stage := range []string{StageDataPending, StageIndexFailed, StageDataFailedPermanent, StageIndexFailedPermanent, StageDeleting} {
		if awaitReached(stage, pb.AwaitTarget_AWAIT_TARGET_DATA_DURABLE) {
			t.Errorf("awaitReached(%s, DATA_DURABLE) = true, want false", stage)
		}
	}
	// An INDEX_READY target is satisfied by READY alone: durable data is not
	// queryable, and asking for READY means asking to be able to read.
	if !awaitReached(StageIndexReady, pb.AwaitTarget_AWAIT_TARGET_INDEX_READY) {
		t.Error("awaitReached(INDEX_READY, INDEX_READY) = false, want true")
	}
	if awaitReached(StageDataDurable, pb.AwaitTarget_AWAIT_TARGET_INDEX_READY) {
		t.Error("awaitReached(DATA_DURABLE, INDEX_READY) = true, want false")
	}
}

func TestAwaitTerminal_StopsOnEverySettledOutcome(t *testing.T) {
	for _, stage := range []string{StageIndexFailed, StageDataFailedPermanent, StageIndexFailedPermanent, StageDeleting} {
		if !awaitTerminal(stage) {
			t.Errorf("awaitTerminal(%s) = false, want true", stage)
		}
	}
	for _, stage := range []string{StageDataPending, StageDataDurable, StageIndexReady} {
		if awaitTerminal(stage) {
			t.Errorf("awaitTerminal(%s) = true, want false", stage)
		}
	}
}

// --- The RPC's behaviour -------------------------------------------------

func TestAwaitVersion_NotReachedIsAnAnswerNotAnError(t *testing.T) {
	svc, _, kbID, v := newAwaitHarness(t)

	start := time.Now()
	resp, err := svc.AwaitVersion(context.Background(), &pb.AwaitVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v,
		Target:          pb.AwaitTarget_AWAIT_TARGET_INDEX_READY,
		WaitTimeoutMs:   300,
	})
	if err != nil {
		t.Fatalf("AwaitVersion on a PENDING version = %v, want an answer", err)
	}
	if resp.GetStage() != StageDataPending {
		t.Errorf("stage = %s, want %s", resp.GetStage(), StageDataPending)
	}
	if resp.GetRetryAfterMs() <= 0 {
		t.Errorf("retry_after_ms = %d, want a positive re-ask interval", resp.GetRetryAfterMs())
	}
	if resp.GetVersion().GetVersionId() != v {
		t.Errorf("version.version_id = %d, want %d", resp.GetVersion().GetVersionId(), v)
	}
	// It must actually wait for the window it was given, rather than answering
	// once and calling that a wait.
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Errorf("returned after %v, want it to wait out the 300 ms window", elapsed)
	}
}

func TestAwaitVersion_ReportsTheTargetStageWhenItArrives(t *testing.T) {
	svc, rn, kbID, v := newAwaitHarness(t)
	ctx := context.Background()

	// The version becomes durable and READY while the caller is waiting.
	go func() {
		time.Sleep(80 * time.Millisecond)
		_ = rn.ProposeMarkVersionDataDurable(ctx, v)
		_ = rn.ProposeUpdateVersionStatus(ctx, v, types.IndexStatusReady, 0)
	}()

	resp, err := svc.AwaitVersion(ctx, &pb.AwaitVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v,
		Target:          pb.AwaitTarget_AWAIT_TARGET_INDEX_READY,
		WaitTimeoutMs:   2000,
	})
	if err != nil {
		t.Fatalf("AwaitVersion: %v", err)
	}
	if resp.GetStage() != StageIndexReady {
		t.Errorf("stage = %s, want %s", resp.GetStage(), StageIndexReady)
	}
	if resp.GetVersion().GetIndexStatus() != pb.IndexStatus_INDEX_STATUS_READY {
		t.Errorf("version.index_status = %v, want READY (the whole metadata travels, so a caller never has to guess)",
			resp.GetVersion().GetIndexStatus())
	}
}

func TestAwaitVersion_DurableTargetReturnsBeforeTheIndexIsReady(t *testing.T) {
	svc, rn, kbID, v := newAwaitHarness(t)
	ctx := context.Background()
	if err := rn.ProposeMarkVersionDataDurable(ctx, v); err != nil {
		t.Fatalf("ProposeMarkVersionDataDurable: %v", err)
	}

	resp, err := svc.AwaitVersion(ctx, &pb.AwaitVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v,
		Target:          pb.AwaitTarget_AWAIT_TARGET_DATA_DURABLE,
		WaitTimeoutMs:   5000,
	})
	if err != nil {
		t.Fatalf("AwaitVersion: %v", err)
	}
	if resp.GetStage() != StageDataDurable {
		t.Errorf("stage = %s, want %s: a DATA_DURABLE target is satisfied by durable data, not by READY",
			resp.GetStage(), StageDataDurable)
	}
}

func TestAwaitVersion_TerminalVerdictIsReportedNotRaised(t *testing.T) {
	svc, rn, kbID, v := newAwaitHarness(t)
	ctx := context.Background()
	if err := rn.ProposeMarkVersionFailedPermanent(ctx, kbID, v, types.FailureSideData, "writer died", 5); err != nil {
		t.Fatalf("ProposeMarkVersionFailedPermanent: %v", err)
	}

	resp, err := svc.AwaitVersion(ctx, &pb.AwaitVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v,
		Target:          pb.AwaitTarget_AWAIT_TARGET_INDEX_READY,
		WaitTimeoutMs:   5000,
	})
	if err != nil {
		t.Fatalf("AwaitVersion on a permanently failed version = %v, want an answer carrying the verdict", err)
	}
	if resp.GetStage() != StageDataFailedPermanent {
		t.Errorf("stage = %s, want %s", resp.GetStage(), StageDataFailedPermanent)
	}
}

func TestAwaitVersion_DeletingStopsTheWait(t *testing.T) {
	svc, rn, kbID, v := newAwaitHarness(t)
	ctx := context.Background()
	// A version has to be settled before DeleteVersion will take it, and
	// deleting it must not make the waiter sit out its whole window.
	if err := rn.ProposeUpdateVersionStatus(ctx, v, types.IndexStatusReady, 0); err != nil {
		t.Fatalf("READY: %v", err)
	}
	if _, err := rn.ProposeMarkVersionDeleting(ctx, kbID, v, types.VersionDeleteSingle); err != nil {
		t.Fatalf("ProposeMarkVersionDeleting: %v", err)
	}

	start := time.Now()
	resp, err := svc.AwaitVersion(ctx, &pb.AwaitVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v,
		Target:          pb.AwaitTarget_AWAIT_TARGET_INDEX_READY,
		WaitTimeoutMs:   5000,
	})
	if err != nil {
		t.Fatalf("AwaitVersion on a deleting version: %v", err)
	}
	if resp.GetStage() != StageDeleting {
		t.Errorf("stage = %s, want %s", resp.GetStage(), StageDeleting)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("waited %v for a terminal state; DELETING must stop the wait", elapsed)
	}
}

func TestAwaitVersion_CancellationIsHonoured(t *testing.T) {
	svc, _, kbID, v := newAwaitHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := svc.AwaitVersion(ctx, &pb.AwaitVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v,
		Target:          pb.AwaitTarget_AWAIT_TARGET_INDEX_READY,
		WaitTimeoutMs:   5000, // the server would happily wait 5 s
	})
	if err == nil {
		t.Fatal("AwaitVersion with an expiring context = nil error, want the caller's cancellation")
	}
	if code := status.Code(err); code != codes.DeadlineExceeded && code != codes.Canceled {
		t.Errorf("cancellation code = %v, want DeadlineExceeded or Canceled", code)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("returned after %v; a cancelled caller must not be made to finish the wait", elapsed)
	}
}

func TestAwaitVersion_UnknownVersionAndKnowledgeBase(t *testing.T) {
	svc, rn, kbID, v := newAwaitHarness(t)
	ctx := context.Background()

	// The window is short here on purpose: a NotFound is now reported only after
	// the wait has expired without the version ever becoming visible (see
	// TestAwaitVersion_NotVisibleYetIsNotReportedAsNotFound), so a long window
	// would make "the version really is absent" a slow answer rather than a wrong
	// one.
	_, err := svc.AwaitVersion(ctx, &pb.AwaitVersionRequest{KnowledgeBaseId: kbID, VersionId: v + 999, WaitTimeoutMs: 200})
	if status.Code(err) != codes.NotFound {
		t.Errorf("unknown version: code = %v, want NotFound", status.Code(err))
	}

	// A version id is unique within a knowledge base, not globally: the same id
	// under another KB must not resolve.
	other, err := svc.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name: "other-kb", EmbedConfig: &pb.EmbedConfig{ServiceAddr: "x", ModelId: "m"},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase: %v", err)
	}
	if _, err := rn.ProposeCreateVersion(ctx, other.KnowledgeBaseId, 0); err != nil {
		t.Fatalf("v1 in the other KB: %v", err)
	}
	if _, err := svc.AwaitVersion(ctx, &pb.AwaitVersionRequest{KnowledgeBaseId: other.KnowledgeBaseId, VersionId: v, WaitTimeoutMs: 200}); status.Code(err) != codes.NotFound {
		t.Errorf("version id from another KB: code = %v, want NotFound", status.Code(err))
	}

	if _, err := svc.AwaitVersion(ctx, &pb.AwaitVersionRequest{KnowledgeBaseId: "no-such-kb", VersionId: 1, WaitTimeoutMs: 200}); status.Code(err) != codes.NotFound {
		t.Errorf("unknown KB: code = %v, want NotFound", status.Code(err))
	}
	if _, err := svc.AwaitVersion(ctx, &pb.AwaitVersionRequest{KnowledgeBaseId: kbID, VersionId: 1, Target: pb.AwaitTarget(99)}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("unknown target: code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := svc.AwaitVersion(ctx, &pb.AwaitVersionRequest{VersionId: 1}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("missing kb_id: code = %v, want InvalidArgument", status.Code(err))
	}
}

// --- data_missing --------------------------------------------------------

func TestProbeDataMissing_RespectsTheAgeThreshold(t *testing.T) {
	svc, _, _, v := newAwaitHarness(t)
	checker := &probeSpy{}
	svc.SetPresenceProbe(checker, func() []string { return []string{"r1", "r2"} })

	// The default threshold is what keeps a version allocated a moment ago from
	// being called missing, and below it the probe must not even ask.
	svc.probeDataMissing(context.Background(), types.VersionMeta{
		KBID: "kb", VersionID: v, DataStatus: types.DataStatusPending,
		CreatedAt: time.Now().Unix(), // allocated a moment ago: below the threshold
	}, StageDataPending)
	if calls := checker.callCount(); calls != 0 {
		t.Errorf("HasVersion called %d times below the age threshold, want 0", calls)
	}
}

func TestProbeDataMissing_ThreeStates(t *testing.T) {
	restore := shortenDataMissingMinAge(t)
	defer restore()

	cases := []struct {
		name    string
		checker *probeSpy
		want    bool
	}{
		{"nobody holds it", &probeSpy{holds: map[string]bool{}}, true},
		{"one replica holds it", &probeSpy{holds: map[string]bool{"r2": true}}, false},
		{"every replica unreachable counts as unknown, not missing", &probeSpy{err: errors.New("dial tcp: connection refused")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, _, v := newAwaitHarness(t)
			svc.SetPresenceProbe(tc.checker, func() []string { return []string{"r1", "r2"} })

			got := svc.probeDataMissing(context.Background(),
				types.VersionMeta{KBID: "kb", VersionID: v, DataStatus: types.DataStatusPending}, StageDataPending)
			if got != tc.want {
				t.Errorf("probeDataMissing = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestProbeDataMissing_OnlyAsksWhilePending(t *testing.T) {
	restore := shortenDataMissingMinAge(t)
	defer restore()

	svc, _, _, v := newAwaitHarness(t)
	checker := &probeSpy{holds: map[string]bool{}}
	svc.SetPresenceProbe(checker, func() []string { return []string{"r1"} })

	if got := svc.probeDataMissing(context.Background(),
		types.VersionMeta{KBID: "kb", VersionID: v, DataStatus: types.DataStatusDurable}, StageIndexReady); got {
		t.Error("data_missing = true for a settled version, want false: the question only has meaning while PENDING")
	}
	if calls := checker.callCount(); calls != 0 {
		t.Errorf("HasVersion called %d times for a non-PENDING stage, want 0", calls)
	}
}

func TestProbeDataMissing_CachesTheVerdict(t *testing.T) {
	restore := shortenDataMissingMinAge(t)
	defer restore()

	svc, _, _, v := newAwaitHarness(t)
	checker := &probeSpy{holds: map[string]bool{}}
	svc.SetPresenceProbe(checker, func() []string { return []string{"r1", "r2"} })
	meta := types.VersionMeta{KBID: "kb", VersionID: v, DataStatus: types.DataStatusPending}

	for i := 0; i < 3; i++ {
		if !svc.probeDataMissing(context.Background(), meta, StageDataPending) {
			t.Fatalf("probe #%d = false, want true", i+1)
		}
	}
	// Two replicas, one probing round: a caller polling every 200 ms must not
	// turn into a replica probe every 200 ms (§6.4).
	if calls := checker.callCount(); calls != 2 {
		t.Errorf("HasVersion called %d times across three probes, want 2 (one probing round)", calls)
	}
}

func TestProbeDataMissing_SkipsWhenUnwired(t *testing.T) {
	restore := shortenDataMissingMinAge(t)
	defer restore()

	svc, _, _, v := newAwaitHarness(t) // no SetPresenceProbe: the probe is unwired
	if got := svc.probeDataMissing(context.Background(),
		types.VersionMeta{KBID: "kb", VersionID: v, DataStatus: types.DataStatusPending}, StageDataPending); got {
		t.Error("data_missing = true without a wired probe, want false (unknown is never a claim of loss)")
	}
}

// --- helpers -------------------------------------------------------------

func newAwaitHarness(t *testing.T) (*KnowledgeBaseServiceImpl, *raft.MockRaftNode, string, int64) {
	t.Helper()
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	svc := NewKnowledgeBaseService(rn,
		coordinator.NewMockWriteCoordinator(),
		coordinator.NewMockDeleteCoordinator(),
		coordinator.NewMockDeleteVersionCoordinator(),
	)
	ctx := context.Background()
	resp, err := svc.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name:             "await-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig:      &pb.EmbedConfig{ServiceAddr: "localhost:8080", ModelId: "test-model"},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase: %v", err)
	}
	v, err := rn.ProposeCreateVersion(ctx, resp.KnowledgeBaseId, 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	return svc, rn, resp.KnowledgeBaseId, v
}

// shortenDataMissingMinAge drops the probe's age threshold to zero so a
// just-created version is probeable. dataMissingMinAgeSec is a var precisely so
// tests can shorten it (see its comment in admin.go).
func shortenDataMissingMinAge(t *testing.T) func() {
	t.Helper()
	previous := dataMissingMinAgeSec
	dataMissingMinAgeSec = 0
	return func() { dataMissingMinAgeSec = previous }
}

// probeSpy stands in for the gRPC presence checker and counts how often it was
// asked, which is how the age threshold and the verdict cache are pinned.
//
// holds maps a replica address to whether it answers "yes"; err makes every
// replica unreachable, the "unknown" case the probe must not report as missing.
// (datamissing_test.go has a stubPresence for the pure function; this one adds
// the call count the await path's caching needs.)
type probeSpy struct {
	mu    sync.Mutex
	holds map[string]bool
	err   error
	calls int
}

func (s *probeSpy) HasVersion(_ context.Context, replicaAddr, _ string, _ int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return false, s.err
	}
	return s.holds[replicaAddr], nil
}

func (s *probeSpy) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestAwaitVersion_NotVisibleYetIsNotReportedAsNotFound pins the distinction the
// live cluster forced: a follower that has not applied the entry yet answers the
// same local question as a node looking for a version that never existed. Only
// one of those means "your write never happened", and a caller cannot tell them
// apart — so the wait must absorb the replication delay instead of reporting it.
//
// Measured on a real 3+3 cluster: the version the leader had committed was
// invisible on a follower that the station then load-balanced the read to, and a
// first await right after a write landed in exactly that window.
func TestAwaitVersion_NotVisibleYetIsNotReportedAsNotFound(t *testing.T) {
	svc, rn, kbID, v := newAwaitHarness(t)

	// The version exists; this node cannot see it yet.
	lagging := &laggingRaftNode{MockRaftNode: rn}
	svc.raftNode = lagging

	go func() {
		time.Sleep(150 * time.Millisecond)
		lagging.makeVisible()
	}()

	resp, err := svc.AwaitVersion(context.Background(), &pb.AwaitVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v,
		Target:          pb.AwaitTarget_AWAIT_TARGET_INDEX_READY,
		WaitTimeoutMs:   3000,
	})
	if err != nil {
		t.Fatalf("AwaitVersion while the node was catching up = %v, want it to keep waiting", err)
	}
	if got := resp.GetVersion().GetVersionId(); got != v {
		t.Errorf("version.version_id = %d, want %d", got, v)
	}
}

// laggingRaftNode hides every version until makeVisible is called: a follower
// that has not applied the entry yet, as the service layer sees it.
type laggingRaftNode struct {
	*raft.MockRaftNode
	mu      sync.Mutex
	visible bool
}

func (l *laggingRaftNode) GetVersion(ctx context.Context, kbID string, versionID int64) (types.VersionMeta, error) {
	l.mu.Lock()
	visible := l.visible
	l.mu.Unlock()
	if !visible {
		return types.VersionMeta{}, stratumerrors.ErrVersionNotFound
	}
	return l.MockRaftNode.GetVersion(ctx, kbID, versionID)
}

func (l *laggingRaftNode) makeVisible() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.visible = true
}

// TestAwaitProbe_OverTheConcurrencyCapAnswersUnknownWithoutBlocking pins §6.4's
// other bound: a probe that cannot get a slot answers "unknown" (false) at once
// rather than queueing, because await's latency must not be spent waiting on its
// own probe — the caller is a client request, and §5 contract 5 caps how long
// that may take.
func TestAwaitProbe_OverTheConcurrencyCapAnswersUnknownWithoutBlocking(t *testing.T) {
	restore := shortenDataMissingMinAge(t)
	defer restore()

	blocking := &blockingPresence{release: make(chan struct{})}
	probe := newAwaitProbe()
	replicas := []string{"r1"}

	// Fill every slot with a probe of a DIFFERENT version: the verdict cache is
	// keyed per version, so distinct versions are distinct probes.
	var wg sync.WaitGroup
	for i := 0; i < awaitProbeConcurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			probe.lookupOrProbe(context.Background(),
				types.VersionMeta{KBID: "kb", VersionID: int64(i + 1), DataStatus: types.DataStatusPending},
				replicas, blocking)
		}(i)
	}
	blocking.waitForInFlight(t, awaitProbeConcurrency)

	// One more version: over the cap, so it must come straight back.
	done := make(chan bool, 1)
	go func() {
		done <- probe.lookupOrProbe(context.Background(),
			types.VersionMeta{KBID: "kb", VersionID: 9999, DataStatus: types.DataStatusPending},
			replicas, blocking)
	}()
	select {
	case got := <-done:
		if got {
			t.Error("data_missing = true for a probe that never ran, want the unknown answer (false)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a probe over the concurrency cap blocked; it must answer unknown instead of queueing")
	}

	close(blocking.release)
	wg.Wait()
}

// blockingPresence holds every probe until release is closed, so a test can pin
// what happens while slots are taken.
type blockingPresence struct {
	release chan struct{}

	mu       sync.Mutex
	inFlight int
}

func (b *blockingPresence) HasVersion(context.Context, string, string, int64) (bool, error) {
	b.mu.Lock()
	b.inFlight++
	b.mu.Unlock()
	<-b.release
	return false, nil
}

func (b *blockingPresence) waitForInFlight(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		got := b.inFlight
		b.mu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("only %d probes reached the checker, want %d (the slots never filled)", b.inFlight, want)
}

// --- event-driven wake-ups (§12 item 1) -----------------------------------

// fakeVersionWatcher hands the await path a channel the test drives, so the
// event-driven path can be exercised without a live apply loop.
type fakeVersionWatcher struct {
	mu         sync.Mutex
	watchCalls int
	stopCalls  int
	ch         chan struct{}
}

func newFakeVersionWatcher() *fakeVersionWatcher {
	return &fakeVersionWatcher{ch: make(chan struct{}, 1)}
}

func (f *fakeVersionWatcher) WatchVersion(int64) (<-chan struct{}, func()) {
	f.mu.Lock()
	f.watchCalls++
	f.mu.Unlock()
	return f.ch, func() {
		f.mu.Lock()
		f.stopCalls++
		f.mu.Unlock()
	}
}

func (f *fakeVersionWatcher) signal() {
	select {
	case f.ch <- struct{}{}:
	default:
	}
}

func (f *fakeVersionWatcher) counts() (watches, stops int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.watchCalls, f.stopCalls
}

// TestAwaitVersion_WakesOnTheWatcherInsteadOfPolling is the whole point of §12
// item 1: the wait costs what the change costs. Without the watcher this call
// would sit out a full awaitPollInterval after the state had already moved.
func TestAwaitVersion_WakesOnTheWatcherInsteadOfPolling(t *testing.T) {
	svc, rn, kbID, v := newAwaitHarness(t)
	watcher := newFakeVersionWatcher()
	svc.SetVersionWatcher(watcher)

	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = rn.ProposeUpdateVersionStatus(context.Background(), v, types.IndexStatusReady, 0)
		watcher.signal()
	}()

	start := time.Now()
	resp, err := svc.AwaitVersion(context.Background(), &pb.AwaitVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v,
		Target:          pb.AwaitTarget_AWAIT_TARGET_INDEX_READY,
		WaitTimeoutMs:   5000,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("AwaitVersion: %v", err)
	}
	if resp.GetStage() != StageIndexReady {
		t.Fatalf("stage = %s, want %s", resp.GetStage(), StageIndexReady)
	}
	// Three quarters of a poll interval: an event-driven wake-up lands in
	// milliseconds, whereas the polling path cannot come back before a full
	// interval has passed.
	if elapsed > awaitPollInterval*3/4 {
		t.Errorf("returned after %v; with a watcher it should return as soon as the state moved (poll interval is %v)",
			elapsed, awaitPollInterval)
	}
	if watches, stops := watcher.counts(); watches == 0 || stops == 0 {
		t.Errorf("watches = %d, stops = %d; the wait must register and then release its registration", watches, stops)
	}
}

// TestAwaitVersion_StillWorksWithoutAWatcher keeps the fallback honest: a
// deployment (or a storage-side raft shape) with no watcher must still converge.
func TestAwaitVersion_StillWorksWithoutAWatcher(t *testing.T) {
	svc, rn, kbID, v := newAwaitHarness(t)

	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = rn.ProposeUpdateVersionStatus(context.Background(), v, types.IndexStatusReady, 0)
	}()

	resp, err := svc.AwaitVersion(context.Background(), &pb.AwaitVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v,
		Target:          pb.AwaitTarget_AWAIT_TARGET_INDEX_READY,
		WaitTimeoutMs:   5000,
	})
	if err != nil {
		t.Fatalf("AwaitVersion without a watcher: %v", err)
	}
	if resp.GetStage() != StageIndexReady {
		t.Errorf("stage = %s, want %s", resp.GetStage(), StageIndexReady)
	}
}

// --- §12 item 2: the leader's aggregate answers before any probe ----------

// fakeHolderSource stands in for the control leader's §7.13.4 aggregate.
type fakeHolderSource struct {
	holders []VersionHolder
	ok      bool
}

func (f *fakeHolderSource) DataVersionHolders(string, int64) ([]VersionHolder, bool) {
	return f.holders, f.ok
}

// TestProbeDataMissing_UsesTheLeadersAggregateFirst pins the cheap path: a holder
// NAMED by the aggregate is proof the data is there, so no replica is asked.
func TestProbeDataMissing_UsesTheLeadersAggregateFirst(t *testing.T) {
	restore := shortenDataMissingMinAge(t)
	defer restore()

	svc, _, _, v := newAwaitHarness(t)
	checker := &probeSpy{holds: map[string]bool{}}
	svc.SetPresenceProbe(checker, func() []string { return []string{"r1"} })
	svc.SetVersionHolderSource(&fakeHolderSource{
		holders: []VersionHolder{{NodeID: 11, Address: "r1"}},
		ok:      true,
	})

	got := svc.probeDataMissing(context.Background(),
		types.VersionMeta{KBID: "kb", VersionID: v, DataStatus: types.DataStatusPending}, StageDataPending)
	if got {
		t.Error("data_missing = true while the leader's aggregate names a holder")
	}
	if calls := checker.callCount(); calls != 0 {
		t.Errorf("HasVersion called %d times; a named holder settles it without probing", calls)
	}
}

// TestProbeDataMissing_EmptyAggregateStillProbes keeps the other direction
// honest: an empty aggregate means "nobody I have heard from", never "nobody has
// it" (§3.1), so the probe still runs — and a non-leader (ok == false) goes
// straight to it.
func TestProbeDataMissing_EmptyAggregateStillProbes(t *testing.T) {
	restore := shortenDataMissingMinAge(t)
	defer restore()

	for _, tc := range []struct {
		name     string
		source   VersionHolderSource
		wantGone bool
	}{
		{"empty aggregate from the leader", &fakeHolderSource{ok: true}, true},
		{"aggregate unavailable (not the leader)", &fakeHolderSource{ok: false}, true},
		{"aggregate names a holder", &fakeHolderSource{holders: []VersionHolder{{NodeID: 11, Address: "r1"}}, ok: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, _, v := newAwaitHarness(t)
			checker := &probeSpy{holds: map[string]bool{}}
			svc.SetPresenceProbe(checker, func() []string { return []string{"r1"} })
			svc.SetVersionHolderSource(tc.source)

			got := svc.probeDataMissing(context.Background(),
				types.VersionMeta{KBID: "kb", VersionID: v, DataStatus: types.DataStatusPending}, StageDataPending)
			if got != tc.wantGone {
				t.Errorf("data_missing = %v, want %v", got, tc.wantGone)
			}
			if tc.wantGone && checker.callCount() == 0 {
				t.Error("the probe never ran: an empty aggregate is not evidence that nobody holds the data")
			}
			if !tc.wantGone && checker.callCount() != 0 {
				t.Error("a named holder did not stop the probe")
			}
		})
	}
}
