// Package-level tests for the in-process plane implementations. They pin the
// two things the upper layers rely on: every report maps onto the intended
// metadata proposal, and the DataPlane's own policy (when to pull, when to
// retry, what counts as durable) stays on the storage side of the contract.
package plane

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// --- stubs ------------------------------------------------------------------

// stubMeta implements MetadataProposer and records what was proposed.
type stubMeta struct {
	kbs      []types.KnowledgeBaseMeta
	versions map[string][]types.VersionMeta
	listErr  error

	statusCalls []statusCall
	digestCalls []digestCall

	// dataDurableCalls records the control layer's own data-side promotions
	// (§10.1b) — the ones §7.9's cursor report drives at startup.
	dataDurableCalls []int64

	permanentCalls []permanentCall
	permanentErr   error
}

// permanentCall records one ProposeMarkVersionFailedPermanent proposal.
type permanentCall struct {
	kbID      string
	versionID int64
	// side is which half of the version the verdict settles (§10.1b). The data
	// side and the index side are counted and recorded separately, so a test
	// asserting "the data side died" has to say which side it means.
	side   types.FailureSide
	reason string
	count  int32
}

type statusCall struct {
	versionID int64
	status    types.IndexStatus
	// nodeID is which replica made the report (0 = the control layer itself).
	// Tests assert on it because §8.6(d)'s serving count is built out of these
	// identities, not out of the status alone.
	nodeID int64
}

type digestCall struct {
	versionID int64
	digest    string
}

func (m *stubMeta) ListKnowledgeBases(context.Context) ([]types.KnowledgeBaseMeta, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.kbs, nil
}

func (m *stubMeta) ListVersions(_ context.Context, kbID string) ([]types.VersionMeta, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.versions[kbID], nil
}

func (m *stubMeta) ProposeUpdateVersionStatus(_ context.Context, versionID int64, status types.IndexStatus, nodeID int64) error {
	m.statusCalls = append(m.statusCalls, statusCall{versionID: versionID, status: status, nodeID: nodeID})
	return nil
}

func (m *stubMeta) ProposeMarkVersionFailedPermanent(_ context.Context, kbID string, versionID int64, side types.FailureSide, reason string, count int32) error {
	if m.permanentErr != nil {
		return m.permanentErr
	}
	m.permanentCalls = append(m.permanentCalls, permanentCall{
		kbID: kbID, versionID: versionID, side: side, reason: reason, count: count,
	})
	return nil
}

func (m *stubMeta) ProposeMarkVersionDataDurable(_ context.Context, versionID int64) error {
	m.dataDurableCalls = append(m.dataDurableCalls, versionID)
	return nil
}

func (m *stubMeta) ProposeUpdateVersionSummary(_ context.Context, versionID int64, digest string) error {
	m.digestCalls = append(m.digestCalls, digestCall{versionID: versionID, digest: digest})
	return nil
}

// stubIndexStore implements IndexStore and records the calls that matter.
type stubIndexStore struct {
	exists    map[int64]bool
	existsErr error

	triggered []int64
	searched  bool
	retention []retentionCall
}

type retentionCall struct {
	kbID         string
	protectedIDs []int64
}

func (s *stubIndexStore) Search(context.Context, string, int64, []float32, int) ([]types.SearchResult, error) {
	s.searched = true
	return nil, nil
}

func (s *stubIndexStore) TriggerBuild(_ context.Context, _ string, versionID int64) error {
	s.triggered = append(s.triggered, versionID)
	return nil
}

func (s *stubIndexStore) IndexExists(_ context.Context, _ string, versionID int64) (bool, error) {
	if s.existsErr != nil {
		return false, s.existsErr
	}
	return s.exists[versionID], nil
}

func (s *stubIndexStore) EnforceDiskRetention(_ context.Context, kbID string, protectedIDs []int64) error {
	s.retention = append(s.retention, retentionCall{kbID: kbID, protectedIDs: protectedIDs})
	return nil
}

// stubPuller counts pulls.
type stubPuller struct {
	calls int
	err   error
}

// PullVersionData satisfies VersionPuller.
func (p *stubPuller) PullVersionData(ctx context.Context, sourceAddr, kbID string, versionID int64) error {
	return p.PullVersion(ctx, sourceAddr, kbID, versionID)
}

func (p *stubPuller) PullVersion(context.Context, string, string, int64) error {
	p.calls++
	return p.err
}

// sequencedPuller can delay each call and fail them, so a test can tell "a
// transfer that takes a while" from "a transfer that fails" — the two states the
// pull loop's bound has to distinguish.
type sequencedPuller struct {
	calls int
	err   error
	delay time.Duration
}

func (p *sequencedPuller) PullVersionData(ctx context.Context, sourceAddr, kbID string, versionID int64) error {
	return p.PullVersion(ctx, sourceAddr, kbID, versionID)
}

func (p *sequencedPuller) PullVersion(ctx context.Context, _, _ string, _ int64) error {
	p.calls++
	if p.delay > 0 {
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.err
}

// tracer records the order in which the write transaction's steps run, so a
// test can pin the framing (BEGIN before the writes, COMMIT after them).
type tracer struct {
	trace []string
}

// stubWAL records the transaction framing.
type stubWAL struct {
	t *tracer

	beginErr error
}

func (w *stubWAL) WriteBegin(_ context.Context, kbID string, parentVersionID int64, _ []types.DocChange) error {
	if w.beginErr != nil {
		return w.beginErr
	}
	w.t.trace = append(w.t.trace, fmt.Sprintf("begin:%s:%d", kbID, parentVersionID))
	return nil
}

func (w *stubWAL) WriteCommit(_ context.Context, versionID int64) error {
	w.t.trace = append(w.t.trace, fmt.Sprintf("commit:%d", versionID))
	return nil
}

// stubExecutor records the storage-write step.
type stubExecutor struct {
	t      *tracer
	docIDs []string
	err    error
}

func (e *stubExecutor) WriteVersionStorage(_ context.Context, _ string, _, versionID int64, _ []types.DocChange) ([]string, error) {
	e.t.trace = append(e.t.trace, fmt.Sprintf("write:%d", versionID))
	return e.docIDs, e.err
}

var (
	_ MetadataProposer     = (*stubMeta)(nil)
	_ IndexStore           = (*stubIndexStore)(nil)
	_ VersionPuller        = (*stubPuller)(nil)
	_ TransactionWAL       = (*stubWAL)(nil)
	_ VersionWriteExecutor = (*stubExecutor)(nil)
)

// --- LocalControlPlane ------------------------------------------------------

func TestLocalControlPlane_ReportIndexReady(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta)

	if err := cp.ReportIndexReady(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("ReportIndexReady: %v", err)
	}
	if len(meta.statusCalls) != 1 {
		t.Fatalf("status proposals = %+v, want exactly one", meta.statusCalls)
	}
	if got := meta.statusCalls[0]; got.versionID != 7 || got.status != types.IndexStatusReady {
		t.Fatalf("proposal = %+v, want READY for v7", got)
	}
}

func TestLocalControlPlane_ReportDataDurable(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta)

	if err := cp.ReportDataDurable(context.Background(), "kb-1", 3, "digest-3"); err != nil {
		t.Fatalf("ReportDataDurable: %v", err)
	}
	if len(meta.digestCalls) != 1 {
		t.Fatalf("digest proposals = %+v, want exactly one", meta.digestCalls)
	}
	if got := meta.digestCalls[0]; got.versionID != 3 || got.digest != "digest-3" {
		t.Fatalf("proposal = %+v, want (3, digest-3)", got)
	}
}

func TestLocalControlPlane_ReportAvailability(t *testing.T) {
	cases := []struct {
		name      string
		state     Availability
		wantCalls int
		wantStat  types.IndexStatus
		wantErr   bool
	}{
		{"available maps to READY", AvailabilityAvailable, 1, types.IndexStatusReady, false},
		{"unavailable maps to FAILED", AvailabilityUnavailable, 1, types.IndexStatusFailed, false},
		{"degraded leaves the version alone", AvailabilityDegraded, 0, 0, false},
		{"unknown availability is rejected", Availability(42), 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := &stubMeta{}
			cp := NewLocalControlPlane(meta)

			err := cp.ReportAvailability(context.Background(), "kb-1", 5, tc.state)
			if tc.wantErr != (err != nil) {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if len(meta.statusCalls) != tc.wantCalls {
				t.Fatalf("status proposals = %+v, want %d", meta.statusCalls, tc.wantCalls)
			}
			if tc.wantCalls == 1 && meta.statusCalls[0].status != tc.wantStat {
				t.Fatalf("status = %v, want %v", meta.statusCalls[0].status, tc.wantStat)
			}
		})
	}
}

func TestLocalControlPlane_ReportEpoch_PromotesPendingOnly(t *testing.T) {
	meta := &stubMeta{versions: map[string][]types.VersionMeta{
		"kb-1": {
			{VersionID: 1, KBID: "kb-1", IndexStatus: types.IndexStatusReady},
			{VersionID: 2, KBID: "kb-1", IndexStatus: types.IndexStatusPending},
			{VersionID: 3, KBID: "kb-1", IndexStatus: types.IndexStatusPending},
		},
	}}
	cp := NewLocalControlPlane(meta)

	// v1 is durable but already READY (no-op); v2 is durable and PENDING
	// (promote); v3 is not reported at all (untouched).
	ready := map[string][]int64{"kb-1": {1, 2}}
	if err := cp.ReportEpoch(context.Background(), 1, map[string]int64{"kb-1": 2}, ready); err != nil {
		t.Fatalf("ReportEpoch: %v", err)
	}
	if len(meta.statusCalls) != 1 {
		t.Fatalf("status proposals = %+v, want exactly one (v2)", meta.statusCalls)
	}
	if got := meta.statusCalls[0]; got.versionID != 2 || got.status != types.IndexStatusReady {
		t.Fatalf("proposal = %+v, want READY for v2", got)
	}
}

func TestLocalControlPlane_ReportEpoch_ListErrorPropagates(t *testing.T) {
	cp := NewLocalControlPlane(&stubMeta{listErr: errors.New("boom")})

	if err := cp.ReportEpoch(context.Background(), 1, nil, map[string][]int64{"kb-1": {1}}); err == nil {
		t.Fatal("ReportEpoch must surface a metadata read failure")
	}
}

// --- LocalDataPlane: EnsureIndex -------------------------------------------

// A resolve that knows no source must NOT read as success: nothing was fetched, and
// reporting that as "done" is indistinguishable from a finished pull. It is what let
// a storage replica that had been away for 55 versions claim a catch-up while its
// cursor and artifact count stood still (lag_catchup logs "caught up with the chain
// tail" on a nil result). The error must stay retryable, because another candidate —
// or the same one a moment later, once the announcement arrives — may have the data.
func TestLocalDataPlane_EnsureIndex_NoSourceIsNotSuccess(t *testing.T) {
	puller := &stubPuller{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		Puller:       puller,
		Verify:       func(context.Context, string, int64) bool { return false },
		Resolve:      func(context.Context, string, int64) (string, bool, error) { return "", false, nil },
	})

	err := dp.EnsureIndex(context.Background(), "kb-1", 4)
	if err == nil {
		t.Fatal("EnsureIndex reported success with no data source: nothing was fetched, so no caller can tell this from a completed pull")
	}
	if !errors.Is(err, stratumerrors.ErrIndexNotReady) {
		t.Fatalf("error = %v, want it to wrap ErrIndexNotReady — that sentinel is what keeps it retryable", err)
	}
	if puller.calls != 0 {
		t.Fatalf("puller calls = %d, want 0 (there is nothing to pull from)", puller.calls)
	}
}

func TestLocalDataPlane_EnsureIndex_PullsUntilVerified(t *testing.T) {
	puller := &stubPuller{}
	verified := 0
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		Puller:       puller,
		Verify: func(context.Context, string, int64) bool {
			verified++
			return verified >= 2
		},
		Resolve: func(context.Context, string, int64) (string, bool, error) { return "peer:7000", true, nil },
	})

	if err := dp.EnsureIndex(context.Background(), "kb-1", 4); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if puller.calls != 2 {
		t.Fatalf("puller calls = %d, want 2 (retry until the committed digest matches)", puller.calls)
	}
}

// The pull loop's bound is about PROGRESS, not about a clock.
//
// This is the shape a large version arrives in. A 20,000-document version (~56 MB)
// does not cross the wire, land and get applied inside any fixed window that a small
// version also fits in — so a wall-clock bound over the whole loop times out on every
// attempt and the retry uses the same window, which means the loop cannot converge at
// any number of attempts. Measured on the 3+3 cluster: 64 rounds of
// `sync: recv SyncEntry: ... DeadlineExceeded` followed by "version data did not
// converge within 30s", for a transfer that completes fine once given room.
//
// Here two attempts complete without verifying and only the third verifies, with an
// idle budget far shorter than the loop's total time: completed transfers are what
// keeps the loop alive.
func TestLocalDataPlane_EnsureIndex_ProgressKeepsTheLoopAlive(t *testing.T) {
	puller := &sequencedPuller{delay: 30 * time.Millisecond}
	verified := 0
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:    &stubIndexStore{},
		Puller:          puller,
		PullIdleTimeout: 20 * time.Millisecond, // far shorter than the whole loop takes
		Verify: func(context.Context, string, int64) bool {
			verified++
			return verified >= 3
		},
		Resolve: func(context.Context, string, int64) (string, bool, error) { return "peer:7000", true, nil },
	})

	if err := dp.EnsureIndex(context.Background(), "kb-1", 4); err != nil {
		t.Fatalf("EnsureIndex: %v — a completed transfer is progress, and progress must not run out of idle budget", err)
	}
	if puller.calls != 3 {
		t.Fatalf("puller calls = %d, want 3", puller.calls)
	}
}

// The other half: attempts that never complete ARE the failure mode, and the idle
// bound has to end them — quickly, and with a diagnosis. The old message said only
// "did not converge within 30s", which left an operator guessing between an
// unreachable peer, a source that lost the version, and a transfer that was simply
// too big.
func TestLocalDataPlane_EnsureIndex_FailureWithoutProgressReportsDiagnosis(t *testing.T) {
	puller := &sequencedPuller{err: errors.New("peer went away")}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:    &stubIndexStore{},
		Puller:          puller,
		PullIdleTimeout: 40 * time.Millisecond,
		Verify:          func(context.Context, string, int64) bool { return false },
		Resolve:         func(context.Context, string, int64) (string, bool, error) { return "peer:7000", true, nil },
	})

	start := time.Now()
	err := dp.EnsureIndex(context.Background(), "kb-1", 7)
	if err == nil {
		t.Fatal("want an error: not one attempt ever completed")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the idle bound did not end the loop in time (took %v)", elapsed)
	}
	for _, want := range []string{"attempts=", "local cursor=", "target=7", "peer went away"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not carry %q", err, want)
		}
	}
	if puller.calls < 2 {
		t.Errorf("puller calls = %d, want at least 2: the loop retries before it gives up", puller.calls)
	}
}

// And the absolute ceiling, so a loop that keeps making a little progress forever
// still ends.
func TestLocalDataPlane_EnsureIndex_AbsoluteCeilingEndsAProgressingLoop(t *testing.T) {
	puller := &sequencedPuller{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:    &stubIndexStore{},
		Puller:          puller,
		PullIdleTimeout: time.Second,
		PullMaxDuration: 80 * time.Millisecond,
		Verify:          func(context.Context, string, int64) bool { return false }, // never satisfied
		Resolve:         func(context.Context, string, int64) (string, bool, error) { return "peer:7000", true, nil },
	})

	err := dp.EnsureIndex(context.Background(), "kb-1", 9)
	if err == nil {
		t.Fatal("want an error: the loop must end even when every attempt completes")
	}
	if !strings.Contains(err.Error(), "within") {
		t.Errorf("error = %v, want the absolute-ceiling wording", err)
	}
}

func TestLocalDataPlane_EnsureIndex_ResolveErrorPropagates(t *testing.T) {
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		Puller:       &stubPuller{},
		Verify:       func(context.Context, string, int64) bool { return false },
		Resolve: func(context.Context, string, int64) (string, bool, error) {
			return "", false, errors.New("no cluster status")
		},
	})

	if err := dp.EnsureIndex(context.Background(), "kb-1", 4); err == nil {
		t.Fatal("EnsureIndex must surface a resolve failure")
	}
}

// --- LocalDataPlane: ReconcileIndexes / EnforceRetention --------------------

func TestLocalDataPlane_ReconcileIndexes_DecisionTable(t *testing.T) {
	meta := &stubMeta{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
		versions: map[string][]types.VersionMeta{
			"kb-1": {
				{VersionID: 1, KBID: "kb-1", IndexStatus: types.IndexStatusReady},   // on disk -> durable
				{VersionID: 2, KBID: "kb-1", IndexStatus: types.IndexStatusPending}, // on disk -> durable
				{VersionID: 3, KBID: "kb-1", IndexStatus: types.IndexStatusPending}, // missing -> rebuild
				{VersionID: 4, KBID: "kb-1", IndexStatus: types.IndexStatusFailed},  // untouched
				// Terminal verdict: nothing re-triggers it and its data may
				// already be gone (§10.1/§10.6), so a missing index must NOT
				// be rebuilt.
				{VersionID: 5, KBID: "kb-1", IndexStatus: types.IndexStatusFailedPermanent}, // untouched
			},
		},
	}
	store := &stubIndexStore{exists: map[int64]bool{1: true, 2: true}}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: store})

	durable, err := dp.ReconcileIndexes(context.Background(), meta, 0)
	if err != nil {
		t.Fatalf("ReconcileIndexes: %v", err)
	}
	if len(durable) != 2 || durable[0].VersionID != 1 || durable[1].VersionID != 2 {
		t.Fatalf("durable = %+v, want v1 and v2", durable)
	}
	if len(store.triggered) != 1 || store.triggered[0] != 3 {
		t.Errorf("triggered = %v, want only v3 (PENDING + missing)", store.triggered)
	}
}

func TestLocalDataPlane_ReconcileIndexes_SkipsRetentionDropped(t *testing.T) {
	meta := &stubMeta{
		// v3 is the active version, and that detail is now load-bearing: inside the
		// window, "missing" alone no longer earns a rebuild — a lazy path exists, so
		// an absent artifact costs only a slow first query. The one artifact
		// reconcile rebuilds here is the one being SERVED. The full policy is pinned
		// by TestReconcileIndexes_RebuildsOnlyPendingAndActive.
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1", ActiveVersionID: 3}},
		versions: map[string][]types.VersionMeta{
			"kb-1": {
				{VersionID: 1, KBID: "kb-1", IndexStatus: types.IndexStatusReady}, // outside the window, missing
				{VersionID: 2, KBID: "kb-1", IndexStatus: types.IndexStatusReady}, // inside the window, on disk
				{VersionID: 3, KBID: "kb-1", IndexStatus: types.IndexStatusReady}, // inside the window, missing, ACTIVE
			},
		},
	}
	store := &stubIndexStore{exists: map[int64]bool{2: true}}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: store})

	durable, err := dp.ReconcileIndexes(context.Background(), meta, 2) // keep the newest 2
	if err != nil {
		t.Fatalf("ReconcileIndexes: %v", err)
	}
	if len(durable) != 1 || durable[0].VersionID != 2 {
		t.Errorf("durable = %+v, want only v2", durable)
	}
	if len(store.triggered) != 1 || store.triggered[0] != 3 {
		t.Errorf("triggered = %v, want only v3 (v1 is retention-dropped, not rebuilt)", store.triggered)
	}
}

func TestLocalDataPlane_EnforceRetention_ShieldsActiveVersion(t *testing.T) {
	meta := &stubMeta{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1", ActiveVersionID: 9}}}
	store := &stubIndexStore{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: store})

	if err := dp.EnforceRetention(context.Background(), meta); err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if len(store.retention) != 1 {
		t.Fatalf("retention calls = %+v, want exactly one", store.retention)
	}
	call := store.retention[0]
	if call.kbID != "kb-1" {
		t.Errorf("kbID = %s, want kb-1", call.kbID)
	}
	if len(call.protectedIDs) != 1 || call.protectedIDs[0] != 9 {
		t.Errorf("protectedIDs = %v, want [9] (the active version)", call.protectedIDs)
	}
}

func TestLocalDataPlane_EnforceRetention_ListErrorPropagates(t *testing.T) {
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: &stubIndexStore{}})

	if err := dp.EnforceRetention(context.Background(), &stubMeta{listErr: errors.New("boom")}); err == nil {
		t.Fatal("EnforceRetention must surface a metadata read failure")
	}
}

// --- LocalDataPlane: the rest of the contract ------------------------------

func TestLocalDataPlane_Search_Delegates(t *testing.T) {
	store := &stubIndexStore{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: store})

	if _, err := dp.Search(context.Background(), "kb-1", 1, []float32{1, 2}, 5); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if !store.searched {
		t.Error("Search must reach the index store")
	}
}

// WriteVersionData owns the whole storage-layer transaction: BEGIN (with the
// replay input) before the writes, COMMIT after them (Stratum_设计文档v13.md
// §7.12).
func TestLocalDataPlane_WriteVersionData_FramesTheTransaction(t *testing.T) {
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
	})

	changes := []types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc-1"}}
	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 3, changes); err != nil {
		t.Fatalf("WriteVersionData: %v", err)
	}
	want := []string{"begin:kb-1:3", "write:7", "commit:7"}
	if !reflect.DeepEqual(tr.trace, want) {
		t.Errorf("transaction order = %v, want %v", tr.trace, want)
	}
}

// A write failure inside the transaction must surface (and must not reach
// COMMIT — the version simply stays unreplayed until recovery).
func TestLocalDataPlane_WriteVersionData_WriteFailureSkipsCommit(t *testing.T) {
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr, err: errors.New("disk full")},
	})

	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 3, nil); err == nil {
		t.Fatal("a failing storage write must surface")
	}
	for _, step := range tr.trace {
		if step == "commit:7" {
			t.Fatalf("transaction committed despite a write failure: %v", tr.trace)
		}
	}
}

func TestLocalDataPlane_WriteVersionData_RequiresConfiguration(t *testing.T) {
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: &stubIndexStore{}})

	if err := dp.WriteVersionData(context.Background(), "kb-1", 1, 0, nil); err == nil {
		t.Error("an unconfigured write transaction must be rejected, not silently skipped")
	}
}

func TestLocalDataPlane_DropVersionDataAndDurabilityPolicy(t *testing.T) {
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: &stubIndexStore{}})
	ctx := context.Background()

	if err := dp.DropVersionData(ctx, "kb-1", 1); err == nil {
		t.Error("DropVersionData is not wired yet (storage-cluster work)")
	}
	if err := dp.SetDurabilityPolicy(ctx, "kb-1", DurabilityPolicy{Replicas: 3}); err != nil {
		t.Errorf("SetDurabilityPolicy is accepted (no-op until stage ④): %v", err)
	}
}

func TestAvailabilityString(t *testing.T) {
	cases := map[Availability]string{
		AvailabilityAvailable:   "AVAILABLE",
		AvailabilityDegraded:    "DEGRADED",
		AvailabilityUnavailable: "UNAVAILABLE",
		Availability(99):        "UNKNOWN",
	}
	for state, want := range cases {
		if got := state.String(); got != want {
			t.Errorf("Availability(%d).String() = %s, want %s", int(state), got, want)
		}
	}
}

// TriggerBuildBackfill satisfies index.IndexManager. Reconcile schedules through
// this one; for this implementation it is the same build as TriggerBuild.
func (s *stubIndexStore) TriggerBuildBackfill(ctx context.Context, kbID string, versionID int64) error {
	return s.TriggerBuild(ctx, kbID, versionID)
}
