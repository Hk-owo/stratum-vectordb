package plane

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	stratuminternalsync "stratum/internal/sync"
	"stratum/internal/types"
)

// stubIndexInstaller records what a fetched artifact lands as. Installation is
// the other half of a fetch: bytes that arrive and are not installed leave the
// version exactly as cold as before.
type stubIndexInstaller struct {
	mu        sync.Mutex
	installed []string
	err       error
}

func (s *stubIndexInstaller) InstallIndex(_ context.Context, kbID string, versionID int64, indexData, sidecarData []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.installed = append(s.installed, fmt.Sprintf("%s:%d:%d", kbID, versionID, len(indexData)))
	return nil
}

func (s *stubIndexInstaller) installedEntries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.installed...)
}

// newFetchPlane builds a plane with just the three pieces an on-demand fetch
// needs, so a test failure can only come from the fetch logic itself.
// stubIndexFetcher answers FetchIndex for the peers named in fetchable; every
// other peer answers "not exported", which is the answer the caller is required
// to move on from rather than retry. fetches records who was ASKED, so a test can
// tell "asked and refused" from "never asked".
type stubIndexFetcher struct {
	mu        sync.Mutex
	fetchable map[string][]byte
	fetches   []string
}

func (s *stubIndexFetcher) FetchIndex(_ context.Context, sourceAddr, kbID string, versionID int64) ([]byte, []byte, error) {
	s.mu.Lock()
	s.fetches = append(s.fetches, sourceAddr)
	data, ok := s.fetchable[sourceAddr]
	s.mu.Unlock()
	if !ok {
		return nil, nil, fmt.Errorf("stub: %s v%d: %w", kbID, versionID, stratuminternalsync.ErrIndexNotExported)
	}
	return data, []byte("sidecar"), nil
}

func (s *stubIndexFetcher) fetchedTargets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.fetches...)
}

var _ IndexFetcher = (*stubIndexFetcher)(nil)

func newFetchPlane(fetcher IndexFetcher, installer IndexInstaller, peers ...string) *LocalDataPlane {
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexFetcher:   fetcher,
		IndexInstaller: installer,
		ResolveReplicas: func(context.Context) ([]string, error) {
			return append([]string(nil), peers...), nil
		},
		Logger: zap.NewNop(),
	})
}

// TestFetchIndexFromPeers_TakesTheFirstPeerThatHasIt: the fetch walks the peers
// and installs from the first one that answers with an artifact — including when
// an earlier peer answered "I do not have it", which is a reason to move on and
// not a failure.
func TestFetchIndexFromPeers_TakesTheFirstPeerThatHasIt(t *testing.T) {
	fetcher := &stubIndexFetcher{fetchable: map[string][]byte{"peer-b": []byte("artifact-bytes")}}
	installer := &stubIndexInstaller{}
	dp := newFetchPlane(fetcher, installer, "peer-a", "peer-b", "peer-c")

	fetched, err := dp.FetchIndexFromPeers(context.Background(), "kb-1", 7)
	if err != nil {
		t.Fatalf("FetchIndexFromPeers: %v", err)
	}
	if !fetched {
		t.Fatal("fetched = false, want true: peer-b holds the artifact")
	}

	if got := installer.installedEntries(); len(got) != 1 || got[0] != "kb-1:7:14" {
		t.Fatalf("installed = %v, want exactly one entry with the fetched payload", got)
	}
	// peer-a was asked and refused; peer-c was never reached, because the fetch
	// stops at the first peer that had it.
	if got := fetcher.fetchedTargets(); len(got) != 2 || got[0] != "peer-a" || got[1] != "peer-b" {
		t.Fatalf("asked %v, want [peer-a peer-b] — stop at the first holder", got)
	}
}

// TestFetchIndexFromPeers_NobodyHasItIsNotAnError: "no peer holds this" is the
// ordinary answer for the first query after a version is created. Reporting it
// as an error would send the caller down a failure path where the right move is
// simply to build.
func TestFetchIndexFromPeers_NobodyHasItIsNotAnError(t *testing.T) {
	fetcher := &stubIndexFetcher{}
	installer := &stubIndexInstaller{}
	dp := newFetchPlane(fetcher, installer, "peer-a", "peer-b")

	fetched, err := dp.FetchIndexFromPeers(context.Background(), "kb-1", 7)
	if err != nil {
		t.Fatalf("FetchIndexFromPeers = %v, want a nil error: nobody holding it is ordinary", err)
	}
	if fetched {
		t.Fatal("fetched = true, want false")
	}
	if got := installer.installedEntries(); len(got) != 0 {
		t.Fatalf("installed = %v, want nothing", got)
	}
	if got := fetcher.fetchedTargets(); len(got) != 2 {
		t.Fatalf("asked %v, want both peers tried", got)
	}
}

// TestFetchIndexFromPeers_SkipsAPeerThatFailsToAnswer: a peer that is
// unreachable or errors is not evidence about the others, so the walk continues
// — and the last error is still reported so the caller can log it.
func TestFetchIndexFromPeers_SkipsAPeerThatFailsToAnswer(t *testing.T) {
	fetcher := &stubIndexFetcher{fetchable: map[string][]byte{"peer-b": []byte("bytes")}}
	installer := &stubIndexInstaller{}

	// peer-a errors at the transport level rather than answering "not exported".
	transportFail := &failingFetchShipper{inner: fetcher, failOn: map[string]bool{"peer-a": true}}
	dp := newFetchPlane(transportFail, installer, "peer-a", "peer-b")

	fetched, err := dp.FetchIndexFromPeers(context.Background(), "kb-1", 7)
	if err != nil {
		t.Fatalf("FetchIndexFromPeers: %v — a failing peer must not stop the walk", err)
	}
	if !fetched {
		t.Fatal("fetched = false, want true: peer-b holds it even though peer-a failed")
	}
}

// TestFetchIndexFromPeers_NotWired: with no installer (or no fetcher) there is
// nowhere for an artifact to land, so the fetch reports "did nothing" rather
// than an error — the caller's fallback (build) is correct in that deployment.
func TestFetchIndexFromPeers_NotWired(t *testing.T) {
	fetcher := &stubIndexFetcher{fetchable: map[string][]byte{"peer-a": []byte("bytes")}}

	for _, tc := range []struct {
		name      string
		fetcher   IndexFetcher
		installer IndexInstaller
	}{
		{"no installer", fetcher, nil},
		{"no fetcher", nil, &stubIndexInstaller{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dp := newFetchPlane(tc.fetcher, tc.installer, "peer-a")
			fetched, err := dp.FetchIndexFromPeers(context.Background(), "kb-1", 7)
			if err != nil || fetched {
				t.Fatalf("FetchIndexFromPeers = (%v, %v), want (false, nil)", fetched, err)
			}
		})
	}
}

// TestFetchIndexFromPeers_InstallFailureIsReported: bytes that arrive but do not
// land are worth an error — the caller may still build, but it should be able to
// say why the fetch did not help.
func TestFetchIndexFromPeers_InstallFailureIsReported(t *testing.T) {
	fetcher := &stubIndexFetcher{fetchable: map[string][]byte{"peer-a": []byte("bytes")}}
	installer := &stubIndexInstaller{err: errors.New("disk full")}
	dp := newFetchPlane(fetcher, installer, "peer-a")

	fetched, err := dp.FetchIndexFromPeers(context.Background(), "kb-1", 7)
	if fetched {
		t.Fatal("fetched = true although the install failed")
	}
	if err == nil {
		t.Fatal("err = nil, want the install failure reported")
	}
}

// failingFetchShipper wraps a fetcher and makes FetchIndex fail at the transport
// level for the named peers — the case that must NOT be confused with "this peer
// answered that it holds nothing".
type failingFetchShipper struct {
	inner  *stubIndexFetcher
	failOn map[string]bool
}

func (f *failingFetchShipper) FetchIndex(ctx context.Context, sourceAddr, kbID string, versionID int64) ([]byte, []byte, error) {
	if f.failOn[sourceAddr] {
		return nil, nil, errors.New("connection refused")
	}
	return f.inner.FetchIndex(ctx, sourceAddr, kbID, versionID)
}

// TestEnsureIndex_FetchesTheArtifactBeforeBuildingIt pins why lag catch-up
// stopped building.
//
// A node can be behind on its contiguous cursor for a reason that has nothing to
// do with the version in question — a gap lower down the chain — and the catch-up
// then reaches EnsureIndex for a version whose data and index are already here.
// The order is: DATA first, then the artifact, then a build if nobody has one.
//
// It used to be artifact-first, which looked cheaper and was wrong: an artifact
// existing on some peer says nothing about THIS node's data. Taking it and
// returning early left the version's records unfetched — measured on the 3+3
// cluster, a catch-up logged "caught up with the chain tail" while the node's
// cursor stayed 55 versions behind.
func TestEnsureIndex_TakesAPeersArtifactOnceTheDataIsHere(t *testing.T) {
	fetcher := &stubIndexFetcher{fetchable: map[string][]byte{"peer-a": []byte("artifact-bytes")}}
	installer := &stubIndexInstaller{}
	puller := &recordingPuller{}
	store := &stubIndexStore{}
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexFetcher:   fetcher,
		IndexInstaller: installer,
		IndexManager:   store,
		WAL:            &stubWAL{t: tr},
		Executor:       &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
		Puller:         puller,
		Verify:         func(context.Context, string, int64) bool { return true },
		ResolveReplicas: func(context.Context) ([]string, error) {
			return []string{"peer-a", "peer-b"}, nil
		},
		Resolve: func(context.Context, string, int64) (string, bool, error) { return "peer-a", true, nil },
		Logger:  zap.NewNop(),
	})

	if err := dp.EnsureIndex(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if len(puller.pulled) != 1 || puller.pulled[0] != 7 {
		t.Fatalf("pulled = %v, want [7]: the version's DATA has to be fetched here", puller.pulled)
	}
	if got := installer.installedEntries(); len(got) != 1 {
		t.Fatalf("installed = %v, want the artifact installed from the peer", got)
	}
	if len(store.triggered) != 0 {
		t.Fatalf("TriggerBuild calls = %v, want none: a peer had the artifact", store.triggered)
	}
}

// TestEnsureIndex_BuildsWhenNobodyHasTheArtifact is the other half, and the one
// that has to keep working: a node asked for a version nobody has an artifact for
// builds it itself. That is the ordinary path for the first request after a
// version is created, and the fallback that keeps a version servable when no peer
// ever built one.
func TestEnsureIndex_BuildsWhenNobodyHasTheArtifact(t *testing.T) {
	fetcher := &stubIndexFetcher{} // nobody holds anything
	installer := &stubIndexInstaller{}
	puller := &recordingPuller{}
	// The rest of the plane's dependencies are the ones newCursorPlane supplies:
	// this case has to run PAST the fetch, all the way into the ordinary pull.
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexFetcher:   fetcher,
		IndexInstaller: installer,
		IndexManager:   &stubIndexStore{},
		WAL:            &stubWAL{t: tr},
		Executor:       &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
		Puller:         puller,
		Verify:         func(context.Context, string, int64) bool { return true },
		ResolveReplicas: func(context.Context) ([]string, error) {
			return []string{"peer-a", "peer-b"}, nil
		},
		Resolve: func(context.Context, string, int64) (string, bool, error) { return "peer-a", true, nil },
		Logger:  zap.NewNop(),
	})

	if err := dp.EnsureIndex(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if len(puller.pulled) != 1 || puller.pulled[0] != 7 {
		t.Fatalf("pulled = %v, want [7]: with no artifact anywhere the node has to pull and build", puller.pulled)
	}
	if got := installer.installedEntries(); len(got) != 0 {
		t.Fatalf("installed = %v, want nothing", got)
	}
}

// slowIndexFetcher blocks inside FetchIndex until released, so a test can hold a
// fetch open and watch what the second caller does.
type slowIndexFetcher struct {
	inner   *stubIndexFetcher
	entered chan struct{}
	release chan struct{}

	mu    sync.Mutex
	calls int
}

func (s *slowIndexFetcher) FetchIndex(ctx context.Context, sourceAddr, kbID string, versionID int64) ([]byte, []byte, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	return s.inner.FetchIndex(ctx, sourceAddr, kbID, versionID)
}

func (s *slowIndexFetcher) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestFetchIndexFromPeers_SharesOneFetchAcrossConcurrentCallers pins the
// deduplication half of H9 of docs/code-review-2026-10-18.md.
//
// The query path asks for the artifact on every PENDING query and the catch-up loop
// on every round, so the same version is asked about repeatedly and concurrently.
// Each ask used to download the whole artifact and rewrite the file on disk
// (InstallIndex serialises per version, so the rewrites queued up rather than
// overlapping).
func TestFetchIndexFromPeers_SharesOneFetchAcrossConcurrentCallers(t *testing.T) {
	fetcher := &slowIndexFetcher{
		inner:   &stubIndexFetcher{fetchable: map[string][]byte{"peer-a": []byte("artifact")}},
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	dp := newFetchPlane(fetcher, &stubIndexInstaller{}, "peer-a")

	type result struct {
		installed bool
		err       error
	}

	// First caller: reaches the download and stays there.
	first := make(chan result, 1)
	go func() {
		installed, err := dp.FetchIndexFromPeers(context.Background(), "kb-1", 7)
		first <- result{installed, err}
	}()
	<-fetcher.entered

	// Second caller, while the first download is still open: it must attach to that
	// download instead of starting another.
	second := make(chan result, 1)
	go func() {
		installed, err := dp.FetchIndexFromPeers(context.Background(), "kb-1", 7)
		second <- result{installed, err}
	}()
	time.Sleep(100 * time.Millisecond)
	if got := fetcher.callCount(); got != 1 {
		t.Fatalf("FetchIndex was called %d times while one download was open, want 1 (the second caller must share it)", got)
	}

	close(fetcher.release)
	for i, ch := range []chan result{first, second} {
		r := <-ch
		if r.err != nil {
			t.Fatalf("caller %d: %v", i, r.err)
		}
		if !r.installed {
			t.Errorf("caller %d did not see the artifact as installed", i)
		}
	}
	if got := fetcher.callCount(); got != 1 {
		t.Errorf("FetchIndex ran %d times for one version, want 1", got)
	}
}

// existingArtifactIndexStore answers IndexExists with true and TriggerBuild with a
// tattle, so a test can tell "asked the disk" from "asked the cluster".
type existingArtifactIndexStore struct {
	mu          sync.Mutex
	existsCalls int
	builds      int
}

func (s *existingArtifactIndexStore) Search(context.Context, string, int64, []float32, int) ([]types.SearchResult, error) {
	return nil, nil
}

// TrySearch reports "cannot answer right now": an empty answer would read as "no
// matches" to the query path (§9.1 risk 1).
func (s *existingArtifactIndexStore) TrySearch(context.Context, string, int64, []float32, int) ([]types.SearchResult, bool, error) {
	return nil, false, nil
}

func (s *existingArtifactIndexStore) TriggerBuild(context.Context, string, int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.builds++
	return nil
}

func (s *existingArtifactIndexStore) TriggerBuildBackfill(context.Context, string, int64) error {
	return nil
}

func (s *existingArtifactIndexStore) IndexExists(context.Context, string, int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.existsCalls++
	return true, nil
}

func (s *existingArtifactIndexStore) EnforceDiskRetention(context.Context, string, []int64) error {
	return nil
}

// TestEnsureOwnIndex_AsksTheDiskBeforeTheCluster pins the ordering half of H9: a
// version whose artifact is already here must not be re-downloaded on every
// EnsureIndex.
func TestEnsureOwnIndex_AsksTheDiskBeforeTheCluster(t *testing.T) {
	fetcher := &stubIndexFetcher{fetchable: map[string][]byte{"peer-a": []byte("artifact")}}
	store := &existingArtifactIndexStore{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:   store,
		IndexFetcher:   fetcher,
		IndexInstaller: &stubIndexInstaller{},
		ResolveReplicas: func(context.Context) ([]string, error) {
			return []string{"peer-a"}, nil
		},
		Logger: zap.NewNop(),
	})

	if err := dp.ensureOwnIndex(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("ensureOwnIndex: %v", err)
	}
	if got := fetcher.fetchedTargets(); len(got) != 0 {
		t.Errorf("the artifact was fetched from %v even though it is already on this node's disk", got)
	}
	store.mu.Lock()
	exists := store.existsCalls
	builds := store.builds
	store.mu.Unlock()
	if exists == 0 {
		t.Error("IndexExists was never consulted")
	}
	if builds != 0 {
		t.Errorf("TriggerBuild ran %d times for a version that is already present", builds)
	}
}
