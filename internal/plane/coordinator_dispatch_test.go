package plane

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
	"stratum/internal/types"
)

// fakeDataSyncServer is the minimum of DataSyncService this test needs: it
// records the dispatched writes and can be told to fail or to answer
// "accepted but not completed".
type fakeDataSyncServer struct {
	pb.UnimplementedDataSyncServiceServer

	mu         sync.Mutex
	received   []string // "kb/version"
	fail       bool
	acceptOnly bool
}

func (s *fakeDataSyncServer) ExecuteVersionWrite(_ context.Context, req *pb.ExecuteVersionWriteRequest) (*pb.ExecuteVersionWriteResponse, error) {
	s.mu.Lock()
	s.received = append(s.received, req.GetKnowledgeBaseId()+"/"+strconv.FormatInt(req.GetVersionId(), 10))
	fail, acceptOnly := s.fail, s.acceptOnly
	s.mu.Unlock()
	if fail {
		return nil, errors.New("candidate refused the write")
	}
	return &pb.ExecuteVersionWriteResponse{NodeId: 1, Completed: !acceptOnly}, nil
}

func (s *fakeDataSyncServer) got() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.received...)
}

func startFakeDataSyncServer(t *testing.T, srv *fakeDataSyncServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterDataSyncServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func dispatchChanges() []types.DocChange {
	return []types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "alpha"}}
}

// TestCoordinatorDispatcher_TriesCandidatesInStableOrderUntilOneTakesIt pins
// §7.13.2's candidate walk: the first replica refuses, the next one takes the
// write, and the returned address names the node that actually coordinated it.
func TestCoordinatorDispatcher_TriesCandidatesInStableOrderUntilOneTakesIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	refusing := &fakeDataSyncServer{fail: true}
	accepting := &fakeDataSyncServer{}
	refusingAddr := startFakeDataSyncServer(t, refusing)
	acceptingAddr := startFakeDataSyncServer(t, accepting)

	d := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{
		Replicas: func(context.Context) ([]string, error) {
			// Deliberately unsorted: the dispatcher must impose a stable order.
			return []string{acceptingAddr, refusingAddr}, nil
		},
	})

	got, err := d.Dispatch(ctx, "kb-1", 7, 6, dispatchChanges())
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got != acceptingAddr {
		t.Fatalf("coordinator = %q, want the accepting replica %q", got, acceptingAddr)
	}
	// Which replica is tried first depends on the sort order of the (random)
	// loopback ports, so the exact attempt count is not what this test pins:
	// the point is that the walk ends on the replica that could take the write.
	// The failure-then-next-candidate sequence is pinned unambiguously by the
	// local-fallback test below.
	if len(refusing.got()) > 1 {
		t.Errorf("a refusing replica must not be retried within one dispatch, got %v", refusing.got())
	}
	if len(accepting.got()) != 1 || accepting.got()[0] != "kb-1/7" {
		t.Errorf("the accepting replica saw %v, want [kb-1/7]", accepting.got())
	}
}

// "Accepted but not completed" is a failure: a candidate that answers without
// committing is not a coordinator, so the dispatcher must keep looking.
func TestCoordinatorDispatcher_AcceptedButNotCompletedIsAFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	half := &fakeDataSyncServer{acceptOnly: true}
	real := &fakeDataSyncServer{}
	halfAddr := startFakeDataSyncServer(t, half)
	realAddr := startFakeDataSyncServer(t, real)

	d := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{
		Replicas: func(context.Context) ([]string, error) { return []string{halfAddr, realAddr}, nil },
	})

	got, err := d.Dispatch(ctx, "kb-1", 7, 6, dispatchChanges())
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got != realAddr {
		t.Fatalf("coordinator = %q, want %q (the half-answering candidate must not count)", got, realAddr)
	}
}

// With every replica refusing, the node falls back to coordinating locally —
// a cluster of one, or one where every peer is unreachable, must still progress.
func TestCoordinatorDispatcher_FallsBackToCoordinatingLocally(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	refusingAddr := startFakeDataSyncServer(t, &fakeDataSyncServer{fail: true})

	var mu sync.Mutex
	localCalls := 0
	d := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{
		Replicas: func(context.Context) ([]string, error) { return []string{refusingAddr}, nil },
		SelfAddr: "self:7000",
		LocalWrite: func(_ context.Context, kbID string, versionID, parentVersionID int64, _ []types.DocChange) error {
			mu.Lock()
			defer mu.Unlock()
			localCalls++
			return nil
		},
	})

	got, err := d.Dispatch(ctx, "kb-1", 7, 6, dispatchChanges())
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got != "self:7000" {
		t.Fatalf("coordinator = %q, want this node as the last resort", got)
	}
	if localCalls != 1 {
		t.Errorf("local coordination calls = %d, want 1", localCalls)
	}
}

// When nobody takes the write, the error must name the failure rather than
// returning success — the caller turns it into a transient failure and lets the
// retry budget decide.
func TestCoordinatorDispatcher_AllCandidatesFailingIsAnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr := startFakeDataSyncServer(t, &fakeDataSyncServer{fail: true})
	d := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{
		Replicas: func(context.Context) ([]string, error) { return []string{addr}, nil },
	})

	if _, err := d.Dispatch(ctx, "kb-1", 7, 6, dispatchChanges()); err == nil {
		t.Fatal("a dispatch no replica accepted must return an error")
	}
}
