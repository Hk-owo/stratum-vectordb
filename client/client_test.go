package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "stratum/api/proto/stratum"
)

// fakeCluster is the smallest useful stand-in for a cluster: it records what the
// client sent and answers the calls this client makes. CreateVersion is
// idempotent by key, exactly as the server's is — that is what makes the re-send
// assertions mean something.
type fakeCluster struct {
	pb.UnimplementedKnowledgeBaseServiceServer

	mu           sync.Mutex
	versionCalls []*pb.CreateVersionRequest
	discardCalls []*pb.DiscardVersionRequest
	created      map[string]int64
	nextVersion  int64
	createErr    error
	awaitStage   string
	dataMissing  bool
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{created: map[string]int64{}, nextVersion: 1, awaitStage: "DATA_PENDING"}
}

func (f *fakeCluster) CreateKnowledgeBase(context.Context, *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
	return &pb.CreateKnowledgeBaseResponse{KnowledgeBaseId: "kb-1"}, nil
}

func (f *fakeCluster) CreateVersion(_ context.Context, req *pb.CreateVersionRequest) (*pb.CreateVersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.versionCalls = append(f.versionCalls, req)
	if f.createErr != nil {
		return nil, f.createErr
	}
	if id, ok := f.created[req.GetClientRequestId()]; ok {
		return &pb.CreateVersionResponse{VersionId: id, ClientRequestId: req.GetClientRequestId()}, nil
	}
	id := f.nextVersion
	f.nextVersion++
	f.created[req.GetClientRequestId()] = id
	return &pb.CreateVersionResponse{VersionId: id, ClientRequestId: req.GetClientRequestId()}, nil
}

func (f *fakeCluster) AwaitVersion(_ context.Context, req *pb.AwaitVersionRequest) (*pb.AwaitVersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &pb.AwaitVersionResponse{
		Version:     &pb.VersionInfo{VersionId: req.GetVersionId()},
		Stage:       f.awaitStage,
		DataMissing: f.dataMissing,
	}, nil
}

func (f *fakeCluster) DiscardVersion(_ context.Context, req *pb.DiscardVersionRequest) (*pb.DiscardVersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.discardCalls = append(f.discardCalls, req)
	return &pb.DiscardVersionResponse{Discarded: true}, nil
}

func (f *fakeCluster) versionCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.versionCalls)
}

func (f *fakeCluster) discardCallsReceived() []*pb.DiscardVersionRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pb.DiscardVersionRequest(nil), f.discardCalls...)
}

// newTestClient wires a Client to an in-memory cluster and hands out predictable
// keys, so "the same key" is something a test can name.
func newTestClient(t *testing.T, fake *fakeCluster) *Client {
	t.Helper()
	return newTestClientWithAdmin(t, fake, newFakeAdmin())
}

func newTestClientWithAdmin(t *testing.T, fake *fakeCluster, admin *fakeAdmin) *Client {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterKnowledgeBaseServiceServer(srv, fake)
	pb.RegisterAdminServiceServer(srv, admin)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	next := 0
	return &Client{
		conn:  conn,
		kb:    pb.NewKnowledgeBaseServiceClient(conn),
		query: pb.NewQueryServiceClient(conn),
		admin: pb.NewAdminServiceClient(conn),
		store: store,
		now:   time.Now,
		newID: func() string {
			next++
			return fmt.Sprintf("key-%d", next)
		},
	}
}

func addChange() []Change {
	return []Change{{Op: "ADD", DocID: "doc-1", Content: "正文"}}
}

// The ordering Submit promises: the record exists (with the key and the changes)
// BEFORE the request leaves, and the version comes back into it afterwards.
func TestSubmit_RecordsFirstThenBackfillsTheVersion(t *testing.T) {
	fake := newFakeCluster()
	c := newTestClient(t, fake)

	w, err := c.Submit(context.Background(), "kb-1", addChange())
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if w.VersionID != 1 {
		t.Errorf("version = %d, want 1", w.VersionID)
	}
	if w.ClientRequestID == "" {
		t.Fatal("no idempotency key was minted")
	}

	// The key the caller holds is the key the cluster was given: if they differ,
	// a re-send cannot land on the same version.
	fake.mu.Lock()
	sent := fake.versionCalls[0]
	fake.mu.Unlock()
	if sent.GetClientRequestId() != w.ClientRequestID {
		t.Errorf("cluster saw key %q, caller holds %q", sent.GetClientRequestId(), w.ClientRequestID)
	}
	if len(sent.GetChanges()) != 1 || sent.GetChanges()[0].GetDocId() != "doc-1" {
		t.Errorf("cluster saw %+v, want the one ADD", sent.GetChanges())
	}

	// And it is durable, not just in memory.
	stored, ok, err := c.Store().Get(w.ID)
	if err != nil || !ok {
		t.Fatalf("record %s missing from the store (ok=%v, err=%v)", w.ID, ok, err)
	}
	if stored.VersionID != 1 || len(stored.Changes) != 1 {
		t.Errorf("stored record = %+v, want version 1 with its changes", stored)
	}
}

// A failed call must NOT lose the record: that record is the only copy of the
// caller's changes, and keeping it is what leaves a re-send possible.
func TestSubmit_KeepsTheRecordWhenTheCallFails(t *testing.T) {
	fake := newFakeCluster()
	fake.createErr = errors.New("kvraft: not leader")
	c := newTestClient(t, fake)

	w, err := c.Submit(context.Background(), "kb-1", addChange())
	if err == nil {
		t.Fatal("Submit = nil error, want the cluster's failure")
	}
	if w.VersionID != 0 {
		t.Errorf("version = %d, want 0 (nothing was allocated)", w.VersionID)
	}

	stored, ok, err := c.Store().Get(w.ID)
	if err != nil || !ok {
		t.Fatalf("the record was dropped on failure (ok=%v, err=%v)", ok, err)
	}
	if len(stored.Changes) != 1 || stored.ClientRequestID != w.ClientRequestID {
		t.Errorf("stored record = %+v, want the changes and the key kept", stored)
	}
}

func TestResend_UsesTheSameKeyAndGetsTheSameVersion(t *testing.T) {
	fake := newFakeCluster()
	c := newTestClient(t, fake)
	ctx := context.Background()

	first, err := c.Submit(ctx, "kb-1", addChange())
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	again, err := c.Resend(ctx, first.ID)
	if err != nil {
		t.Fatalf("Resend: %v", err)
	}
	if again.VersionID != first.VersionID {
		t.Errorf("re-send allocated version %d, want the original %d", again.VersionID, first.VersionID)
	}

	fake.mu.Lock()
	keys := []string{fake.versionCalls[0].GetClientRequestId(), fake.versionCalls[1].GetClientRequestId()}
	fake.mu.Unlock()
	if keys[0] != keys[1] {
		t.Errorf("re-send used key %q, want the original %q", keys[1], keys[0])
	}
}

func TestResend_RefusesOnceTheLocalChangesAreGone(t *testing.T) {
	fake := newFakeCluster()
	c := newTestClient(t, fake)
	ctx := context.Background()

	w, err := c.Submit(ctx, "kb-1", addChange())
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := c.ForgetChanges(w.ID); err != nil {
		t.Fatalf("ForgetChanges: %v", err)
	}
	if _, err := c.Resend(ctx, w.ID); err == nil {
		t.Fatal("Resend without local changes = nil error, want a refusal")
	}
	// The key and version survive forgetting: only the payload is gone, and that
	// is exactly the state Decide's haveChanges argument describes.
	stored, _, err := c.Store().Get(w.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.ClientRequestID == "" || stored.VersionID == 0 || len(stored.Changes) != 0 {
		t.Errorf("record after forgetting = %+v, want key+version kept, changes gone", stored)
	}
}

func TestDiscard_SendsTheVersionAndSettlesTheRecord(t *testing.T) {
	fake := newFakeCluster()
	c := newTestClient(t, fake)
	ctx := context.Background()

	w, err := c.Submit(ctx, "kb-1", addChange())
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := c.ForgetChanges(w.ID); err != nil {
		t.Fatalf("ForgetChanges: %v", err)
	}
	if err := c.Discard(ctx, w.ID); err != nil {
		t.Fatalf("Discard: %v", err)
	}

	calls := fake.discardCallsReceived()
	if len(calls) != 1 || calls[0].GetVersionId() != w.VersionID || calls[0].GetKnowledgeBaseId() != "kb-1" {
		t.Errorf("discard calls = %+v, want one for kb-1/v%d", calls, w.VersionID)
	}
	stored, _, err := c.Store().Get(w.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !stored.Settled {
		t.Error("the record was not marked settled after a successful discard")
	}
}

func TestDecision_FollowsTheServersStage(t *testing.T) {
	fake := newFakeCluster()
	c := newTestClient(t, fake)
	ctx := context.Background()

	w, err := c.Submit(ctx, "kb-1", addChange())
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	fake.mu.Lock()
	fake.awaitStage, fake.dataMissing = "DATA_PENDING", true
	fake.mu.Unlock()
	if got, _, err := c.Decision(ctx, w.ID); err != nil || got != DecisionResend {
		t.Errorf("Decision = %v (err %v), want RESEND: the data is missing but the changes are still here", got, err)
	}

	if err := c.ForgetChanges(w.ID); err != nil {
		t.Fatalf("ForgetChanges: %v", err)
	}
	if got, _, err := c.Decision(ctx, w.ID); err != nil || got != DecisionDiscard {
		t.Errorf("Decision = %v (err %v), want DISCARD: the data is missing and the changes are gone", got, err)
	}

	fake.mu.Lock()
	fake.awaitStage, fake.dataMissing = "INDEX_READY", false
	fake.mu.Unlock()
	if got, _, err := c.Decision(ctx, w.ID); err != nil || got != DecisionDone {
		t.Errorf("Decision = %v (err %v), want DONE", got, err)
	}
}

// A submission whose response never arrived has no version to wait on; saying so
// points the caller at the one action that helps (re-send under the same key)
// instead of at a deadline.
func TestAwaitOnce_WithoutAVersionPointsAtTheResend(t *testing.T) {
	fake := newFakeCluster()
	fake.createErr = errors.New("connection reset")
	c := newTestClient(t, fake)

	w, err := c.Submit(context.Background(), "kb-1", addChange())
	if err == nil {
		t.Fatal("Submit = nil error, want the failure")
	}
	if _, _, err := c.AwaitOnce(context.Background(), w.ID); err == nil {
		t.Fatal("AwaitOnce on a version-less record = nil error, want a pointer at the re-send")
	}
	if got := fake.versionCallCount(); got != 1 {
		t.Errorf("CreateVersion calls = %d, want 1", got)
	}
}

// fakeAdmin answers the operator-facing calls this client makes and records what it
// was asked. It can fail them on demand, which is how the pass-through contract is
// pinned: a refused data-side retry must reach the caller with the server's
// explanation intact.
type fakeAdmin struct {
	pb.UnimplementedAdminServiceServer

	mu          sync.Mutex
	lastList    *pb.ListFailedVersionsRequest
	lastRetry   *pb.ForceRetryVersionRequest
	lastAbandon *pb.ForceAbandonVersionRequest
	failed      []*pb.FailedVersion
	deleted     []int64
	retryErr    error
}

func newFakeAdmin() *fakeAdmin { return &fakeAdmin{} }

func (f *fakeAdmin) ListFailedVersions(_ context.Context, req *pb.ListFailedVersionsRequest) (*pb.ListFailedVersionsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastList = req
	return &pb.ListFailedVersionsResponse{Versions: f.failed}, nil
}

func (f *fakeAdmin) ForceRetryVersion(_ context.Context, req *pb.ForceRetryVersionRequest) (*pb.ForceRetryVersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastRetry = req
	if f.retryErr != nil {
		return nil, f.retryErr
	}
	return &pb.ForceRetryVersionResponse{Success: true, Side: pb.FailureSide_FAILURE_SIDE_INDEX}, nil
}

func (f *fakeAdmin) ForceAbandonVersion(_ context.Context, req *pb.ForceAbandonVersionRequest) (*pb.ForceAbandonVersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastAbandon = req
	return &pb.ForceAbandonVersionResponse{Success: true, DeletedVersionIds: f.deleted}, nil
}

// TestClient_FailedVersionQueueAndTheTwoAnswers: the library carries the operator's
// three calls like any other RPC and decides nothing itself. The load-bearing
// assertion is the middle one — a data-side retry is refused by the server, and the
// refusal (which names ForceAbandonVersion) must reach the caller rather than being
// flattened into a generic error or, worse, swallowed.
func TestClient_FailedVersionQueueAndTheTwoAnswers(t *testing.T) {
	admin := newFakeAdmin()
	c := newTestClientWithAdmin(t, newFakeCluster(), admin)
	ctx := context.Background()

	admin.failed = []*pb.FailedVersion{{
		KbId: "kb-1", VersionId: 7, Reason: "data unavailable on every replica",
		FailureCount: 5, Side: pb.FailureSide_FAILURE_SIDE_DATA,
	}}

	got, err := c.ListFailedVersions(ctx, "kb-1")
	if err != nil {
		t.Fatalf("ListFailedVersions: %v", err)
	}
	if len(got) != 1 || got[0].GetVersionId() != 7 || got[0].GetSide() != pb.FailureSide_FAILURE_SIDE_DATA {
		t.Fatalf("queue = %v, want kb-1 v7 on the data side", got)
	}
	if admin.lastList.GetKnowledgeBaseId() != "kb-1" {
		t.Errorf("asked about %q, want kb-1", admin.lastList.GetKnowledgeBaseId())
	}

	// A data-side verdict: the server refuses, and the operator has to see why.
	admin.retryErr = status.Error(codes.FailedPrecondition,
		"version 7's data side is FAILED_PERMANENT: its data will never arrive, "+
			"so its index cannot be rebuilt — abandon the version instead (ForceAbandonVersion)")
	if err := c.ForceRetryVersion(ctx, "kb-1", 7); err == nil {
		t.Fatal("a refused retry must surface as an error, not as a silent success")
	} else if !strings.Contains(err.Error(), "ForceAbandonVersion") {
		t.Errorf("err = %v, want the server's explanation carried through", err)
	}

	admin.deleted = []int64{7}
	ids, err := c.ForceAbandonVersion(ctx, "kb-1", 7)
	if err != nil {
		t.Fatalf("ForceAbandonVersion: %v", err)
	}
	if len(ids) != 1 || ids[0] != 7 {
		t.Errorf("deleted = %v, want [7]", ids)
	}
	if admin.lastAbandon.GetVersionId() != 7 {
		t.Errorf("abandoned v%d, want v7", admin.lastAbandon.GetVersionId())
	}
}
