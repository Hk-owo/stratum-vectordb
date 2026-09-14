package sync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/types"
)

// recordingWriteExecutor stands in for *plane.LocalDataPlane: it records what a
// dispatched coordinator was asked to apply.
type recordingWriteExecutor struct {
	mu      sync.Mutex
	calls   int
	kbID    string
	version int64
	parent  int64
	changes []types.DocChange
	err     error
}

func (e *recordingWriteExecutor) WriteVersionData(_ context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	e.kbID, e.version, e.parent, e.changes = kbID, versionID, parentVersionID, changes
	return e.err
}

func (e *recordingWriteExecutor) snapshot() (int, string, int64, int64, []types.DocChange) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls, e.kbID, e.version, e.parent, e.changes
}

func dialSyncServer(t *testing.T, addr string) pb.DataSyncServiceClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewDataSyncServiceClient(conn)
}

// TestPushHandler_ExecuteVersionWrite_RunsTheWriteLocally pins §7.13.2's
// dispatch half: the control layer picked this node as the coordinator, so the
// storage-layer transaction runs HERE, with the changes the dispatcher carried —
// and it is the local executor that is asked to do it, not something re-derived
// from metadata (the changes deliberately never enter the Raft log).
func TestPushHandler_ExecuteVersionWrite_RunsTheWriteLocally(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exec := &recordingWriteExecutor{}
	_, _, addr := startPushServer(t, 5, WithVersionWriteExecutor(exec))
	client := dialSyncServer(t, addr)

	resp, err := client.ExecuteVersionWrite(ctx, &pb.ExecuteVersionWriteRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       9,
		ParentVersionId: 8,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "alpha"},
			{Op: pb.ChangeOp_CHANGE_OP_DELETE, DocId: "doc-2"},
		},
		ClientRequestId: "req-1",
	})
	if err != nil {
		t.Fatalf("ExecuteVersionWrite: %v", err)
	}
	if !resp.GetCompleted() {
		t.Error("completed must be true once the local transaction reached COMMIT")
	}
	if resp.GetNodeId() != 5 {
		t.Errorf("node_id = %d, want 5 (the node that ran the write)", resp.GetNodeId())
	}

	calls, kbID, version, parent, changes := exec.snapshot()
	if calls != 1 {
		t.Fatalf("executor calls = %d, want 1", calls)
	}
	if kbID != "kb-1" || version != 9 || parent != 8 {
		t.Fatalf("executor saw (%s, v%d, parent v%d), want (kb-1, v9, parent v8)", kbID, version, parent)
	}
	if len(changes) != 2 ||
		changes[0].Op != types.ChangeOpAdd || changes[0].DocID != "doc-1" || changes[0].Content != "alpha" ||
		changes[1].Op != types.ChangeOpDelete || changes[1].DocID != "doc-2" {
		t.Fatalf("executor saw %+v, want the dispatched changes with the operator mapped", changes)
	}
}

// A node with no write executor must say so loudly. A dispatch that lands
// nowhere would leave the version's data missing while the control layer
// believes the write was handed off.
func TestPushHandler_ExecuteVersionWrite_WithoutExecutorIsLoud(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, addr := startPushServer(t, 5) // no WithVersionWriteExecutor
	client := dialSyncServer(t, addr)

	_, err := client.ExecuteVersionWrite(ctx, &pb.ExecuteVersionWriteRequest{
		KnowledgeBaseId: "kb-1", VersionId: 9, ParentVersionId: 8,
	})
	if err == nil {
		t.Fatal("a dispatch to a node with no write executor must fail, not silently succeed")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("status code = %v, want FailedPrecondition", got)
	}
}

// A failed local write must surface as an error to the dispatcher, which is what
// lets it try the next candidate.
func TestPushHandler_ExecuteVersionWrite_SurfacesWriteFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exec := &recordingWriteExecutor{err: errors.New("disk full")}
	_, _, addr := startPushServer(t, 5, WithVersionWriteExecutor(exec))
	client := dialSyncServer(t, addr)

	_, err := client.ExecuteVersionWrite(ctx, &pb.ExecuteVersionWriteRequest{
		KnowledgeBaseId: "kb-1", VersionId: 9, ParentVersionId: 8,
	})
	if err == nil {
		t.Fatal("a failed local write must be reported back to the dispatcher")
	}
	if got := status.Code(err); got != codes.Internal {
		t.Errorf("status code = %v, want Internal", got)
	}
}
