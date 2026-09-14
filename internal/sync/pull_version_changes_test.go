package sync

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// stubVersionChanges answers a range request from a fixed table and records what
// it was asked for.
type stubVersionChanges struct {
	deltas  map[int64]wal.VersionDelta
	gotKBID string
	gotFrom int64
	gotTo   int64
}

func (s *stubVersionChanges) ChangesInRange(_ context.Context, kbID string, fromExclusive, toInclusive int64) (map[int64]wal.VersionDelta, error) {
	s.gotKBID, s.gotFrom, s.gotTo = kbID, fromExclusive, toInclusive
	return s.deltas, nil
}

// TestPushHandler_PullVersionChanges_StreamsDeltasInOrder pins §7.5's delta read:
// a lagging peer gets the recorded changes for the range — version by version, in
// ascending order, each carrying the parent it was applied to (the parent is what
// makes the replayed document set come out right).
func TestPushHandler_PullVersionChanges_StreamsDeltasInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reader := &stubVersionChanges{deltas: map[int64]wal.VersionDelta{
		3: {VersionID: 3, ParentVersionID: 2, Changes: []types.DocChange{{Op: types.ChangeOpAdd, DocID: "d3", Content: "three"}}},
		2: {VersionID: 2, ParentVersionID: 1, Changes: []types.DocChange{{Op: types.ChangeOpAdd, DocID: "d2", Content: "two"}}},
	}}
	_, _, addr := startPushServer(t, 4, WithVersionChangesReader(reader))
	client := dialSyncServer(t, addr)

	stream, err := client.PullVersionChanges(ctx, &pb.PullVersionChangesRequest{
		KnowledgeBaseId: "kb-1", FromExclusive: 1, ToInclusive: 3,
	})
	if err != nil {
		t.Fatalf("PullVersionChanges: %v", err)
	}
	var got []*pb.VersionChanges
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		got = append(got, msg)
	}

	if reader.gotKBID != "kb-1" || reader.gotFrom != 1 || reader.gotTo != 3 {
		t.Errorf("the server asked for (%s, (%d,%d]), want (kb-1, (1,3])", reader.gotKBID, reader.gotFrom, reader.gotTo)
	}
	if len(got) != 2 {
		t.Fatalf("streamed %d deltas, want 2", len(got))
	}
	if got[0].GetVersionId() != 2 || got[1].GetVersionId() != 3 {
		t.Fatalf("streamed versions = [%d %d], want [2 3] ascending", got[0].GetVersionId(), got[1].GetVersionId())
	}
	if got[0].GetParentVersionId() != 1 || got[1].GetParentVersionId() != 2 {
		t.Errorf("parents = [%d %d], want [1 2]", got[0].GetParentVersionId(), got[1].GetParentVersionId())
	}
	if len(got[0].GetChanges()) != 1 ||
		got[0].GetChanges()[0].GetDocId() != "d2" || got[0].GetChanges()[0].GetContent() != "two" {
		t.Errorf("v2 delta = %+v, want the recorded change", got[0].GetChanges())
	}
}

// No reader wired must be loud: silence would look like "this node has no changes
// in that range", which is precisely the wrong conclusion for a catching-up peer.
func TestPushHandler_PullVersionChanges_WithoutReaderIsLoud(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, addr := startPushServer(t, 4) // no WithVersionChangesReader
	client := dialSyncServer(t, addr)

	stream, err := client.PullVersionChanges(ctx, &pb.PullVersionChangesRequest{
		KnowledgeBaseId: "kb-1", FromExclusive: 1, ToInclusive: 3,
	})
	if err == nil {
		_, err = stream.Recv()
	}
	if err == nil {
		t.Fatal("a node with no changes reader must refuse, not answer an empty range")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("status code = %v, want FailedPrecondition", got)
	}
}
