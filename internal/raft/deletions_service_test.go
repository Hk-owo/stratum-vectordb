package raft

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"

	pb "stratum/api/proto/stratum"
)

// The internal service is how a storage node reaches the log, so the shape it returns
// has to carry the three facts together — entries, the window, and the epoch.
func TestInternalServiceServer_DeletionsSinceShapesTheResponse(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	impl.sm.mu.Lock()
	impl.sm.recordDeletion("kb-1", 4)
	impl.sm.recordDeletion("kb-2", 0) // a whole knowledge base
	impl.sm.mu.Unlock()

	srv := NewInternalServiceServer(impl)
	resp, err := srv.DeletionsSince(context.Background(), &pb.DeletionsSinceRequest{AfterSeq: 0, Limit: 10})
	if err != nil {
		t.Fatalf("DeletionsSince: %v", err)
	}
	if resp.GetLatestSeq() != 2 || resp.GetOldestSeq() != 1 {
		t.Fatalf("latest/oldest = %d/%d, want 2/1", resp.GetLatestSeq(), resp.GetOldestSeq())
	}
	if resp.GetEpoch() == "" {
		t.Fatal("epoch must travel with the entries: a consumer compares it with its watermark")
	}
	if len(resp.GetEntries()) != 2 {
		t.Fatalf("entries = %+v, want two", resp.GetEntries())
	}
	if first := resp.GetEntries()[0]; first.GetKnowledgeBaseId() != "kb-1" || first.GetVersionId() != 4 || first.GetSeq() != 1 {
		t.Fatalf("first entry = %+v, want kb-1/v4 at seq 1", first)
	}
	if second := resp.GetEntries()[1]; second.GetKnowledgeBaseId() != "kb-2" || second.GetVersionId() != 0 {
		t.Fatalf("second entry = %+v, want kb-2 with version 0 (the KB itself)", second)
	}

	// A caught-up read returns the window and nothing else.
	caught, err := srv.DeletionsSince(context.Background(), &pb.DeletionsSinceRequest{AfterSeq: resp.GetLatestSeq(), Limit: 10})
	if err != nil {
		t.Fatalf("DeletionsSince (caught up): %v", err)
	}
	if len(caught.GetEntries()) != 0 {
		t.Fatalf("caught-up entries = %+v, want none", caught.GetEntries())
	}
}

// The storage-node side of the same read: a RemoteRaftNode asks whichever control node
// it can reach and rebuilds the same shape.
func (f *fakeControlNode) DeletionsSince(context.Context, *pb.DeletionsSinceRequest) (*pb.DeletionsSinceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deletionsErr != nil {
		return nil, f.deletionsErr
	}
	return f.deletionsResp, nil
}

func TestRemoteRaftNode_DeletionsSinceReadsTheLog(t *testing.T) {
	fake := &fakeControlNode{deletionsResp: &pb.DeletionsSinceResponse{
		LatestSeq: 9,
		OldestSeq: 5,
		Epoch:     "epoch-7",
		Entries: []*pb.DeletedVersionEntry{
			{Seq: 6, KnowledgeBaseId: "kb-1", VersionId: 3},
			{Seq: 7, KnowledgeBaseId: "kb-2", VersionId: 0},
		},
	}}
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: fake})
	defer cleanup()

	entries, latest, oldest, epoch, err := node.DeletionsSince(context.Background(), 5, 2)
	if err != nil {
		t.Fatalf("DeletionsSince: %v", err)
	}
	if latest != 9 || oldest != 5 || epoch != "epoch-7" {
		t.Fatalf("window = (%d, %d, %q), want (9, 5, epoch-7)", latest, oldest, epoch)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %+v, want two", entries)
	}
	if entries[0].Seq != 6 || entries[0].KBID != "kb-1" || entries[0].VersionID != 3 {
		t.Fatalf("first entry = %+v, want seq 6 / kb-1 / v3", entries[0])
	}
	if entries[1].VersionID != 0 {
		t.Fatalf("second entry = %+v, want the KB-level entry (version 0)", entries[1])
	}
}

// A proposal whose caller gives up must not leave a waiter behind: the table is what
// the apply side matches against, and a stale entry would be settled with a result
// nobody is waiting for.
func TestRaftNodeImpl_ACancelledProposalLeavesNoWaiter(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)

	// Block the apply side by taking the state machine's lock is not possible from here;
	// instead cancel a context that is already done, which is the path that has to clean
	// up before any proposal is registered.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := impl.proposeAndWait(ctx, newCreateKBCommand(testKB("kb-x"))); err == nil {
		t.Fatal("expected the cancelled context to be reported")
	}

	impl.pendingMu.Lock()
	left := len(impl.pending)
	impl.pendingMu.Unlock()
	if left != 0 {
		t.Fatalf("pending proposals = %d, want 0", left)
	}

	// And the node still works afterwards.
	ids, _, _, _, err := impl.DeletionsSince(context.Background(), 0, 10)
	if err != nil || len(ids) != 0 {
		t.Fatalf("DeletionsSince after a cancelled proposal = (%+v, %v)", ids, err)
	}
}

// Guard against a helper drifting away from the proto: the entries are ordered by
// sequence, and the ids are what a caller sorts on.
func TestDeletedVersionEntriesAreOrderedBySequence(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	for _, id := range []int64{3, 1, 2} {
		impl.sm.mu.Lock()
		impl.sm.recordDeletion("kb-1", id)
		impl.sm.mu.Unlock()
	}
	entries, _, _, _, err := impl.DeletionsSince(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("DeletionsSince: %v", err)
	}
	seqs := make([]string, 0, len(entries))
	for _, e := range entries {
		seqs = append(seqs, strconv.FormatInt(e.Seq, 10))
	}
	if got := strings.Join(seqs, ","); got != "1,2,3" {
		t.Fatalf("sequences = %s, want 1,2,3", got)
	}
	if !sort.SliceIsSorted(entries, func(i, j int) bool { return entries[i].Seq < entries[j].Seq }) {
		t.Fatal("entries must be ascending by sequence")
	}
}
