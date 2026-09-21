package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// fakeControlNode is one in-memory control node: it records the commands it is
// handed and answers the metadata reads a storage node makes. One type serves
// all three services because a real control node exposes them on one endpoint.
type fakeControlNode struct {
	pb.UnimplementedInternalServiceServer
	pb.UnimplementedKnowledgeBaseServiceServer
	pb.UnimplementedAdminServiceServer

	mu          sync.Mutex
	proposals   [][]byte
	proposeResp *pb.ProposeResponse

	kbInfo      *pb.KnowledgeBaseInfo
	kbErr       error
	versions    []*pb.VersionInfo
	versionsErr error
	deleted     []int64
	deletedErr  error
	clusterResp *pb.GetClusterStatusResponse

	// listReq is the last request that arrived through the ListVersions RPC, so a
	// caller's narrowing can be asserted on the wire: the ranged reads travel that
	// same RPC, which is the whole point of the bounds being request fields.
	listReq *pb.ListVersionsRequest
}

// listVersionsRequest returns the last ListVersions request, if any.
func (f *fakeControlNode) listVersionsRequest() *pb.ListVersionsRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listReq
}

func (f *fakeControlNode) Propose(_ context.Context, req *pb.ProposeRequest) (*pb.ProposeResponse, error) {
	f.mu.Lock()
	f.proposals = append(f.proposals, req.GetCommand())
	resp := f.proposeResp
	f.mu.Unlock()
	if resp == nil {
		return &pb.ProposeResponse{}, nil
	}
	return resp, nil
}

func (f *fakeControlNode) GetKnowledgeBase(context.Context, *pb.GetKnowledgeBaseRequest) (*pb.GetKnowledgeBaseResponse, error) {
	if f.kbErr != nil {
		return nil, f.kbErr
	}
	return &pb.GetKnowledgeBaseResponse{KnowledgeBase: f.kbInfo}, nil
}

// ListDeletedVersions backs RemoteRaftNode.DeletionsInRange.
func (f *fakeControlNode) ListDeletedVersions(context.Context, *pb.ListDeletedVersionsRequest) (*pb.ListDeletedVersionsResponse, error) {
	if f.deletedErr != nil {
		return nil, f.deletedErr
	}
	return &pb.ListDeletedVersionsResponse{VersionIds: f.deleted}, nil
}

func (f *fakeControlNode) ListVersions(_ context.Context, req *pb.ListVersionsRequest) (*pb.ListVersionsResponse, error) {
	f.mu.Lock()
	f.listReq = req
	f.mu.Unlock()
	if f.versionsErr != nil {
		return nil, f.versionsErr
	}
	return &pb.ListVersionsResponse{Versions: f.versions}, nil
}

func (f *fakeControlNode) ListKnowledgeBases(context.Context, *pb.ListKnowledgeBasesRequest) (*pb.ListKnowledgeBasesResponse, error) {
	return &pb.ListKnowledgeBasesResponse{KnowledgeBases: []*pb.KnowledgeBaseInfo{f.kbInfo}}, nil
}

func (f *fakeControlNode) GetClusterStatus(context.Context, *pb.GetClusterStatusRequest) (*pb.GetClusterStatusResponse, error) {
	if f.clusterResp == nil {
		return &pb.GetClusterStatusResponse{}, nil
	}
	return f.clusterResp, nil
}

// proposedCommands decodes every command this node received, in order.
func (f *fakeControlNode) proposedCommands(t *testing.T) []command {
	t.Helper()
	f.mu.Lock()
	raw := append([][]byte(nil), f.proposals...)
	f.mu.Unlock()

	out := make([]command, 0, len(raw))
	for _, data := range raw {
		cmd, err := decodeCommand(data)
		if err != nil {
			t.Fatalf("decode forwarded command: %v", err)
		}
		out = append(out, cmd)
	}
	return out
}

// newRemoteNode wires a RemoteRaftNode to one in-memory control node per ID.
func newRemoteNode(t *testing.T, nodes map[int64]*fakeControlNode) (*RemoteRaftNode, func()) {
	t.Helper()

	listeners := make(map[int64]*bufconn.Listener, len(nodes))
	servers := make([]*grpc.Server, 0, len(nodes))
	for id, node := range nodes {
		lis := bufconn.Listen(1 << 20)
		srv := grpc.NewServer()
		pb.RegisterInternalServiceServer(srv, node)
		pb.RegisterKnowledgeBaseServiceServer(srv, node)
		pb.RegisterAdminServiceServer(srv, node)
		go func(s *grpc.Server, l *bufconn.Listener) { _ = s.Serve(l) }(srv, lis)
		listeners[id] = lis
		servers = append(servers, srv)
	}

	addrs := make(map[int64]string, len(nodes))
	for id := range nodes {
		addrs[id] = fmt.Sprintf("control-%d", id)
	}

	node := &RemoteRaftNode{
		ControlAddrs: addrs,
		Dial: func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
			id, err := strconv.ParseInt(strings.TrimPrefix(addr, "control-"), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("unexpected address %q", addr)
			}
			lis, ok := listeners[id]
			if !ok {
				return nil, fmt.Errorf("no listener for %q", addr)
			}
			return grpc.NewClient("passthrough:///"+addr,
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return lis.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
	}
	return node, func() {
		for _, s := range servers {
			s.Stop()
		}
	}
}

// --- role ---

// TestRemoteRaftNode_IsNeverLeader pins the one answer a storage node must not
// get wrong: it is not a Raft member, so it never leads. The §7.13.4 leader
// gate and DataVersionHolders both read this, and both would misbehave if it
// ever claimed leadership.
func TestRemoteRaftNode_IsNeverLeader(t *testing.T) {
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: {}})
	defer cleanup()

	if node.IsLeader() {
		t.Fatal("a storage node must never report itself as leader")
	}
}

// --- proposals ---

// TestRemoteRaftNode_ForwardsProposalsToTheControlNode checks that a proposal
// reaches the control node encoded as the shared constructor encodes it.
func TestRemoteRaftNode_ForwardsProposalsToTheControlNode(t *testing.T) {
	control := &fakeControlNode{}
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: control})
	defer cleanup()

	if err := node.ProposeUpdateVersionStatus(proposeCtx(t), 7, types.IndexStatusReady, 0); err != nil {
		t.Fatalf("ProposeUpdateVersionStatus: %v", err)
	}

	got := control.proposedCommands(t)
	if len(got) != 1 {
		t.Fatalf("control node received %d commands, want 1", len(got))
	}
	want := newUpdateVersionStatusCommand(7, types.IndexStatusReady, 0)
	if got[0] != want {
		t.Errorf("forwarded command = %+v, want %+v", got[0], want)
	}
}

// TestRemoteRaftNode_FollowsARedirectToTheRealLeader covers the case the
// forwarding path exists for: the node it asked is not the leader and names the
// one that is. The command must land on that node, exactly once, and the
// original failure must not surface.
func TestRemoteRaftNode_FollowsARedirectToTheRealLeader(t *testing.T) {
	// Node 1 is a follower that names node 2; node 2 has no leader to name.
	follower := &fakeControlNode{proposeResp: &pb.ProposeResponse{LeaderId: 2}}
	leader := &fakeControlNode{proposeResp: &pb.ProposeResponse{VersionId: 42}}

	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: follower, 2: leader})
	defer cleanup()

	versionID, err := node.ProposeCreateVersion(proposeCtx(t), "kb-1", 3, WithClientRequestID("req-9"))
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	if versionID != 42 {
		t.Errorf("versionID = %d, want 42 (the leader's answer)", versionID)
	}

	if got := len(follower.proposedCommands(t)); got != 1 {
		t.Errorf("follower received %d commands, want 1", got)
	}
	atLeader := leader.proposedCommands(t)
	if len(atLeader) != 1 {
		t.Fatalf("leader received %d commands, want 1", len(atLeader))
	}
	want := newCreateVersionCommand("kb-1", 3, "req-9")
	if atLeader[0] != want {
		t.Errorf("command at the leader = %+v, want %+v", atLeader[0], want)
	}

	// The redirect is remembered: a second proposal goes straight to node 2.
	if err := node.ProposeRollback(proposeCtx(t), "kb-1", 1); err != nil {
		t.Fatalf("ProposeRollback: %v", err)
	}
	if got := len(follower.proposedCommands(t)); got != 1 {
		t.Errorf("follower received %d commands after the redirect was learned, want 1", got)
	}
}

// TestRemoteRaftNode_RebuildsSentinelErrors checks that a business error decided
// by the control node's state machine is still matchable with errors.Is on the
// storage side. The apply error travels as a wire name for exactly this reason.
func TestRemoteRaftNode_RebuildsSentinelErrors(t *testing.T) {
	control := &fakeControlNode{proposeResp: &pb.ProposeResponse{
		ErrorName:    stratumerrors.Name(stratumerrors.ErrVersionIsActive),
		ErrorMessage: "version is active",
	}}
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: control})
	defer cleanup()

	_, err := node.ProposeMarkVersionDeleting(proposeCtx(t), "kb-1", 4, types.VersionDeleteSubtree)
	if !errors.Is(err, stratumerrors.ErrVersionIsActive) {
		t.Fatalf("error = %v, want ErrVersionIsActive (the wire name must rebuild the sentinel)", err)
	}
}

// TestRemoteRaftNode_ReportsWhenNoControlNodeIsReachable keeps the failure mode
// honest: an unreachable control layer must surface as an error, never as a
// silent success that would look like "the metadata says nothing".
func TestRemoteRaftNode_ReportsWhenNoControlNodeIsReachable(t *testing.T) {
	node := &RemoteRaftNode{ControlAddrs: map[int64]string{1: "control-1"}}
	if err := node.ProposeUpdateVersionStatus(proposeCtx(t), 1, types.IndexStatusReady, 0); err == nil {
		t.Fatal("expected an error when no control node is reachable")
	}
	if _, err := node.GetKB(context.Background(), "kb-1"); err == nil {
		t.Fatal("expected an error when no control node is reachable")
	}
}

// TestRemoteRaftNode_ProposalOutcomeCarriesVersionIDs covers the two proposals
// whose result is not just an error: the version a create allocated, and the
// versions a delete actually marked.
func TestRemoteRaftNode_ProposalOutcomeCarriesVersionIDs(t *testing.T) {
	control := &fakeControlNode{proposeResp: &pb.ProposeResponse{DeletedVersionIds: []int64{4, 5}}}
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: control})
	defer cleanup()

	ids, err := node.ProposeMarkVersionDeleting(proposeCtx(t), "kb-1", 6, types.VersionDeleteAncestors)
	if err != nil {
		t.Fatalf("ProposeMarkVersionDeleting: %v", err)
	}
	if len(ids) != 2 || ids[0] != 4 || ids[1] != 5 {
		t.Errorf("deleted version IDs = %v, want [4 5]", ids)
	}

	got := control.proposedCommands(t)
	want := newMarkVersionDeletingCommand("kb-1", 6, types.VersionDeleteAncestors)
	if len(got) != 1 || got[0] != want {
		t.Errorf("forwarded command = %+v, want %+v", got, want)
	}
}

// --- metadata reads ---

// TestRemoteRaftNode_ReadsKnowledgeBaseMetadata is the mapping that matters most
// for correctness: the index build forwards this quantizer configuration to the
// vecstore, so a field dropped on the way back would silently produce a
// full-precision index where a quantized one was asked for.
func TestRemoteRaftNode_ReadsKnowledgeBaseMetadata(t *testing.T) {
	control := &fakeControlNode{kbInfo: &pb.KnowledgeBaseInfo{
		KnowledgeBaseId:  "kb-1",
		Name:             "docs",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		IndexType:        pb.IndexType_INDEX_TYPE_HNSW,
		Similarity:       pb.Similarity_SIMILARITY_EUCLIDEAN,
		Quantizer:        pb.QuantizerType_QUANTIZER_PQ,
		PqM:              96,
		PqNbits:          8,
		EmbedConfig:      &pb.EmbedConfig{ServiceAddr: "http://embed:8080", ModelId: "m-1"},
		ActiveVersionId:  12,
		Status:           pb.KBStatus_KB_STATUS_ACTIVE,
	}}
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: control})
	defer cleanup()

	kb, err := node.GetKB(context.Background(), "kb-1")
	if err != nil {
		t.Fatalf("GetKB: %v", err)
	}
	if kb.KBID != "kb-1" || kb.Name != "docs" {
		t.Errorf("identity = (%q, %q), want (kb-1, docs)", kb.KBID, kb.Name)
	}
	if kb.ChunkWindowSize != 512 || kb.ChunkOverlapSize != 64 {
		t.Errorf("chunking = (%d, %d), want (512, 64)", kb.ChunkWindowSize, kb.ChunkOverlapSize)
	}
	if kb.IndexType != "HNSW" || kb.Similarity != "EUCLIDEAN" {
		t.Errorf("index = (%q, %q), want (HNSW, EUCLIDEAN)", kb.IndexType, kb.Similarity)
	}
	if kb.QuantizerType != "PQ" || kb.QuantizerPQM != 96 || kb.QuantizerPQNBits != 8 {
		t.Errorf("quantizer = (%q, %d, %d), want (PQ, 96, 8)",
			kb.QuantizerType, kb.QuantizerPQM, kb.QuantizerPQNBits)
	}
	if kb.EmbedConfig.ServiceAddr != "http://embed:8080" || kb.EmbedConfig.ModelID != "m-1" {
		t.Errorf("embed config = %+v", kb.EmbedConfig)
	}
	if kb.ActiveVersionID != 12 {
		t.Errorf("active version = %d, want 12", kb.ActiveVersionID)
	}
}

// TestRemoteRaftNode_ListVersionsCarriesTheVersionChain covers the read the
// reconcile and the parent lookup are built on.
func TestRemoteRaftNode_ListVersionsCarriesTheVersionChain(t *testing.T) {
	control := &fakeControlNode{versions: []*pb.VersionInfo{
		{VersionId: 1, ParentVersionId: 0, CreatedAt: 100, IndexStatus: pb.IndexStatus_INDEX_STATUS_READY},
		{VersionId: 2, ParentVersionId: 1, CreatedAt: 200, IndexStatus: pb.IndexStatus_INDEX_STATUS_PENDING},
	}}
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: control})
	defer cleanup()

	versions, err := node.ListVersions(context.Background(), "kb-1")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("got %d versions, want 2", len(versions))
	}
	if versions[1].VersionID != 2 || versions[1].ParentVersionID != 1 {
		t.Errorf("version 2 = %+v, want id 2 with parent 1", versions[1])
	}
	if versions[1].KBID != "kb-1" {
		t.Errorf("KBID = %q, want kb-1 (threaded in, not on the wire)", versions[1].KBID)
	}
	if versions[1].IndexStatus != types.IndexStatusPending {
		t.Errorf("index status = %v, want Pending", versions[1].IndexStatus)
	}
	if versions[0].IndexStatus != types.IndexStatusReady {
		t.Errorf("version 1 index status = %v, want Ready", versions[0].IndexStatus)
	}
}

// TestRemoteRaftNode_ListKnowledgeBases covers the startup sweep's read (chunk
// bloom rebuild, retention, orphan-chunk GC all start here).
func TestRemoteRaftNode_ListKnowledgeBases(t *testing.T) {
	control := &fakeControlNode{kbInfo: &pb.KnowledgeBaseInfo{
		KnowledgeBaseId: "kb-1",
		ActiveVersionId: 3,
		Quantizer:       pb.QuantizerType_QUANTIZER_SQ8,
	}}
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: control})
	defer cleanup()

	kbs, err := node.ListKnowledgeBases(context.Background())
	if err != nil {
		t.Fatalf("ListKnowledgeBases: %v", err)
	}
	if len(kbs) != 1 || kbs[0].KBID != "kb-1" {
		t.Fatalf("knowledge bases = %+v, want one kb-1", kbs)
	}
	if kbs[0].ActiveVersionID != 3 || kbs[0].QuantizerType != "SQ8" {
		t.Errorf("kb = %+v, want active 3 with SQ8", kbs[0])
	}
}

// TestRemoteRaftNode_ReadsClusterStatus covers the leader lookup the pull path
// falls back to.
func TestRemoteRaftNode_ReadsClusterStatus(t *testing.T) {
	control := &fakeControlNode{clusterResp: &pb.GetClusterStatusResponse{
		NodeId: 1, HasLeader: true, LeaderId: 2, MemberCount: 3,
	}}
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: control})
	defer cleanup()

	st, err := node.GetClusterStatus(context.Background())
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if !st.HasLeader || st.LeaderID != 2 || st.MemberCount != 3 {
		t.Errorf("cluster status = %+v, want leader 2 of 3", st)
	}
}

// --- error identity across the wire ---

// TestRemoteRaftNode_KeepsTheMissingKnowledgeBaseSentinel pins the one place a
// sentinel would otherwise be lost: stratumerrors.ToGRPCStatus travels a code
// and a message, so a GetKB for a deleted knowledge base would come back as an
// anonymous NotFound. Callers match this error with errors.Is, and treating it
// as "the metadata says nothing" instead of "that knowledge base is gone" would
// change what they do about it.
func TestRemoteRaftNode_KeepsTheMissingKnowledgeBaseSentinel(t *testing.T) {
	control := &fakeControlNode{kbErr: stratumerrors.ToGRPCStatus(stratumerrors.ErrKnowledgeBaseNotFound)}
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: control})
	defer cleanup()

	_, err := node.GetKB(context.Background(), "kb-missing")
	if err == nil {
		t.Fatal("expected an error for a missing knowledge base")
	}
	if !errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Errorf("error %v does not match ErrKnowledgeBaseNotFound", err)
	}
}

// TestRemoteRaftNode_SkipsAFailingReadNode keeps a partly-down control layer
// from becoming a total read outage: every control node applies the same log, so
// a read that fails at one is served by the next.
func TestRemoteRaftNode_SkipsAFailingReadNode(t *testing.T) {
	down := &fakeControlNode{kbErr: status.Error(codes.Unavailable, "control node is down")}
	up := &fakeControlNode{kbInfo: &pb.KnowledgeBaseInfo{KnowledgeBaseId: "kb-1", ActiveVersionId: 5}}

	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: down, 2: up})
	defer cleanup()

	kb, err := node.GetKB(context.Background(), "kb-1")
	if err != nil {
		t.Fatalf("GetKB: %v", err)
	}
	if kb.KBID != "kb-1" || kb.ActiveVersionID != 5 {
		t.Errorf("kb = %+v, want kb-1 with active version 5", kb)
	}
}

// TestRemoteRaftNode_GetVersionReadsOneVersionAndReportsMissing covers the
// storage node's shape of a single-version read: the await path
// (docs/await-version-plan.md §7 Step 2), and — since a storage node serves the
// query path — the meta stage of EVERY query. A storage node has no state
// machine, so this goes to the control tier; what it must not do is drag the
// whole chain along (§F's second half: "look at one version, read one version").
// The bounds ride the same ListVersions RPC, so the wire carries one version.
func TestRemoteRaftNode_GetVersionReadsOneVersionAndReportsMissing(t *testing.T) {
	control := &fakeControlNode{versions: []*pb.VersionInfo{
		{VersionId: 1, ParentVersionId: 0, CreatedAt: 100, IndexStatus: pb.IndexStatus_INDEX_STATUS_READY},
		{VersionId: 2, ParentVersionId: 1, CreatedAt: 200, IndexStatus: pb.IndexStatus_INDEX_STATUS_PENDING},
	}}
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: control})
	defer cleanup()
	ctx := proposeCtx(t)

	got, err := node.GetVersion(ctx, "kb-1", 2)
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if got.VersionID != 2 || got.ParentVersionID != 1 {
		t.Errorf("GetVersion(2) = %+v, want id 2 with parent 1", got)
	}
	if got.KBID != "kb-1" {
		t.Errorf("KBID = %q, want kb-1 (threaded in, not on the wire)", got.KBID)
	}
	if got.IndexStatus != types.IndexStatusPending {
		t.Errorf("index status = %v, want Pending", got.IndexStatus)
	}

	// The read is NARROW: (versionID-1, versionID] carried by the same RPC.
	if req := control.listVersionsRequest(); req == nil ||
		req.GetFromExclusive() != 1 || req.GetToInclusive() != 2 {
		t.Errorf("ListVersions request = %+v, want the narrow bounds (1,2]", req)
	}

	if _, err := node.GetVersion(ctx, "kb-1", 99); !errors.Is(err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("GetVersion(99) = %v, want ErrVersionNotFound", err)
	}

	// A non-positive id must not make the lower bound a no-op that pulls the whole
	// chain back: version IDs start at 1.
	if _, err := node.GetVersion(ctx, "kb-1", 0); !errors.Is(err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("GetVersion(0) = %v, want ErrVersionNotFound", err)
	}

	// An unreachable control tier is an ERROR, not an empty answer: "I could not
	// ask" must never be reported as "it is not there".
	control.versionsErr = errors.New("control tier down")
	if _, err := node.GetVersion(ctx, "kb-1", 2); err == nil {
		t.Error("GetVersion with an unreachable control tier = nil error, want a failure")
	}
}

// TestRemoteRaftNode_DeletionsInRange_AsksTheControlTier pins that a storage node
// can obtain the "confirmed deleted" verdict at all. Without it the reconciler runs
// on the node that holds no leftovers (the control node keeps no stores) while the
// node that holds them has no state machine — the hole docs/known-gaps.md §B
// describes.
func TestRemoteRaftNode_DeletionsInRange_AsksTheControlTier(t *testing.T) {
	control := &fakeControlNode{deleted: []int64{4, 9}}
	node, cleanup := newRemoteNode(t, map[int64]*fakeControlNode{1: control})
	defer cleanup()

	got, err := node.DeletionsInRange(proposeCtx(t), "kb-1", 0, 100)
	if err != nil {
		t.Fatalf("DeletionsInRange: %v", err)
	}
	if len(got) != 2 || got[0] != 4 || got[1] != 9 {
		t.Errorf("DeletionsInRange = %v, want [4 9]", got)
	}
}
