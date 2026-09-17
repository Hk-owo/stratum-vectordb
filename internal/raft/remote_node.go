package raft

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/authmeta"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"stratum/internal/wire"
)

// RemoteRaftNode is the RaftNode a storage-layer node holds when the control
// layer runs in its own process (Stratum_设计文档v13.md §7.0 的 v1 演进阶段 2、
// §11 阶段 ④「存储集群独立进程」).
//
// A storage node is not a Raft member: it holds no log, votes in no election,
// and never leads. But every component the storage layer is built from — the
// control-plane contract (plane.ControlPlane), the write executor, the index
// manager's metadata reads, the orphan-chunk collector — reaches replicated
// metadata through the raft.RaftNode interface. This type satisfies that
// interface by carrying the same operations to a control node over gRPC.
//
// Keeping the seam here rather than at the plane contract is deliberate: the
// storage layer's code is identical in both topologies, so a storage node and
// a same-process node cannot drift apart in how they read metadata or report
// progress. What crosses the process boundary is the same command set the
// local implementation appends to its log — encoded by the shared constructors
// in command.go, so the two paths cannot encode different commands for one
// logical operation.
//
// It writes nothing to local disk and owns no state beyond the leader hint it
// learns from a redirect.
type RemoteRaftNode struct {
	// ControlAddrs maps each control-layer node ID to its gRPC service address.
	// Every control node applies the same log, so reads may go to any of them;
	// proposals must reach the leader, which the redirect below finds.
	ControlAddrs map[int64]string

	// Dial is injectable for tests; nil means a plain insecure dial.
	Dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)

	mu       sync.Mutex
	leaderID int64 // 0 = not yet learned

	// connMu guards conns: one long-lived connection per control address.
	//
	// Reads used to dial per call and close immediately after, so every query a
	// storage node served paid a full TCP + HTTP/2 handshake to read metadata
	// that the control node answers from an in-memory map. Measured on the 3+3
	// cluster: the storage node's ListVersions took 7–22 ms and accounted for
	// 72–95% of the query's total, while the real work (vecstore search,
	// chunk→doc mapping, document reads) came to ~1–2.5 ms. Connections live for
	// the process's lifetime; a control node that goes away is grpc-go's to
	// reconnect, which is what a pooled connection is for.
	connMu sync.Mutex
	conns  map[string]*grpc.ClientConn
}

var _ RaftNode = (*RemoteRaftNode)(nil)

// IsLeader always answers false, and that is the correct answer rather than a
// stub: a storage node is not a Raft member, so it can never be the leader.
//
// The callers that read it agree: the §7.13.4 leader gate must not reset a
// leader's aggregate on a storage node, and DataVersionHolders must report
// "unknown" (not "nobody holds it") when asked off the leader.
func (r *RemoteRaftNode) IsLeader() bool { return false }

// --- proposals (forwarded to whichever control node leads) ---

// ProposeCreateKB implements RaftNode. A storage node has no reason to create a
// knowledge base, but the interface is implemented in full rather than partially:
// a half-implemented interface would fail at the call site with a panic, in
// production, on the path that was never exercised.
func (r *RemoteRaftNode) ProposeCreateKB(ctx context.Context, kb types.KnowledgeBaseMeta) error {
	res, err := r.propose(ctx, newCreateKBCommand(kb))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) ProposeMarkKBDeleting(ctx context.Context, kbID string) error {
	res, err := r.propose(ctx, newMarkKBDeletingCommand(kbID))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) ProposeMarkKBDeleteFailed(ctx context.Context, kbID string) error {
	res, err := r.propose(ctx, newMarkKBDeleteFailedCommand(kbID))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) ProposeRemoveKBMeta(ctx context.Context, kbID string) error {
	res, err := r.propose(ctx, newRemoveKBMetaCommand(kbID))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) ProposeCreateVersion(ctx context.Context, kbID string, parentVersionID int64, opts ...ProposeOption) (int64, error) {
	o := resolveProposeOptions(opts)
	res, err := r.propose(ctx, newCreateVersionCommand(kbID, parentVersionID, o.clientRequestID, o.emptyVersion))
	if err != nil {
		return 0, err
	}
	if res.Err != nil {
		return 0, res.Err
	}
	return res.VersionID, nil
}

func (r *RemoteRaftNode) ProposeUpdateVersionStatus(ctx context.Context, versionID int64, status types.IndexStatus, nodeID int64) error {
	res, err := r.propose(ctx, newUpdateVersionStatusCommand(versionID, status, nodeID))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) ProposeMarkVersionFailedPermanent(ctx context.Context, kbID string, versionID int64, side types.FailureSide, reason string, count int32) error {
	res, err := r.propose(ctx, newMarkVersionFailedPermanentCommand(kbID, versionID, side, reason, count))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) ProposeUpdateVersionSummary(ctx context.Context, versionID int64, docIDSetHash string) error {
	res, err := r.propose(ctx, newUpdateVersionSummaryCommand(versionID, docIDSetHash))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) ProposeMarkVersionDataDurable(ctx context.Context, versionID int64) error {
	res, err := r.propose(ctx, newMarkDataDurableCommand(versionID))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) ProposeRollback(ctx context.Context, kbID string, targetVersionID int64) error {
	res, err := r.propose(ctx, newRollbackCommand(kbID, targetVersionID))
	if err != nil {
		return err
	}
	return res.Err
}

func (r *RemoteRaftNode) ProposeMarkVersionDeleting(ctx context.Context, kbID string, versionID int64, mode types.VersionDeleteMode) ([]int64, error) {
	res, err := r.propose(ctx, newMarkVersionDeletingCommand(kbID, versionID, mode))
	if err != nil {
		return nil, err
	}
	if res.Err != nil {
		return nil, res.Err
	}
	return res.DeletedVersionIDs, nil
}

func (r *RemoteRaftNode) ProposeRemoveVersionMeta(ctx context.Context, kbID string, versionID int64) error {
	res, err := r.propose(ctx, newRemoveVersionMetaCommand(kbID, versionID))
	if err != nil {
		return err
	}
	return res.Err
}

// propose carries one command to a control node and returns its apply outcome.
//
// Candidates are the cached leader first, then every control node in ascending
// ID order, each tried at most once. A redirect (the receiving node naming a
// different leader) caches the new leader and moves on; the next iteration
// picks it up. The loop is bounded by the number of control nodes, so a pair of
// nodes that each believe the other leads cannot cause an endless chase.
func (r *RemoteRaftNode) propose(ctx context.Context, cmd command) (ForwardedResult, error) {
	data, err := encodeCommand(cmd)
	if err != nil {
		return ForwardedResult{}, err
	}

	tried := make(map[int64]bool, len(r.ControlAddrs))
	var lastErr error
	for attempt := 0; attempt <= len(r.ControlAddrs); attempt++ {
		id, addr, ok := r.nextCandidate(tried)
		if !ok {
			break
		}
		tried[id] = true

		res, redirect, err := r.proposeAt(ctx, id, addr, data)
		if err != nil {
			lastErr = err
			continue
		}
		if redirect != 0 {
			r.setLeader(redirect)
			lastErr = fmt.Errorf("raft: remote: control node %d is not the leader (it named %d)", id, redirect)
			continue
		}
		return res, nil
	}

	if lastErr == nil {
		lastErr = errors.New("raft: remote: no control node reachable")
	}
	return ForwardedResult{}, lastErr
}

// nextCandidate returns the next control node to try: the cached leader when it
// has not been tried yet, otherwise the lowest-numbered untried node.
func (r *RemoteRaftNode) nextCandidate(tried map[int64]bool) (id int64, addr string, ok bool) {
	r.mu.Lock()
	leader := r.leaderID
	r.mu.Unlock()

	if leader != 0 && !tried[leader] {
		if addr, present := r.ControlAddrs[leader]; present {
			return leader, addr, true
		}
	}

	ids := make([]int64, 0, len(r.ControlAddrs))
	for candidate := range r.ControlAddrs {
		if !tried[candidate] {
			ids = append(ids, candidate)
		}
	}
	if len(ids) == 0 {
		return 0, "", false
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids[0], r.ControlAddrs[ids[0]], true
}

// setLeader caches the leader a control node named.
func (r *RemoteRaftNode) setLeader(id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leaderID = id
}

// proposeAt runs one command on one control node. A non-zero redirect means the
// node is not the leader and names the one it believes is; that is an answer,
// not a failure, which is why it travels beside the result instead of inside it.
func (r *RemoteRaftNode) proposeAt(ctx context.Context, id int64, addr string, data []byte) (ForwardedResult, int64, error) {
	conn, err := r.connFor(ctx, addr)
	if err != nil {
		return ForwardedResult{}, 0, fmt.Errorf("raft: remote: dial control node %d (%s): %w", id, addr, err)
	}

	// Same reason as the reads: a proposal travels control-plane traffic, and
	// the receiving control node treats a mark-less call as one that reached its
	// port directly.
	resp, err := pb.NewInternalServiceClient(conn).Propose(authmeta.WithVerifiedMark(ctx), &pb.ProposeRequest{Command: data})
	if err != nil {
		return ForwardedResult{}, 0, fmt.Errorf("raft: remote: propose at control node %d (%s): %w", id, addr, err)
	}
	if moved := resp.GetLeaderId(); moved != 0 {
		return ForwardedResult{}, moved, nil
	}

	applyErr := stratumerrors.ByName(resp.GetErrorName())
	if applyErr == nil && resp.GetErrorMessage() != "" {
		// Unknown sentinel — a newer peer may know errors this build does not.
		applyErr = errors.New(resp.GetErrorMessage())
	}
	return ForwardedResult{
		VersionID:         resp.GetVersionId(),
		DeletedVersionIDs: resp.GetDeletedVersionIds(),
		Err:               applyErr,
	}, 0, nil
}

// --- reads (served by any control node) ---

// GetKB implements RaftNode.
func (r *RemoteRaftNode) GetKB(ctx context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
	var out types.KnowledgeBaseMeta
	err := r.readAtAnyControl(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := pb.NewKnowledgeBaseServiceClient(conn).GetKnowledgeBase(ctx,
			&pb.GetKnowledgeBaseRequest{KnowledgeBaseId: kbID})
		if err != nil {
			return err
		}
		kb, err := wire.KnowledgeBaseFromInfo(resp.GetKnowledgeBase())
		if err != nil {
			return err
		}
		out = kb
		return nil
	})
	return out, kbScopedError(err)
}

// ListVersions implements RaftNode.
func (r *RemoteRaftNode) ListVersions(ctx context.Context, kbID string) ([]types.VersionMeta, error) {
	var out []types.VersionMeta
	err := r.readAtAnyControl(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := pb.NewKnowledgeBaseServiceClient(conn).ListVersions(ctx,
			&pb.ListVersionsRequest{KnowledgeBaseId: kbID})
		if err != nil {
			return err
		}
		out = wire.VersionsFromInfos(kbID, resp.GetVersions())
		return nil
	})
	return out, kbScopedError(err)
}

// ListKnowledgeBases implements RaftNode.
func (r *RemoteRaftNode) ListKnowledgeBases(ctx context.Context) ([]types.KnowledgeBaseMeta, error) {
	var out []types.KnowledgeBaseMeta
	err := r.readAtAnyControl(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := pb.NewKnowledgeBaseServiceClient(conn).ListKnowledgeBases(ctx,
			&pb.ListKnowledgeBasesRequest{})
		if err != nil {
			return err
		}
		kbs := make([]types.KnowledgeBaseMeta, 0, len(resp.GetKnowledgeBases()))
		for _, info := range resp.GetKnowledgeBases() {
			kb, err := wire.KnowledgeBaseFromInfo(info)
			if err != nil {
				return err
			}
			kbs = append(kbs, kb)
		}
		out = kbs
		return nil
	})
	return out, err
}

// GetClusterStatus implements RaftNode.
func (r *RemoteRaftNode) GetClusterStatus(ctx context.Context) (types.ClusterStatus, error) {
	var out types.ClusterStatus
	err := r.readAtAnyControl(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := pb.NewAdminServiceClient(conn).GetClusterStatus(ctx, &pb.GetClusterStatusRequest{})
		if err != nil {
			return err
		}
		out = wire.ClusterStatusFromProto(resp)
		return nil
	})
	return out, err
}

// readAtAnyControl runs one read-only call against the first control node that
// answers.
//
// Reads are deliberately not leader-bound: every control node applies the same
// log, so any of them holds the metadata. A control node whose log has fallen
// behind can briefly serve stale metadata — the same characteristic the
// in-process implementation documents as a known limitation for GetKB and
// ListVersions alike, which is why this introduces no new staleness, only a
// wider window in which to observe the existing one.
//
// A failing node is skipped rather than retried on the same one: every error
// here is either a transport failure (another node may be up) or a
// deterministic answer the next node will repeat (which costs one wasted call,
// not a wrong result).
func (r *RemoteRaftNode) readAtAnyControl(ctx context.Context, op func(ctx context.Context, conn *grpc.ClientConn) error) error {
	ids := make([]int64, 0, len(r.ControlAddrs))
	for id := range r.ControlAddrs {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return errors.New("raft: remote: no control node configured")
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var lastErr error
	for _, id := range ids {
		conn, err := r.connFor(ctx, r.ControlAddrs[id])
		if err != nil {
			lastErr = fmt.Errorf("raft: remote: dial control node %d (%s): %w", id, r.ControlAddrs[id], err)
			continue
		}
		// The mark says "this came from inside the system". A storage node reads
		// replicated metadata through the control layer's client-facing service,
		// and a node configured with require_authenticated serves that service
		// only to calls carrying the mark — which this one is in kind, just not
		// in path. Without it a gated cluster's storage tier cannot read
		// metadata at all: the reads come back Unauthenticated and every query
		// fails. That is how this was found.
		err = op(authmeta.WithVerifiedMark(ctx), conn)
		if err == nil {
			return nil
		}
		lastErr = fmt.Errorf("raft: remote: read at control node %d (%s): %w", id, r.ControlAddrs[id], err)
	}
	return lastErr
}

// kbScopedError restores the sentinel identity a knowledge base scoped call
// loses on the wire.
//
// stratumerrors.ToGRPCStatus maps a business error onto a status code and keeps
// only the message, so the receiver can no longer match it with errors.Is. For
// GetKnowledgeBase and ListVersions the ambiguity is resolvable: both are
// knowledge base scoped, so a NotFound can only mean the knowledge base is
// missing — no other sentinel maps to that code from these two calls.
func kbScopedError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %s", stratumerrors.ErrKnowledgeBaseNotFound, err.Error())
	}
	return err
}

func (r *RemoteRaftNode) dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	if r.Dial != nil {
		return r.Dial(ctx, addr)
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// connFor returns a connection to a control address, dialing it once and
// reusing it afterwards.
//
// Why it exists: readAtAnyControl and proposeAt used to call dial() and close
// the connection immediately after, so every read a storage node served paid a
// fresh TCP + HTTP/2 handshake — to fetch metadata the control node answers from
// an in-memory map. Measured: ListVersions cost 7–22 ms per query and was 72–95%
// of the query's total time, while the actual work (vecstore search, chunk→doc
// mapping, document reads) came to ~1–2.5 ms. See the comment on the conns
// field for the rest.
func (r *RemoteRaftNode) connFor(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	if conn, ok := r.conns[addr]; ok {
		return conn, nil
	}
	conn, err := r.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	if r.conns == nil {
		r.conns = make(map[string]*grpc.ClientConn)
	}
	r.conns[addr] = conn
	return conn, nil
}
