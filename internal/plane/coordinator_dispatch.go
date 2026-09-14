package plane

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
	"stratum/internal/types"
)

// CoordinatorDispatcher hands a committed version's write to one of the
// knowledge base's replicas, which then acts as that write's coordinator
// (Stratum_设计文档v13.md §7.13.2): the control layer picks the node, the node
// runs the storage-layer transaction locally and fans out from there. This is
// what moves the data-plane cost (split/embed/store + fan-out) off the control
// leader, which is the whole point of the §7.13 model.
//
// Candidates are the KB's replica topology (ResolveReplicas — "the *other*
// replicas that should hold a written version") plus this node itself, tried in
// a stable order so a failing sequence is reproducible in logs. Keeping this
// node in the list is deliberate: a single-node cluster, or one where every peer
// is unreachable, must still make progress — the leader simply coordinates
// itself.
type CoordinatorDispatcher struct {
	replicas  ReplicaResolver
	selfAddr  string
	localhost func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error
	dial      func(ctx context.Context, addr string) (*grpc.ClientConn, error)
	logger    *zap.Logger
}

// CoordinatorDispatcherConfig wires a CoordinatorDispatcher.
type CoordinatorDispatcherConfig struct {
	// Replicas lists the other replicas that should hold a written version.
	// Optional: without it this node is the only candidate.
	Replicas ReplicaResolver

	// SelfAddr is this node's own DataSyncService address. It is what lets the
	// dispatcher fall back to coordinating locally.
	SelfAddr string

	// LocalWrite runs the write on this node without an RPC. Required for
	// SelfAddr to be a usable candidate; without it this node is skipped.
	LocalWrite func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error

	// Dial overrides the gRPC dialer, so tests can point at an in-process
	// listener. Optional.
	Dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)

	Logger *zap.Logger
}

// NewCoordinatorDispatcher returns a dispatcher that dials replicas directly.
func NewCoordinatorDispatcher(cfg CoordinatorDispatcherConfig) *CoordinatorDispatcher {
	dial := cfg.Dial
	if dial == nil {
		dial = func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
			return grpc.DialContext(ctx, addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
		}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &CoordinatorDispatcher{
		replicas:  cfg.Replicas,
		selfAddr:  cfg.SelfAddr,
		localhost: cfg.LocalWrite,
		dial:      dial,
		logger:    logger,
	}
}

// Dispatch tries each candidate until one of them completes the write locally,
// returning the address that did it. The error, when every candidate failed,
// wraps the last failure — the caller treats it as transient and lets the retry
// budget decide (§7.13.2: "try candidates in order until one succeeds").
func (d *CoordinatorDispatcher) Dispatch(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) (string, error) {
	candidates, err := d.candidates(ctx)
	if err != nil {
		return "", err
	}

	var lastErr error
	for _, addr := range candidates {
		if addr == d.selfAddr {
			// Coordinating locally: no RPC, no serialization.
			if err := d.localhost(ctx, kbID, versionID, parentVersionID, changes); err != nil {
				lastErr = fmt.Errorf("coordinate %s v%d locally: %w", kbID, versionID, err)
				d.logger.Warn("plane: dispatch: local coordination failed, trying the next candidate",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
				continue
			}
			return addr, nil
		}
		if err := d.dispatchTo(ctx, addr, kbID, versionID, parentVersionID, changes); err != nil {
			lastErr = err
			d.logger.Warn("plane: dispatch: candidate did not take the write",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.String("candidate", addr), zap.Error(err))
			continue
		}
		return addr, nil
	}

	if lastErr == nil {
		lastErr = errors.New("no candidate replicas")
	}
	return "", fmt.Errorf("plane: dispatch write for %s v%d: %w", kbID, versionID, lastErr)
}

// candidates returns the addresses to try, in a stable order: the other
// replicas sorted, then this node last (so the local fallback is the last
// resort rather than the default).
func (d *CoordinatorDispatcher) candidates(ctx context.Context) ([]string, error) {
	var others []string
	if d.replicas != nil {
		got, err := d.replicas(ctx)
		if err != nil {
			return nil, fmt.Errorf("plane: dispatch: resolve replicas: %w", err)
		}
		others = got
	}
	sort.Strings(others)

	out := make([]string, 0, len(others)+1)
	for _, addr := range others {
		if addr != "" && addr != d.selfAddr {
			out = append(out, addr)
		}
	}
	if d.selfAddr != "" && d.localhost != nil {
		out = append(out, d.selfAddr)
	}
	return out, nil
}

// dispatchTo asks one replica to run the write, and insists on its answer: a
// candidate that accepts the call but does not complete the transaction is not a
// coordinator, and the caller must move on to the next one.
func (d *CoordinatorDispatcher) dispatchTo(ctx context.Context, addr, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
	conn, err := d.dial(ctx, addr)
	if err != nil {
		return fmt.Errorf("dial replica %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := pb.NewDataSyncServiceClient(conn).ExecuteVersionWrite(ctx, &pb.ExecuteVersionWriteRequest{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
		ParentVersionId: parentVersionID,
		Changes:         docChangesToProto(changes),
	})
	if err != nil {
		return fmt.Errorf("ExecuteVersionWrite(%s v%d) at %s: %w", kbID, versionID, addr, err)
	}
	if !resp.GetCompleted() {
		return fmt.Errorf("ExecuteVersionWrite(%s v%d) at %s: accepted but not completed", kbID, versionID, addr)
	}
	return nil
}

// docChangesToProto is the wire form of a change list, the counterpart of
// sync's docChangesFromProto.
func docChangesToProto(in []types.DocChange) []*pb.DocChange {
	if len(in) == 0 {
		return nil
	}
	out := make([]*pb.DocChange, len(in))
	for i, c := range in {
		op := pb.ChangeOp_CHANGE_OP_ADD
		switch c.Op {
		case types.ChangeOpDelete:
			op = pb.ChangeOp_CHANGE_OP_DELETE
		case types.ChangeOpUpdate:
			op = pb.ChangeOp_CHANGE_OP_UPDATE
		}
		out[i] = &pb.DocChange{Op: op, DocId: c.DocID, Content: c.Content}
	}
	return out
}
