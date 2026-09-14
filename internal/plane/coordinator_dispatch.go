package plane

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

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

	// candidateTimeout bounds one candidate's turn at the write.
	candidateTimeout time.Duration

	// health is this node's own view of which peers it recently reached
	// (§7.13.5). It only reorders candidates — it never removes one — and it is
	// fed by the attempts below, so it needs no probe loop of its own.
	health *LocalHealthView
}

// defaultCandidateTimeout is how long one candidate may hold a dispatch.
//
// The candidate set is the knowledge base's replica topology, and a member of it
// can be gone. The caller's context cannot bound this for us: the write path
// dispatches from a background goroutine whose context is never cancelled, so
// without a per-candidate deadline the first unreachable replica absorbs the
// whole dispatch — and every replica that IS up goes unasked, leaving the
// version PENDING indefinitely rather than merely delayed.
const defaultCandidateTimeout = 15 * time.Second

// candidateTimeoutPerDoc adds to a candidate's budget in proportion to the work
// it was handed.
//
// That floor is only right for a candidate doing no work: it bounds dialling and
// handshaking, and keeps a candidate that never answers from absorbing the whole
// dispatch. A candidate that IS answering is writing documents — split, embed,
// store, fan out — and that cost grows with the batch. Holding it to a constant
// turns an ordinary large write into "candidate did not take the write":
// TestT4_DataVolume (1000 docs per version) had every candidate cut off while
// writing the version's doc list, so the version stayed empty, its index never
// reached READY, and the test timed out at 603s.
//
// The budget therefore scales with the batch. The number is deliberately loose:
// overshooting only delays the next candidate, while undershooting throws away a
// write that was about to land.
const candidateTimeoutPerDoc = 50 * time.Millisecond

// maxCandidateTimeout caps a scaled budget so a very large batch cannot hold one
// candidate indefinitely. Batches are bounded by the 4 MiB gRPC message limit
// (~1400 docs each), so this cap sits far above what a real batch needs.
const maxCandidateTimeout = 10 * time.Minute

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

	// Health is this node's view of which peers it recently reached (§7.13.5).
	// Optional — without it the dispatcher makes its own, so a node gets the
	// §7.13.5 ordering by default. Injected by an assembly that wants to share
	// one view across a node's outgoing traffic.
	Health *LocalHealthView

	Logger *zap.Logger
}

// NewCoordinatorDispatcher returns a dispatcher that dials replicas directly.
func NewCoordinatorDispatcher(cfg CoordinatorDispatcherConfig) *CoordinatorDispatcher {
	dial := cfg.Dial
	if dial == nil {
		// grpc.NewClient, not DialContext with WithBlock: a candidate that is
		// gone rather than slow must fail fast so the next one gets its turn.
		// Blocking here blocks the whole dispatch (see candidateTimeout).
		dial = func(_ context.Context, addr string) (*grpc.ClientConn, error) {
			return grpc.NewClient(addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	health := cfg.Health
	if health == nil {
		health = NewLocalHealthView(HealthViewConfig{})
	}
	return &CoordinatorDispatcher{
		replicas:         cfg.Replicas,
		selfAddr:         cfg.SelfAddr,
		localhost:        cfg.LocalWrite,
		dial:             dial,
		logger:           logger,
		candidateTimeout: defaultCandidateTimeout,
		health:           health,
	}
}

// candidateBudget is how long one candidate may hold this dispatch's write.
//
// It is the fixed floor plus a per-document allowance, capped — see
// candidateTimeoutPerDoc for why a constant cannot be right.
func (d *CoordinatorDispatcher) candidateBudget(docs int) time.Duration {
	if docs < 0 {
		docs = 0
	}
	budget := d.candidateTimeout + time.Duration(docs)*candidateTimeoutPerDoc
	if budget > maxCandidateTimeout {
		budget = maxCandidateTimeout
	}
	if budget <= 0 {
		budget = defaultCandidateTimeout
	}
	return budget
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

	// One budget for every candidate in this dispatch: they are being handed
	// the same work, so a candidate that needs longer than the others is not
	// slow, it is gone.
	budget := d.candidateBudget(len(changes))

	var lastErr error
	for _, addr := range candidates {
		// Each candidate gets its own bounded turn: one that is unreachable must
		// cost this dispatch a timeout, not the whole write.
		attemptCtx, cancel := context.WithTimeout(ctx, budget)

		if addr == d.selfAddr {
			// Coordinating locally: no RPC, no serialization.
			err := d.localhost(attemptCtx, kbID, versionID, parentVersionID, changes)
			cancel()
			// The local attempt is an observation too, but it is about this
			// node rather than a peer: recording it would let a node demote
			// itself, and §7.13.5's view exists to order PEERS.
			if err != nil {
				lastErr = fmt.Errorf("coordinate %s v%d locally: %w", kbID, versionID, err)
				d.logger.Warn("plane: dispatch: local coordination failed, trying the next candidate",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
				continue
			}
			return addr, nil
		}

		err := d.dispatchTo(attemptCtx, addr, kbID, versionID, parentVersionID, changes)
		cancel()
		// §7.13.5: the attempt just made IS the observation — no probe loop,
		// no extra RPC. It only ever reorders later dispatches.
		d.health.Observe(addr, err == nil)
		if err != nil {
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
	// §7.13.5: the sort above is what makes a failing sequence reproducible in
	// logs; the local view then moves peers this node recently could not reach
	// to the back, so a dispatch does not spend its first (expensive, scaled)
	// attempt on a node that is gone. It REORDERS only — every candidate is
	// still tried, which is what keeps a wrong verdict cheap.
	others = d.health.Rank(others)

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
