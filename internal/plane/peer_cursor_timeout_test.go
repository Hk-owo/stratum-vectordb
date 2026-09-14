package plane

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

// blockingCursorQuerier models a peer that accepts the call and then says nothing —
// exactly what a storage node sees at boot, when its peers are still inside their own
// startup reconcile and have not reached grpcServer.Serve yet.
type blockingCursorQuerier struct{ called chan string }

func (q *blockingCursorQuerier) LocalVersionOf(ctx context.Context, peerAddr, _ string) (int64, error) {
	select {
	case q.called <- peerAddr:
	default:
	}
	<-ctx.Done() // never answers; only a deadline ends this
	return 0, ctx.Err()
}

// TestSafeDurableVersion_DoesNotBlockForeverOnSilentPeers is the boot deadlock reduced
// to its essentials.
//
// Every storage node runs ReconcileIndexes concurrently and reaches grpcServer.Serve
// only after its own reconcile returns — so during startup each one is asking peers
// that are inside this very call. With the caller's unbounded context (a
// context.Background() from the startup path) the whole cluster hung at boot: a node
// with historical data never logged "Stratum gRPC server listening", and control saw
// "connection refused" from every storage address. With a bounded per-peer deadline the
// node gives up, the quorum check does its job, and it boots.
func TestSafeDurableVersion_DoesNotBlockForeverOnSilentPeers(t *testing.T) {
	querier := &blockingCursorQuerier{called: make(chan string, 4)}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:  &stubIndexStore{},
		CursorQuerier: querier,
		ResolveReplicas: func(context.Context) ([]string, error) {
			return []string{"peer-a:7000", "peer-b:7000"}, nil
		},
		Logger: zap.NewNop(),
	})

	start := time.Now()
	_, _, err := dp.SafeDurableVersion(context.Background(), "kb-1")
	elapsed := time.Since(start)

	if err == nil {
		t.Error("with only silent peers there is no quorum, so this must report an error " +
			"rather than claim a safe durable version")
	}
	// Two peers, so two deadlines at most. Generous headroom for a loaded CI box, but
	// far below "forever" — which is the property under test.
	if limit := 3 * peerCursorTimeout; elapsed > limit {
		t.Errorf("SafeDurableVersion took %v against silent peers; want <= %v — a peer that "+
			"is not serving must be given up on, not waited on indefinitely", elapsed, limit)
	}
}
