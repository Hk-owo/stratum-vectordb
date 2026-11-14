package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"stratum/internal/chunkstore"
	"stratum/internal/index"
	"stratum/internal/nodestate"
	"stratum/internal/plane"
	stratumsync "stratum/internal/sync"
)

// shutdownDeps is what watching for a signal needs: the server to stop taking
// work, the phase machine to declare STOPPING on, and the two reporters the
// metrics endpoint samples.
type shutdownDeps struct {
	cfg        appConfig
	logger     *zap.Logger
	nodeState  *nodestate.Machine
	grpcServer *grpc.Server
	indexMgr   *index.IndexManagerImpl
	// chunkStore is only ever asked for a disk figure; nil on a control node.
	chunkStore *chunkstore.VecstoreChunkStore
}

// startShutdownWatcher turns SIGINT/SIGTERM into an orderly stop: declare
// STOPPING, close the metrics endpoint, then let the server finish the requests
// it already accepted.
func startShutdownWatcher(d shutdownDeps) {
	cfg, logger := d.cfg, d.logger
	nodeState, grpcServer := d.nodeState, d.grpcServer
	indexMgr, chunkStore := d.indexMgr, d.chunkStore

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// node.metrics_addr: the Prometheus endpoint, when the deployment asked for
	// one. Started before the signal handler so the handler can close it, and a
	// listen failure is only a warning — losing metrics must not keep the node
	// from serving.
	metricsSource := nodeMetricsSource{}
	if indexMgr != nil {
		metricsSource.LoadedIndexes = indexMgr.LoadedCount
	}
	if chunkStore != nil {
		metricsSource.ChunkStoreBytes = chunkStore.DiskUsage
	}
	metricsSrv, err := startMetricsServer(cfg.MetricsAddr, logger, metricsSource)
	if err != nil {
		logger.Warn("metrics endpoint not started", zap.Error(err))
	}

	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
		// docs/storage-node-state-machine-plan.md §3.1: STOPPING is terminal, and
		// it is declared before the server stops taking work — so a status call
		// arriving mid-shutdown sees the wind-down rather than a healthy node
		// that then stops answering.
		if nodeState != nil {
			if err := nodeState.Transition(nodestate.NodeStopping); err != nil {
				logger.Warn("node state: entering STOPPING", zap.Error(err))
			}
		}
		if metricsSrv != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()
			_ = metricsSrv.Shutdown(shutdownCtx)
		}
		grpcServer.GracefulStop()
	}()
}

// lagCatchupDeps is what the background catch-up is assembled from. A control
// node passes storageLocal=false and gets a nil sink — see the comment at the
// call site for why that is a property of the deployment rather than a switch.
type lagCatchupDeps struct {
	cfg          appConfig
	logger       *zap.Logger
	storageLocal bool
	dataPlane    *plane.LocalDataPlane
	holdersCache *plane.HoldersCache
	nodeState    *nodestate.Machine
	indexMgr     *index.IndexManagerImpl
}

// newLagCatchup returns the chain-tail sink the heartbeater feeds.
func newLagCatchup(d lagCatchupDeps) stratumsync.ChainTailSink {
	cfg, logger := d.cfg, d.logger
	storageLocal, dataPlane := d.storageLocal, d.dataPlane
	holdersCache, nodeState, indexMgr := d.holdersCache, d.nodeState, d.indexMgr

	var chainTails stratumsync.ChainTailSink

	// docs/active-lag-detection-design.md: turn the chain tail the leader sends back
	// into a background catch-up. There is no switch, and it is deliberately damped — a
	// random delay per signal and a bound on how many knowledge bases may catch up at
	// once — because the alternative is a node returning from an outage starting every
	// knowledge base together. It schedules nothing of its own: Ensure is the same call
	// a query makes, so the existing gates still apply.
	//
	// A control node gets NO catch-up, and that is not an optimisation. It has no
	// storage: no cursor to compare against a chain tail, and no puller to fetch with.
	// Wiring it anyway is what made every control node crash-loop (11–14 restarts, stack
	// at LagCatchup.catchUp → EnsureIndex → a nil *Follower) the moment the switch that
	// used to hide it was removed. "Can this node catch up at all" is a property of the
	// deployment, so the gate belongs here, in the assembly.
	if storageLocal {
		lagCatchup := plane.NewLagCatchup(plane.LagCatchupConfig{
			MinLagVersions:   int64(cfg.LagCatchupMinLagVersions),
			Jitter:           time.Duration(cfg.LagCatchupJitterMS) * time.Millisecond,
			MaxConcurrentKBs: cfg.LagCatchupMaxConcurrentKBs,
			// One catch-up's budget and how often it re-asks its peers who can
			// serve it (lag_catchup.timeout_ms / holders_refresh_interval_ms).
			Timeout:                cfg.LagCatchupTimeout,
			HoldersRefreshInterval: cfg.LagCatchupHoldersRefresh,
			Ensure:                 dataPlane.EnsureIndex,
			Cursor:                 dataPlane.DataVersionsSnapshot,
			// docs/data-source-holders-fallback-plan.md: a node that is behind finds out
			// who can serve it, off the Raft apply path where the lookup cannot ask.
			FindHolders: func(ctx context.Context, kbID string) {
				if version, addrs, ok := dataPlane.PeerHolders(ctx, kbID); ok {
					holdersCache.Store(kbID, version, addrs)
				}
			},
			// docs/storage-node-state-machine-plan.md §3.3: declare the
			// knowledge bases this node is catching up on, so the node's state
			// answers "is it behind" without re-deriving it from the cursors at
			// each reader. Only reachable inside this branch, where the machine
			// exists — see versionStateRecorder for the typed-nil discipline the
			// other wiring needs.
			NodeState: nodeState,
			Logger:    logger,
		})
		chainTails = lagCatchup

		// §8.6(d): the tombstone scan targets the versions a later append can never
		// clean up — the one being served AND the one at the chain tail. "Served" alone
		// leaves a hole that is not hypothetical: CreateVersion does not move the active
		// pointer (only RollbackVersion does), so a knowledge base that was never rolled
		// back has an EMPTY active set, the scan examines nothing, and its tail keeps
		// whatever tombstones its writes left. The tail comes from the mirror the leader
		// already feeds this node on every cursor report — no new RPC.
		if indexMgr != nil {
			indexMgr.SetChainTailVersionsProvider(func(context.Context) (map[string]int64, error) {
				return lagCatchup.ChainTails(), nil
			})
		}
		// Say so at startup: the pace it runs at is otherwise invisible until it does
		// something, and "nothing happened" is exactly what an operator needs to be able
		// to tell apart from "it is not wired".
		logger.Info("lag catch-up wired",
			zap.Int("min_lag_versions", cfg.LagCatchupMinLagVersions),
			zap.Int("jitter_ms", cfg.LagCatchupJitterMS),
			zap.Int("max_concurrent_kbs", cfg.LagCatchupMaxConcurrentKBs))
	}
	return chainTails
}
