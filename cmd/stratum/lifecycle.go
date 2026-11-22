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
	"stratum/service"
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
	// rpcMetrics is the same counter set the gRPC server increments.
	rpcMetrics *service.RPCMetrics
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
	metricsSource := nodeMetricsSource{RPCMetrics: d.rpcMetrics}
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
		// docs/已实施/storage-node-state-machine-plan.md §3.1: STOPPING is terminal, and
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
}

// newLagCatchup returns the chain-tail sink the heartbeater feeds and an
// optional reader returning the latest chain tails for index GC scans.
func newLagCatchup(d lagCatchupDeps) (stratumsync.ChainTailSink, func() map[string]int64) {
	cfg, logger := d.cfg, d.logger
	storageLocal, dataPlane := d.storageLocal, d.dataPlane
	holdersCache, nodeState := d.holdersCache, d.nodeState

	var chainTails stratumsync.ChainTailSink
	var chainTailsFn func() map[string]int64

	if storageLocal {
		lagCatchup := plane.NewLagCatchup(plane.LagCatchupConfig{
			MinLagVersions:         int64(cfg.LagCatchupMinLagVersions),
			Jitter:                 time.Duration(cfg.LagCatchupJitterMS) * time.Millisecond,
			MaxConcurrentKBs:       cfg.LagCatchupMaxConcurrentKBs,
			Timeout:                cfg.LagCatchupTimeout,
			HoldersRefreshInterval: cfg.LagCatchupHoldersRefresh,
			Ensure:                 dataPlane.EnsureIndex,
			Cursor:                 dataPlane.DataVersionsSnapshot,
			FindHolders: func(ctx context.Context, kbID string) {
				if version, addrs, ok := dataPlane.PeerHolders(ctx, kbID); ok {
					holdersCache.Store(kbID, version, addrs)
				}
			},
			NodeState: nodeState,
			Logger:    logger,
		})
		chainTails = lagCatchup
		chainTailsFn = lagCatchup.ChainTails

		logger.Info("lag catch-up wired",
			zap.Int("min_lag_versions", cfg.LagCatchupMinLagVersions),
			zap.Int("jitter_ms", cfg.LagCatchupJitterMS),
			zap.Int("max_concurrent_kbs", cfg.LagCatchupMaxConcurrentKBs))
	}
	return chainTails, chainTailsFn
}
