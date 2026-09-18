package main

// metrics.go — the node's Prometheus endpoint (node.metrics_addr).
//
// Why it exists: node.metrics_addr was advertised in the config file from the
// start and read by nobody, so a deployment that set it got no endpoint at all
// — the Prometheus client sat in go.mod as an unused indirect dependency. This
// wires the setting up.
//
// The payload is deliberately small, and that is a scope decision rather than a
// placeholder: the per-stage timings the query and write paths already emit at
// debug level answer "where did this request spend its time", and the collectors
// here answer the other question an operator asks — "is this node healthy and how
// loaded is it". Counters per RPC would need instrumentation on paths that
// currently have none, and are not what this setting promised.
//
// It is a plain read-only HTTP endpoint with no authentication of its own: the
// node's gRPC auth gate (§9.3(5)) does not apply to it. Binding it to a public
// interface publishes whatever it reports.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// nodeMetricsSource is the node state the endpoint reads.
//
// Either field may be nil, and a nil field simply leaves its series out: a
// control-role node has no index manager and no chunk store, and reporting a
// false 0 for "indexes in memory" would be worse than reporting nothing.
type nodeMetricsSource struct {
	// LoadedIndexes is how many version indexes this node holds in memory right
	// now (IndexManager.LoadedCount).
	LoadedIndexes func() int
	// ChunkStoreBytes is the chunk store's on-disk footprint
	// (ChunkStore.DiskUsage).
	ChunkStoreBytes func(ctx context.Context) (uint64, error)
}

// metricsRegistry builds what /metrics serves: the standard Go runtime and
// process collectors, plus what this node can answer about itself.
//
// A fresh registry rather than the client's package-level default: the default
// is global state, and registering the same collector into it twice — two nodes
// in one process, or a test that starts the endpoint more than once — panics.
func metricsRegistry(src nodeMetricsSource) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	if src.LoadedIndexes != nil {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "stratum_loaded_indexes",
			Help: "Version indexes currently resident in this node's memory.",
		}, func() float64 { return float64(src.LoadedIndexes()) }))
	}
	if src.ChunkStoreBytes != nil {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "stratum_chunk_store_bytes",
			Help: "Bytes the chunk store occupies on disk.",
		}, func() float64 {
			// Scrapes are best-effort: a failing disk query must not turn the
			// whole scrape into a 500 and hide every other series with it.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			n, err := src.ChunkStoreBytes(ctx)
			if err != nil {
				return 0
			}
			return float64(n)
		}))
	}
	return reg
}

// startMetricsServer serves /metrics on addr and returns the server so the
// caller can shut it down.
//
// An empty addr disables the endpoint, which is the default and stays the
// default: this is opt-in because the endpoint reports node state without
// authenticating the caller. A listen failure is returned rather than fatal —
// losing metrics must never keep a node from serving traffic.
func startMetricsServer(addr string, logger *zap.Logger, src nodeMetricsSource) (*http.Server, error) {
	if addr == "" {
		return nil, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics listen %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metricsRegistry(src), promhttp.HandlerOpts{}))

	srv := &http.Server{Addr: ln.Addr().String(), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Warn("metrics endpoint stopped", zap.Error(serveErr))
		}
	}()
	logger.Info("metrics endpoint listening",
		zap.String("addr", ln.Addr().String()), zap.String("path", "/metrics"))
	return srv, nil
}
