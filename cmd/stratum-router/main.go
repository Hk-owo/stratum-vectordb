// stratum-router is the Stratum routing layer: a gRPC front that lets
// external clients reach a Raft cluster through a single address. It
// re-exposes the three external services (KnowledgeBaseService /
// QueryService / AdminService); write operations are forwarded to the
// current leader, read operations are load-balanced across all nodes.
//
// Existing clients — including the HTTP gateway — can point their gRPC
// address at the router instead of a specific node.
//
// Usage:
//
//	stratum-router -listen 0.0.0.0:7009 -nodes 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
	"stratum/internal/router"
)

func main() {
	listen := flag.String("listen", "0.0.0.0:7009", "router gRPC listen address")
	nodes := flag.String("nodes", "127.0.0.1:7000", "comma-separated control-layer node gRPC addresses")
	storageNodes := flag.String("storage-nodes", "", "comma-separated storage-layer node gRPC addresses (default: same as -nodes)")
	tokensPath := flag.String("tokens", "", "path to the token table YAML; when set, client calls must present a credential from it")
	routeRefresh := flag.Duration("route-refresh", 5*time.Second, "how often to re-observe storage cursors and expected versions (0 disables the routing table)")
	flag.Parse()

	split := func(v string) []string {
		out := []string{}
		for _, a := range strings.Split(v, ",") {
			if a = strings.TrimSpace(a); a != "" {
				out = append(out, a)
			}
		}
		return out
	}

	addrs := split(*nodes)
	if len(addrs) == 0 {
		log.Fatalf("invalid -nodes %q", *nodes)
	}
	// 两层拓扑下读请求必须落在存储层：QueryService 只在持有索引的节点上存在，
	// 而 isRetryableErr 把 Unimplemented 当终态，发错层就是直接失败。
	// 留空表示单层拓扑——每个节点两样都做，读也在 -nodes 里。
	storage := split(*storageNodes)

	cfg := router.Config{Addrs: addrs, StorageAddrs: storage, RouteRefreshInterval: *routeRefresh}

	// §9.3(5): the credential table is a static file, loaded once. Loading is
	// all-or-nothing: a station that came up with a broken table would look
	// exactly like one whose table denies everything, so a parse failure is
	// fatal rather than a warning.
	if *tokensPath != "" {
		table, err := router.LoadTokenTable(*tokensPath)
		if err != nil {
			log.Fatalf("%v", err)
		}
		cfg.Auth = table.Authenticator()
		log.Printf("authentication enabled: %d credentials loaded from %s", table.Len(), *tokensPath)
	} else {
		log.Printf("authentication disabled: no -tokens given (access is controlled by isolation)")
	}

	rt, err := router.NewRouter(cfg)
	if err != nil {
		log.Fatalf("router: %v", err)
	}
	defer rt.Close()

	// §9.3(1): the routing table refreshes in the background; queries read it
	// rather than waiting on it.
	routeCtx, stopRoutes := context.WithCancel(context.Background())
	defer stopRoutes()
	rt.Start(routeCtx)

	gs := grpc.NewServer()
	pb.RegisterKnowledgeBaseServiceServer(gs, router.NewKBServer(rt))
	pb.RegisterQueryServiceServer(gs, router.NewQueryServer(rt))
	pb.RegisterAdminServiceServer(gs, router.NewAdminServer(rt))

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("router: listen %s: %v", *listen, err)
	}
	if len(storage) == 0 {
		log.Printf("stratum-router listening on %s, control=%v, storage=same", *listen, addrs)
	} else {
		log.Printf("stratum-router listening on %s, control=%v, storage=%v", *listen, addrs, storage)
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("received signal, shutting down")
		gs.GracefulStop()
	}()
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("router: serve: %v", err)
	}
	log.Printf("stratum-router stopped")
}
