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
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
	"stratum/internal/router"
)

func main() {
	listen := flag.String("listen", "0.0.0.0:7009", "router gRPC listen address")
	nodes := flag.String("nodes", "127.0.0.1:7000", "comma-separated cluster node gRPC addresses")
	flag.Parse()

	addrs := []string{}
	for _, a := range strings.Split(*nodes, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		log.Fatalf("invalid -nodes %q", *nodes)
	}

	rt, err := router.NewRouter(router.Config{Addrs: addrs})
	if err != nil {
		log.Fatalf("router: %v", err)
	}
	defer rt.Close()

	gs := grpc.NewServer()
	pb.RegisterKnowledgeBaseServiceServer(gs, router.NewKBServer(rt))
	pb.RegisterQueryServiceServer(gs, router.NewQueryServer(rt))
	pb.RegisterAdminServiceServer(gs, router.NewAdminServer(rt))

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("router: listen %s: %v", *listen, err)
	}
	log.Printf("stratum-router listening on %s, nodes=%v", *listen, addrs)

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
