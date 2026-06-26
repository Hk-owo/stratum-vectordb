package kvraft

import (
	"fmt"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	kvraftpb "stratum/api/proto/kvraft"
)

// StartGRPC binds addr and starts serving this node's Raft RPCs
// (RequestVote / AppendEntries / InstallSnapshot) in the background.
func (rf *Raft) StartGRPC(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("kvraft: listen on %s: %w", addr, err)
	}

	server := grpc.NewServer()
	kvraftpb.RegisterRaftServer(server, rf)
	rf.grpcServer = server

	go func() {
		if err := server.Serve(ln); err != nil {
			rf.logger.Debug("Raft gRPC server stopped serving", zap.Error(err))
		}
	}()
	return nil
}

// StopGRPC gracefully stops the Raft gRPC server, waiting for any
// in-flight RPC handlers to finish before returning. Safe to call when no
// server was ever started (no-op) or more than once.
func (rf *Raft) StopGRPC() {
	if rf.grpcServer != nil {
		rf.grpcServer.GracefulStop()
	}
}
