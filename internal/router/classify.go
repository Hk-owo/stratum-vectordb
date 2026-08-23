// Package router implements the Stratum routing layer, the frontend of
// cmd/stratum-router: a gRPC front that lets external clients reach a Raft
// cluster through one address. Write operations (those that commit state
// through Raft) are forwarded to the current leader; read operations are
// load-balanced across all nodes.
//
// The router re-exposes the same three external services as a node
// (KnowledgeBaseService / QueryService / AdminService), so existing
// clients — including the HTTP gateway — only need to point their gRPC
// address at the router.
package router

import (
	pb "stratum/api/proto/stratum"
)

// writeMethods lists the RPCs that must run on the Raft leader. These are
// exactly the methods that mutate committed state (the Propose* paths in
// the service layer) or trigger index builds with side effects. Every
// other method is read-only and safe on any node.
//
// Keep this list in sync with the RPC sets in the service layer: if a
// method starts calling RaftNode.Propose*, it becomes a write.
var writeMethods = map[string]bool{
	pb.KnowledgeBaseService_CreateKnowledgeBase_FullMethodName: true,
	pb.KnowledgeBaseService_DeleteKnowledgeBase_FullMethodName: true,
	pb.KnowledgeBaseService_CreateVersion_FullMethodName:       true,
	pb.KnowledgeBaseService_RollbackVersion_FullMethodName:     true,
	pb.KnowledgeBaseService_DeleteVersion_FullMethodName:       true,
	pb.AdminService_RebuildIndex_FullMethodName:                true,
	pb.AdminService_WarmupVersion_FullMethodName:               true,
}

// isWriteMethod reports whether fullMethod (e.g.
// "/stratum.KnowledgeBaseService/CreateKnowledgeBase") is a leader-bound
// write operation.
func isWriteMethod(fullMethod string) bool {
	return writeMethods[fullMethod]
}
