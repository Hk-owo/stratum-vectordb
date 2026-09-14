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
	"strings"

	pb "stratum/api/proto/stratum"
)

// writeMethods lists the RPCs that must run on the Raft leader. These are
// exactly the methods that mutate committed state (the Propose* paths in
// the service layer) or trigger index builds with side effects. Every
// other method is read-only and safe on any node.
//
// CreateVersion belongs here again (§7.13.2): the coordinator is chosen by the
// control layer, not by whichever node happens to accept the request — the
// leader picks a candidate from the KB's replica topology and has that candidate
// run the write. Until that dispatch path exists, the leader coordinates the
// write itself, which is what this entry preserves.
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

// isStorageMethod reports whether fullMethod is served by the storage layer —
// the nodes that hold data — rather than the control layer.
//
// QueryService and AdminService are both storage-side in fact: their
// constructors take the index manager and the local stores, so a control node
// cannot build them at all (§7.0's contract boundary). Routing them by layer
// rather than by "write vs read" is what keeps a query off a node that has no
// indices: reading the corpus is a read, but it is not a read of the control
// layer's metadata.
//
// AdminService is included whole. Its per-operation leaders differ — RebuildIndex
// acts on a version's storage, GetClusterStatus reports Raft connectivity — but
// every one of them is answerable from a storage node, and none from a control
// node.
func isStorageMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/stratum.QueryService/") ||
		strings.HasPrefix(fullMethod, "/stratum.AdminService/")
}
