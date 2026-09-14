package sync

import (
	"context"

	pb "stratum/api/proto/stratum"
)

// NodeHandler is a node's complete data-plane service. A single gRPC service
// can only be registered once, so the export side (LeaderHandler) and the
// receive side (PushHandler) are combined here: peers pull from this node,
// push to it, and ask it about its presence and its data cursor.
type NodeHandler struct {
	pb.UnimplementedDataSyncServiceServer

	exporter *LeaderHandler
	receiver *PushHandler
}

// NewNodeHandler combines the two directions for registration.
func NewNodeHandler(exporter *LeaderHandler, receiver *PushHandler) *NodeHandler {
	return &NodeHandler{exporter: exporter, receiver: receiver}
}

var _ pb.DataSyncServiceServer = (*NodeHandler)(nil)

// PullVersionData serves a peer that is pulling this node's data.
func (h *NodeHandler) PullVersionData(
	req *pb.PullVersionDataRequest,
	stream pb.DataSyncService_PullVersionDataServer,
) error {
	return h.exporter.PullVersionData(req, stream)
}

// PushVersionData accepts a version pushed by its write coordinator.
func (h *NodeHandler) PushVersionData(stream pb.DataSyncService_PushVersionDataServer) error {
	return h.receiver.PushVersionData(stream)
}

// VersionPresence reports whether this node holds a version's data.
func (h *NodeHandler) VersionPresence(ctx context.Context, req *pb.VersionPresenceRequest) (*pb.VersionPresenceResponse, error) {
	return h.receiver.VersionPresence(ctx, req)
}

// LocalVersion reports how far this node's contiguous history reaches.
func (h *NodeHandler) LocalVersion(ctx context.Context, req *pb.LocalVersionRequest) (*pb.LocalVersionResponse, error) {
	return h.receiver.LocalVersion(ctx, req)
}

// DeleteVersionData reclaims a version's physical data on this node.
func (h *NodeHandler) DeleteVersionData(ctx context.Context, req *pb.DeleteVersionDataRequest) (*pb.DeleteVersionDataResponse, error) {
	return h.receiver.DeleteVersionData(ctx, req)
}

// PushIndexData accepts an index built by another node (§8.4).
func (h *NodeHandler) PushIndexData(stream pb.DataSyncService_PushIndexDataServer) error {
	return h.receiver.PushIndexData(stream)
}
