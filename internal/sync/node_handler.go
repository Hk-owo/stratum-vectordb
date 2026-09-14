package sync

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

// NodeHandler is a node's complete data-plane service. A single gRPC service
// can only be registered once, so the export side (LeaderHandler) and the
// receive side (PushHandler) are combined here: peers pull from this node,
// push to it, and ask it about its presence and its data cursor.
//
// Every method of the service has to be forwarded explicitly — embedding
// UnimplementedDataSyncServiceServer makes a method this type forgets compile
// and register fine, and answer Unimplemented at runtime.
//
// That is not a hypothetical: four methods were missing here and each took a
// different feature down with it, silently.
//
//   - ExecuteVersionWrite: the §7.13.2 dispatch never reached a coordinator and
//     every write quietly fell back to running on the node that accepted it. In
//     an all-in-one cluster that fallback looks like normal operation, which is
//     why it survived; a topology where the control layer owns no storage has
//     nothing to fall back to.
//   - ConfirmVersionWrite: replicas kept their §7.3 takeover timers armed, so
//     every write ended with a replica announcing a version nobody had waited
//     for — visible only as warnings.
//   - PullVersionChanges: a lagging peer could not replay recorded deltas (§7.5)
//     and had to pull each version in full.
//   - ReportDataVersions: storage nodes could not report their cursors, leaving
//     the §7.13.4 aggregate empty — the leader's answer to "who holds version V"
//     was permanently "unknown".
//
// testdata/… covers this: a test drives every method of the service through a
// registered NodeHandler and fails if any of them answers Unimplemented, so the
// next method added to the proto fails a test instead of a feature.
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

// ExecuteVersionWrite runs a version's storage transaction on this node on the
// control layer's behalf (§7.13.2): the leader picks the coordinator from the
// knowledge base's replica topology and dispatches the work here.
func (h *NodeHandler) ExecuteVersionWrite(ctx context.Context, req *pb.ExecuteVersionWriteRequest) (*pb.ExecuteVersionWriteResponse, error) {
	return h.receiver.ExecuteVersionWrite(ctx, req)
}

// PullVersionData serves a peer that is pulling this node's data.
func (h *NodeHandler) PullVersionData(
	req *pb.PullVersionDataRequest,
	stream pb.DataSyncService_PullVersionDataServer,
) error {
	// A node with no storage of its own exports nothing — a control node is
	// exactly that, and it still serves this service for the cursor reports it
	// owns as leader. Saying so is better than a nil dereference.
	if h.exporter == nil {
		return status.Errorf(codes.Unimplemented, "sync: PullVersionData: this node exports no data")
	}
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

// ConfirmVersionWrite tells a replica that the version it received did reach
// quorum, so the §7.3 takeover timer it started can stand down.
func (h *NodeHandler) ConfirmVersionWrite(ctx context.Context, req *pb.ConfirmVersionWriteRequest) (*pb.ConfirmVersionWriteResponse, error) {
	return h.receiver.ConfirmVersionWrite(ctx, req)
}

// PullVersionChanges serves a lagging peer replaying a range of versions'
// recorded changes instead of pulling each version in full (§7.5).
func (h *NodeHandler) PullVersionChanges(req *pb.PullVersionChangesRequest, stream pb.DataSyncService_PullVersionChangesServer) error {
	return h.receiver.PullVersionChanges(req, stream)
}

// ReportDataVersions accepts a storage node's periodic cursor report, which the
// control leader aggregates into the "which node holds version V" view (§7.13.4).
func (h *NodeHandler) ReportDataVersions(ctx context.Context, req *pb.ReportDataVersionsRequest) (*pb.ReportDataVersionsResponse, error) {
	return h.receiver.ReportDataVersions(ctx, req)
}
