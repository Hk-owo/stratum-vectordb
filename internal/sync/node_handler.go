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

	// indexReader reads a locally built artifact back off disk so a peer can
	// FETCH it — §8.4's "分发 N 份", and since the push half was removed, its only
	// direction (docs/index-owner-sharding-plan.md §7). Optional: without it
	// FetchIndexData answers Unimplemented and replicas fall back to building
	// their own, which is the behaviour before on-demand fetch existed — and the
	// documented fallback (a node that cannot fetch a version's artifact builds
	// it) makes that a degenerate case rather than a broken one.
	indexReader IndexArtifactReader

	// fetchSem bounds how many artifact transfers this node serves at once.
	//
	// §8.4(a) gated the PUSH side and left the pull side open; with push gone,
	// this is the only gate on the only path. It matters for the reason §8.4(a)
	// gave about push, verbatim: each transfer reads the WHOLE artifact into
	// memory (ReadIndexFiles → os.ReadFile), so "no limit" also means "unbounded
	// memory on the one node every replica is asking". N−1 replicas fetching the
	// same (kb,version) at the same moment is the ordinary consequence of a
	// version switch, not a pathological case.
	//
	// nil means unlimited, which is what a NodeHandler assembled without the
	// option gets — the same shape as the other optional wiring here.
	fetchSem chan struct{}
}

// IndexArtifactReader reads a version's persisted index and its sidecar.
//
// Declared here, by the consumer, like the handler's other narrow inputs:
// *index.IndexManagerImpl satisfies it, and the plane declares the identical one
// for the node that fetches.
type IndexArtifactReader interface {
	ReadIndexFiles(kbID string, versionID int64) (indexData, sidecarData []byte, err error)
}

// NodeHandlerOption configures a NodeHandler.
type NodeHandlerOption func(*NodeHandler)

// WithNodeIndexReader wires the artifact export a peer's FetchIndexData needs.
func WithNodeIndexReader(r IndexArtifactReader) NodeHandlerOption {
	return func(h *NodeHandler) { h.indexReader = r }
}

// WithMaxConcurrentIndexFetch bounds concurrent artifact transfers; n <= 0
// leaves the handler unlimited.
func WithMaxConcurrentIndexFetch(n int) NodeHandlerOption {
	return func(h *NodeHandler) { h.fetchSem = newFetchSem(n) }
}

// newFetchSem returns a gate of capacity n, or nil for "unlimited".
func newFetchSem(n int) chan struct{} {
	if n <= 0 {
		return nil
	}
	return make(chan struct{}, n)
}

// NewNodeHandler combines the two directions for registration.
func NewNodeHandler(exporter *LeaderHandler, receiver *PushHandler, opts ...NodeHandlerOption) *NodeHandler {
	h := &NodeHandler{exporter: exporter, receiver: receiver}
	for _, opt := range opts {
		opt(h)
	}
	return h
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

// FetchIndexData serves a peer that wants this node's copy of a version's
// artifact (§8.4's distribution, other direction).
//
// NotFound is the answer the CALLER acts on — "this node does not have it, ask
// someone else or build" — so the failure has to keep its wire name rather than
// collapsing into a generic Internal.
func (h *NodeHandler) FetchIndexData(req *pb.FetchIndexDataRequest, stream pb.DataSyncService_FetchIndexDataServer) error {
	if h.indexReader == nil {
		return status.Error(codes.Unimplemented, "sync: this node does not export index artifacts")
	}
	kbID, versionID := req.GetKnowledgeBaseId(), req.GetVersionId()
	if kbID == "" {
		return status.Error(codes.InvalidArgument, "sync: FetchIndexData: empty knowledge base id")
	}
	// Taken AFTER the argument checks: a malformed request costs no slot, and a
	// caller checking its own request should not have to wait behind transfers.
	if h.fetchSem != nil {
		select {
		case h.fetchSem <- struct{}{}:
			defer func() { <-h.fetchSem }()
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}

	indexData, sidecarData, err := h.indexReader.ReadIndexFiles(kbID, versionID)
	if err != nil {
		return status.Errorf(codes.NotFound, "sync: FetchIndexData(%s v%d): %v", kbID, versionID, err)
	}
	if len(indexData) == 0 {
		return status.Errorf(codes.NotFound, "sync: FetchIndexData(%s v%d): empty artifact", kbID, versionID)
	}
	return sendIndexArtifact(stream.Send, kbID, versionID, indexData, sidecarData)
}
