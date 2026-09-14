package sync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// VersionExporter is the storage layer's "hand one version's records to a
// sink" capability. *LeaderHandler implements it (it reads the local stores);
// keeping it an interface lets the Pusher be tested against a fake exporter.
type VersionExporter interface {
	ExportVersion(ctx context.Context, kbID string, versionID int64, send func(*pb.SyncEntry) error) error
}

// Pusher replicates one version's records to a target replica by driving that
// replica's PushVersionData stream. It is the write coordinator's side of the
// push direction (Stratum_设计文档v13.md §7.2): the coordinator exports from
// its own stores and the target applies, so a version that was just written
// reaches its replicas instead of waiting for them to poll for it.
type Pusher struct {
	exporter VersionExporter
	nodeID   int64
	dial     func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// PusherConfig wires a Pusher.
type PusherConfig struct {
	// Exporter supplies the version's records (see VersionExporter).
	Exporter VersionExporter
	// NodeID identifies this coordinator; echoed in diagnostics.
	NodeID int64
	// Dial overrides the default gRPC dialer, so tests can target an
	// in-process listener. Optional.
	Dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// NewPusher returns a Pusher over the given exporter.
func NewPusher(cfg PusherConfig) *Pusher {
	dial := cfg.Dial
	if dial == nil {
		dial = func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
			return grpc.DialContext(ctx, addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
		}
	}
	return &Pusher{exporter: cfg.Exporter, nodeID: cfg.NodeID, dial: dial}
}

// PushVersion hands (kbID, versionID) to the replica at targetAddr and waits
// for its acknowledgement. The receiving side applies records idempotently, so
// retrying after a lost acknowledgement is safe.
func (p *Pusher) PushVersion(ctx context.Context, targetAddr, kbID string, versionID int64) (*pb.PushVersionDataResponse, error) {
	conn, err := p.dial(ctx, targetAddr)
	if err != nil {
		return nil, fmt.Errorf("sync: dial replica %s: %w", targetAddr, err)
	}
	defer func() { _ = conn.Close() }()

	stream, err := pb.NewDataSyncServiceClient(conn).PushVersionData(ctx)
	if err != nil {
		return nil, fmt.Errorf("sync: open PushVersionData to %s: %w", targetAddr, err)
	}

	if err := p.exporter.ExportVersion(ctx, kbID, versionID, stream.Send); err != nil {
		return nil, fmt.Errorf("sync: export version %d to %s: %w", versionID, targetAddr, err)
	}

	ack, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("sync: close PushVersionData to %s: %w", targetAddr, err)
	}
	return ack, nil
}

// PushHandler is this node's data-plane service: it receives pushed versions
// and answers presence queries about what it holds. Writes go through exactly
// the logic the pull path uses (Follower.applyEntry), so both directions
// converge on the same local state.
type PushHandler struct {
	pb.UnimplementedDataSyncServiceServer

	follower *Follower
	nodeID   int64

	// localVersion reports this node's contiguous data cursor, so peers can
	// ask how far its history reaches (Stratum_设计文档v13.md §7.6). Optional:
	// without it the node answers 0 ("nothing known"), which makes it a
	// useless backfill source rather than a wrong one.
	localVersion LocalVersionReporter

	// advanceVersion moves that cursor once this node has received a version's
	// records. Optional, but a node without it answers 0 forever: the records
	// arrived, the data is complete, and the station's freshness check (§9.3(2))
	// still refuses this replica with "local history reaches version 0".
	advanceVersion LocalVersionAdvancer

	// dropper reclaims a version's physical data on request (v13 §10.6).
	dropper VersionDataDropper

	// watcher is the §7.3 takeover hook: this node received a version, so it
	// should be ready to stand up for it if the coordinator goes quiet.
	watcher VersionWriteWatcher

	// installer receives indexes built elsewhere (§8.4).
	installer IndexInstaller

	// dataSources, when set, records which peer announced it holds a version's
	// data (§8.5): the confirmation carries the writer's own address, and the
	// cleanup that reclaims a version's data drops the entry again. Optional:
	// without it this node learns nothing and keeps resolving sources the
	// pre-§8.5 way (ask the leader).
	dataSources DataSourceRegistry

	// writeExecutor, when set, lets this node act as the coordinator the
	// control layer dispatched a write to (§7.13.2): the local write
	// transaction runs here, and the fan-out to the other replicas starts here.
	writeExecutor VersionWriteExecutor

	// versionChanges, when set, answers a lagging peer's request for the changes
	// recorded over a version range (§7.5). Optional: without it this node cannot
	// serve delta backfill at all, and peers fall back to pulling full records.
	versionChanges VersionChangesReader

	// dataVersions, when set, receives the storage layer's periodic §7.13.4
	// report. Only the control leader aggregates: isLeader tells this node
	// whether it is the one that should accept, so a follower answers
	// accepted=false instead of silently holding a view nobody reads.
	dataVersions DataVersionRecorder
	isLeader     func() bool

	// watermarks, when set, supplies the §7.5 reclaim watermarks this node carries
	// back on the report's response. The reporter is the node that may need to act on
	// them, and under §7.13.2 it need not be the leader — so the leader sends them
	// rather than waiting to be asked.
	watermarks ReclaimWatermarkSource
}

// ReclaimWatermarkSource answers "how far can this knowledge base's recorded changes
// be discarded?" (v13 §7.5). *plane.LocalControlPlane implements it. Declared here,
// rather than importing plane, because plane imports this package.
type ReclaimWatermarkSource interface {
	ReclaimableChangesThrough(kbID string) (int64, bool)
}

// DataVersionRecorder is the control leader's §7.13.4 aggregate, reduced to the
// one call the receive side makes: store a node's complete report. *plane.
// DataVersionRegistry implements it. Declared here, rather than importing plane,
// because plane imports this package.
type DataVersionRecorder interface {
	Record(nodeID int64, address string, dataVersions map[string]int64)
}

// VersionWriteExecutor runs the storage layer's write transaction for a version
// the control layer has already committed — what a dispatched coordinator does
// (§7.13.2). *plane.LocalDataPlane implements it. Declared here, rather than
// importing plane, because plane imports this package.
type VersionWriteExecutor interface {
	WriteVersionData(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error
}

// WithVersionWriteExecutor wires the local write path a dispatched coordinator
// runs (§7.13.2).
func WithVersionWriteExecutor(e VersionWriteExecutor) PushHandlerOption {
	return func(h *PushHandler) { h.writeExecutor = e }
}

// VersionChangesReader reads back the changes this node recorded for a range of
// versions — what a lagging peer asks for when it catches up by replaying the
// delta instead of pulling each version's full record set (§7.5). *wal.FileWAL
// implements it; declared narrow here so this package does not depend on the WAL
// implementation.
type VersionChangesReader interface {
	ChangesInRange(ctx context.Context, kbID string, fromExclusive, toInclusive int64) (map[int64]wal.VersionDelta, error)
}

// WithVersionChangesReader wires the §7.5 delta source.
func WithVersionChangesReader(r VersionChangesReader) PushHandlerOption {
	return func(h *PushHandler) { h.versionChanges = r }
}

// WithDataVersionAggregator wires the §7.13.4 report sink and the leader check
// that decides whether this node should aggregate at all. Both are passed
// together because they are two halves of one decision: a node that aggregates
// without knowing whether it is the leader would store reports nobody reads,
// and one that knows without aggregating would answer accepted=true and drop the
// data. isLeader may be nil (treated as "never the leader": the single-node
// default for tests and for nodes with no control leadership).
func WithDataVersionAggregator(rec DataVersionRecorder, isLeader func() bool) PushHandlerOption {
	return func(h *PushHandler) {
		h.dataVersions = rec
		h.isLeader = isLeader
	}
}

// WithReclaimWatermarks wires the source of the §7.5 watermarks carried back on a
// report's response. It is separate from WithDataVersionAggregator because the two
// answer different questions: the aggregator decides whether to KEEP reports, this
// decides what to tell reporters they may DISCARD. A leader normally passes itself
// for both.
func WithReclaimWatermarks(src ReclaimWatermarkSource) PushHandlerOption {
	return func(h *PushHandler) { h.watermarks = src }
}

// DataSourceRegistry is the slice of plane.DataSourceRegistry that the receive
// side needs: record an announcement, forget one when the data goes away. It is
// declared here rather than importing plane, so the sync package stays
// independent of the storage layer's types.
type DataSourceRegistry interface {
	Register(kbID string, versionID int64, addr string)
	ForgetVersion(kbID string, versionID int64)
}

// WithDataSourceRegistry wires the §8.5 announcement sink.
func WithDataSourceRegistry(reg DataSourceRegistry) PushHandlerOption {
	return func(h *PushHandler) { h.dataSources = reg }
}

// LocalVersionReporter reports the highest version a node holds contiguously
// for a knowledge base (0 = nothing known yet).
type LocalVersionReporter interface {
	LocalVersionOf(kbID string) int64
}

// VersionDataDropper removes one version's physical data on this node. The
// write path falls back to the local stores (not the replicated metadata)
// because §10.6's cleanup reclaims exactly the physical data whose
// acknowledgement was lost.
type VersionDataDropper interface {
	DropVersionStorage(ctx context.Context, kbID string, versionID int64) error
}

// WithVersionDataDropper wires the cleanup target for DeleteVersionData.
func WithVersionDataDropper(d VersionDataDropper) PushHandlerOption {
	return func(h *PushHandler) { h.dropper = d }
}

// VersionWriteWatcher is the §7.3 takeover hook. The receiving side tells the
// storage layer "I have just taken delivery of version V", and the storage
// layer watches for the coordinator's confirmation — announcing the version
// itself if that confirmation never comes. The two calls are a pair: every
// Watch should end in a Confirm or in a takeover.
type VersionWriteWatcher interface {
	WatchVersionWrite(kbID string, versionID int64)
	ConfirmVersionWrite(kbID string, versionID int64)
}

// WithVersionWriteWatcher wires the §7.3 takeover hook.
func WithVersionWriteWatcher(w VersionWriteWatcher) PushHandlerOption {
	return func(h *PushHandler) { h.watcher = w }
}

// IndexInstaller writes a distributed index into this node's index directory
// and loads it, so a replica can serve a version it never built
// (Stratum_设计文档v13.md §8.4). *index.IndexManagerImpl implements it.
type IndexInstaller interface {
	InstallIndex(ctx context.Context, kbID string, versionID int64, indexData, sidecarData []byte) error
}

// WithIndexInstaller wires the §8.4 destination of a pushed index.
func WithIndexInstaller(i IndexInstaller) PushHandlerOption {
	return func(h *PushHandler) { h.installer = i }
}

// PushHandlerOption configures a PushHandler.
type PushHandlerOption func(*PushHandler)

// WithLocalVersion wires the node's data cursor.
func WithLocalVersion(r LocalVersionReporter) PushHandlerOption {
	return func(h *PushHandler) { h.localVersion = r }
}

// LocalVersionAdvancer moves the cursor LocalVersionReporter answers from.
//
// Receiving a version's records — over the push or the pull path — is what
// makes this node hold it, and the cursor is how it says so (§7.6/§9.3(2)).
// The reporter alone is not enough, and its absence is silent in the worst
// way: a replica with every record on disk answered "version 0", the station's
// freshness check refused it as stale, and reads fell through to whichever
// candidate happened to be as far behind.
type LocalVersionAdvancer interface {
	MarkVersionContiguous(kbID string, versionID int64)
}

// WithLocalVersionAdvancer wires the cursor update a received version performs.
func WithLocalVersionAdvancer(a LocalVersionAdvancer) PushHandlerOption {
	return func(h *PushHandler) { h.advanceVersion = a }
}

// NewPushHandler returns a PushHandler applying through follower.
func NewPushHandler(follower *Follower, nodeID int64, opts ...PushHandlerOption) *PushHandler {
	h := &PushHandler{follower: follower, nodeID: nodeID}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	return h
}

var _ pb.DataSyncServiceServer = (*PushHandler)(nil)

// PushVersionData implements DataSyncService.PushVersionData.
func (h *PushHandler) PushVersionData(stream pb.DataSyncService_PushVersionDataServer) error {
	// Applying a pushed version needs the local stores. A control node has none,
	// and it still serves this service for the cursor reports it owns as leader
	// — so the answer is "this node does not do that", not a nil dereference.
	if h.follower == nil {
		return status.Errorf(codes.Unimplemented, "sync: PushVersionData: this node holds no storage")
	}
	ctx := stream.Context()

	var kbID string
	var versionID int64
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := h.follower.applyEntry(ctx, entry); err != nil {
			return fmt.Errorf("sync: push apply (%s v%d): %w", entry.GetKbId(), entry.GetVersionId(), err)
		}
		// Only some entry types carry version_id (chunk records do not), so
		// take the last non-zero one rather than the last record's value.
		kbID = entry.GetKbId()
		if v := entry.GetVersionId(); v != 0 {
			versionID = v
		}
	}

	if kbID != "" {
		// Same post-apply step as the pull path: the records are in the local
		// stores, so this node holds the version — move the cursor, then
		// schedule this node's own index build.
		if h.advanceVersion != nil {
			h.advanceVersion.MarkVersionContiguous(kbID, versionID)
		}
		if err := h.follower.indexManager.TriggerBuild(ctx, kbID, versionID); err != nil {
			return fmt.Errorf("sync: push trigger build (%s v%d): %w", kbID, versionID, err)
		}
		// Start the §7.3 takeover timer. The data is here; the only thing that
		// can still go missing is the coordinator's word that a quorum holds
		// it, and this is how a replica notices that word never arrived.
		if h.watcher != nil {
			h.watcher.WatchVersionWrite(kbID, versionID)
		}
	}

	return stream.SendAndClose(&pb.PushVersionDataResponse{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
		AcceptorId:      h.nodeID,
	})
}

// ConfirmVersionWrite implements DataSyncService.ConfirmVersionWrite: the
// coordinator says the version reached quorum, so this node stands down its
// §7.3 takeover timer (Stratum_设计文档v13.md §7.3). The same message is §8.5's
// data-source announcement — req.source_addr names the writer — so this node
// records where the version's data lives; later readers resolve it from that
// fact instead of asking the leader (see plane.DataSourceRegistry).
//
// It is ALSO this node's cue that the version exists, which matters most for the
// version nobody sends: one with no document changes is never fanned out, so
// without acting here a replica would never learn of it and its cursor would
// stay behind a version it in fact holds.
func (h *PushHandler) ConfirmVersionWrite(_ context.Context, req *pb.ConfirmVersionWriteRequest) (*pb.ConfirmVersionWriteResponse, error) {
	kbID, versionID := req.GetKnowledgeBaseId(), req.GetVersionId()
	if h.watcher != nil {
		h.watcher.ConfirmVersionWrite(kbID, versionID)
	}
	if h.dataSources != nil && req.GetSourceAddr() != "" {
		h.dataSources.Register(kbID, versionID, req.GetSourceAddr())
	}
	// empty_version: this version has no document changes, so it was never
	// fanned out and there is nothing to fetch. Moving the cursor here is the
	// whole job — and it costs no I/O, which is why the coordinator sends the
	// fact rather than expecting the replica to discover it by pulling. Without
	// it the replica answers "version 0" to the station's freshness check
	// (§9.3(2)) for a version it holds, and its queries get refused.
	if req.GetEmptyVersion() && h.advanceVersion != nil {
		h.advanceVersion.MarkVersionContiguous(kbID, versionID)
	}
	return &pb.ConfirmVersionWriteResponse{NodeId: h.nodeID}, nil
}

// ExecuteVersionWrite implements DataSyncService.ExecuteVersionWrite: the
// control layer picked this node as the write's coordinator (§7.13.2), so the
// storage-layer write transaction runs here — the data lands on this node, and
// the fan-out to the remaining replicas starts from here as well. Idempotent:
// the local transaction is keyed and replayable (§7.12), so a dispatched retry
// and a client retry of the same write converge.
func (h *PushHandler) ExecuteVersionWrite(ctx context.Context, req *pb.ExecuteVersionWriteRequest) (*pb.ExecuteVersionWriteResponse, error) {
	if h.writeExecutor == nil {
		// Loud, not silent: a dispatch that lands nowhere would leave the
		// version's data missing while the control layer believes it was
		// handed off.
		return nil, status.Errorf(codes.FailedPrecondition,
			"sync: ExecuteVersionWrite: no write executor wired")
	}
	kbID, versionID, parentID := req.GetKnowledgeBaseId(), req.GetVersionId(), req.GetParentVersionId()
	if err := h.writeExecutor.WriteVersionData(ctx, kbID, versionID, parentID, docChangesFromProto(req.GetChanges())); err != nil {
		return nil, status.Errorf(codes.Internal,
			"sync: ExecuteVersionWrite(%s v%d): %v", kbID, versionID, err)
	}
	return &pb.ExecuteVersionWriteResponse{NodeId: h.nodeID, Completed: true}, nil
}

// PullVersionChanges implements DataSyncService.PullVersionChanges: it streams the
// recorded changes for (fromExclusive, toInclusive], so a peer that is behind can
// replay the delta instead of transferring each version's full record set (§7.5).
//
// Versions this node holds no record for are simply skipped — the caller has to
// SEE the gap and fall back to a full state transfer, rather than replay a range
// that silently omits versions.
func (h *PushHandler) PullVersionChanges(req *pb.PullVersionChangesRequest, stream pb.DataSyncService_PullVersionChangesServer) error {
	if h.versionChanges == nil {
		return status.Errorf(codes.FailedPrecondition, "sync: PullVersionChanges: no changes reader wired")
	}
	kbID := req.GetKnowledgeBaseId()
	deltas, err := h.versionChanges.ChangesInRange(stream.Context(), kbID, req.GetFromExclusive(), req.GetToInclusive())
	if err != nil {
		return status.Errorf(codes.Internal, "sync: PullVersionChanges(%s, (%d,%d]): %v",
			kbID, req.GetFromExclusive(), req.GetToInclusive(), err)
	}

	// Ascending version order: the peer replays in sequence, so it should not have
	// to buffer and sort the whole range first.
	versions := make([]int64, 0, len(deltas))
	for v := range deltas {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })

	for _, v := range versions {
		d := deltas[v]
		if err := stream.Send(&pb.VersionChanges{
			VersionId:       d.VersionID,
			ParentVersionId: d.ParentVersionID,
			Changes:         docChangesToProto(d.Changes),
		}); err != nil {
			return err
		}
	}
	return nil
}

// ReportDataVersions receives one node's periodic cursor report (§7.13.4) and, if
// this node is the control leader, folds it into the in-memory aggregate.
//
// A non-leader answers accepted=false rather than an error: the reporter's next
// interval re-resolves the leader (§7.13.1's pattern), and an error would have it
// retry the same wrong address. accepted=false is deliberately a success on the
// wire — "you reached the wrong node" is not a transport failure.
func (h *PushHandler) ReportDataVersions(ctx context.Context, req *pb.ReportDataVersionsRequest) (*pb.ReportDataVersionsResponse, error) {
	if h.dataVersions == nil || h.isLeader == nil || !h.isLeader() {
		return &pb.ReportDataVersionsResponse{Accepted: false, NodeId: h.nodeID}, nil
	}
	// A report is accepted on the reporter's own node_id, not on anything in the
	// payload: a node vouching for someone else's cursors would let a stale or
	// malicious reporter speak for a healthy node.
	if err := ctx.Err(); err != nil {
		return nil, status.Errorf(codes.Canceled, "sync: ReportDataVersions: %v", err)
	}
	h.dataVersions.Record(req.GetNodeId(), req.GetAddress(), req.GetDataVersions())

	resp := &pb.ReportDataVersionsResponse{Accepted: true, NodeId: h.nodeID}
	// Carry back the watermarks for the knowledge bases this reporter just told us
	// about: those are the ones it holds data for, and hence whose changes could be
	// sitting in its WAL. Nothing is sent for a knowledge base whose watermark cannot
	// be established — absent means "keep everything".
	if h.watermarks != nil {
		reclaimable := make(map[string]int64)
		for kbID := range req.GetDataVersions() {
			if watermark, ok := h.watermarks.ReclaimableChangesThrough(kbID); ok {
				reclaimable[kbID] = watermark
			}
		}
		if len(reclaimable) > 0 {
			resp.Reclaimable = reclaimable
		}
	}
	return resp, nil
}

// docChangesToProto is the wire form of a change list — the counterpart of
// docChangesFromProto.
func docChangesToProto(in []types.DocChange) []*pb.DocChange {
	if len(in) == 0 {
		return nil
	}
	out := make([]*pb.DocChange, len(in))
	for i, c := range in {
		op := pb.ChangeOp_CHANGE_OP_ADD
		switch c.Op {
		case types.ChangeOpDelete:
			op = pb.ChangeOp_CHANGE_OP_DELETE
		case types.ChangeOpUpdate:
			op = pb.ChangeOp_CHANGE_OP_UPDATE
		}
		out[i] = &pb.DocChange{Op: op, DocId: c.DocID, Content: c.Content}
	}
	return out
}

// docChangesFromProto converts the wire form of a change list into the storage
// layer's own type.
func docChangesFromProto(in []*pb.DocChange) []types.DocChange {
	if len(in) == 0 {
		return nil
	}
	out := make([]types.DocChange, len(in))
	for i, c := range in {
		op := types.ChangeOpAdd
		switch c.GetOp() {
		case pb.ChangeOp_CHANGE_OP_DELETE:
			op = types.ChangeOpDelete
		case pb.ChangeOp_CHANGE_OP_UPDATE:
			op = types.ChangeOpUpdate
		}
		out[i] = types.DocChange{Op: op, DocID: c.GetDocId(), Content: c.GetContent()}
	}
	return out
}

// VersionPresence implements DataSyncService.VersionPresence: it reports
// whether this node holds (kbID, versionID)'s data. The control layer asks
// every candidate replica, and a version none of them holds is one whose data
// never landed — as opposed to one that is merely waiting for its index build
// (Stratum_设计文档v13.md §7.12 step ①).
func (h *PushHandler) VersionPresence(ctx context.Context, req *pb.VersionPresenceRequest) (*pb.VersionPresenceResponse, error) {
	// Presence is answered from the local document list, so a node without one
	// cannot answer it at all. That has to be an error and not "present: false":
	// the caller uses a negative answer to conclude a version's data never
	// landed anywhere (§7.12 step ①), and "I hold no data" is not that claim.
	//
	// A control node is exactly such a node — it serves this service for the
	// cursor reports it owns as leader, so a request here is possible rather
	// than hypothetical, and without this it was a nil dereference that took
	// the process down.
	if h.follower == nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"sync: VersionPresence: this node holds no storage")
	}
	kbID, versionID := req.GetKnowledgeBaseId(), req.GetVersionId()
	docIDs, err := h.follower.versionDoc.ListDocIDs(ctx, kbID, versionID)
	if err != nil {
		return nil, fmt.Errorf("sync: presence check (%s v%d): %w", kbID, versionID, err)
	}
	return &pb.VersionPresenceResponse{
		Present: len(docIDs) > 0,
		NodeId:  h.nodeID,
	}, nil
}

// PushIndexData implements DataSyncService.PushIndexData: it receives an index
// built by another node and installs it, so this replica serves the version
// without building anything (Stratum_设计文档v13.md §8.4).
//
// The stream is buffered in memory rather than written through: installation
// has to be atomic (see index.InstallIndex), and a half-installed index is the
// one state worth paying memory to avoid. Indexes are tens of megabytes, not
// gigabytes.
func (h *PushHandler) PushIndexData(stream pb.DataSyncService_PushIndexDataServer) error {
	// Same reason as PushVersionData: installing an index needs a local index
	// directory, which a control node does not have.
	if h.installer == nil {
		return status.Errorf(codes.Unimplemented, "sync: PushIndexData: this node holds no storage")
	}
	var (
		kbID       string
		versionID  int64
		indexBuf   bytes.Buffer
		sidecarBuf bytes.Buffer
	)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		kbID = chunk.GetKnowledgeBaseId()
		versionID = chunk.GetVersionId()
		if chunk.GetSidecar() {
			sidecarBuf.Write(chunk.GetData())
		} else {
			indexBuf.Write(chunk.GetData())
		}
		if chunk.GetLast() {
			break
		}
	}
	if kbID == "" {
		return status.Error(codes.InvalidArgument, "sync: PushIndexData: no knowledge base id in the stream")
	}
	if h.installer == nil {
		return status.Error(codes.FailedPrecondition, "sync: PushIndexData: index install is not wired on this node")
	}
	if err := h.installer.InstallIndex(stream.Context(), kbID, versionID, indexBuf.Bytes(), sidecarBuf.Bytes()); err != nil {
		return status.Errorf(codes.Internal, "sync: PushIndexData(%s v%d): %v", kbID, versionID, err)
	}
	return stream.SendAndClose(&pb.PushIndexDataResponse{NodeId: h.nodeID})
}

// DeleteVersionData implements DataSyncService.DeleteVersionData: it reclaims
// the version's physical data here, on the control layer's instruction
// (Stratum_设计文档v13.md §10.6). Without a wired dropper it answers
// dropped=false instead of failing: a node with nothing to drop is a normal
// case, not an error.
func (h *PushHandler) DeleteVersionData(ctx context.Context, req *pb.DeleteVersionDataRequest) (*pb.DeleteVersionDataResponse, error) {
	if h.dropper == nil {
		return &pb.DeleteVersionDataResponse{NodeId: h.nodeID}, nil
	}
	kbID, versionID := req.GetKnowledgeBaseId(), req.GetVersionId()
	if err := h.dropper.DropVersionStorage(ctx, kbID, versionID); err != nil {
		return nil, status.Errorf(codes.Internal, "sync: DeleteVersionData(%s v%d): %v", kbID, versionID, err)
	}
	// The version's data is gone here, so any §8.5 announcement pointing at
	// this node is stale: drop it, or a later pull would aim at a peer that no
	// longer holds the version.
	if h.dataSources != nil {
		h.dataSources.ForgetVersion(kbID, versionID)
	}
	return &pb.DeleteVersionDataResponse{Dropped: true, NodeId: h.nodeID}, nil
}

// LocalVersion implements DataSyncService.LocalVersion: it reports how far
// this node's contiguous history reaches for the knowledge base, which is what
// lets a node that is behind pick a peer able to fill its gap
// (Stratum_设计文档v13.md §7.6).
func (h *PushHandler) LocalVersion(_ context.Context, req *pb.LocalVersionRequest) (*pb.LocalVersionResponse, error) {
	var version int64
	if h.localVersion != nil {
		version = h.localVersion.LocalVersionOf(req.GetKnowledgeBaseId())
	}
	return &pb.LocalVersionResponse{Version: version, NodeId: h.nodeID}, nil
}
