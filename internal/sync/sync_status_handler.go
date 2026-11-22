package sync

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
)

// Heartbeat receives one node's periodic "I am alive" and, if this node is the control
// leader, records it.
//
// A non-leader answers accepted=false rather than an error: the reporter's next interval
// re-resolves the leader (§7.13.1's pattern), and an error would have it retry the same
// wrong address. accepted=false is deliberately a success on the wire — "you reached the
// wrong node" is not a transport failure.
func (h *PushHandler) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if h.heartbeats == nil || h.isLeader == nil || !h.isLeader() {
		return &pb.HeartbeatResponse{Accepted: false, NodeId: h.nodeID}, nil
	}
	h.heartbeats.Record(req.GetNodeId())

	resp := &pb.HeartbeatResponse{Accepted: true, NodeId: h.nodeID}
	// Carry back the chain generation, not the tails: this answer goes out on every
	// heartbeat of every node, so it must stay the same size however many knowledge bases
	// there are. A node pulls the tails only when this changes (GetChainTails).
	if h.chainTails != nil {
		if source, ok := h.chainTails.(interface{ StorageDirectoryRevision() uint64 }); ok {
			resp.DirectoryRevision = source.StorageDirectoryRevision()
		}
		if term, seq, ok := h.chainTails.ChainGeneration(); ok {
			resp.ChainGeneration = &pb.ChainGeneration{Term: term, Seq: seq}
		}
	}
	return resp, nil
}

// GetChainTails answers a node whose last pull is at a different chain generation.
//
// A non-leader answers accepted=false, like Heartbeat, because a follower's
// metadata can lag and a lagging tail would tell a node it is fine when it is not.
func (h *PushHandler) GetChainTails(ctx context.Context, req *pb.GetChainTailsRequest) (*pb.GetChainTailsResponse, error) {
	if h.chainTails == nil || h.isLeader == nil || !h.isLeader() {
		return &pb.GetChainTailsResponse{Accepted: false}, nil
	}
	// The generation first: the tails read after it are at least as new as it says, so a
	// change landing between the two reads costs one extra pull rather than a missed one.
	resp := &pb.GetChainTailsResponse{Accepted: true}
	if term, seq, ok := h.chainTails.ChainGeneration(); ok {
		resp.Generation = &pb.ChainGeneration{Term: term, Seq: seq}
	}
	tails, err := h.chainTails.ChainTails(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "sync: GetChainTails: %v", err)
	}
	resp.ChainTails = tails
	if h.placements != nil {
		if req.GetNodeId() <= 0 {
			return nil, status.Error(codes.InvalidArgument, "sync: node identity is required")
		}
		reader, ok := h.chainTails.(storage.Reader)
		if !ok {
			return nil, stratumerrors.ToGRPCStatus(stratumerrors.ErrPlacementUnknown)
		}
		dir, err := reader.StorageDirectory(ctx)
		if err != nil {
			return nil, stratumerrors.ToGRPCStatus(err)
		}
		resp.DirectoryRevision = dir.Revision
		filtered := make(map[string]int64)
		for kb, tail := range tails {
			if p, err := dir.Lookup(kb); err == nil && p.Contains(req.GetNodeId()) {
				filtered[kb] = tail
			}
		}
		resp.ChainTails = filtered
	}
	return resp, nil
}

// DocChangesToProto is the wire form of a change list — the counterpart of
// docChangesFromProto.
//
// Exported because the storage layer encodes the same payload: the coordinator
// dispatcher ships a version's changes to the peer that will coordinate it
// (plane.CoordinatorDispatcher), so the two encodings have to stay one function
// rather than two that can drift.
func DocChangesToProto(in []types.DocChange) []*pb.DocChange {
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
// (docs/参考/历史/Stratum_设计文档v13.md §7.12 step ①).
func (h *PushHandler) VersionPresence(ctx context.Context, req *pb.VersionPresenceRequest) (*pb.VersionPresenceResponse, error) {
	if _, err := h.bindPlacement(ctx, req.GetKnowledgeBaseId()); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
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

// LocalVersion implements DataSyncService.LocalVersion: it reports how far
// this node's contiguous history reaches for the knowledge base, which is what
// lets a node that is behind pick a peer able to fill its gap
// (docs/参考/历史/Stratum_设计文档v13.md §7.6).
func (h *PushHandler) LocalVersion(ctx context.Context, req *pb.LocalVersionRequest) (*pb.LocalVersionResponse, error) {
	if _, err := h.bindPlacement(ctx, req.GetKnowledgeBaseId()); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	var version int64
	if h.localVersion != nil {
		version = h.localVersion.LocalVersionOf(req.GetKnowledgeBaseId())
	}
	return &pb.LocalVersionResponse{Version: version, NodeId: h.nodeID}, nil
}
