// Package service implements Stratum's three external gRPC services
// (KnowledgeBaseService, QueryService, AdminService), per
// Stratum_接口设计v9.md and Stratum_实现顺序.md Phase 6.
//
// Each service is a thin layer: validate inputs, convert between proto
// messages and internal types, delegate to the coordinator/raft/index
// layer, convert errors to gRPC status codes via errors.ToGRPCStatus, and
// return responses. No business logic lives here.
package service

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/coordinator"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/raft"
	"stratum/internal/types"
)

// KnowledgeBaseServiceImpl implements pb.KnowledgeBaseServiceServer.
type KnowledgeBaseServiceImpl struct {
	pb.UnimplementedKnowledgeBaseServiceServer

	raftNode           raft.RaftNode
	writeCoord         coordinator.WriteCoordinator
	deleteCoord        coordinator.DeleteCoordinator
	deleteVersionCoord coordinator.DeleteVersionCoordinator

	// versionHolders is the control leader's data-version aggregate, exposed to
	// the service station's route table (storage-coordination-and-service-station
	// -design.md §3.1). Optional: without it the RPC answers known=false and the
	// station keeps its previous behaviour.
	versionHolders VersionHolderSource
}

// VersionHolder is one node that reported holding a version, with the address it
// reported for itself. It mirrors plane's holder rather than importing it: this
// is the control-layer service, and its caller is the station.
type VersionHolder struct {
	NodeID  int64
	Address string
}

// VersionHolderSource answers "which nodes reported holding kbID at or past
// versionID". ok=false means the answering node is not the control leader, so an
// empty list is "I have heard from nobody", never "nobody has it" — the two
// justify opposite actions, since the second would license deleting data (§10.6).
//
// *plane.LocalControlPlane implements it; declared narrow here so this package
// does not depend on plane.
type VersionHolderSource interface {
	DataVersionHolders(kbID string, versionID int64) ([]VersionHolder, bool)
}

// SetVersionHolderSource wires the aggregate the holders RPC answers from.
func (s *KnowledgeBaseServiceImpl) SetVersionHolderSource(src VersionHolderSource) {
	s.versionHolders = src
}

// GetDataVersionHolders answers the station's route-table question: which nodes
// reported a contiguous cursor reaching versionID for kbID.
//
// Deliberately not an error when this node is not the leader: it answers
// known=false, which the station reads as "ask someone else" rather than as a
// fact about where data lives. A station treating a follower's empty list as
// "nobody has it" would route every query away from nodes that do hold it.
func (s *KnowledgeBaseServiceImpl) GetDataVersionHolders(ctx context.Context, req *pb.GetDataVersionHoldersRequest) (*pb.GetDataVersionHoldersResponse, error) {
	if s.versionHolders == nil {
		return &pb.GetDataVersionHoldersResponse{}, nil
	}
	holders, ok := s.versionHolders.DataVersionHolders(req.GetKnowledgeBaseId(), req.GetVersionId())
	if !ok {
		return &pb.GetDataVersionHoldersResponse{}, nil
	}
	resp := &pb.GetDataVersionHoldersResponse{
		Known:   true,
		Holders: make([]*pb.DataVersionHolder, 0, len(holders)),
	}
	for _, h := range holders {
		resp.Holders = append(resp.Holders, &pb.DataVersionHolder{NodeId: h.NodeID, Address: h.Address})
	}
	return resp, nil
}

// NewKnowledgeBaseService constructs a KnowledgeBaseServiceImpl.
func NewKnowledgeBaseService(
	rn raft.RaftNode,
	wc coordinator.WriteCoordinator,
	dc coordinator.DeleteCoordinator,
	dvc coordinator.DeleteVersionCoordinator,
) *KnowledgeBaseServiceImpl {
	return &KnowledgeBaseServiceImpl{
		raftNode:           rn,
		writeCoord:         wc,
		deleteCoord:        dc,
		deleteVersionCoord: dvc,
	}
}

// CreateKnowledgeBase implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) CreateKnowledgeBase(ctx context.Context, req *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
	kbID := generateKBID(req.Name)

	indexType := req.IndexType.String()
	if req.IndexType == pb.IndexType_INDEX_TYPE_HNSW || indexType == "" || indexType == "INDEX_TYPE_HNSW" {
		indexType = "HNSW"
	}
	similarity := req.Similarity.String()
	if similarity == "" || similarity == "SIMILARITY_COSINE" {
		similarity = "COSINE"
	}
	quantizer := quantizerFromProto(req.Quantizer)
	pqM := int(req.PqM)
	if quantizer == "PQ" && pqM <= 0 {
		pqM = 96 // default
	}
	pqNBits := int(req.PqNbits)
	if quantizer == "PQ" && pqNBits <= 0 {
		pqNBits = 8 // default
	}

	windowSize := int(req.ChunkWindowSize)
	if windowSize <= 0 {
		windowSize = 512 // default
	}
	overlapSize := int(req.ChunkOverlapSize)
	if overlapSize < 0 {
		overlapSize = 64
	}

	kb := types.KnowledgeBaseMeta{
		KBID:             kbID,
		Name:             req.Name,
		ChunkWindowSize:  windowSize,
		ChunkOverlapSize: overlapSize,
		IndexType:        indexType,
		Similarity:       similarity,
		QuantizerType:    quantizer,
		QuantizerPQM:     pqM,
		QuantizerPQNBits: pqNBits,
		EmbedConfig: types.EmbedConfig{
			ServiceAddr: req.EmbedConfig.GetServiceAddr(),
			ModelID:     req.EmbedConfig.GetModelId(),
		},
		Status: types.KBStatusActive,
	}

	if err := s.raftNode.ProposeCreateKB(ctx, kb); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Create the initial version.
	versionID, err := s.raftNode.ProposeCreateVersion(ctx, kbID, 0)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Mark the initial version as READY (there are no chunks to index).
	// nodeID 0: settled by the control layer, not reported by a replica.
	_ = s.raftNode.ProposeUpdateVersionStatus(ctx, versionID, types.IndexStatusReady, 0)

	// Set the active version. Since the RaftNode interface has no explicit
	// "set active version" RPC outside of Rollback, we use Rollback to set it.
	_ = s.raftNode.ProposeRollback(ctx, kbID, versionID)

	return &pb.CreateKnowledgeBaseResponse{
		KnowledgeBaseId:  kbID,
		InitialVersionId: versionID,
	}, nil
}

// DeleteKnowledgeBase implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) DeleteKnowledgeBase(ctx context.Context, req *pb.DeleteKnowledgeBaseRequest) (*pb.DeleteKnowledgeBaseResponse, error) {
	kbID := req.KnowledgeBaseId

	// Mark the KB as deleting.
	if err := s.raftNode.ProposeMarkKBDeleting(ctx, kbID); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Launch async cleanup.
	go func() {
		_ = s.deleteCoord.Execute(context.Background(), kbID)
	}()

	return &pb.DeleteKnowledgeBaseResponse{Success: true}, nil
}

// CreateVersion implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) CreateVersion(ctx context.Context, req *pb.CreateVersionRequest) (*pb.CreateVersionResponse, error) {
	changes := make([]types.DocChange, len(req.Changes))
	for i, c := range req.Changes {
		op := types.ChangeOpAdd
		switch c.Op {
		case pb.ChangeOp_CHANGE_OP_DELETE:
			op = types.ChangeOpDelete
		case pb.ChangeOp_CHANGE_OP_UPDATE:
			op = types.ChangeOpUpdate
		}
		changes[i] = types.DocChange{
			Op:      op,
			DocID:   c.DocId,
			Content: c.Content,
		}
	}

	versionID, err := s.writeCoord.Execute(ctx, req.KnowledgeBaseId, req.ParentVersionId, changes, req.ClientRequestId)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	return &pb.CreateVersionResponse{VersionId: versionID}, nil
}

// ListVersions implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) ListVersions(ctx context.Context, req *pb.ListVersionsRequest) (*pb.ListVersionsResponse, error) {
	versions, err := s.raftNode.ListVersions(ctx, req.KnowledgeBaseId)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	out := make([]*pb.VersionInfo, len(versions))
	for i, v := range versions {
		out[i] = &pb.VersionInfo{
			VersionId:       v.VersionID,
			ParentVersionId: v.ParentVersionID,
			CreatedAt:       v.CreatedAt,
			IndexStatus:     pb.IndexStatus(v.IndexStatus),
			Deleting:        v.Deleting,
			// The data side travels alongside the index side (§10.1b): callers
			// that only look at index_status cannot tell "the data never landed"
			// from "everything is fine".
			DataStatus: pb.DataStatus(v.DataStatus),
		}
	}
	return &pb.ListVersionsResponse{Versions: out}, nil
}

// RollbackVersion implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) RollbackVersion(ctx context.Context, req *pb.RollbackVersionRequest) (*pb.RollbackVersionResponse, error) {
	// Validate target version exists and is READY.
	versions, err := s.raftNode.ListVersions(ctx, req.KnowledgeBaseId)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	found := false
	for _, v := range versions {
		if v.VersionID == req.TargetVersionId {
			found = true
			if v.IndexStatus == types.IndexStatusPending {
				return nil, status.Error(codes.FailedPrecondition, "target version is PENDING")
			}
			if v.IndexStatus == types.IndexStatusFailed {
				return nil, status.Error(codes.FailedPrecondition, "target version index is FAILED")
			}
			break
		}
	}
	if !found {
		return nil, stratumerrors.ToGRPCStatus(stratumerrors.ErrVersionNotFound)
	}

	if err := s.raftNode.ProposeRollback(ctx, req.KnowledgeBaseId, req.TargetVersionId); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	return &pb.RollbackVersionResponse{Success: true}, nil
}

// DeleteVersion implements KnowledgeBaseServiceServer.
//
// Marks the version set selected by req.Mode relative to req.VersionId as
// Deleting, then launches the asynchronous cleanup (index discard,
// VersionDocList / DocStore removal, metadata removal). The three modes
// cover "remove this subtree" (default), "remove just this middle version,
// splicing its children onto its parent", and "remove every preceding
// version, making this one the new base".
//
// All constraint checks — the version exists and belongs to the KB, and no
// version in the selected set is the active version or still PENDING — are
// enforced deterministically in the Raft state machine's apply phase, so
// this method performs no additional validation. The response echoes the
// exact set of versions marked for deletion.
func (s *KnowledgeBaseServiceImpl) DeleteVersion(ctx context.Context, req *pb.DeleteVersionRequest) (*pb.DeleteVersionResponse, error) {
	mode, err := versionDeleteModeFromProto(req.Mode)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	deleted, err := s.raftNode.ProposeMarkVersionDeleting(ctx, req.KnowledgeBaseId, req.VersionId, mode)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Launch async cleanup. The coordinator re-discovers every Deleting
	// version of the KB (including any left over from a previous crashed
	// cleanup) and is idempotent end-to-end.
	go func() {
		_ = s.deleteVersionCoord.Execute(context.Background(), req.KnowledgeBaseId)
	}()

	return &pb.DeleteVersionResponse{Success: true, DeletedVersionIds: deleted}, nil
}

// versionDeleteModeFromProto maps the wire enum onto the internal type.
// The zero value (VERSION_DELETE_MODE_SUBTREE, also the value an unset
// field carries) preserves the historical DeleteVersion semantics.
//
// An unrecognized value is rejected rather than silently downgraded to
// SUBTREE: the caller clearly meant something specific, and guessing wrong
// would turn a mistyped mode into a destructive "delete the whole subtree".
func versionDeleteModeFromProto(m pb.VersionDeleteMode) (types.VersionDeleteMode, error) {
	switch m {
	case pb.VersionDeleteMode_VERSION_DELETE_MODE_SUBTREE:
		return types.VersionDeleteSubtree, nil
	case pb.VersionDeleteMode_VERSION_DELETE_MODE_SINGLE:
		return types.VersionDeleteSingle, nil
	case pb.VersionDeleteMode_VERSION_DELETE_MODE_ANCESTORS:
		return types.VersionDeleteAncestors, nil
	default:
		return types.VersionDeleteSubtree, fmt.Errorf("unknown version delete mode %d: %w", m, stratumerrors.ErrInvalidArgument)
	}
}

// ListKnowledgeBases implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) ListKnowledgeBases(ctx context.Context, _ *pb.ListKnowledgeBasesRequest) (*pb.ListKnowledgeBasesResponse, error) {
	kbs, err := s.raftNode.ListKnowledgeBases(ctx)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	out := make([]*pb.KnowledgeBaseInfo, 0, len(kbs))
	for _, kb := range kbs {
		out = append(out, kbToProto(kb))
	}
	return &pb.ListKnowledgeBasesResponse{KnowledgeBases: out}, nil
}

// GetKnowledgeBase implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) GetKnowledgeBase(ctx context.Context, req *pb.GetKnowledgeBaseRequest) (*pb.GetKnowledgeBaseResponse, error) {
	kb, err := s.raftNode.GetKB(ctx, req.KnowledgeBaseId)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	return &pb.GetKnowledgeBaseResponse{KnowledgeBase: kbToProto(kb)}, nil
}

// kbToProto converts internal knowledge base metadata to its console-facing
// proto representation.
func kbToProto(kb types.KnowledgeBaseMeta) *pb.KnowledgeBaseInfo {
	return &pb.KnowledgeBaseInfo{
		KnowledgeBaseId:  kb.KBID,
		Name:             kb.Name,
		ChunkWindowSize:  int32(kb.ChunkWindowSize),
		ChunkOverlapSize: int32(kb.ChunkOverlapSize),
		IndexType:        indexTypeToProto(kb.IndexType),
		Similarity:       similarityToProto(kb.Similarity),
		Quantizer:        quantizerToProto(kb.QuantizerType),
		PqM:              int32(kb.QuantizerPQM),
		PqNbits:          int32(kb.QuantizerPQNBits),
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: kb.EmbedConfig.ServiceAddr,
			ModelId:     kb.EmbedConfig.ModelID,
		},
		ActiveVersionId: kb.ActiveVersionID,
		Status:          kbStatusToProto(kb.Status),
	}
}

func indexTypeToProto(s string) pb.IndexType {
	switch s {
	case "IVF":
		return pb.IndexType_INDEX_TYPE_IVF
	case "FLAT":
		return pb.IndexType_INDEX_TYPE_FLAT
	default:
		return pb.IndexType_INDEX_TYPE_HNSW
	}
}

func similarityToProto(s string) pb.Similarity {
	switch s {
	case "EUCLIDEAN":
		return pb.Similarity_SIMILARITY_EUCLIDEAN
	case "INNER_PRODUCT":
		return pb.Similarity_SIMILARITY_INNER_PRODUCT
	default:
		return pb.Similarity_SIMILARITY_COSINE
	}
}

// quantizerFromProto maps the console-facing proto enum to the short
// internal name stored in KnowledgeBaseMeta ("" / "QUANTIZER_OFF" ⇒ OFF).
func quantizerFromProto(q pb.QuantizerType) string {
	switch q {
	case pb.QuantizerType_QUANTIZER_SQ8:
		return "SQ8"
	case pb.QuantizerType_QUANTIZER_SQ_BF16:
		return "SQ_BF16"
	case pb.QuantizerType_QUANTIZER_SQ_FP16:
		return "SQ_FP16"
	case pb.QuantizerType_QUANTIZER_PQ:
		return "PQ"
	default:
		return "OFF"
	}
}

func quantizerToProto(s string) pb.QuantizerType {
	switch s {
	case "SQ8":
		return pb.QuantizerType_QUANTIZER_SQ8
	case "SQ_BF16":
		return pb.QuantizerType_QUANTIZER_SQ_BF16
	case "SQ_FP16":
		return pb.QuantizerType_QUANTIZER_SQ_FP16
	case "PQ":
		return pb.QuantizerType_QUANTIZER_PQ
	default:
		return pb.QuantizerType_QUANTIZER_OFF
	}
}

func kbStatusToProto(s types.KBStatus) pb.KBStatus {
	switch s {
	case types.KBStatusDeleting:
		return pb.KBStatus_KB_STATUS_DELETING
	case types.KBStatusDeleteFailed:
		return pb.KBStatus_KB_STATUS_DELETE_FAILED
	default:
		return pb.KBStatus_KB_STATUS_ACTIVE
	}
}

// generateKBID produces a unique knowledge base ID. Uses a simple
// counter-based approach; in production, a UUID library would be used,
// but the design docs do not specify a particular ID scheme, and a
// short ID is friendlier for debugging/ops. The name is folded in for
// human readability.
var kbIDCounter int

func generateKBID(name string) string {
	kbIDCounter++
	short := name
	if len(short) > 20 {
		short = short[:20]
	}
	return fmt.Sprintf("%s-%d", short, kbIDCounter)
}

// Ensure interface compliance.
var _ pb.KnowledgeBaseServiceServer = (*KnowledgeBaseServiceImpl)(nil)
