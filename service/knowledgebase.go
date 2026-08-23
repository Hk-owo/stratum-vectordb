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
	_ = s.raftNode.ProposeUpdateVersionStatus(ctx, versionID, types.IndexStatusReady)

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

	versionID, err := s.writeCoord.Execute(ctx, req.KnowledgeBaseId, req.ParentVersionId, changes)
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
// Marks the version (and, recursively, every descendant version) within
// the knowledge base as Deleting, then launches the asynchronous cleanup
// (index discard, VersionDocList / DocStore removal, metadata removal).
// All constraint checks — the version exists and belongs to the KB, is not
// the active version, and no version in the recursive subtree is PENDING —
// are enforced deterministically in the Raft state machine's apply phase,
// so this method performs no additional validation.
func (s *KnowledgeBaseServiceImpl) DeleteVersion(ctx context.Context, req *pb.DeleteVersionRequest) (*pb.DeleteVersionResponse, error) {
	if err := s.raftNode.ProposeMarkVersionDeleting(ctx, req.KnowledgeBaseId, req.VersionId); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Launch async cleanup. The coordinator re-discovers every Deleting
	// version of the KB (including any left over from a previous crashed
	// cleanup) and is idempotent end-to-end.
	go func() {
		_ = s.deleteVersionCoord.Execute(context.Background(), req.KnowledgeBaseId)
	}()

	return &pb.DeleteVersionResponse{Success: true}, nil
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
