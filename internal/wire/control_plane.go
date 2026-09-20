// Package wire maps the gRPC proto messages onto Stratum's domain types.
//
// Both directions belong here. The service layer converts domain → proto when
// it answers a call; the storage layer converts proto → domain when it reads
// replicated metadata back through RemoteRaftNode. Keeping the mirror in one
// package is what stops a field from being added on one side and silently
// dropped on the other.
//
// See Stratum_设计文档v13.md §11 阶段 ④ (存储集群独立进程).
package wire

import (
	"fmt"

	pb "stratum/api/proto/stratum"
	"stratum/internal/types"
)

// KnowledgeBaseFromInfo converts the console-facing knowledge base metadata
// back into the domain form the storage layer works with.
//
// Every field the storage layer reads is mapped explicitly — the quantizer
// configuration above all, which the index build forwards to the vecstore and
// which a default would silently turn into a full-precision build.
//
// The round trip is not byte-identical, and does not need to be: the proto
// carries an enum where the domain carries a short string, so a knowledge base
// whose IndexType/Similarity was left unset comes back as the proto default's
// name ("HNSW" / "COSINE") rather than "". Those spellings are equivalent to
// the storage layer, which resolves the same default.
func KnowledgeBaseFromInfo(info *pb.KnowledgeBaseInfo) (types.KnowledgeBaseMeta, error) {
	if info == nil {
		return types.KnowledgeBaseMeta{}, fmt.Errorf("wire: knowledge base info is nil")
	}
	return types.KnowledgeBaseMeta{
		KBID:             info.GetKnowledgeBaseId(),
		Name:             info.GetName(),
		ChunkWindowSize:  int(info.GetChunkWindowSize()),
		ChunkOverlapSize: int(info.GetChunkOverlapSize()),
		IndexType:        indexTypeFromProto(info.GetIndexType()),
		Similarity:       similarityFromProto(info.GetSimilarity()),
		QuantizerType:    QuantizerFromProto(info.GetQuantizer()),
		QuantizerPQM:     int(info.GetPqM()),
		QuantizerPQNBits: int(info.GetPqNbits()),
		EmbedConfig: types.EmbedConfig{
			ServiceAddr: info.GetEmbedConfig().GetServiceAddr(),
			ModelID:     info.GetEmbedConfig().GetModelId(),
		},
		ActiveVersionID: info.GetActiveVersionId(),
		Status:          kbStatusFromProto(info.GetStatus()),
	}, nil
}

// VersionFromInfo converts one version's metadata. kbID is threaded in rather
// than read off the message: it is not on the wire, and the caller already knows
// which knowledge base it asked about.
//
// Every field VersionInfo carries is carried across — the digest especially. A
// storage node decides whether it holds a version from this metadata alone, and
// "durable with no digest" is exactly how a version whose digest was never committed
// looks, as well as how an empty version looks. An empty string means "unknown", the
// empty set's own digest means "there is nothing to hold", and that difference decides
// whether a restarting node's cursor may step over the version.
func VersionFromInfo(kbID string, info *pb.VersionInfo) types.VersionMeta {
	return types.VersionMeta{
		VersionID:       info.GetVersionId(),
		ParentVersionID: info.GetParentVersionId(),
		KBID:            kbID,
		CreatedAt:       info.GetCreatedAt(),
		IndexStatus:     indexStatusFromProto(info.GetIndexStatus()),
		// Both of these are on the wire and have to be carried across. Left at their
		// zero values they do not read as "absent" downstream — they read as positive
		// facts:
		//
		//   - a dropped DataStatus is PENDING, and PENDING is what a recovering node
		//     consults to conclude it does not hold a version
		//     (LocalDataPlane.holdsVersionLocally), which is what lets its cursor step
		//     over one. Measured: every storage node read a DURABLE empty version as
		//     PENDING, so each restart reported cursor 0 for the whole knowledge base
		//     and no cursor promotion could ever move it.
		//   - a dropped Deleting is "not being deleted", the unsafe direction for
		//     anything that reclaims data.
		//
		DataStatus:   dataStatusFromProto(info.GetDataStatus()),
		Deleting:     info.GetDeleting(),
		DocIDSetHash: info.GetDocIdSetHash(),
		// IndexReadyNodes is what §8.6(d)'s rolling cleanup counts before a
		// storage node takes itself out of service. It is a fact the CONTROL layer
		// aggregates, and this is the only route by which a storage node can read
		// it — dropping it here silently zeroed that count and made collection
		// impossible in every split deployment.
		IndexReadyNodes: info.GetIndexReadyNodes(),
	}
}

// dataStatusFromProto mirrors indexStatusFromProto for the DATA side, which is a
// separate state on the same version (Stratum_设计文档v13.md §10.1b). PENDING is the
// default because it is the conservative one: "not known to be durable" must never
// be read as "durable".
func dataStatusFromProto(s pb.DataStatus) types.DataStatus {
	switch s {
	case pb.DataStatus_DATA_STATUS_DURABLE:
		return types.DataStatusDurable
	case pb.DataStatus_DATA_STATUS_FAILED_PERMANENT:
		return types.DataStatusFailedPermanent
	default:
		return types.DataStatusPending
	}
}

// VersionsFromInfos converts a whole version list.
func VersionsFromInfos(kbID string, infos []*pb.VersionInfo) []types.VersionMeta {
	out := make([]types.VersionMeta, 0, len(infos))
	for _, info := range infos {
		out = append(out, VersionFromInfo(kbID, info))
	}
	return out
}

// ClusterStatusFromProto converts the admin view of Raft connectivity.
func ClusterStatusFromProto(resp *pb.GetClusterStatusResponse) types.ClusterStatus {
	return types.ClusterStatus{
		HasLeader:   resp.GetHasLeader(),
		MemberCount: int(resp.GetMemberCount()),
		LeaderID:    resp.GetLeaderId(),
	}
}

// QuantizerFromProto maps the console-facing quantizer enum back to the short
// internal name stored in KnowledgeBaseMeta ("" / QUANTIZER_OFF ⇒ OFF).
//
// Exported because the knowledge base service performs the same conversion when
// it accepts a create request, and one spelling of this table is enough.
func QuantizerFromProto(q pb.QuantizerType) string {
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

// The remaining enum mirrors stay unexported: only this package needs them, and
// they resolve to the plain strings the state machine committed.
//
// The index statuses happen to line up numerically with their proto
// counterparts today. They are still mapped by hand so that renumbering either
// side cannot silently reclassify a version.

func indexTypeFromProto(t pb.IndexType) string {
	switch t {
	case pb.IndexType_INDEX_TYPE_IVF:
		return "IVF"
	case pb.IndexType_INDEX_TYPE_FLAT:
		return "FLAT"
	default:
		return "HNSW"
	}
}

func similarityFromProto(s pb.Similarity) string {
	switch s {
	case pb.Similarity_SIMILARITY_EUCLIDEAN:
		return "EUCLIDEAN"
	case pb.Similarity_SIMILARITY_INNER_PRODUCT:
		return "INNER_PRODUCT"
	default:
		return "COSINE"
	}
}

func kbStatusFromProto(s pb.KBStatus) types.KBStatus {
	switch s {
	case pb.KBStatus_KB_STATUS_DELETING:
		return types.KBStatusDeleting
	case pb.KBStatus_KB_STATUS_DELETE_FAILED:
		return types.KBStatusDeleteFailed
	default:
		return types.KBStatusActive
	}
}

func indexStatusFromProto(s pb.IndexStatus) types.IndexStatus {
	switch s {
	case pb.IndexStatus_INDEX_STATUS_READY:
		return types.IndexStatusReady
	case pb.IndexStatus_INDEX_STATUS_FAILED:
		return types.IndexStatusFailed
	case pb.IndexStatus_INDEX_STATUS_FAILED_PERMANENT:
		return types.IndexStatusFailedPermanent
	default:
		return types.IndexStatusPending
	}
}
