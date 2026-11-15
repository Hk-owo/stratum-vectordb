package wire

import (
	"maps"
	"sort"

	pb "stratum/api/proto/stratum"
	"stratum/internal/storage"
)

func StorageDirectoryToProto(d storage.Directory) *pb.GetStorageDirectoryResponse {
	out := &pb.GetStorageDirectoryResponse{Revision: d.Revision, ActiveVersions: maps.Clone(d.ActiveVersions)}
	if d.ServiceBlocks != nil {
		out.ServiceBlocks = make(map[int64]*pb.StorageServiceBlock, len(d.ServiceBlocks))
		for id, b := range d.ServiceBlocks {
			out.ServiceBlocks[id] = &pb.StorageServiceBlock{OperationId: b.OperationID, Revision: b.Revision, Revoked: b.Revoked}
		}
	}
	for _, c := range d.Cleanups {
		out.Cleanups = append(out.Cleanups, &pb.StorageCleanupEntry{Id: c.ID, KnowledgeBaseId: c.KBID, OperationId: c.OperationID, NodeId: c.NodeID, Epoch: c.Epoch, ArtifactsOnly: c.ArtifactsOnly, Done: c.Done})
	}
	sort.Slice(out.Cleanups, func(i, j int) bool { return out.Cleanups[i].Id < out.Cleanups[j].Id })
	for _, n := range d.Nodes {
		out.Nodes = append(out.Nodes, &pb.StorageNodeEntry{NodeId: n.ID, Address: n.Addr, ManagementState: string(n.State)})
	}
	for _, p := range d.Placements {
		out.Placements = append(out.Placements, &pb.KBPlacementEntry{
			KnowledgeBaseId: p.KBID, PlacementEpoch: p.Epoch, ReplicaIds: append([]int64(nil), p.ReplicaIDs...), RequiredReplicas: int32(p.RequiredReplicas), OperationId: p.OperationID,
		})
	}
	for _, t := range d.Tombstones {
		out.Tombstones = append(out.Tombstones, &pb.KBStorageTombstone{
			KnowledgeBaseId: t.KBID, DeletionEpoch: t.Epoch, CleanupNodeIds: append([]int64(nil), t.CleanupIDs...),
		})
	}
	for _, m := range d.Migrations {
		out.Migrations = append(out.Migrations, &pb.KBMigrationEntry{Install: migrationInstallToProto(m.Install), Serve: migrationServeToProto(m.Serve), Stage: migrationStageToProto(m.Stage), Fault: m.Request.Fault, FailedBlock: serviceBlockToProto(m.FailedBlock), OperationId: m.Request.OperationID, KnowledgeBaseId: m.Request.KBID, ExpectedEpoch: m.Request.ExpectedEpoch, FromNodeId: m.Request.FromID, ToNodeId: m.Request.ToID, OldPlacement: &pb.KBPlacementEntry{KnowledgeBaseId: m.Old.KBID, PlacementEpoch: m.Old.Epoch, ReplicaIds: append([]int64(nil), m.Old.ReplicaIDs...), RequiredReplicas: int32(m.Old.RequiredReplicas), OperationId: m.Old.OperationID}, NewReplicaIds: append([]int64(nil), m.NewReplicaIDs...), Phase: string(m.Phase), FencedNodeIds: append([]int64(nil), m.FencedIDs...)})
	}
	sort.Slice(out.Migrations, func(i, j int) bool { return out.Migrations[i].OperationId < out.Migrations[j].OperationId })
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].NodeId < out.Nodes[j].NodeId })
	sort.Slice(out.Placements, func(i, j int) bool { return out.Placements[i].KnowledgeBaseId < out.Placements[j].KnowledgeBaseId })
	sort.Slice(out.Tombstones, func(i, j int) bool { return out.Tombstones[i].KnowledgeBaseId < out.Tombstones[j].KnowledgeBaseId })
	return out
}

func StorageDirectoryFromProto(in *pb.GetStorageDirectoryResponse) storage.Directory {
	out := storage.NewDirectory()
	out.Revision = in.GetRevision()
	if len(in.GetCleanups()) > 0 {
		out.Cleanups = map[string]storage.Cleanup{}
		for _, c := range in.GetCleanups() {
			out.Cleanups[c.Id] = storage.Cleanup{ID: c.Id, KBID: c.KnowledgeBaseId, OperationID: c.OperationId, NodeID: c.NodeId, Epoch: c.Epoch, ArtifactsOnly: c.ArtifactsOnly, Done: c.Done}
		}
	}
	if in.GetServiceBlocks() != nil {
		out.ServiceBlocks = make(map[int64]storage.ServiceBlock, len(in.GetServiceBlocks()))
		for id, b := range in.GetServiceBlocks() {
			out.ServiceBlocks[id] = storage.ServiceBlock{OperationID: b.GetOperationId(), Revision: b.GetRevision(), Revoked: b.GetRevoked()}
		}
	}
	if len(in.GetMigrations()) > 0 {
		out.Migrations = make(map[string]storage.Migration)
	}
	for _, m := range in.GetMigrations() {
		p := m.GetOldPlacement()
		out.Migrations[m.GetOperationId()] = storage.Migration{Install: migrationInstallFromProto(m.GetInstall()), Serve: migrationServeFromProto(m.GetServe()), Stage: migrationStageFromProto(m.GetStage()), FailedBlock: storage.ServiceBlock{OperationID: m.GetFailedBlock().GetOperationId(), Revision: m.GetFailedBlock().GetRevision(), Revoked: m.GetFailedBlock().GetRevoked()}, Request: storage.MigrationRequest{Fault: m.GetFault(), OperationID: m.GetOperationId(), KBID: m.GetKnowledgeBaseId(), ExpectedEpoch: m.GetExpectedEpoch(), FromID: m.GetFromNodeId(), ToID: m.GetToNodeId()}, Old: storage.Placement{KBID: p.GetKnowledgeBaseId(), Epoch: p.GetPlacementEpoch(), ReplicaIDs: append([]int64(nil), p.GetReplicaIds()...), RequiredReplicas: int(p.GetRequiredReplicas()), OperationID: p.GetOperationId()}, NewReplicaIDs: append([]int64(nil), m.GetNewReplicaIds()...), Phase: storage.MigrationPhase(m.GetPhase()), FencedIDs: append([]int64(nil), m.GetFencedNodeIds()...)}
	}
	if in.GetActiveVersions() != nil {
		out.ActiveVersions = make(map[string]int64, len(in.GetActiveVersions()))
		for id, v := range in.GetActiveVersions() {
			out.ActiveVersions[id] = v
		}
	}
	for _, n := range in.GetNodes() {
		out.Nodes[n.GetNodeId()] = storage.Node{ID: n.GetNodeId(), Addr: n.GetAddress(), State: storage.NodeState(n.GetManagementState())}
	}
	for _, p := range in.GetPlacements() {
		out.Placements[p.GetKnowledgeBaseId()] = storage.Placement{
			KBID: p.GetKnowledgeBaseId(), Epoch: p.GetPlacementEpoch(), ReplicaIDs: append([]int64(nil), p.GetReplicaIds()...), RequiredReplicas: int(p.GetRequiredReplicas()), OperationID: p.GetOperationId(),
		}
	}
	for _, t := range in.GetTombstones() {
		out.Tombstones[t.GetKnowledgeBaseId()] = storage.Tombstone{KBID: t.GetKnowledgeBaseId(), Epoch: t.GetDeletionEpoch(), CleanupIDs: append([]int64(nil), t.GetCleanupNodeIds()...)}
	}
	return out
}

func serviceBlockToProto(b storage.ServiceBlock) *pb.StorageServiceBlock {
	if b.OperationID == "" {
		return nil
	}
	return &pb.StorageServiceBlock{OperationId: b.OperationID, Revision: b.Revision, Revoked: b.Revoked}
}

func migrationStageToProto(r *storage.MigrationStageReceipt) *pb.MigrationStageEntry {
	if r == nil {
		return nil
	}
	return &pb.MigrationStageEntry{SourceNodeId: r.SourceID, TargetNodeId: r.TargetID, ManifestSha256: r.ManifestHash, PayloadSha256: r.PayloadHash, EntryCount: r.Entries, ByteCount: r.Bytes}
}
func migrationStageFromProto(r *pb.MigrationStageEntry) *storage.MigrationStageReceipt {
	if r == nil {
		return nil
	}
	return &storage.MigrationStageReceipt{SourceID: r.SourceNodeId, TargetID: r.TargetNodeId, ManifestHash: r.ManifestSha256, PayloadHash: r.PayloadSha256, Entries: r.EntryCount, Bytes: r.ByteCount}
}
func migrationInstallToProto(r *storage.MigrationInstallReceipt) *pb.MigrationInstallEntry {
	if r == nil {
		return nil
	}
	return &pb.MigrationInstallEntry{SourceNodeId: r.SourceID, TargetNodeId: r.TargetID, ManifestSha256: r.ManifestHash, StageSha256: r.StageHash, VersionCount: r.Versions, DocumentCount: r.Documents, ChunkCount: r.Chunks}
}
func migrationInstallFromProto(r *pb.MigrationInstallEntry) *storage.MigrationInstallReceipt {
	if r == nil {
		return nil
	}
	return &storage.MigrationInstallReceipt{SourceID: r.SourceNodeId, TargetID: r.TargetNodeId, ManifestHash: r.ManifestSha256, StageHash: r.StageSha256, Versions: r.VersionCount, Documents: r.DocumentCount, Chunks: r.ChunkCount}
}
func migrationServeToProto(r *storage.MigrationServeReceipt) *pb.MigrationServeEntry {
	if r == nil {
		return nil
	}
	return &pb.MigrationServeEntry{TargetNodeId: r.TargetID, ManifestSha256: r.ManifestHash, ActiveVersion: r.ActiveVersion, CursorVersion: r.CursorVersion, BloomVersionCount: r.BloomVersions, IndexReady: r.IndexReady}
}
func migrationServeFromProto(r *pb.MigrationServeEntry) *storage.MigrationServeReceipt {
	if r == nil {
		return nil
	}
	return &storage.MigrationServeReceipt{TargetID: r.TargetNodeId, ManifestHash: r.ManifestSha256, ActiveVersion: r.ActiveVersion, CursorVersion: r.CursorVersion, BloomVersions: r.BloomVersionCount, IndexReady: r.IndexReady}
}
