package wire

import (
	"reflect"
	"testing"

	pb "stratum/api/proto/stratum"
	"stratum/internal/storage"
)

// fullDirectory is the shape every field of storage.Directory takes when it has
// something to say. A field dropped here is not "absent" on the other side of the
// wire — it is a zero, which reads as a fact: an empty replica list is an empty
// placement, a missing phase is the empty phase, revision 0 is "no directory".
func fullDirectory() storage.Directory {
	d := storage.NewDirectory()
	d.Revision = 41
	d.ActiveVersions = map[string]int64{"kb-1": 7, "kb-2": 9}
	d.ServiceBlocks = map[int64]storage.ServiceBlock{
		11: {OperationID: "op-block-11", Revision: 3, Revoked: true},
		13: {OperationID: "op-block-13", Revision: 5},
	}
	d.Cleanups = map[string]storage.Cleanup{
		"cleanup-b": {ID: "cleanup-b", KBID: "kb-2", OperationID: "op-clean-b", NodeID: 13, Epoch: 4, ArtifactsOnly: true},
		"cleanup-a": {ID: "cleanup-a", KBID: "kb-1", OperationID: "op-clean-a", NodeID: 11, Epoch: 2, Done: true},
	}
	d.Nodes = map[int64]storage.Node{
		13: {ID: 13, Addr: "10.0.0.13:7101", State: storage.NodeDraining},
		11: {ID: 11, Addr: "10.0.0.11:7101", State: storage.NodeActive},
	}
	d.Placements = map[string]storage.Placement{
		"kb-2": {KBID: "kb-2", Epoch: 2, ReplicaIDs: []int64{12, 14}, RequiredReplicas: 2, OperationID: "op-place-b"},
		"kb-1": {KBID: "kb-1", Epoch: 1, ReplicaIDs: []int64{11, 13}, RequiredReplicas: 2, OperationID: "op-place-a"},
	}
	d.Tombstones = map[string]storage.Tombstone{
		"kb-2": {KBID: "kb-2", Epoch: 3, CleanupIDs: []int64{13, 15}},
		"kb-1": {KBID: "kb-1", Epoch: 2, CleanupIDs: []int64{11}},
	}
	d.Migrations = map[string]storage.Migration{
		"op-migrate-b": {
			Request: storage.MigrationRequest{OperationID: "op-migrate-b", KBID: "kb-2", ExpectedEpoch: 2, FromID: 12, ToID: 15},
			Phase:   storage.MigrationFenced,
		},
		"op-migrate-a": {
			Request:       storage.MigrationRequest{Fault: true, OperationID: "op-migrate-a", KBID: "kb-1", ExpectedEpoch: 1, FromID: 11, ToID: 12},
			Old:           storage.Placement{KBID: "kb-1", Epoch: 1, ReplicaIDs: []int64{11, 13}, RequiredReplicas: 2, OperationID: "op-place-a"},
			NewReplicaIDs: []int64{12, 13},
			FencedIDs:     []int64{11, 13, 12},
			Phase:         storage.MigrationStaged,
			FailedBlock:   storage.ServiceBlock{OperationID: "op-block-11", Revision: 3, Revoked: true},
			Stage:         &storage.MigrationStageReceipt{SourceID: 11, TargetID: 12, ManifestHash: "manifest-a", PayloadHash: "payload-a", Entries: 8, Bytes: 4096},
			Install:       &storage.MigrationInstallReceipt{SourceID: 11, TargetID: 12, ManifestHash: "manifest-a", StageHash: "stage-a", Versions: 3, Documents: 21, Chunks: 34},
			Serve:         &storage.MigrationServeReceipt{TargetID: 12, ManifestHash: "manifest-a", ActiveVersion: 7, CursorVersion: 6, BloomVersions: 2, IndexReady: true},
		},
	}
	return d
}

// Every collection in the response is deterministic and complete: two control
// nodes that hold the same directory must serialise it identically, or every
// client sees a spurious diff on each refresh.
func TestStorageDirectoryToProto_CarriesEveryFieldInOrder(t *testing.T) {
	got := StorageDirectoryToProto(fullDirectory())

	if got.GetRevision() != 41 {
		t.Errorf("revision = %d, want 41", got.GetRevision())
	}
	if !reflect.DeepEqual(got.GetActiveVersions(), map[string]int64{"kb-1": 7, "kb-2": 9}) {
		t.Errorf("active versions = %v", got.GetActiveVersions())
	}
	if block := got.GetServiceBlocks()[13]; block.GetOperationId() != "op-block-13" || block.GetRevision() != 5 || block.GetRevoked() {
		t.Errorf("service block 13 = %+v", block)
	}
	if block := got.GetServiceBlocks()[11]; !block.GetRevoked() {
		t.Errorf("service block 11 = %+v, want revoked", block)
	}

	if ids := cleanupIDs(got.GetCleanups()); !reflect.DeepEqual(ids, []string{"cleanup-a", "cleanup-b"}) {
		t.Errorf("cleanup order = %v, want sorted by id", ids)
	}
	if c := got.GetCleanups()[0]; !c.GetDone() || c.GetArtifactsOnly() || c.GetKnowledgeBaseId() != "kb-1" || c.GetNodeId() != 11 || c.GetEpoch() != 2 {
		t.Errorf("cleanup-a = %+v", c)
	}

	if ids := nodeIDs(got.GetNodes()); !reflect.DeepEqual(ids, []int64{11, 13}) {
		t.Errorf("node order = %v, want sorted by node id", ids)
	}
	if n := got.GetNodes()[1]; n.GetAddress() != "10.0.0.13:7101" || n.GetManagementState() != string(storage.NodeDraining) {
		t.Errorf("node 13 = %+v", n)
	}

	if kbs := placementKBs(got.GetPlacements()); !reflect.DeepEqual(kbs, []string{"kb-1", "kb-2"}) {
		t.Errorf("placement order = %v, want sorted by KB", kbs)
	}
	if p := got.GetPlacements()[1]; !reflect.DeepEqual(p.GetReplicaIds(), []int64{12, 14}) || p.GetPlacementEpoch() != 2 || p.GetRequiredReplicas() != 2 || p.GetOperationId() != "op-place-b" {
		t.Errorf("placement kb-2 = %+v", p)
	}

	if kbs := tombstoneKBs(got.GetTombstones()); !reflect.DeepEqual(kbs, []string{"kb-1", "kb-2"}) {
		t.Errorf("tombstone order = %v, want sorted by KB", kbs)
	}
	if tomb := got.GetTombstones()[1]; tomb.GetDeletionEpoch() != 3 || !reflect.DeepEqual(tomb.GetCleanupNodeIds(), []int64{13, 15}) {
		t.Errorf("tombstone kb-2 = %+v", tomb)
	}

	if ops := migrationOps(got.GetMigrations()); !reflect.DeepEqual(ops, []string{"op-migrate-a", "op-migrate-b"}) {
		t.Errorf("migration order = %v, want sorted by operation id", ops)
	}
	m := got.GetMigrations()[0]
	if m.GetOperationId() != "op-migrate-a" || m.GetKnowledgeBaseId() != "kb-1" || m.GetExpectedEpoch() != 1 || !m.GetFault() {
		t.Errorf("migration request = %+v", m)
	}
	if m.GetFromNodeId() != 11 || m.GetToNodeId() != 12 || m.GetPhase() != string(storage.MigrationStaged) {
		t.Errorf("migration identity/phase = %+v", m)
	}
	if !reflect.DeepEqual(m.GetNewReplicaIds(), []int64{12, 13}) || !reflect.DeepEqual(m.GetFencedNodeIds(), []int64{11, 13, 12}) {
		t.Errorf("migration replica/fenced ids = %v / %v", m.GetNewReplicaIds(), m.GetFencedNodeIds())
	}
	if old := m.GetOldPlacement(); old.GetKnowledgeBaseId() != "kb-1" || old.GetPlacementEpoch() != 1 || !reflect.DeepEqual(old.GetReplicaIds(), []int64{11, 13}) || old.GetRequiredReplicas() != 2 || old.GetOperationId() != "op-place-a" {
		t.Errorf("migration old placement = %+v", old)
	}
	if st := m.GetStage(); st.GetSourceNodeId() != 11 || st.GetTargetNodeId() != 12 || st.GetManifestSha256() != "manifest-a" || st.GetPayloadSha256() != "payload-a" || st.GetEntryCount() != 8 || st.GetByteCount() != 4096 {
		t.Errorf("migration stage receipt = %+v", st)
	}
	if in := m.GetInstall(); in.GetStageSha256() != "stage-a" || in.GetVersionCount() != 3 || in.GetDocumentCount() != 21 || in.GetChunkCount() != 34 || in.GetManifestSha256() != "manifest-a" {
		t.Errorf("migration install receipt = %+v", in)
	}
	if sv := m.GetServe(); sv.GetTargetNodeId() != 12 || sv.GetActiveVersion() != 7 || sv.GetCursorVersion() != 6 || sv.GetBloomVersionCount() != 2 || !sv.GetIndexReady() || sv.GetManifestSha256() != "manifest-a" {
		t.Errorf("migration serve receipt = %+v", sv)
	}
	if fb := m.GetFailedBlock(); fb.GetOperationId() != "op-block-11" || fb.GetRevision() != 3 || !fb.GetRevoked() {
		t.Errorf("migration failed block = %+v", fb)
	}

	// The second migration carries no receipts: those must come out nil rather
	// than as an empty receipt whose all-zero fields read as "nothing was
	// transferred, and that is the measured result".
	bare := got.GetMigrations()[1]
	if bare.GetStage() != nil || bare.GetInstall() != nil || bare.GetServe() != nil || bare.GetFailedBlock() != nil {
		t.Errorf("empty migration receipts = %+v", bare)
	}
	// OldPlacement is not treated the same way: it is always carried, because a
	// migration without an old placement is already an invalid record and an
	// empty message makes that visible instead of hiding it behind nil.
	if bare.GetOldPlacement() == nil {
		t.Error("old placement = nil, want the empty message every migration carries")
	}
}

// A directory with nothing registered must still serialise to a valid response,
// and a service block with no operation id is "no block" rather than a block with
// revision 0 — the latter would claim an operation nobody ever ran.
func TestStorageDirectoryToProto_EmptyDirectory(t *testing.T) {
	got := StorageDirectoryToProto(storage.Directory{})
	if got == nil {
		t.Fatal("nil response")
	}
	if got.GetRevision() != 0 || got.GetActiveVersions() != nil || got.GetServiceBlocks() != nil {
		t.Errorf("empty directory = %+v", got)
	}
	if len(got.GetNodes()) != 0 || len(got.GetPlacements()) != 0 || len(got.GetTombstones()) != 0 || len(got.GetCleanups()) != 0 || len(got.GetMigrations()) != 0 {
		t.Errorf("empty directory carried entries: %+v", got)
	}
	if b := serviceBlockToProto(storage.ServiceBlock{Revision: 9, Revoked: true}); b != nil {
		t.Errorf("serviceBlockToProto without operation id = %+v, want nil", b)
	}
}

// The reverse direction has to rebuild the same directory, including the nil-ness
// of the optional maps: a non-nil empty map reads as "the control layer has data
// here and it happens to be empty", which is not the same as "not carried".
func TestStorageDirectoryFromProto_RebuildsTheDirectory(t *testing.T) {
	want := fullDirectory()
	got := StorageDirectoryFromProto(StorageDirectoryToProto(want))

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip lost or invented state:\n got = %#v\nwant = %#v", got, want)
	}

	empty := StorageDirectoryFromProto(&pb.GetStorageDirectoryResponse{Revision: 3})
	if empty.Revision != 3 {
		t.Errorf("revision = %d, want 3", empty.Revision)
	}
	if empty.Cleanups != nil || empty.ServiceBlocks != nil || empty.Migrations != nil || empty.ActiveVersions != nil {
		t.Errorf("optional maps invented: %#v", empty)
	}
	if empty.Nodes == nil || empty.Placements == nil || empty.Tombstones == nil {
		t.Errorf("required maps must stay usable: %#v", empty)
	}
}

// The receipt converters are also used on their own (a target node reports a
// single receipt), so nil-in/nil-out has to hold without going through a
// directory: a non-nil receipt built from a nil message carries all-zero fields
// and would be accepted as evidence that nothing was transferred.
func TestMigrationReceiptConverters_NilStaysNil(t *testing.T) {
	if migrationStageToProto(nil) != nil || migrationInstallToProto(nil) != nil || migrationServeToProto(nil) != nil {
		t.Error("nil storage receipt must not become an empty proto receipt")
	}
	if migrationStageFromProto(nil) != nil || migrationInstallFromProto(nil) != nil || migrationServeFromProto(nil) != nil {
		t.Error("nil proto receipt must not become an empty storage receipt")
	}

	stage := &storage.MigrationStageReceipt{SourceID: 1, TargetID: 2, ManifestHash: "m", PayloadHash: "p", Entries: 3, Bytes: 4}
	if got := migrationStageFromProto(migrationStageToProto(stage)); !reflect.DeepEqual(got, stage) {
		t.Errorf("stage round trip = %+v, want %+v", got, stage)
	}
	install := &storage.MigrationInstallReceipt{SourceID: 1, TargetID: 2, ManifestHash: "m", StageHash: "s", Versions: 3, Documents: 4, Chunks: 5}
	if got := migrationInstallFromProto(migrationInstallToProto(install)); !reflect.DeepEqual(got, install) {
		t.Errorf("install round trip = %+v, want %+v", got, install)
	}
	serve := &storage.MigrationServeReceipt{TargetID: 2, ManifestHash: "m", ActiveVersion: 3, CursorVersion: 4, BloomVersions: 5, IndexReady: true}
	if got := migrationServeFromProto(migrationServeToProto(serve)); !reflect.DeepEqual(got, serve) {
		t.Errorf("serve round trip = %+v, want %+v", got, serve)
	}
}

// The proto is a copy: a caller that mutates it (or a slice it handed in) must
// not reach back into the directory the leader is serving from.
func TestStorageDirectoryToProto_CopiesSlicesAndMaps(t *testing.T) {
	d := fullDirectory()
	got := StorageDirectoryToProto(d)
	got.Placements[0].ReplicaIds[0] = 99
	got.ActiveVersions["kb-1"] = 99
	got.Migrations[0].NewReplicaIds[0] = 99
	got.Migrations[0].FencedNodeIds[0] = 99
	got.Tombstones[1].CleanupNodeIds[0] = 99

	if d.Placements["kb-1"].ReplicaIDs[0] == 99 || d.ActiveVersions["kb-1"] == 99 ||
		d.Migrations["op-migrate-a"].NewReplicaIDs[0] == 99 || d.Migrations["op-migrate-a"].FencedIDs[0] == 99 ||
		d.Tombstones["kb-2"].CleanupIDs[0] == 99 {
		t.Error("mutating the proto reached back into the directory")
	}

	// Same on the way in: the decoded directory must own its slices.
	in := StorageDirectoryToProto(fullDirectory())
	decoded := StorageDirectoryFromProto(in)
	in.Placements[0].ReplicaIds[0] = 77
	in.Migrations[0].NewReplicaIds[0] = 77
	in.Migrations[0].OldPlacement.ReplicaIds[0] = 77
	in.Tombstones[1].CleanupNodeIds[0] = 77
	if decoded.Placements["kb-1"].ReplicaIDs[0] == 77 || decoded.Migrations["op-migrate-a"].NewReplicaIDs[0] == 77 ||
		decoded.Migrations["op-migrate-a"].Old.ReplicaIDs[0] == 77 || decoded.Tombstones["kb-2"].CleanupIDs[0] == 77 {
		t.Error("the decoded directory aliases the proto it was built from")
	}
}

func cleanupIDs(in []*pb.StorageCleanupEntry) []string {
	out := make([]string, 0, len(in))
	for _, c := range in {
		out = append(out, c.GetId())
	}
	return out
}

func nodeIDs(in []*pb.StorageNodeEntry) []int64 {
	out := make([]int64, 0, len(in))
	for _, n := range in {
		out = append(out, n.GetNodeId())
	}
	return out
}

func placementKBs(in []*pb.KBPlacementEntry) []string {
	out := make([]string, 0, len(in))
	for _, p := range in {
		out = append(out, p.GetKnowledgeBaseId())
	}
	return out
}

func tombstoneKBs(in []*pb.KBStorageTombstone) []string {
	out := make([]string, 0, len(in))
	for _, t := range in {
		out = append(out, t.GetKnowledgeBaseId())
	}
	return out
}

func migrationOps(in []*pb.KBMigrationEntry) []string {
	out := make([]string, 0, len(in))
	for _, m := range in {
		out = append(out, m.GetOperationId())
	}
	return out
}
