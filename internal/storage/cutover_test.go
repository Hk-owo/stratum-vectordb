package storage

import (
	"errors"
	"reflect"
	serrors "stratum/internal/errors"
	"testing"
)

func readyCutover(t *testing.T) (Directory, MigrationRequest, MigrationManifest, MigrationActivation) {
	t.Helper()
	d, req, manifest, stage := stagedMigrationTask(t)
	install := installReceiptFor(stage)
	if err := d.RecordMigrationInstall(req.OperationID, install, manifest); err != nil {
		t.Fatal(err)
	}
	serve := MigrationServeReceipt{TargetID: req.ToID, ManifestHash: manifest.Hash(), ActiveVersion: 1, CursorVersion: 1, BloomVersions: 1, IndexReady: true}
	if err := d.RecordMigrationServe(req.OperationID, serve, manifest); err != nil {
		t.Fatal(err)
	}
	return d, req, manifest, MigrationActivation{Epoch: 1, Install: install, Serve: serve}
}
func TestMigrationCutoverAtomicallyChangesPlacementAndOwesCleanup(t *testing.T) {
	d, req, manifest, a := readyCutover(t)
	before := d.Clone()
	bad := a
	bad.Epoch++
	if err := d.ActivateMigration(req.OperationID, bad, manifest); err == nil || !reflect.DeepEqual(d, before) {
		t.Fatal("stale activation mutated directory", err)
	}
	if err := d.ActivateMigration(req.OperationID, a, manifest); err != nil {
		t.Fatal(err)
	}
	p := d.Placements[req.KBID]
	if p.Epoch != 2 || p.Contains(req.FromID) || !p.Contains(req.ToID) || p.RequiredReplicas != before.Placements[req.KBID].RequiredReplicas || p.Quorum() != before.Placements[req.KBID].Quorum() {
		t.Fatal(p)
	}
	if _, ok := d.ActiveMigration(req.KBID); ok {
		t.Fatal("activated task still freezes writes")
	}
	if len(d.Cleanups) != 2 {
		t.Fatal("cutover lost cleanup obligations", d.Cleanups)
	}
	rev := d.Revision
	if err := d.ActivateMigration(req.OperationID, a, MigrationManifest{}); err != nil || d.Revision != rev {
		t.Fatal("activation retry not idempotent", err)
	}
	if err := d.BeginMigration(MigrationRequest{OperationID: "return", KBID: req.KBID, ExpectedEpoch: 2, FromID: req.ToID, ToID: req.FromID}); !errors.Is(err, serrors.ErrMigrationInProgress) {
		t.Fatal("reuse before cleanup", err)
	}
	for id := range d.Cleanups {
		if err := d.AckCleanup(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.BeginMigration(MigrationRequest{OperationID: "return", KBID: req.KBID, ExpectedEpoch: 2, FromID: req.ToID, ToID: req.FromID}); err != nil {
		t.Fatal("reuse after cleanup", err)
	}
}
func TestMigrationCutoverRequiresCurrentServingAndFaultProof(t *testing.T) {
	for _, mutation := range []func(*Directory, MigrationRequest){
		func(d *Directory, r MigrationRequest) {
			m := d.Migrations[r.OperationID]
			m.Serve = nil
			d.Migrations[r.OperationID] = m
		},
		func(d *Directory, r MigrationRequest) { p := d.Placements[r.KBID]; p.Epoch++; d.Placements[r.KBID] = p },
		func(d *Directory, r MigrationRequest) {
			m := d.Migrations[r.OperationID]
			m.Request.Fault = true
			m.FailedBlock = ServiceBlock{OperationID: "old", Revision: 1, Revoked: true}
			d.Migrations[r.OperationID] = m
			d.ServiceBlocks = map[int64]ServiceBlock{r.FromID: {OperationID: "new", Revision: 2, Revoked: true}}
		},
	} {
		d, r, manifest, a := readyCutover(t)
		mutation(&d, r)
		before := d.Clone()
		if err := d.ActivateMigration(r.OperationID, a, manifest); err == nil || !reflect.DeepEqual(d, before) {
			t.Fatal("invalid activation accepted or changed state", err)
		}
	}
}
func TestStorageDrainIsAtomicAndRetirementReservesIdentity(t *testing.T) {
	d, req := migrationDirectory(t)
	before := d.Clone()
	if err := d.DrainNode(DrainRequest{OperationID: "empty", NodeID: req.FromID, ExpectedRevision: d.Revision, TargetIDs: []int64{req.FromID}}); err == nil || !reflect.DeepEqual(d, before) {
		t.Fatal("failed drain partially mutated directory", err)
	}
	if err := d.DrainNode(DrainRequest{OperationID: "whole", NodeID: req.FromID, ExpectedRevision: d.Revision, TargetIDs: []int64{req.ToID}}); err != nil {
		t.Fatal(err)
	}
	rev := d.Revision
	if err := d.DrainNode(DrainRequest{OperationID: "whole", NodeID: req.FromID, TargetIDs: []int64{req.ToID}}); err != nil || d.Revision != rev {
		t.Fatal("drain retry changed state", err)
	}
	if err := d.ManageNode(NodeChange{NodeID: req.FromID, ExpectedRevision: d.Revision, Action: "retire"}); err == nil {
		t.Fatal("retired referenced node")
	}
	// An unused node can retire only after a committed revocation proof.
	d = before
	node := req.ToID
	d.ServiceBlocks = map[int64]ServiceBlock{node: {OperationID: "retire", Revision: 1, Revoked: true}}
	if err := d.ManageNode(NodeChange{NodeID: node, ExpectedRevision: d.Revision, Action: "retire"}); err != nil {
		t.Fatal(err)
	}
	n := d.Nodes[node]
	n.State = NodeActive
	if err := d.RegisterNodes(d.Revision, []Node{n}); err == nil {
		t.Fatal("retired identity reused")
	}
}

func TestStorageAddressChangeRequiresDrainAndRevocation(t *testing.T) {
	d, req := migrationDirectory(t)
	id := req.ToID
	if err := d.ManageNode(NodeChange{NodeID: id, ExpectedRevision: d.Revision, Action: "address", Address: "moved"}); err == nil {
		t.Fatal("changed address without revoking old instance")
	}
	d.ServiceBlocks = map[int64]ServiceBlock{id: {OperationID: "move-address", Revision: 1, Revoked: true}}
	if err := d.ManageNode(NodeChange{NodeID: id, ExpectedRevision: d.Revision, Action: "address", Address: "moved"}); err != nil {
		t.Fatal(err)
	}
	if d.Nodes[id].Addr != "moved" {
		t.Fatal(d.Nodes[id])
	}
	if err := d.AuthorizeService(ServiceRequest{NodeID: id, Address: "moved"}); err == nil {
		t.Fatal("address change implicitly resumed service")
	}
	b := d.ServiceBlocks[id]
	if err := d.ResumeService(ServiceRequest{NodeID: id, ExpectedRevision: d.Revision, OperationID: b.OperationID, BlockRevision: b.Revision}); err != nil {
		t.Fatal(err)
	}
	if err := d.AuthorizeService(ServiceRequest{NodeID: id, Address: "s4"}); err == nil {
		t.Fatal("old endpoint renewed service")
	}
	if err := d.AuthorizeService(ServiceRequest{NodeID: id, Address: "moved"}); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationCutoverRefreshesServingCursorAfterRestart(t *testing.T) {
	d, req, manifest, a := readyCutover(t)
	a.Serve.CursorVersion++
	if err := d.ActivateMigration(req.OperationID, a, manifest); err != nil {
		t.Fatal("fresh valid cursor conflicted with historical serving evidence", err)
	}
	if d.Migrations[req.OperationID].Serve.CursorVersion != a.Serve.CursorVersion {
		t.Fatal("activation did not retain fresh evidence")
	}
}
