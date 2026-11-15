package storage

import (
	"errors"
	"reflect"
	"testing"

	serrors "stratum/internal/errors"
)

func fullDirectory(t *testing.T) Directory {
	t.Helper()
	d := NewDirectory()
	if err := d.RegisterNodes(0, []Node{{ID: 3, Addr: "s3"}, {ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestDirectoryRegistrationIsAtomicAndCASProtected(t *testing.T) {
	d := fullDirectory(t)
	before := d.Clone()
	if err := d.RegisterNodes(0, []Node{{ID: 1, Addr: "s1", State: NodeDraining}}); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, d) {
		t.Fatal("stale command changed the directory")
	}
	if err := d.RegisterNodes(d.Revision, []Node{{ID: 1, Addr: "s1", State: NodeDraining}, {ID: 2, Addr: "s1"}}); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, d) {
		t.Fatal("invalid command partially applied")
	}
	if err := d.RegisterNodes(0, []Node{{ID: 1, Addr: "s1"}}); err != nil {
		t.Fatal("lost-response retry:", err)
	}
	if d.Revision != before.Revision || len(d.Nodes) != 3 {
		t.Fatal("retry changed revision or removed omitted nodes")
	}
	if err := d.RegisterNodes(d.Revision, []Node{{ID: 4, Addr: "s4"}}); err != nil {
		t.Fatal("runtime registration:", err)
	}
	if err := d.RegisterNodes(d.Revision, []Node{{ID: 1, Addr: "replacement"}}); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Fatal("P1 enabled an address change:", err)
	}
}

func TestDirectoryAssignmentRetriesAndFencesPartialPlacement(t *testing.T) {
	d := fullDirectory(t)
	p := d.InitialPlacement("kb", "op1")
	p.ReplicaIDs = []int64{3, 1, 2}
	if err := d.AssignInitial(p); err != nil {
		t.Fatal(err)
	}
	revision := d.Revision
	p.ReplicaIDs = []int64{2, 3, 1}
	if err := d.AssignInitial(p); err != nil || d.Revision != revision {
		t.Fatal("retry must be a no-op:", err)
	}
	p.OperationID = "op2"
	if err := d.AssignInitial(p); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Fatal(err)
	}
	p.KBID, p.ReplicaIDs, p.RequiredReplicas = "partial", []int64{1, 2}, 2
	if err := d.AssignInitial(p); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Fatal("partial placement enabled before P2/P3:", err)
	}
	p.ReplicaIDs, p.RequiredReplicas = []int64{1, 1, 2}, 3
	if err := d.AssignInitial(p); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Fatal("duplicate ACK identity:", err)
	}
	if d.Revision != revision {
		t.Fatal("rejected operation changed revision")
	}
}

func TestDirectorySnapshotCannotShrinkQuorumOrAliasState(t *testing.T) {
	d := fullDirectory(t)
	if err := d.AssignInitial(d.InitialPlacement("kb", "op")); err != nil {
		t.Fatal(err)
	}
	snap, err := d.Resolve("kb")
	if err != nil || snap.Placement.Quorum() != 2 || len(snap.Nodes) != 3 {
		t.Fatalf("%+v %v", snap, err)
	}
	snap.Placement.ReplicaIDs[0] = 99
	clone := d.Clone()
	clone.Placements["kb"].ReplicaIDs[0] = 88
	if d.Placements["kb"].ReplicaIDs[0] != 1 {
		t.Fatal("snapshot aliases state")
	}
	delete(d.Nodes, 3)
	if _, err := d.Resolve("kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Fatal("missing replica silently reduced the denominator:", err)
	}
}

func TestDirectoryDeletionKeepsIdentityAndCleanupTargets(t *testing.T) {
	d := fullDirectory(t)
	if err := d.AssignInitial(d.InitialPlacement("old-id", "op")); err != nil {
		t.Fatal(err)
	}
	d.Delete("old-id")
	before := d.Clone()
	d.Delete("old-id")
	if !reflect.DeepEqual(d, before) {
		t.Fatal("deletion retry changed the fence")
	}
	if _, err := d.Resolve("old-id"); !errors.Is(err, serrors.ErrKnowledgeBaseDeleted) {
		t.Fatal(err)
	}
	if err := d.AssignInitial(d.InitialPlacement("old-id", "late")); !errors.Is(err, serrors.ErrKnowledgeBaseDeleted) {
		t.Fatal("late command resurrected identity:", err)
	}
	if err := d.AssignInitial(d.InitialPlacement("new-id", "new")); err != nil {
		t.Fatal("same display name may use a fresh internal identity:", err)
	}
	clone := d.Clone()
	clone.Tombstones["old-id"].CleanupIDs[0] = 99
	if !reflect.DeepEqual(d.Tombstones["old-id"].CleanupIDs, []int64{1, 2, 3}) {
		t.Fatal("cleanup targets lost or aliased")
	}
}

func TestDirectoryUnknownAndLegalSingleReplicaDiffer(t *testing.T) {
	d := NewDirectory()
	if _, err := d.Resolve("kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Fatal(err)
	}
	if err := d.RegisterNodes(0, []Node{{ID: 1, Addr: "s1"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.AssignInitial(d.InitialPlacement("kb", "op")); err != nil {
		t.Fatal(err)
	}
	snap, err := d.Resolve("kb")
	if err != nil || snap.Placement.Quorum() != 1 {
		t.Fatalf("single replica: %+v %v", snap, err)
	}
}

func TestDeletionBeforeLegacyRegistrationRetainsCleanupSet(t *testing.T) {
	d := NewDirectory()
	d.Delete("old")
	if err := d.RegisterNodes(d.Revision, []Node{{ID: 1, Addr: "s1"}}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(d.Tombstones["old"].CleanupIDs, []int64{1}) {
		t.Fatal("legacy deletion has no cleanup targets")
	}
}

func TestDynamicNodeRegistrationPreservesKBPolicyAndDrainingStopsNewAssignments(t *testing.T) {
	d := fullDirectory(t)
	if err := d.AssignNew(d.InitialPlacement("existing", "create/existing")); err != nil {
		t.Fatal(err)
	}
	before, _ := d.Lookup("existing")
	if err := d.RegisterNodes(d.Revision, []Node{{ID: 9, Addr: "s9"}}); err != nil {
		t.Fatal(err)
	}
	after, _ := d.Lookup("existing")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("adding a node changed existing durability policy")
	}
	if err := d.RegisterNodes(d.Revision, []Node{{ID: 2, Addr: "s2", State: NodeDraining}}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Resolve("existing"); err != nil {
		t.Fatal("draining broke existing placement:", err)
	}
	initial := d.InitialPlacement("new", "create/new")
	if !reflect.DeepEqual(initial.ReplicaIDs, []int64{1, 3, 9}) {
		t.Fatal("draining node received new KB:", initial)
	}
	if err := d.AssignNew(initial); err != nil {
		t.Fatal(err)
	}
	stale := d.Revision - 1
	snapshot := d.Clone()
	if err := d.RegisterNodes(stale, []Node{{ID: 10, Addr: "s10"}}); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot, d) {
		t.Fatal("stale registration partially applied")
	}
}
