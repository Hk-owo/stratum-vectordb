package plane

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"google.golang.org/grpc/metadata"

	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

// planeDirectory answers the two reads the data plane makes, so the placement
// helpers can be exercised without a control plane.
type planeDirectory struct {
	directory storage.Directory
	err       error
}

func (r planeDirectory) StorageDirectory(context.Context) (storage.Directory, error) {
	return r.directory.Clone(), r.err
}

func (r planeDirectory) GetKBPlacement(_ context.Context, kbID string) (storage.Snapshot, error) {
	return r.directory.Resolve(kbID)
}

// contextWithPlaneEpoch presents a placement epoch the way a peer would.
func contextWithPlaneEpoch(t *testing.T, epoch uint64) context.Context {
	t.Helper()
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-stratum-placement-epoch", strconv.FormatUint(epoch, 10)))
}

func planeSnapshot(kbID string) storage.Snapshot {
	return storage.Snapshot{
		ActiveVersion: 4,
		Placement:     storage.Placement{KBID: kbID, Epoch: 2, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/" + kbID},
		Nodes:         []storage.Node{{ID: 1, Addr: "s1", State: storage.NodeActive}, {ID: 2, Addr: "s2", State: storage.NodeActive}},
	}
}

func planeDirectoryFor(t *testing.T, snapshot storage.Snapshot) planeDirectory {
	t.Helper()
	d := storage.NewDirectory()
	d.Revision = snapshot.Revision
	d.Placements[snapshot.Placement.KBID] = snapshot.Placement
	d.ActiveVersions = map[string]int64{snapshot.Placement.KBID: snapshot.ActiveVersion}
	for _, n := range snapshot.Nodes {
		d.Nodes[n.ID] = n
	}
	return planeDirectory{directory: d}
}

// bindPlacement is the plane's own admission check: a node that is not a replica of
// the KB must not write to it, and the snapshot it does bind is the one the rest of
// the operation carries.
func TestLocalDataPlane_BindPlacement(t *testing.T) {
	ctx := context.Background()
	noResolver := &LocalDataPlane{nodeID: 1}
	if got, err := noResolver.bindPlacement(ctx, "kb"); err != nil || got != ctx {
		t.Errorf("without a resolver = %v, %v; want the context passed through", got, err)
	}

	resolverErr := &LocalDataPlane{nodeID: 1, placements: func(context.Context, string) (storage.Snapshot, error) {
		return storage.Snapshot{}, serrors.ErrNoLeaderYet
	}}
	if _, err := resolverErr.bindPlacement(ctx, "kb"); !errors.Is(err, serrors.ErrNoLeaderYet) {
		t.Errorf("resolver failure = %v", err)
	}

	outsider := &LocalDataPlane{nodeID: 9, placements: func(context.Context, string) (storage.Snapshot, error) {
		return planeSnapshot("kb"), nil
	}}
	if _, err := outsider.bindPlacement(ctx, "kb"); !errors.Is(err, serrors.ErrNotKBReplica) {
		t.Errorf("non-member = %v, want ErrNotKBReplica", err)
	}
	if owned, err := outsider.OwnsKnowledgeBase(ctx, "kb"); owned || err != nil {
		t.Errorf("OwnsKnowledgeBase for a non-member = %v, %v; want false, nil", owned, err)
	}

	member := &LocalDataPlane{nodeID: 1, placements: func(context.Context, string) (storage.Snapshot, error) {
		return planeSnapshot("kb"), nil
	}}
	bound, err := member.bindPlacement(ctx, "kb")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	snap, boundOK := storage.Bound(bound, "kb")
	if !boundOK || snap.Placement.Epoch != 2 {
		t.Errorf("bound snapshot = %+v, %v", snap, boundOK)
	}
	if owned, err := member.OwnsKnowledgeBase(ctx, "kb"); !owned || err != nil {
		t.Errorf("OwnsKnowledgeBase = %v, %v; want true", owned, err)
	}
	// Any other failure is surfaced rather than read as "does not own it".
	broken := &LocalDataPlane{nodeID: 1, placements: func(context.Context, string) (storage.Snapshot, error) {
		return storage.Snapshot{}, serrors.ErrPlacementUnknown
	}}
	if owned, err := broken.OwnsKnowledgeBase(ctx, "kb"); owned || !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("OwnsKnowledgeBase on failure = %v, %v", owned, err)
	}
}

// enterMutation decides whether a write may proceed: a bound snapshot goes through
// the gate, and a plane with a gate but no placement is refused rather than admitted
// unguarded.
func TestLocalDataPlane_EnterMutation(t *testing.T) {
	ctx := context.Background()
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	unguarded := &LocalDataPlane{nodeID: 1}
	if _, release, err := unguarded.enterMutation(ctx, "kb"); err != nil {
		t.Errorf("plane without placements or gate = %v", err)
	} else {
		release()
	}

	// A gate without placements cannot tell whether this node is a replica, and
	// guessing "yes" would let it write for a KB it does not hold.
	if _, _, err := (&LocalDataPlane{nodeID: 1, executionGate: gate}).enterMutation(ctx, "kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("gate without placements = %v, want ErrPlacementUnknown", err)
	}

	placed := &LocalDataPlane{nodeID: 1, executionGate: gate, placements: func(context.Context, string) (storage.Snapshot, error) {
		return planeSnapshot("kb"), nil
	}}
	wctx, release, err := placed.enterMutation(ctx, "kb")
	if err != nil {
		t.Fatalf("enter: %v", err)
	}
	release()
	// The gate is now applied to this KB: fencing it refuses the next write.
	if err := gate.Fence(wctx, "kb", 2, "move"); !errors.Is(err, serrors.ErrMigrationConflict) {
		// Fencing from inside the operation is a conflict by design; use a fresh
		// context to prove the fence takes effect afterwards.
		if err := gate.Fence(ctx, "kb", 2, "move"); err != nil {
			t.Fatalf("fence: %v", err)
		}
	}
	if _, _, err := placed.enterMutation(ctx, "kb"); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Errorf("write after fence = %v, want ErrKBWritePaused", err)
	}
}

// The droppers are the reclaim paths: each one must go through EnterCleanup so a
// deletion cannot delete a KB that is still serving here.
func TestGatedDroppers_RequireCleanupAdmission(t *testing.T) {
	ctx := context.Background()
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := planeSnapshot("kb")
	reader := planeDirectoryFor(t, snapshot)
	node := int64(1)

	var versionDrops []int64
	versionDropper := gatedVersionDropper{gate: gate, reader: reader, nodeID: node, next: versionDataDropperFunc(func(_ context.Context, _ string, v int64) error {
		versionDrops = append(versionDrops, v)
		return nil
	})}
	if err := versionDropper.DropVersionStorage(ctx, "kb", 7); err != nil {
		t.Fatalf("drop version: %v", err)
	}
	if !reflect.DeepEqual(versionDrops, []int64{7}) {
		t.Errorf("dropped = %v, want the version", versionDrops)
	}

	var kbDrops []string
	kbDropper := gatedKBDropper{gate: gate, reader: reader, nodeID: node, next: kbStorageDropperFunc(func(_ context.Context, kbID string) error {
		kbDrops = append(kbDrops, kbID)
		return nil
	})}
	if err := kbDropper.DropKnowledgeBase(ctx, "kb"); err != nil {
		t.Fatalf("drop KB: %v", err)
	}
	if !reflect.DeepEqual(kbDrops, []string{"kb"}) {
		t.Errorf("dropped = %v", kbDrops)
	}

	// A node that is not a replica may not delete the KB's data.
	outsider := gatedKBDropper{gate: gate, reader: reader, nodeID: 9, next: kbStorageDropperFunc(func(context.Context, string) error { return nil })}
	if err := outsider.DropKnowledgeBase(ctx, "kb"); !errors.Is(err, serrors.ErrNotKBReplica) {
		t.Errorf("outsider drop = %v, want ErrNotKBReplica", err)
	}
	// A reader failure is surfaced, and nothing is deleted.
	blind := gatedVersionDropper{gate: gate, reader: planeDirectory{err: serrors.ErrNoLeaderYet}, nodeID: node, next: versionDataDropperFunc(func(context.Context, string, int64) error {
		t.Error("deleted data without an admitted cleanup")
		return nil
	})}
	if err := blind.DropVersionStorage(ctx, "kb", 7); !errors.Is(err, serrors.ErrNoLeaderYet) {
		t.Errorf("unreadable directory = %v", err)
	}
}

// peers and source selection decide where data is copied to and read from, so each
// branch has to answer with the placement it was given — never with a wider set.
func TestLocalDataPlane_PeersAndSources(t *testing.T) {
	ctx := context.Background()
	snapshot := planeSnapshot("kb")
	resolver := func(context.Context, string) (storage.Snapshot, error) { return snapshot, nil }

	if (&LocalDataPlane{}).hasReplicaResolver() {
		t.Error("a bare plane claims a resolver")
	}
	if !(&LocalDataPlane{resolveReplicas: func(context.Context) ([]string, error) { return nil, nil }}).hasReplicaResolver() {
		t.Error("a replica resolver is not counted")
	}

	peers := &LocalDataPlane{nodeID: 1, placements: resolver}
	got, err := peers.replicaPeers(ctx, "kb")
	if err != nil || !reflect.DeepEqual(got, []string{"s2"}) {
		t.Errorf("replica peers = %v, %v; want the other member", got, err)
	}
	fallback := &LocalDataPlane{resolveReplicas: func(context.Context) ([]string, error) { return []string{"x"}, nil }}
	if got, err := fallback.replicaPeers(ctx, "kb"); err != nil || !reflect.DeepEqual(got, []string{"x"}) {
		t.Errorf("fallback peers = %v, %v", got, err)
	}
	if got, err := (&LocalDataPlane{}).replicaPeers(ctx, "kb"); got != nil || err != nil {
		t.Errorf("no resolver = %v, %v; want nothing", got, err)
	}
	failing := &LocalDataPlane{placements: func(context.Context, string) (storage.Snapshot, error) {
		return storage.Snapshot{}, serrors.ErrPlacementUnknown
	}}
	if _, err := failing.replicaPeers(ctx, "kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("resolver failure = %v", err)
	}

	if !(&LocalDataPlane{}).sourceAllowed(ctx, "kb", "s1") {
		t.Error("a plane without placements cannot filter sources and must accept a non-empty address")
	}
	if (&LocalDataPlane{}).sourceAllowed(ctx, "kb", "") {
		t.Error("an empty address was allowed as a source")
	}
	if !peers.sourceAllowed(ctx, "kb", "s2") || peers.sourceAllowed(ctx, "kb", "elsewhere") {
		t.Error("source filtering does not follow the placement")
	}

	// A source the placement does not name is replaced by whatever the transfer
	// picker offers; with no cursor querier wired that is nothing, and the caller is
	// told so instead of being handed the unrelated address.
	redirected := &LocalDataPlane{nodeID: 1, placements: resolver, resolve: func(context.Context, string, int64) (string, bool, error) {
		return "elsewhere", true, nil
	}}
	if addr, ok, err := redirected.resolveSource(ctx, "kb", 7); err != nil || ok || addr == "elsewhere" {
		t.Errorf("resolveSource = %q, %v, %v; want the unrelated source dropped", addr, ok, err)
	}
	// A source the placement names is kept as is.
	direct := &LocalDataPlane{nodeID: 1, placements: resolver, resolve: func(context.Context, string, int64) (string, bool, error) {
		return "s2", true, nil
	}}
	if addr, _, err := direct.resolveSource(ctx, "kb", 7); err != nil || addr != "s2" {
		t.Errorf("resolveSource = %q, %v", addr, err)
	}
	// Resolver failures are returned rather than papered over with a fallback.
	if _, _, err := (&LocalDataPlane{resolve: func(context.Context, string, int64) (string, bool, error) {
		return "", false, serrors.ErrVersionNotFound
	}}).resolveSource(ctx, "kb", 7); !errors.Is(err, serrors.ErrVersionNotFound) {
		t.Errorf("resolver failure = %v", err)
	}
}

// Cleanup peers come from the tombstone once the placement is gone, and that path
// binds the deletion's own epoch — a cleanup cannot be authorized by a newer one.
func TestLocalDataPlane_CleanupPeers(t *testing.T) {
	ctx := context.Background()
	if _, peers, err := (&LocalDataPlane{}).cleanupPeers(ctx, "kb"); err != nil || peers != nil {
		t.Errorf("plane without a directory = %v, %v", peers, err)
	}

	blind := &LocalDataPlane{directory: planeDirectory{err: serrors.ErrNoLeaderYet}}
	if _, _, err := blind.cleanupPeers(ctx, "kb"); !errors.Is(err, serrors.ErrNoLeaderYet) {
		t.Errorf("directory failure = %v", err)
	}

	d := storage.NewDirectory()
	if err := d.RegisterNodes(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.AssignNew(storage.Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"}); err != nil {
		t.Fatal(err)
	}
	live := &LocalDataPlane{nodeID: 1, directory: planeDirectory{directory: d}}
	bound, peers, err := live.cleanupPeers(ctx, "kb")
	if err != nil || !reflect.DeepEqual(peers, []string{"s2"}) {
		t.Fatalf("live placement = %v, %v", peers, err)
	}
	if snap, ok := storage.Bound(bound, "kb"); !ok || snap.Placement.Epoch != 1 {
		t.Errorf("bound snapshot = %+v, %v", snap, ok)
	}

	// A tombstone keeps the reclaim targets even though the placement is gone.
	deleted := d.Clone()
	deleted.Delete("kb")
	deleted.Tombstones["kb"] = storage.Tombstone{KBID: "kb", Epoch: 2, CleanupIDs: []int64{1, 2, 3}}
	tomb := &LocalDataPlane{nodeID: 1, directory: planeDirectory{directory: deleted}}
	bound, peers, err = tomb.cleanupPeers(ctx, "kb")
	if err != nil || !reflect.DeepEqual(peers, []string{"s2", "s3"}) {
		t.Fatalf("tombstone peers = %v, %v", peers, err)
	}
	if snap, ok := storage.Bound(bound, "kb"); !ok || snap.Placement.Epoch != 2 {
		t.Errorf("tombstone credential = %+v, %v; want the deletion's epoch", snap, ok)
	}

	// A cleanup target with no address is a hard error: guessing one could send
	// the reclaim to a different node.
	broken := deleted.Clone()
	broken.Nodes[3] = storage.Node{ID: 3}
	if _, _, err := (&LocalDataPlane{nodeID: 1, directory: planeDirectory{directory: broken}}).cleanupPeers(ctx, "kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("addressless cleanup target = %v, want ErrPlacementUnknown", err)
	}

	// A task that still blocks data writes holds the reclaim back.
	blocked := d.Clone()
	blocked.Migrations = map[string]storage.Migration{
		"move": {Request: storage.MigrationRequest{OperationID: "move", KBID: "kb", FromID: 1, ToID: 3}, Old: d.Placements["kb"], Phase: storage.MigrationFenced},
	}
	if _, _, err := (&LocalDataPlane{nodeID: 1, directory: planeDirectory{directory: blocked}}).cleanupPeers(ctx, "kb"); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Errorf("blocked task = %v, want ErrKBWritePaused", err)
	}

	// A stale epoch credential is refused even on the cleanup path.
	stale := &LocalDataPlane{nodeID: 1, directory: planeDirectory{directory: d}}
	if _, _, err := stale.cleanupPeers(contextWithPlaneEpoch(t, 9), "kb"); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("stale credential = %v, want ErrPlacementStale", err)
	}
}

func TestLocalDataPlane_ExecutionGateIsReadable(t *testing.T) {
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if (&LocalDataPlane{executionGate: gate}).ExecutionGate() != gate {
		t.Error("ExecutionGate does not return the wired gate")
	}
	if (&LocalDataPlane{}).ExecutionGate() != nil {
		t.Error("a plane without a gate reported one")
	}
}

type versionDataDropperFunc func(context.Context, string, int64) error

func (f versionDataDropperFunc) DropVersionStorage(ctx context.Context, kbID string, versionID int64) error {
	return f(ctx, kbID, versionID)
}

type kbStorageDropperFunc func(context.Context, string) error

func (f kbStorageDropperFunc) DropKnowledgeBase(ctx context.Context, kbID string) error {
	return f(ctx, kbID)
}
