package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	serrors "stratum/internal/errors"
)

// ForNode is what a storage node is told about itself, so it must carry exactly
// its own obligations: a member set it is not part of is not its business, and an
// obligation it does own must never be dropped.
func sortedIDs(m map[int64]Node) []int64 {
	out := make([]int64, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// contextWithEpoch presents a placement epoch the way a peer would.
func contextWithEpoch(t *testing.T, epoch uint64) context.Context {
	t.Helper()
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(epochHeader, strconv.FormatUint(epoch, 10)))
}

func TestDirectoryForNode_KeepsOnlyThisNodesObligations(t *testing.T) {
	d := NewDirectory()
	if err := d.RegisterNodes(0, []Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}, {ID: 4, Addr: "s4"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.AssignNew(Placement{KBID: "mine", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/mine"}); err != nil {
		t.Fatal(err)
	}
	if err := d.AssignNew(Placement{KBID: "other", Epoch: 1, ReplicaIDs: []int64{2, 3}, RequiredReplicas: 2, OperationID: "create/other"}); err != nil {
		t.Fatal(err)
	}
	d.ActiveVersions = map[string]int64{"mine": 7, "other": 9}
	d.ServiceBlocks = map[int64]ServiceBlock{1: {OperationID: "block-1", Revision: 3}, 2: {OperationID: "block-2", Revision: 4}}
	d.addCleanup(Cleanup{ID: "mine-cleanup", KBID: "mine", NodeID: 1})
	d.addCleanup(Cleanup{ID: "other-cleanup", KBID: "other", NodeID: 2})
	d.Tombstones["mine"] = Tombstone{KBID: "mine", Epoch: 2, CleanupIDs: []int64{1, 2}}
	d.Tombstones["other"] = Tombstone{KBID: "other", Epoch: 2, CleanupIDs: []int64{2}}
	d.Migrations = map[string]Migration{
		"active":  {Request: MigrationRequest{OperationID: "active", KBID: "mine", FromID: 1, ToID: 4}, Old: d.Placements["mine"], Phase: MigrationFenced},
		"settled": {Request: MigrationRequest{OperationID: "settled", KBID: "mine", FromID: 1, ToID: 4}, Old: d.Placements["mine"], Phase: MigrationCancelled},
	}

	out := d.ForNode(1)
	if !reflect.DeepEqual(sortedIDs(out.Nodes), []int64{1, 2, 4}) {
		t.Errorf("nodes = %v, want the node, its peer, and the migration target", sortedIDs(out.Nodes))
	}
	if len(out.Placements) != 1 || !out.Placements["mine"].Contains(1) {
		t.Errorf("placements = %+v, want only the KBs this node serves", out.Placements)
	}
	if out.ActiveVersions["mine"] != 7 || len(out.ActiveVersions) != 1 {
		t.Errorf("active versions = %v", out.ActiveVersions)
	}
	if block, ok := out.ServiceBlocks[1]; !ok || block.OperationID != "block-1" || len(out.ServiceBlocks) != 1 {
		t.Errorf("service blocks = %+v, want only this node's", out.ServiceBlocks)
	}
	if _, ok := out.Cleanups["mine-cleanup"]; !ok || len(out.Cleanups) != 1 {
		t.Errorf("cleanups = %+v, want only this node's", out.Cleanups)
	}
	if _, ok := out.Tombstones["mine"]; !ok || len(out.Tombstones) != 1 {
		t.Errorf("tombstones = %+v", out.Tombstones)
	}
	if _, ok := out.Migrations["active"]; !ok || len(out.Migrations) != 1 {
		t.Errorf("migrations = %+v, want only the in-flight one", out.Migrations)
	}
	if out.Revision != d.Revision {
		t.Errorf("revision = %d, want %d", out.Revision, d.Revision)
	}

	// A node the directory has never heard of gets an empty, usable projection
	// rather than a panic or a nil map.
	stranger := d.ForNode(99)
	if len(stranger.Nodes) != 0 || len(stranger.Placements) != 0 || stranger.ServiceBlocks != nil {
		t.Errorf("unknown node projection = %+v", stranger)
	}
}

// The fence has to survive a fsync that never landed: the in-memory fence is kept
// conservative and the persist error is reported, so a retry re-persists instead of
// acknowledging a fence that is not on disk.
func TestExecutionGate_FenceKeepsTheInMemoryFenceWhenPersistFails(t *testing.T) {
	ctx := context.Background()
	gate := testGate(t)
	gate.dir = filepath.Join(gate.dir, "missing-child")

	err := gate.Fence(ctx, "kb", 2, "move")
	if err == nil {
		t.Fatal("a failed persistence was reported as success")
	}
	if _, _, err := gate.Enter(ctx, "kb", 2); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Errorf("write after a failed fence = %v, want the conservative fence kept", err)
	}
	// A retry repeats persistence rather than turning the failure into an ACK.
	if err := gate.Fence(ctx, "kb", 2, "move"); err == nil {
		t.Error("a retry reported success without persisting")
	}
}

func TestExecutionGate_FenceRefusals(t *testing.T) {
	ctx := context.Background()
	gate := testGate(t)
	if err := (*ExecutionGate)(nil).Fence(ctx, "kb", 1, "move"); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("nil gate = %v", err)
	}
	for name, tc := range map[string]struct {
		kb    string
		epoch uint64
		op    string
	}{
		"no knowledge base": {epoch: 1, op: "move"},
		"no epoch":          {kb: "kb", op: "move"},
		"no operation":      {kb: "kb", epoch: 1},
	} {
		if err := gate.Fence(ctx, tc.kb, tc.epoch, tc.op); !errors.Is(err, serrors.ErrInvalidArgument) {
			t.Errorf("%s: err = %v, want ErrInvalidArgument", name, err)
		}
	}

	if err := gate.Fence(ctx, "kb", 2, "move"); err != nil {
		t.Fatalf("fence: %v", err)
	}
	// Repeating the same fence is idempotent once it is durable.
	if err := gate.Fence(ctx, "kb", 2, "move"); err != nil {
		t.Errorf("repeated fence = %v", err)
	}
	// A different operation at the same epoch is a conflict; an older epoch is
	// stale. Neither may replace the fence that is already durable.
	if err := gate.Fence(ctx, "kb", 2, "other"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("different operation = %v, want ErrMigrationConflict", err)
	}
	if err := gate.Fence(ctx, "kb", 1, "move"); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("older epoch = %v, want ErrPlacementStale", err)
	}

	// A fence issued from inside an operation on the same KB cannot drain itself.
	held, release, err := gate.Enter(ctx, "kb2", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := gate.Fence(held, "kb2", 1, "move"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("self-fence = %v, want ErrMigrationConflict", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := gate.Fence(cancelled, "kb3", 1, "move"); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled fence = %v, want context.Canceled", err)
	}

	// A nested operation at the same epoch is free, and at a different epoch it is
	// stale: the caller already holds a configuration the fence has moved past.
	if _, _, err := gate.enterWith(held, "kb2", 2, true); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("nested enter at another epoch = %v, want ErrPlacementStale", err)
	}
	if _, _, err := gate.enterWith(ctx, "", 1, false); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("empty knowledge base = %v, want ErrPlacementUnknown", err)
	}
	if _, _, err := gate.enterWith(ctx, "kb4", 0, false); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("zero epoch = %v, want ErrPlacementUnknown", err)
	}
	// enterForCleanup admits the deletion's own epoch, which plain Enter refuses.
	if err := gate.Fence(ctx, "kb5", 3, "cleanup"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := gate.Enter(ctx, "kb5", 3); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatalf("plain enter at the fenced epoch = %v", err)
	}
	if _, release, err := gate.enterForCleanup(ctx, "kb5", 3); err != nil {
		t.Errorf("cleanup enter at the fenced epoch = %v", err)
	} else {
		release()
	}
}

// EnterImport admits only the task that owns the durable fence: a stale operation id
// or an epoch the fence has moved past is a refusal, never an admission.
func TestExecutionGate_EnterImportRequiresTheOwningTask(t *testing.T) {
	ctx := context.Background()
	gate := testGate(t)
	if _, _, err := gate.EnterImport(ctx, "", 1, "op"); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("empty knowledge base = %v", err)
	}
	if _, _, err := gate.EnterImport(ctx, "kb", 1, "op"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("unfenced KB = %v, want ErrMigrationConflict", err)
	}
	if err := gate.Fence(ctx, "kb", 1, "op"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := gate.EnterImport(ctx, "kb", 1, "other"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("another operation = %v, want ErrMigrationConflict", err)
	}
	_, release, err := gate.EnterImport(ctx, "kb", 1, "op")
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	// The import holds the KB exclusively: an ordinary write waits and then sees
	// the fence when its context expires.
	blocked, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, _, err := gate.Enter(blocked, "kb", 2); err == nil {
		t.Error("a write entered while an import held the KB")
	}
	release()
	release() // idempotent
}

// Cleanup admission is the reclaim path: it has to admit the deletion's own epoch and
// refuse a node the tombstone does not name, because that node would delete data it
// does not own.
func TestEnterCleanup_RequiresTheTombstoneOrMembership(t *testing.T) {
	ctx := context.Background()
	gate := testGate(t)
	if _, _, err := EnterCleanup(ctx, nil, nil, 1, "kb"); err != nil {
		t.Errorf("nil gate = %v", err)
	}
	if _, _, err := EnterCleanup(ctx, gate, nil, 1, "kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("nil reader = %v, want ErrPlacementUnknown", err)
	}

	d := NewDirectory()
	if err := d.RegisterNodes(0, []Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.AssignNew(Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"}); err != nil {
		t.Fatal(err)
	}
	reader := fenceDirectoryReader{d: d}

	if _, _, err := EnterCleanup(ctx, gate, reader, 9, "kb"); !errors.Is(err, serrors.ErrNotKBReplica) {
		t.Errorf("non-member = %v, want ErrNotKBReplica", err)
	}
	if _, release, err := EnterCleanup(ctx, gate, reader, 1, "kb"); err != nil {
		t.Errorf("member = %v", err)
	} else {
		release()
	}

	// A tombstone names exactly which nodes may still reclaim. The epoch admitted is
	// the deletion's own, one below the placement that replaced it.
	deleted := d.Clone()
	deleted.Delete("kb")
	deletedReader := fenceDirectoryReader{d: deleted}
	tomb := deleted.Tombstones["kb"]
	if _, _, err := EnterCleanup(ctx, gate, deletedReader, 9, "kb"); !errors.Is(err, serrors.ErrNotKBReplica) {
		t.Errorf("node outside the tombstone = %v, want ErrNotKBReplica", err)
	}
	_, release, err := EnterCleanup(ctx, gate, deletedReader, tomb.CleanupIDs[0], "kb")
	if err != nil {
		t.Fatalf("tombstone cleanup = %v", err)
	}
	release()

	// The epoch credential, when a peer presents one, still has to match.
	stale := contextWithEpoch(t, 9)
	if _, _, err := EnterCleanup(stale, gate, reader, 1, "kb"); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("stale credential = %v, want ErrPlacementStale", err)
	}

	broken := fenceDirectoryReader{d: NewDirectory()}
	if _, _, err := EnterCleanup(ctx, gate, broken, 1, "kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("unknown KB = %v, want ErrPlacementUnknown", err)
	}
}

// A paused lease keeps refusing work after the fact: the gate is checked on every
// entry, so a node that lost its authorization cannot keep writing on a context it
// obtained earlier through a fresh Enter.
func TestServiceLease_ForNodeAndPauseKeepTheGateClosed(t *testing.T) {
	lease := NewServiceLease(1, "s1")
	t.Cleanup(lease.Pause)
	d := NewDirectory()
	if err := d.RegisterNodes(0, []Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.AssignNew(Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"}); err != nil {
		t.Fatal(err)
	}
	d.ActiveVersions = map[string]int64{"kb": 3}
	d.Migrations = map[string]Migration{
		"move": {Request: MigrationRequest{OperationID: "move", KBID: "kb", FromID: 1, ToID: 2}, Old: d.Placements["kb"], Phase: MigrationFenced},
	}
	out := d.ForNode(1)
	if len(out.Migrations) != 1 || !out.Migrations["move"].Active() {
		t.Errorf("migration in the projection = %+v", out.Migrations)
	}
	// The projection is detached: changing it must not reach the directory.
	projected := out.Placements["kb"]
	projected.ReplicaIDs[0] = 99
	if d.Placements["kb"].ReplicaIDs[0] == 99 {
		t.Error("the projection aliases the live placement")
	}

	// An empty directory still projects to a usable value rather than a nil map.
	if got := (Directory{}).ForNode(1); got.Revision != 0 || got.ActiveVersions == nil {
		t.Errorf("empty projection = %+v", got)
	}
	if _, err := (Directory{}).Resolve("kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("resolving an unknown KB = %v", err)
	}
}

// Installing a lease is where a node is told it may serve again, so each refusal
// protects a different way of resuming service on evidence that is not current: a
// stale directory, an unresolvable placement, or a grant from the past.
func TestServiceLease_InstallRefusals(t *testing.T) {
	installFixture := func(t *testing.T) (*ServiceLease, Directory, time.Time) {
		t.Helper()
		g, dir, _, start := leasedNode(t)
		if err := dir.AssignNew(Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"}); err != nil {
			t.Fatal(err)
		}
		return g, dir, start
	}

	g, d, start := installFixture(t)
	if err := g.Install(start, 0, d.Revision, d); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("zero duration = %v, want ErrStorageServicePaused", err)
	}
	if err := g.Install(start, ServiceLeaseDuration+time.Second, d.Revision, d); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("oversized duration = %v, want ErrStorageServicePaused", err)
	}
	if err := g.Install(start, ServiceLeaseDuration, d.Revision+5, d); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("grant from the future = %v, want ErrStorageServicePaused", err)
	}

	// The lease is only valid for the node and address it was created with: an
	// unregistered node cannot be authorized at all.
	wrongNode := NewServiceLease(9, "s9")
	wrongNode.now = g.now
	t.Cleanup(wrongNode.Pause)
	if err := wrongNode.Install(start, ServiceLeaseDuration, d.Revision, d); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("unregistered node = %v, want ErrStorageServicePaused", err)
	}

	// Every placement this node is part of has to resolve, or the lease would resume
	// service for a KB whose members cannot be addressed.
	broken := d.Clone()
	broken.Nodes[2] = Node{ID: 2, Addr: "s1"} // the same address as node 1
	if err := g.Install(start, ServiceLeaseDuration, broken.Revision, broken); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("unresolvable placement = %v, want ErrPlacementUnknown", err)
	}

	if err := g.Install(start, ServiceLeaseDuration, d.Revision, d); err != nil {
		t.Fatalf("install: %v", err)
	}
	// Reinstalling a lease that started earlier would extend a generation the node
	// has already replaced.
	if err := g.Install(start.Add(-time.Second), ServiceLeaseDuration, d.Revision, d); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("earlier grant = %v, want ErrPlacementStale", err)
	}
	// A directory behind the one already accepted cannot extend the lease either.
	cooled, coldDir, coldStart := installFixture(t)
	if err := cooled.Install(coldStart, ServiceLeaseDuration, coldDir.Revision, coldDir); err != nil {
		t.Fatal(err)
	}
	behind := coldDir.Clone()
	behind.Revision = coldDir.Revision - 1
	if err := cooled.Install(coldStart.Add(time.Second), ServiceLeaseDuration, behind.Revision, behind); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("older directory = %v, want ErrStorageServicePaused", err)
	}
}

// persist must not report success when the rename or directory fsync did not land: a
// fence that is not on disk is gone after a restart, and the KB would reopen.
func TestExecutionGate_PersistRefusesAnUnusableTarget(t *testing.T) {
	gate := testGate(t)
	// A directory sitting where the fence file belongs makes the rename fail.
	if err := os.MkdirAll(gate.path("kb"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := gate.persist(fenceRecord{KBID: "kb", Epoch: 2, OperationID: "move"}); err == nil {
		t.Error("a fence was reported as persisted without a rename")
	}
	// The same through Fence: the in-memory fence stays conservative and the error
	// surfaces, so a retry repeats the persistence.
	if err := gate.Fence(context.Background(), "kb", 2, "move"); err == nil {
		t.Error("Fence reported success with an unusable fence path")
	}
	if _, _, err := gate.Enter(context.Background(), "kb", 2); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Errorf("write after a failed persistence = %v, want the fence kept", err)
	}

	// A gate whose directory disappeared cannot persist either.
	gone := testGate(t)
	if err := os.RemoveAll(gone.dir); err != nil {
		t.Fatal(err)
	}
	if err := gone.persist(fenceRecord{KBID: "kb", Epoch: 1, OperationID: "move"}); err == nil {
		t.Error("a fence was persisted into a directory that no longer exists")
	}
}

// A lease generation is what long transfers hold onto: Enter and Fork must both
// refuse a context that belongs to an older generation, or a renewed lease would let
// work started before a reconcile continue as if nothing happened.
func TestServiceLease_GenerationBoundaries(t *testing.T) {
	ctx := context.Background()
	g, d, _, start := leasedNode(t)
	if err := d.AssignNew(Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"}); err != nil {
		t.Fatal(err)
	}

	// A nil lease is "no authorization configured", not "paused".
	var nilLease *ServiceLease
	if c, release, err := nilLease.Enter(ctx); err != nil || c != ctx {
		t.Errorf("nil lease Enter = %v, %v", c, err)
	} else {
		release()
	}
	if c, release, err := nilLease.Fork(ctx); err != nil || c == nil {
		t.Errorf("nil lease Fork = %v, %v", c, err)
	} else {
		release()
	}

	// Before installation the lease is closed, and Enter/Fork both say so.
	if _, _, err := g.Enter(ctx); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("Enter before install = %v", err)
	}
	if _, _, err := g.Fork(ctx); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("Fork before install = %v", err)
	}

	if err := g.Install(start, ServiceLeaseDuration, d.Revision, d); err != nil {
		t.Fatal(err)
	}
	// A nested Enter inside the same scope is free, and the outer release does not
	// invalidate it.
	outer, releaseOuter, err := g.Enter(ctx)
	if err != nil {
		t.Fatalf("enter: %v", err)
	}
	nested, releaseNested, err := g.Enter(outer)
	if err != nil {
		t.Fatalf("nested enter: %v", err)
	}
	releaseNested()
	if err := g.CheckContext(nested); err != nil {
		t.Errorf("nested context rejected: %v", err)
	}

	// Forking keeps the same generation: finishing the caller does not cancel it.
	forked, releaseForked, err := g.Fork(outer)
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	if err := g.CheckContext(forked); err != nil {
		t.Errorf("forked context rejected: %v", err)
	}
	releaseOuter()
	if err := g.CheckContext(forked); err != nil {
		t.Errorf("fork did not outlive its caller: %v", err)
	}
	releaseForked()

	// A paused lease cancels the generation that was handed out earlier.
	paused, releasePaused, err := g.Enter(ctx)
	if err != nil {
		t.Fatalf("enter before pause: %v", err)
	}
	g.Pause()
	if err := g.CheckContext(paused); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("context after pause = %v, want ErrStorageServicePaused", err)
	}
	releasePaused()
	if _, _, err := g.Enter(ctx); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("enter after pause = %v, want ErrStorageServicePaused", err)
	}

	// A cancelled context is refused before the lease is consulted at all, so a
	// long operation cannot start on a context whose caller already gave up.
	fresh := NewServiceLease(1, "s1")
	fresh.now = g.now
	t.Cleanup(fresh.Pause)
	freshDir := d.Clone()
	if err := fresh.Install(start, ServiceLeaseDuration, freshDir.Revision, freshDir); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := fresh.Enter(cancelled); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled enter = %v, want context.Canceled", err)
	}
	if _, _, err := fresh.Fork(cancelled); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled fork = %v, want context.Canceled", err)
	}
}

// A fence waiting for in-flight writes gives up when its own context ends, and the
// KB is left fenced in memory rather than admitting a write mid-drain.
func TestExecutionGate_FenceWaitHonoursTheContext(t *testing.T) {
	ctx := context.Background()
	gate := testGate(t)
	_, release, err := gate.Enter(ctx, "kb", 1)
	if err != nil {
		t.Fatal(err)
	}
	waiting, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := gate.Fence(waiting, "kb", 1, "move"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("fence with an active write = %v, want the deadline", err)
	}
	// The write is still admitted while the fence never landed.
	release()
	if err := gate.Fence(ctx, "kb", 1, "move"); err != nil {
		t.Fatalf("fence after the write drained: %v", err)
	}
	if _, _, err := gate.Enter(ctx, "kb", 1); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Errorf("write after fence = %v", err)
	}
}

// Waits are the gate's whole point: an operation entering while a fence is draining,
// or a fence arriving while another is persisting, has to wait for the state to
// settle rather than race past it — and give up when its own context ends.
func TestExecutionGate_WaitsHonourTheirContext(t *testing.T) {
	ctx := context.Background()
	gate := testGate(t)

	// Hold a write so the fence has to drain against it.
	_, releaseWrite, err := gate.Enter(ctx, "kb", 1)
	if err != nil {
		t.Fatal(err)
	}
	fencing := make(chan struct{})
	fenced := make(chan error, 1)
	go func() {
		close(fencing)
		fenced <- gate.Fence(ctx, "kb", 1, "move")
	}()
	<-fencing
	// Wait until the fence owns the state, so the next call takes the fencing branch.
	for {
		gate.mu.Lock()
		fencingNow := gate.state("kb").fencing
		gate.mu.Unlock()
		if fencingNow {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// A second fence and an ordinary write both wait; with a short deadline they give
	// up instead of admitting a write mid-drain.
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := gate.Fence(short, "kb", 1, "move"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("second fence = %v, want the deadline", err)
	}
	if _, _, err := gate.Enter(short, "kb", 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("enter during a fence = %v, want the deadline", err)
	}

	releaseWrite()
	select {
	case err := <-fenced:
		if err != nil {
			t.Fatalf("the first fence did not land: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the first fence never finished")
	}
	if _, _, err := gate.Enter(ctx, "kb", 1); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Errorf("write after the fence = %v", err)
	}

	// A maintenance scope waits for an in-flight write too, and stops when its
	// context ends.
	_, releaseBusy, err := gate.Enter(ctx, "kb2", 1)
	if err != nil {
		t.Fatal(err)
	}
	maintenance, cancelMaintenance := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelMaintenance()
	if _, _, err := gate.EnterMaintenance(maintenance, Cleanup{ID: "c", KBID: "kb2", Epoch: 1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("maintenance during a write = %v, want the deadline", err)
	}
	releaseBusy()
	if _, release, err := gate.EnterMaintenance(ctx, Cleanup{ID: "c", KBID: "kb2", Epoch: 1}); err != nil {
		t.Errorf("maintenance after the write drained = %v", err)
	} else {
		release()
	}
}

// A fence directory the process cannot read is a startup failure: ignoring it would
// reopen writes the previous process had fenced.
func TestNewExecutionGate_RefusesAnUnreadableFenceDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits do not bind root")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fence.json"), []byte(`{"kb_id":"kb","epoch":1,"operation_id":"op"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := NewExecutionGate(dir); err == nil {
		t.Error("an unreadable fence directory was accepted")
	}
}

// A gate wired with a service lease consults it on every entry: an expired or paused
// lease closes the gate even though the local fence floor is untouched.
func TestExecutionGate_EnterConsultsTheServiceLease(t *testing.T) {
	ctx := context.Background()
	gate := testGate(t)
	lease := NewServiceLease(1, "s1")
	t.Cleanup(lease.Pause)
	gate.SetServiceLease(lease)

	// The lease starts closed, so nothing enters.
	if _, _, err := gate.Enter(ctx, "kb", 1); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("closed lease = %v, want ErrStorageServicePaused", err)
	}

	d := NewDirectory()
	if err := d.RegisterNodes(0, []Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.AssignNew(Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := lease.Install(start, ServiceLeaseDuration, d.Revision, d); err != nil {
		t.Fatal(err)
	}
	if _, release, err := gate.Enter(ctx, "kb", 1); err != nil {
		t.Errorf("authorized lease = %v", err)
	} else {
		release()
	}
	// Pausing the lease closes the gate again on the next entry.
	lease.Pause()
	if _, _, err := gate.Enter(ctx, "kb", 1); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("after pause = %v, want ErrStorageServicePaused", err)
	}
}

// A second import waits while the first holds the KB: the fence is exclusive, so the
// two cannot interleave their replays, and the wait honours its own context.
func TestExecutionGate_EnterImportWaitsForTheHoldingImport(t *testing.T) {
	ctx := context.Background()
	gate := testGate(t)
	if err := gate.Fence(ctx, "kb", 1, "op"); err != nil {
		t.Fatal(err)
	}
	_, release, err := gate.EnterImport(ctx, "kb", 1, "op")
	if err != nil {
		t.Fatal(err)
	}
	waiting, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, _, err := gate.EnterImport(waiting, "kb", 1, "op"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("second import = %v, want the deadline", err)
	}
	release()
	if _, releaseSecond, err := gate.EnterImport(ctx, "kb", 1, "op"); err != nil {
		t.Errorf("import after release = %v", err)
	} else {
		releaseSecond()
	}
}
