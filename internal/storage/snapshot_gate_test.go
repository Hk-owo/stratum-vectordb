package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/grpc/metadata"
	serrors "stratum/internal/errors"
	"stratum/internal/types"
)

func snapshotFixture() Snapshot {
	return Snapshot{
		Revision:      9,
		ActiveVersion: 4,
		Placement:     Placement{KBID: "kb", Epoch: 3, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"},
		Nodes:         []Node{{ID: 1, Addr: "s1", State: NodeActive}, {ID: 2, Addr: "s2", State: NodeDraining}},
	}
}

func TestSnapshotAddressesAndHasAddress(t *testing.T) {
	s := snapshotFixture()
	if got := s.Addresses(0); !reflect.DeepEqual(got, []string{"s1", "s2"}) {
		t.Errorf("addresses = %v", got)
	}
	if got := s.Addresses(1); !reflect.DeepEqual(got, []string{"s2"}) {
		t.Errorf("addresses without node 1 = %v, want the other member only", got)
	}
	if got := (&Snapshot{}).Addresses(1); len(got) != 0 {
		t.Errorf("addresses of an empty snapshot = %v", got)
	}
	if !s.HasAddress("s2") || s.HasAddress("s9") || (&Snapshot{}).HasAddress("s1") {
		t.Error("HasAddress disagrees with the node list")
	}
}

// The epoch header is the credential a remote write presents. Anything malformed,
// repeated, or missing has to be refused before the write reaches a store, and a
// bound snapshot stands in for it during local work.
func TestRequireRequestEpoch(t *testing.T) {
	ctx := context.Background()
	if err := RequireRequestEpoch(ctx, "kb"); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("no credential = %v, want ErrPlacementStale", err)
	}

	bound := Bind(ctx, snapshotFixture())
	if err := RequireRequestEpoch(bound, "kb"); err != nil {
		t.Errorf("bound snapshot = %v, want it accepted", err)
	}
	// A snapshot bound for another KB is not a credential for this one.
	if err := RequireRequestEpoch(bound, "other"); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("bound for another KB = %v", err)
	}

	withOne := metadata.NewIncomingContext(ctx, metadata.Pairs(epochHeader, "3"))
	if err := RequireRequestEpoch(withOne, "kb"); err != nil {
		t.Errorf("single epoch header = %v", err)
	}
	repeated := metadata.NewIncomingContext(ctx, metadata.Pairs(epochHeader, "3", epochHeader, "4"))
	if err := RequireRequestEpoch(repeated, "kb"); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("repeated credential = %v, want ErrPlacementStale", err)
	}
	empty := metadata.NewIncomingContext(ctx, metadata.MD{})
	if err := RequireRequestEpoch(empty, "kb"); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("empty metadata = %v, want ErrPlacementStale", err)
	}
}

// Resolve prefers the bound configuration, validates whatever the adapter returns,
// and treats an unknown KB carrying a credential as "this placement is gone"
// rather than "this KB never existed" — the caller retries on the former.
func TestResolve_ValidatesBeforeReturning(t *testing.T) {
	ctx := context.Background()
	bound := Bind(ctx, snapshotFixture())
	resolved := 0
	resolver := func(context.Context, string) (Snapshot, error) {
		resolved++
		return snapshotFixture(), nil
	}
	got, err := Resolve(bound, resolver, "kb")
	if err != nil || resolved != 0 {
		t.Fatalf("bound resolve = %v (resolver called %d times)", err, resolved)
	}
	if got.Placement.Epoch != 3 {
		t.Errorf("bound snapshot = %+v", got.Placement)
	}

	got, err = Resolve(ctx, resolver, "kb")
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}
	if got.ActiveVersion != 4 || len(got.Nodes) != 2 {
		t.Errorf("resolved snapshot = %+v, want the node list carried through", got)
	}

	// Adapters are validated too: two members with one address would be counted
	// as a quorum of two.
	duplicate := func(context.Context, string) (Snapshot, error) {
		s := snapshotFixture()
		s.Nodes[1].Addr = s.Nodes[0].Addr
		return s, nil
	}
	if _, err := Resolve(ctx, duplicate, "kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("duplicate address = %v, want ErrPlacementUnknown", err)
	}
	// Same identity twice is refused before the address check.
	twice := func(context.Context, string) (Snapshot, error) {
		s := snapshotFixture()
		s.Nodes[1].ID = s.Nodes[0].ID
		return s, nil
	}
	if _, err := Resolve(ctx, twice, "kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("duplicate node id = %v, want ErrPlacementUnknown", err)
	}
	// A placement the adapter got wrong is refused by the same rules a directory
	// read is held to.
	broken := func(context.Context, string) (Snapshot, error) {
		s := snapshotFixture()
		s.Placement.RequiredReplicas = 3
		return s, nil
	}
	if _, err := Resolve(ctx, broken, "kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("mismatched required replicas = %v, want ErrPlacementUnknown", err)
	}
	// A migration carried by the adapter stays attached to the resolved view.
	withMigration := func(context.Context, string) (Snapshot, error) {
		s := snapshotFixture()
		s.Migration = &Migration{Request: MigrationRequest{OperationID: "move", KBID: "kb"}, Phase: MigrationStaged}
		return s, nil
	}
	got, err = Resolve(ctx, withMigration, "kb")
	if err != nil || got.Migration == nil || got.Migration.Request.OperationID != "move" {
		t.Errorf("migration view = %+v, %v", got.Migration, err)
	}

	failing := func(context.Context, string) (Snapshot, error) { return Snapshot{}, serrors.ErrPlacementStale }
	if _, err := Resolve(ctx, failing, "kb"); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("resolver failure = %v, want it surfaced", err)
	}

	// A credential plus "not found" means the placement was removed under us; the
	// answer has to be retryable rather than terminal.
	notFound := func(context.Context, string) (Snapshot, error) { return Snapshot{}, serrors.ErrKnowledgeBaseNotFound }
	if _, err := Resolve(ctx, notFound, "kb"); !errors.Is(err, serrors.ErrKnowledgeBaseNotFound) {
		t.Errorf("without a credential = %v, want the original error", err)
	}
	authenticated := metadata.NewIncomingContext(ctx, metadata.Pairs(epochHeader, "3"))
	if _, err := Resolve(authenticated, notFound, "kb"); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("with a credential = %v, want ErrPlacementUnknown", err)
	}

	// The epoch the caller presented has to match the one just resolved.
	stale := metadata.NewIncomingContext(ctx, metadata.Pairs(epochHeader, "2"))
	if _, err := Resolve(stale, resolver, "kb"); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("stale credential = %v, want ErrPlacementStale", err)
	}
}

func TestCheckRequestEpoch_RejectsMalformedCredentials(t *testing.T) {
	ctx := context.Background()
	if err := CheckRequestEpoch(ctx, "kb", 3); err != nil {
		t.Errorf("no credential = %v, want it accepted", err)
	}
	for name, md := range map[string]metadata.MD{
		"not a number":    metadata.Pairs(epochHeader, "three"),
		"negative":        metadata.Pairs(epochHeader, "-3"),
		"different":       metadata.Pairs(epochHeader, "4"),
		"two credentials": metadata.Pairs(epochHeader, "3", epochHeader, "3"),
	} {
		err := CheckRequestEpoch(metadata.NewIncomingContext(ctx, md), "kb", 3)
		if !errors.Is(err, serrors.ErrPlacementStale) {
			t.Errorf("%s: err = %v, want ErrPlacementStale", name, err)
		}
	}
	if err := CheckRequestEpoch(metadata.NewIncomingContext(ctx, metadata.Pairs(epochHeader, "3")), "kb", 3); err != nil {
		t.Errorf("matching credential = %v", err)
	}
	// A bound snapshot of a newer epoch invalidates the request even when its own
	// header matches.
	newer := Bind(ctx, snapshotFixture())
	if err := CheckRequestEpoch(newer, "kb", 2); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("bound epoch ahead of the request = %v, want ErrPlacementStale", err)
	}
	if err := CheckRequestEpoch(newer, "other", 9); err != nil {
		t.Errorf("bound for another KB = %v, want it ignored", err)
	}
}

// The gate's own durability: a directory that cannot be created, a fence file that
// is not a regular file, and one that does not parse are all startup failures —
// silently ignoring them would reopen writes the previous process had fenced.
func TestNewExecutionGate_RejectsUnusableFenceDirectories(t *testing.T) {
	if _, err := NewExecutionGate(""); err == nil {
		t.Error("empty directory accepted")
	}

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "fence.json"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := NewExecutionGate(dir); err == nil {
		t.Error("a directory named like a fence file was accepted")
	}

	corrupt := t.TempDir()
	if err := os.WriteFile(filepath.Join(corrupt, "fence.json"), []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewExecutionGate(corrupt); err == nil {
		t.Error("a corrupt fence was accepted")
	}

	// A well-formed record filed under the wrong name is not this KB's fence.
	g := &ExecutionGate{dir: t.TempDir()}
	foreign := t.TempDir()
	name := filepath.Base(g.path("kb"))
	if err := os.WriteFile(filepath.Join(foreign, "other.json"), []byte(`{"kb_id":"kb","epoch":2,"operation_id":"op"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewExecutionGate(foreign); err == nil {
		t.Errorf("a fence stored under %q instead of %q was accepted", "other.json", name)
	}

	// A non-fence file is ignored: the directory may hold other state.
	other := t.TempDir()
	if err := os.WriteFile(filepath.Join(other, "readme.txt"), []byte("hi"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewExecutionGate(other); err != nil {
		t.Errorf("unrelated file rejected: %v", err)
	}
}

func TestExecutionGate_SetServiceLeaseAndEnterMaintenance(t *testing.T) {
	ctx := context.Background()
	g := testGate(t)
	g.SetServiceLease(nil)

	// A maintenance scope needs a whole cleanup identity: without it there is no
	// way to tell which deletion this scope belongs to.
	if _, _, err := g.EnterMaintenance(ctx, Cleanup{KBID: "kb", Epoch: 2}); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("no cleanup id = %v", err)
	}
	if _, _, err := g.EnterMaintenance(ctx, Cleanup{ID: "c", Epoch: 2}); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("no KB = %v", err)
	}
	if _, _, err := g.EnterMaintenance(ctx, Cleanup{ID: "c", KBID: "kb"}); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("no epoch = %v", err)
	}

	// A full cleanup owns the KB exclusively and persists its fence.
	cleanup := Cleanup{ID: "cleanup/op", KBID: "kb", Epoch: 2}
	scopeCtx, release, err := g.EnterMaintenance(ctx, cleanup)
	if err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	if _, ok := scopeCtx.Value(maintenanceScopeKey{}).(*maintenanceScope); !ok {
		t.Error("the scope context does not carry its scope")
	}
	// Ordinary mutations are refused while the cleanup holds the KB, at any epoch.
	if _, _, err := g.Enter(ctx, "kb", 2); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Errorf("mutation during cleanup = %v, want ErrKBWritePaused", err)
	}
	release()
	release() // idempotent
	// The fence outlives the scope: writes stay paused until a newer epoch exists.
	if _, _, err := g.Enter(ctx, "kb", 2); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Errorf("mutation after cleanup = %v, want the fence kept", err)
	}
	restarted, err := NewExecutionGate(g.dir)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if _, _, err := restarted.Enter(ctx, "kb", 2); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Error("restart forgot the cleanup fence")
	}

	// An artifacts-only scope never fences the KB: it may only delete files.
	artifacts := Cleanup{ID: "cleanup/artifacts", KBID: "kb2", Epoch: 1, ArtifactsOnly: true}
	if _, releaseArtifacts, err := g.EnterMaintenance(ctx, artifacts); err != nil {
		t.Fatalf("artifacts-only scope: %v", err)
	} else {
		releaseArtifacts()
	}
	if _, releaseWrite, err := g.Enter(ctx, "kb2", 1); err != nil {
		t.Errorf("artifacts-only cleanup fenced the KB: %v", err)
	} else {
		releaseWrite()
	}

	// A fence newer than the cleanup makes the cleanup stale, unless it only
	// touches artifacts.
	if _, _, err := g.EnterMaintenance(ctx, Cleanup{ID: "old", KBID: "kb", Epoch: 1}); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("older cleanup = %v, want ErrPlacementStale", err)
	}
	if _, release, err := g.EnterMaintenance(ctx, Cleanup{ID: "artifacts", KBID: "kb", Epoch: 1, ArtifactsOnly: true}); err != nil {
		t.Errorf("artifacts-only scope on a fenced KB = %v", err)
	} else {
		release()
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := g.EnterMaintenance(cancelled, Cleanup{ID: "c", KBID: "kb3", Epoch: 1}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled scope = %v, want context.Canceled", err)
	}

	if _, _, err := (*ExecutionGate)(nil).EnterMaintenance(ctx, cleanup); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("nil gate = %v", err)
	}
}

// A mutation whose task still blocks data writes is refused before the gate is
// even consulted: the fence is about the task, not about local state.
func TestEnterMutation_RefusesBlockedTasks(t *testing.T) {
	ctx := context.Background()
	g := testGate(t)
	s := snapshotFixture()
	s.Migration = &Migration{Request: MigrationRequest{OperationID: "move", KBID: "kb"}, Phase: MigrationFenced}
	if _, _, err := EnterMutation(ctx, g, s); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatalf("blocked task = %v, want ErrKBWritePaused", err)
	}
	s.Migration.Phase = MigrationStaged
	if _, _, err := EnterMutation(ctx, g, s); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatalf("staged task = %v, want ErrKBWritePaused", err)
	}
	s.Migration.Phase = MigrationDraining
	if _, release, err := EnterMutation(ctx, g, s); err != nil {
		t.Errorf("draining task = %v, want mutations admitted while it drains", err)
	} else {
		release()
	}
}

// BuildMigrationManifest is the contract a target verifies against, so every
// refusal has to be a distinct, stable error instead of a manifest that is merely
// incomplete.
func TestBuildMigrationManifest_Refusals(t *testing.T) {
	digest := types.EmptyDocIDSetHash
	durable := func(id, parent int64) types.VersionMeta {
		return types.VersionMeta{VersionID: id, ParentVersionID: parent, KBID: "kb", DataStatus: types.DataStatusDurable, DocIDSetHash: digest}
	}
	cases := []struct {
		name     string
		kb       string
		active   int64
		versions []types.VersionMeta
		want     error
	}{
		{name: "no knowledge base", kb: "", active: 0, want: serrors.ErrInvalidArgument},
		{name: "negative active version", kb: "kb", active: -1, want: serrors.ErrInvalidArgument},
		{name: "version of another KB", kb: "kb", active: 0, versions: []types.VersionMeta{{VersionID: 1, KBID: "other", DataStatus: types.DataStatusDurable, DocIDSetHash: digest}}, want: serrors.ErrInvalidArgument},
		{name: "version id zero", kb: "kb", active: 0, versions: []types.VersionMeta{{VersionID: 0, KBID: "kb", DataStatus: types.DataStatusDurable, DocIDSetHash: digest}}, want: serrors.ErrInvalidArgument},
		{name: "parent at or after the child", kb: "kb", active: 0, versions: []types.VersionMeta{{VersionID: 2, ParentVersionID: 2, KBID: "kb", DataStatus: types.DataStatusDurable, DocIDSetHash: digest}}, want: serrors.ErrInvalidArgument},
		{name: "duplicate version ids", kb: "kb", active: 1, versions: []types.VersionMeta{durable(1, 0), durable(1, 0)}, want: serrors.ErrInvalidArgument},
		{
			name: "a version is being deleted", kb: "kb", active: 0,
			versions: []types.VersionMeta{{VersionID: 1, KBID: "kb", Deleting: true, DataStatus: types.DataStatusDurable, DocIDSetHash: digest}},
			want:     serrors.ErrMigrationBusy,
		},
		{
			name: "a version is still pending", kb: "kb", active: 0,
			versions: []types.VersionMeta{{VersionID: 1, KBID: "kb", DataStatus: types.DataStatusPending}},
			want:     serrors.ErrMigrationBusy,
		},
		{
			name: "durable version without a committed digest", kb: "kb", active: 0,
			versions: []types.VersionMeta{{VersionID: 1, KBID: "kb", DataStatus: types.DataStatusDurable}},
			want:     serrors.ErrMigrationBusy,
		},
		{
			name: "version in an unusable state", kb: "kb", active: 0,
			versions: []types.VersionMeta{{VersionID: 1, KBID: "kb", DataStatus: types.DataStatus(99)}},
			want:     serrors.ErrMigrationConflict,
		},
		{
			name: "active version is not durable", kb: "kb", active: 1,
			versions: []types.VersionMeta{{VersionID: 1, KBID: "kb", DataStatus: types.DataStatusFailedPermanent}},
			want:     serrors.ErrMigrationConflict,
		},
		{
			name: "active version is missing", kb: "kb", active: 2,
			versions: []types.VersionMeta{durable(1, 0)},
			want:     serrors.ErrMigrationConflict,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := BuildMigrationManifest(tc.kb, tc.active, tc.versions); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}

	// The happy path sorts by version id and keeps the parent chain, because the
	// target replays versions in this order.
	m, err := BuildMigrationManifest("kb", 2, []types.VersionMeta{durable(2, 1), durable(1, 0)})
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(m.Versions) != 2 || m.Versions[0].ID != 1 || m.Versions[1].Parent != 1 {
		t.Errorf("manifest = %+v", m)
	}
	if m.ActiveVersion != 2 || m.KBID != "kb" {
		t.Errorf("manifest identity = %+v", m)
	}
}

// RecordMigrationStage is what turns "the target copied some bytes" into evidence,
// so each mismatch has to be refused rather than recorded.
func TestRecordMigrationStage_RefusesUnverifiedReceipts(t *testing.T) {
	drained := func(t *testing.T) Directory {
		t.Helper()
		d := rulesDirectory(t)
		if err := d.BeginMigration(migrationRequest()); err != nil {
			t.Fatal(err)
		}
		if err := d.StartFencing("op-1"); err != nil {
			t.Fatal(err)
		}
		for _, id := range []int64{1, 2, 3} {
			if err := d.AckFence("op-1", 1, id); err != nil {
				t.Fatal(err)
			}
		}
		return d
	}
	manifest := MigrationManifest{KBID: "kb", ActiveVersion: 0}
	good := MigrationStageReceipt{SourceID: 1, TargetID: 3, ManifestHash: manifest.Hash(), PayloadHash: manifest.Hash(), Entries: 1, Bytes: 10}

	if err := (&Directory{}).RecordMigrationStage("missing", good, manifest); !errors.Is(err, serrors.ErrMigrationNotFound) {
		t.Errorf("unknown operation = %v", err)
	}

	tasks := drained(t)
	cancelled := tasks.Clone()
	m := cancelled.Migrations["op-1"]
	m.Phase = MigrationCancelled
	cancelled.Migrations["op-1"] = m
	if err := cancelled.RecordMigrationStage("op-1", good, manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("cancelled task = %v, want ErrMigrationConflict", err)
	}

	cases := map[string]func(*MigrationStageReceipt, *MigrationManifest){
		"manifest of another KB":        func(*MigrationStageReceipt, *MigrationManifest) {},
		"receipt for another manifest":  func(r *MigrationStageReceipt, _ *MigrationManifest) { r.ManifestHash = "other" },
		"payload hash is not a sha256":  func(r *MigrationStageReceipt, _ *MigrationManifest) { r.PayloadHash = "payload" },
		"receipt for an empty artifact": func(r *MigrationStageReceipt, _ *MigrationManifest) { r.Bytes = 0 },
		"receipt for another target":    func(r *MigrationStageReceipt, _ *MigrationManifest) { r.TargetID = 2 },
		"receipt from a non-member":     func(r *MigrationStageReceipt, _ *MigrationManifest) { r.SourceID = 9 },
	}
	for name, mutate := range cases {
		d := drained(t)
		receipt, man := good, manifest
		if name == "manifest of another KB" {
			man.KBID = "other"
		} else {
			mutate(&receipt, &man)
		}
		if err := d.RecordMigrationStage("op-1", receipt, man); !errors.Is(err, serrors.ErrMigrationConflict) {
			t.Errorf("%s: err = %v, want ErrMigrationConflict", name, err)
		}
	}

	// A source that never acknowledged its fence cannot report on the artifact.
	unfenced := drained(t)
	m = unfenced.Migrations["op-1"]
	m.FencedIDs = []int64{2, 3}
	unfenced.Migrations["op-1"] = m
	if err := unfenced.RecordMigrationStage("op-1", good, manifest); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Errorf("incomplete fence set = %v, want ErrMigrationBusy", err)
	}

	// The directory moving on makes the receipt stale rather than acceptable.
	moved := drained(t)
	p := moved.Placements["kb"]
	p.Epoch = 5
	moved.Placements["kb"] = p
	if err := moved.RecordMigrationStage("op-1", good, manifest); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("moved placement = %v, want ErrPlacementStale", err)
	}

	// Once recorded, the same receipt is a retry and a different one is a conflict.
	recorded := drained(t)
	revision := recorded.Revision
	if err := recorded.RecordMigrationStage("op-1", good, manifest); err != nil {
		t.Fatalf("record: %v", err)
	}
	if recorded.Migrations["op-1"].Phase != MigrationStaged || recorded.Revision != revision+1 {
		t.Errorf("staged task = %+v rev=%d", recorded.Migrations["op-1"], recorded.Revision)
	}
	if err := recorded.RecordMigrationStage("op-1", good, manifest); err != nil || recorded.Revision != revision+1 {
		t.Errorf("retry = %v (revision %d)", err, recorded.Revision)
	}
	conflicting := good
	conflicting.Bytes = 11
	if err := recorded.RecordMigrationStage("op-1", conflicting, manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("different receipt = %v, want ErrMigrationConflict", err)
	}
}
