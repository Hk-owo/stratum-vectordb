package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"

	serrors "stratum/internal/errors"
	"stratum/internal/types"
)

// bootstrapManager is the smallest control plane Bootstrap talks to. Every
// command it receives is recorded so the tests can assert on what was proposed,
// not only on the returned error — a bootstrap that "succeeds" without proposing
// anything leaves the cluster with no placements at all.
type bootstrapManager struct {
	dir         Directory
	dirErr      error
	readErr     error // StorageDirectory error on the Nth read (2 = after register)
	reads       int
	registerErr error
	assignErr   map[string]error
	assigned    []Placement
	registered  []Node
	placement   map[string]Snapshot
	placeErr    error
}

func (m *bootstrapManager) StorageDirectory(context.Context) (Directory, error) {
	m.reads++
	if m.reads == 1 && m.dirErr != nil {
		return Directory{}, m.dirErr
	}
	if m.reads > 1 && m.readErr != nil {
		return Directory{}, m.readErr
	}
	return m.dir.Clone(), nil
}

func (m *bootstrapManager) GetKBPlacement(_ context.Context, kbID string) (Snapshot, error) {
	if m.placeErr != nil {
		return Snapshot{}, m.placeErr
	}
	if snap, ok := m.placement[kbID]; ok {
		return snap, nil
	}
	return Snapshot{}, serrors.ErrKnowledgeBaseNotFound
}

func (m *bootstrapManager) ProposeRegisterStorageNodes(_ context.Context, _ uint64, nodes []Node) error {
	if m.registerErr != nil {
		return m.registerErr
	}
	m.registered = nodes
	for _, n := range nodes {
		if n.State == "" {
			n.State = NodeActive // RegisterNodes fills the default in.
		}
		m.dir.Nodes[n.ID] = n
	}
	m.dir.Revision++
	return nil
}

func (m *bootstrapManager) ProposeAssignKBPlacement(_ context.Context, p Placement) error {
	if err := m.assignErr[p.KBID]; err != nil {
		return err
	}
	m.assigned = append(m.assigned, p)
	m.dir.Placements[p.KBID] = p
	m.dir.Revision++
	return nil
}

type bootstrapInventory struct {
	kbs    []types.KnowledgeBaseMeta
	err    error
	onCall func()
}

func (i bootstrapInventory) ListKnowledgeBases(context.Context) ([]types.KnowledgeBaseMeta, error) {
	if i.onCall != nil {
		i.onCall()
	}
	return i.kbs, i.err
}

func registeredManager(t *testing.T, nodes []Node) *bootstrapManager {
	t.Helper()
	d := NewDirectory()
	if err := d.RegisterNodes(0, nodes); err != nil {
		t.Fatalf("seed directory: %v", err)
	}
	return &bootstrapManager{dir: d, assignErr: map[string]error{}, placement: map[string]Snapshot{}}
}

func TestInitialize_OnlyWritesAnEmptyDirectory(t *testing.T) {
	nodes := []Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}

	empty := &bootstrapManager{dir: NewDirectory(), assignErr: map[string]error{}, placement: map[string]Snapshot{}}
	if err := Initialize(context.Background(), empty, nodes); err != nil {
		t.Fatalf("empty directory: %v", err)
	}
	if !reflect.DeepEqual(empty.registered, nodes) {
		t.Errorf("registered = %+v, want the deployment hints", empty.registered)
	}

	// A directory that already knows nodes is authoritative: re-registering on
	// every restart would erase draining state and runtime registrations.
	existing := registeredManager(t, []Node{{ID: 7, Addr: "s7"}})
	before := existing.dir.Clone()
	if err := Initialize(context.Background(), existing, nodes); err != nil {
		t.Fatalf("existing directory: %v", err)
	}
	if existing.registered != nil || !reflect.DeepEqual(before, existing.dir) {
		t.Errorf("existing directory was rewritten: registered=%+v dir=%+v", existing.registered, existing.dir)
	}

	if err := Initialize(context.Background(), &bootstrapManager{dirErr: serrors.ErrPlacementStale}, nodes); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("read error = %v, want it surfaced", err)
	}
	if err := Initialize(context.Background(), &bootstrapManager{registerErr: serrors.ErrPlacementStale}, nodes); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("register error = %v, want it surfaced", err)
	}
}

func TestBootstrap_RejectsADifferentDeployment(t *testing.T) {
	ctx := context.Background()
	ideal := []Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}

	cases := []struct {
		name    string
		manager *bootstrapManager
		nodes   []Node
		want    error
	}{
		{
			name:    "fewer stored nodes than configured",
			manager: registeredManager(t, []Node{{ID: 1, Addr: "s1"}}),
			nodes:   ideal,
			want:    serrors.ErrPlacementChangeUnsafe,
		},
		{
			name:    "same count, unknown id",
			manager: registeredManager(t, []Node{{ID: 1, Addr: "s1"}, {ID: 9, Addr: "s9"}}),
			nodes:   ideal,
			want:    serrors.ErrPlacementChangeUnsafe,
		},
		{
			name:    "same id, different address",
			manager: registeredManager(t, []Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "moved"}}),
			nodes:   ideal,
			want:    serrors.ErrPlacementChangeUnsafe,
		},
		{
			name:    "configured node repeated",
			manager: registeredManager(t, ideal),
			nodes:   []Node{{ID: 1, Addr: "s1"}, {ID: 1, Addr: "s1"}},
			want:    serrors.ErrPlacementChangeUnsafe,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Bootstrap(ctx, tc.manager, bootstrapInventory{}, tc.nodes)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if len(tc.manager.assigned) != 0 {
				t.Errorf("a mismatched deployment still proposed placements: %+v", tc.manager.assigned)
			}
		})
	}
}

// The first leader on an empty directory registers the original deployment and
// then reads it back: the CAS revision comes from the directory it just wrote.
func TestBootstrap_RegistersBeforeAssigning(t *testing.T) {
	ctx := context.Background()
	nodes := []Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}
	m := &bootstrapManager{dir: NewDirectory(), assignErr: map[string]error{}, placement: map[string]Snapshot{}}
	inv := bootstrapInventory{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}, {KBID: "kb-2"}}}

	if err := Bootstrap(ctx, m, inv, nodes); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if !reflect.DeepEqual(m.registered, nodes) {
		t.Errorf("registered = %+v, want the deployment", m.registered)
	}
	if len(m.assigned) != 2 {
		t.Fatalf("assigned = %+v, want one placement per legacy KB", m.assigned)
	}
	// The original full set, at epoch 1, with the bootstrap operation id — the
	// id is what makes a re-run after a crash idempotent instead of a conflict.
	want := Placement{KBID: "kb-1", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "bootstrap/kb-1"}
	if !reflect.DeepEqual(m.assigned[0], want) {
		t.Errorf("placement = %+v, want %+v", m.assigned[0], want)
	}
}

func TestBootstrap_SkipsWhatItMustNotTouch(t *testing.T) {
	ctx := context.Background()
	nodes := []Node{{ID: 1, Addr: "s1"}}

	m := registeredManager(t, nodes)
	inv := bootstrapInventory{kbs: []types.KnowledgeBaseMeta{
		{KBID: "kb-inactive", Status: types.KBStatusDeleting},
		{KBID: "kb-placed"},
		{KBID: "kb-race"},
		{KBID: "kb-deleted"},
		{KBID: "kb-missing"},
	}}
	m.dir.Tombstones["kb-deleted"] = Tombstone{KBID: "kb-deleted", Epoch: 2}
	m.dir.Placements["kb-placed"] = Placement{KBID: "kb-placed", Epoch: 3, ReplicaIDs: []int64{1}, RequiredReplicas: 1, OperationID: "op-placed"}
	assigned := m.dir.Placements["kb-placed"]
	m.assignErr["kb-race"] = serrors.ErrPlacementChangeUnsafe
	m.placement["kb-race"] = Snapshot{Placement: m.dir.InitialPlacement("kb-race", "bootstrap/kb-race")}
	m.assignErr["kb-deleted"] = serrors.ErrKnowledgeBaseDeleted
	m.assignErr["kb-missing"] = serrors.ErrKnowledgeBaseNotFound

	if err := Bootstrap(ctx, m, inv, nodes); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if got := m.dir.Placements["kb-placed"]; !reflect.DeepEqual(got, assigned) {
		t.Errorf("existing placement = %+v, want it untouched (%+v)", got, assigned)
	}
	if len(m.assigned) != 0 {
		t.Errorf("assigned = %+v, want nothing: every KB is either placed, gone, or already assigned by the race", m.assigned)
	}
	if _, ok := m.dir.Placements["kb-inactive"]; ok {
		t.Error("a non-active KB got a placement")
	}
}

// A creation that committed between the directory read and the proposal wins.
// But the manager has to prove it is the SAME placement: a different membership
// under the same operation id is not a race, it is a conflict.
func TestBootstrap_RaceOnAssignmentIsCheckedNotAssumed(t *testing.T) {
	ctx := context.Background()
	nodes := []Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}
	m := registeredManager(t, nodes)
	m.assignErr["kb-1"] = serrors.ErrPlacementChangeUnsafe
	m.placement["kb-1"] = Snapshot{Placement: m.dir.InitialPlacement("kb-1", "bootstrap/kb-1")}
	if err := Bootstrap(ctx, m, bootstrapInventory{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}}}, nodes); err != nil {
		t.Fatalf("identical placement lost the race: %v", err)
	}

	conflict := registeredManager(t, nodes)
	conflict.assignErr["kb-1"] = serrors.ErrPlacementChangeUnsafe
	conflict.placement["kb-1"] = Snapshot{Placement: Placement{KBID: "kb-1", Epoch: 1, ReplicaIDs: []int64{1, 2, 3}, RequiredReplicas: 3, OperationID: "bootstrap/kb-1"}}
	err := Bootstrap(ctx, conflict, bootstrapInventory{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}}}, nodes)
	if !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Fatalf("err = %v, want the conflict surfaced", err)
	}

	// The same when the follow-up read itself fails: an unreadable directory is
	// not evidence that the placement is ours.
	blind := registeredManager(t, nodes)
	blind.assignErr["kb-1"] = serrors.ErrPlacementChangeUnsafe
	blind.placeErr = serrors.ErrPlacementStale
	if err := Bootstrap(ctx, blind, bootstrapInventory{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}}}, nodes); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Fatalf("unreadable placement: err = %v, want the proposal error", err)
	}
}

func TestBootstrap_PropagatesRealFailures(t *testing.T) {
	ctx := context.Background()
	nodes := []Node{{ID: 1, Addr: "s1"}}

	readFail := &bootstrapManager{dirErr: serrors.ErrPlacementStale, assignErr: map[string]error{}, placement: map[string]Snapshot{}}
	if err := Bootstrap(ctx, readFail, bootstrapInventory{}, nodes); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("directory read error = %v", err)
	}

	registerFail := &bootstrapManager{dir: NewDirectory(), registerErr: serrors.ErrPlacementStale, assignErr: map[string]error{}, placement: map[string]Snapshot{}}
	if err := Bootstrap(ctx, registerFail, bootstrapInventory{}, nodes); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("register error = %v", err)
	}

	// Registering succeeded but the read-back failed: without the read back the
	// membership check below has nothing to compare against.
	readBackFail := &bootstrapManager{dir: NewDirectory(), readErr: serrors.ErrPlacementStale, assignErr: map[string]error{}, placement: map[string]Snapshot{}}
	if err := Bootstrap(ctx, readBackFail, bootstrapInventory{}, nodes); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("read-back error = %v", err)
	}

	m := registeredManager(t, nodes)
	if err := Bootstrap(ctx, m, bootstrapInventory{err: serrors.ErrPlacementStale}, nodes); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("inventory error = %v", err)
	}

	failing := registeredManager(t, nodes)
	failing.assignErr["kb-1"] = errors.New("raft unavailable")
	err := Bootstrap(ctx, failing, bootstrapInventory{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}}}, nodes)
	if err == nil || !errors.Is(err, errors.New("raft unavailable")) && err.Error() != "bootstrap placement kb-1: raft unavailable" {
		t.Errorf("unknown proposal error = %v, want it wrapped with the KB id", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	stopped := registeredManager(t, nodes)
	err = Bootstrap(cancelled, stopped, bootstrapInventory{
		kbs:    []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
		onCall: cancel,
	}, nodes)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled bootstrap = %v, want context.Canceled", err)
	}
	if len(stopped.assigned) != 0 {
		t.Errorf("assigned after cancellation: %+v", stopped.assigned)
	}
}

func TestSameMembers_ComparesTheWholeConfiguration(t *testing.T) {
	base := Placement{KBID: "kb", Epoch: 2, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "op"}
	if !sameMembers(base, base.Clone()) {
		t.Error("identical placements differ")
	}
	if sameMembers(base, Placement{KBID: "kb", Epoch: 3, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "op"}) {
		t.Error("epoch is part of the configuration")
	}
	if sameMembers(base, Placement{KBID: "kb", Epoch: 2, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 1, OperationID: "op"}) {
		t.Error("quorum policy is part of the configuration")
	}
	if sameMembers(base, Placement{KBID: "kb", Epoch: 2, ReplicaIDs: []int64{1}, RequiredReplicas: 2, OperationID: "op"}) {
		t.Error("a shorter replica list is a different set")
	}
	if sameMembers(base, Placement{KBID: "kb", Epoch: 2, ReplicaIDs: []int64{1, 3}, RequiredReplicas: 2, OperationID: "op"}) {
		t.Error("different members are a different set")
	}
}
