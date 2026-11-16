package storage

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	serrors "stratum/internal/errors"
	"stratum/internal/types"
)

// fenceManager is a directory plus a record of every command the scheduler
// proposed. The scheduler's only real job is deciding *which* commands go out and
// in what order, so the assertions are on the recorded calls, not just on errors.
type fenceManager struct {
	mu          sync.Mutex
	dir         Directory
	dirErr      error
	tasks       []Migration
	listErr     error
	listCalls   int
	fenceErr    error
	ackErr      error
	stageErr    error
	installErr  error
	serveErr    error
	fenced      []string
	acked       []string
	staged      []MigrationStageReceipt
	installed   []MigrationInstallReceipt
	served      []MigrationServeReceipt
	proposed    []string
	onDirectory func()
}

func (m *fenceManager) StorageDirectory(context.Context) (Directory, error) {
	m.mu.Lock()
	hook := m.onDirectory
	dir, err := m.dir.Clone(), m.dirErr
	m.mu.Unlock()
	if hook != nil {
		hook()
	}
	return dir, err
}

// put registers a migration without caring whether the fixture already built the
// map, which keeps each case readable.
func (m *fenceManager) put(op string, mig Migration) {
	if m.dir.Migrations == nil {
		m.dir.Migrations = map[string]Migration{}
	}
	m.dir.Migrations[op] = mig
}

func (m *fenceManager) GetKBPlacement(context.Context, string) (Snapshot, error) {
	return Snapshot{}, serrors.ErrKnowledgeBaseNotFound
}

func (m *fenceManager) ListKBMigrations(context.Context) ([]Migration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listCalls++
	return append([]Migration(nil), m.tasks...), m.listErr
}

func (m *fenceManager) ProposeBeginKBMigration(context.Context, MigrationRequest) error {
	return nil
}

func (m *fenceManager) ProposeFenceKBMigration(_ context.Context, op string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.proposed = append(m.proposed, "fence/"+op)
	return m.fenceErr
}

func (m *fenceManager) ProposeAckKBMigrationFence(_ context.Context, op string, _ uint64, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = append(m.acked, op)
	return m.ackErr
}

func (m *fenceManager) ProposeCancelKBMigration(_ context.Context, op string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.proposed = append(m.proposed, "cancel/"+op)
	return nil
}

func (m *fenceManager) ProposeRecordKBMigrationStage(_ context.Context, op string, r MigrationStageReceipt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.staged = append(m.staged, r)
	return m.stageErr
}

func (m *fenceManager) ProposeRecordKBMigrationInstall(_ context.Context, op string, r MigrationInstallReceipt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.installed = append(m.installed, r)
	return m.installErr
}

func (m *fenceManager) ProposeRecordKBMigrationServe(_ context.Context, op string, r MigrationServeReceipt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.served = append(m.served, r)
	return m.serveErr
}

// leaderFlag is mutable so a test can observe the scheduler losing leadership
// half way through a pass, which is the failure mode the leader-lifetime checks
// exist for.
type leaderFlag struct {
	mu   sync.Mutex
	down bool
}

func (l *leaderFlag) isLeader() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.down
}

func (l *leaderFlag) lose() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.down = true
}

// migrationFixture builds a directory with two active storage nodes and one
// migration in the requested phase.
func migrationFixture(t *testing.T, m Migration) *fenceManager {
	t.Helper()
	d := NewDirectory()
	if err := d.RegisterNodes(0, []Node{{ID: 11, Addr: "s11"}, {ID: 12, Addr: "s12"}}); err != nil {
		t.Fatal(err)
	}
	d.Migrations = map[string]Migration{m.Request.OperationID: m}
	return &fenceManager{dir: d, tasks: []Migration{m}}
}

func newTask(phase MigrationPhase) Migration {
	return Migration{
		Request: MigrationRequest{OperationID: "op-1", KBID: "kb-1", ExpectedEpoch: 1, FromID: 11, ToID: 12},
		Old:     Placement{KBID: "kb-1", Epoch: 1, ReplicaIDs: []int64{11}, RequiredReplicas: 1, OperationID: "bootstrap/kb-1"},
		Phase:   phase,
	}
}

// A scheduler with no manager, no leadership probe, or no leadership does
// nothing at all — an ex-leader that kept acting would be the bug.
func TestFenceSchedulerStep_WithoutLeadershipDoesNothing(t *testing.T) {
	ctx := context.Background()
	if err := (&FenceScheduler{}).Step(ctx); err != nil {
		t.Errorf("no manager: %v", err)
	}
	if err := (&FenceScheduler{Manager: &fenceManager{}}).Step(ctx); err != nil {
		t.Errorf("no leadership probe: %v", err)
	}
	flag := &leaderFlag{}
	flag.lose()
	m := migrationFixture(t, newTask(MigrationFenced))
	svc := &FenceScheduler{Manager: m, IsLeader: flag.isLeader}
	if err := svc.Step(ctx); err != nil {
		t.Errorf("not leader: %v", err)
	}
	if m.listCalls != 0 {
		t.Errorf("a follower listed migrations: %d calls", m.listCalls)
	}
}

func TestFenceSchedulerStep_ListFailureIsReported(t *testing.T) {
	m := &fenceManager{listErr: serrors.ErrNoLeaderYet}
	svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
	if err := svc.Step(context.Background()); !errors.Is(err, serrors.ErrNoLeaderYet) {
		t.Fatalf("err = %v, want the list error", err)
	}
}

// Cleanups are independent of migrations: a KB deleted before its replicas were
// cleaned still owes those cleanups after a leader change.
func TestFenceSchedulerStep_RunsPendingCleanups(t *testing.T) {
	m := migrationFixture(t, newTask(MigrationActivated))
	m.dir.Cleanups = map[string]Cleanup{
		"done":   {ID: "done", KBID: "kb-1", NodeID: 11, Done: true},
		"orphan": {ID: "orphan", KBID: "kb-1", NodeID: 99},
		"live":   {ID: "live", KBID: "kb-1", NodeID: 12, Epoch: 2},
	}
	var cleaned []string
	svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
	svc.Cleanup = func(_ context.Context, n Node, c Cleanup) error {
		cleaned = append(cleaned, c.ID+"@"+n.Addr)
		return nil
	}
	if err := svc.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if !reflect.DeepEqual(cleaned, []string{"live@s12"}) {
		t.Errorf("cleaned = %v, want only the pending cleanup on a known node", cleaned)
	}

	// A cleanup failure is collected and reported, not fatal: the next tick
	// retries it, and a repeated cleanup is idempotent by construction.
	failing := migrationFixture(t, newTask(MigrationActivated))
	failing.dir.Cleanups = map[string]Cleanup{"live": {ID: "live", NodeID: 11}}
	failure := &FenceScheduler{Manager: failing, IsLeader: func() bool { return true }}
	failure.Cleanup = func(context.Context, Node, Cleanup) error { return serrors.ErrPlacementUnknown }
	if err := failure.Step(context.Background()); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("cleanup failure = %v, want it reported", err)
	}

	// Losing leadership during the cleanup loop stops the pass with a distinct
	// error, so a caller can tell "interrupted" from "nothing to do".
	flag := &leaderFlag{}
	losing := migrationFixture(t, newTask(MigrationActivated))
	losing.dir.Cleanups = map[string]Cleanup{"a": {ID: "a", NodeID: 11}, "b": {ID: "b", NodeID: 12}}
	interrupted := &FenceScheduler{Manager: losing, IsLeader: flag.isLeader}
	interrupted.Cleanup = func(context.Context, Node, Cleanup) error { flag.lose(); return nil }
	if err := interrupted.Step(context.Background()); !errors.Is(err, serrors.ErrNoLeaderYet) {
		t.Errorf("leadership lost mid-pass = %v, want ErrNoLeaderYet", err)
	}

	// The directory read for cleanups is not retried through attempt: if it
	// fails the whole pass fails, because cleanups left undone are silent.
	blind := migrationFixture(t, newTask(MigrationActivated))
	blind.dirErr = serrors.ErrPlacementStale
	blindSvc := &FenceScheduler{Manager: blind, IsLeader: func() bool { return true }}
	blindSvc.Cleanup = func(context.Context, Node, Cleanup) error { return nil }
	if err := blindSvc.Step(context.Background()); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("directory error = %v, want it surfaced", err)
	}
}

// FENCED + Stage: the scheduler copies an isolated artifact and records the
// receipt. It must not touch membership — nothing in this test changes epoch.
func TestFenceSchedulerStep_StageRecordsTheArtifact(t *testing.T) {
	task := newTask(MigrationFenced)
	task.FencedIDs = []int64{11, 12}
	m := migrationFixture(t, task)
	var staged []string
	svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
	svc.Stage = func(_ context.Context, target, source Node, _ Migration) (MigrationStageReceipt, error) {
		staged = append(staged, source.Addr+"->"+target.Addr)
		return MigrationStageReceipt{SourceID: source.ID, TargetID: target.ID, ManifestHash: "manifest", PayloadHash: "payload"}, nil
	}
	if err := svc.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if !reflect.DeepEqual(staged, []string{"s11->s12"}) {
		t.Errorf("staged = %v, want the source that carried the data", staged)
	}
	if len(m.staged) != 1 || m.staged[0].PayloadHash != "payload" {
		t.Errorf("staged receipts = %+v", m.staged)
	}

	cases := []struct {
		name   string
		mutate func(m *fenceManager)
		stage  func(target, source Node) (MigrationStageReceipt, error)
		leader func() bool
		want   error
	}{
		{
			name:   "task disappeared from the directory",
			mutate: func(m *fenceManager) { m.dir.Migrations = map[string]Migration{} },
		},
		{
			name:   "phase moved on under us",
			mutate: func(m *fenceManager) { m.put("op-1", newTask(MigrationStaged)) },
		},
		{
			// Fault mode without the committed revocation proof in the directory
			// is a stale directory, not a licence to skip the failed member.
			name: "fault mode without a committed revocation proof",
			mutate: func(m *fenceManager) {
				m.tasks = []Migration{faultedTask(MigrationFenced)}
				m.put("op-1", faultedTask(MigrationFenced))
			},
			want: serrors.ErrPlacementStale,
		},
		{
			name:   "target node unknown",
			mutate: func(m *fenceManager) { m.dir.Nodes = map[int64]Node{11: {ID: 11, Addr: "s11"}} },
			want:   serrors.ErrPlacementUnknown,
		},
		{
			name: "receipt is about a different target",
			stage: func(Node, Node) (MigrationStageReceipt, error) {
				return MigrationStageReceipt{SourceID: 11, TargetID: 99}, nil
			},
			want: serrors.ErrMigrationConflict,
		},
		{
			name: "stage itself fails",
			stage: func(Node, Node) (MigrationStageReceipt, error) {
				return MigrationStageReceipt{}, serrors.ErrPlacementUnknown
			},
			want: serrors.ErrPlacementUnknown,
		},
		{
			name: "recording the receipt fails",
			mutate: func(m *fenceManager) {
				m.stageErr = serrors.ErrPlacementStale
			},
			want: serrors.ErrPlacementStale,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := newTask(MigrationFenced)
			task.FencedIDs = []int64{11}
			manager := migrationFixture(t, task)
			if tc.mutate != nil {
				tc.mutate(manager)
			}
			leader := tc.leader
			if leader == nil {
				leader = func() bool { return true }
			}
			svc := &FenceScheduler{Manager: manager, IsLeader: leader}
			svc.Stage = func(_ context.Context, target, source Node, _ Migration) (MigrationStageReceipt, error) {
				if tc.stage != nil {
					return tc.stage(target, source)
				}
				return MigrationStageReceipt{SourceID: source.ID, TargetID: target.ID}, nil
			}
			err := svc.Step(context.Background())
			if tc.want == nil {
				if err != nil {
					t.Fatalf("err = %v, want a quiet pass", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}

	// A fenced member whose node is gone from the directory is not stageable,
	// but it is also not an error the pass should abort on: ErrMigrationBusy
	// says "still waiting", which is the truth.
	gone := migrationFixture(t, task)
	gone.dir.Nodes = map[int64]Node{12: {ID: 12, Addr: "s12"}}
	goneSvc := &FenceScheduler{Manager: gone, IsLeader: func() bool { return true }}
	goneSvc.Stage = func(context.Context, Node, Node, Migration) (MigrationStageReceipt, error) {
		return MigrationStageReceipt{}, nil
	}
	if err := goneSvc.Step(context.Background()); !errors.Is(err, serrors.ErrMigrationBusy) {
		t.Errorf("missing source node = %v, want ErrMigrationBusy", err)
	}
}

// STAGED + Install, then INSTALLED + Serve: both paths check that the node's
// receipt belongs to the task in the directory before it becomes evidence.
func TestFenceSchedulerStep_InstallAndServeCheckTheirReceipts(t *testing.T) {
	staged := newTask(MigrationStaged)
	staged.Stage = &MigrationStageReceipt{SourceID: 11, TargetID: 12, ManifestHash: "manifest", PayloadHash: "payload"}
	m := migrationFixture(t, staged)
	svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
	svc.Install = func(_ context.Context, target Node, task Migration) (MigrationInstallReceipt, error) {
		return MigrationInstallReceipt{SourceID: task.Stage.SourceID, TargetID: target.ID, ManifestHash: task.Stage.ManifestHash, StageHash: task.Stage.PayloadHash}, nil
	}
	if err := svc.Step(context.Background()); err != nil {
		t.Fatalf("install step: %v", err)
	}
	if len(m.installed) != 1 || m.installed[0].StageHash != "payload" {
		t.Fatalf("installed receipts = %+v", m.installed)
	}

	installed := newTask(MigrationInstalled)
	installed.Stage = staged.Stage
	installed.Install = &MigrationInstallReceipt{SourceID: 11, TargetID: 12, ManifestHash: "manifest", StageHash: "payload"}
	served := migrationFixture(t, installed)
	serveSvc := &FenceScheduler{Manager: served, IsLeader: func() bool { return true }}
	serveSvc.Serve = func(_ context.Context, target Node, task Migration) (MigrationServeReceipt, error) {
		return MigrationServeReceipt{TargetID: target.ID, ManifestHash: task.Install.ManifestHash, ActiveVersion: 7}, nil
	}
	if err := serveSvc.Step(context.Background()); err != nil {
		t.Fatalf("serve step: %v", err)
	}
	if len(served.served) != 1 || served.served[0].ActiveVersion != 7 {
		t.Fatalf("served receipts = %+v", served.served)
	}

	conflicting := migrationFixture(t, installed)
	conflictSvc := &FenceScheduler{Manager: conflicting, IsLeader: func() bool { return true }}
	conflictSvc.Serve = func(context.Context, Node, Migration) (MigrationServeReceipt, error) {
		// A receipt for a different artifact is not evidence about this one.
		return MigrationServeReceipt{TargetID: 12, ManifestHash: "other"}, nil
	}
	if err := conflictSvc.Step(context.Background()); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("mismatched serve receipt = %v, want ErrMigrationConflict", err)
	}

	// INSTALLED without a serve receipt and without a Serve hook is left alone.
	noHook := migrationFixture(t, installed)
	if err := (&FenceScheduler{Manager: noHook, IsLeader: func() bool { return true }}).Step(context.Background()); err != nil {
		t.Errorf("no serve hook: %v", err)
	}
}

// INSTALLED + a recorded serve receipt + Activate: the scheduler proposes the
// activation. Membership still only changes through that committed command.
func TestFenceSchedulerStep_ActivatesAnInstalledTask(t *testing.T) {
	task := newTask(MigrationInstalled)
	task.Install = &MigrationInstallReceipt{SourceID: 11, TargetID: 12, ManifestHash: "manifest", StageHash: "payload"}
	task.Serve = &MigrationServeReceipt{TargetID: 12, ManifestHash: "manifest"}
	m := migrationFixture(t, task)
	var activated []int64
	svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
	svc.Activate = func(_ context.Context, n Node, _ Migration) error {
		activated = append(activated, n.ID)
		return nil
	}
	if err := svc.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if !reflect.DeepEqual(activated, []int64{12}) {
		t.Errorf("activated = %v, want the target node", activated)
	}

	missing := migrationFixture(t, task)
	missing.dir.Nodes = map[int64]Node{11: {ID: 11, Addr: "s11"}}
	missingSvc := &FenceScheduler{Manager: missing, IsLeader: func() bool { return true }}
	missingSvc.Activate = func(context.Context, Node, Migration) error { return nil }
	if err := missingSvc.Step(context.Background()); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("missing target = %v, want ErrPlacementUnknown", err)
	}

	failing := migrationFixture(t, task)
	failSvc := &FenceScheduler{Manager: failing, IsLeader: func() bool { return true }}
	failSvc.Activate = func(context.Context, Node, Migration) error { return serrors.ErrMigrationConflict }
	if err := failSvc.Step(context.Background()); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("activation failure = %v, want it reported", err)
	}
}

// DRAINING waits for every version of the KB to stop being in flight, then
// proposes the fence. A KB with pending or deleting versions is left for the
// next pass: fencing mid-write would strand that write on a dead replica.
func TestFenceSchedulerStep_DrainingWaitsForQuietVersions(t *testing.T) {
	m := migrationFixture(t, newTask(MigrationDraining))
	svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
	if err := svc.Step(context.Background()); err != nil {
		t.Fatalf("step without a versions hook: %v", err)
	}
	if len(m.proposed) != 0 {
		t.Errorf("proposed = %v, want nothing without a versions hook", m.proposed)
	}

	quiet := migrationFixture(t, newTask(MigrationDraining))
	quietSvc := &FenceScheduler{Manager: quiet, IsLeader: func() bool { return true }}
	quietSvc.Versions = func(context.Context, string) ([]types.VersionMeta, error) {
		return []types.VersionMeta{{VersionID: 1, DataStatus: types.DataStatusDurable}}, nil
	}
	if err := quietSvc.Step(context.Background()); err != nil {
		t.Fatalf("quiet step: %v", err)
	}
	if !reflect.DeepEqual(quiet.proposed, []string{"fence/op-1"}) {
		t.Errorf("proposed = %v, want the fence command", quiet.proposed)
	}

	for name, versions := range map[string][]types.VersionMeta{
		"a deleting version": {{VersionID: 1, Deleting: true}},
		"a pending version":  {{VersionID: 2, DataStatus: types.DataStatusPending}},
	} {
		busy := migrationFixture(t, newTask(MigrationDraining))
		busySvc := &FenceScheduler{Manager: busy, IsLeader: func() bool { return true }}
		busySvc.Versions = func(context.Context, string) ([]types.VersionMeta, error) { return versions, nil }
		if err := busySvc.Step(context.Background()); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if len(busy.proposed) != 0 {
			t.Errorf("%s: proposed %v, want to wait", name, busy.proposed)
		}
	}

	unreadable := migrationFixture(t, newTask(MigrationDraining))
	unreadableSvc := &FenceScheduler{Manager: unreadable, IsLeader: func() bool { return true }}
	unreadableSvc.Versions = func(context.Context, string) ([]types.VersionMeta, error) {
		return nil, serrors.ErrPlacementStale
	}
	if err := unreadableSvc.Step(context.Background()); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("versions failure = %v, want it reported", err)
	}

	// ErrMigrationBusy from the fence command means "nothing to change", which is
	// the normal steady state of a task somebody else already fenced.
	settled := migrationFixture(t, newTask(MigrationDraining))
	settled.fenceErr = serrors.ErrMigrationBusy
	settledSvc := &FenceScheduler{Manager: settled, IsLeader: func() bool { return true }}
	settledSvc.Versions = func(context.Context, string) ([]types.VersionMeta, error) { return nil, nil }
	if err := settledSvc.Step(context.Background()); err != nil {
		t.Errorf("already fenced: %v", err)
	}

	rejected := migrationFixture(t, newTask(MigrationDraining))
	rejected.fenceErr = serrors.ErrPlacementStale
	rejectedSvc := &FenceScheduler{Manager: rejected, IsLeader: func() bool { return true }}
	rejectedSvc.Versions = func(context.Context, string) ([]types.VersionMeta, error) { return nil, nil }
	if err := rejectedSvc.Step(context.Background()); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("rejected fence = %v, want it reported", err)
	}
}

// FENCING: every required member is fenced and acknowledged individually. The
// list is persisted, so a pass that loses leadership half way through resumes
// without re-fencing the members that already acknowledged.
func TestFenceSchedulerStep_FencesEveryRequiredMember(t *testing.T) {
	task := newTask(MigrationFencing)
	task.Old.ReplicaIDs = []int64{11, 13}
	m := &fenceManager{dir: NewDirectory(), tasks: []Migration{task}}
	if err := m.dir.RegisterNodes(0, []Node{{ID: 11, Addr: "s11"}, {ID: 12, Addr: "s12"}, {ID: 13, Addr: "s13"}}); err != nil {
		t.Fatal(err)
	}
	task.FencedIDs = []int64{11} // 11 already acknowledged in an earlier pass.
	m.put("op-1", task)
	var fenced []int64
	svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
	svc.Fence = func(_ context.Context, n Node, _ Migration) error {
		fenced = append(fenced, n.ID)
		return nil
	}
	if err := svc.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if !reflect.DeepEqual(fenced, []int64{12, 13}) {
		t.Errorf("fenced = %v, want exactly the members not yet acknowledged", fenced)
	}
	if !reflect.DeepEqual(m.acked, []string{"op-1", "op-1"}) {
		t.Errorf("acked = %v, want one ack per fenced member", m.acked)
	}

	// A member with no address cannot be fenced: that is reported, and the pass
	// carries on so the rest of the set still makes progress.
	unknown := &fenceManager{dir: NewDirectory(), tasks: []Migration{task}}
	if err := unknown.dir.RegisterNodes(0, []Node{{ID: 11, Addr: "s11"}, {ID: 12, Addr: "s12"}}); err != nil {
		t.Fatal(err)
	}
	unknownTask := newTask(MigrationFencing)
	unknownTask.Old.ReplicaIDs = []int64{11, 13}
	unknownTask.FencedIDs = []int64{11}
	unknown.put("op-1", unknownTask)
	unknownSvc := &FenceScheduler{Manager: unknown, IsLeader: func() bool { return true }}
	unknownSvc.Fence = func(context.Context, Node, Migration) error { return nil }
	if err := unknownSvc.Step(context.Background()); !errors.Is(err, serrors.ErrPlacementUnknown) {
		t.Errorf("missing member = %v, want ErrPlacementUnknown", err)
	}

	// A Fence RPC failure is collected; the pass still tries the rest of the set
	// so one unreachable member does not stall the others.
	failing := &fenceManager{dir: NewDirectory(), tasks: []Migration{task}}
	if err := failing.dir.RegisterNodes(0, []Node{{ID: 11, Addr: "s11"}, {ID: 12, Addr: "s12"}, {ID: 13, Addr: "s13"}}); err != nil {
		t.Fatal(err)
	}
	failing.put("op-1", task)
	failingSvc := &FenceScheduler{Manager: failing, IsLeader: func() bool { return true }}
	failingSvc.Fence = func(_ context.Context, n Node, _ Migration) error {
		if n.ID == 12 {
			return serrors.ErrPlacementStale
		}
		return nil
	}
	err := failingSvc.Step(context.Background())
	if !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("fence failure = %v, want it reported", err)
	}
	if !reflect.DeepEqual(failing.acked, []string{"op-1"}) {
		t.Errorf("acked = %v, want only the member that succeeded", failing.acked)
	}

	// Losing leadership while fencing stops the pass: the next leader resumes
	// from the persisted ack list.
	flag := &leaderFlag{}
	losing := &fenceManager{dir: NewDirectory(), tasks: []Migration{task}}
	if err := losing.dir.RegisterNodes(0, []Node{{ID: 11, Addr: "s11"}, {ID: 12, Addr: "s12"}, {ID: 13, Addr: "s13"}}); err != nil {
		t.Fatal(err)
	}
	losing.put("op-1", task)
	losingSvc := &FenceScheduler{Manager: losing, IsLeader: flag.isLeader}
	losingSvc.Fence = func(context.Context, Node, Migration) error { flag.lose(); return nil }
	if err := losingSvc.Step(context.Background()); !errors.Is(err, serrors.ErrNoLeaderYet) {
		t.Errorf("leadership lost mid-fence = %v, want ErrNoLeaderYet", err)
	}

	// The directory moving on (phase changed, request replaced) ends this task's
	// handling without an error: somebody else's pass owns it now.
	moved := &fenceManager{dir: NewDirectory(), tasks: []Migration{newTask(MigrationFencing)}}
	if err := moved.dir.RegisterNodes(0, []Node{{ID: 11, Addr: "s11"}, {ID: 12, Addr: "s12"}}); err != nil {
		t.Fatal(err)
	}
	moved.put("op-1", newTask(MigrationStaged))
	movedSvc := &FenceScheduler{Manager: moved, IsLeader: func() bool { return true }}
	movedSvc.Fence = func(context.Context, Node, Migration) error { return nil }
	if err := movedSvc.Step(context.Background()); err != nil {
		t.Errorf("moved phase: %v", err)
	}
}

// A faulted migration may drop the failed member from the fence set, but only
// with the committed revocation proof in the directory — never on the task's own
// say-so.
func TestFenceSchedulerStep_FaultModeRequiresTheRevocationProof(t *testing.T) {
	task := faultedTask(MigrationDraining)
	m := migrationFixture(t, task)
	m.dir.ServiceBlocks = map[int64]ServiceBlock{11: {OperationID: "op-block", Revision: 3}}
	m.put("op-1", task)
	var revoked []int64
	svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
	svc.Revoke = func(_ context.Context, id int64, _ ServiceBlock) error {
		revoked = append(revoked, id)
		return nil
	}
	svc.Versions = func(context.Context, string) ([]types.VersionMeta, error) { return nil, nil }
	if err := svc.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if !reflect.DeepEqual(revoked, []int64{11}) {
		t.Errorf("revoked = %v, want the failed member", revoked)
	}
	if len(m.proposed) != 0 {
		t.Errorf("proposed = %v, want no fence before the proof is committed", m.proposed)
	}

	// Without a Revoke hook the task simply waits; the next pass retries.
	noHookErr := &fenceManager{dir: NewDirectory(), tasks: []Migration{faultedTask(MigrationDraining)}}
	if err := noHookErr.dir.RegisterNodes(0, []Node{{ID: 11, Addr: "s11"}, {ID: 12, Addr: "s12"}}); err != nil {
		t.Fatal(err)
	}
	// The proof exists but is not committed yet: ErrMigrationBusy, so the task
	// waits instead of dropping the failed member.
	noHookErr.dir.ServiceBlocks = map[int64]ServiceBlock{11: {OperationID: "op-block", Revision: 3}}
	noHookErr.put("op-1", faultedTask(MigrationDraining))
	noHookSvc := &FenceScheduler{Manager: noHookErr, IsLeader: func() bool { return true }}
	noHookSvc.Versions = func(context.Context, string) ([]types.VersionMeta, error) { return nil, nil }
	if err := noHookSvc.Step(context.Background()); err != nil {
		t.Errorf("no revoke hook: %v", err)
	}

	// A revocation RPC failure is collected, not swallowed.
	revokeFail := migrationFixture(t, task)
	revokeFail.dir.ServiceBlocks = map[int64]ServiceBlock{11: {OperationID: "op-block", Revision: 3}}
	revokeFail.put("op-1", task)
	revokeSvc := &FenceScheduler{Manager: revokeFail, IsLeader: func() bool { return true }}
	revokeSvc.Revoke = func(context.Context, int64, ServiceBlock) error { return serrors.ErrStorageServicePaused }
	if err := revokeSvc.Step(context.Background()); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Errorf("revocation failure = %v, want it reported", err)
	}

	// Once the proof is committed the task proceeds as an ordinary drain.
	proven := migrationFixture(t, task)
	provenTask := faultedTask(MigrationDraining)
	provenTask.FailedBlock.Revoked = true
	proven.dir.ServiceBlocks = map[int64]ServiceBlock{11: {OperationID: "op-block", Revision: 3, Revoked: true}}
	proven.put("op-1", provenTask)
	proven.tasks = []Migration{provenTask}
	provenSvc := &FenceScheduler{Manager: proven, IsLeader: func() bool { return true }}
	provenSvc.Versions = func(context.Context, string) ([]types.VersionMeta, error) { return nil, nil }
	if err := provenSvc.Step(context.Background()); err != nil {
		t.Fatalf("proven step: %v", err)
	}
	if !reflect.DeepEqual(proven.proposed, []string{"fence/op-1"}) {
		t.Errorf("proposed = %v, want the fence once the revocation is committed", proven.proposed)
	}

	// A directory that no longer carries the task (or carries a different
	// request under the same id) is somebody else's problem now.
	stale := migrationFixture(t, task)
	stale.put("op-1", faultedTask(MigrationCancelled))
	staleSvc := &FenceScheduler{Manager: stale, IsLeader: func() bool { return true }}
	staleSvc.Revoke = func(context.Context, int64, ServiceBlock) error { return nil }
	if err := staleSvc.Step(context.Background()); err != nil {
		t.Errorf("cancelled task: %v", err)
	}

	// Reading the fresh directory can fail; that is collected per task rather
	// than aborting the whole pass.
	blind := migrationFixture(t, task)
	blind.dirErr = serrors.ErrPlacementStale
	blindSvc := &FenceScheduler{Manager: blind, IsLeader: func() bool { return true }}
	if err := blindSvc.Step(context.Background()); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("directory read failure = %v, want it collected", err)
	}
}

// Cancellation between tasks ends the pass; the scheduler never keeps proposing
// on behalf of a context that is already done.
func TestFenceSchedulerStep_StopsOnCancellation(t *testing.T) {
	m := migrationFixture(t, newTask(MigrationDraining))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
	svc.Versions = func(context.Context, string) ([]types.VersionMeta, error) { return nil, nil }
	if err := svc.Step(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled step = %v, want context.Canceled", err)
	}
	if len(m.proposed) != 0 {
		t.Errorf("proposed after cancellation: %v", m.proposed)
	}
}

// runTask cancels the work as soon as this process stops being the leader, so a
// former leader never finishes a copy on the new leader's behalf.
func TestFenceSchedulerRunTask_CancelsWhenLeadershipIsLost(t *testing.T) {
	flag := &leaderFlag{}
	svc := &FenceScheduler{IsLeader: flag.isLeader, StageTimeout: 5 * time.Second}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := runTask(svc, context.Background(), func(c context.Context) (bool, error) {
			close(started)
			<-c.Done()
			return false, c.Err()
		})
		done <- err
	}()
	<-started
	flag.lose()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want the work cancelled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runTask did not stop after leadership was lost")
	}

	// The default budget applies when none is configured.
	budget := &FenceScheduler{IsLeader: func() bool { return true }}
	if _, err := runTask(budget, context.Background(), func(c context.Context) (int, error) {
		deadline, ok := c.Deadline()
		if !ok {
			t.Error("work ran without a deadline")
		}
		if time.Until(deadline) > 11*time.Minute {
			t.Errorf("default budget = %v, want 10m", time.Until(deadline))
		}
		return 1, nil
	}); err != nil {
		t.Fatalf("default budget: %v", err)
	}

	// attempt has its own, shorter default: an RPC budget must not become the
	// protocol budget.
	short := &FenceScheduler{}
	count := 0
	if err := short.attempt(context.Background(), func(c context.Context) error {
		deadline, ok := c.Deadline()
		if !ok {
			t.Error("attempt ran without a deadline")
		}
		if until := time.Until(deadline); until > 3*time.Second {
			t.Errorf("default attempt budget = %v, want 2s", until)
		}
		count++
		return serrors.ErrPlacementStale
	}); !errors.Is(err, serrors.ErrPlacementStale) {
		t.Errorf("attempt error = %v, want it returned", err)
	}
	if count != 1 {
		t.Errorf("attempt ran %d times, want exactly once", count)
	}
}

// Run is a ticker around Step: it must apply the interval default, tolerate a
// missing logger, and return as soon as its context ends.
func TestFenceSchedulerRun_TicksAndStops(t *testing.T) {
	m := migrationFixture(t, newTask(MigrationDraining))
	svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }, Interval: 5 * time.Millisecond, Logger: zap.NewNop()}
	svc.Versions = func(context.Context, string) ([]types.VersionMeta, error) { return nil, nil }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); svc.Run(ctx) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop with its context")
	}
	if len(m.proposed) == 0 {
		t.Error("Run never reached a pass")
	}

	// A nil logger with a failing Step must not panic, and the default interval
	// path is exercised by starting and immediately stopping.
	failing := &fenceManager{listErr: serrors.ErrPlacementStale}
	quiet := &FenceScheduler{Manager: failing, IsLeader: func() bool { return true }}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() { defer close(done2); quiet.Run(ctx2) }()
	cancel2()
	select {
	case <-done2:
	case <-time.After(5 * time.Second):
		t.Fatal("Run with the default interval did not stop")
	}
}

func faultedTask(phase MigrationPhase) Migration {
	task := newTask(phase)
	task.Request.Fault = true
	task.FailedBlock = ServiceBlock{OperationID: "op-block", Revision: 3}
	return task
}

// The three task handlers are called directly here: each one re-reads the
// directory and refuses to act on a task that has moved on, lost its target, or
// stopped being this node's to run. Those refusals are the safety property, so
// they are asserted one by one rather than through a whole pass.
func TestFenceSchedulerTaskHandlers_RefuseStaleWork(t *testing.T) {
	ctx := context.Background()
	staged := newTask(MigrationStaged)
	staged.Stage = &MigrationStageReceipt{SourceID: 11, TargetID: 12, ManifestHash: "manifest", PayloadHash: "payload"}
	installed := newTask(MigrationInstalled)
	installed.Install = &MigrationInstallReceipt{SourceID: 11, TargetID: 12, ManifestHash: "manifest", StageHash: "payload"}
	fenced := newTask(MigrationFenced)
	fenced.FencedIDs = []int64{11}

	installCases := []struct {
		name   string
		mutate func(m *fenceManager)
		hook   func(context.Context, Node, Migration) (MigrationInstallReceipt, error)
		leader func() bool
		want   error
	}{
		{
			name:   "directory unreadable",
			mutate: func(m *fenceManager) { m.dirErr = serrors.ErrPlacementStale },
			want:   serrors.ErrPlacementStale,
		},
		{
			name:   "task gone from the directory",
			mutate: func(m *fenceManager) { m.dir.Migrations = map[string]Migration{} },
		},
		{
			name: "task replaced by another request",
			mutate: func(m *fenceManager) {
				other := newTask(MigrationStaged)
				other.Request.ToID = 13
				m.put("op-1", other)
			},
		},
		{
			name:   "phase moved past staged",
			mutate: func(m *fenceManager) { m.put("op-1", newTask(MigrationInstalled)) },
		},
		{
			name:   "no stage receipt to install from",
			mutate: func(m *fenceManager) { empty := newTask(MigrationStaged); m.put("op-1", empty) },
		},
		{
			name: "fault mode whose revocation proof is not committed",
			mutate: func(m *fenceManager) {
				task := faultedTask(MigrationStaged)
				task.Stage = staged.Stage
				m.put("op-1", task)
				m.tasks = []Migration{task}
			},
			want: serrors.ErrPlacementStale,
		},
		{
			name:   "target node unknown",
			mutate: func(m *fenceManager) { m.dir.Nodes = map[int64]Node{11: {ID: 11, Addr: "s11"}} },
			want:   serrors.ErrPlacementUnknown,
		},
		{
			name:   "target node has no address",
			mutate: func(m *fenceManager) { m.dir.Nodes[12] = Node{ID: 12} },
			want:   serrors.ErrPlacementUnknown,
		},
		{
			name:   "not the leader",
			leader: func() bool { return false },
			want:   serrors.ErrNoLeaderYet,
		},
		{
			name: "install fails",
			hook: func(context.Context, Node, Migration) (MigrationInstallReceipt, error) {
				return MigrationInstallReceipt{}, serrors.ErrPlacementStale
			},
			want: serrors.ErrPlacementStale,
		},
		{
			name: "receipt names a different artifact",
			hook: func(context.Context, Node, Migration) (MigrationInstallReceipt, error) {
				return MigrationInstallReceipt{SourceID: 11, TargetID: 12, ManifestHash: "manifest", StageHash: "other"}, nil
			},
			want: serrors.ErrMigrationConflict,
		},
		{
			name:   "recording the install fails",
			mutate: func(m *fenceManager) { m.installErr = serrors.ErrPlacementStale },
			want:   serrors.ErrPlacementStale,
		},
	}
	for _, tc := range installCases {
		t.Run("install/"+tc.name, func(t *testing.T) {
			m := migrationFixture(t, staged)
			if tc.mutate != nil {
				tc.mutate(m)
			}
			leader := tc.leader
			if leader == nil {
				leader = func() bool { return true }
			}
			svc := &FenceScheduler{Manager: m, IsLeader: leader}
			svc.Install = func(_ context.Context, target Node, task Migration) (MigrationInstallReceipt, error) {
				if tc.hook != nil {
					return tc.hook(ctx, target, task)
				}
				return MigrationInstallReceipt{SourceID: task.Stage.SourceID, TargetID: target.ID, ManifestHash: task.Stage.ManifestHash, StageHash: task.Stage.PayloadHash}, nil
			}
			assertTaskError(t, svc.installTask(ctx, m.tasks[0]), tc.want)
		})
	}

	serveCases := []struct {
		name   string
		mutate func(m *fenceManager)
		hook   func(context.Context, Node, Migration) (MigrationServeReceipt, error)
		leader func() bool
		want   error
	}{
		{
			name:   "directory unreadable",
			mutate: func(m *fenceManager) { m.dirErr = serrors.ErrPlacementStale },
			want:   serrors.ErrPlacementStale,
		},
		{
			name:   "task gone from the directory",
			mutate: func(m *fenceManager) { m.dir.Migrations = map[string]Migration{} },
		},
		{
			name:   "phase moved past installed",
			mutate: func(m *fenceManager) { m.put("op-1", newTask(MigrationActivated)) },
		},
		{
			name:   "no install receipt to serve from",
			mutate: func(m *fenceManager) { m.put("op-1", newTask(MigrationInstalled)) },
		},
		{
			name: "already served",
			mutate: func(m *fenceManager) {
				served := installed
				served.Serve = &MigrationServeReceipt{TargetID: 12}
				m.put("op-1", served)
			},
		},
		{
			name:   "target node unknown",
			mutate: func(m *fenceManager) { m.dir.Nodes = map[int64]Node{11: {ID: 11, Addr: "s11"}} },
			want:   serrors.ErrPlacementUnknown,
		},
		{
			name:   "not the leader",
			leader: func() bool { return false },
			want:   serrors.ErrNoLeaderYet,
		},
		{
			name: "serve fails",
			hook: func(context.Context, Node, Migration) (MigrationServeReceipt, error) {
				return MigrationServeReceipt{}, serrors.ErrPlacementStale
			},
			want: serrors.ErrPlacementStale,
		},
		{
			name: "receipt names a different artifact",
			hook: func(context.Context, Node, Migration) (MigrationServeReceipt, error) {
				return MigrationServeReceipt{TargetID: 12, ManifestHash: "other"}, nil
			},
			want: serrors.ErrMigrationConflict,
		},
		{
			name:   "recording the serve evidence fails",
			mutate: func(m *fenceManager) { m.serveErr = serrors.ErrPlacementStale },
			want:   serrors.ErrPlacementStale,
		},
	}
	for _, tc := range serveCases {
		t.Run("serve/"+tc.name, func(t *testing.T) {
			m := migrationFixture(t, installed)
			if tc.mutate != nil {
				tc.mutate(m)
			}
			leader := tc.leader
			if leader == nil {
				leader = func() bool { return true }
			}
			svc := &FenceScheduler{Manager: m, IsLeader: leader}
			svc.Serve = func(_ context.Context, target Node, task Migration) (MigrationServeReceipt, error) {
				if tc.hook != nil {
					return tc.hook(ctx, target, task)
				}
				return MigrationServeReceipt{TargetID: target.ID, ManifestHash: task.Install.ManifestHash}, nil
			}
			assertTaskError(t, svc.serveTask(ctx, m.tasks[0]), tc.want)
		})
	}

	t.Run("stage/skips the revoked member", func(t *testing.T) {
		task := faultedTask(MigrationFenced)
		task.Old.ReplicaIDs = []int64{11, 12}
		task.Old.RequiredReplicas = 2
		task.FencedIDs = []int64{11, 12}
		m := migrationFixture(t, task)
		m.dir.ServiceBlocks = map[int64]ServiceBlock{11: {OperationID: "op-block", Revision: 3, Revoked: true}}
		task.FailedBlock.Revoked = true
		m.put("op-1", task)
		m.tasks = []Migration{task}
		var sources []int64
		svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
		svc.Stage = func(_ context.Context, target, source Node, _ Migration) (MigrationStageReceipt, error) {
			sources = append(sources, source.ID)
			return MigrationStageReceipt{SourceID: source.ID, TargetID: target.ID}, nil
		}
		if err := svc.stageTask(ctx, task); err != nil {
			t.Fatalf("stage: %v", err)
		}
		if !reflect.DeepEqual(sources, []int64{12}) {
			t.Errorf("sources = %v, want the revoked member skipped", sources)
		}
	})

	t.Run("stage/skips members that never fenced", func(t *testing.T) {
		task := newTask(MigrationFenced)
		task.FencedIDs = []int64{12} // only the target acknowledged, never the member
		m := migrationFixture(t, task)
		svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
		svc.Stage = func(context.Context, Node, Node, Migration) (MigrationStageReceipt, error) {
			t.Error("staged a member that never acknowledged its fence")
			return MigrationStageReceipt{}, nil
		}
		if err := svc.stageTask(ctx, task); !errors.Is(err, serrors.ErrMigrationBusy) {
			t.Fatalf("err = %v, want ErrMigrationBusy", err)
		}
	})

	t.Run("stage/no source left to copy from", func(t *testing.T) {
		task := newTask(MigrationFenced)
		task.FencedIDs = []int64{11}
		m := migrationFixture(t, task)
		m.dir.Nodes = map[int64]Node{12: {ID: 12, Addr: "s12"}}
		svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
		svc.Stage = func(context.Context, Node, Node, Migration) (MigrationStageReceipt, error) {
			return MigrationStageReceipt{}, nil
		}
		if err := svc.stageTask(ctx, task); !errors.Is(err, serrors.ErrMigrationBusy) {
			t.Fatalf("err = %v, want ErrMigrationBusy", err)
		}
	})

	t.Run("stage/leadership lost before copying", func(t *testing.T) {
		task := newTask(MigrationFenced)
		task.FencedIDs = []int64{11}
		m := migrationFixture(t, task)
		svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return false }}
		svc.Stage = func(context.Context, Node, Node, Migration) (MigrationStageReceipt, error) {
			t.Error("staged data without leadership")
			return MigrationStageReceipt{}, nil
		}
		if err := svc.stageTask(ctx, task); !errors.Is(err, serrors.ErrNoLeaderYet) {
			t.Fatalf("err = %v, want ErrNoLeaderYet", err)
		}
	})

	t.Run("stage/conflicting receipt is reported with the wait", func(t *testing.T) {
		task := newTask(MigrationFenced)
		task.FencedIDs = []int64{11}
		m := migrationFixture(t, task)
		svc := &FenceScheduler{Manager: m, IsLeader: func() bool { return true }}
		svc.Stage = func(_ context.Context, target, _ Node, _ Migration) (MigrationStageReceipt, error) {
			return MigrationStageReceipt{SourceID: 99, TargetID: target.ID}, nil
		}
		err := svc.stageTask(ctx, task)
		if !errors.Is(err, serrors.ErrMigrationConflict) || !errors.Is(err, serrors.ErrMigrationBusy) {
			t.Fatalf("err = %v, want the conflict and the pending wait", err)
		}
	})
}

func assertTaskError(t *testing.T, err, want error) {
	t.Helper()
	if want == nil {
		if err != nil {
			t.Fatalf("err = %v, want a quiet refusal", err)
		}
		return
	}
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}
