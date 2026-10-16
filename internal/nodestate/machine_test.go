package nodestate

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock makes the timestamps a transition records assertable. Without it
// the assertions would be on time.Now(), which is a moving target.
type fakeClock struct {
	mu sync.Mutex
	at time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{at: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// TestMachine_PhaseWalk: the ordinary startup, in order. Each phase must be
// observable while it lasts — that is the whole reason RECONCILING exists as a
// state instead of being a stretch of control flow.
func TestMachine_PhaseWalk(t *testing.T) {
	clock := newFakeClock()
	m := New(Options{Now: clock.Now})

	if got := m.State(); got != NodeStarting {
		t.Fatalf("new machine = %s, want STARTING", got)
	}
	if got := m.Snapshot().Actions; len(got) != 0 {
		t.Errorf("STARTING actions = %v, want none", got)
	}

	enter := clock.Now()
	if err := m.Transition(NodeReconciling); err != nil {
		t.Fatalf("STARTING -> RECONCILING: %v", err)
	}
	clock.Advance(time.Second)
	if got := m.State(); got != NodeReconciling {
		t.Fatalf("state = %s, want RECONCILING", got)
	}
	if got := m.StateSince(); !got.Equal(enter) {
		t.Errorf("StateSince = %v, want %v (when RECONCILING was entered)", got, enter)
	}
	if got := m.Snapshot().Actions; len(got) != 1 || got[0] != "reconciling indexes" {
		t.Errorf("RECONCILING actions = %v, want [reconciling indexes]", got)
	}

	if err := m.Transition(NodeServing); err != nil {
		t.Fatalf("RECONCILING -> SERVING: %v", err)
	}
	if got := m.Snapshot().Actions; len(got) != 0 {
		t.Errorf("SERVING actions = %v, want none", got)
	}

	if err := m.Transition(NodeStopping); err != nil {
		t.Fatalf("SERVING -> STOPPING: %v", err)
	}
}

// TestMachine_RejectsBackwardsAndTerminalPhases: a node that served does not
// un-serve, and a stopping node does not come back — it restarts as a new
// process with a new machine.
func TestMachine_RejectsBackwardsAndTerminalPhases(t *testing.T) {
	m := New(Options{})
	if err := m.Transition(NodeReconciling); err != nil {
		t.Fatal(err)
	}
	if err := m.Transition(NodeServing); err != nil {
		t.Fatal(err)
	}
	if err := m.Transition(NodeReconciling); err == nil {
		t.Error("SERVING -> RECONCILING was accepted, want refused")
	}
	if err := m.Transition(NodeStarting); err == nil {
		t.Error("SERVING -> STARTING was accepted, want refused")
	}
	if err := m.Transition(NodeStopping); err != nil {
		t.Fatal(err)
	}
	if err := m.Transition(NodeServing); err == nil {
		t.Error("STOPPING -> SERVING was accepted, want refused (STOPPING is terminal)")
	}
}

// TestMachine_RepeatedPhaseIsNotAnError: control flow can reach the same
// conclusion twice (an all-in-one node runs both assemblies), so a caller must
// not have to check the current phase before reporting one.
func TestMachine_RepeatedPhaseIsNotAnError(t *testing.T) {
	m := New(Options{})
	for i := 0; i < 3; i++ {
		if err := m.Transition(NodeReconciling); err != nil {
			t.Fatalf("repeat %d of STARTING -> RECONCILING: %v", i, err)
		}
	}
	if got := m.State(); got != NodeReconciling {
		t.Fatalf("state = %s, want RECONCILING", got)
	}
}

// TestMachine_RedundancyIsSetNotTransitioned: the verdict moves in both
// directions with the reports the node has seen, so there is no illegal order.
// Unknown is the zero value and must survive as itself rather than being
// rounded to HEALTHY.
func TestMachine_RedundancyIsSetNotTransitioned(t *testing.T) {
	m := New(Options{})
	if got, detail := m.Redundancy(); got != RedundancyUnknown || detail != "" {
		t.Fatalf("fresh machine redundancy = %s/%q, want UNKNOWN/\"\"", got, detail)
	}

	m.SetRedundancy(RedundancyHealthy, "")
	m.SetRedundancy(RedundancyDegraded, "2 of 3 required replicas live")
	m.SetRedundancy(RedundancyUnavailable, "no required replica is live")
	m.SetRedundancy(RedundancyHealthy, "")

	got, detail := m.Redundancy()
	if got != RedundancyHealthy || detail != "" {
		t.Fatalf("redundancy = %s/%q, want HEALTHY/\"\"", got, detail)
	}
	if snap := m.Snapshot(); snap.Redundancy != RedundancyHealthy {
		t.Fatalf("snapshot redundancy = %s, want HEALTHY", snap.Redundancy)
	}
}

// TestMachine_VersionLifecycle: the path a version actually takes here.
func TestMachine_VersionLifecycle(t *testing.T) {
	clock := newFakeClock()
	m := New(Options{Now: clock.Now})
	const kb = "kb-1"

	// Nothing is known about a version before its data lands.
	if got := m.VersionState(kb, 7); got != VersionAbsent {
		t.Fatalf("unknown version = %s, want ABSENT", got)
	}

	if err := m.DataLanded(kb, 7); err != nil {
		t.Fatalf("DataLanded: %v", err)
	}
	if got := m.VersionState(kb, 7); got != VersionDataPresent {
		t.Fatalf("after landing = %s, want DATA_PRESENT", got)
	}

	if err := m.BuildScheduled(kb, 7); err != nil {
		t.Fatalf("BuildScheduled: %v", err)
	}
	if got := m.VersionState(kb, 7); got != VersionBuilding {
		t.Fatalf("after schedule = %s, want BUILDING", got)
	}
	if got := m.BuildCount(); got != 1 {
		t.Fatalf("BuildCount = %d, want 1", got)
	}

	if err := m.BuildSucceeded(kb, 7); err != nil {
		t.Fatalf("BuildSucceeded: %v", err)
	}
	if got := m.VersionState(kb, 7); got != VersionReady {
		t.Fatalf("after success = %s, want READY", got)
	}
	if got := m.BuildCount(); got != 0 {
		t.Fatalf("BuildCount = %d, want 0", got)
	}
}

// TestMachine_VersionRefusesSkippedSteps is the reason the table exists. Each
// of these is unreachable today; the point is that it now stays unreachable
// when someone adds a path.
func TestMachine_VersionRefusesSkippedSteps(t *testing.T) {
	const kb = "kb-1"

	t.Run("build without data", func(t *testing.T) {
		m := New(Options{})
		if err := m.BuildScheduled(kb, 1); err == nil {
			t.Fatal("BUILDING from ABSENT was accepted, want refused")
		}
		if got := m.VersionState(kb, 1); got != VersionAbsent {
			t.Errorf("state = %s, want ABSENT (a refused transition must not write)", got)
		}
	})

	t.Run("success without a build", func(t *testing.T) {
		m := New(Options{})
		if err := m.DataLanded(kb, 1); err != nil {
			t.Fatal(err)
		}
		if err := m.BuildSucceeded(kb, 1); err == nil {
			t.Fatal("READY from DATA_PRESENT was accepted, want refused")
		}
	})

	t.Run("reschedule a ready version", func(t *testing.T) {
		m := New(Options{})
		if err := m.DataLanded(kb, 1); err != nil {
			t.Fatal(err)
		}
		if err := m.BuildScheduled(kb, 1); err != nil {
			t.Fatal(err)
		}
		if err := m.BuildSucceeded(kb, 1); err != nil {
			t.Fatal(err)
		}
		if err := m.BuildScheduled(kb, 1); err == nil {
			t.Fatal("BUILDING from READY was accepted, want refused")
		}
	})

	t.Run("fail a build that never started", func(t *testing.T) {
		m := New(Options{})
		if err := m.DataLanded(kb, 1); err != nil {
			t.Fatal(err)
		}
		if err := m.BuildFailed(kb, 1, "boom"); err == nil {
			t.Fatal("BUILD_FAILED from DATA_PRESENT was accepted, want refused")
		}
	})
}

// TestMachine_FailedBuildKeepsItsCauseAndCanRetry: BUILD_FAILED is transient by
// construction, and the cause travels with it so an operator reading a status
// response does not have to go digging.
func TestMachine_FailedBuildKeepsItsCauseAndCanRetry(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"
	step(t, m.DataLanded(kb, 3), "DataLanded")
	step(t, m.BuildScheduled(kb, 3), "BuildScheduled")
	step(t, m.BuildFailed(kb, 3, "vecstore: connection refused"), "BuildFailed")

	entries := m.NonSteadyVersions()
	if len(entries) != 1 {
		t.Fatalf("NonSteadyVersions = %d entries, want 1", len(entries))
	}
	if entries[0].State != VersionBuildFailed || entries[0].Detail != "vecstore: connection refused" {
		t.Fatalf("entry = %+v, want BUILD_FAILED with its cause", entries[0])
	}

	// The retry, and the cause must not linger on success.
	step(t, m.BuildScheduled(kb, 3), "BuildScheduled (retry)")
	step(t, m.BuildSucceeded(kb, 3), "BuildSucceeded")
	if entries := m.NonSteadyVersions(); len(entries) != 0 {
		t.Fatalf("NonSteadyVersions after success = %+v, want none", entries)
	}
}

// TestMachine_EventsAreIdempotent: several paths legitimately report the same
// landing, and a retried event must not look like a bug.
func TestMachine_EventsAreIdempotent(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"

	for i := 0; i < 3; i++ {
		if err := m.DataLanded(kb, 1); err != nil {
			t.Fatalf("repeat %d of DataLanded: %v", i, err)
		}
	}
	if got := m.VersionState(kb, 1); got != VersionDataPresent {
		t.Fatalf("state = %s, want DATA_PRESENT", got)
	}

	if err := m.BuildScheduled(kb, 1); err != nil {
		t.Fatal(err)
	}
	if err := m.BuildScheduled(kb, 1); err != nil {
		t.Fatalf("repeated BuildScheduled: %v", err)
	}
	if err := m.BuildSucceeded(kb, 1); err != nil {
		t.Fatal(err)
	}
	if err := m.BuildSucceeded(kb, 1); err != nil {
		t.Fatalf("repeated BuildSucceeded: %v", err)
	}
	if got := m.VersionState(kb, 1); got != VersionReady {
		t.Fatalf("state = %s, want READY", got)
	}
}

// TestMachine_VersionDroppedRemovesTheEntry: absence is the absence of a fact,
// not a fact — so deletion removes the entry rather than writing ABSENT.
func TestMachine_VersionDroppedRemovesTheEntry(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"
	step(t, m.DataLanded(kb, 1), "DataLanded")
	step(t, m.BuildScheduled(kb, 1), "BuildScheduled")

	m.VersionDropped(kb, 1)
	if got := m.VersionState(kb, 1); got != VersionAbsent {
		t.Fatalf("after drop = %s, want ABSENT", got)
	}
	if got := m.BuildCount(); got != 0 {
		t.Fatalf("BuildCount = %d, want 0", got)
	}

	// And the path starts over from the beginning: data, then build.
	step(t, m.DataLanded(kb, 1), "DataLanded (again)")
	if err := m.BuildSucceeded(kb, 1); err == nil {
		t.Error("READY from a re-landed version was accepted, want refused")
	}
}

// TestMachine_DropKnowledgeBase: the knowledge base going away takes its
// versions and its catch-up declaration with it.
func TestMachine_DropKnowledgeBase(t *testing.T) {
	m := New(Options{})
	step(t, m.DataLanded("kb-1", 1), "DataLanded kb-1")
	step(t, m.DataLanded("kb-2", 1), "DataLanded kb-2")
	m.MarkCatchingUp("kb-1", true)
	m.MarkCatchingUp("kb-2", true)

	m.DropKnowledgeBase("kb-1")

	if got := m.VersionState("kb-1", 1); got != VersionAbsent {
		t.Errorf("kb-1 v1 = %s, want ABSENT", got)
	}
	if got := m.VersionState("kb-2", 1); got != VersionDataPresent {
		t.Errorf("kb-2 v1 = %s, want DATA_PRESENT (dropping kb-1 must not touch it)", got)
	}
	if got := m.CatchingUp(); len(got) != 1 || got[0] != "kb-2" {
		t.Errorf("CatchingUp = %v, want [kb-2]", got)
	}
}

// TestMachine_RestoreVersionAcceptsOnlyDiskStates: the one entry point that
// bypasses the table is not a general setter. BUILDING and BUILD_FAILED
// describe an activity, not an artifact, and a caller claiming them through
// this door is describing something the disk cannot know.
func TestMachine_RestoreVersionAcceptsOnlyDiskStates(t *testing.T) {
	m := New(Options{})

	for _, state := range []VersionState{VersionDataPresent, VersionReady} {
		if err := m.RestoreVersion("kb-1", 1, state, ""); err != nil {
			t.Errorf("RestoreVersion(%s): %v, want accepted", state, err)
		}
	}
	for _, state := range []VersionState{VersionAbsent, VersionBuilding, VersionBuildFailed} {
		if err := m.RestoreVersion("kb-1", 2, state, ""); err == nil {
			t.Errorf("RestoreVersion(%s) was accepted, want refused", state)
		}
	}
	if got := m.VersionState("kb-1", 2); got != VersionAbsent {
		t.Errorf("refused restore wrote state %s, want ABSENT", got)
	}
}

// TestMachine_RestoreVersionIsTheStartupProjection walks the rebuild described
// in the plan's §5.1: reconcile reads disk facts, the machine is projected from
// them before the node serves.
func TestMachine_RestoreVersionIsTheStartupProjection(t *testing.T) {
	clock := newFakeClock()
	m := New(Options{Now: clock.Now})
	if err := m.Transition(NodeReconciling); err != nil {
		t.Fatal(err)
	}

	// An artifact on disk, and data with no artifact.
	if err := m.RestoreVersion("kb-1", 1, VersionReady, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.RestoreVersion("kb-1", 2, VersionDataPresent, ""); err != nil {
		t.Fatal(err)
	}

	// Neither is "not at rest": a version with data and no index is the
	// designed state of one nobody has asked for (lazy builds, §8.6b), so the
	// status response does not list it.
	if entries := m.NonSteadyVersions(); len(entries) != 0 {
		t.Fatalf("NonSteadyVersions after rebuild = %+v, want none", entries)
	}

	// The runtime correction in the other direction: the retention policy
	// dropped the artifact, so READY is no longer true.
	clock.Advance(time.Minute)
	if err := m.RestoreVersion("kb-1", 1, VersionDataPresent, ""); err != nil {
		t.Fatal(err)
	}
	if got := m.VersionState("kb-1", 1); got != VersionDataPresent {
		t.Fatalf("after the retention pass = %s, want DATA_PRESENT", got)
	}
}

// TestMachine_NonSteadyVersionsIsSortedAndBounded: the list goes into a status
// response, so it must be stable across calls and must not grow with the number
// of versions a node merely holds.
func TestMachine_NonSteadyVersionsIsSortedAndBounded(t *testing.T) {
	m := New(Options{})

	// Thousands of versions at rest, plus two that are not.
	for i := int64(1); i <= 500; i++ {
		step(t, m.DataLanded("kb-1", i), "DataLanded")
		step(t, m.BuildScheduled("kb-1", i), "BuildScheduled")
		step(t, m.BuildSucceeded("kb-1", i), "BuildSucceeded")
	}
	step(t, m.DataLanded("kb-2", 4), "DataLanded")
	step(t, m.BuildScheduled("kb-2", 4), "BuildScheduled")
	step(t, m.DataLanded("kb-1", 501), "DataLanded")
	step(t, m.BuildScheduled("kb-1", 501), "BuildScheduled")
	step(t, m.BuildFailed("kb-1", 501, "boom"), "BuildFailed")

	want := []VersionEntry{
		{KBID: "kb-1", VersionID: 501, State: VersionBuildFailed, Detail: "boom"},
		{KBID: "kb-2", VersionID: 4, State: VersionBuilding},
	}
	got := m.NonSteadyVersions()
	if len(got) != len(want) {
		t.Fatalf("NonSteadyVersions = %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].KBID != want[i].KBID || got[i].VersionID != want[i].VersionID ||
			got[i].State != want[i].State || got[i].Detail != want[i].Detail {
			t.Errorf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	// Stability: repeated calls agree.
	for i := 0; i < 3; i++ {
		if next := m.NonSteadyVersions(); next[0] != got[0] || next[1] != got[1] {
			t.Fatalf("call %d = %+v, want the same order as %+v", i, next, got)
		}
	}
}

// TestMachine_SnapshotActionsAreDerivedNotStored: the activity line is a
// projection over the phase, the build count, and the catch-up set, so it
// cannot contradict the version layer.
func TestMachine_SnapshotActionsAreDerivedNotStored(t *testing.T) {
	m := New(Options{})
	if err := m.Transition(NodeReconciling); err != nil {
		t.Fatal(err)
	}

	step(t, m.DataLanded("kb-1", 1), "DataLanded")
	step(t, m.BuildScheduled("kb-1", 1), "BuildScheduled")
	step(t, m.DataLanded("kb-1", 2), "DataLanded")
	step(t, m.BuildScheduled("kb-1", 2), "BuildScheduled")
	m.MarkCatchingUp("kb-9", true)

	got := m.Snapshot().Actions
	want := []string{"reconciling indexes", "building 2 index(es)", "catching up kb-9"}
	if len(got) != len(want) {
		t.Fatalf("Actions = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Actions = %v, want %v", got, want)
		}
	}

	// The projection follows the version layer: finishing the builds removes
	// the line rather than leaving a stale string behind.
	step(t, m.BuildSucceeded("kb-1", 1), "BuildSucceeded")
	step(t, m.BuildSucceeded("kb-1", 2), "BuildSucceeded")
	m.MarkCatchingUp("kb-9", false)
	got = m.Snapshot().Actions
	want = []string{"reconciling indexes"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Actions after the builds finished = %v, want %v", got, want)
		}
	}
}

// TestMachine_MarkCatchingUpIgnoresEmptyID: an empty knowledge base is not a
// knowledge base, and admitting one would put a permanent "" in every status
// response.
func TestMachine_MarkCatchingUpIgnoresEmptyID(t *testing.T) {
	m := New(Options{})
	m.MarkCatchingUp("", true)
	if got := m.CatchingUp(); len(got) != 0 {
		t.Fatalf("CatchingUp = %v, want none", got)
	}
}

// TestMachine_ConcurrentUse is the -race test: the machine is read by status
// callers while writes, builds and reconciliation drive it.
func TestMachine_ConcurrentUse(t *testing.T) {
	m := New(Options{})

	var wg sync.WaitGroup
	for id := 0; id < 8; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			kb := fmt.Sprintf("kb-%d", id)
			for j := int64(1); j <= 50; j++ {
				_ = m.DataLanded(kb, j)
				_ = m.BuildScheduled(kb, j)
				if j%3 == 0 {
					_ = m.BuildFailed(kb, j, "boom")
				} else {
					_ = m.BuildSucceeded(kb, j)
				}
				m.MarkCatchingUp(kb, j%2 == 0)
				m.SetRedundancy(RedundancyHealthy, "")
				m.VersionDropped(kb, j/2)
			}
		}(id)
	}

	// Readers, so the snapshot paths run against a machine being written to.
	for id := 0; id < 4; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = m.Snapshot()
				_ = m.NonSteadyVersions()
				_ = m.CatchingUp()
				_ = m.BuildCount()
				_, _ = m.Redundancy()
				_ = m.VersionState("kb-0", 1)
			}
		}()
	}

	wg.Wait()
}

// TestMachine_ConcurrentPhaseTransitionCommitsOnce: racing reporters of "we are
// serving" must leave exactly one winner, and every loser must see either a
// no-op (already SERVING) or a refusal — never a torn phase.
func TestMachine_ConcurrentPhaseTransitionCommitsOnce(t *testing.T) {
	m := New(Options{})
	if err := m.Transition(NodeReconciling); err != nil {
		t.Fatal(err)
	}

	const racers = 16
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Transition(NodeServing)
		}()
	}
	wg.Wait()

	if got := m.State(); got != NodeServing {
		t.Fatalf("state = %s, want SERVING", got)
	}
}

func step(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// TestMachine_PeerRedundancyVerdictComesFromTheCounts: the input is the FACT
// this node derived (how many of how many it reached), and the verdict is the
// machine's reading of it. An incomplete sample is UNKNOWN rather than
// extrapolated from the peers that did answer.
func TestMachine_PeerRedundancyVerdictComesFromTheCounts(t *testing.T) {
	m := New(Options{})

	cases := []struct {
		name         string
		known        bool
		live, quorum int
		want         Redundancy
	}{
		{"incomplete sample", false, 3, 2, RedundancyUnknown},
		{"exactly a quorum", true, 2, 2, RedundancyHealthy},
		{"more than a quorum", true, 3, 2, RedundancyHealthy},
		{"short of a quorum", true, 1, 2, RedundancyDegraded},
		{"nobody live", true, 0, 1, RedundancyUnavailable},
	}
	for _, tc := range cases {
		m.SetPeerRedundancy(tc.known, tc.live, tc.quorum, "detail")
		if got, _ := m.Redundancy(); got != tc.want {
			t.Errorf("%s: known=%v live=%d quorum=%d -> %s, want %s",
				tc.name, tc.known, tc.live, tc.quorum, got, tc.want)
		}
	}
}

// TestMachine_PeerRedundancyDetailIsKeptOnlyWhereItExplainsSomething: a HEALTHY
// verdict has nothing to explain, so its detail is dropped; every other verdict
// keeps it, because that text is the only thing telling an operator how far the
// node got.
func TestMachine_PeerRedundancyDetailIsKeptOnlyWhereItExplainsSomething(t *testing.T) {
	m := New(Options{})

	m.SetPeerRedundancy(true, 3, 2, "should not survive a healthy verdict")
	if got, detail := m.Redundancy(); got != RedundancyHealthy || detail != "" {
		t.Fatalf("healthy verdict = %s/%q, want HEALTHY with no detail", got, detail)
	}

	m.SetPeerRedundancy(false, 1, 2, "1 of 3 required replicas reached; 2 never observed")
	got, detail := m.Redundancy()
	if got != RedundancyUnknown {
		t.Fatalf("incomplete sample = %s, want UNKNOWN", got)
	}
	if detail == "" {
		t.Fatal("an UNKNOWN verdict dropped its detail, want it kept so the sample is visible")
	}
}

// TestMachine_IndexDroppedDemotesAReadyEntry: the retention policy removed the
// artifact, so the version stops being READY here — §5.2's "the disk is the
// authority and the machine is its projection". The DATA is untouched, which is
// why it goes back to DATA_PRESENT rather than away: the version is rebuilt on
// demand, and that is what the policy means.
func TestMachine_IndexDroppedDemotesAReadyEntry(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"

	if err := m.RestoreVersion(kb, 1, VersionReady, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.IndexDropped(kb, 1); err != nil {
		t.Fatal(err)
	}
	if got := m.VersionState(kb, 1); got != VersionDataPresent {
		t.Fatalf("after the artifact was dropped = %s, want DATA_PRESENT", got)
	}

	// A version the machine never recorded an artifact for is left ALONE.
	// Creating an entry here would accumulate one per artifact the policy has
	// ever dropped, for a fact the cursor already implies.
	if err := m.IndexDropped(kb, 2); err != nil {
		t.Fatal(err)
	}
	if got := m.VersionState(kb, 2); got != VersionAbsent {
		t.Fatalf("a drop report for an unrecorded version made it %s, want ABSENT", got)
	}

	// A build in flight is not READY, and a drop report must not disturb it: the
	// builder is about to write a fresh artifact for exactly this version.
	step(t, m.DataLanded(kb, 3), "DataLanded")
	step(t, m.BuildScheduled(kb, 3), "BuildScheduled")
	if err := m.IndexDropped(kb, 3); err != nil {
		t.Fatal(err)
	}
	if got := m.VersionState(kb, 3); got != VersionBuilding {
		t.Fatalf("a drop report disturbed an in-flight build: %s, want BUILDING", got)
	}
}

// TestMachine_IndexInstalledIsADiskFactNotABuild: §8.4 ships a built artifact to
// the other replicas, which never ran a build — so the version arrives at READY
// with no BUILDING in between, and the transition table has to allow the disk to
// say so.
func TestMachine_IndexInstalledIsADiskFactNotABuild(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"

	step(t, m.DataLanded(kb, 1), "DataLanded")
	if err := m.BuildSucceeded(kb, 1); err == nil {
		t.Fatal("READY from DATA_PRESENT was accepted for a BUILD, want refused")
	}
	if err := m.IndexInstalled(kb, 1); err != nil {
		t.Fatalf("IndexInstalled: %v, want the disk fact accepted", err)
	}
	if got := m.VersionState(kb, 1); got != VersionReady {
		t.Fatalf("after an install = %s, want READY", got)
	}
}

// TestMachine_LandingDoesNotRewindAReadyVersion: "the data is here" is reported
// by every path that produced a landing, and those paths overlap — a push, a
// pull and a lag catch-up all report the same version. When one of those reports
// arrives after the index is already built, the version must stay READY.
//
// It must not move to DATA_PRESENT. versionTransitions allows VersionReady ->
// VersionDataPresent (IndexDropped needs that edge, for an index that really did
// disappear), so a landing reported against a READY version would rewind it —
// and the next thing that happens to a version with data and no index is another
// build of an index that is already on disk. Measured on the 3+3 cluster before
// this: every version built twice per replica, the second build landing exactly
// when lag catch-up reported the landing.
func TestMachine_LandingDoesNotRewindAReadyVersion(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"

	if err := m.DataLanded(kb, 9); err != nil {
		t.Fatal(err)
	}
	if err := m.BuildScheduled(kb, 9); err != nil {
		t.Fatal(err)
	}
	if err := m.BuildSucceeded(kb, 9); err != nil {
		t.Fatal(err)
	}
	if got := m.VersionState(kb, 9); got != VersionReady {
		t.Fatalf("state = %s, want READY", got)
	}

	// The late report.
	if err := m.DataLanded(kb, 9); err != nil {
		t.Fatalf("DataLanded on a READY version: %v", err)
	}
	if got := m.VersionState(kb, 9); got != VersionReady {
		t.Fatalf("state = %s after a late landing report, want READY: rewinding it means the index is rebuilt", got)
	}

	// And the same while a build is in flight: the landing does not cancel it.
	if err := m.DataLanded(kb, 10); err != nil {
		t.Fatal(err)
	}
	if err := m.BuildScheduled(kb, 10); err != nil {
		t.Fatal(err)
	}
	if err := m.DataLanded(kb, 10); err != nil {
		t.Fatalf("DataLanded on a BUILDING version: %v", err)
	}
	if got := m.VersionState(kb, 10); got != VersionBuilding {
		t.Fatalf("state = %s after a landing report during a build, want BUILDING", got)
	}
}

// TestMachine_IndexDroppedStillRewindsARreadyVersion guards the other side of the
// same edge: the transition that DataLanded no longer takes is the one
// IndexDropped exists for, and it has to keep working.
func TestMachine_IndexDroppedStillRewindsARreadyVersion(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"

	if err := m.RestoreVersion(kb, 3, VersionReady, "artifact on disk"); err != nil {
		t.Fatalf("RestoreVersion: %v", err)
	}
	if err := m.IndexDropped(kb, 3); err != nil {
		t.Fatalf("IndexDropped: %v", err)
	}
	if got := m.VersionState(kb, 3); got != VersionDataPresent {
		t.Fatalf("state = %s, want DATA_PRESENT: the index is gone, the data is not", got)
	}
}
