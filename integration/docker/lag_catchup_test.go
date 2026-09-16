//go:build docker
// +build docker

package docker_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
)

// This case exists to put a number on IndexRetentionCount, which is two things at
// once: the on-disk quota per knowledge base, and (because §8.4 ships artifacts
// rather than rebuilding them) the depth of lag that distribution can still
// repair. Its default is 50; nothing measured it.
//
// The measurement is a batch of version switches on one knowledge base, with a
// storage node watching. Two quantities come out:
//
//	artifact counts, before and after — how deep the window actually is
//	"read local index" failures        — a ship whose artifact retention had already taken
//
// The second is the one that matters: PushIndexToReplicas reads the whole file
// before shipping it, so a distribution that is queued behind the push gate when
// the next retention pass runs finds nothing to send. (It degrades to "that
// replica builds its own" — an optimisation lost, never a correctness problem.)
//
// The numbers are PRINTED, not asserted: what counts as too shallow depends on a
// deployment's write rate. STRATUM_LAG_VERSIONS picks the batch size (default 55,
// just past the 50-window); STRATUM_LAG_OFFLINE=1 adds the lagging-replica leg.
func TestT4_LagCatchupRetentionWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), lagCatchupTimeout())
	defer cancel()

	// The storage group has to be able to take a write before the first
	// CreateVersion: right after a cluster rebuild the dispatch path finds no
	// reachable candidate, the write is abandoned as TRANSIENT, and the version
	// never reaches READY — which looks exactly like a slow index build.
	waitForStorageGroupReady(t, 3*time.Minute)

	leaderIdx, kbID := waitForLeader(t, ctx, "lag-window", 60*time.Second)
	leaderAddr := nodeAddrs[leaderIdx]
	t.Logf("leader: node %d (%s); KB %s", leaderIdx, leaderAddr, kbID)

	// A storage node that is not the leader plays the replica we watch.
	behind := 0
	for i := range storageServices {
		if storageAddrs[i] != leaderAddr {
			behind = i
			break
		}
	}
	t.Logf("replica under observation: %s (%s)", storageServices[behind], storageAddrs[behind])

	// One real version first, so the KB exists for everyone before we start.
	seed := genUniqueDocs(1, 600)
	parent := writeChanges(t, ctx, leaderAddr, kbID, 0, lagAddChange(seed[0]))
	waitVersionStatus(t, ctx, leaderAddr, kbID, parent, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())

	before := artifactCounts(t, kbID)
	t.Logf("artifacts before the batch: %v", before)

	var offlineSvc string
	if os.Getenv("STRATUM_LAG_OFFLINE") == "1" {
		offlineSvc = storageServices[behind]
		killNode(t, offlineSvc)
		defer startNode(t, offlineSvc)
		t.Logf("%s goes offline for the batch", offlineSvc)
	}

	versions := lagCatchupVersions()
	t.Logf("advancing %d versions", versions)
	pool := genUniqueDocs(versions, 600)
	for i := 0; i < versions; i++ {
		parent = writeChanges(t, ctx, leaderAddr, kbID, parent, lagAddChange(pool[i]))
		waitVersionStatus(t, ctx, leaderAddr, kbID, parent, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
		if (i+1)%10 == 0 {
			t.Logf("  %d/%d advanced (latest v%d)", i+1, versions, parent)
		}
	}
	t.Logf("batch complete: latest version v%d", parent)

	if offlineSvc != "" {
		startNode(t, offlineSvc)
		waitForNodeToSeeKB(t, ctx, storageAddrs[behind], kbID, 60*time.Second)
		t.Logf("%s is back; giving it %v to settle", offlineSvc, lagCatchupSettle())
		time.Sleep(lagCatchupSettle())
	}

	after := artifactCounts(t, kbID)
	fail := collectDistributionFailures(t, kbID)

	t.Logf("STRATUM_LAG_VERSIONS=%d (window default 50)", versions)
	t.Logf("artifacts after the batch: %v", after)
	t.Logf("distribution failures on this KB: read-failure(artifact already gone)=%d "+
		"checksum(pair rejected by the receiver)=%d unreachable=%d",
		fail.readFailure, fail.checksum, fail.unreachable)
	t.Logf("reconcile \"retention-dropped\" explanations on this KB: %d", fail.dropped)
	t.Logf("note: with one KB advanced strictly one version at a time, each distribution runs " +
		"while the next build is still going, so read-failures are the rare case. A checksum " +
		"failure is a different animal: the index/sidecar pair that arrived did not validate, " +
		"which means it was read — or written — mid-swap.")
}

// lagAddChange wraps one document as the single change of a version.
func lagAddChange(d docUnit) []*pb.DocChange {
	return []*pb.DocChange{{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: d.id, Content: d.content}}
}

// artifactCount counts the `.index` artifacts storage node i holds for kbID.
// Counting files, not bytes, is deliberate: it is the same set EnforceDiskRetention
// works on.
func artifactCount(t *testing.T, i int, kbID string) int {
	t.Helper()
	svc := storageServices[i]
	dir := fmt.Sprintf("%s/%s", indexDirOf(i), kbID)
	out := dockerCmd(t, "exec", svc, "sh", "-c",
		fmt.Sprintf("ls %s 2>/dev/null | grep -c '\\.index$' || true", dir))
	n, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		t.Logf("artifactCount(%s): %q is not a number", svc, out)
		return 0
	}
	return n
}

// waitForStorageGroupReady blocks until every storage container reports healthy.
//
// The dispatch path needs a reachable candidate, and it has no retry budget for
// "the group is still coming up": a write that arrives too early is abandoned as
// TRANSIENT and never reaches READY, so the test would sit on its first version
// until the deadline, looking like a slow build instead of a startup race.
func waitForStorageGroupReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for _, svc := range storageServices {
		for {
			out := dockerCmd(t, "inspect", "-f", "{{.State.Health.Status}}", svc)
			if strings.TrimSpace(out) == "healthy" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never became healthy; the storage group cannot take writes", svc)
			}
			time.Sleep(2 * time.Second)
		}
	}
	t.Logf("storage group ready: %v", storageServices)
}

// artifactCounts counts the `.index` artifacts each storage node holds for kbID.
// Per node, not summed: which node holds what is the whole question when the
// point is a lagging replica.
func artifactCounts(t *testing.T, kbID string) []int {
	t.Helper()
	out := make([]int, 0, len(storageServices))
	for i := range storageServices {
		out = append(out, artifactCount(t, i, kbID))
	}
	return out
}

// distFailures classifies kbID's distribution failures, read from the storage
// logs. Filtering by kbID is not optional: the cluster's data volumes survive a
// rebuild, so every node's log still carries the failures of every earlier run
// against the same cluster.
type distFailures struct {
	readFailure int // the artifact was already gone when the ship read it
	checksum    int // the pair that arrived did not validate on the receiver
	unreachable int // the peer could not be reached at all
	dropped     int // reconcile explained an absence by the retention policy
}

func collectDistributionFailures(t *testing.T, kbID string) distFailures {
	t.Helper()
	var f distFailures
	for _, svc := range storageServices {
		for _, line := range strings.Split(nodeLogsSince(t, svc), "\n") {
			if !strings.Contains(line, kbID) {
				continue
			}
			switch {
			case strings.Contains(line, "skipping rebuild of retention-dropped index"):
				f.dropped++
			case !strings.Contains(line, "index distribution failed"):
				// Not a failure line at all.
			case strings.Contains(line, "read local index"):
				f.readFailure++
			case strings.Contains(line, "checksum mismatch"):
				f.checksum++
			default:
				f.unreachable++
			}
		}
	}
	return f
}

// lagCatchupVersions is the batch size. Just past the 50-version default window by
// default, so a run can cross it without STRATUM_LAG_VERSIONS.
func lagCatchupVersions() int {
	if v := os.Getenv("STRATUM_LAG_VERSIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 55
}

// lagCatchupSettle is how long a returning replica is left alone before its
// artifacts are counted, and how long the active-catch-up case waits for the node to
// say it caught up. A settlement window, not a timeout: with lag_catchup off nothing
// happens in it (which is what the window case measures), and with it on the node is
// expected to report a catch-up well inside it.
func lagCatchupSettle() time.Duration {
	if v := os.Getenv("STRATUM_LAG_SETTLE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 45 * time.Second
}

// lagCatchupTimeout scales with the batch: every version waits for its index to
// reach READY before the next one is chained onto it.
func lagCatchupTimeout() time.Duration {
	if v := os.Getenv("STRATUM_LAG_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 40 * time.Minute
}

// TestT4_ActiveLagCatchupCatchesUpWithoutAQuery is the case
// docs/active-lag-detection-design.md exists for: a node that fell behind catches up
// on its own, with nothing asking it for data.
//
// It kills one storage node, advances versions while it is away, brings it back, and
// then sends nothing at all — no query, no rebuild request. The assertion is the log
// line the catch-up itself writes, because "an artifact appeared" alone would also be
// explained by the startup reconcile giving the active version a head start.
//
// The cluster must be built with LAG_CATCHUP_ENABLED=true (scripts/docker-cluster-both.sh
// reads it, and LOG_LEVEL=debug makes the scheduler's decisions visible).
//
// SKIPPED_FIXED: the two prerequisites this case was blocked on are both repaired.
//
//  1. `RecoverLocalCursors` handed a returning node a cursor equal to the CHAIN TAIL,
//     because the third condition of holdsVersionLocally read the INDEX side
//     (IndexStatus == READY) as evidence about the DATA side — while fan-out
//     deliberately withholds the digest without quorum, so an index could be READY on
//     a node that holds no data. It now reads DataStatus, and is conservative by
//     design: a version whose records are complete but whose index was never built
//     here is reported as "not held".
//  2. The chain tails never reached the reporter because a CONTROL node does not
//     register AdminService, and the storage side resolves the leader through
//     AdminService.GetClusterStatus (its `rn` is a RemoteRaftNode). Every report died
//     with "Unimplemented: unknown service stratum.AdminService", so SetChainTails was
//     never reached. Fixed by registering a control-side admin service that answers
//     GetClusterStatus.
//
// Measured after both repairs: the reporter logs "data-version report landed" with
// chain_tails=2, and the scheduler logs "nothing behind the chain tail" with
// tails_received=2 — i.e. the signal now arrives carrying tails.
func TestT4_ActiveLagCatchupCatchesUpWithoutAQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), lagCatchupTimeout())
	defer cancel()

	waitForStorageGroupReady(t, 3*time.Minute)

	enabled := dockerCmd(t, "exec", storageServices[0], "sh", "-c",
		"grep -A1 '^lag_catchup:' /etc/stratum/config.yaml 2>/dev/null || true")
	if !strings.Contains(enabled, "enabled: true") {
		t.Skipf("cluster was built without lag_catchup enabled "+
			"(rebuild with LAG_CATCHUP_ENABLED=true); config reads: %q", strings.TrimSpace(enabled))
	}

	leaderIdx, kbID := waitForLeader(t, ctx, "lag-active", 60*time.Second)
	leaderAddr := nodeAddrs[leaderIdx]

	behind := 0
	for i := range storageServices {
		if storageAddrs[i] != leaderAddr {
			behind = i
			break
		}
	}
	behindSvc := storageServices[behind]

	// One version everybody gets, so the knowledge base is real for this node before
	// it goes away — and its artifact count has a baseline.
	seed := genUniqueDocs(1, 600)
	parent := writeChanges(t, ctx, leaderAddr, kbID, 0, lagAddChange(seed[0]))
	waitVersionStatus(t, ctx, leaderAddr, kbID, parent, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
	before := artifactCount(t, behind, kbID)

	killNode(t, behindSvc)
	defer startNode(t, behindSvc)

	versions := lagCatchupVersions()
	pool := genUniqueDocs(versions, 600)
	for i := 0; i < versions; i++ {
		parent = writeChanges(t, ctx, leaderAddr, kbID, parent, lagAddChange(pool[i]))
		waitVersionStatus(t, ctx, leaderAddr, kbID, parent, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
	}
	t.Logf("%s was away for %d versions (chain tail v%d); artifacts before: %d",
		behindSvc, versions, parent, before)

	// From here on nothing touches the node: it is not queried, and nobody asks it to
	// rebuild. Whatever it does, it does on its own.
	startNode(t, behindSvc)

	deadline := time.Now().Add(lagCatchupSettle())
	caughtUp := false
	for time.Now().Before(deadline) {
		if strings.Contains(nodeLogsSince(t, behindSvc), "caught up with the chain tail") {
			caughtUp = true
			break
		}
		time.Sleep(5 * time.Second)
	}
	after := artifactCount(t, behind, kbID)

	t.Logf("LAG_CATCHUP_ENABLED=true, %d versions while offline", versions)
	t.Logf("artifacts on the returning node: %d → %d", before, after)
	t.Logf("caught up on its own (logged): %v", caughtUp)

	if !caughtUp {
		t.Errorf("%s never reported a chain-tail catch-up within %v: with nothing asking "+
			"it for data, it stayed behind", behindSvc, lagCatchupSettle())
	}
	if after <= before {
		t.Errorf("artifacts on %s did not grow (%d → %d), so the catch-up landed nothing",
			behindSvc, before, after)
	}
}
