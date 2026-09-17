//go:build docker
// +build docker

package docker_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
)

// storageHostAddrs are the storage tier's PUBLISHED host ports — the addresses to
// dial when a test has to reach one particular storage node.
//
// storageAddrs cannot serve that purpose: TestMain points it at the station
// (three copies of one address), which is right for every client-facing assertion
// and useless for "ask this node what it holds". They are indexed in step with
// storageServices, so the name and the port always describe the same container.
var storageHostAddrs = splitEnv("STRATUM_T4_STORAGE_HOST_ADDRS", "localhost:17100,localhost:17101,localhost:17102")

// storageDial opens a node-to-node connection to one storage node.
//
// DataSyncService is a node-to-node service: §9.3(5) gates the three
// client-facing services, not this one, so it can be reached without a station's
// trust mark. That is what makes it the right probe here — the questions below are
// about ONE node's own state, and going through the station would answer about the
// tier instead.
func storageDial(hostAddr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(hostAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// holdsVersionLocally asks one storage node whether it holds (kbID, versionID)'s
// DATA — the direct question, and the one this case asserts on.
//
// Deliberately not LocalVersion: a cursor is a contiguous-prefix statement, so a
// node that missed an early empty version reports 0 forever even after it has
// pulled everything else (an empty version is never fanned out; the only thing
// that moves a cursor over one is the writer's one-shot confirmation, which is not
// replayed after a restart). VersionPresence answers about the version asked for,
// which is what "did the fourth layer get the data here" means.
func holdsVersionLocally(ctx context.Context, hostAddr, kbID string, versionID int64) (bool, error) {
	conn, err := storageDial(hostAddr)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	resp, err := pb.NewDataSyncServiceClient(conn).VersionPresence(ctx, &pb.VersionPresenceRequest{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
	})
	if err != nil {
		return false, err
	}
	return resp.GetPresent(), nil
}

// awaitVersionPresence polls a storage node until it holds versionID's data, and
// returns the last answer it saw either way.
func awaitVersionPresence(t *testing.T, ctx context.Context, hostAddr, kbID string, versionID int64, timeout time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last bool
	var lastErr error
	for time.Now().Before(deadline) {
		held, err := holdsVersionLocally(ctx, hostAddr, kbID, versionID)
		if err != nil {
			lastErr = err
		} else {
			lastErr = nil
			last = held
			if held {
				return true
			}
		}
		time.Sleep(2 * time.Second)
	}
	if lastErr != nil {
		t.Logf("%s never answered (last error: %v)", hostAddr, lastErr)
	}
	t.Logf("%s still does not hold v%d (last answer: %v)", hostAddr, versionID, last)
	return last
}

// awaitDataStatus polls the control layer until versionID reports want as its
// DATA-side status, returning what it last saw.
//
// It exists because this case is only decidable on a version whose data side really
// reached DURABLE. A version that never formed a quorum is left PENDING with no
// document-set digest, and a returning node cannot tell that from an EMPTY version:
// it advances its cursor over it without pulling anything (the empty-version rule,
// §7.5). That is not the fourth layer failing to answer — it is the question never
// being asked, and a test that ignored the distinction passes without ever touching
// the code under test.
func awaitDataStatus(t *testing.T, ctx context.Context, addr, kbID string, versionID int64, want pb.DataStatus, timeout time.Duration) pb.DataStatus {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last pb.DataStatus
	for time.Now().Before(deadline) {
		if status, ok := dataStatusOf(ctx, addr, kbID, versionID); ok {
			last = status
			if status == want {
				return status
			}
		}
		time.Sleep(2 * time.Second)
	}
	return last
}

// dataStatusOf reads one version's data-side status from the control layer.
func dataStatusOf(ctx context.Context, addr, kbID string, versionID int64) (pb.DataStatus, bool) {
	_, _, _, conn, err := dialNode(addr)
	if err != nil {
		return pb.DataStatus_DATA_STATUS_PENDING, false
	}
	defer conn.Close()

	versions, err := pb.NewKnowledgeBaseServiceClient(conn).ListVersions(ctx, &pb.ListVersionsRequest{
		KnowledgeBaseId: kbID,
	})
	if err != nil {
		return pb.DataStatus_DATA_STATUS_PENDING, false
	}
	for _, v := range versions.GetVersions() {
		if v.GetVersionId() == versionID {
			return v.GetDataStatus(), true
		}
	}
	return pb.DataStatus_DATA_STATUS_PENDING, false
}

// holdersFallbackSettle is how long the returning node is given to find a source
// and pull the version it missed. It covers a report interval (5s — the control
// leader's answer rides the report RESPONSE, so there is no separate refresh call
// to wait for), the catch-up decision, and a pull plus index build. Generous on
// purpose: what is asserted is "it happens at all", not "it happens fast".
func holdersFallbackSettle() time.Duration {
	return envDuration("STRATUM_HOLDERS_SETTLE", 4*time.Minute)
}

func holdersFallbackTimeout() time.Duration {
	return envDuration("STRATUM_HOLDERS_TIMEOUT", 8*time.Minute)
}

func envDuration(key string, def time.Duration) time.Duration {
	d, err := time.ParseDuration(strings.TrimSpace(os.Getenv(key)))
	if err != nil || d <= 0 {
		return def
	}
	return d
}

// TestT4_HoldersFallbackPullsTheVersionItMissed is the cluster-level proof of
// docs/data-source-holders-fallback-plan.md: a replica that missed the §8.5
// announcement — so its announced-holder table has nothing for the version — still
// finds a source and pulls the version back.
//
// The three layers of the source lookup are what make the outcome unambiguous:
//
//	① §8.5 table      miss: the writer's confirmation never reached this node
//	                  (it was down), and nothing replays it;
//	② holders mirror  miss at first — it is filled from the control leader's
//	                  aggregate, which arrives on the report RESPONSE, so one report
//	                  interval is all it takes;
//	③ leader fallback reaches the CONTROL leader, which in this topology exports no
//	                  data at all ("PullVersionData: this node exports no data").
//
// So "the stranded node got the data" has exactly one explanation left: layer ② was
// consulted and answered. No push can account for it — the builders shipped v2 while
// the node was down and never retry — and nothing queries the node (it is not in
// the station's route table, precisely because it reports a low cursor).
//
// WHY THE CLUSTER NEEDS lag_catchup: a restarting replica's reconcile rebuilds
// missing INDEXES; it does not fetch data, and it gives its head start only to
// PENDING and ACTIVE versions. Nothing else asks a behind node for anything —
// which is exactly the gap docs/active-lag-detection-design.md fills. So the
// catch-up is the TRIGGER and the fourth layer is the SOURCE; without the trigger
// the source is never consulted and this case would be measuring the wrong thing.
// The case skips when the switch is off.
//
// The case also SKIPS rather than fails when a version's data side never reaches
// DURABLE: no quorum means no digest, which makes the version indistinguishable
// from an empty one to a returning node, so the fourth layer is never asked.
func TestT4_HoldersFallbackPullsTheVersionItMissed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), holdersFallbackTimeout())
	defer cancel()

	waitForStorageGroupReady(t, 3*time.Minute)

	if len(storageServices) < 3 || len(storageHostAddrs) < 3 {
		t.Skipf("needs 3 storage nodes with their host ports, have %d/%d",
			len(storageServices), len(storageHostAddrs))
	}
	// Writes and the rollback go through the station: nodeAddrs point at it, which is
	// the deployed shape and the only path a client has.
	station := nodeAddrs[0]
	_, kbID := waitForLeader(t, ctx, "holders-fallback", 60*time.Second)
	t.Logf("KB %s", kbID)

	behindSvc, behindHost := storageServices[0], storageHostAddrs[0]

	// A version everyone gets, so the node has a baseline and the probe against it is
	// known to work before the interesting part.
	seed := genUniqueDocs(2, 600)
	v1 := writeChanges(t, ctx, station, kbID, 0, lagAddChange(seed[0]))
	waitVersionStatus(t, ctx, station, kbID, v1, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
	if got := awaitDataStatus(t, ctx, station, kbID, v1, pb.DataStatus_DATA_STATUS_DURABLE, 90*time.Second); got != pb.DataStatus_DATA_STATUS_DURABLE {
		t.Skipf("baseline v%d never reached DATA_STATUS_DURABLE (last: %s): this cluster is not "+
			"forming a quorum for writes, so nothing here can be measured", v1, got)
	}
	if !awaitVersionPresence(t, ctx, behindHost, kbID, v1, 90*time.Second) {
		t.Fatalf("%s never held the baseline v%d (%s): the probe itself is unusable", behindSvc, v1, behindHost)
	}
	t.Logf("%s holds v%d; taking it down", behindSvc, v1)

	// Strand it: while it is gone, v2's fan-out AND its §8.5 announcement both fail
	// to reach it. The announcement is a bounded broadcast, which is the whole
	// premise of the fourth layer.
	killNode(t, behindSvc)
	defer startNode(t, behindSvc)

	v2 := writeChanges(t, ctx, station, kbID, v1, lagAddChange(seed[1]))
	waitVersionStatus(t, ctx, station, kbID, v2, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
	status := awaitDataStatus(t, ctx, station, kbID, v2, pb.DataStatus_DATA_STATUS_DURABLE, 90*time.Second)
	t.Logf("v%d: index READY, data %s (written while %s was down)", v2, status, behindSvc)
	if status != pb.DataStatus_DATA_STATUS_DURABLE {
		t.Logf("%s's log tail:\n%s", behindSvc, dockerCmd(t, "logs", "--tail", "20", behindSvc))
		t.Skipf("v%d never reached DATA_STATUS_DURABLE (last: %s). Without a quorum its digest is "+
			"never committed, and a returning node cannot tell such a version from an EMPTY one: it "+
			"advances its cursor over it without pulling, so the fourth layer is never asked and this "+
			"run would prove nothing either way", v2, status)
	}
	if held, _ := holdsVersionLocally(ctx, behindHost, kbID, v2); held {
		t.Fatalf("%s already holds v%d while it is supposed to be down: the fault did not land", behindSvc, v2)
	}

	if err := rollbackTo(ctx, station, kbID, v2); err != nil {
		t.Fatalf("rollback to v%d: %v", v2, err)
	}
	t.Logf("active version is now v%d", v2)

	// Bring it back. From here nothing else touches it: no query, no rebuild request,
	// no push. Its own catch-up is the only actor, and the source it must find is the
	// one the announced-holder table cannot give it.
	startNode(t, behindSvc)

	if !awaitVersionPresence(t, ctx, behindHost, kbID, v2, holdersFallbackSettle()) {
		t.Logf("%s's log tail:\n%s", behindSvc, dockerCmd(t, "logs", "--tail", "30", behindSvc))
		t.Errorf("%s never pulled v%d. With the §8.5 announcement missing and the leader fallback "+
			"pointing at a control node that exports no data, the fourth layer is the only layer that "+
			"could have answered (docs/data-source-holders-fallback-plan.md)", behindSvc, v2)
		return
	}
	t.Logf("%s pulled v%d back on its own: the fourth layer answered where the table could not",
		behindSvc, v2)
}
