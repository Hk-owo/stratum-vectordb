//go:build docker
// +build docker

package docker_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
)

// TestProbeChainTiming is a TEMPORARY probe (not part of the regression suite):
// it splits the read path into externally observable segments, because the
// server exposes no metrics and logs no per-stage durations (checked: no
// prometheus anywhere, no time.Since in service/query.go or index/impl.go, and
// docker-cluster-both.sh does not even publish the metrics port).
//
// Segments, all measured through the station (the only public entry point):
//
//	A. ListVersions      — control-plane read + one station hop: the floor.
//	B. Query, zero vector— what the shipped stress suite sends.
//	C. Query, random vec — the same call with a query vector that is not
//	                       equidistant from everything.
//	D. Query, topk=50    — does the answer size matter, or only the walk?
//	E. Query after restarting one storage replica — the cold sample, printed
//	                       per attempt because the station load-balances and
//	                       only one of the attempts lands on the restarted node.
//
//	Usage: STRATUM_PROBE_KB=<kbID> go test ./integration/docker/ -tags=docker \
//	         -count=1 -run TestProbeChainTiming -v -timeout 900s
func TestProbeChainTiming(t *testing.T) {
	if os.Getenv("STRATUM_PROBE") == "" {
		t.Skip("diagnostic probe — restarts a storage replica and takes minutes; set STRATUM_PROBE=1 to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
	defer cancel()

	kbID := os.Getenv("STRATUM_PROBE_KB")
	if kbID == "" {
		kbID = pickProbeKB(t, ctx)
	}
	versionID := latestReadyVersion(t, ctx, kbID)
	t.Logf("probe target: KB %s version %d (via station %s)", kbID, versionID, nodeAddrs[0])

	// Wait until some replica serves it, so segment timings measure steady state.
	if resp := awaitServable(t, ctx, storageAddrs[0], kbID, versionID, 90*time.Second); resp == nil {
		t.Fatalf("no replica serves %s v%d", kbID, versionID)
	}

	const dim = 768
	zero := make([]float32, dim)
	rng := rand.New(rand.NewSource(20240915))
	random := make([]float32, dim)
	for i := range random {
		random[i] = rng.Float32()*2 - 1
	}

	// A. Control-plane floor: metadata read through the station.
	probeStats(t, "A ListVersions (control RTT)", probeLoop(t, 30, func() (int, error) {
		_, _, _, conn, err := dialNode(nodeAddrs[0])
		if err != nil {
			return 0, err
		}
		defer conn.Close()
		resp, err := pb.NewKnowledgeBaseServiceClient(conn).ListVersions(ctx,
			&pb.ListVersionsRequest{KnowledgeBaseId: kbID})
		if err != nil {
			return 0, err
		}
		return len(resp.GetVersions()), nil
	}))

	// B. The shipped suite's vector.
	probeStats(t, "B Query zero-vector topk=5", probeLoop(t, 30, func() (int, error) {
		return probeQuery(ctx, nodeAddrs[0], kbID, versionID, zero, 5)
	}))

	// C. Same call, a vector that is not equidistant from everything.
	probeStats(t, "C Query random-vector topk=5", probeLoop(t, 30, func() (int, error) {
		return probeQuery(ctx, nodeAddrs[0], kbID, versionID, random, 5)
	}))

	// D. Does the answer size matter, or only the walk?
	probeStats(t, "D Query random-vector topk=50", probeLoop(t, 10, func() (int, error) {
		return probeQuery(ctx, nodeAddrs[0], kbID, versionID, random, 50)
	}))

	// E. Cold: restart one replica (its in-memory index goes away, the artifact
	// stays on disk) and time every attempt — the station spreads them over the
	// three replicas, so only some of them pay the Load.
	t.Logf("restarting %s for the cold segment", storageServices[0])
	killNode(t, storageServices[0])
	startNode(t, storageServices[0])
	if err := waitForListen(storageAddrs[0], 60*time.Second); err != nil {
		t.Fatalf("storage replica did not come back: %v", err)
	}
	probeStats(t, "E Query random-vector after restart", probeLoop(t, 9, func() (int, error) {
		return probeQuery(ctx, nodeAddrs[0], kbID, versionID, random, 5)
	}))
}

// probeQuery performs one query through addr and returns the result count.
func probeQuery(ctx context.Context, addr, kbID string, versionID int64, vector []float32, topK int) (int, error) {
	resp, err := serveVector(ctx, addr, kbID, versionID, vector, topK)
	if err != nil {
		return 0, err
	}
	return len(resp.GetResults()), nil
}

// probeLoop runs do n times, timing each call, and returns the durations of the
// successful ones.
func probeLoop(t *testing.T, n int, do func() (int, error)) []time.Duration {
	t.Helper()
	out := make([]time.Duration, 0, n)
	failures := 0
	for i := 0; i < n; i++ {
		start := time.Now()
		got, err := do()
		elapsed := time.Since(start)
		if err != nil {
			failures++
			t.Logf("  attempt %d failed after %v: %v", i+1, elapsed, err)
			continue
		}
		if got == 0 {
			t.Logf("  attempt %d returned no results (%v)", i+1, elapsed)
		}
		out = append(out, elapsed)
	}
	if failures > 0 {
		t.Logf("  %d/%d attempts failed", failures, n)
	}
	return out
}

// probeStats prints min/p50/p95/p99/max/mean for one segment.
func probeStats(t *testing.T, label string, d []time.Duration) {
	t.Helper()
	if len(d) == 0 {
		t.Logf("PROBE %-42s no successful samples", label)
		return
	}
	lo, hi := d[0], d[0]
	for _, v := range d {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}
	p50, p95, p99, mean := percentiles(d)
	t.Logf("PROBE %-42s n=%d min=%v p50=%v p95=%v p99=%v max=%v mean=%v",
		label, len(d), lo, p50, p95, p99, hi, mean)
}

// pickProbeKB chooses a knowledge base to probe: the one named by
// STRATUM_PROBE_KB, else the first datavolume-* one, else the first listed.
func pickProbeKB(t *testing.T, ctx context.Context) string {
	t.Helper()
	_, _, _, conn, err := dialNode(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial station: %v", err)
	}
	defer conn.Close()
	resp, err := pb.NewKnowledgeBaseServiceClient(conn).ListKnowledgeBases(ctx, &pb.ListKnowledgeBasesRequest{})
	if err != nil {
		t.Fatalf("ListKnowledgeBases: %v", err)
	}
	kbs := resp.GetKnowledgeBases()
	if len(kbs) == 0 {
		t.Fatal("no knowledge bases exist; set STRATUM_PROBE_KB or write one first")
	}
	for _, kb := range kbs {
		if strings.HasPrefix(kb.GetKnowledgeBaseId(), "datavolume-") {
			return kb.GetKnowledgeBaseId()
		}
	}
	return kbs[0].GetKnowledgeBaseId()
}

// latestReadyVersion returns the highest READY version of kbID.
func latestReadyVersion(t *testing.T, ctx context.Context, kbID string) int64 {
	t.Helper()
	_, _, _, conn, err := dialNode(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial station: %v", err)
	}
	defer conn.Close()
	resp, err := pb.NewKnowledgeBaseServiceClient(conn).ListVersions(ctx,
		&pb.ListVersionsRequest{KnowledgeBaseId: kbID})
	if err != nil {
		t.Fatalf("ListVersions(%s): %v", kbID, err)
	}
	var best int64
	for _, v := range resp.GetVersions() {
		if v.GetIndexStatus() != pb.IndexStatus_INDEX_STATUS_READY {
			continue
		}
		if v.GetVersionId() > best {
			best = v.GetVersionId()
		}
	}
	if best == 0 {
		t.Fatalf("no READY version for %s", kbID)
	}
	return best
}

// TestProbeKBReport is a read-only diagnostic: it prints a knowledge base's
// version chain (id / parent / status). That is what tells an EMPTY knowledge
// base — a single empty initial version, which the leader never commits a
// digest for — apart from one whose data really landed. Nothing is written and
// no replica is touched.
//
// It exists because the chain-timing probe failed against a knowledge base
// whose index directory held nothing but a 1-byte <v>.index.mem: whether that
// knowledge base ever had data decides whether "no artifact" means "nothing to
// build" or "the artifact is missing".
func TestProbeKBReport(t *testing.T) {
	if os.Getenv("STRATUM_PROBE") == "" {
		t.Skip("diagnostic probe — set STRATUM_PROBE=1 to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	kbID := os.Getenv("STRATUM_PROBE_KB")
	if kbID == "" {
		kbID = pickProbeKB(t, ctx)
	}
	_, _, _, conn, err := dialNode(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial station: %v", err)
	}
	defer conn.Close()

	resp, err := pb.NewKnowledgeBaseServiceClient(conn).ListVersions(ctx,
		&pb.ListVersionsRequest{KnowledgeBaseId: kbID})
	if err != nil {
		t.Fatalf("ListVersions(%s): %v", kbID, err)
	}
	t.Logf("KB %s: %d versions", kbID, len(resp.GetVersions()))
	for _, v := range resp.GetVersions() {
		t.Logf("  v%-4d parent=%-4d status=%-14s deleting=%v",
			v.GetVersionId(), v.GetParentVersionId(), v.GetIndexStatus(), v.GetDeleting())
	}

	for i, addr := range storageAddrs {
		resp, err := probeQuery(ctx, addr, kbID, latestReadyVersion(t, ctx, kbID), make([]float32, 768), 3)
		t.Logf("  replica %d query: results=%d err=%v", i+1, resp, err)
	}
}

var _ = fmt.Sprintf
