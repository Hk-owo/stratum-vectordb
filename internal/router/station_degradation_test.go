package router

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
)

// stubQueryClient answers Query and nothing else, which is the whole interface.
type stubQueryClient struct {
	pb.QueryServiceClient
	resp *pb.QueryResponse
}

func (s stubQueryClient) Query(context.Context, *pb.QueryRequest, ...grpc.CallOption) (*pb.QueryResponse, error) {
	return s.resp, nil
}

// stubAdminClient answers HealthCheck and nothing else. The embedded interface is
// nil, so any other call panics — deliberate: these tests are about one method,
// and a panic is a louder failure than a silently mis-wired stub.
type stubAdminClient struct {
	pb.AdminServiceClient
	health *pb.HealthCheckResponse
}

func (s stubAdminClient) HealthCheck(context.Context, *pb.HealthCheckRequest, ...grpc.CallOption) (*pb.HealthCheckResponse, error) {
	return s.health, nil
}

// The summary is what the station's cluster-level answers are built from, so its
// three-way answer has to be right: degraded, healthy, and "no verdict at all".
func TestRouteTable_DegradationSummary(t *testing.T) {
	t.Run("no verdict in the snapshot is unknown, not healthy", func(t *testing.T) {
		if _, _, known := tableWithVerdicts(nil).DegradationSummary(); known {
			t.Error("a snapshot holding no verdicts cannot summarize them")
		}
	})

	t.Run("before the first refresh it is unknown too", func(t *testing.T) {
		if _, _, known := NewRouteTable(nil, 0, nil).DegradationSummary(); known {
			t.Error("a table that has never refreshed knows nothing")
		}
	})

	t.Run("every KB healthy", func(t *testing.T) {
		table := tableWithVerdicts(map[string]degradationVerdict{
			"kb-1": {degraded: false, detail: "3 of 3 required replicas live, quorum is 2"},
		})
		degraded, _, known := table.DegradationSummary()
		if !known || degraded {
			t.Errorf("summary = (degraded=%v, known=%v), want (false, true)", degraded, known)
		}
	})

	t.Run("one degraded KB is enough", func(t *testing.T) {
		table := tableWithVerdicts(map[string]degradationVerdict{
			"kb-ok":  {degraded: false},
			"kb-bad": {degraded: true, detail: "1 of 3 required replicas live, below the quorum of 2"},
		})
		degraded, detail, known := table.DegradationSummary()
		if !known || !degraded {
			t.Fatalf("summary = (degraded=%v, known=%v), want (true, true)", degraded, known)
		}
		if detail != "1 of 3 required replicas live, below the quorum of 2" {
			t.Errorf("detail = %q, want the degraded KB's diagnosis", detail)
		}
	})

	t.Run("the answer does not depend on map iteration order", func(t *testing.T) {
		table := tableWithVerdicts(map[string]degradationVerdict{
			"kb-a": {degraded: true, detail: "a"},
			"kb-b": {degraded: true, detail: "b"},
		})
		_, first, _ := table.DegradationSummary()
		for i := 0; i < 20; i++ {
			if _, again, _ := table.DegradationSummary(); again != first {
				t.Fatalf("summary changed between calls: %q then %q — an operator would chase a ghost", first, again)
			}
		}
	})
}

// queryStation wires a station in front of one stub storage node.
func queryStation(t *testing.T, verdicts map[string]degradationVerdict, resp *pb.QueryResponse) *QueryServer {
	t.Helper()
	return NewQueryServer(&Router{
		storageAddrs: []string{"stub:1"},
		querys:       []pb.QueryServiceClient{stubQueryClient{resp: resp}},
		routes:       tableWithVerdicts(verdicts),
	})
}

// The point of §10.1: a client that only ever READS still learns the storage layer
// is below quorum. Without this the only way to find out is a write failing.
func TestQueryServer_ReportsStorageDegradationOnTheReadResponse(t *testing.T) {
	cases := map[string]struct {
		verdicts map[string]degradationVerdict
		want     bool
	}{
		"below quorum": {map[string]degradationVerdict{
			"kb-1": {degraded: true, detail: "1 of 3 required replicas live"},
		}, true},
		"healthy":                     {map[string]degradationVerdict{"kb-1": {degraded: false}}, false},
		"no verdict at all (unknown)": {nil, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// The stub's response is built fresh per case: the injection mutates it.
			resp, err := queryStation(t, tc.verdicts, &pb.QueryResponse{VersionId: 7}).
				Query(context.Background(), &pb.QueryRequest{KnowledgeBaseId: "kb-1"})
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if resp.GetStorageDegraded() != tc.want {
				t.Errorf("storage_degraded = %v, want %v", resp.GetStorageDegraded(), tc.want)
			}
			if resp.GetVersionId() != 7 {
				t.Errorf("version_id = %d, want the proxied answer otherwise untouched", resp.GetVersionId())
			}
		})
	}
}

// HealthCheck is where a probe looks, and the storage verdict is the one thing the
// answering node cannot know.
func TestAdminServer_HealthCheckCarriesTheStorageDiagnosis(t *testing.T) {
	station := func(nodeDetails string, verdicts map[string]degradationVerdict) *AdminServer {
		return NewAdminServer(&Router{
			storageAddrs: []string{"stub:1"},
			admins:       []pb.AdminServiceClient{stubAdminClient{health: &pb.HealthCheckResponse{Details: nodeDetails}}},
			routes:       tableWithVerdicts(verdicts),
		})
	}
	belowQuorum := map[string]degradationVerdict{
		"kb-1": {degraded: true, detail: "1 of 3 required replicas live, below the quorum of 2"},
	}

	check := func(t *testing.T, s *AdminServer, want string) {
		t.Helper()
		resp, err := s.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
		if err != nil {
			t.Fatalf("HealthCheck failed: %v", err)
		}
		if resp.GetDetails() != want {
			t.Errorf("details = %q, want %q", resp.GetDetails(), want)
		}
	}

	t.Run("a bare ok is replaced by the diagnosis", func(t *testing.T) {
		check(t, station("ok", belowQuorum), "storage: 1 of 3 required replicas live, below the quorum of 2")
	})

	t.Run("an existing complaint keeps it appended", func(t *testing.T) {
		check(t, station("index manager: boom", belowQuorum),
			"index manager: boom; storage: 1 of 3 required replicas live, below the quorum of 2")
	})

	t.Run("an all-in-one node's own line is not duplicated", func(t *testing.T) {
		// There the node IS the control leader, so service/admin.go already filled
		// this line from its own control plane.
		check(t, station("storage: 1 of 3 required replicas live, below the quorum of 2", belowQuorum),
			"storage: 1 of 3 required replicas live, below the quorum of 2")
	})

	t.Run("healthy and unknown both leave the node's answer alone", func(t *testing.T) {
		check(t, station("ok", map[string]degradationVerdict{"kb-1": {degraded: false}}), "ok")
		check(t, station("ok", nil), "ok")
	})
}
