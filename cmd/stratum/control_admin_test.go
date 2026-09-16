package main

import (
	"context"
	"testing"

	pb "stratum/api/proto/stratum"
	"stratum/internal/types"
)

// clusterStatusRaftNode answers GetClusterStatus with a fixed view, so the
// control-node admin service can be tested without the rest of the node. It
// embeds the reconcile stub for the remaining methods (a control node never calls
// them here) and overrides the one that matters.
type clusterStatusRaftNode struct {
	*reconcileRaftNode
	status types.ClusterStatus
}

func (r clusterStatusRaftNode) GetClusterStatus(context.Context) (types.ClusterStatus, error) {
	return r.status, nil
}

// A control node must be able to answer GetClusterStatus. It is the only node
// that has the Raft view, and every storage node's cursor reporter resolves the
// leader through exactly this call. Leaving AdminService unregistered on control
// nodes made it answer "Unimplemented: unknown service stratum.AdminService", so
// §7.13.4's report never left the storage node — and the chain tails (lag
// catch-up), the holder view and the station's read routing went with it.
func TestControlAdminService_GetClusterStatusReportsTheRaftView(t *testing.T) {
	rn := clusterStatusRaftNode{status: types.ClusterStatus{
		HasLeader:   true,
		LeaderID:    2,
		MemberCount: 3,
	}}
	svc := newControlAdminService(7, rn)

	resp, err := svc.GetClusterStatus(context.Background(), &pb.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if resp.GetNodeId() != 7 {
		t.Errorf("node_id = %d, want 7", resp.GetNodeId())
	}
	if !resp.GetHasLeader() || resp.GetLeaderId() != 2 {
		t.Errorf("leader = (%v, %d), want (true, 2)", resp.GetHasLeader(), resp.GetLeaderId())
	}
	if resp.GetMemberCount() != 3 {
		t.Errorf("member_count = %d, want 3", resp.GetMemberCount())
	}
}

// The rest of AdminService reads the local stores, which a control node does not
// have. It must answer Unimplemented rather than pretend otherwise — that is what
// embedding UnimplementedAdminServiceServer buys, and it is what keeps a control
// node from looking like a half-broken storage node.
func TestControlAdminService_StoreReadingCallsAreUnimplemented(t *testing.T) {
	svc := newControlAdminService(7, clusterStatusRaftNode{})

	if _, err := svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{}); err == nil {
		t.Error("HealthCheck on a control node = nil error, want Unimplemented")
	}
}
