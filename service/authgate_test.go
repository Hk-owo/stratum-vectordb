package service

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"stratum/internal/router"
)

// asArrivingFromTheStation builds the context a node actually sees when the
// station forwards a call. WithVerifiedMark writes OUTGOING metadata — the
// station's side of the wire — and a real gRPC hop turns that into the
// receiver's INCOMING metadata. The gate here is called directly, with no hop,
// so the test constructs the receiver's view itself.
func asArrivingFromTheStation() context.Context {
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(router.VerifiedMetadataKey, "1"))
}

// TestClientFacingClassifiesTheTwoKindOfTraffic pins the line the gate uses.
func TestClientFacingClassifiesTheTwoKindOfTraffic(t *testing.T) {
	client := []string{
		"/stratum.QueryService/Query",
		"/stratum.KnowledgeBaseService/CreateVersion",
		"/stratum.AdminService/GetSystemStatus",
	}
	for _, m := range client {
		if !ClientFacing(m) {
			t.Errorf("%s should be client-facing", m)
		}
	}
	internal := []string{
		"/stratum.DataSyncService/PushVersionData",
		"/stratum.DataSyncService/ReportDataVersions",
		"/stratum.DataSyncService/LocalVersion",
		"/stratum.InternalService/Propose",
	}
	for _, m := range internal {
		if ClientFacing(m) {
			t.Errorf("%s is node-to-node collaboration and must not be gated", m)
		}
	}
}

// TestAuthGate_GatesClientFacingCallsOnly is the split the design asks for:
// the client-facing surface refuses a call that did not come through the
// station, while internal collaboration keeps working untouched.
//
// Gating the internal side would break fan-out, catch-up and Raft forwarding
// outright — they carry no end user's credential and never should.
func TestAuthGate_GatesClientFacingCallsOnly(t *testing.T) {
	gate := UnaryAuthGate(true)
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	for _, m := range []string{
		"/stratum.QueryService/Query",
		"/stratum.KnowledgeBaseService/CreateVersion",
		"/stratum.AdminService/GetSystemStatus",
	} {
		if _, err := gate(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: m}, handler); status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s without the station's mark: err = %v, want Unauthenticated", m, err)
		}
		if _, err := gate(asArrivingFromTheStation(), nil, &grpc.UnaryServerInfo{FullMethod: m}, handler); err != nil {
			t.Errorf("%s with the station's mark: %v", m, err)
		}
	}

	for _, m := range []string{
		"/stratum.DataSyncService/PushVersionData",
		"/stratum.DataSyncService/ReportDataVersions",
		"/stratum.InternalService/Propose",
	} {
		if _, err := gate(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: m}, handler); err != nil {
			t.Errorf("%s is internal and must not be gated, got %v", m, err)
		}
	}
}

// TestAuthGate_OffAcceptsEverything keeps a deployment without a station
// working: the gate is opt-in, and turning it on is what closes the
// direct-to-node hole.
func TestAuthGate_OffAcceptsEverything(t *testing.T) {
	gate := UnaryAuthGate(false)
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	if _, err := gate(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/stratum.QueryService/Query"}, handler); err != nil {
		t.Errorf("with the gate off every call is accepted, got %v", err)
	}
}
