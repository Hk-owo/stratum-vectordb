package service

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"stratum/internal/authmeta"
)

// testStationSecret is the shared key these tests stamp and verify with. Both
// sides hold the same value by construction, which is the whole point of the
// mark: a party without it cannot produce one.
const testStationSecret = "test-station-secret"

// testStation is the signer the service station would hold.
func testStation() *authmeta.Signer {
	return authmeta.NewSigner([]byte(testStationSecret), 0)
}

// asIncoming moves a context's OUTGOING metadata into the INCOMING metadata a
// receiver sees. Stamp writes the station's side of the wire, and a real gRPC hop
// turns one into the other; the gate is called directly here, with no hop, so the
// test performs that step itself.
func asIncoming(ctx context.Context) context.Context {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		return metadata.NewIncomingContext(context.Background(), md)
	}
	return context.Background()
}

// arrivingFromTheStation is the context a node sees when the station forwards a
// call with a valid mark.
func arrivingFromTheStation() context.Context {
	return asIncoming(testStation().Stamp(context.Background()))
}

// incomingWith builds the receiver's view of a call carrying a single mark value.
func incomingWith(value string) context.Context {
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(authmeta.VerifiedMetadataKey, value))
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
	gate := UnaryAuthGate(true, testStation())
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	for _, m := range []string{
		"/stratum.QueryService/Query",
		"/stratum.KnowledgeBaseService/CreateVersion",
		"/stratum.AdminService/GetSystemStatus",
	} {
		if _, err := gate(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: m}, handler); status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s without the station's mark: err = %v, want Unauthenticated", m, err)
		}
		if _, err := gate(arrivingFromTheStation(), nil, &grpc.UnaryServerInfo{FullMethod: m}, handler); err != nil {
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
	gate := UnaryAuthGate(false, nil)
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	if _, err := gate(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/stratum.QueryService/Query"}, handler); err != nil {
		t.Errorf("with the gate off every call is accepted, got %v", err)
	}
}

// TestAuthGate_RefusesAForgedMark is H4 of docs/code-review-2026-09-24.md: the
// mark used to be the constant "1", which anything able to reach the node's port
// could send. It is now an HMAC only the station can produce, so a caller echoing
// the old value — or replaying someone else's mark — gets nothing.
func TestAuthGate_RefusesAForgedMark(t *testing.T) {
	gate := UnaryAuthGate(true, testStation())
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/stratum.QueryService/Query"}

	otherSigner := asIncoming(authmeta.NewSigner([]byte("a-different-secret"), 0).
		Stamp(context.Background()))

	for name, ctx := range map[string]context.Context{
		"the old constant mark":      incomingWith("1"),
		"a well-formed but fake MAC": incomingWith("v1:1700000000:00"),
		"another signer's real mark": otherSigner,
		"no metadata at all":         context.Background(),
	} {
		if _, err := gate(ctx, nil, info, handler); status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s: err = %v, want Unauthenticated", name, err)
		}
	}
}

// TestAuthGate_RefusesAMarkWithNoSharedKey: with require=true and no secret
// configured, the node must refuse rather than accept unverified calls. "No key"
// cannot quietly mean "no gate".
func TestAuthGate_RefusesAMarkWithNoSharedKey(t *testing.T) {
	gate := UnaryAuthGate(true, authmeta.NewSigner(nil, 0))
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	// Even a stamp produced by a signer with the same (empty) key is evidence of
	// nothing, so the call is refused.
	ctx := asIncoming(authmeta.NewSigner(nil, 0).Stamp(context.Background()))
	if _, err := gate(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/stratum.QueryService/Query"}, handler); status.Code(err) != codes.Unauthenticated {
		t.Errorf("err = %v, want Unauthenticated", err)
	}
}
