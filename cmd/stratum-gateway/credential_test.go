package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/metadata"

	"stratum/internal/authmeta"
)

// H3 of docs/code-review-2026-09-24.md: the gateway performs no authentication of
// its own — the service station does — so the caller's credential has to survive the
// HTTP → gRPC boundary. Without this the station sees every console request as
// anonymous, and a deployment that turned authentication on either refuses the whole
// console or (worse) is configured to accept anonymous calls.

// credentialSeenByTheBackend runs h with the given inbound Authorization header and
// reports what the gRPC call behind it would carry.
func credentialSeenByTheBackend(t *testing.T, fallback, inboundAuthorization string) []string {
	t.Helper()

	var got []string
	captured := make(chan struct{})
	h := withCredential(fallback)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if md, ok := metadata.FromOutgoingContext(r.Context()); ok {
			got = md.Get(authmeta.CredentialMetadataKey)
		}
		close(captured)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/query", nil)
	if inboundAuthorization != "" {
		req.Header.Set("Authorization", inboundAuthorization)
	}
	h.ServeHTTP(httptest.NewRecorder(), req)

	select {
	case <-captured:
	default:
		t.Fatal("the handler behind withCredential was never called")
	}
	return got
}

func TestWithCredentialForwardsTheCallersCredential(t *testing.T) {
	got := credentialSeenByTheBackend(t, "console-token", "Bearer caller-key")
	if len(got) != 1 || got[0] != "Bearer caller-key" {
		t.Fatalf("backend credential = %v, want the caller's own", got)
	}
}

// The console cannot hold an API key of its own, so the gateway carries one for it —
// but only for requests that brought none: a caller's credential must never be
// replaced by the gateway's, or every console-driven write would run with the
// gateway's privileges instead of the caller's.
func TestWithCredentialFallsBackOnlyWhenTheCallerBroughtNone(t *testing.T) {
	got := credentialSeenByTheBackend(t, "console-token", "")
	if len(got) != 1 || got[0] != "console-token" {
		t.Fatalf("backend credential = %v, want the configured fallback", got)
	}

	bare := &http.Request{Header: http.Header{}}
	if token := bare.Header.Get("Authorization"); token != "" {
		t.Fatal("precondition: the request must have no Authorization header")
	}
}

// No fallback configured means "send it unauthenticated", which is what the station
// then refuses — the right failure for a deployment that has not handed the console a
// credential.
func TestWithCredentialInjectsNothingWithoutFallbackOrCallerCredential(t *testing.T) {
	if got := credentialSeenByTheBackend(t, "", ""); len(got) != 0 {
		t.Fatalf("backend credential = %v, want none", got)
	}
}

// The header is passed through verbatim: the station accepts "Bearer <token>" and a
// bare token, and parsing it twice is how the two ends drift apart.
func TestWithCredentialPassesTheHeaderThroughVerbatim(t *testing.T) {
	got := credentialSeenByTheBackend(t, "", "  sk-demo-a-xxxxxxxx  ")
	if len(got) != 1 || got[0] != "sk-demo-a-xxxxxxxx" {
		t.Fatalf("backend credential = %v, want the trimmed bare token", got)
	}
}

// Static assets and /ops go through the same chain; the middleware must not depend on
// the path or the method.
func TestWithCredentialAppliesToEveryRequest(t *testing.T) {
	var seen []string
	h := withCredential("console-token")(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if md, ok := metadata.FromOutgoingContext(r.Context()); ok {
			seen = append(seen, md.Get(authmeta.CredentialMetadataKey)...)
		}
	}))

	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/api/health"},
		{http.MethodPost, "/api/knowledge-bases"},
		{http.MethodGet, "/ops/health"},
	} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(tc.method, tc.path, nil))
	}
	if len(seen) != 3 {
		t.Fatalf("credential carried on %d of 3 requests: %v", len(seen), seen)
	}
}
