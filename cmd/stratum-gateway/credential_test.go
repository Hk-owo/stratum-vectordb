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
//
// C1 of docs/code-review-2026-10-18.md: the fallback the console borrows is a
// PRIVILEGE decision (scripts/gateway.sh mints that token with kb_ids:["*"]), so it is
// handed out only to the console's own pages — /api/* calls from a same-origin
// document. Everything else that arrives without a credential is refused instead of
// being upgraded.

// credentialSeenByTheBackend runs h with the given inbound Authorization header and
// reports what the gRPC call behind it would carry. The request looks like one a
// console page makes: same-origin, per the browser-set Fetch Metadata header.
func credentialSeenByTheBackend(t *testing.T, fallback, inboundAuthorization string) []string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/query", nil)
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	if inboundAuthorization != "" {
		req.Header.Set("Authorization", inboundAuthorization)
	}
	got, called := serveWithCredential(fallback, req)
	if !called {
		t.Fatal("the handler behind withCredential was never called")
	}
	return got
}

// serveWithCredential runs one request through the middleware and reports what the
// wrapped handler saw, plus whether it ran at all.
func serveWithCredential(fallback string, req *http.Request) (carried []string, called bool) {
	var got []string
	h := withCredential(fallback)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		called = true
		if md, ok := metadata.FromOutgoingContext(r.Context()); ok {
			got = md.Get(authmeta.CredentialMetadataKey)
		}
	}))
	h.ServeHTTP(httptest.NewRecorder(), req)
	return got, called
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

// TestWithCredentialRefusesAnAnonymousCallFromOutsideTheConsole is the C1 rule: the
// fallback token is the console's, and it reaches the station with kb_ids:["*"]. An
// anonymous /api/ request that no browser made — no Authorization, no Fetch Metadata,
// no Origin — used to be served AS THE CONSOLE, which on a published :8081 is full
// access for everyone who can reach the port.
func TestWithCredentialRefusesAnAnonymousCallFromOutsideTheConsole(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/knowledge-bases", nil)

	h := withCredential("console-token")(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("the handler ran with the console's credential for an anonymous request")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

// A cross-site page is not the console, even though a browser did make the request.
func TestWithCredentialRefusesACrossSiteCall(t *testing.T) {
	for _, tc := range []struct{ name, header, value string }{
		{"fetch metadata", "Sec-Fetch-Site", "cross-site"},
		{"fetch metadata, same-site", "Sec-Fetch-Site", "same-site"},
		{"origin of another host", "Origin", "http://evil.example"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/knowledge-bases", nil)
			req.Header.Set(tc.header, tc.value)

			h := withCredential("console-token")(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("the handler ran with the console's credential for a cross-site request")
			}))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", rec.Code)
			}
		})
	}
}

// A browser too old for Fetch Metadata still identifies itself with Origin, and that
// must keep working or the console would break on it.
func TestWithCredentialAcceptsASameOriginRequestIdentifiedByOrigin(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/query", nil)
	req.Header.Set("Origin", "http://"+req.Host)

	got, called := serveWithCredential("console-token", req)
	if !called {
		t.Fatal("a same-origin request identified by Origin must be served")
	}
	if len(got) != 1 || got[0] != "console-token" {
		t.Fatalf("backend credential = %v, want the fallback", got)
	}
}

// The fallback belongs to the station-facing API only. Static assets and /ops never go
// through the station, so they must not silently acquire the console's credential
// either — /ops has its own guards, and the assets need none.
func TestWithCredentialAppliesOnlyToTheAPIPrefix(t *testing.T) {
	for _, tc := range []struct {
		path string
		want bool
	}{
		{"/api/health", true},
		{"/api/knowledge-bases", true},
		{"/ops/health", false},
		{"/index.html", false},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		got, called := serveWithCredential("console-token", req)
		if !called {
			t.Errorf("%s: the request was refused", tc.path)
			continue
		}
		if carried := len(got) == 1 && got[0] == "console-token"; carried != tc.want {
			t.Errorf("%s: credential carried = %v (%v), want %v", tc.path, carried, got, tc.want)
		}
	}
}
