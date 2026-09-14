package router

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"stratum/internal/authmeta"
)

// The metadata keys live in internal/authmeta because three layers share them
// and none owns the others: the station stamps the mark, a storage node stamps
// it when reading metadata, and the service layer reads it.
const (
	// CredentialMetadataKey carries the caller's API key.
	CredentialMetadataKey = authmeta.CredentialMetadataKey

	// VerifiedMetadataKey marks a call that came from inside the system.
	VerifiedMetadataKey = authmeta.VerifiedMetadataKey
)

// Grant is what a tenant may do with one knowledge base.
//
// The granularity stops at the knowledge base because that is the only
// isolation unit the system actually has: durability policy, retention, serving
// replicas and the other per-tenant settings all hang off a KB. A finer model
// (per document) would have no existing mechanism able to enforce it — a layer
// of complexity with nothing underneath it.
//
// Two verbs are enough: read (query) and write (create/delete a version and
// everything else that commits state). Anything finer would be verbs nothing
// distinguishes.
type Grant struct {
	Read  bool
	Write bool
}

// Principal is an authenticated caller.
type Principal struct {
	// TenantID comes from the credential lookup and never from a request field.
	// A client able to assert its own tenant would make the credential check
	// decorative: the token may well be valid, but whose it is cannot be the
	// client's to declare.
	TenantID string

	// Grants is the tenant's access, keyed by knowledge base ID.
	Grants map[string]Grant
}

// Allows reports whether the principal may perform (kbID, write).
func (p Principal) Allows(kbID string, write bool) bool {
	g, ok := p.Grants[kbID]
	if !ok {
		return false
	}
	if write {
		return g.Write
	}
	return g.Read
}

// Authenticator is the service station's single authentication checkpoint
// (§9.3(5)).
//
// The credential is an opaque API key and the station does not parse it: it
// looks the token up in a table mapping token → tenant → grants. That is
// deliberate. A self-describing token (JWT and relatives) would drag signature
// verification and claim semantics into the routing layer, and the one claim
// that matters here — which tenant — is exactly the one that must come from a
// trusted lookup rather than from the token's own contents.
//
// How the table is maintained and how credentials are issued are deployment
// concerns, deliberately outside this type: it takes a lookup function.
type Authenticator struct {
	lookup func(token string) (Principal, bool)
}

// NewAuthenticator returns an authenticator backed by lookup. A nil lookup
// rejects every credential.
func NewAuthenticator(lookup func(token string) (Principal, bool)) *Authenticator {
	return &Authenticator{lookup: lookup}
}

// ErrUnauthenticated is what a caller with no usable credential gets.
var ErrUnauthenticated = status.Error(codes.Unauthenticated, "router: missing or invalid credential")

// Authenticate verifies the caller's credential and returns its principal.
func (a *Authenticator) Authenticate(ctx context.Context) (Principal, error) {
	token, ok := bearerToken(ctx)
	if !ok {
		return Principal{}, ErrUnauthenticated
	}
	if a == nil || a.lookup == nil {
		return Principal{}, ErrUnauthenticated
	}
	p, ok := a.lookup(token)
	if !ok {
		return Principal{}, ErrUnauthenticated
	}
	return p, nil
}

// Authorize authenticates the caller and checks (kbID, write) in one step.
//
// An empty kbID means a knowledge-base-independent request (listing KBs, health
// checks): only the credential is required, since there is no per-KB grant to
// consult.
func (a *Authenticator) Authorize(ctx context.Context, kbID string, write bool) (Principal, error) {
	p, err := a.Authenticate(ctx)
	if err != nil {
		return Principal{}, err
	}
	if kbID != "" && !p.Allows(kbID, write) {
		return Principal{}, status.Errorf(codes.PermissionDenied,
			"router: tenant %q may not %s knowledge base %q", p.TenantID, verb(write), kbID)
	}
	return p, nil
}

func verb(write bool) string {
	if write {
		return "write"
	}
	return "read"
}

// bearerToken pulls the API key out of the incoming metadata. Both
// "Bearer <token>" and a bare token are accepted: the first is what standard
// gRPC clients send, the second keeps ad-hoc tools (grpcurl) usable.
func bearerToken(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	values := md.Get(CredentialMetadataKey)
	if len(values) == 0 {
		return "", false
	}
	token := strings.TrimSpace(values[0])
	if token == "" {
		return "", false
	}
	if rest, found := strings.CutPrefix(token, "Bearer "); found {
		token = strings.TrimSpace(rest)
	}
	if token == "" {
		return "", false
	}
	return token, true
}
