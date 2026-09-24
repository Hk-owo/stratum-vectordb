// Package authmeta carries the metadata keys that distinguish "this call arrived
// through a service station" from "this call reached the port directly", and the
// signature that makes that claim unforgeable.
//
// It is its own package because three layers need the same constants and none of
// them owns the others: internal/router stamps the mark when it forwards,
// internal/raft stamps it when a storage node reads metadata on the cluster's
// behalf, and the service layer reads it to decide whether to serve a
// client-facing call.
package authmeta

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
)

const (
	// CredentialMetadataKey carries a caller's API key. It is the standard
	// Authorization header, so an ordinary gRPC client can set it without
	// knowing anything about Stratum.
	CredentialMetadataKey = "authorization"

	// VerifiedMetadataKey marks a call that came from inside the system: a
	// station forwarding a client's request, or a node reading metadata on the
	// cluster's behalf.
	//
	// It carries no identity on purpose. A receiving node does not need to know
	// WHO is asking — only that something with the authority to ask vouched for
	// the request. What it does have to be is UNFORGEABLE: the mark used to be
	// the literal "1", so any caller able to reach a node's port could satisfy a
	// gate that was otherwise doing its job.
	VerifiedMetadataKey = "x-stratum-authenticated"

	// markVersion is the mark's own version tag. It is covered by the HMAC, so a
	// future format cannot be replayed into code that only knows this one.
	markVersion = "v1"
)

// DefaultMarkTTL is how long a stamp stays acceptable. It bounds replay: a mark
// observed on the wire is worthless once this much wall-clock time has passed.
//
// The window has to cover the clock skew between processes sharing the key —
// they are inside one cluster by construction — which is why it is tens of
// seconds rather than one. A tighter window buys nothing an attacker already
// able to read internal traffic could not do anyway, and costs a cluster that
// refuses its own station whenever a clock steps.
const DefaultMarkTTL = 30 * time.Second

// Verifier reports whether an incoming call carries a valid internal mark.
type Verifier interface {
	Verify(ctx context.Context) bool
}

// Signer stamps outbound calls with the internal mark and verifies it on the way
// in. Both halves need the same key, and that is the point: the mark is an HMAC
// over a timestamp, so a party without the key cannot produce one, and the
// timestamp keeps an observed mark from being replayed indefinitely.
//
// Where the key comes from is a deployment concern (a config field on the node,
// a flag on the station). The cluster is also expected to be unreachable from
// outside; this is defence in depth, and the previous constant mark was the
// reason it was needed: a gate plus a forgeable mark is a gate in name only.
type Signer struct {
	key []byte
	ttl time.Duration
	now func() time.Time // injectable for tests
}

// NewSigner returns a Signer over key. An empty key yields a DISABLED signer: it
// stamps nothing and verifies nothing, so a cluster that never configured a key
// keeps its previous behaviour instead of locking itself out.
func NewSigner(key []byte, ttl time.Duration) *Signer {
	if ttl <= 0 {
		ttl = DefaultMarkTTL
	}
	return &Signer{key: key, ttl: ttl, now: time.Now}
}

// Enabled reports whether this signer can produce and check marks at all.
func (s *Signer) Enabled() bool { return s != nil && len(s.key) > 0 }

// Stamp returns ctx carrying a fresh mark. A disabled signer returns ctx
// unchanged — see NewSigner.
func (s *Signer) Stamp(ctx context.Context) context.Context {
	if !s.Enabled() {
		return ctx
	}
	seconds := strconv.FormatInt(s.now().Unix(), 10)
	return metadata.AppendToOutgoingContext(ctx, VerifiedMetadataKey,
		markVersion+":"+seconds+":"+s.mac(seconds))
}

// Verify reports whether ctx carries a mark this signer could have produced, and
// one that is still fresh.
//
// Every failure is a plain false: the caller's only decision is "serve or
// refuse", and telling an attacker which half failed hands them an oracle for
// probing the key and the clock.
func (s *Signer) Verify(ctx context.Context) bool {
	if !s.Enabled() {
		return false
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	values := md.Get(VerifiedMetadataKey)
	if len(values) == 0 {
		return false
	}
	parts := strings.Split(values[0], ":")
	if len(parts) != 3 || parts[0] != markVersion {
		return false
	}
	seconds, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return false
	}
	if skew := s.now().Sub(time.Unix(seconds, 0)); skew > s.ttl || skew < -s.ttl {
		return false // stale, or stamped by a clock far ahead of ours
	}
	// Constant-time compare (L1 of docs/code-review-2026-09-24.md): the wrong MAC
	// and the right one must not be distinguishable by timing.
	return subtle.ConstantTimeCompare([]byte(parts[2]), []byte(s.mac(parts[1]))) == 1
}

// mac returns the hex HMAC-SHA256 over "<version>:<unix seconds>".
func (s *Signer) mac(seconds string) string {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte(markVersion + ":" + seconds))
	return hex.EncodeToString(m.Sum(nil))
}
