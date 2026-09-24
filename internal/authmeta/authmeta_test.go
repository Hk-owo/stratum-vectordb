package authmeta

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

// H4 of docs/code-review-2026-09-24.md: the internal trust mark used to be the
// constant "1", which anything able to reach a node's port could send. It is now an
// HMAC over a timestamp under a key shared with the station, and this file is what
// pins that down — the service-layer tests only exercise the gate that consumes it.

const testKey = "station-key-for-tests"

func TestSigner_StampThenVerify(t *testing.T) {
	s := NewSigner([]byte(testKey), 0)
	if !s.Enabled() {
		t.Fatal("a signer with a key must be enabled")
	}

	incoming := asIncoming(s.Stamp(context.Background()))
	if !s.Verify(incoming) {
		t.Fatal("a stamp this signer produced must verify with the same key")
	}
}

func TestSigner_VerifyRejectsAForgedMAC(t *testing.T) {
	s := NewSigner([]byte(testKey), 0)
	stamped := outgoingValue(t, s.Stamp(context.Background()))

	// Flip one hex character of the MAC: the same shape, a different value.
	tampered := []byte(stamped)
	tampered[len(tampered)-1] = flipHex(tampered[len(tampered)-1])

	if s.Verify(incomingWithValue(string(tampered))) {
		t.Fatal("a stamp whose MAC does not match its timestamp must be refused")
	}
	if s.Verify(incomingWithValue(stamped + "00")) {
		t.Fatal("a padded MAC must be refused")
	}
}

func TestSigner_VerifyRejectsAStampFromAnotherKey(t *testing.T) {
	mine := NewSigner([]byte(testKey), 0)
	theirs := NewSigner([]byte("some-other-secret"), 0)

	if mine.Verify(asIncoming(theirs.Stamp(context.Background()))) {
		t.Fatal("a stamp signed under another key must be refused")
	}
}

// The timestamp is what makes an observed mark expire. Signing with a clock two
// hours behind and verifying with the real one is the same thing as waiting two
// hours, without the wait.
func TestSigner_VerifyRejectsAStaleStamp(t *testing.T) {
	s := NewSigner([]byte(testKey), time.Minute)
	s.now = func() time.Time { return time.Now().Add(-2 * time.Hour) }
	stale := asIncoming(s.Stamp(context.Background()))

	fresh := NewSigner([]byte(testKey), time.Minute)
	if fresh.Verify(stale) {
		t.Fatal("a stamp older than the TTL must be refused")
	}
}

// A mark stamped by a clock far ahead of ours is not "fresh"; it is a mark we
// cannot place in time, and the skew bound applies in both directions.
func TestSigner_VerifyRejectsAFutureStamp(t *testing.T) {
	s := NewSigner([]byte(testKey), time.Minute)
	s.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	ahead := asIncoming(s.Stamp(context.Background()))

	if NewSigner([]byte(testKey), time.Minute).Verify(ahead) {
		t.Fatal("a stamp from far in the future must be refused")
	}
}

// The MAC covers the version tag, so a future format cannot be replayed into code
// that only knows this one — and nothing malformed may be accepted on the way.
func TestSigner_VerifyRejectsMalformedStamps(t *testing.T) {
	s := NewSigner([]byte(testKey), time.Minute)

	for name, value := range map[string]string{
		"the old constant mark": "1",
		"empty":                 "",
		"wrong version tag":     "v2:1700000000:" + s.mac("1700000000"),
		"missing MAC":           "v1:1700000000",
		"extra field":           "v1:1700000000:" + s.mac("1700000000") + ":extra",
		"non-numeric timestamp": "v1:not-a-time:" + s.mac("not-a-time"),
	} {
		if s.Verify(incomingWithValue(value)) {
			t.Errorf("%s must be refused", name)
		}
	}
	if s.Verify(context.Background()) {
		t.Error("a call with no metadata at all must be refused")
	}
}

// No key means no trust: a deployment that never configured one must not silently
// start accepting marks — including marks produced by another keyless signer.
func TestSigner_WithoutAKeyStampsAndVerifiesNothing(t *testing.T) {
	disabled := NewSigner(nil, 0)
	if disabled.Enabled() {
		t.Fatal("a signer without a key must be disabled")
	}

	stamped := disabled.Stamp(context.Background())
	if md, ok := metadata.FromOutgoingContext(stamped); ok && len(md.Get(VerifiedMetadataKey)) > 0 {
		t.Fatalf("a disabled signer must not stamp anything, got %v", md.Get(VerifiedMetadataKey))
	}
	if disabled.Verify(asIncoming(NewSigner(nil, 0).Stamp(context.Background()))) {
		t.Fatal("a keyless signer must not verify a keyless stamp")
	}
}

// A nil *Signer is what an unconfigured deployment ends up with in several call
// sites, so the methods have to be safe on it.
func TestSigner_NilIsSafe(t *testing.T) {
	var s *Signer
	if s.Enabled() {
		t.Error("a nil signer is not enabled")
	}
	if md, ok := metadata.FromOutgoingContext(s.Stamp(context.Background())); ok {
		t.Errorf("a nil signer must not stamp, got %v", md)
	}
	if s.Verify(context.Background()) {
		t.Error("a nil signer verifies nothing")
	}
}

// A non-positive TTL is a configuration mistake, not "marks never expire".
func TestSigner_NonPositiveTTLFallsBackToTheDefault(t *testing.T) {
	s := NewSigner([]byte(testKey), -time.Second)
	if s.ttl != DefaultMarkTTL {
		t.Fatalf("ttl = %v, want the default %v", s.ttl, DefaultMarkTTL)
	}
	if !s.Verify(asIncoming(s.Stamp(context.Background()))) {
		t.Fatal("the round trip must still work")
	}
}

// asIncoming moves a context's OUTGOING metadata into the INCOMING metadata a
// receiver sees — the hop Stamp/Verify do not perform when called directly.
func asIncoming(ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return context.Background()
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func incomingWithValue(value string) context.Context {
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(VerifiedMetadataKey, value))
}

func outgoingValue(t *testing.T, ctx context.Context) string {
	t.Helper()
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("Stamp produced no metadata")
	}
	values := md.Get(VerifiedMetadataKey)
	if len(values) != 1 {
		t.Fatalf("stamp = %v, want exactly one value", values)
	}
	return values[0]
}

func flipHex(c byte) byte {
	switch {
	case c >= '0' && c <= '8':
		return c + 1
	case c == '9':
		return 'a'
	case c >= 'a' && c < 'f':
		return c + 1
	case c == 'f':
		return '0'
	default:
		return 'f'
	}
}
