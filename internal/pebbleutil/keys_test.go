package pebbleutil

import (
	"bytes"
	"sort"
	"testing"
)

func TestEncodeString_NoAmbiguousBoundary(t *testing.T) {
	// kbID="ab" + docID="c" must not collide with kbID="a" + docID="bc".
	a := append(EncodeString("ab"), EncodeString("c")...)
	b := append(EncodeString("a"), EncodeString("bc")...)
	if bytes.Equal(a, b) {
		t.Fatalf("EncodeString concatenation is ambiguous: %x == %x", a, b)
	}
}

func TestEncodeString_RoundTripLength(t *testing.T) {
	s := "hello world"
	enc := EncodeString(s)
	if len(enc) != 4+len(s) {
		t.Fatalf("encoded length = %d, want %d", len(enc), 4+len(s))
	}
	if string(enc[4:]) != s {
		t.Fatalf("encoded content = %q, want %q", enc[4:], s)
	}
}

func TestEncodeUint64_LexicographicOrderMatchesNumeric(t *testing.T) {
	values := []uint64{0, 1, 2, 255, 256, 65535, 65536, 1 << 32, 1<<63 - 1}
	encoded := make([][]byte, len(values))
	for i, v := range values {
		encoded[i] = EncodeUint64(v)
	}
	// values is already sorted ascending; encoded byte slices must sort
	// the same way under bytes.Compare.
	sorted := append([][]byte(nil), encoded...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i], sorted[j]) < 0 })
	for i := range encoded {
		if !bytes.Equal(encoded[i], sorted[i]) {
			t.Fatalf("byte ordering does not match numeric ordering at index %d", i)
		}
	}
}

func TestEncodeVersionID_PanicsOnNegative(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for negative versionID, got none")
		}
	}()
	EncodeVersionID(-1)
}

func TestPrefixSuccessor_ScopesExactlyThePrefix(t *testing.T) {
	prefix := EncodeString("kb1")
	succ := PrefixSuccessor(prefix)

	// A key with this exact prefix must fall within [prefix, succ).
	withPrefix := append(append([]byte(nil), prefix...), []byte("anything")...)
	if bytes.Compare(withPrefix, prefix) < 0 {
		t.Fatalf("key with prefix sorts before prefix itself")
	}
	if bytes.Compare(withPrefix, succ) >= 0 {
		t.Fatalf("key with prefix does not sort before PrefixSuccessor: key=%x succ=%x", withPrefix, succ)
	}

	// A key belonging to a lexicographically later, unrelated prefix must
	// fall at or after succ.
	otherPrefix := EncodeString("kb2")
	otherKey := append(append([]byte(nil), otherPrefix...), []byte("anything")...)
	if bytes.Compare(otherKey, succ) < 0 {
		t.Fatalf("unrelated later-prefixed key sorts before PrefixSuccessor: key=%x succ=%x", otherKey, succ)
	}
}

func TestPrefixSuccessor_AllFF(t *testing.T) {
	prefix := []byte{0xFF, 0xFF}
	succ := PrefixSuccessor(prefix)
	if succ != nil {
		t.Fatalf("PrefixSuccessor(all-0xFF) = %x, want nil (no finite successor)", succ)
	}
}

func TestPrefixSuccessor_TrailingFFCarries(t *testing.T) {
	prefix := []byte{0x01, 0xFF}
	succ := PrefixSuccessor(prefix)
	want := []byte{0x02}
	if !bytes.Equal(succ, want) {
		t.Fatalf("PrefixSuccessor(%x) = %x, want %x", prefix, succ, want)
	}
}

func TestPrefixSuccessor_Empty(t *testing.T) {
	succ := PrefixSuccessor(nil)
	if succ != nil {
		t.Fatalf("PrefixSuccessor(empty) = %x, want nil", succ)
	}
}
