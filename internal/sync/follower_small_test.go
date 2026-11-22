package sync

import (
	"context"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
	"stratum/internal/types"
)

// DigestOf is what a takeover announcement carries, so it has to be computed from
// the replica's own document set rather than invented: a follower verifies the
// data it pulls against this value.
func TestFollower_DigestOfReadsItsOwnDocumentSet(t *testing.T) {
	ctx := context.Background()
	fixture := newLeaderFixture(t)
	for _, doc := range []string{"doc-a", "doc-b"} {
		if err := fixture.versionDoc.Write(ctx, "kb", 1, doc); err != nil {
			t.Fatal(err)
		}
	}
	f := &Follower{versionDoc: fixture.versionDoc}
	got, err := f.DigestOf(ctx, "kb", 1)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if want := ComputeDocIDSetHash([]string{"doc-a", "doc-b"}); got != want {
		t.Errorf("digest = %q, want %q", got, want)
	}
	// A version with no documents has the empty-set digest, not an empty string:
	// "" reads downstream as "no digest was ever committed".
	empty, err := f.DigestOf(ctx, "kb", 2)
	if err != nil {
		t.Fatalf("empty digest: %v", err)
	}
	if empty != types.EmptyDocIDSetHash {
		t.Errorf("empty digest = %q, want %q", empty, types.EmptyDocIDSetHash)
	}
}

// The pull handshake bound is configuration, and a non-positive value must keep the
// default rather than become an instant timeout.
func TestFollower_WithPullDialTimeout(t *testing.T) {
	f := NewFollower(nil, nil, nil, nil, nil)
	if got := f.dialTimeout(); got != DefaultPullDialTimeout {
		t.Errorf("default = %v, want %v", got, DefaultPullDialTimeout)
	}
	fZero := NewFollower(nil, nil, nil, nil, nil, WithPullDialTimeout(0))
	if got := fZero.dialTimeout(); got != DefaultPullDialTimeout {
		t.Errorf("zero value = %v, want the default kept", got)
	}
	fConfigured := NewFollower(nil, nil, nil, nil, nil, WithPullDialTimeout(3*time.Second))
	if got := fConfigured.dialTimeout(); got != 3*time.Second {
		t.Errorf("configured = %v, want 3s", got)
	}
}

// An empty frame is not a frame: the encoder refuses it so an artifact can never be
// a stream of zero-length records that only the reader would notice.
func TestEncodeMigrationFrame_RefusesEmptyFrames(t *testing.T) {
	if _, err := encodeMigrationFrame(&pb.MigrationDataFrame{}); err == nil {
		t.Error("an empty frame was encoded")
	}
	frame := &pb.MigrationDataFrame{OperationId: "move"}
	b, err := encodeMigrationFrame(frame)
	if err != nil || len(b) == 0 {
		t.Fatalf("encode = %d bytes, %v", len(b), err)
	}
}
