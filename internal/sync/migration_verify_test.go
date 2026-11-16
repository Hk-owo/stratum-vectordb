package sync

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"stratum/internal/chunkstore"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
)

// migrationTargetFollower builds a follower over real Pebble stores, so the
// installation verification runs against documents that were actually written.
func migrationTargetFollower(t *testing.T) (*Follower, *leaderFixture, *chunkstore.MockChunkStore) {
	t.Helper()
	fixture := newLeaderFixture(t)
	chunks := chunkstore.NewMockChunkStore()
	return NewFollower(fixture.docStore, fixture.chunkDoc, fixture.versionDoc, chunks, nil), fixture, chunks
}

// A target may complete an interrupted installation, but it must not absorb data an
// unrelated replica put there: rows of a version outside the manifest would survive
// the replay and look like imported data afterwards.
func TestCheckMigrationTarget_BoundsWhatIsAlreadyThere(t *testing.T) {
	ctx := context.Background()
	if err := (*Follower)(nil).checkMigrationTarget(ctx, storage.MigrationManifest{}); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("nil follower = %v", err)
	}
	if err := (&Follower{}).checkMigrationTarget(ctx, storage.MigrationManifest{}); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("follower without stores = %v", err)
	}

	follower, fixture, _ := migrationTargetFollower(t)
	manifest := storage.MigrationManifest{KBID: "kb", ActiveVersion: 1, Versions: []storage.MigrationVersion{{ID: 1, DataStatus: types.DataStatusDurable}}}
	if err := follower.checkMigrationTarget(ctx, manifest); err != nil {
		t.Fatalf("empty target: %v", err)
	}
	// The version the manifest carries is expected to exist already.
	if err := fixture.versionDoc.Write(ctx, "kb", 1, "doc"); err != nil {
		t.Fatal(err)
	}
	if err := follower.checkMigrationTarget(ctx, manifest); err != nil {
		t.Errorf("version from the manifest = %v, want it accepted", err)
	}
	// A version the manifest does not name is a foreign replica's data.
	if err := fixture.versionDoc.Write(ctx, "kb", 9, "doc"); err != nil {
		t.Fatal(err)
	}
	if err := follower.checkMigrationTarget(ctx, manifest); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("foreign version = %v, want ErrMigrationConflict", err)
	}
}

// The post-install verification re-reads the ordinary stores: the digest, every
// document and every vector has to be there, or the receipt would vouch for data the
// node does not hold.
func TestVerifyMigrationInstall_ReReadsTheStores(t *testing.T) {
	ctx := context.Background()
	if _, err := (*Follower)(nil).verifyMigrationInstall(ctx, storage.MigrationManifest{}, func() error { return nil }); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("nil follower = %v", err)
	}
	if _, err := (&Follower{}).verifyMigrationInstall(ctx, storage.MigrationManifest{}, func() error { return nil }); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("follower without stores = %v", err)
	}

	follower, fixture, chunks := migrationTargetFollower(t)
	manifest := storage.MigrationManifest{
		KBID:          "kb",
		ActiveVersion: 1,
		Versions:      []storage.MigrationVersion{{ID: 1, DataStatus: types.DataStatusDurable, DocIDSetHash: ComputeDocIDSetHash([]string{"doc"})}},
	}

	// Nothing installed yet: the document set does not match the frozen digest.
	if _, err := follower.verifyMigrationInstall(ctx, manifest, func() error { return nil }); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("empty target = %v, want ErrMigrationConflict", err)
	}

	if err := fixture.versionDoc.Write(ctx, "kb", 1, "doc"); err != nil {
		t.Fatal(err)
	}
	// The document is listed but its body never landed.
	if _, err := follower.verifyMigrationInstall(ctx, manifest, func() error { return nil }); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("missing body = %v, want ErrMigrationConflict", err)
	}

	if err := fixture.docStore.Write(ctx, "kb", "doc", 1, []byte("body")); err != nil {
		t.Fatal(err)
	}
	// A chunk relation whose vector is missing fails the install: the vector is part
	// of the data, not a rebuildable artifact.
	if err := fixture.chunkDoc.Write(ctx, "kb", "chunk", "doc"); err != nil {
		t.Fatal(err)
	}
	if _, err := follower.verifyMigrationInstall(ctx, manifest, func() error { return nil }); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("missing vector = %v, want ErrMigrationConflict", err)
	}

	if err := chunks.Write(ctx, "kb", "chunk", []float32{1, 2}); err != nil {
		t.Fatal(err)
	}
	receipt, err := follower.verifyMigrationInstall(ctx, manifest, func() error { return nil })
	if err != nil {
		t.Fatalf("verification: %v", err)
	}
	if receipt.Versions != 1 || receipt.Documents != 1 || receipt.Chunks != 1 {
		t.Errorf("receipt = %+v, want the counts it actually found", receipt)
	}

	// A progress check that fails aborts the verification rather than continuing to
	// vouch for data after its caller stopped caring.
	stop := fmt.Errorf("sweep interrupted")
	if _, err := follower.verifyMigrationInstall(ctx, manifest, func() error { return stop }); !errors.Is(err, stop) {
		t.Errorf("progress failure = %v, want %v", err, stop)
	}
}
