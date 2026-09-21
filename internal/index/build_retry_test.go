package index

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"stratum/internal/types"
)

// TestIsTransientBuildErr_AChunkVectorThatHasNotLandedIsRetryable pins the whole
// point of chunkVectorReadErr: vecstore says NOT FOUND for a chunk whose vector is
// not stored yet, and that is a state of the DATA, not a verdict about the build.
// Reading it as deterministic retires the version on every replica — see the
// function's comment for what that cost on the 3+3 cluster.
func TestIsTransientBuildErr_AChunkVectorThatHasNotLandedIsRetryable(t *testing.T) {
	stillLanding := chunkVectorReadErr("chunk-a", status.Error(codes.NotFound, "NotFound: "))
	if !isTransientBuildErr(stillLanding) {
		t.Fatalf("a vector that has not landed yet must be retried, but it was read as a deterministic failure: %v", stillLanding)
	}

	// The other direction, so the mapping cannot degenerate into "retry everything":
	// a refusal that will not change still fails immediately.
	rejected := chunkVectorReadErr("chunk-a", status.Error(codes.InvalidArgument, "dimension mismatch"))
	if isTransientBuildErr(rejected) {
		t.Fatalf("a rejected read must not be retried: %v", rejected)
	}
}

// TestIndexManager_BuildWaitsForAChunkVectorThatIsStillLanding is the case the fix
// exists for, end to end through the build path: the version's documents are visible
// before its vectors are (what a replica looks like while a write is being pulled),
// so the first read of one chunk comes back NOT FOUND. The build must wait for the
// data and finish READY — not report FAILED_PERMANENT, which would make the version
// unqueryable on every replica, including the ones that built it fine.
//
// Without the chunkVectorReadErr mapping this reports FAILED_PERMANENT (and the
// t.Fatalf below turns red), because NOT FOUND is not in isTransientBuildErr's list.
func TestIndexManager_BuildWaitsForAChunkVectorThatIsStillLanding(t *testing.T) {
	vc := newMockVectorIndexClient()
	ds := newDocSource()
	ds.addDoc(1, "doc-1", []string{"chunk-a"}, map[string][]float32{"chunk-a": {0.1, 0.2, 0.3}})
	ds.missing["chunk-a"] = 1 // the first read lands ahead of the vector

	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		VecstoreAddr:    "unused", // the client is injected below
	})
	im.vectorIndexClient = vc
	im.listDocIDs = ds.ListDocIDs
	im.listChunkIDsByDocs = ds.ListChunkIDsByDocs
	im.readChunkVector = ds.ReadChunkVector

	// Buffered and non-blocking: invokeCallback may report more than once.
	outcome := make(chan types.IndexStatus, 4)
	im.RegisterBuildCallback(func(_ string, _ int64, st types.IndexStatus) error {
		select {
		case outcome <- st:
		default:
		}
		return nil
	})

	if err := im.TriggerBuild(context.Background(), "kb-1", 1); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}

	// buildRetryInterval is 2 s, so the retry that succeeds lands well inside this.
	select {
	case st := <-outcome:
		if st != types.IndexStatusReady {
			t.Fatalf("build reported %v; a vector that is still landing must be waited for, not retired", st)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the build never reported an outcome")
	}
}
