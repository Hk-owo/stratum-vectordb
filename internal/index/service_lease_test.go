package index

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"stratum/internal/storage"
	"stratum/internal/types"
)

func TestServiceLeaseExpiredBuildDoesNotPublishFailedOrReady(t *testing.T) {
	gate := storage.NewServiceLease(1, "s1")
	defer gate.Pause()
	d := storage.NewDirectory()
	if err := d.RegisterNodes(0, []storage.Node{{ID: 1, Addr: "s1"}}); err != nil {
		t.Fatal(err)
	}
	if err := gate.Install(time.Now(), storage.ServiceLeaseDuration, d.Revision, d); err != nil {
		t.Fatal(err)
	}
	im := newIndexManagerWithData(t, WithServiceLease(gate))
	defer im.Close()
	var calls atomic.Int32
	im.RegisterBuildCallback(func(string, int64, types.IndexStatus) error { calls.Add(1); return nil })
	im.listDocIDs = func(ctx context.Context, _ string, _ int64) ([]string, error) {
		gate.Pause()
		return nil, context.Canceled
	}
	key := indexKey{"kb", 1}
	im.mu.Lock()
	im.loading[key] = true
	im.mu.Unlock()
	im.doBuild(buildRequest{kbID: "kb", versionID: 1})
	if calls.Load() != 0 {
		t.Fatal("expired worker published an outcome")
	}
	im.mu.Lock()
	loading := im.loading[key]
	im.mu.Unlock()
	if loading {
		t.Fatal("cancelled worker stranded waiters")
	}
	if err := im.TriggerBuild(context.Background(), "kb", 1); err == nil {
		t.Fatal("paused node scheduled new build")
	}
}

func TestServiceLeaseContextCallbackCancelsInFlightReport(t *testing.T) {
	gate := storage.NewServiceLease(1, "s1")
	defer gate.Pause()
	d := storage.NewDirectory()
	if err := d.RegisterNodes(0, []storage.Node{{ID: 1, Addr: "s1"}}); err != nil {
		t.Fatal(err)
	}
	if err := gate.Install(time.Now(), storage.ServiceLeaseDuration, d.Revision, d); err != nil {
		t.Fatal(err)
	}
	im := newIndexManagerWithData(t, WithServiceLease(gate))
	defer im.Close()
	var calls atomic.Int32
	im.RegisterBuildContextCallback(func(ctx context.Context, _ string, _ int64, _ types.IndexStatus) error {
		calls.Add(1)
		gate.Pause()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			t.Error("report lost authorization cancellation")
			return nil
		}
	})
	ctx, release, err := gate.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	im.mu.Lock()
	callbacks := im.callbacksLocked(ctx)
	im.mu.Unlock()
	im.invokeCallback(callbacks[0], "kb", 1, types.IndexStatusReady, ctx)
	if calls.Load() != 1 {
		t.Fatal("expired report retried", calls.Load())
	}
}
