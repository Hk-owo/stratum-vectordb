package storage

import (
	"context"
	"errors"
	serrors "stratum/internal/errors"
	"sync/atomic"
	"testing"
	"time"
)

func leasedNode(t *testing.T) (*ServiceLease, Directory, func(time.Duration), time.Time) {
	t.Helper()
	start := time.Now()
	var elapsed atomic.Int64
	g := NewServiceLease(1, "s1")
	g.now = func() time.Time { return start.Add(time.Duration(elapsed.Load())) }
	t.Cleanup(g.Pause)
	d := fullDirectory(t)
	return g, d, func(d time.Duration) { elapsed.Store(int64(d)) }, start
}
func TestServiceLeaseStartsClosedAndRejectsLateOrUnreconciledRenewal(t *testing.T) {
	g, d, advance, start := leasedNode(t)
	if _, _, err := g.Enter(context.Background()); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Fatal(err)
	}
	if err := g.Renew(start, ServiceLeaseDuration, d.Revision); err == nil {
		t.Fatal("renew bypassed startup reconcile")
	}
	advance(ServiceLeaseDuration)
	if err := g.Install(start, ServiceLeaseDuration, d.Revision, d); err == nil {
		t.Fatal("late response minted a fresh lifetime")
	}
	advance(0)
	if err := g.Install(start, ServiceLeaseDuration, d.Revision, d); err != nil {
		t.Fatal(err)
	}
	advance(ServiceLeaseDuration)
	if err := g.Renew(start.Add(ServiceLeaseDuration), ServiceLeaseDuration, d.Revision); err == nil {
		t.Fatal("expired admission reopened without reconcile")
	}
	if !g.NeedsReconcile(d.Revision) {
		t.Fatal("expiry lost")
	}
}
func TestServiceLeaseHealthyRenewalKeepsLongOperationButCannotReviveExpiredGeneration(t *testing.T) {
	g, d, advance, start := leasedNode(t)
	if err := g.Install(start, ServiceLeaseDuration, d.Revision, d); err != nil {
		t.Fatal(err)
	}
	ctx, release, err := g.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	advance(10 * time.Second)
	if err := g.Renew(start.Add(10*time.Second), ServiceLeaseDuration, d.Revision); err != nil {
		t.Fatal(err)
	}
	advance(20 * time.Second)
	if err := g.CheckContext(ctx); err != nil {
		t.Fatal("healthy long transfer expired:", err)
	}
	advance(26 * time.Second)
	if err := g.Check(); err == nil {
		t.Fatal("expired grant admitted work")
	}
	if err := g.Install(start.Add(26*time.Second), ServiceLeaseDuration, d.Revision, d); err != nil {
		t.Fatal(err)
	}
	if err := g.CheckContext(ctx); err == nil {
		t.Fatal("new grant revived old operation")
	}
	if _, _, err := g.Enter(ctx); err == nil {
		t.Fatal("nested entry revived old operation")
	}
	fresh, done, err := g.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	done()
	if err := g.CheckContext(fresh); err == nil {
		t.Fatal("released scope reused")
	}
}
func TestServiceLeaseReconcileChecksDirectoryAndNodeIdentity(t *testing.T) {
	g, d, _, start := leasedNode(t)
	if err := g.Install(start, ServiceLeaseDuration, d.Revision+1, d); err == nil {
		t.Fatal("stale directory accepted")
	}
	d.Nodes[1] = Node{ID: 1, Addr: "replacement", State: NodeActive}
	if err := g.Install(start, ServiceLeaseDuration, d.Revision, d); err == nil {
		t.Fatal("wrong process endpoint accepted")
	}
	d.Nodes[1] = Node{ID: 1, Addr: "s1", State: NodeActive}
	if err := d.BlockService(ServiceRequest{NodeID: 1, OperationID: "fault", ExpectedRevision: d.Revision}); err != nil {
		t.Fatal(err)
	}
	if err := g.Install(start, ServiceLeaseDuration, d.Revision, d); err == nil {
		t.Fatal("blocked service resumed")
	}
}
func TestServiceLeaseWallJumpCannotExtendAuthorization(t *testing.T) {
	g, d, _, start := leasedNode(t)
	if err := g.Install(start, ServiceLeaseDuration, d.Revision, d); err != nil {
		t.Fatal(err)
	}
	// Loss of the monotonic component models resume after host suspension.
	g.mu.Lock()
	g.now = func() time.Time { return start.Round(0).Add(time.Hour) }
	g.mu.Unlock()
	if err := g.Check(); err == nil {
		t.Fatal("wall elapsed time ignored")
	}
}
func TestServiceBlocksPersistGenerationAndDoNotChangeReplicaPolicy(t *testing.T) {
	d := fullDirectory(t)
	if err := d.AssignNew(d.InitialPlacement("kb", "create")); err != nil {
		t.Fatal(err)
	}
	p, _ := d.Lookup("kb")
	req := ServiceRequest{NodeID: 1, OperationID: "fault", ExpectedRevision: d.Revision}
	if err := d.BlockService(req); err != nil {
		t.Fatal(err)
	}
	block := d.ServiceBlocks[1]
	if err := d.BlockService(req); err != nil || d.ServiceBlocks[1] != block {
		t.Fatal("retry reset revocation generation", err)
	}
	if err := d.AuthorizeService(ServiceRequest{NodeID: 1, Address: "s1"}); !errors.Is(err, serrors.ErrStorageServicePaused) {
		t.Fatal(err)
	}
	if after, _ := d.Lookup("kb"); !equalPlacement(after, p) {
		t.Fatal("block changed quorum")
	}
	if d.InitialPlacement("new", "create/new").Contains(1) {
		t.Fatal("blocked node received new KB")
	}
	cloned := d.Clone()
	delete(cloned.ServiceBlocks, 1)
	if len(d.ServiceBlocks) != 1 {
		t.Fatal("snapshot aliases block")
	}
	resume := ServiceRequest{NodeID: 1, OperationID: "fault", BlockRevision: block.Revision, ExpectedRevision: d.Revision}
	if err := d.ResumeService(resume); err != nil {
		t.Fatal(err)
	}
	req.ExpectedRevision = d.Revision
	if err := d.BlockService(req); err != nil {
		t.Fatal(err)
	}
	req.BlockRevision = block.Revision
	if err := d.BlockService(req); err == nil {
		t.Fatal("old wait proof survived unblock/reblock")
	}
}
func TestServiceNodeDirectoryOmitsUnrelatedKBsAndIncludesCleanup(t *testing.T) {
	d := fullDirectory(t)
	for kb, ids := range map[string][]int64{"mine": {1, 2}, "other": {2, 3}} {
		if err := d.AssignNew(Placement{KBID: kb, Epoch: 1, ReplicaIDs: ids, RequiredReplicas: 2, OperationID: kb}); err != nil {
			t.Fatal(err)
		}
	}
	d.Delete("mine")
	out := d.ForNode(1)
	if len(out.Placements) != 0 || len(out.Tombstones) != 1 || len(out.Nodes) != 2 {
		t.Fatal("wrong node projection:", out)
	}
}

func TestServiceLeaseForkOutlivesCallerButNotServiceGeneration(t *testing.T) {
	g, d, _, start := leasedNode(t)
	if err := g.Install(start, ServiceLeaseDuration, d.Revision, d); err != nil {
		t.Fatal(err)
	}
	ctx, done, err := g.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	child, finish, err := g.Fork(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	done()
	if err := g.CheckContext(child); err != nil {
		t.Fatal("caller completion cancelled background work", err)
	}
	g.Pause()
	if err := g.CheckContext(child); err == nil {
		t.Fatal("background callback escaped expired generation")
	}
}
func TestServiceLeaseTimerCancelsIdleOperation(t *testing.T) {
	g := NewServiceLease(1, "s1")
	defer g.Pause()
	d := fullDirectory(t)
	if err := g.Install(time.Now(), 20*time.Millisecond, d.Revision, d); err != nil {
		t.Fatal(err)
	}
	ctx, release, err := g.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("idle operation survived grant expiry")
	}
}
