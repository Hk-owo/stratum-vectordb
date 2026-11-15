package storage

import (
	"context"
	"sync"
	"time"

	serrors "stratum/internal/errors"
)

// These fixed protocol bounds must be upgraded together on all components.
// Timers must progress within 25% of real time while processes are running.
// Twice the grant duration covers that drift; clock jumps never extend a
// receiver's grant, which is measured from before its RPC using elapsed time.
const ServiceLeaseDuration = 15 * time.Second
const ServiceRevocationWait = 2 * ServiceLeaseDuration

type ServiceBlock struct {
	Revoked     bool   `json:"revoked,omitempty"`
	OperationID string `json:"operation_id"`
	Revision    uint64 `json:"revision"`
}

type ServiceRequest struct {
	NodeID           int64  `json:"node_id"`
	Address          string `json:"address,omitempty"`
	OperationID      string `json:"operation_id,omitempty"`
	ExpectedRevision uint64 `json:"expected_revision,omitempty"`
	BlockRevision    uint64 `json:"block_revision,omitempty"`
}

// BlockService rejects future grants after this mutation commits. Repeated
// requests retain the original generation; a different operation cannot replace
// a block. Removing and reinstalling the same operation creates a NEW generation.
func (d *Directory) BlockService(req ServiceRequest) error {
	if req.NodeID <= 0 || req.OperationID == "" {
		return serrors.ErrInvalidArgument
	}
	if _, ok := d.Nodes[req.NodeID]; !ok {
		return serrors.ErrInvalidArgument
	}
	if old, ok := d.ServiceBlocks[req.NodeID]; ok {
		if old.OperationID != req.OperationID || (req.BlockRevision != 0 && old.Revision != req.BlockRevision) {
			return serrors.ErrMigrationConflict
		}
		return nil
	}
	if req.BlockRevision != 0 {
		return serrors.ErrPlacementStale
	}
	if req.ExpectedRevision != d.Revision {
		return serrors.ErrPlacementStale
	}
	d.Revision++
	if d.ServiceBlocks == nil {
		d.ServiceBlocks = map[int64]ServiceBlock{}
	}
	d.ServiceBlocks[req.NodeID] = ServiceBlock{OperationID: req.OperationID, Revision: d.Revision}
	return nil
}

// ConfirmServiceRevoked is committed only after a leader has waited the full
// revocation bound. The proof remains valid across terms while the EXACT
// service block generation stays installed: no later grant can pass it.
func (d *Directory) ConfirmServiceRevoked(req ServiceRequest) error {
	b, ok := d.ServiceBlocks[req.NodeID]
	if !ok || req.OperationID != b.OperationID || req.BlockRevision == 0 || req.BlockRevision != b.Revision {
		return serrors.ErrPlacementStale
	}
	if b.Revoked {
		return nil
	}
	b.Revoked = true
	d.ServiceBlocks[req.NodeID] = b
	for op, m := range d.Migrations {
		if m.Active() && m.Request.Fault && m.Request.FromID == req.NodeID && m.FailedBlock.OperationID == b.OperationID && m.FailedBlock.Revision == b.Revision {
			m.FailedBlock = b
			d.Migrations[op] = m
		}
	}
	d.Revision++
	return nil
}

func (d *Directory) ResumeService(req ServiceRequest) error {
	block, ok := d.ServiceBlocks[req.NodeID]
	if !ok {
		return serrors.ErrMigrationConflict
	}
	if req.OperationID != block.OperationID || req.BlockRevision != block.Revision {
		return serrors.ErrPlacementStale
	}
	if req.ExpectedRevision != d.Revision {
		return serrors.ErrPlacementStale
	}
	for _, m := range d.Migrations {
		if m.Active() {
			for _, id := range m.FenceIDs() {
				if id == req.NodeID {
					return serrors.ErrMigrationInProgress
				}
			}
		}
	}
	delete(d.ServiceBlocks, req.NodeID)
	d.Revision++
	return nil
}

func (d Directory) AuthorizeService(req ServiceRequest) error {
	n, ok := d.Nodes[req.NodeID]
	if !ok || n.Addr != req.Address || (n.State != NodeActive && n.State != NodeDraining) {
		return serrors.ErrStorageServicePaused
	}
	if _, blocked := d.ServiceBlocks[req.NodeID]; blocked {
		return serrors.ErrStorageServicePaused
	}
	return nil
}

// ServiceLease is volatile: restart always begins closed. Successful recovery
// installs a directory observed at or after the committed grant, before opening
// admission. Renewal never restores an expired lease without another reconcile.
type serviceGeneration struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type ServiceLease struct {
	generation *serviceGeneration
	timer      *time.Timer
	schedule   uint64
	mu         sync.Mutex
	now        func() time.Time
	nodeID     int64
	addr       string
	revision   uint64
	start      time.Time
	duration   time.Duration
	ready      bool
}

func NewServiceLease(nodeID int64, addr string) *ServiceLease {
	return &ServiceLease{now: time.Now, nodeID: nodeID, addr: addr}
}
func leaseAlive(now, start time.Time, d time.Duration) bool {
	elapsed := now.Sub(start)
	wall := now.Round(0).Sub(start.Round(0))
	return d > 0 && !start.IsZero() && elapsed >= 0 && elapsed < d && wall >= 0 && wall < d
}
func (g *ServiceLease) NeedsReconcile(revision uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.ready || !leaseAlive(g.now(), g.start, g.duration) || g.revision != revision
}
func (g *ServiceLease) pauseLocked() {
	g.ready = false
	if g.generation != nil {
		g.generation.cancel()
	}
	if g.timer != nil {
		g.timer.Stop()
	}
	g.schedule++
}
func (g *ServiceLease) Pause() {
	if g != nil {
		g.mu.Lock()
		g.pauseLocked()
		g.mu.Unlock()
	}
}
func (g *ServiceLease) armLocked() {
	if g.timer != nil {
		g.timer.Stop()
	}
	g.schedule++
	seq := g.schedule
	remaining := g.duration - g.now().Sub(g.start)
	g.timer = time.AfterFunc(remaining, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.schedule != seq {
			return
		}
		if !leaseAlive(g.now(), g.start, g.duration) {
			g.pauseLocked()
		} else {
			g.armLocked()
		}
	})
}
func (g *ServiceLease) Check() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.ready || !leaseAlive(g.now(), g.start, g.duration) {
		g.pauseLocked()
		return serrors.ErrStorageServicePaused
	}
	return nil
}

func (g *ServiceLease) Install(start time.Time, d time.Duration, grantRevision uint64, dir Directory) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if d <= 0 || d > ServiceLeaseDuration || !leaseAlive(g.now(), start, d) || dir.Revision < grantRevision || dir.Revision < g.revision {
		return serrors.ErrStorageServicePaused
	}
	if err := dir.AuthorizeService(ServiceRequest{NodeID: g.nodeID, Address: g.addr}); err != nil {
		return err
	}
	// Validate every assigned KB before allowing its local service to resume.
	for kb, p := range dir.Placements {
		if p.Contains(g.nodeID) {
			if _, err := dir.Resolve(kb); err != nil {
				return err
			}
		}
	}
	if g.ready && start.Before(g.start) {
		return serrors.ErrPlacementStale
	}
	if g.generation != nil {
		g.generation.cancel()
	}
	c, cancel := context.WithCancel(context.Background())
	g.generation = &serviceGeneration{ctx: c, cancel: cancel}
	g.start, g.duration, g.revision, g.ready = start, d, dir.Revision, true
	g.armLocked()
	return nil
}
func (g *ServiceLease) Renew(start time.Time, d time.Duration, revision uint64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	if !g.ready || !leaseAlive(now, g.start, g.duration) || g.revision != revision || start.Before(g.start) || d <= 0 || d > ServiceLeaseDuration || !leaseAlive(now, start, d) {
		return serrors.ErrStorageServicePaused
	}
	g.start, g.duration = start, d
	g.armLocked()
	return nil
}

type serviceScope struct {
	gate       *ServiceLease
	generation *serviceGeneration
	active     bool
	mu         sync.Mutex
}
type serviceScopeKey struct{}

// Enter binds cancellation to one uninterrupted service generation. Healthy
// renewals extend that generation, allowing long transfers to finish. Expiry,
// pause or reconciliation cancels it; new grants cannot revive old operations.
func (g *ServiceLease) Enter(ctx context.Context) (context.Context, func(), error) {
	if scope, ok := ctx.Value(maintenanceScopeKey{}).(*maintenanceScope); ok && scope.service == g && scope.active.Load() {
		return ctx, func() {}, ctx.Err()
	}
	noop := func() {}
	if g == nil {
		return ctx, noop, nil
	}
	if err := ctx.Err(); err != nil {
		return ctx, noop, err
	}
	if err := g.Check(); err != nil {
		return ctx, noop, err
	}
	if scope, ok := ctx.Value(serviceScopeKey{}).(*serviceScope); ok && scope.gate == g {
		scope.mu.Lock()
		active := scope.active && scope.generation.ctx.Err() == nil
		scope.mu.Unlock()
		if active {
			return ctx, noop, nil
		}
		return ctx, noop, serrors.ErrStorageServicePaused
	}
	g.mu.Lock()
	if !g.ready || !leaseAlive(g.now(), g.start, g.duration) {
		g.pauseLocked()
		g.mu.Unlock()
		return ctx, noop, serrors.ErrStorageServicePaused
	}
	generation := g.generation
	g.mu.Unlock()
	c, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(generation.ctx, cancel)
	scope := &serviceScope{gate: g, generation: generation, active: true}
	var once sync.Once
	release := func() { once.Do(func() { scope.mu.Lock(); scope.active = false; scope.mu.Unlock(); stop(); cancel() }) }
	return context.WithValue(c, serviceScopeKey{}, scope), release, nil
}

// CheckContext also rejects an old operation after a later reconcile succeeds.
func (g *ServiceLease) CheckContext(ctx context.Context) error {
	if scope, ok := ctx.Value(maintenanceScopeKey{}).(*maintenanceScope); ok && scope.service == g && scope.active.Load() {
		return ctx.Err()
	}
	if g == nil {
		return nil
	}
	if err := g.Check(); err != nil {
		return err
	}
	if scope, ok := ctx.Value(serviceScopeKey{}).(*serviceScope); ok && scope.gate == g {
		scope.mu.Lock()
		valid := scope.active && scope.generation.ctx.Err() == nil
		scope.mu.Unlock()
		if !valid {
			return serrors.ErrStorageServicePaused
		}
	}
	return ctx.Err()
}

// ForNode returns only this node's placements and cleanup obligations. Nodes
// needed to resolve those placements are included; unrelated KBs are omitted.
func (d Directory) ForNode(nodeID int64) Directory {
	out := NewDirectory()
	out.Revision = d.Revision
	if n, ok := d.Nodes[nodeID]; ok {
		out.Nodes[nodeID] = n
	}
	if b, ok := d.ServiceBlocks[nodeID]; ok {
		out.ServiceBlocks = map[int64]ServiceBlock{nodeID: b}
	}
	out.ActiveVersions = map[string]int64{}
	for _, c := range d.Cleanups {
		if c.NodeID == nodeID {
			out.addCleanup(c)
		}
	}
	include := func(ids []int64) {
		for _, id := range ids {
			if n, ok := d.Nodes[id]; ok {
				out.Nodes[id] = n
			}
		}
	}
	for kb, p := range d.Placements {
		if p.Contains(nodeID) {
			out.Placements[kb] = p.Clone()
			out.ActiveVersions[kb] = d.ActiveVersions[kb]
			include(p.ReplicaIDs)
		}
	}
	for kb, t := range d.Tombstones {
		for _, id := range t.CleanupIDs {
			if id == nodeID {
				t.CleanupIDs = append([]int64(nil), t.CleanupIDs...)
				out.Tombstones[kb] = t
				include(t.CleanupIDs)
				break
			}
		}
	}
	for op, m := range d.Migrations {
		if !m.Active() {
			continue
		}
		for _, id := range m.FenceIDs() {
			if id == nodeID {
				if out.Migrations == nil {
					out.Migrations = map[string]Migration{}
				}
				out.Migrations[op] = m.Clone()
				include(m.FenceIDs())
				break
			}
		}
	}
	return out
}

type NodeDirectoryReader interface {
	GetNodeStorageDirectory(context.Context, int64) (Directory, error)
}

// Fork gives queued background work an independent lifetime in the SAME service
// generation. Finishing the caller does not cancel it; expiry/reconcile still
// does. A newer generation cannot be substituted during the handoff.
func (g *ServiceLease) Fork(ctx context.Context) (context.Context, func(), error) {
	if g == nil {
		return context.WithoutCancel(ctx), func() {}, nil
	}
	if err := g.CheckContext(ctx); err != nil {
		return ctx, func() {}, err
	}
	g.mu.Lock()
	if !g.ready || !leaseAlive(g.now(), g.start, g.duration) {
		g.pauseLocked()
		g.mu.Unlock()
		return ctx, func() {}, serrors.ErrStorageServicePaused
	}
	generation := g.generation
	if source, ok := ctx.Value(serviceScopeKey{}).(*serviceScope); ok && source.gate == g && source.generation != generation {
		g.mu.Unlock()
		return ctx, func() {}, serrors.ErrStorageServicePaused
	}
	g.mu.Unlock()
	c, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(generation.ctx, cancel)
	scope := &serviceScope{gate: g, generation: generation, active: true}
	var once sync.Once
	release := func() { once.Do(func() { scope.mu.Lock(); scope.active = false; scope.mu.Unlock(); stop(); cancel() }) }
	return context.WithValue(c, serviceScopeKey{}, scope), release, nil
}

// ServiceManager is the persisted control boundary. Waiting for revocation is
// leader-local and intentionally not part of this remote management interface.
type ServiceManager interface {
	Reader
	ProposeBlockStorageService(context.Context, ServiceRequest) error
	ProposeResumeStorageService(context.Context, ServiceRequest) error
}
