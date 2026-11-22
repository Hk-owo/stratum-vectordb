package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	serrors "stratum/internal/errors"
	"stratum/internal/fsutil"
)

type fenceRecord struct {
	KBID        string `json:"kb_id"`
	Epoch       uint64 `json:"epoch"`
	OperationID string `json:"operation_id"`
}
type executionState struct {
	active  int
	fencing bool
	changed chan struct{}
	fence   fenceRecord
	durable bool
}

// ExecutionGate covers local DATA mutation lifetimes. A fence drains existing
// entrants, durably rejects that epoch and all older epochs, then acknowledges.
// Memory alone is insufficient: an offline old replica must keep the fence
// after a process restart even if its metadata view is temporarily behind.
type ExecutionGate struct {
	service atomic.Pointer[ServiceLease]
	mu      sync.Mutex
	dir     string
	states  map[string]*executionState
}

func NewExecutionGate(dir string) (*ExecutionGate, error) {
	if dir == "" {
		return nil, fmt.Errorf("storage: persistent fence directory is required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := fsutil.SyncDir(filepath.Dir(dir)); err != nil {
		return nil, err
	}
	g := &ExecutionGate{dir: dir, states: make(map[string]*executionState)}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if !entry.Type().IsRegular() {
			return nil, fmt.Errorf("storage: fence %s is not a regular file", entry.Name())
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		var record fenceRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, fmt.Errorf("storage: corrupt fence %s: %w", entry.Name(), err)
		}
		if err := fsutil.ValidatePathSegment(record.KBID); err != nil || record.Epoch == 0 || record.OperationID == "" || filepath.Base(g.path(record.KBID)) != entry.Name() {
			return nil, fmt.Errorf("storage: invalid fence %s", entry.Name())
		}
		g.states[record.KBID] = &executionState{changed: make(chan struct{}), fence: record, durable: true}
	}
	return g, nil
}

func (g *ExecutionGate) path(kbID string) string {
	hash := sha256.Sum256([]byte(kbID))
	return filepath.Join(g.dir, hex.EncodeToString(hash[:])+".json")
}
func (g *ExecutionGate) state(kbID string) *executionState {
	s := g.states[kbID]
	if s == nil {
		s = &executionState{changed: make(chan struct{})}
		g.states[kbID] = s
	}
	return s
}
func signalState(s *executionState) { close(s.changed); s.changed = make(chan struct{}) }

type executionLease struct {
	gate   *ExecutionGate
	kbID   string
	epoch  uint64
	active atomic.Bool
}
type executionLeaseKey struct{}

func (g *ExecutionGate) SetServiceLease(lease *ServiceLease) { g.service.Store(lease) }

func (g *ExecutionGate) Enter(ctx context.Context, kbID string, epoch uint64) (context.Context, func(), error) {
	if g == nil {
		return ctx, func() {}, nil
	}
	c, leave, err := g.service.Load().Enter(ctx)
	if err != nil {
		return ctx, func() {}, err
	}
	c, release, err := g.enter(c, kbID, epoch)
	if err != nil {
		leave()
		return ctx, func() {}, err
	}
	return c, func() { release(); leave() }, nil
}

// EnterImport admits only the task which owns this node's durable fence. It
// holds the KB exclusively, including against newer-epoch writes after cancel.
// The caller must re-read the committed task after admission: a directory read
// before waiting for the gate cannot authorize work after a concurrent cancel.
func (g *ExecutionGate) EnterImport(ctx context.Context, kbID string, epoch uint64, operationID string) (context.Context, func(), error) {
	if g == nil || kbID == "" || epoch == 0 || operationID == "" {
		return ctx, func() {}, serrors.ErrPlacementChangeUnsafe
	}
	c, leave, err := g.service.Load().Enter(ctx)
	if err != nil {
		return ctx, func() {}, err
	}
	for {
		if err := c.Err(); err != nil {
			leave()
			return ctx, func() {}, err
		}
		g.mu.Lock()
		s := g.state(kbID)
		if !s.durable || s.fence.Epoch != epoch || s.fence.OperationID != operationID {
			g.mu.Unlock()
			leave()
			return ctx, func() {}, serrors.ErrMigrationConflict
		}
		if s.fencing || s.active != 0 {
			changed := s.changed
			g.mu.Unlock()
			select {
			case <-c.Done():
				leave()
				return ctx, func() {}, c.Err()
			case <-changed:
				continue
			}
		}
		s.fencing = true
		g.mu.Unlock()
		var once sync.Once
		return c, func() {
			once.Do(func() {
				g.mu.Lock()
				s.fencing = false
				signalState(s)
				g.mu.Unlock()
				leave()
			})
		}, nil
	}
}

func (g *ExecutionGate) enter(ctx context.Context, kbID string, epoch uint64) (context.Context, func(), error) {
	return g.enterWith(ctx, kbID, epoch, false)
}

// enterForCleanup admits the durable fence's OWN epoch.
//
// The ordinary rule ("fence.Epoch must be BELOW the epoch being entered") is what
// keeps a fenced replica from accepting writes, and for cleanup it is exactly
// backwards: the delete flow writes the fence under the deletion epoch and then
// asks the replicas to reclaim, so the reclaim arrives with
// epoch == fence.Epoch. Applying the ordinary rule refused every reclaim with
// kb_write_paused, the cleanup broadcast never finished, and the knowledge base
// stayed in DELETE_FAILED — measured on the five-storage-node acceptance cluster.
//
// Older epochs are still refused, so a cleanup planned before a later fence can
// never cross it: the floor keeps its meaning.
func (g *ExecutionGate) enterForCleanup(ctx context.Context, kbID string, epoch uint64) (context.Context, func(), error) {
	return g.enterWith(ctx, kbID, epoch, true)
}

// enterWith is the shared body of enter and enterForCleanup. allowEqual decides
// whether the fence's own epoch may be entered.
func (g *ExecutionGate) enterWith(ctx context.Context, kbID string, epoch uint64, allowEqual bool) (context.Context, func(), error) {
	noop := func() {}
	if g == nil {
		return ctx, noop, nil
	}
	if kbID == "" || epoch == 0 {
		return ctx, noop, serrors.ErrPlacementUnknown
	}
	if lease, ok := ctx.Value(executionLeaseKey{}).(*executionLease); ok && lease.gate == g && lease.kbID == kbID && lease.active.Load() {
		if lease.epoch != epoch {
			return ctx, noop, serrors.ErrPlacementStale
		}
		return ctx, noop, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return ctx, noop, err
		}
		g.mu.Lock()
		s := g.state(kbID)
		if s.fence.Epoch > epoch || (s.fence.Epoch == epoch && !allowEqual) {
			g.mu.Unlock()
			return ctx, noop, serrors.ErrKBWritePaused
		}
		if s.fencing {
			changed := s.changed
			g.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx, noop, ctx.Err()
			case <-changed:
				continue
			}
		}
		s.active++
		lease := &executionLease{gate: g, kbID: kbID, epoch: epoch}
		lease.active.Store(true)
		g.mu.Unlock()
		var once sync.Once
		release := func() {
			once.Do(func() { lease.active.Store(false); g.mu.Lock(); s.active--; signalState(s); g.mu.Unlock() })
		}
		return context.WithValue(ctx, executionLeaseKey{}, lease), release, nil
	}
}

func (g *ExecutionGate) Fence(ctx context.Context, kbID string, epoch uint64, operationID string) error {
	if g == nil {
		return serrors.ErrPlacementChangeUnsafe
	}
	if kbID == "" || epoch == 0 || operationID == "" {
		return serrors.ErrInvalidArgument
	}
	if err := fsutil.ValidatePathSegment(kbID); err != nil {
		return fmt.Errorf("%w: %v", serrors.ErrInvalidArgument, err)
	}
	if lease, ok := ctx.Value(executionLeaseKey{}).(*executionLease); ok && lease.gate == g && lease.kbID == kbID && lease.active.Load() {
		return serrors.ErrMigrationConflict
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		g.mu.Lock()
		s := g.state(kbID)
		if s.fence.Epoch > epoch {
			g.mu.Unlock()
			return serrors.ErrPlacementStale
		}
		if s.fence.Epoch == epoch {
			if s.fence.OperationID != operationID {
				g.mu.Unlock()
				return serrors.ErrMigrationConflict
			}
			if s.durable {
				g.mu.Unlock()
				return nil
			}
		}
		if s.fencing {
			changed := s.changed
			g.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
				continue
			}
		}
		s.fencing = true
		for s.active > 0 {
			changed := s.changed
			g.mu.Unlock()
			select {
			case <-ctx.Done():
				g.mu.Lock()
				s.fencing = false
				signalState(s)
				g.mu.Unlock()
				return ctx.Err()
			case <-changed:
				g.mu.Lock()
			}
		}
		// Keep the conservative in-memory fence even if fsync fails. A retry
		// must repeat persistence, never turn that failed attempt into an ACK.
		s.fence = fenceRecord{KBID: kbID, Epoch: epoch, OperationID: operationID}
		s.durable = false
		err := g.persist(s.fence)
		s.durable = err == nil
		s.fencing = false
		signalState(s)
		g.mu.Unlock()
		return err
	}
}

func (g *ExecutionGate) persist(record fenceRecord) error {
	if err := fsutil.ValidatePathSegment(record.KBID); err != nil {
		return fmt.Errorf("%w: %v", serrors.ErrInvalidArgument, err)
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return fsutil.AtomicWriteFile(g.path(record.KBID), append(data, '\n'), fsutil.WithTempPattern(".fence-*"))
}

// EnterMutation checks the bound operation configuration as well as the durable
// local floor. Existing operations may finish during draining; fencing refuses
// new mutations, while queries can keep using data already held locally.
func EnterMutation(ctx context.Context, g *ExecutionGate, s Snapshot) (context.Context, func(), error) {
	if s.Migration != nil && s.Migration.BlocksDataWrites() {
		return ctx, func() {}, serrors.ErrKBWritePaused
	}
	return g.Enter(ctx, s.Placement.KBID, s.Placement.Epoch)
}

func EnterCleanup(ctx context.Context, g *ExecutionGate, reader Reader, nodeID int64, kbID string) (context.Context, func(), error) {
	if g == nil {
		return ctx, func() {}, nil
	}
	if reader == nil {
		return ctx, func() {}, serrors.ErrPlacementUnknown
	}
	dir, err := reader.StorageDirectory(ctx)
	if err != nil {
		return ctx, func() {}, err
	}
	if tomb, ok := dir.Tombstones[kbID]; ok {
		found := false
		for _, id := range tomb.CleanupIDs {
			if id == nodeID {
				found = true
			}
		}
		if !found {
			return ctx, func() {}, serrors.ErrNotKBReplica
		}
		// The tombstone's epoch is the deletion's own fence epoch, so the reclaim
		// has to be admitted AT that epoch rather than above it (see enterForCleanup).
		return g.enterForCleanup(ctx, kbID, tomb.Epoch)
	}
	s, err := dir.Resolve(kbID)
	if err != nil {
		return ctx, func() {}, err
	}
	if !s.Placement.Contains(nodeID) {
		return ctx, func() {}, serrors.ErrNotKBReplica
	}
	if err := CheckRequestEpoch(ctx, kbID, s.Placement.Epoch); err != nil {
		return ctx, func() {}, err
	}
	return EnterMutation(ctx, g, s)
}

// Maintenance never reopens ordinary service on a revoked node. The scoped
// context only lets the authorized cleanup delete local layers while holding
// this KB exclusively; it cannot be reused once release has run.
type maintenanceScopeKey struct{}
type maintenanceScope struct {
	service *ServiceLease
	active  atomic.Bool
}

func (g *ExecutionGate) EnterMaintenance(ctx context.Context, c Cleanup) (context.Context, func(), error) {
	if g == nil || c.KBID == "" || c.Epoch == 0 || c.ID == "" {
		return ctx, func() {}, serrors.ErrPlacementChangeUnsafe
	}
	if err := fsutil.ValidatePathSegment(c.KBID); err != nil {
		return ctx, func() {}, fmt.Errorf("%w: %v", serrors.ErrInvalidArgument, err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return ctx, func() {}, err
		}
		g.mu.Lock()
		s := g.state(c.KBID)
		if !c.ArtifactsOnly && s.fence.Epoch > c.Epoch {
			g.mu.Unlock()
			return ctx, func() {}, serrors.ErrPlacementStale
		}
		if s.fencing || s.active > 0 {
			ch := s.changed
			g.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx, func() {}, ctx.Err()
			case <-ch:
				continue
			}
		}
		s.fencing = true
		var err error
		if !c.ArtifactsOnly {
			s.fence = fenceRecord{KBID: c.KBID, Epoch: c.Epoch, OperationID: c.ID}
			s.durable = false
			err = g.persist(s.fence)
			s.durable = err == nil
		}
		if err != nil {
			s.fencing = false
			signalState(s)
			g.mu.Unlock()
			return ctx, func() {}, err
		}
		scope := &maintenanceScope{service: g.service.Load()}
		scope.active.Store(true)
		g.mu.Unlock()
		var once sync.Once
		return context.WithValue(ctx, maintenanceScopeKey{}, scope), func() {
			once.Do(func() { scope.active.Store(false); g.mu.Lock(); s.fencing = false; signalState(s); g.mu.Unlock() })
		}, nil
	}
}
