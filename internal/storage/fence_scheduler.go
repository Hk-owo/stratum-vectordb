package storage

import (
	"context"
	"errors"
	"slices"
	"time"

	"go.uber.org/zap"
	serrors "stratum/internal/errors"
	"stratum/internal/types"
)

// FenceScheduler resumes persisted preparation tasks on the current leader.
// A missing node never changes effective membership. Fault-mode ACK exclusion
// requires a committed service-revocation proof; ordinary migrations still
// require every node. Fenced tasks await verified data transfer/cutover.
type FenceScheduler struct {
	Manager        MigrationManager
	IsLeader       func() bool
	Versions       func(context.Context, string) ([]types.VersionMeta, error)
	Fence          func(context.Context, Node, Migration) error
	Revoke         func(context.Context, int64, ServiceBlock) error
	Stage          func(context.Context, Node, Node, Migration) (MigrationStageReceipt, error)
	Install        func(context.Context, Node, Migration) (MigrationInstallReceipt, error)
	Activate       func(context.Context, Node, Migration) error
	Cleanup        func(context.Context, Node, Cleanup) error
	Serve          func(context.Context, Node, Migration) (MigrationServeReceipt, error)
	StageTimeout   time.Duration
	Interval       time.Duration
	AttemptTimeout time.Duration
	Logger         *zap.Logger
}

func (s *FenceScheduler) Step(ctx context.Context) error {
	if s.Manager == nil || s.IsLeader == nil || !s.IsLeader() {
		return nil
	}
	var tasks []Migration
	err := s.attempt(ctx, func(c context.Context) error { var e error; tasks, e = s.Manager.ListKBMigrations(c); return e })
	if err != nil {
		return err
	}
	var failures []error
	if s.Cleanup != nil {
		d, e := s.Manager.StorageDirectory(ctx)
		if e != nil {
			return e
		}
		for _, c := range d.Cleanups {
			if c.Done {
				continue
			}
			n, ok := d.Nodes[c.NodeID]
			if !ok {
				continue
			}
			if !s.IsLeader() {
				return serrors.ErrNoLeaderYet
			}
			if err := s.attempt(ctx, func(call context.Context) error { return s.Cleanup(call, n, c) }); err != nil {
				failures = append(failures, err)
			}
		}
	}

	for _, task := range tasks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !s.IsLeader() {
			break
		}
		if task.Phase == MigrationInstalled && task.Serve != nil && s.Activate != nil {
			_, err := runTask(s, ctx, func(c context.Context) (bool, error) {
				d, e := s.Manager.StorageDirectory(c)
				if e != nil {
					return false, e
				}
				n, ok := d.Nodes[task.Request.ToID]
				if !ok {
					return false, serrors.ErrPlacementUnknown
				}
				return true, s.Activate(c, n, task)
			})
			if err != nil {
				failures = append(failures, err)
			}
			continue
		}
		if task.Phase == MigrationFenced && s.Stage != nil {
			if err := s.stageTask(ctx, task); err != nil {
				failures = append(failures, err)
			}
			continue
		}
		if task.Phase == MigrationStaged && s.Install != nil {
			if err := s.installTask(ctx, task); err != nil {
				failures = append(failures, err)
			}
			continue
		}
		if task.Phase == MigrationInstalled && task.Serve == nil && s.Serve != nil {
			if err := s.serveTask(ctx, task); err != nil {
				failures = append(failures, err)
			}
			continue
		}
		if task.Phase == MigrationDraining && task.Request.Fault && !task.FailedBlock.Revoked {
			// A preceding KB may have committed the shared node proof during
			// this same pass. Never repeat a full wait from a stale task list.
			var latest Directory
			err := s.attempt(ctx, func(c context.Context) error { var e error; latest, e = s.Manager.StorageDirectory(c); return e })
			if err != nil {
				failures = append(failures, err)
				continue
			}
			current, exists := latest.Migrations[task.Request.OperationID]
			if !exists || current.Phase != MigrationDraining || current.Request != task.Request {
				continue
			}
			if err := latest.ValidateFailedBlock(current); err != nil {
				if !errors.Is(err, serrors.ErrMigrationBusy) {
					failures = append(failures, err)
					continue
				}
				if s.Revoke == nil {
					continue
				}
				if !s.IsLeader() {
					break
				}
				// The ordinary RPC budget must not abort the protocol wait.
				c, cancel := context.WithTimeout(ctx, ServiceRevocationWait+5*time.Second)
				err = s.Revoke(c, current.Request.FromID, current.FailedBlock)
				cancel()
				if err != nil {
					failures = append(failures, err)
				}
				continue
			}
			task = current
		}
		if task.Phase == MigrationDraining {
			if s.Versions == nil {
				continue
			}
			var versions []types.VersionMeta
			err := s.attempt(ctx, func(c context.Context) error { var e error; versions, e = s.Versions(c, task.Request.KBID); return e })
			if err != nil {
				failures = append(failures, err)
				continue
			}
			busy := false
			for _, v := range versions {
				if v.Deleting || v.DataStatus == types.DataStatusPending {
					busy = true
					break
				}
			}
			if busy {
				continue
			}
			err = s.attempt(ctx, func(c context.Context) error { return s.Manager.ProposeFenceKBMigration(c, task.Request.OperationID) })
			if err != nil && !errors.Is(err, serrors.ErrMigrationBusy) {
				failures = append(failures, err)
			}
			continue
		}
		if task.Phase != MigrationFencing || s.Fence == nil {
			continue
		}
		var dir Directory
		err := s.attempt(ctx, func(c context.Context) error { var e error; dir, e = s.Manager.StorageDirectory(c); return e })
		if err != nil {
			failures = append(failures, err)
			continue
		}
		current, ok := dir.Migrations[task.Request.OperationID]
		if !ok || current.Phase != MigrationFencing {
			continue
		}
		if err := dir.ValidateFailedBlock(current); err != nil {
			failures = append(failures, err)
			continue
		}
		for _, id := range current.RequiredFenceIDs() {
			if !s.IsLeader() {
				return errors.Join(failures...)
			}
			if slices.Contains(current.FencedIDs, id) {
				continue
			}
			node, ok := dir.Nodes[id]
			if !ok || node.Addr == "" {
				failures = append(failures, serrors.ErrPlacementUnknown)
				continue
			}
			err := s.attempt(ctx, func(c context.Context) error {
				if err := s.Fence(c, node, current); err != nil {
					return err
				}
				if !s.IsLeader() {
					return serrors.ErrNoLeaderYet
				}
				return s.Manager.ProposeAckKBMigrationFence(c, current.Request.OperationID, current.Request.ExpectedEpoch, id)
			})
			if err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

// runTask owns the leader-lifetime discipline for work that can outlive a single
// RPC budget: one bounded context, cancelled as soon as this process stops being
// the leader, so a former leader never keeps acting on a task's behalf.
func runTask[T any](s *FenceScheduler, ctx context.Context, run func(context.Context) (T, error)) (T, error) {
	var zero T
	budget := s.StageTimeout
	if budget <= 0 {
		budget = 10 * time.Minute
	}
	c, cancel := context.WithTimeout(ctx, budget)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-c.Done():
				return
			case <-ticker.C:
				if !s.IsLeader() {
					cancel()
					return
				}
			}
		}
	}()
	result, err := run(c)
	cancel()
	<-stopped
	if err != nil {
		return zero, err
	}
	return result, nil
}

func (s *FenceScheduler) attempt(ctx context.Context, run func(context.Context) error) error {
	budget := s.AttemptTimeout
	if budget <= 0 {
		budget = 2 * time.Second
	}
	c, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	return run(c)
}
func (s *FenceScheduler) Run(ctx context.Context) {
	interval := s.Interval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := s.Step(ctx); err != nil && ctx.Err() == nil && s.Logger != nil {
			s.Logger.Debug("migration preparation remains pending", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// installTask turns a verified artifact into the target's own stored data. It is
// still not an activation: membership, epoch and write admission are untouched.
func (s *FenceScheduler) installTask(ctx context.Context, observed Migration) error {
	var d Directory
	if err := s.attempt(ctx, func(c context.Context) error { var err error; d, err = s.Manager.StorageDirectory(c); return err }); err != nil {
		return err
	}
	m, ok := d.Migrations[observed.Request.OperationID]
	if !ok || m.Request != observed.Request || m.Phase != MigrationStaged || m.Stage == nil {
		return nil
	}
	if err := d.ValidateFailedBlock(m); err != nil {
		return err
	}
	target, ok := d.Nodes[m.Request.ToID]
	if !ok || target.Addr == "" {
		return serrors.ErrPlacementUnknown
	}
	if !s.IsLeader() {
		return serrors.ErrNoLeaderYet
	}
	receipt, err := runTask(s, ctx, func(c context.Context) (MigrationInstallReceipt, error) {
		return s.Install(c, target, m)
	})
	if !s.IsLeader() {
		return serrors.ErrNoLeaderYet
	}
	if err != nil {
		return err
	}
	if receipt.TargetID != target.ID || receipt.SourceID != m.Stage.SourceID || receipt.ManifestHash != m.Stage.ManifestHash || receipt.StageHash != m.Stage.PayloadHash {
		return serrors.ErrMigrationConflict
	}
	return s.attempt(ctx, func(c context.Context) error {
		return s.Manager.ProposeRecordKBMigrationInstall(c, m.Request.OperationID, receipt)
	})
}

// serveTask records the target's local serving evidence. It still activates
// nothing: membership, epoch and write admission are untouched.
func (s *FenceScheduler) serveTask(ctx context.Context, observed Migration) error {
	var d Directory
	if err := s.attempt(ctx, func(c context.Context) error { var err error; d, err = s.Manager.StorageDirectory(c); return err }); err != nil {
		return err
	}
	m, ok := d.Migrations[observed.Request.OperationID]
	if !ok || m.Request != observed.Request || m.Phase != MigrationInstalled || m.Install == nil || m.Serve != nil {
		return nil
	}
	if err := d.ValidateFailedBlock(m); err != nil {
		return err
	}
	target, ok := d.Nodes[m.Request.ToID]
	if !ok || target.Addr == "" {
		return serrors.ErrPlacementUnknown
	}
	if !s.IsLeader() {
		return serrors.ErrNoLeaderYet
	}
	receipt, err := runTask(s, ctx, func(c context.Context) (MigrationServeReceipt, error) {
		return s.Serve(c, target, m)
	})
	if !s.IsLeader() {
		return serrors.ErrNoLeaderYet
	}
	if err != nil {
		return err
	}
	if receipt.TargetID != target.ID || receipt.ManifestHash != m.Install.ManifestHash {
		return serrors.ErrMigrationConflict
	}
	return s.attempt(ctx, func(c context.Context) error {
		return s.Manager.ProposeRecordKBMigrationServe(c, m.Request.OperationID, receipt)
	})
}

// stageTask only records an isolated artifact. No learner becomes a replica.
func (s *FenceScheduler) stageTask(ctx context.Context, observed Migration) error {
	var d Directory
	if err := s.attempt(ctx, func(c context.Context) error { var err error; d, err = s.Manager.StorageDirectory(c); return err }); err != nil {
		return err
	}
	m, ok := d.Migrations[observed.Request.OperationID]
	if !ok || m.Request != observed.Request || m.Phase != MigrationFenced {
		return nil
	}
	if err := d.ValidateFailedBlock(m); err != nil {
		return err
	}
	target, ok := d.Nodes[m.Request.ToID]
	if !ok {
		return serrors.ErrPlacementUnknown
	}
	var failures []error
	for _, id := range m.Old.ReplicaIDs {
		if m.Request.Fault && id == m.Request.FromID {
			continue
		}
		if !containsNode(m.FencedIDs, id) {
			continue
		}
		source, ok := d.Nodes[id]
		if !ok {
			continue
		}
		if !s.IsLeader() {
			return serrors.ErrNoLeaderYet
		}
		receipt, err := runTask(s, ctx, func(c context.Context) (MigrationStageReceipt, error) {
			return s.Stage(c, target, source, m)
		})
		if !s.IsLeader() {
			return serrors.ErrNoLeaderYet
		}
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if receipt.TargetID != target.ID || receipt.SourceID != source.ID {
			failures = append(failures, serrors.ErrMigrationConflict)
			continue
		}
		return s.attempt(ctx, func(c context.Context) error {
			return s.Manager.ProposeRecordKBMigrationStage(c, m.Request.OperationID, receipt)
		})
	}
	return errors.Join(append(failures, serrors.ErrMigrationBusy)...)
}
