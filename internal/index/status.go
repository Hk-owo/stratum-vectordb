// Status reporting (§5.7, §8.6(d)): what the control layer is told about a build
// or a dropped artifact, and the observability accessors the admin surface reads.
package index

import (
	"context"
	"go.uber.org/zap"
)

// reportBuildScheduled, reportBuildSucceeded, reportBuildFailed and
// reportIndexInstalled forward one transition to the node state machine, if one
// is wired.
//
// A refusal is logged, never returned: these run around work whose outcome has
// already been decided — a build that finished, an artifact already on disk —
// and a bookkeeping disagreement must not turn into a build failure the caller
// acts on.
func (im *IndexManagerImpl) reportBuildScheduled(kbID string, versionID int64) {
	if im.cfg.BuildState == nil {
		return
	}
	if err := im.cfg.BuildState.BuildScheduled(kbID, versionID); err != nil {
		im.logger.Warn("index: build start not recorded in the node state machine",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

func (im *IndexManagerImpl) reportBuildSucceeded(kbID string, versionID int64) {
	if im.cfg.BuildState == nil {
		return
	}
	if err := im.cfg.BuildState.BuildSucceeded(kbID, versionID); err != nil {
		im.logger.Warn("index: build success not recorded in the node state machine",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

func (im *IndexManagerImpl) reportBuildFailed(kbID string, versionID int64, detail string) {
	if im.cfg.BuildState == nil {
		return
	}
	if err := im.cfg.BuildState.BuildFailed(kbID, versionID, detail); err != nil {
		im.logger.Warn("index: build failure not recorded in the node state machine",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

func (im *IndexManagerImpl) reportIndexDropped(kbID string, versionID int64) {
	if im.cfg.BuildState == nil {
		return
	}
	if err := im.cfg.BuildState.IndexDropped(kbID, versionID); err != nil {
		im.logger.Warn("index: retention drop not recorded in the node state machine",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

func (im *IndexManagerImpl) reportIndexInstalled(kbID string, versionID int64) {
	if im.cfg.BuildState == nil {
		return
	}
	if err := im.cfg.BuildState.IndexInstalled(kbID, versionID); err != nil {
		im.logger.Warn("index: install not recorded in the node state machine",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

// Ping implements IndexManager.
func (im *IndexManagerImpl) Ping(_ context.Context) error {
	return nil
}

// LoadedCount implements IndexManager: returns how many indexes are
// currently in memory.
func (im *IndexManagerImpl) LoadedCount() int {
	im.mu.Lock()
	defer im.mu.Unlock()
	return len(im.loaded)
}

// IsLoaded reports whether (kbID, versionID)'s index is currently in memory.
func (im *IndexManagerImpl) IsLoaded(kbID string, versionID int64) bool {
	im.mu.Lock()
	defer im.mu.Unlock()
	_, ok := im.loaded[indexKey{kbID, versionID}]
	return ok
}

// RefCount returns the current reference count for (kbID, versionID)'s
// index, or 0 if it is not loaded.
func (im *IndexManagerImpl) RefCount(kbID string, versionID int64) int {
	im.mu.Lock()
	defer im.mu.Unlock()
	if idx, ok := im.loaded[indexKey{kbID, versionID}]; ok {
		return idx.refCount
	}
	return 0
}
