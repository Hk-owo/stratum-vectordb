package index

import (
	"context"
	"time"
)

// PauseKnowledgeBase prevents new builds and drains queued/running ones before
// deleting files. It stays paused until a new committed migration imports data.
func (im *IndexManagerImpl) PauseKnowledgeBase(ctx context.Context, kb string) error {
	im.mu.Lock()
	if im.pausedKB == nil {
		im.pausedKB = map[string]bool{}
	}
	im.pausedKB[kb] = true
	im.mu.Unlock()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		im.mu.Lock()
		busy := false
		for k, v := range im.loading {
			if k.kbID == kb && v {
				busy = true
				break
			}
		}
		im.mu.Unlock()
		if !busy {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (im *IndexManagerImpl) ResumeKnowledgeBase(kb string) {
	im.mu.Lock()
	delete(im.pausedKB, kb)
	delete(im.deletedKBs, kb)
	for key := range im.deletedVersions {
		if key.kbID == kb {
			delete(im.deletedVersions, key)
		}
	}
	im.mu.Unlock()
}
