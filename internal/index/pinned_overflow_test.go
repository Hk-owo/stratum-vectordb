package index

import (
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// M8 of docs/code-review-2026-09-24.md: when every resident index is pinned
// (refCount != 0) nothing can be evicted, so the manager stays over LRUCapacity /
// memory_threshold — and it did so silently, which is what made the bound look
// enforced. The remedy chosen here is a rate-limited warning rather than a refusal:
// the pins belong to searches that are about to finish, so the overage is transient
// by construction and failing a query over it would trade a short spike for an
// outage.
func TestMakeRoomLockedWarnsWhenEverythingIsPinned(t *testing.T) {
	im := NewIndexManager(IndexManagerConfig{LRUCapacity: 1, MemoryThresholdMB: 0})
	core, logs := observer.New(zap.WarnLevel)
	im.SetLogger(zap.New(core))

	im.mu.Lock()
	im.loaded[indexKey{"kb-1", 1}] = &loadedIndex{refCount: 1, lastAccess: time.Now().Add(-time.Minute)}
	im.loaded[indexKey{"kb-1", 2}] = &loadedIndex{refCount: 1, lastAccess: time.Now()}
	evicted := im.makeRoomLocked()
	im.mu.Unlock()

	if len(evicted) != 0 {
		t.Fatalf("nothing is evictable, so nothing may be evicted; got %v", evicted)
	}
	if logs.Len() != 1 {
		t.Fatalf("over capacity with everything pinned must be logged once, got %d lines", logs.Len())
	}
	entry := logs.All()[0]
	if entry.Level != zap.WarnLevel {
		t.Errorf("level = %v, want Warn", entry.Level)
	}
	// The numbers are the point: how far over the bound, and why nothing could go.
	fields := entry.ContextMap()
	if fields["loaded"] != int64(2) || fields["lru_capacity"] != int64(1) || fields["pinned"] != int64(2) {
		t.Errorf("the line must carry loaded/capacity/pinned, got %v", fields)
	}

	// Rate-limited: the condition lasts as long as the burst, and each search in it
	// walks this path.
	im.mu.Lock()
	im.makeRoomLocked()
	im.makeRoomLocked()
	im.mu.Unlock()
	if logs.Len() != 1 {
		t.Errorf("the warning must be rate-limited; got %d lines", logs.Len())
	}
}

// The ordinary case stays quiet: an evictable index is evicted to get under the
// bound, and nothing is logged about memory.
func TestMakeRoomLockedStaysQuietWhenSomethingIsEvictable(t *testing.T) {
	im := NewIndexManager(IndexManagerConfig{LRUCapacity: 2})
	core, logs := observer.New(zap.WarnLevel)
	im.SetLogger(zap.New(core))

	im.mu.Lock()
	// Two residents against a capacity of two: making room for one more needs
	// exactly one eviction, and there is an unpinned candidate for it.
	im.loaded[indexKey{"kb-1", 1}] = &loadedIndex{refCount: 0, lastAccess: time.Now().Add(-time.Minute)}
	im.loaded[indexKey{"kb-1", 2}] = &loadedIndex{refCount: 1, lastAccess: time.Now()}
	evicted := im.makeRoomLocked()
	im.mu.Unlock()

	if len(evicted) != 1 || evicted[0] != (indexKey{"kb-1", 1}) {
		t.Fatalf("evicted = %v, want the unpinned, least-recently-used kb-1/1", evicted)
	}
	if logs.Len() != 0 {
		t.Errorf("a normal eviction must not warn, got %v", logs.All())
	}
}
