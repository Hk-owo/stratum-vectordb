package coordinator

import "context"

// DeleteCoordinator owns the asynchronous cleanup orchestration that runs
// after DeleteKnowledgeBase marks a knowledge base as deleting. The real
// implementation (DeleteCoordinatorImpl, built in Phase 5 (5-B)) holds
// references to IndexManager / DocStore / ChunkStore / ChunkDocMapper /
// VersionDocList / RaftNode / WAL.
//
// The full cleanup flow this interface's real implementation executes is
// documented in Stratum_接口设计v9.md's "DeleteKnowledgeBase" logical flow
// and Stratum_设计文档v10.md's "删除知识库" section:
//
//  1. IndexManager.EvictByKB
//  2. delete on-disk index files (ignore ErrNotExist)
//  3. DocStore.DeleteByKB
//  4. ChunkStore.DeleteByKB
//  5. ChunkDocMapper.DeleteByKB
//  6. VersionDocList.DeleteByKB
//  7. RaftNode.ProposeRemoveKBMeta (ErrKnowledgeBaseNotFound treated as success)
//  8. WAL.WriteDeleteComplete
//
// Idempotency: on-disk file deletion ignores ErrNotExist;
// ProposeRemoveKBMeta receiving ErrKnowledgeBaseNotFound is treated as
// success; every other step is a prefix-scan delete and therefore
// naturally idempotent. This lets the whole flow safely re-run from
// scratch after a crash at any step.
//
// Permanent error handling: each step retries with exponential backoff on
// failure; once delete_coordinator.max_retries is exhausted, Execute calls
// RaftNode.ProposeMarkKBDeleteFailed, stops retrying, and surfaces the
// knowledge base via GetSystemStatus for operator intervention.
type DeleteCoordinator interface {
	// Execute runs the full cleanup orchestration for kbID. Called
	// asynchronously after Service.DeleteKnowledgeBase marks the
	// knowledge base as deleting.
	Execute(ctx context.Context, kbID string) error
}
