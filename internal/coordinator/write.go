// Package coordinator defines the WriteCoordinator and DeleteCoordinator
// interfaces — the orchestration layer between the gRPC Service layer and
// the internal storage/index/Raft/WAL modules.
//
// See Stratum_接口设计v9.md "WriteCoordinator" / "DeleteCoordinator" and
// Stratum_设计文档v10.md "写路径" / "删除知识库" for the authoritative
// design. This file contains only the WriteCoordinator interface
// definition; the real orchestration implementation (WriteCoordinatorImpl,
// wiring together WAL/RaftNode/ChunkSplitter/EmbedClient/BloomFilter/
// ChunkStore/ChunkDocMapper/DocStore/VersionDocList/IndexManager per the
// CreateVersion logical flow) is built in Phase 5 (5-A).
package coordinator

import (
	"context"

	"stratum/internal/types"
)

// WriteCoordinator owns the internal orchestration logic for CreateVersion.
// Service.CreateVersion is expected to do only request validation and
// response translation; all orchestration is delegated here, with
// dependencies injected via the implementation's constructor.
//
// The full CreateVersion flow this interface's real implementation
// executes (synchronously through WAL COMMIT, then asynchronously for
// index build) is documented in Stratum_接口设计v9.md's "CreateVersion"
// logical flow and Stratum_设计文档v10.md's "写路径" section:
//
//  1. WAL.WriteBegin
//  2. RaftNode.ProposeCreateVersion (apply phase writes WAL.WriteVersionID
//     first, then allocates the version into the state machine)
//  3. For each changed document: ChunkSplitter.Split -> EmbedClient.Embed
//     -> per chunk: BloomFilter.Test -> ChunkStore.Exists (false-positive
//     confirmation) -> ChunkStore.Write + BloomFilter.Add ->
//     ChunkDocMapper.Write -> DocStore.Write
//  4. VersionDocList.Write (parent version's full doc set + this version's
//     changes)
//  5. BloomFilter.Serialize (version-document filter persistence)
//  6. WAL.WriteCommit
//  7. IndexManager.TriggerBuild (asynchronous; Execute returns before this
//     completes)
//
// IO error retries: non-permanent storage-layer write errors are retried
// internally with exponential backoff (count/interval from
// write_coordinator.max_retries / write_coordinator.retry_base_interval_ms
// configuration). Once retries are exhausted, Execute returns an error and
// the caller is expected to simply re-invoke CreateVersion — crash
// recovery is handled automatically via WAL on the next process restart;
// callers never need to reason about PENDING state themselves.
type WriteCoordinator interface {
	// Execute runs the full CreateVersion orchestration for kbID, with
	// parentVersionID as the new version's parent and changes as the set
	// of document mutations to apply. Returns the newly allocated version
	// ID once synchronous steps (WAL COMMIT) have completed; index build
	// continues asynchronously.
	Execute(ctx context.Context, kbID string, parentVersionID int64, changes []types.DocChange) (int64, error)
}
