// Package sync provides the inter-node data replication layer for
// Stratum's follower replicas. The leader streams DocStore, ChunkDocMapper,
// VersionDocList, and ChunkStore entries to followers via a server-side
// streaming gRPC; followers apply each entry idempotently to their local
// stores, then trigger an independent HNSW index build.
//
// See Stratum_设计文档v10.md "多副本构建" and api/proto/sync.proto.
package sync

import "context"

// FollowerSync is the follower's side of the data-sync flow. It is invoked
// by the Raft apply path when this node (a non-leader) applies a
// cmdCreateVersion entry — i.e. the Raft state machine has a new version
// whose storage-layer data needs to be pulled from the leader.
type FollowerSync interface {
	// PullVersion pulls the full storage-layer dataset for (kbID,
	// versionID) from the leader at leaderAddr and writes it into this
	// node's local DocStore, ChunkDocMapper, VersionDocList, and
	// ChunkStore.
	//
	// Idempotent: every write to the local PebbleDB stores is keyed by
	// (kbID, versionID, docID/chunkID) and all four stores are
	// idempotent-by-key. Calling PullVersion with the same arguments
	// more than once is safe.
	//
	// On success the follower calls IndexManager.TriggerBuild to kick
	// off an independent HNSW build; on failure the caller (the Raft
	// apply loop) retries with exponential backoff.
	PullVersion(ctx context.Context, leaderAddr string, kbID string, versionID int64) error
}
