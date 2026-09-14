package sync

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/versiondoc"
)

// Follower implements FollowerSync. On PullVersion it opens a
// server-side streaming connection to the leader's DataSyncService,
// applies each SyncEntry to the local stores, and then triggers an
// HNSW index build via IndexManager.
type Follower struct {
	docStore     docstore.DocStore
	chunkDoc     chunkdoc.ChunkDocMapper
	versionDoc   versiondoc.VersionDocList
	chunkStore   chunkstore.ChunkStore
	indexManager IndexBuildTrigger
}

// DigestOf computes the document-set digest of (kbID, versionID) from this
// node's own stores — the same value the coordinator reports as durable.
//
// The §7.3 takeover needs it: a replica announcing a version must announce a
// digest that followers can verify their pulled data against, so it has to be
// computed from the data rather than invented.
func (f *Follower) DigestOf(ctx context.Context, kbID string, versionID int64) (string, error) {
	docIDs, err := f.versionDoc.ListDocIDs(ctx, kbID, versionID)
	if err != nil {
		return "", fmt.Errorf("sync: digest of %s v%d: %w", kbID, versionID, err)
	}
	return ComputeDocIDSetHash(docIDs), nil
}

// IndexBuildTrigger is the subset of index.IndexManager that the sync
// follower needs: triggering a build after data is in place.
type IndexBuildTrigger interface {
	TriggerBuild(ctx context.Context, kbID string, versionID int64) error
}

// NewFollower constructs a Follower.
func NewFollower(
	docStore docstore.DocStore,
	chunkDoc chunkdoc.ChunkDocMapper,
	versionDoc versiondoc.VersionDocList,
	chunkStore chunkstore.ChunkStore,
	indexManager IndexBuildTrigger,
) *Follower {
	return &Follower{
		docStore:     docStore,
		chunkDoc:     chunkDoc,
		versionDoc:   versionDoc,
		chunkStore:   chunkStore,
		indexManager: indexManager,
	}
}

// PullVersion implements FollowerSync.
// PullVersionOptions controls what a pull does beyond fetching the data.
type PullVersionOptions struct {
	// SkipIndexBuild leaves the index unbuilt after the data lands. It is what
	// a version that is not the active one wants (Stratum_设计文档v13.md §8.6b):
	// the data has to be here, but its index is built lazily, on first query.
	SkipIndexBuild bool
}

func (f *Follower) PullVersion(ctx context.Context, leaderAddr string, kbID string, versionID int64) error {
	return f.PullVersionWith(ctx, leaderAddr, kbID, versionID, PullVersionOptions{})
}

// PullVersionData fetches the version's data and deliberately leaves its index
// unbuilt, for a version whose index should be built lazily (§8.6b).
func (f *Follower) PullVersionData(ctx context.Context, leaderAddr string, kbID string, versionID int64) error {
	return f.PullVersionWith(ctx, leaderAddr, kbID, versionID, PullVersionOptions{SkipIndexBuild: true})
}

// PullVersionWith is PullVersion with explicit control over the post-pull step.
func (f *Follower) PullVersionWith(ctx context.Context, leaderAddr string, kbID string, versionID int64, opts PullVersionOptions) error {
	// 兜底:不信任调用方 ctx 无超时(如 context.Background())。
	// grpc.WithBlock() 在 leader 不可达(域名无法解析/拒绝连接)时会一直
	// 等待连接建立;这里强制给整个拉取过程设上界,失败由调用方的 deadline
	// 循环重试。否则一旦卡死会连带阻塞 raft 的 apply 循环。
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("sync: dial leader %s: %w", leaderAddr, err)
	}
	defer conn.Close()

	client := pb.NewDataSyncServiceClient(conn)
	stream, err := client.PullVersionData(ctx, &pb.PullVersionDataRequest{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
	})
	if err != nil {
		return fmt.Errorf("sync: PullVersionData(%s, %d): %w", kbID, versionID, err)
	}

	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("sync: recv SyncEntry: %w", err)
		}

		if err := f.applyEntry(ctx, entry); err != nil {
			return fmt.Errorf("sync: apply %s entry: %w", entry.GetEntryType(), err)
		}
	}

	// All data written; trigger an independent HNSW build on this node.
	if opts.SkipIndexBuild {
		return nil
	}
	if err := f.indexManager.TriggerBuild(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("sync: TriggerBuild(%s, %d): %w", kbID, versionID, err)
	}

	return nil
}

// applyDocStoreEntry writes a DOC_STORE entry to the local docstore. The
// wire payload carries docstore's tag byte verbatim (tagContent 0x01 +
// content, or tagTombstone 0x00) so the follower can distinguish a live
// document with empty content ({0x01}) from a tombstone ({0x00}) — see
// LeaderHandler.streamDocStore. The tag is stripped before the value is
// handed to DocStore.Write, which applies its own tag/tombstone semantics.
func (f *Follower) applyDocStoreEntry(ctx context.Context, entry *pb.SyncEntry) error {
	payload := entry.GetPayload()
	if len(payload) == 0 {
		return fmt.Errorf("sync: DOC_STORE entry for %s/%s v%d missing tag byte", entry.GetKbId(), entry.GetDocId(), entry.GetVersionId())
	}
	switch payload[0] {
	case tagTombstone:
		return f.docStore.Write(ctx, entry.GetKbId(), entry.GetDocId(), entry.GetVersionId(), nil)
	case tagContent:
		return f.docStore.Write(ctx, entry.GetKbId(), entry.GetDocId(), entry.GetVersionId(), payload[1:])
	default:
		return fmt.Errorf("sync: DOC_STORE entry for %s/%s v%d has unknown tag 0x%02x", entry.GetKbId(), entry.GetDocId(), entry.GetVersionId(), payload[0])
	}
}

// applyEntry writes a single SyncEntry to the appropriate local store.
// All writes are idempotent: re-applying the same entry is a no-op.
func (f *Follower) applyEntry(ctx context.Context, entry *pb.SyncEntry) error {
	switch entry.GetEntryType() {
	case pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST:
		return f.versionDoc.Write(ctx, entry.GetKbId(), entry.GetVersionId(), entry.GetDocId())

	case pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE:
		return f.applyDocStoreEntry(ctx, entry)

	case pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_FORWARD:
		// Forward and reverse are the same Write call (PebbleChunkDocMapper
		// writes both directions in a single batch).
		return f.chunkDoc.Write(ctx, entry.GetKbId(), entry.GetChunkId(), entry.GetDocId())

	case pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_DOC_REVERSE:
		// Already handled by CHUNK_DOC_FORWARD above — the Write is a
		// single atomic batch creating both directions. Receiving a
		// reverse-only entry without a forward entry means the leader is
		// missing the forward mapping, which shouldn't happen; write it
		// anyway (idempotent).
		return f.chunkDoc.Write(ctx, entry.GetKbId(), entry.GetChunkId(), entry.GetDocId())

	case pb.SyncEntryType_SYNC_ENTRY_TYPE_CHUNK_VECTOR:
		vector := bytesToFloat32s(entry.GetPayload())
		return f.chunkStore.Write(ctx, entry.GetKbId(), entry.GetChunkId(), vector)

	default:
		return fmt.Errorf("sync: unknown SyncEntryType %v", entry.GetEntryType())
	}
}

var _ FollowerSync = (*Follower)(nil)
