package sync

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
	"stratum/internal/bloom"
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

	// advanceVersion records that this node now holds a version contiguously,
	// once a pull has written its records. Optional, but without it a replica
	// that pulled every record still reports version 0: the station's freshness
	// check (§9.3(2)) then refuses a replica that is in fact current, and the
	// read falls through to whichever node answers "0 < required".
	advanceVersion LocalVersionAdvancer

	// versionBloom rebuilds a version's document filter once its data has landed.
	// Optional, but a replica that never rebuilds keeps whatever filter it built
	// first — and a filter built before the data arrived is empty, which drops every
	// search hit (see SetVersionBloom).
	versionBloom VersionBloomStore

	// pullDialTimeout bounds the connection handshake of a pull, and ONLY the
	// handshake. Zero means DefaultPullDialTimeout. Tests set it to prove the
	// bound does not leak onto the transfer (see PullVersionWith).
	pullDialTimeout time.Duration
}

// DefaultPullDialTimeout bounds the connection handshake of a pull.
const DefaultPullDialTimeout = 15 * time.Second

// dialTimeout returns the handshake bound, defaulting for a zero-valued Follower.
func (f *Follower) dialTimeout() time.Duration {
	if f.pullDialTimeout > 0 {
		return f.pullDialTimeout
	}
	return DefaultPullDialTimeout
}

// VersionBloomStore is the slice of bloom.VersionBloomStore the sync paths need: the
// REBUILD of a version's document filter from a document set that is now local.
// Declared here so this package depends on none of the store's read/delete surface.
type VersionBloomStore interface {
	BuildAndPersist(kbID string, versionID int64, docIDs []string) (bloom.BloomFilter, error)
}

// SetLocalVersionAdvancer wires the cursor update a completed pull performs.
//
// A setter rather than a constructor argument because the storage plane that
// implements it is built after the follower in every assembly.
func (f *Follower) SetLocalVersionAdvancer(a LocalVersionAdvancer) {
	f.advanceVersion = a
}

// SetVersionBloom wires the document-filter rebuild a completed transfer performs.
//
// The writer builds this filter inside its write transaction; a replica that receives
// the same data has to build it too. That is not a nicety: the filter drops the search
// hits that are not in the version, so a replica whose filter was built before the
// data arrived (a query that came in early, a push racing a read) answers "nothing
// matched" — with no error — for a version it holds in full. A setter rather than a
// constructor argument, following SetLocalVersionAdvancer: the store is assembled
// after the follower in every assembly.
func (f *Follower) SetVersionBloom(b VersionBloomStore) {
	f.versionBloom = b
}

// rebuildVersionBloom re-derives the version's document filter from the document set
// that just landed. Best effort: the filter is an accelerator, so a failure costs
// precision and never correctness — the query path confirms every hit against the
// version doc list anyway.
func (f *Follower) rebuildVersionBloom(ctx context.Context, kbID string, versionID int64) {
	if f.versionBloom == nil {
		return
	}
	docIDs, err := f.versionDoc.ListDocIDs(ctx, kbID, versionID)
	if err != nil {
		return
	}
	if _, err := f.versionBloom.BuildAndPersist(kbID, versionID, docIDs); err != nil {
		_ = err // non-fatal; the next transfer or the query path rebuilds it
	}
}

// markVersionContiguous moves the cursor if an advancer is wired.
func (f *Follower) markVersionContiguous(kbID string, versionID int64) {
	if f.advanceVersion != nil {
		f.advanceVersion.MarkVersionContiguous(kbID, versionID)
	}
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
	// The connection HANDSHAKE gets its own bound; the TRANSFER does not.
	//
	// grpc.WithBlock() waits for the connection to be established, so a peer that
	// cannot be reached at all (unresolvable name, refused connection) would wait
	// forever without one — and that is the whole purpose of the bound below. What
	// it must not do is bound the transfer as well: a version's payload grows with
	// the knowledge base, and a single fixed window over "handshake + stream +
	// apply" claims the pull fits in that window at every scale. Measured at 20,000
	// documents (~56 MB): a 30 s window over the whole pull produced 64 rounds of
	// `sync: recv SyncEntry: ... DeadlineExceeded` followed by "version data did not
	// converge within 30s" — for a transfer that completes perfectly well once given
	// room. The caller's context is the transfer's budget; EnsureIndex bounds it by
	// PROGRESS (not by wall clock) and the lag catch-up by its own timeout.
	//
	// Nothing here runs on the Raft apply path, so a long transfer cannot stall
	// log application: EnsureIndex is reached from the lag catch-up and from the
	// query path's background pull, both off the apply loop.
	dialCtx, cancelDial := context.WithTimeout(ctx, f.dialTimeout())
	conn, err := grpc.DialContext(dialCtx, leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	cancelDial()
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

	// Every record is in the local stores, so this node holds the version —
	// move the cursor now, before the build and before anything can ask about
	// it. The cursor is what §9.3(2) reads, and a replica whose cursor never
	// moves is refused as stale however complete its data is.
	f.markVersionContiguous(kbID, versionID)

	// The data is complete, so the version's document filter can be derived from it —
	// the same step the writer's transaction performs. See SetVersionBloom for why a
	// replica has to do this rather than inherit the writer's filter.
	f.rebuildVersionBloom(ctx, kbID, versionID)

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
