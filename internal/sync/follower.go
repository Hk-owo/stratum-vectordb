package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/rpcpool"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
)

// Follower implements FollowerSync. On PullVersionData it opens a
// server-side streaming connection to the leader's DataSyncService,
// applies each SyncEntry to the local stores, and then triggers an
// HNSW index build via IndexManager.
type Follower struct {
	executionGate *storage.ExecutionGate
	placements    storage.Resolver
	nodeID        int64
	docStore      docstore.DocStore
	chunkDoc      chunkdoc.ChunkDocMapper
	versionDoc    versiondoc.VersionDocList
	chunkStore    chunkstore.ChunkStore
	indexManager  IndexBuildTrigger

	// advanceVersion records that this node now holds a version contiguously,
	// once a pull has written its records. Optional, but without it a replica
	// that pulled every record still reports version 0: the station's freshness
	// check (§9.3(2)) then refuses a replica that is in fact current, and the
	// read falls through to whichever node answers "0 < required".
	advanceVersion LocalVersionAdvancer

	// docIDSetHash answers the writer's committed document-set digest for a version.
	// It is the only fact that tells "this version has no documents" apart from "the
	// source sent nothing for it" — see confirmVersionIsEmpty. Optional, like the
	// others: without it an empty receive is taken at face value, which is the
	// behaviour every deployment had before this existed.
	docIDSetHash VersionDocIDSetHash

	// pullDialTimeout bounds the connection handshake of a pull, and ONLY the
	// handshake. Zero means DefaultPullDialTimeout. Tests set it to prove the
	// bound does not leak onto the transfer (see PullVersionData).
	pullDialTimeout time.Duration

	// pool supplies long-lived outbound storage-peer channels when wired.
	pool *rpcpool.Pool

	// tombstones is this node's record of versions it has been told are deleted.
	// A pull runs on the receiving side, so it is the one place a version whose
	// metadata no longer exists can still be recognised — the source cannot know
	// that this node already brought it up to date past the deletion.
	// Optional: without it a pull of such a version writes it back and the reverse
	// reconciliation finds it later.
	tombstones VersionTombstoneReader
}

// FollowerOption configures optional dependencies and timeouts of a Follower at
// construction time.
type FollowerOption func(*Follower)

// WithFollowerStoragePlacement wires the replica placement check for pulls.
func WithFollowerStoragePlacement(resolver storage.Resolver, nodeID int64) FollowerOption {
	return func(f *Follower) {
		f.placements, f.nodeID = resolver, nodeID
	}
}

// WithFollowerExecutionGate wires the storage execution gate for pulls.
func WithFollowerExecutionGate(gate *storage.ExecutionGate) FollowerOption {
	return func(f *Follower) { f.executionGate = gate }
}

// WithFollowerPool binds a shared outbound peer connection pool for pulls.
func WithFollowerPool(pool *rpcpool.Pool) FollowerOption {
	return func(f *Follower) { f.pool = pool }
}

func (f *Follower) flushReceivedData(ctx context.Context) error {
	if f.chunkStore == nil {
		return nil
	}
	return f.chunkStore.FlushWAL(ctx)
}

// flushAndCommitVersionDocList flushes received chunk vectors and then writes
// the deferred VersionDocList in one commit so VersionDocList (and its digest)
// never becomes visible before DocStore, ChunkDocMapper, and ChunkStore land.
func (f *Follower) flushAndCommitVersionDocList(ctx context.Context, kbID string, versionID int64, docIDs []string) error {
	if err := f.flushReceivedData(ctx); err != nil {
		return err
	}
	if len(docIDs) > 0 {
		if err := f.versionDoc.WriteMany(ctx, kbID, versionID, docIDs); err != nil {
			return fmt.Errorf("sync: write version doc list (%s v%d): %w", kbID, versionID, err)
		}
	}
	return nil
}

// WithFollowerVersionTombstones wires this node's record of deleted versions.
func WithFollowerVersionTombstones(t VersionTombstoneReader) FollowerOption {
	return func(f *Follower) { f.tombstones = t }
}

// errVersionTombstoned reports that a version was refused because this node knows
// it is deleted. Nothing is broken and the version will never be wanted again, so
// callers in this package treat it as "no work" rather than as a failure.
//
// The landing paths in internal/plane check the table themselves before they get
// here (that is where the skip belongs — the cursor must not move), so this is the
// last line rather than the first.
var errVersionTombstoned = errors.New("sync: version was deleted on this node")

// DefaultPullDialTimeout bounds the connection handshake of a pull.
const DefaultPullDialTimeout = 15 * time.Second

// WithPullDialTimeout overrides the pull handshake bound (sync.pull_dial_timeout_ms).
// A non-positive value keeps DefaultPullDialTimeout.
func WithPullDialTimeout(d time.Duration) FollowerOption {
	return func(f *Follower) { f.pullDialTimeout = d }
}

// dialTimeout returns the handshake bound, defaulting for a zero-valued Follower.
func (f *Follower) dialTimeout() time.Duration {
	if f.pullDialTimeout > 0 {
		return f.pullDialTimeout
	}
	return DefaultPullDialTimeout
}

// WithFollowerLocalVersionAdvancer wires the cursor update a completed pull performs.
func WithFollowerLocalVersionAdvancer(a LocalVersionAdvancer) FollowerOption {
	return func(f *Follower) { f.advanceVersion = a }
}

// VersionDocIDSetHash answers the document-set digest the writer committed for a version
// (docs/参考/历史/Stratum_设计文档v13.md §7.12). Declared here because it is the one fact the sync paths need
// from the replicated metadata: an empty receive is only a version with no documents if
// the writer said so.
//
// raft.DocIDSetHashReader adapts any raft.RaftNode onto it — the remote shape included, so
// a storage node judges its own pulls the same way a voter does.
type VersionDocIDSetHash interface {
	DocIDSetHash(ctx context.Context, kbID string, versionID int64) (string, error)
}

// WithVersionDocIDSetHash wires the metadata read that an empty receive is confirmed against.
func WithVersionDocIDSetHash(r VersionDocIDSetHash) FollowerOption {
	return func(f *Follower) { f.docIDSetHash = r }
}

// markVersionContiguous moves the cursor if an advancer is wired.
func (f *Follower) markVersionContiguous(kbID string, versionID int64) {
	if f.advanceVersion != nil {
		f.advanceVersion.MarkVersionContiguous(kbID, versionID)
	}
}

// confirmVersionIsEmpty checks an EMPTY receive against the replicated metadata: the
// version counts as empty only if the writer's committed document-set digest says it has
// no documents.
//
// Nothing on the wire separates the two cases — a version with no documents and a source
// with no data for the version both arrive as an empty stream that reports success — and
// guessing wrong is one-directional. The cursor would move onto a version this node does
// not hold, and EnsureIndex/FetchVersionData open with "the cursor is already there,
// nothing to fetch", so the miss is never retried.
//
// A digest that cannot be READ is not read as "empty": that would turn the metadata being
// away into an assertion about the data, the same mistake one level up. The pull fails
// instead, so the caller retries — the version is not known to be here either way.
//
// No reader wired keeps the previous behaviour: an empty receive is taken at face value.
func (f *Follower) confirmVersionIsEmpty(ctx context.Context, kbID string, versionID int64) error {
	if f.docIDSetHash == nil {
		return nil
	}
	hash, err := f.docIDSetHash.DocIDSetHash(ctx, kbID, versionID)
	if err != nil {
		return fmt.Errorf("sync: PullVersionData(%s, %d): an empty transfer must be confirmed against the writer's document-set digest, and it could not be read: %w",
			kbID, versionID, err)
	}
	if hash == types.EmptyDocIDSetHash {
		return nil
	}
	return fmt.Errorf("sync: PullVersionData(%s, %d): the source sent no records, but the version's document set is not empty: %w",
		kbID, versionID, stratumerrors.ErrIndexNotReady)
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
	opts ...FollowerOption,
) *Follower {
	f := &Follower{
		docStore:     docStore,
		chunkDoc:     chunkDoc,
		versionDoc:   versionDoc,
		chunkStore:   chunkStore,
		indexManager: indexManager,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(f)
		}
	}
	return f
}

// PullVersionData fetches the version's data and leaves its index unbuilt.
//
// The index is not this path's business, and that is §8.6(b) rather than an
// omission: a version is built when something ASKS for it — a query, an explicit
// warm-up, or the active-version pre-build — and a data transfer is not an ask.
//
// There used to be a PullVersion that did build on arrival, plus a
// PullVersionOptions to choose between them. Both are gone. What they encoded
// was the eager behaviour §8.6(b) replaced: a pull runs on every replica a
// version's data reaches, so building here meant every replica built every
// version, which is the cost lazy construction exists to remove.
func (f *Follower) PullVersionData(ctx context.Context, leaderAddr string, kbID string, versionID int64) error {
	var guardErr error
	ctx, guardErr = bindStoragePlacement(ctx, f.placements, f.nodeID, kbID)
	if guardErr != nil {
		return guardErr
	}
	if snap, ok := storage.Bound(ctx, kbID); ok {
		var release func()
		ctx, release, guardErr = storage.EnterMutation(ctx, f.executionGate, snap)
		if guardErr != nil {
			return guardErr
		}
		defer release()
	} else if f.executionGate != nil {
		return stratumerrors.ErrPlacementUnknown
	}
	if f.placements != nil {
		snap, _ := storage.Bound(ctx, kbID)
		if !snap.HasAddress(leaderAddr) {
			return stratumerrors.ErrNotKBReplica
		}
	}
	// Refused before the connection is made: this node was told the version is
	// deleted, so pulling it would write data back that the deletion already
	// reclaimed. The refusal is a RETURN VALUE rather than a silent success because
	// the caller must not advance this node's cursor over a version it does not
	// hold — VersionTombstoned is how it tells this apart from "the pull worked".
	if f.tombstones != nil {
		if kbGone, err := f.tombstones.ContainsKB(ctx, kbID); err == nil && kbGone {
			return fmt.Errorf("sync: pull %s v%d from %s: %w", kbID, versionID, leaderAddr, errVersionTombstoned)
		}
		gone, err := f.tombstones.Contains(ctx, kbID, versionID)
		switch {
		case err != nil:
			// Unreadable is not "alive": the pull proceeds, and the reverse
			// reconciliation covers anything it lets through.
		case gone:
			return fmt.Errorf("sync: pull %s v%d from %s: %w", kbID, versionID, leaderAddr, errVersionTombstoned)
		}
	}

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
	conn, release, err := rpcpool.Acquire(dialCtx, f.pool, nil, nil, leaderAddr)
	cancelDial()
	if err != nil {
		return fmt.Errorf("sync: dial leader %s: %w", leaderAddr, err)
	}
	defer release()

	client := pb.NewDataSyncServiceClient(conn)
	stream, err := client.PullVersionData(ctx, &pb.PullVersionDataRequest{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
	})
	if err != nil {
		return fmt.Errorf("sync: PullVersionData(%s, %d): %w", kbID, versionID, err)
	}

	received := 0
	var versionDocIDs []string
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("sync: recv SyncEntry: %w", err)
		}

		received++
		// ExportVersion streams VERSION_DOC_LIST first. Buffer it until all
		// DocStore, ChunkDocMapper, and ChunkVector entries have been applied
		// and flushed so concurrent readers never see a complete DocIDSetHash
		// while chunks or documents are still landing.
		if entry.GetEntryType() == pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST &&
			entry.GetKbId() == kbID && entry.GetVersionId() == versionID {
			versionDocIDs = append(versionDocIDs, entry.GetDocId())
			continue
		}
		if err := f.applyEntry(ctx, entry); err != nil {
			return fmt.Errorf("sync: apply %s entry: %w", entry.GetEntryType(), err)
		}
	}

	// "The receive completed" is not "this version's records are here". A source that
	// holds no data for the version answers with an empty stream and reports success —
	// byte for byte what a version with no documents looks like — so an empty receive
	// has to be confirmed before the cursor may claim the version.
	if received == 0 {
		if err := f.confirmVersionIsEmpty(ctx, kbID, versionID); err != nil {
			return err
		}
	}

	// Every record is in the local stores, so this node holds the version —
	if err := f.flushAndCommitVersionDocList(ctx, kbID, versionID, versionDocIDs); err != nil {
		return err
	}
	// move the cursor now, before the build and before anything can ask about
	// it. The cursor is what §9.3(2) reads, and a replica whose cursor never
	// moves is refused as stale however complete its data is.
	f.markVersionContiguous(kbID, versionID)

	// All data written. NO BUILD — see the method comment.
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
