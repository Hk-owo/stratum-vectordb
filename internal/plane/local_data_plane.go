package plane

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	stratinternalsync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// VersionPuller is the storage layer's data-transfer primitive: it copies one
// version's storage-layer records (documents, chunk↔doc mapping, version doc
// list, chunks) onto this node. *stratumsync.Follower implements it; keeping
// it as an interface here lets the DataPlane be exercised without a live peer.
type VersionPuller interface {
	PullVersion(ctx context.Context, sourceAddr string, kbID string, versionID int64) error
	// PullVersionData fetches the version's data without scheduling an index
	// build (see FetchVersionData).
	PullVersionData(ctx context.Context, sourceAddr string, kbID string, versionID int64) error
}

// VersionChangesFetcher fetches the changes a peer recorded over a version range —
// the client side of §7.5's delta backfill. *sync.VersionChangesPuller implements
// it; declared here so this package does not depend on the transport.
type VersionChangesFetcher interface {
	ChangesInRange(ctx context.Context, peerAddr, kbID string, fromExclusive, toInclusive int64) (map[int64]wal.VersionDelta, error)
}

// VersionExistenceChecker answers "which versions still exist for this knowledge
// base?", from the replicated metadata. It exists because the storage layer cannot
// answer it: absent rows mean "never written", "empty" and "deleted" all at once.
//
// It returns the WHOLE set rather than one version at a time because its caller is
// a backfill that may walk a long gap: asking per version would re-read the version
// list once per step, making one gap O(gap × versions). The set is read once and
// consulted in memory.
//
// A cheap, local lookup by contract, for the same reason SourceResolver is: the
// backfill runs on the Raft apply path, so a probe here would stall every later
// log entry.
type VersionExistenceChecker interface {
	ExistingVersions(ctx context.Context, kbID string) (map[int64]bool, error)
}

// DataVerifier reports whether this node's local stores already hold the
// complete data for (kbID, versionID). The write path commits a document-set
// digest for the version, and this is how the storage layer checks a pull
// against it.
type DataVerifier func(ctx context.Context, kbID string, versionID int64) bool

// SourceResolver answers "which peer holds this version's data right now?".
// ok=false means there is nothing to fetch: either this node is the writer
// (the data landed through the write path) or no source is known yet.
//
// It is deliberately a cheap, non-probing lookup: this runs on the Raft apply
// path (via onVersionCreated during log replay), so any blocking RPC here would
// stall every later log entry. §8.5's "any node can coordinate a write" is
// built on top of that constraint rather than around it: a writer announces
// itself, and this lookup reads the announcement (see DataSourceRegistry)
// before falling back to the leader.
type SourceResolver func(ctx context.Context, kbID string, versionID int64) (addr string, ok bool, err error)

// VersionPusher replicates one version's records to a peer replica and returns
// that replica's node ID (its acknowledgement). *stratumsync.Pusher satisfies
// it through a small adapter in the node assembly; keeping the interface this
// narrow keeps the DataPlane independent of the transport.
type VersionPusher interface {
	PushVersion(ctx context.Context, targetAddr, kbID string, versionID int64) (acceptorID int64, err error)
}

// IndexShipper ships one version's built index to a peer
// (DataSyncService.PushIndexData). *sync.IndexPusher implements it.
type IndexShipper interface {
	PushIndex(ctx context.Context, targetAddr, kbID string, versionID int64, indexData, sidecarData []byte) error
}

// IndexReader reads a version's persisted index files from this node's disk.
// *index.IndexManagerImpl implements it.
type IndexReader interface {
	ReadIndexFiles(kbID string, versionID int64) (indexData, sidecarData []byte, err error)
}

// WriteConfirmer tells a replica that a version it received did reach quorum,
// so its §7.3 takeover timer can stand down. The same message carries the
// writer's own DataSyncService address (sourceAddr) — §8.5's announcement: the
// replica records it, so a later reader knows where the data is instead of
// having to ask the leader. *sync.ConfirmBroadcaster implements it.
type WriteConfirmer interface {
	ConfirmVersionWrite(ctx context.Context, peerAddr, kbID string, versionID int64, sourceAddr string, empty bool) error
}

// VersionPresenceQuerier asks a peer whether it holds a version's data
// (DataSyncService.VersionPresence). *sync.PresenceChecker implements it.
type VersionPresenceQuerier interface {
	HasVersion(ctx context.Context, peerAddr, kbID string, versionID int64) (bool, error)
}

// VersionDigest computes a version's document-set digest from this node's own
// stores. The §7.3 takeover needs it: a replica announcing "V is durable" must
// announce the same digest the coordinator would have, or followers verifying
// a pulled version against it would reject data that is perfectly fine.
type VersionDigest interface {
	DigestOf(ctx context.Context, kbID string, versionID int64) (string, error)
}

// VersionDataDropper removes one version's physical data on this node. The
// write path reaches the stores through VersionWriteExecutor; dropping gets its
// own narrow seam so the cleanup of §10.6 does not depend on the whole write
// surface.
type VersionDataDropper interface {
	DropVersionStorage(ctx context.Context, kbID string, versionID int64) error
}

// VersionDataCleaner asks one peer to reclaim a version's physical data
// (DataSyncService.DeleteVersionData). *sync.VersionDataCleaner implements it.
type VersionDataCleaner interface {
	DeleteVersionData(ctx context.Context, peerAddr, kbID string, versionID int64, reason string) error
}

// CursorQuerier asks a peer how far its contiguous history reaches, so a node
// that is backfilling can pick a source that actually holds the gap instead of
// always asking the leader — which fails when the leader is the one that is
// behind (Stratum_设计文档v13.md §7.6). *sync.LocalVersionQuerier implements it;
// keeping it an interface keeps the DataPlane independent of the transport.
type CursorQuerier interface {
	LocalVersionOf(ctx context.Context, peerAddr, kbID string) (int64, error)
}

// ReplicaResolver answers "which other replicas should hold this version?".
// Nil means replication is not configured (a single-node deployment, or a
// test), in which case the local write is the whole quorum.
//
// The target list is storage-layer topology, so it is resolved here rather
// than sent down by the control layer (Stratum_设计文档v13.md §7.1).
type ReplicaResolver func(ctx context.Context) ([]string, error)

// IndexStore is the slice of the storage layer's index manager the DataPlane
// uses: search, schedule a build, probe disk, trim by retention. The full
// index.IndexManager satisfies it; keeping it narrow keeps the DataPlane (and
// its tests) independent of the rest of the manager surface.
type IndexStore interface {
	Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error)
	TriggerBuild(ctx context.Context, kbID string, versionID int64) error
	IndexExists(ctx context.Context, kbID string, versionID int64) (bool, error)
	EnforceDiskRetention(ctx context.Context, kbID string, protectedIDs []int64) error
}

// LocalDataPlane is the in-process DataPlane (control-data-separation-design.md
// §4.1, stage ① of §7). It owns everything the control layer must not know:
// whether this node needs a version's data at all, which peer to fetch it
// from, how long to retry a pull, how many replicas must acknowledge a write
// before it counts as durable, and how a build gets scheduled.
type LocalDataPlane struct {
	indexMgr        IndexStore
	puller          VersionPuller
	changesFetcher  VersionChangesFetcher
	versionExists   VersionExistenceChecker
	verify          DataVerifier
	resolve         SourceResolver
	wal             TransactionWAL
	executor        VersionWriteExecutor
	control         ControlPlane
	pusher          VersionPusher
	resolveReplicas ReplicaResolver
	cursorQuerier   CursorQuerier
	dropper         VersionDataDropper
	cleaner         VersionDataCleaner
	presence        VersionPresenceQuerier
	digest          VersionDigest
	confirmer       WriteConfirmer
	indexReader     IndexReader
	indexShipper    IndexShipper

	// selfDataSyncAddr is what this node announces as the holder of the
	// versions it writes (§8.5). See LocalDataPlaneConfig.SelfDataSyncAddr.
	selfDataSyncAddr string

	// limiter queues writes per knowledge base so a stalled version cannot let
	// an unbounded backlog form behind it (Stratum_设计文档v13.md §7.7).
	limiter *writeLimiter

	logger *zap.Logger

	// localVersion is this node's data cursor per knowledge base: the highest
	// version it holds contiguously. 0 means "unknown yet" (a node that has
	// not applied anything for that KB). Guarded by versionMu; it only ever
	// moves forward.
	versionMu    sync.RWMutex
	localVersion map[string]int64

	// takeoverMu guards pendingTakeovers: the §7.3 timers this node started
	// for versions it received via fan-out, keyed like the failure counters.
	takeoverMu       sync.Mutex
	pendingTakeovers map[string]*takeoverWatch

	// cleanupMu guards cleanupQueue: the §10.6 cleanups that did not reach
	// every replica and are waiting for another pass.
	cleanupMu    sync.Mutex
	cleanupQueue map[string]*cleanupTask
}

// LocalDataPlaneConfig wires a LocalDataPlane.
type LocalDataPlaneConfig struct {
	// IndexManager schedules and serves version indexes.
	IndexManager IndexStore
	// Puller copies a version's storage-layer data from a peer.
	Puller VersionPuller
	// Verify checks a completed pull against the committed document-set
	// digest.
	Verify DataVerifier
	// Resolve locates the peer to pull from (see SourceResolver).
	Resolve SourceResolver
	// SelfDataSyncAddr is this node's own DataSyncService address. It is what
	// the writer announces with the §7.3 confirmation, so replicas learn where
	// this version's data lives (§8.5). Empty means "do not announce": the
	// confirmation keeps its pre-§8.5 shape and receivers learn nothing.
	SelfDataSyncAddr string
	// WAL frames the storage layer's write transaction (WriteVersionData).
	WAL TransactionWAL
	// Executor performs the per-version storage writes inside that
	// transaction.
	Executor VersionWriteExecutor
	// Control receives the reports a completed write produces (the
	// document-set digest). Optional: without it the digest is simply not
	// reported, exactly as a missed propose was before.
	Control ControlPlane
	// Pusher replicates a written version to a peer replica.
	Pusher VersionPusher
	// ChangesFetcher fetches the changes a peer recorded over a version range, so
	// a backfill can replay the delta instead of pulling every version in full
	// (v13 §7.5). Optional: without it backfill always transfers full records.
	ChangesFetcher VersionChangesFetcher
	// VersionExistence answers whether a version still exists in the replicated
	// metadata. The data plane cannot tell on its own: a version that was never
	// written, an empty version, and a deleted version all look identical at the
	// storage layer (no rows). Without this, a backfill that receives "success,
	// no records" cannot tell an empty version from a vanished one and would
	// advance its cursor over history it never received (§7.5). Optional: absent
	// means the plane keeps its old behaviour and never concludes "deleted".
	VersionExistence VersionExistenceChecker
	// ResolveReplicas lists the *other* replicas that should hold a written
	// version. Nil (or an empty list) means "no replication": the local write
	// is the whole quorum — the single-node and test default. It doubles as
	// the candidate set for backfill sources.
	ResolveReplicas ReplicaResolver
	// CursorQuerier asks peers how far their history reaches, so a backfill
	// can pick a source that holds the gap (v13 §7.6). Optional: without it
	// the source stays whatever SourceResolver returned.
	CursorQuerier CursorQuerier

	// DataDropper removes a version's physical data on this node, and
	// CleanupBroadcaster asks the candidate replicas to do the same. Both are
	// used by the §10.6 cleanup after a permanent failure; without them
	// DropVersionData reports that it is not wired.
	DataDropper        VersionDataDropper
	CleanupBroadcaster VersionDataCleaner

	// Presence and Digest are what the §7.3 takeover needs to stand up for a
	// version the coordinator never announced: Presence asks peers whether a
	// quorum holds it, Digest produces the same document-set digest the
	// coordinator would have reported. Without both, a replica can receive
	// data but never vouch for it.
	Presence  VersionPresenceQuerier
	Digest    VersionDigest
	Confirmer WriteConfirmer

	// IndexReader and IndexShipper carry a built index to the other replicas
	// (v13 §8.4). Both are optional: without them a build stays local, and
	// every replica builds for itself exactly as before.
	IndexReader  IndexReader
	IndexShipper IndexShipper

	// MaxInFlightWrites caps concurrent in-flight writes per knowledge base
	// (§7.7). Zero means DefaultMaxInFlightWrites. Per-KB values arrive later
	// through SetDurabilityPolicy.
	MaxInFlightWrites int

	// Logger receives progress/retry diagnostics. Optional.
	Logger *zap.Logger
}

// NewLocalDataPlane returns a DataPlane over the given storage-layer pieces.
func NewLocalDataPlane(cfg LocalDataPlaneConfig) *LocalDataPlane {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &LocalDataPlane{
		indexMgr:         cfg.IndexManager,
		puller:           cfg.Puller,
		changesFetcher:   cfg.ChangesFetcher,
		versionExists:    cfg.VersionExistence,
		verify:           cfg.Verify,
		resolve:          cfg.Resolve,
		wal:              cfg.WAL,
		executor:         cfg.Executor,
		control:          cfg.Control,
		pusher:           cfg.Pusher,
		resolveReplicas:  cfg.ResolveReplicas,
		cursorQuerier:    cfg.CursorQuerier,
		dropper:          cfg.DataDropper,
		cleaner:          cfg.CleanupBroadcaster,
		presence:         cfg.Presence,
		digest:           cfg.Digest,
		confirmer:        cfg.Confirmer,
		indexReader:      cfg.IndexReader,
		indexShipper:     cfg.IndexShipper,
		selfDataSyncAddr: cfg.SelfDataSyncAddr,
		limiter:          newWriteLimiter(cfg.MaxInFlightWrites),
		logger:           logger,
		localVersion:     make(map[string]int64),
		pendingTakeovers: make(map[string]*takeoverWatch),
	}
}

var _ DataPlane = (*LocalDataPlane)(nil)

// EnsureIndex brings (kbID, versionID) to a queryable state on this node:
// the version's data is fetched if this node does not hold it yet — the
// "replication lives inside the storage layer" move of
// control-data-separation-design.md §7 — and the index build is then scheduled
// by the puller itself. Idempotent, so re-running it after a crash is safe.
func (d *LocalDataPlane) EnsureIndex(ctx context.Context, kbID string, versionID int64) error {
	// Holding the version's data already is the authoritative "nothing to
	// fetch" fact, and after §8.5 it is the *only* one: the old code read
	// resolve()'s ok=false as "I am the writer", which held only while the
	// writer was also the leader. The contiguous cursor says it directly — it
	// advances exactly when a version's data has landed (write path, pull, or
	// backfill) and never moves backwards.
	if d.localVersionOf(kbID) >= versionID {
		return nil
	}
	addr, ok, err := d.resolve(ctx, kbID, versionID)
	if err != nil {
		return fmt.Errorf("plane: EnsureIndex(%s, %d): resolve data source: %w", kbID, versionID, err)
	}
	if !ok {
		// No source is known yet (nobody has announced this version, and there
		// is no leader to fall back on): nothing to do here. The next apply or
		// reconcile pass tries again, by which time the writer's announcement
		// has usually arrived.
		return nil
	}

	// A node must never hold version V without holding V-1: fill any known gap
	// before pulling versionID itself (Stratum_设计文档v13.md §7.5). Without
	// this a node that missed a version would apply the next one onto an
	// incomplete document set, and "version number comparison implies
	// completeness" would stop holding.
	if err := d.backfillTo(ctx, addr, kbID, versionID); err != nil {
		return err
	}

	if versionID <= 1 {
		// Version 1: created together with the knowledge base and carrying no
		// document changes, so no digest is ever committed for it. Pull once (a
		// no-op stream) and done.
		//
		// This is only the FAST PATH for the case where the id really is 1.
		// Version ids increase globally, so a knowledge base created later gets
		// an initial version numbered far above 1 — that one is caught by the
		// empty-version check inside the pull loop below, which is what makes
		// "a new knowledge base's first query" work at all.
		if err := d.puller.PullVersion(ctx, addr, kbID, versionID); err != nil {
			return err
		}
		d.advanceLocalVersion(kbID, versionID)
		return nil
	}

	// Pull with digest verification, retrying until the data is complete. The
	// writer commits the version's document-set digest only after its storage
	// writes finish, so this node recomputes the digest from its local store
	// after each pull and retries until it matches — closing the race where a
	// pull arrives before the writer's writes land. If the digest never
	// arrives (a missed propose on the writer), a pull that produced data is
	// accepted (the verifier's fallback).
	const pullTimeout = 30 * time.Second
	deadline := time.Now().Add(pullTimeout)
	backoff := 200 * time.Millisecond
	for {
		// Re-checked every attempt: when this node is the coordinator, its own
		// apply can run ahead of its storage writes, so the callback reaches
		// here before the data lands locally (§8.5). The write path advances
		// the cursor as it finishes, and this sees it — instead of pulling its
		// own data back from a peer until the deadline expires.
		if d.localVersionOf(kbID) >= versionID {
			return nil
		}
		// Re-resolved every attempt as well: a coordinator that is not the
		// leader announces itself only once its own writes finish, so the first
		// resolution legitimately answers "the leader", which holds no such
		// data. Holding on to that answer would mean pulling from a pointless
		// source for the whole timeout while the announcement sits unread.
		if fresh, freshOK, freshErr := d.resolve(ctx, kbID, versionID); freshErr != nil {
			d.logger.Warn("plane: resolve data source failed, will retry",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(freshErr))
		} else if freshOK && fresh != addr {
			addr = fresh
		}
		if err := d.puller.PullVersion(ctx, addr, kbID, versionID); err != nil {
			d.logger.Warn("plane: data pull failed, will retry",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		} else if d.verify(ctx, kbID, versionID) {
			d.advanceLocalVersion(kbID, versionID)
			return nil
		} else if empty, emptyErr := d.versionHasNoDocuments(ctx, kbID, versionID); emptyErr == nil && empty {
			// The pull succeeded and the version holds no documents — so the
			// empty set IS its content. Waiting for a digest here means waiting
			// forever: a writer never commits one for a version with no document
			// set. The cursor would stay below a version this node in fact
			// holds, which is what made a freshly created knowledge base answer
			// "local history reaches version 0" to the station's freshness
			// check (§9.3(2)) and refuse the query.
			//
			// Gated on the pull having SUCCEEDED just above: a pull that failed
			// never reaches here, so a broken transfer can never be mistaken for
			// an empty version.
			d.advanceLocalVersion(kbID, versionID)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf(
				"plane: EnsureIndex(%s, %d): version data did not converge within %s",
				kbID, versionID, pullTimeout)
		}
		time.Sleep(backoff)
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// versionHasNoDocuments reports whether this node holds no documents for the
// version — so the empty set IS the version's content.
//
// Why it needs saying out loud: §7.5's pull loop waits for the writer's
// document-set digest to match, and a version created with no changes never gets
// one (there is no document set to hash). Left alone, the loop spins out its
// whole timeout while the node's contiguous cursor stays below a version it in
// fact holds. A freshly created knowledge base is exactly that state, and the
// station's freshness check (§9.3(2)) reads the cursor — so its first query was
// refused with "local history reaches version 0" depending on whether the route
// table had refreshed yet.
//
// Conservative by construction: with no digest source it answers "no", so a gap
// here degrades to the previous behaviour (wait, then fail) instead of advancing
// the cursor on evidence it does not have. Callers must have a successful pull
// behind them before trusting the answer.
func (d *LocalDataPlane) versionHasNoDocuments(ctx context.Context, kbID string, versionID int64) (bool, error) {
	if d.digest == nil {
		return false, nil
	}
	got, err := d.digest.DigestOf(ctx, kbID, versionID)
	if err != nil {
		return false, err
	}
	return got == stratinternalsync.ComputeDocIDSetHash(nil), nil
}

// FetchVersionData brings the version's data here without building its index.
//
// It is what a non-active version needs (§8.6b): the data must be present for
// the version to count as durable, but building an index nobody may ever query
// is wasted work. A later query builds it lazily.
func (d *LocalDataPlane) FetchVersionData(ctx context.Context, kbID string, versionID int64) error {
	// Same fact as in EnsureIndex: this node holding the version already means
	// there is nothing to fetch — which after §8.5 can no longer be inferred
	// from "I am the leader/writer".
	if d.localVersionOf(kbID) >= versionID {
		return nil
	}
	addr, ok, err := d.resolve(ctx, kbID, versionID)
	if err != nil {
		return fmt.Errorf("plane: FetchVersionData(%s, %d): resolve data source: %w", kbID, versionID, err)
	}
	if !ok {
		// No source is known yet: nothing to fetch. A later apply or reconcile
		// pass retries.
		return nil
	}
	if err := d.backfillTo(ctx, addr, kbID, versionID); err != nil {
		return err
	}
	if err := d.puller.PullVersionData(ctx, addr, kbID, versionID); err != nil {
		return err
	}
	d.advanceLocalVersion(kbID, versionID)
	return nil
}

// advanceLocalVersion records that this node now holds versionID contiguously.
// The cursor only moves forward: re-applying an older version (idempotent
// retries, backfill replay) must not move it back.
func (d *LocalDataPlane) advanceLocalVersion(kbID string, versionID int64) {
	d.versionMu.Lock()
	defer d.versionMu.Unlock()
	if versionID > d.localVersion[kbID] {
		d.localVersion[kbID] = versionID
	}
}

// MarkVersionContiguous implements sync.LocalVersionAdvancer: a version whose
// records this node has received — pushed by the coordinator or pulled from a
// peer — is one it holds, and the cursor is how it says so.
//
// The two halves are wired separately on purpose: this plane owns the cursor,
// while the sync handler is what knows the records landed. Without the wiring a
// replica with every record on disk still answered "version 0" to the station's
// freshness check (§9.3(2)) and was refused as stale.
func (d *LocalDataPlane) MarkVersionContiguous(kbID string, versionID int64) {
	d.advanceLocalVersion(kbID, versionID)
}

// LocalVersionOf reports the highest version this node holds contiguously for
// kbID (0 = nothing known yet). It is the storage layer's answer to peers
// looking for a backfill source (Stratum_设计文档v13.md §7.6).
func (d *LocalDataPlane) LocalVersionOf(kbID string) int64 {
	return d.localVersionOf(kbID)
}

// localVersionOf reports the highest version this node holds contiguously for
// kbID (0 = nothing known yet).
func (d *LocalDataPlane) localVersionOf(kbID string) int64 {
	d.versionMu.RLock()
	defer d.versionMu.RUnlock()
	return d.localVersion[kbID]
}

// DataVersionsSnapshot returns a copy of every knowledge base's contiguous
// cursor — the whole view a node reports to the control leader (§7.13.4).
//
// A copy, not the live map: the caller may encode it, retry it, or hold it across
// a tick, and a concurrent write must not mutate what was already reported.
// Knowledge bases with no local data are absent rather than zero, so the receiver
// can tell "reported 0" from "never reported" — a distinction §7.13.4's aggregate
// depends on.
func (d *LocalDataPlane) DataVersionsSnapshot() map[string]int64 {
	d.versionMu.RLock()
	defer d.versionMu.RUnlock()
	snapshot := make(map[string]int64, len(d.localVersion))
	for kbID, versionID := range d.localVersion {
		snapshot[kbID] = versionID
	}
	return snapshot
}

// backfillTo closes a known gap before versionID is applied, by pulling
// (local cursor, versionID) from sourceAddr in order. It is deliberately
// conservative: with no cursor yet (a node that has applied nothing for this
// KB) there is no *known* gap to close, and the versions it learns about are
// pulled whole. The versions it fills are then part of the node's local
// history, so the cursor advances as each one lands.
func (d *LocalDataPlane) backfillTo(ctx context.Context, sourceAddr, kbID string, versionID int64) error {
	local := d.localVersionOf(kbID)
	if local == 0 || local >= versionID-1 {
		return nil
	}
	// Pick a peer that actually holds the gap: the default source (the leader)
	// is precisely what a lagging follower cannot rely on
	// (Stratum_设计文档v13.md §7.6).
	source := d.pickBackfillSource(ctx, kbID, versionID-1, sourceAddr)

	// §7.5: try the delta path first — it transfers what changed instead of every
	// version's full record set. It only applies when the source has a record for
	// EVERY version in the gap: a missing delta cannot be told apart from a version
	// that changed nothing, and replaying the rest would leave a hole behind the
	// cursor.
	if d.changesFetcher != nil {
		err := d.backfillByChanges(ctx, source, kbID, local, versionID)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errBackfillGapIncomplete) {
			// A transport failure says nothing about the data: fall through to full
			// records, which is the path that always works.
			d.logger.Warn("plane: delta backfill failed; using full records",
				zap.String("kb_id", kbID), zap.Int64("from_version", local),
				zap.Int64("to_version", versionID), zap.Error(err))
		}
	}

	// Read the existing-version set ONCE, before the loop: the gap may be long, and
	// asking per version would re-read the whole version list at every step.
	var existing map[int64]bool
	if d.versionExists != nil {
		var err error
		existing, err = d.versionExists.ExistingVersions(ctx, kbID)
		if err != nil {
			// "I could not find out" is not "it is gone": stop.
			return fmt.Errorf("plane: backfill %s: read existing versions: %w", kbID, err)
		}
	}

	for v := local + 1; v < versionID; v++ {
		// Check BEFORE pulling. A version can be genuinely EMPTY (nothing to send)
		// or GONE (deleted, its records no longer on any node), and the storage layer
		// cannot tell them apart — absent rows mean both. The replicated metadata can,
		// so it is asked. Two things follow from asking first: a confirmed-deleted
		// version is never fetched at all (no pointless transfer), and "the gap was
		// handed to the full-state path without pulling the deleted version" becomes
		// something a caller can observe — which is how the end-to-end case proves the
		// fallback actually ran. Advancing the cursor over a vanished version would
		// claim history this node never received, and the cursor's whole meaning is
		// "contiguous up to here" (§7.5).
		if d.versionExists != nil && !existing[v] {
			// §6.4: the version is confirmed gone (a middle version may be
			// deleted), so its records exist nowhere and the gap cannot be filled
			// version by version. Fall back to a full-state transfer.
			return d.transferFullState(ctx, source, kbID, local, versionID, v)
		}
		if err := d.puller.PullVersion(ctx, source, kbID, v); err != nil {
			return fmt.Errorf("plane: backfill %s v%d from %s (local cursor %d): %w", kbID, v, source, local, err)
		}
		d.advanceLocalVersion(kbID, v)
	}
	return nil
}

// transferFullState is §6.4's last resort — the analogue of Raft's InstallSnapshot.
// It runs only when a version in the gap is CONFIRMED deleted by the metadata, never
// on a transport failure or an unreadable topology: those are "I could not find
// out", and the answer to them stays "abort the apply" rather than "silently skip".
// That distinction is the whole reason VersionExistenceChecker exists.
//
// The node transfers ONE whole version's state and moves its cursor straight to it.
// The version it transfers is versionID — the version being applied — and NOT
// versionID-1, which is what "the newest one the gap needs" would suggest. The
// reason is the whole point of this path: the gap is unfillable BECAUSE one of its
// versions was deleted, and versionID-1 may well be that version. Naming a version
// we know is gone as the snapshot would leave this path failing exactly when it is
// needed. versionID, by contrast, is committed and therefore present everywhere the
// apply reached. It can be transferred without dragging the chain behind it because
// a version's docID list is stored whole, not as a delta from its parent.
//
// What this costs, stated plainly: the versions between the old cursor and the
// snapshot are NOT held afterwards, so the cursor now means "contiguous up to here,
// with everything below this point replaced by the snapshot". Reads of one of those
// versions fail — which is where they already stood, since a deleted middle version
// is unreadable to every node.
func (d *LocalDataPlane) transferFullState(ctx context.Context, sourceAddr, kbID string, local, versionID, deletedVersion int64) error {
	snapshot := versionID
	if err := d.puller.PullVersionData(ctx, sourceAddr, kbID, snapshot); err != nil {
		return fmt.Errorf("plane: backfill %s from %s: v%d was deleted (local cursor %d) and the full-state transfer of v%d failed: %w",
			kbID, sourceAddr, deletedVersion, local, snapshot, err)
	}
	d.advanceLocalVersion(kbID, snapshot)
	d.logger.Warn("plane: backfilled via full-state transfer after a deleted version; the skipped versions are not readable locally",
		zap.String("kb_id", kbID), zap.Int64("from_version", local),
		zap.Int64("deleted_version", deletedVersion), zap.Int64("snapshot_version", snapshot))
	return nil
}

// errBackfillGapIncomplete reports that the source has no recorded changes for at
// least one version in the gap, so the delta path cannot be used at all.
var errBackfillGapIncomplete = errors.New("backfill: the source has no record for part of the gap")

// backfillByChanges replays the source's recorded deltas for (local, versionID-1],
// in ascending order. It refuses the WHOLE range when any version is missing: a
// partial replay would leave a hole in this node's history while advancing its
// cursor over it, and the cursor is what makes "version number comparison implies
// completeness" true (§7.5).
func (d *LocalDataPlane) backfillByChanges(ctx context.Context, sourceAddr, kbID string, local, versionID int64) error {
	deltas, err := d.changesFetcher.ChangesInRange(ctx, sourceAddr, kbID, local, versionID-1)
	if err != nil {
		return fmt.Errorf("plane: delta backfill %s (%d,%d] from %s: %w", kbID, local, versionID-1, sourceAddr, err)
	}
	for v := local + 1; v < versionID; v++ {
		if _, ok := deltas[v]; !ok {
			return fmt.Errorf("plane: delta backfill %s (%d,%d] from %s: no record for v%d: %w",
				kbID, local, versionID-1, sourceAddr, v, errBackfillGapIncomplete)
		}
	}

	for v := local + 1; v < versionID; v++ {
		delta := deltas[v]
		if err := d.ApplyBackfillChanges(ctx, kbID, v, delta.ParentVersionID, delta.Changes); err != nil {
			return err
		}
	}
	return nil
}

// pickBackfillSource returns the first candidate whose contiguous history
// already reaches need, falling back to fallback when none does — or when no
// cursor querier or candidate list is configured. A peer that cannot be
// reached is skipped: it cannot serve as a source, but its silence says
// nothing about the others.
func (d *LocalDataPlane) pickBackfillSource(ctx context.Context, kbID string, need int64, fallback string) string {
	if d.cursorQuerier == nil || d.resolveReplicas == nil {
		return fallback
	}
	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		return fallback
	}
	for _, peer := range peers {
		if peer == fallback {
			continue // already the default source
		}
		cursor, err := d.cursorQuerier.LocalVersionOf(ctx, peer, kbID)
		if err != nil {
			continue
		}
		if cursor >= need {
			d.logger.Info("plane: backfill source selected",
				zap.String("peer", peer), zap.String("kb_id", kbID), zap.Int64("peer_cursor", cursor))
			return peer
		}
	}
	return fallback
}

// Search runs a vector query against versionID's index.
func (d *LocalDataPlane) Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error) {
	return d.indexMgr.Search(ctx, kbID, versionID, vector, topK)
}

// WriteVersionData runs the storage layer's write transaction for an
// already-allocated version: BEGIN (persisting the replay input) → the
// per-change storage writes → COMMIT. The control layer only allocated the
// version ID; the transaction that makes the data durable is entirely the
// storage layer's (Stratum_设计文档v13.md §7.12).
func (d *LocalDataPlane) WriteVersionData(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
	// One version is one Saga. Queue behind the writes already in flight for
	// this KB instead of letting them pile up on the storage nodes
	// (Stratum_设计文档v13.md §7.7). The wait honours ctx, so a caller that gives
	// up stops waiting rather than holding a place it no longer wants.
	if err := d.limiter.Acquire(ctx, kbID); err != nil {
		return err
	}
	defer d.limiter.Release(kbID)

	docIDs, err := d.writeLocalTransaction(ctx, kbID, versionID, parentVersionID, changes)
	if err != nil {
		// The storage layer reports that an attempt failed; the control layer
		// owns the terminal verdict (Stratum_设计文档v13.md §10.1).
		d.reportFailure(ctx, kbID, versionID, classifyLocalWriteFailure(err), fmt.Sprintf("local write failed: %v", err))
		return err
	}
	// Replicate to the other replicas and require a quorum before reporting
	// the version durable (v13 §7.1/§7.2). The version is only "durable" once
	// enough replicas hold it — the writer's own copy is one acknowledgement.
	if err := d.fanOut(ctx, kbID, versionID); err != nil {
		// A failed replication is transient by definition: peers come back.
		d.reportFailure(ctx, kbID, versionID, types.FailureTransient, fmt.Sprintf("replication failed: %v", err))
		return err
	}
	d.reportAndSchedule(ctx, kbID, versionID, docIDs)
	// len(docIDs) == 0 is the whole reason the announcement carries a flag: a
	// version with no documents is never fanned out (fanOut above sends
	// nothing), so replicas that did NOT coordinate it have no other way to
	// learn it exists — and their cursors would stay behind it, which the
	// station reads as "stale" (§9.3(2)).
	d.broadcastConfirmation(kbID, versionID, len(docIDs) == 0)
	return nil
}

// PushIndexToReplicas ships a locally built index to the candidate replicas, so
// they load the artifact instead of building their own — "建一次、分发 N 份"
// (Stratum_设计文档v13.md §8.4).
//
// Best effort per replica, and it never fails the build. A replica that cannot
// be reached falls back to building for itself, which is exactly the behaviour
// the cluster had before distribution existed: distribution is an optimisation
// over that baseline, not a new way for a version to become unservable.
func (d *LocalDataPlane) PushIndexToReplicas(ctx context.Context, kbID string, versionID int64) error {
	if d.indexReader == nil || d.indexShipper == nil || d.resolveReplicas == nil {
		return nil // distribution not wired; each replica builds its own
	}
	indexData, sidecarData, err := d.indexReader.ReadIndexFiles(kbID, versionID)
	if err != nil {
		return fmt.Errorf("plane: PushIndexToReplicas(%s v%d): read local index: %w", kbID, versionID, err)
	}
	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		return fmt.Errorf("plane: PushIndexToReplicas: resolve replicas: %w", err)
	}
	for _, peer := range peers {
		if err := d.indexShipper.PushIndex(ctx, peer, kbID, versionID, indexData, sidecarData); err != nil {
			d.logger.Warn("plane: index distribution failed; that replica will build its own",
				zap.String("peer", peer), zap.String("kb_id", kbID),
				zap.Int64("version_id", versionID), zap.Error(err))
		}
	}
	return nil
}

// broadcastConfirmation tells the candidate replicas that the version reached
// quorum, so any §7.3 takeover timer they started can stand down.
//
// Best effort and asynchronous: the write is already durable and reported, and
// a replica that misses this message only checks for itself later — an extra
// announcement, never a missing one. That asymmetry is why the caller is not
// made to wait on it.
//
// It goes to *every* candidate rather than only the replicas that acknowledged,
// for the same reason the cleanup broadcast does (§10.6): the coordinator does
// not track which push succeeded, and a stray confirmation to a replica that
// never received the data is harmless.
func (d *LocalDataPlane) broadcastConfirmation(kbID string, versionID int64, empty bool) {
	if d.confirmer == nil || d.resolveReplicas == nil {
		return
	}
	go func() {
		// A fresh context: the request that ran the write is over by now.
		ctx, cancel := context.WithTimeout(context.Background(), takeoverTimeout*5)
		defer cancel()
		peers, err := d.resolveReplicas(ctx)
		if err != nil {
			return
		}
		for _, peer := range peers {
			// The confirmation doubles as the §8.5 announcement: it carries this
			// node's own address so the peer records where the version's data
			// is (see DataSourceRegistry). Empty means "announce nothing".
			//
			// empty tells the peer the version has no documents, so it can move
			// its own cursor over it without fetching anything — the only cue
			// such a version produces, since it is never fanned out.
			if err := d.confirmer.ConfirmVersionWrite(ctx, peer, kbID, versionID, d.selfDataSyncAddr, empty); err != nil {
				d.logger.Warn("plane: confirm version write",
					zap.String("peer", peer), zap.String("kb_id", kbID),
					zap.Int64("version_id", versionID), zap.Error(err))
			}
		}
	}()
}

// classifyLocalWriteFailure decides how the control layer should treat a failed
// local write (Stratum_设计文档v13.md §10.1).
//
// Only the cases that no amount of retrying can fix are fatal: a knowledge base
// that is gone, or input the state machine will keep rejecting. Everything
// else — a full disk, an unavailable dependency, a timeout — is worth another
// attempt, possibly on another node.
func classifyLocalWriteFailure(err error) types.FailureClass {
	switch {
	case errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound),
		errors.Is(err, stratumerrors.ErrKnowledgeBaseDeleted),
		errors.Is(err, stratumerrors.ErrInvalidArgument),
		errors.Is(err, stratumerrors.ErrInvalidParentVersion),
		errors.Is(err, stratumerrors.ErrVersionNotFound):
		return types.FailureFatalGlobal
	default:
		return types.FailureTransient
	}
}

// reportFailure tells the control layer that an attempt at making the version
// durable failed. A report error is logged rather than returned: the caller is
// already handling a failure, and losing a report costs retries — not
// correctness, since the verdict is only ever "stop retrying automatically".
func (d *LocalDataPlane) reportFailure(ctx context.Context, kbID string, versionID int64, class types.FailureClass, detail string) {
	if d.control == nil {
		return
	}
	terminal, err := d.control.ReportVersionFailure(ctx, kbID, versionID, class, detail)
	if err != nil {
		d.logger.Warn("plane: report version failure",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}
	if !terminal {
		return
	}
	// The verdict just landed: reclaim whatever physical data made it to disk,
	// including on replicas whose acknowledgement was lost
	// (Stratum_设计文档v13.md §10.6).
	if err := d.DropVersionData(ctx, kbID, versionID); err != nil {
		d.logger.Warn("plane: cleanup after a permanent failure",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

// writeLocalTransaction frames and runs this node's half of the write:
// BEGIN (persisting the replay input) → the per-change storage writes →
// COMMIT. Returns the version's document-ID set.
func (d *LocalDataPlane) writeLocalTransaction(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) ([]string, error) {
	if d.wal == nil || d.executor == nil {
		return nil, fmt.Errorf("plane: WriteVersionData: the storage write transaction is not configured")
	}
	if err := d.wal.WriteBegin(ctx, kbID, parentVersionID, changes); err != nil {
		return nil, fmt.Errorf("plane: WriteVersionData: WAL.WriteBegin: %w", err)
	}
	docIDs, err := d.executor.WriteVersionStorage(ctx, kbID, parentVersionID, versionID, changes)
	if err != nil {
		return nil, err
	}
	if err := d.wal.WriteCommit(ctx, versionID); err != nil {
		return nil, fmt.Errorf("plane: WriteVersionData: WAL.WriteCommit: %w", err)
	}
	d.advanceLocalVersion(kbID, versionID)
	return docIDs, nil
}

// fanOut hands the version to the other replicas concurrently and fails unless
// a majority acknowledged. Counting this node's own completed write as the
// first acknowledgement, quorum is a majority of (1 + targets) — see
// QuorumSize.
//
// A failed or missing replica does not fail the write on its own: only falling
// short of quorum does, and the error names the shortfall so the caller's Saga
// can decide (retry, or hand the version to §10.1's permanent-failure path).
// ApplyBackfillChanges writes one version's data on a node that is catching up
// (§7.5): the changes are replayed from a peer's WAL, so this node ends up with
// the same records the original writer produced.
//
// It deliberately stops at the local transaction, skipping everything
// WriteVersionData layers on top:
//
//   - no fan-out: the replicas it would push to are the very peers this node is
//     learning from, and they already hold the version. A node catching up does
//     not notify the nodes it is copying from.
//   - no durable report (digest): this version's durability was settled long ago —
//     that is precisely why the peer's WAL range was still readable. Reporting it
//     again confirms a fact nobody is waiting on.
//   - no index scheduling: historical versions build lazily or never (§8.6b), so
//     an unconditional build attempt spends work on something no query asks for.
//     If the version happens to be the active one, the very apply that triggered
//     this backfill schedules its build right after.
//   - no failure report: reportFailure is the *coordinator's* channel and can end
//     in §10.1's terminal verdict. A node that merely cannot fetch someone else's
//     history must not be able to declare that version permanently failed.
func (d *LocalDataPlane) ApplyBackfillChanges(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
	// writeLocalTransaction advances the cursor itself, so there is nothing else to
	// do here: the version counts as held once its data is in the stores.
	if _, err := d.writeLocalTransaction(ctx, kbID, versionID, parentVersionID, changes); err != nil {
		return fmt.Errorf("plane: backfill apply %s v%d: %w", kbID, versionID, err)
	}
	return nil
}

func (d *LocalDataPlane) fanOut(ctx context.Context, kbID string, versionID int64) error {
	if d.pusher == nil || d.resolveReplicas == nil {
		return nil // replication not configured: the local write is the quorum
	}
	targets, err := d.resolveReplicas(ctx)
	if err != nil {
		return fmt.Errorf("plane: fan-out for version %d: resolve replicas: %w", versionID, err)
	}
	if len(targets) == 0 {
		// 解析不出任何副本。这里从前静默返回成功,而"没有副本"在 quorum 判定上
		// 与"副本就是我自己"是同一件事——于是这一版被当成已复制完成,控制层据此
		// 推进,其余副本永远拿不到它。
		//
		// 单层集群里协调者就是受理者,它的副本列表非空,这条路径不会走到;派发
		// 真正生效之后(§7.13.2)就会:写入落在一台远程节点上,而那台节点解析不出
		// 副本列表时,它会把单副本当成 quorum。
		d.logger.Warn("plane: fan-out found no replica targets; treating this node's own copy as the quorum",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
		return nil
	}

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		acked  = 1 // this node's own local write
		needed = QuorumSize(1 + len(targets))
		total  = 1 + len(targets)
	)
	for _, target := range targets {
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			if _, err := d.pusher.PushVersion(ctx, target, kbID, versionID); err != nil {
				d.logger.Warn("plane: replica push failed",
					zap.String("replica", target), zap.String("kb_id", kbID),
					zap.Int64("version_id", versionID), zap.Error(err))
				return
			}
			mu.Lock()
			acked++
			mu.Unlock()
		}(target)
	}
	wg.Wait()

	if acked < needed {
		return fmt.Errorf(
			"plane: version %d of %s reached %d/%d acknowledgements, below the quorum of %d",
			versionID, kbID, acked, total, needed)
	}
	return nil
}

// QuorumSize is the durable threshold for n replicas (Stratum_设计文档v13.md
// §7.1): ⌈(n+1)/2⌉, a bare majority, so any two quorums intersect.
func QuorumSize(n int) int {
	if n <= 0 {
		return 0
	}
	return (n+1)/2 + (n+1)%2
}

// ResumeVersionWrite re-runs the storage writes and commits without writing a
// second BEGIN: the transaction's BEGIN record is already durable (crash
// recovery drove this path).
func (d *LocalDataPlane) ResumeVersionWrite(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
	if d.wal == nil || d.executor == nil {
		return fmt.Errorf("plane: ResumeVersionWrite: the storage write transaction is not configured")
	}
	docIDs, err := d.executor.WriteVersionStorage(ctx, kbID, parentVersionID, versionID, changes)
	if err != nil {
		return err
	}
	if err := d.wal.WriteCommit(ctx, versionID); err != nil {
		return fmt.Errorf("plane: ResumeVersionWrite: WAL.WriteCommit: %w", err)
	}
	d.advanceLocalVersion(kbID, versionID)
	d.reportAndSchedule(ctx, kbID, versionID, docIDs)
	return nil
}

// reportAndSchedule finishes a completed write transaction: it reports the
// version's document-set digest up (v1 §4.2 ReportDataDurable — followers use
// it to verify their data pulls) and schedules the index build. Both stay
// best-effort, exactly as they were inside the coordinator: a missed digest
// only costs followers a best-effort pull, and a failed build can be retried.
func (d *LocalDataPlane) reportAndSchedule(ctx context.Context, kbID string, versionID int64, docIDs []string) {
	if d.control != nil {
		_ = d.control.ReportDataDurable(ctx, kbID, versionID, stratinternalsync.ComputeDocIDSetHash(docIDs))
	}
	if d.indexMgr != nil {
		_ = d.indexMgr.TriggerBuild(ctx, kbID, versionID)
	}
}

// DropVersionData: see WriteVersionData.
// DropVersionData removes versionID's physical data — on this node and on
// every candidate replica.
//
// It deliberately covers *all* candidates rather than only the replicas that
// acknowledged the write: when the control layer declares a version permanently
// failed it does not know which replicas actually landed the data, so a no-op
// on a replica that never received it is far cheaper than leaking the data on
// the one that did (Stratum_设计文档v13.md §10.6).
//
// The cursor is advanced past the version: a permanently failed version will
// never have data, so treating it as "known" is what keeps the gap from
// looking like something a later backfill should fetch.
func (d *LocalDataPlane) DropVersionData(ctx context.Context, kbID string, versionID int64) error {
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if d.cleaner != nil && d.resolveReplicas != nil {
		peers, err := d.resolveReplicas(ctx)
		if err != nil {
			record(fmt.Errorf("plane: DropVersionData: resolve replicas: %w", err))
		} else {
			for _, peer := range peers {
				if err := d.cleaner.DeleteVersionData(ctx, peer, kbID, versionID, "permanently failed version"); err != nil {
					record(fmt.Errorf("plane: DropVersionData: broadcast to %s: %w", peer, err))
				}
			}
		}
	}

	if d.dropper != nil {
		if err := d.dropper.DropVersionStorage(ctx, kbID, versionID); err != nil {
			record(fmt.Errorf("plane: DropVersionData: local drop: %w", err))
		}
	} else if d.cleaner == nil {
		return fmt.Errorf("plane: DropVersionData: not wired (no local dropper and no broadcaster)")
	}
	d.advanceLocalVersion(kbID, versionID)
	if firstErr != nil {
		// §10.6: a cleanup that did not reach everyone deserves another pass.
		// The version is terminal either way, but its physical data is not
		// reclaimed yet.
		d.scheduleCleanupRetry(kbID, versionID)
	}
	return firstErr
}

// SetDurabilityPolicy records the declarative target for kbID.
//
// The failure budget it carries is passed straight on to the control layer,
// which owns the terminal verdict (Stratum_设计文档v13.md §10.1): the policy is
// declared at the storage-layer boundary but enforced where the decision lives.
// The replication target itself stays advisory for now — the replica set comes
// from the cluster's topology (§7.1/§7.2), not from this value.
func (d *LocalDataPlane) SetDurabilityPolicy(ctx context.Context, kbID string, policy DurabilityPolicy) error {
	// §7.7: the in-flight cap is enforced here, where the backlog would form.
	d.limiter.SetLimit(kbID, policy.MaxInFlightWrites)

	if d.control == nil || policy.MaxFailures <= 0 {
		return nil
	}
	if err := d.control.SetFailureBudget(ctx, kbID, policy.MaxFailures); err != nil {
		return fmt.Errorf("plane: SetDurabilityPolicy(%s): %w", kbID, err)
	}
	return nil
}

// TransactionWAL frames the storage layer's write transaction. *wal.FileWAL
// satisfies it; keeping it narrow keeps the DataPlane independent of the rest
// of the WAL surface (deletes, recovery, replay counters).
type TransactionWAL interface {
	WriteBegin(ctx context.Context, kbID string, parentVersionID int64, changes []types.DocChange) error
	WriteCommit(ctx context.Context, versionID int64) error
}

// VersionWriteExecutor performs one version's storage-layer writes: split,
// embed, chunk/docstore/versiondoc writes and the version document bloom. It
// deliberately does NOT frame the WAL transaction (BEGIN/COMMIT) — that
// framing is what makes the transaction the storage layer's own, and it lives
// in WriteVersionData.
type VersionWriteExecutor interface {
	WriteVersionStorage(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) ([]string, error)
}

// MetadataLister is the read-only view of replicated metadata the storage
// layer needs in order to reconcile itself against the control layer.
// *raft.RaftNode satisfies it; the storage layer never proposes through it.
type MetadataLister interface {
	ListKnowledgeBases(ctx context.Context) ([]types.KnowledgeBaseMeta, error)
	ListVersions(ctx context.Context, kbID string) ([]types.VersionMeta, error)
}

// EnforceRetention applies the disk retention policy once at startup: for
// every knowledge base, drop on-disk index files older than the newest
// IndexRetentionCount versions, shielding the active version. A no-op when
// retention is unconfigured (the manager decides that). The same policy also
// runs after every successful build; this is the startup pass, and it must
// precede ReconcileIndexes so the reconcile sees post-retention disk facts.
func (d *LocalDataPlane) EnforceRetention(ctx context.Context, meta MetadataLister) error {
	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		return fmt.Errorf("plane: EnforceRetention: list knowledge bases: %w", err)
	}
	for _, kb := range kbs {
		if err := d.indexMgr.EnforceDiskRetention(ctx, kb.KBID, []int64{kb.ActiveVersionID}); err != nil {
			d.logger.Warn("plane: retention: EnforceDiskRetention failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
		}
	}
	return nil
}

// takeoverTimeout is how long a replica waits for the coordinator's "quorum
// reached" signal before checking for itself (Stratum_设计文档v13.md §7.3).
// Short by design: replicas sit on the same LAN as the coordinator, and the
// cost of waiting too long is a version stranded in PENDING. Like the other
// §10.4 numbers it is a placeholder.
//
// A var so tests can shorten it.
var takeoverTimeout = 200 * time.Millisecond

// takeoverWatch is one pending §7.3 timer.
type takeoverWatch struct {
	timer *time.Timer
}

// WatchVersionWrite starts the §7.3 takeover timer for a version this node has
// just received via fan-out.
//
// This is the replica's half of coordinator-crash recovery: the data is already
// here, so the only thing that can go missing is the announcement that a quorum
// holds it. If that announcement never arrives, the replica checks for itself
// rather than letting the version rot in PENDING.
func (d *LocalDataPlane) WatchVersionWrite(kbID string, versionID int64) {
	if d.control == nil || d.resolveReplicas == nil || d.presence == nil || d.digest == nil {
		return // takeover not wired; the client-retry path still covers this
	}
	key := failureKey(kbID, versionID)
	d.takeoverMu.Lock()
	if old, ok := d.pendingTakeovers[key]; ok {
		old.timer.Stop()
	}
	d.pendingTakeovers[key] = &takeoverWatch{
		timer: time.AfterFunc(takeoverTimeout, func() { d.attemptTakeover(kbID, versionID) }),
	}
	d.takeoverMu.Unlock()
}

// ConfirmVersionWrite cancels the takeover timer: the coordinator reported that
// the version reached quorum, so there is nothing left for a replica to
// announce (Stratum_设计文档v13.md §7.3).
func (d *LocalDataPlane) ConfirmVersionWrite(kbID string, versionID int64) {
	key := failureKey(kbID, versionID)
	d.takeoverMu.Lock()
	if w, ok := d.pendingTakeovers[key]; ok {
		w.timer.Stop()
		delete(d.pendingTakeovers, key)
	}
	d.takeoverMu.Unlock()
}

// attemptTakeover is §7.3's "no coordinator signal arrived" path: count how
// many replicas hold the version and, if that is a quorum, announce it.
//
// It replaces only the *announcement*, never the transfer: the data is already
// on disk here and on whichever peers answer. Announcing is safe to repeat
// across replicas — the proposal is idempotent and the state machine drops
// reports for versions that are already settled (§10.6) — which is exactly why
// no replica has to be elected to do it.
func (d *LocalDataPlane) attemptTakeover(kbID string, versionID int64) {
	key := failureKey(kbID, versionID)
	d.takeoverMu.Lock()
	_, stillPending := d.pendingTakeovers[key]
	delete(d.pendingTakeovers, key)
	d.takeoverMu.Unlock()
	if !stillPending {
		return // confirmed in the meantime
	}

	// A fresh context: the request that delivered the data is long gone by
	// now, and the announcement is this node's own business.
	ctx, cancel := context.WithTimeout(context.Background(), takeoverTimeout*10)
	defer cancel()

	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		d.logger.Warn("plane: takeover: resolve replicas",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}

	// This node received the push, so its own copy counts — the same "self is
	// one acknowledgement" rule fan-out uses.
	acks := 1
	for _, peer := range peers {
		holds, err := d.presence.HasVersion(ctx, peer, kbID, versionID)
		if err != nil || !holds {
			// Unreachable and not-holding both go uncounted: an unreachable
			// peer is not evidence either way, and the quorum test below is
			// what decides whether the remaining answers suffice.
			continue
		}
		acks++
	}
	quorum := QuorumSize(len(peers) + 1)
	if acks < quorum {
		// Not enough evidence to announce anything. Better to let the version
		// run out its retry budget and reach §10.1's terminal state than to
		// claim durability on a minority.
		d.logger.Info("plane: takeover: quorum not reached, leaving the version to its retry budget",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.Int("acks", acks), zap.Int("quorum", quorum))
		return
	}

	digest, err := d.digest.DigestOf(ctx, kbID, versionID)
	if err != nil {
		d.logger.Warn("plane: takeover: compute digest",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}
	if err := d.control.ReportDataDurable(ctx, kbID, versionID, digest); err != nil {
		d.logger.Warn("plane: takeover: report durable",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}
	d.logger.Info("plane: takeover: announced a version the coordinator never announced",
		zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Int("acks", acks))
}

// SafeDurableVersion reports the highest version this node may claim durable
// for kbID after a restart: the largest version a quorum's worth of cursors
// still reaches (Stratum_设计文档v13.md §7.8).
//
// The contiguous cursor lives in memory, so a restarted node starts at 0 and
// cannot tell how far behind it is. Asking the peers is the only way to find
// out, and requiring a quorum's worth of answers is what keeps the claim safe:
// the answer can never overstate progress, because a version only counts if
// enough replicas still report holding it.
//
// ok=false means "there is no replica set to ask" (single-node deployment, or
// nothing wired), in which case the caller falls back to its local view —
// there is nobody to disagree with. An error means a quorum could not be
// assembled: "no answer" must never be mistaken for "no data".
func (d *LocalDataPlane) SafeDurableVersion(ctx context.Context, kbID string) (int64, bool, error) {
	if d.cursorQuerier == nil || d.resolveReplicas == nil {
		return 0, false, nil
	}
	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("plane: SafeDurableVersion: resolve replicas: %w", err)
	}
	if len(peers) == 0 {
		return 0, false, nil
	}

	// This node always counts, even at cursor 0: its own (possibly lost)
	// progress is part of the picture, not an absence of one.
	reports := []int64{d.localVersionOf(kbID)}
	for _, peer := range peers {
		cursor, err := d.cursorQuerier.LocalVersionOf(ctx, peer, kbID)
		if err != nil {
			// An unreachable peer is not evidence of anything. It is skipped,
			// and the quorum requirement below decides whether what is left
			// suffices.
			continue
		}
		reports = append(reports, cursor)
	}

	quorum := QuorumSize(len(peers) + 1)
	if len(reports) < quorum {
		return 0, false, fmt.Errorf("plane: SafeDurableVersion: %s: only %d of a required %d cursors arrived",
			kbID, len(reports), quorum)
	}
	// The largest version a quorum's worth of reports still reaches: sorted
	// descending, that is the quorum-th report.
	sort.Slice(reports, func(i, j int) bool { return reports[i] > reports[j] })
	return reports[quorum-1], true, nil
}

// ReconcileIndexes is the storage layer's half of the startup reconcile — the
// "block report" of control-data-separation-design.md §5.3. It walks the
// versions the control layer knows about, compares them against the indexes
// actually present on this node's disk, (re)schedules builds for the ones
// missing inside the retention window, and returns the versions whose index is
// durable. The caller hands that set to ControlPlane.ReportEpoch, which is
// what promotes a lost READY proposal.
//
// Storage-layer policy (the retention window, rebuild-on-demand) stays in
// here; every control-layer state change stays on the other side of the
// contract.
func (d *LocalDataPlane) ReconcileIndexes(ctx context.Context, meta MetadataLister, retentionCount int) ([]VersionRef, error) {
	kbs, err := meta.ListKnowledgeBases(ctx)
	if err != nil {
		return nil, fmt.Errorf("plane: ReconcileIndexes: list knowledge bases: %w", err)
	}

	var durable []VersionRef
	for _, kb := range kbs {
		versions, err := meta.ListVersions(ctx, kb.KBID)
		if err != nil {
			d.logger.Warn("plane: reconcile: ListVersions failed",
				zap.String("kb_id", kb.KBID), zap.Error(err))
			continue
		}

		// retentionCutoff is the smallest version ID inside the retention
		// window; versions strictly below it are eligible to be dropped by
		// the retention policy and are skipped for rebuild.
		retentionCutoff := int64(-1)
		if retentionCount > 0 && len(versions) > retentionCount {
			ids := make([]int64, len(versions))
			for i, v := range versions {
				ids[i] = v.VersionID
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			retentionCutoff = ids[len(ids)-retentionCount]
		}

		for _, v := range versions {
			// FAILED is retryable — RebuildIndex may trigger another attempt — but
			// FAILED_PERMANENT is the control layer's terminal verdict (§10.1):
			// nothing re-triggers it, and its data may already have been reclaimed
			// (§10.6). Rebuilding here would contradict the recorded verdict and,
			// with the data gone, fail on every sweep.
			if v.IndexStatus == types.IndexStatusFailed || v.IndexStatus == types.IndexStatusFailedPermanent {
				continue
			}
			exists, err := d.indexMgr.IndexExists(ctx, kb.KBID, v.VersionID)
			if err != nil {
				d.logger.Warn("plane: reconcile: IndexExists failed",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID), zap.Error(err))
				continue
			}
			switch {
			case exists:
				// The index is on disk: the version is durable, and the
				// control layer promotes a lost READY proposal from this
				// report.
				durable = append(durable, VersionRef{KBID: kb.KBID, VersionID: v.VersionID})
			case retentionCount > 0 && v.VersionID < retentionCutoff:
				// Intentionally dropped by the disk retention policy
				// (PENDING or READY, outside the newest retentionCount
				// versions): leave it absent and rebuild on demand.
				d.logger.Info("plane: reconcile: skipping rebuild of retention-dropped index",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID))
			case v.IndexStatus == types.IndexStatusPending || v.VersionID == kb.ActiveVersionID:
				// Worth a head start, and only these two:
				//
				//   PENDING — the writer is waiting for this very build; without it
				//   the write stalls instead of merely being slower.
				//
				//   the active version — every query for this knowledge base lands on
				//   it, so its build is needed immediately.
				//
				// TriggerBuild is idempotent, and the build re-persists and
				// re-reports the status.
				d.logger.Info("plane: reconcile: (re)building missing index",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID),
					zap.String("status", v.IndexStatus.String()))
				if err := d.indexMgr.TriggerBuild(ctx, kb.KBID, v.VersionID); err != nil {
					d.logger.Warn("plane: reconcile: TriggerBuild failed",
						zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID), zap.Error(err))
				}
			default:
				// READY, artifact missing, inside the window, and NOT the active
				// version: leave it absent.
				//
				// The retention branch above gives the same answer for the same
				// reason — EnsureIndex builds on demand, so an absent artifact costs a
				// slow first query and nothing else. Rebuilding it here instead is how
				// a restart over a populated volume turns into a rebuild storm: one
				// eager build per historical version, every one of them competing with
				// the writes that actually need the CPU, disk and vecstore. Measured: a
				// fresh write waited 601s for READY while 41 historical artifacts were
				// rebuilt ahead of it.
				d.logger.Info("plane: reconcile: leaving a non-active missing index absent",
					zap.String("kb_id", kb.KBID), zap.Int64("version_id", v.VersionID),
					zap.String("status", v.IndexStatus.String()))
			}
		}
	}
	return durable, nil
}
