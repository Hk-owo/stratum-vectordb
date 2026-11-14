package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"stratum/internal/authmeta"
	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/coordinator"
	"stratum/internal/docstore"
	"stratum/internal/embed"
	"stratum/internal/index"
	"stratum/internal/nodestate"
	"stratum/internal/plane"
	"stratum/internal/raft"
	"stratum/internal/splitter"
	stratumsync "stratum/internal/sync"
	"stratum/internal/tombstone"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"
)

// nodeAssembly is what main() is assembling: the node's halves, in the order
// they have to be built, and the handles one stage hands to the next.
//
// The type exists because the stages genuinely share state in both directions —
// the write coordinator needs a dispatcher, the dispatcher needs the data plane,
// and the data plane needs the coordinator (§7.13.2) — so a struct with a
// well-defined build order is the honest shape. It is deliberately NOT a
// service locator: every field is written once by the stage that builds it, and
// the stage methods below read only what earlier stages produced.
type nodeAssembly struct {
	cfg    appConfig
	logger *zap.Logger
	ctx    context.Context

	// The metadata channel, and Raft's own log (which every node keeps).
	walImpl  *wal.FileWAL
	rn       raft.RaftNode
	raftNode *raft.RaftNodeImpl

	storageLocal bool
	nodeState    *nodestate.Machine
	topology     *clusterTopology
	controlPlane *plane.LocalControlPlane

	// Local storage. All nil on a control node — that is the role split, not a
	// latent panic: the paths that would dereference them are the ones the split
	// removed.
	ds                *docstore.PebbleDocStore
	cdm               *chunkdoc.PebbleChunkDocMapper
	vd                *versiondoc.PebbleVersionDocList
	chunkStore        *chunkstore.VecstoreChunkStore
	vBloomStore       *bloom.VersionBloomStore
	chunkBloom        *bloom.BitsAndBloomsFilter
	chunkSplitter     splitter.ChunkSplitter
	indexMgr          *index.IndexManagerImpl
	embedClient       *embed.HTTPEmbedClient
	versionTombstones *tombstone.PebbleStore

	// Orchestration (built by wireCoordinators).
	gcImpl             *coordinator.ChunkGarbageCollectorImpl
	writeCoord         *coordinator.WriteCoordinatorImpl
	dataSources        *plane.DataSourceRegistry
	deleteCoord        *coordinator.DeleteCoordinatorImpl
	deleteVersionCoord *coordinator.DeleteVersionCoordinatorImpl

	// The node-to-node halves and the data plane (built by wireDataPlane).
	syncLeader      *stratumsync.LeaderHandler
	syncFollower    *stratumsync.Follower
	syncPusher      *stratumsync.Pusher
	holdersCache    *plane.HoldersCache
	dataPlane       *plane.LocalDataPlane
	writeDispatcher *plane.CoordinatorDispatcher

	// The two late-bound closures the coordinators were built with: the write
	// dispatch (needs the data plane) and the replica topology (needs nothing,
	// but is consumed by both stages).
	dispatchVersionWrite func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error
	resolveReplicaAddrs  func(ctx context.Context) ([]string, error)

	// liveness is the metadata channel's "has this version been deleted"
	// answer, or nil when it does not provide one.
	liveness plane.VersionLivenessLister
}

// wireCoordinators builds the three orchestrators (write, KB delete, version
// delete) and the chunk collector they share, plus the data-source registry the
// deletion flows announce into.
//
// The order below is forced: the write coordinator dispatches through a closure
// that is only assigned once the data plane exists (wireDataPlane), and both
// droppers resolve the data plane lazily for the same reason — a broadcast needs
// a plane that does not exist yet at construction time.
func (a *nodeAssembly) wireCoordinators() {
	cfg, logger := a.cfg, a.logger
	rn, walImpl := a.rn, a.walImpl
	ds, cdm, vd := a.ds, a.cdm, a.vd
	chunkStore, vBloomStore := a.chunkStore, a.vBloomStore
	chunkBloom, chunkSplitter := a.chunkBloom, a.chunkSplitter
	indexMgr, embedClient := a.indexMgr, a.embedClient
	versionTombstones, controlPlane := a.versionTombstones, a.controlPlane

	kbLocks := coordinator.NewKBLockSet()

	// The storage handles above are declared as concrete pointer types
	// (var ds *docstore.PebbleDocStore, ...) because that is what the
	// constructors produce, but the config fields are interfaces. Assigning a nil
	// pointer straight through — `DocStore: ds` — yields an interface that is NOT
	// nil while its receiver IS, so every `if cfg.DocStore != nil` guard passes
	// and the first method call dereferences nil.
	//
	// Not hypothetical: on a control-role node, which has no local storage at
	// all, deleting a knowledge base broadcast the drop to it and
	// DropVersionStorage panicked inside Pebble, killing the process mid-cleanup
	// and leaving the KB stuck in KB_STATUS_DELETING (once per delete, on every
	// control node).
	//
	// Converting here keeps "not wired" meaning exactly one thing — a nil
	// interface — which is what those guards can actually see.
	var (
		docStoreIface   docstore.DocStore
		versionDocIface versiondoc.VersionDocList
		chunkStoreIface chunkstore.ChunkStore
		chunkDocIface   chunkdoc.ChunkDocMapper
		indexMgrIface   index.IndexManager
	)
	if ds != nil {
		docStoreIface = ds
	}
	if vd != nil {
		versionDocIface = vd
	}
	if chunkStore != nil {
		chunkStoreIface = chunkStore
	}
	if cdm != nil {
		chunkDocIface = cdm
	}
	if indexMgr != nil {
		indexMgrIface = indexMgr
	}

	// --- Orphan-chunk garbage collector ---
	// The sweep is a storage-layer task (it walks this node's chunk mappings), and
	// its PRIMARY trigger is now the delete flow rather than the timer: a chunk can
	// only become an orphan when the version that referenced it is deleted, so
	// LocalVersionDropper.DropVersion and WriteCoordinatorImpl.DropVersionStorage
	// call ReclaimVersionChunks while the version's document set still names the
	// candidates. The periodic sweep stays as a safety net (default: hourly) for
	// leftovers that no deletion event can name any more.
	//
	// Built here, ahead of the coordinators, so both delete paths can be handed the
	// same value; Run starts further down on the nodes that hold storage.
	gcImpl := coordinator.NewChunkGarbageCollectorImpl(coordinator.ChunkGarbageCollectorConfig{
		SweepIntervalSec: cfg.GCSweepIntervalSec,
		Locks:            kbLocks, // same set as WriteCoordinatorConfig.Locks
		RaftNode:         rn,
		ChunkDocMapper:   chunkDocIface,
		DocStore:         docStoreIface,
		ChunkStore:       chunkStoreIface,
		VersionDocList:   versionDocIface,
	})
	gcImpl.SetLogger(logger.Named("gc"))

	writeCoord := coordinator.NewWriteCoordinatorImpl(coordinator.WriteCoordinatorConfig{
		MaxRetries:          cfg.WriteMaxRetries,
		RetryBaseIntervalMS: cfg.WriteRetryBaseMS,
		// write_coordinator.*: the background dispatch's outer budget, its claim
		// window, the give-up report's round trip, and the per-document fan-out
		// bound. Zero takes each one's package default.
		DispatchTimeout:             cfg.WriteDispatchTimeout,
		DispatchClaimWindow:         cfg.WriteDispatchClaimWindow,
		AbandonReportTimeout:        cfg.WriteAbandonReportTimeout,
		MaxConcurrentDocumentWrites: cfg.WriteMaxConcurrentDocs,
		Locks:                       kbLocks,
		WAL:                         walImpl,
		RaftNode:                    rn,
		Splitter:                    chunkSplitter,
		EmbedClient:                 embedClient,
		ChunkBloom:                  chunkBloom,
		VersionBloom:                vBloomStore,
		ChunkStore:                  chunkStoreIface,
		ChunkDocMapper:              chunkDocIface,
		DocStore:                    docStoreIface,
		VersionDocList:              versionDocIface,
		IndexManager:                indexMgrIface,
		// The event-driven half of the chunk GC: a deleted version's chunks go when
		// its document set still names them (see DropVersionStorage).
		ChunkReclaimer: gcImpl,
		// §7.13.2: a committed write is handed to the coordinator the control
		// layer picks, instead of being run by whoever accepted it. The dispatcher
		// is built further down (it needs the data plane), so the call goes through
		// the late-bound closure.
		Dispatch: func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
			if a.dispatchVersionWrite == nil {
				return fmt.Errorf("stratum: write dispatch is not wired")
			}
			return a.dispatchVersionWrite(ctx, kbID, versionID, parentVersionID, changes)
		},
		Logger: logger,
		// §10.1: a version whose data never landed has to reach the failure
		// accounting instead of sitting PENDING forever — see AbandonDispatch.
		ControlPlane: controlPlane,
	})

	// A deleted knowledge base's data is reclaimed by whoever holds it: the local
	// stores on an all-in-one node, and the storage group on a control node that
	// keeps none. The data plane is assembled further down (it needs the
	// coordinators), so the choice is resolved on first use rather than guessed
	// here.
	//
	// dataPlane is declared here and assigned below for exactly that reason: a
	// control node's dropper needs it, and the two constructions would otherwise
	// have to be interleaved.
	kbDropper := coordinator.NewDeferredKBDropper(func() coordinator.KBStorageDropper {
		// The reclaim has two halves and a split deployment needs both: the
		// storage group holds the data, and this node may hold a copy of its
		// own. Broadcasting is what makes the delete work at all when the data
		// is not here — the local layer deletes alone would clear this node's
		// (possibly empty) stores and leave the storage group's data readable.
		//
		// This mirrors the data plane's own DropVersionData, which broadcasts
		// and drops locally rather than choosing one.
		droppers := coordinator.KBStorageDroppers{
			coordinator.BroadcastKBDropper{Metadata: rn, Dropper: a.dataPlane},
		}
		if cfg.Role != NodeRoleControl {
			droppers = append(droppers, coordinator.LocalKBDropper{
				IndexManager:   indexMgr,
				DocStore:       ds,
				ChunkStore:     chunkStore,
				ChunkDocMapper: cdm,
				VersionDocList: vd,
				VersionBloom:   vBloomStore,
			})
		}
		return droppers
	})
	// §8.5: data-source announcements. A writer tells its replicas where a
	// version's data lives — the §7.3 confirmation carries its own address —
	// and the source lookup reads that table before falling back to the leader.
	// That keeps the lookup a map read, which matters because it runs on the
	// Raft apply path; probing peers there is what made the first attempt at
	// §8.5 unusable.
	//
	// Built here, ahead of the delete coordinator, because a knowledge base's
	// deletion has to drop its entries (DeleteCoordinatorConfig.
	// SourceAnnouncements). It reaches the receive side further down.
	dataSources := plane.NewDataSourceRegistry()

	deleteCoord := coordinator.NewDeleteCoordinatorImpl(coordinator.DeleteCoordinatorConfig{
		MaxRetries:          cfg.DeleteMaxRetries,
		RetryBaseIntervalMS: cfg.DeleteRetryBaseMS,
		WAL:                 walImpl,
		RaftNode:            rn,
		IndexManager:        indexMgr,
		DocStore:            ds,
		ChunkStore:          chunkStore,
		ChunkDocMapper:      cdm,
		VersionDocList:      vd,
		VersionBloom:        vBloomStore,
		Dropper:             kbDropper,
		SourceAnnouncements: dataSources,
	})

	// Same two halves as the whole-KB dropper above, for the same reason: the
	// version's records live in the storage group under a split topology, and
	// this node may hold a copy of its own. The receiving node applies the
	// visibility anchor itself, since it is the one holding the records.
	versionDropper := coordinator.NewDeferredVersionDropper(func() coordinator.VersionStorageDropper {
		droppers := coordinator.VersionStorageDroppers{
			coordinator.BroadcastVersionDropper{Dropper: a.dataPlane},
		}
		if cfg.Role != NodeRoleControl {
			droppers = append(droppers, coordinator.LocalVersionDropper{
				IndexManager:   indexMgr,
				DocStore:       ds,
				VersionDocList: vd,
				VersionBloom:   vBloomStore,
				Metadata:       rn,
				ChunkReclaimer: gcImpl,
			})
		}
		return droppers
	})
	deleteVersionCoord := coordinator.NewDeleteVersionCoordinatorImpl(coordinator.DeleteVersionCoordinatorConfig{
		MaxRetries:          cfg.DeleteMaxRetries,
		RetryBaseIntervalMS: cfg.DeleteRetryBaseMS,
		WAL:                 walImpl,
		RaftNode:            rn,
		IndexManager:        indexMgr,
		DocStore:            ds,
		VersionDocList:      vd,
		VersionBloom:        vBloomStore,
		Dropper:             versionDropper,
		// The crash-recovery half of the deletion: a resumed version delete records the
		// tombstone BEFORE touching bytes, so a late push for that version is refused on
		// a node that crashed mid-deletion. Optional like the table itself.
		Tombstones: tombstoneRecorder(versionTombstones),
	})
	a.gcImpl, a.writeCoord = gcImpl, writeCoord
	a.dataSources, a.deleteCoord = dataSources, deleteCoord
	a.deleteVersionCoord = deleteVersionCoord
}

// wireDataPlane builds the node-to-node halves (leader handler, follower,
// pusher), the holders cache the data-source lookup reads, the data plane
// itself, and the dispatcher the write coordinator calls.
//
// The order is forced in the other direction from wireCoordinators: the data
// plane needs the coordinator that already exists, and the coordinator's
// dispatch closure is assigned at the end — which is the late binding that keeps
// the §7.13.2 cycle open.
func (a *nodeAssembly) wireDataPlane() {
	cfg, logger, ctx := a.cfg, a.logger, a.ctx
	rn, raftNode, walImpl := a.rn, a.raftNode, a.walImpl
	storageLocal, nodeState := a.storageLocal, a.nodeState
	controlPlane := a.controlPlane
	ds, cdm, vd := a.ds, a.cdm, a.vd
	chunkStore, vBloomStore := a.chunkStore, a.vBloomStore
	indexMgr, writeCoord := a.indexMgr, a.writeCoord
	versionTombstones, dataSources := a.versionTombstones, a.dataSources

	var syncLeader *stratumsync.LeaderHandler
	var syncFollower *stratumsync.Follower
	var syncPusher *stratumsync.Pusher
	var dataPlane *plane.LocalDataPlane

	// Leader handler: serves storage-layer data to followers via gRPC.
	if storageLocal {
		syncLeader = stratumsync.NewLeaderHandler(
			ds.DB(),
			cdm.DB(),
			vd.DB(),
			chunkStore.VecstoreClient(),
		)
	}

	// The deployment shape every consumer below derives from: peer addresses,
	// the data-capable set, this node's own address as its peers see it, and the
	// replica set a write must reach. One value rather than four locals — see
	// clusterTopology for why they must be derived from the same declaration.
	topology := newClusterTopology(cfg)

	// resolveReplicaAddrs is the replica topology the write dispatcher walks
	// (§7.13.2) — the same list §8.4 distribution and the §8.5 confirmation use.
	// Consumed through closures, so the assembly order does not matter.
	resolveReplicaAddrs := topology.resolveReplicaAddrs

	// Any node may have a fact to report — a replica that just finished
	// writing, or a node running its startup reconcile. Raft still only
	// appends on the leader, so the access layer carries those proposals
	// there instead of the caller having to know who leads
	// (Stratum_设计文档v13.md §7.3/§7.8).
	// Only a node that holds a log forwards a proposal to whoever leads; a
	// storage node reports through the same commands, but they travel to a
	// control node that appends them (RemoteRaftNode).
	if raftNode != nil {
		raftNode.SetNodeID(cfg.NodeID)
		raftNode.SetForwarder(&raft.GRPCProposeForwarder{
			AddrByID: topology.peerAddr,
		})
	}

	// Which nodes a write must reach. "Should hold it" is a deployment fact, not a
	// runtime observation — deriving it from who has reported would let a silent
	// node silently leave the requirement.

	// Follower: pulls data when this node applies a version written by
	// the leader. The sync module is wired via OnVersionCreated.
	if storageLocal {
		syncFollower = stratumsync.NewFollower(ds, cdm, vd, chunkStore, indexMgr)
		// A replica that receives a version's data builds its document filter, exactly as
		// the writer's transaction does: the filter drops the search hits that are not in
		// the version, so one built before the data arrived is empty — and an empty filter
		// answers "nothing matched", with no error, for data this node holds in full.
		syncFollower.SetVersionBloom(vBloomStore)
		// A pull runs on the receiving side, so it is where a version whose metadata
		// no longer exists anywhere can still be recognised (see
		// LocalDataPlane.SetVersionTombstones for the reconciliation it does NOT
		// replace).
		syncFollower.SetVersionTombstones(versionTombstones)
		// sync.pull_dial_timeout_ms: the handshake bound of a pull, and only the
		// handshake — a transfer that is merely large is not a failure.
		syncFollower.SetPullDialTimeout(cfg.PullDialTimeout)
	}

	// DataPlane owns "does this node need the version's data, and where from"
	// (stage ① of the control/data separation: replication moved inside the
	// storage layer — control-data-separation-design.md §7).
	// Write-path replication: the coordinator exports the version's records to
	// the other replicas and requires a quorum of acknowledgements before the
	// storage layer reports it durable (Stratum_设计文档v13.md §7.1/§7.2).
	if storageLocal {
		syncPusher = stratumsync.NewPusher(stratumsync.PusherConfig{
			Exporter: syncLeader,
			NodeID:   cfg.NodeID,
		})
	}

	// §8.5's fourth layer (docs/data-source-holders-fallback-plan.md): the table
	// above is filled only by a writer's confirmation, which is a bounded
	// fire-and-forget broadcast. A replica that missed it has no source at all —
	// the leader this lookup falls back to is the control leader, which in a
	// two-tier deployment exports no data and refuses. The holders cache below keeps
	// what this node's catch-up learned from its peers, so such a replica can still
	// find one.
	//
	// The lookup stays a pure map read: it runs on the Raft apply path, where
	// dialling anyone is what made the first attempt at §8.5 unusable.
	// Which peers can serve a knowledge base, read by the data-source lookup. A node that
	// is behind fills it while catching up (see the LagCatchup wiring below): the lookup
	// may run on the Raft apply path, so it can only read memory.
	holdersCache := plane.NewHoldersCache(plane.HoldersCacheConfig{
		TTL:    cfg.HoldersCacheTTL,
		Limit:  cfg.HoldersCacheLimit,
		Logger: logger,
	})

	// §B/§C: the "is this version gone?" judgement reads CURRENT state from wherever the
	// metadata lives — the state machine on a node that has one, the control tier through
	// the internal service on a storage node — so both shapes judge the same way, and
	// neither depends on a removal record that pruning can drop.
	var liveness plane.VersionLivenessLister
	if lv, ok := rn.(plane.VersionLivenessLister); ok {
		liveness = lv
	}

	dataPlane = plane.NewLocalDataPlane(dataPlaneDeps{
		cfg: cfg, logger: logger, rn: rn, controlPlane: controlPlane,
		indexMgr: indexMgr, vd: vd, ds: ds, cdm: cdm, chunkStore: chunkStore,
		vBloomStore: vBloomStore, walImpl: walImpl, writeCoord: writeCoord,
		syncFollower: syncFollower, syncPusher: syncPusher,
		dataSources: dataSources, holdersCache: holdersCache, topology: topology,
		nodeState: nodeState, liveness: liveness,
	}.config())

	// One plane serves both paths: the write path runs its storage transaction
	// through it, and the read/sync path resolves pulls through it.
	writeCoord.SetDataPlane(dataPlane)

	// The tombstone table: the landing paths refuse a version this node already
	// knows was deleted, and its own retry loop finishes a reclaim that a crash or a
	// failure left unfinished — neither of which needs the control tier. Unwired on
	// a node with no store, which is the behaviour before this table existed.
	if versionTombstones != nil {
		dataPlane.SetVersionTombstones(versionTombstones)
		dataPlane.StartDeletedVersionRetry(ctx, cfg.DeletedVersionRetryInterval, cfg.TombstoneGrace)
	}

	// The pull path is the third: it is what knows a version's records have
	// landed here, and this plane is what owns the cursor §9.3(2) reads. Wired
	// after the plane exists because the follower is built well before it.
	if syncFollower != nil {
		syncFollower.SetLocalVersionAdvancer(dataPlane)
		// An empty transfer is ambiguous — a version with no documents and a source
		// with no data for it look identical on the wire — and the writer's committed
		// digest is what tells them apart (see sync.Follower.confirmVersionIsEmpty).
		syncFollower.SetVersionDocIDSetHash(raft.DocIDSetHashReader{Node: rn})
	}

	// §7.13.2: the dispatcher picks a write's coordinator and hands it the work.
	// It is built here because it needs the data plane (for the local fallback)
	// and the replica topology, and it is published through the closure the
	// coordinator captured above.
	// §7.13.2/§2.2: the write goes to a candidate from the KB's replica topology,
	// tried in order until one takes it.
	//
	// A control node dispatches too — it may be the leader at apply time, and it
	// holds no data. What it must NOT be is one of its own candidates: it is not a
	// replica. LocalWrite is precisely what puts this node into candidates() (that
	// list only ever gains selfAddr when localhost != nil), and on a control node
	// dataPlane is nil. A nil receiver's method value is legal to form, so nothing
	// complains there; it panics only when called, deep inside the write
	// transaction — which is how a nil pointer in a dispatcher surfaced as a
	// stack trace through the storage layer.
	//
	// So: set LocalWrite only where there is a data plane. On a control node the
	// candidates are the storage group and every dispatch is remote, which is what
	// the topology asks for.
	dispatcherCfg := plane.CoordinatorDispatcherConfig{
		Replicas: resolveReplicaAddrs,
		SelfAddr: topology.localAddr,
		Logger:   logger,
		// dispatcher.*: one candidate's budget — a floor, a per-document
		// allowance and the cap on their sum. Zero takes the plane's defaults.
		CandidateTimeout:       cfg.CandidateTimeout,
		CandidateTimeoutPerDoc: cfg.CandidateTimeoutPerDoc,
		MaxCandidateTimeout:    cfg.MaxCandidateTimeout,
		// health.*: the §7.13.5 peer-demotion heuristic. Built here so the numbers
		// come from the config file; without it the dispatcher makes its own view
		// with the same defaults, which is what it did before the knob existed.
		Health: plane.NewLocalHealthView(plane.HealthViewConfig{
			DemoteAfter: cfg.HealthDemoteAfter,
			DemoteFor:   cfg.HealthDemoteFor,
		}),
	}
	// docs/storage-node-state-machine-plan.md §3.2: this node's own redundancy
	// verdict — what IT observed while reaching its peers, not the control
	// leader's aggregate over everyone's reports. The two are computed from
	// different evidence, which is why they are expected to disagree; neither is
	// derived from the other.
	//
	// Wired only where there is a state machine: a method value taken from a nil
	// *Machine is itself non-nil, so a control node must leave the func unset.
	if nodeState != nil {
		dispatcherCfg.Redundancy = nodeState.SetPeerRedundancy
	}
	if storageLocal {
		// Coordinating locally is the last resort: it keeps a single-node cluster
		// — or one whose peers are all unreachable — progressing without a second
		// code path for the write.
		dispatcherCfg.LocalWrite = dataPlane.WriteVersionData
	}
	writeDispatcher := plane.NewCoordinatorDispatcher(dispatcherCfg)
	dispatchVersionWrite := func(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error {
		// The coordinator's contract reports only success or failure — which node
		// took the write is the dispatcher's business. The apply hook below, which
		// wants the address for its log line, calls writeDispatcher directly.
		_, err := writeDispatcher.Dispatch(ctx, kbID, versionID, parentVersionID, changes)
		return err
	}
	a.syncLeader, a.syncFollower, a.syncPusher = syncLeader, syncFollower, syncPusher
	a.topology, a.holdersCache = topology, holdersCache
	a.dataPlane, a.writeDispatcher = dataPlane, writeDispatcher
	a.dispatchVersionWrite, a.resolveReplicaAddrs = dispatchVersionWrite, resolveReplicaAddrs
	a.liveness = liveness
}

// recoverStartup is everything a restart has to do before the node may answer:
// replay the interrupted transactions the WAL scan found, reconcile the index
// status against disk facts, project the node state, queue the terminal-version
// reclaims, run the deletion reconciliation, then wire the apply hooks and the
// two deletion sweeps.
//
// It returns the epoch-durable set: publishing it has to wait until every node
// is serving (see reportEpochWhenPeersAreUp), which is after Serve, so the
// caller holds it.
func (a *nodeAssembly) recoverStartup(records []types.PendingRecord) []plane.VersionRef {
	cfg, logger, ctx := a.cfg, a.logger, a.ctx
	rn, raftNode, storageLocal := a.rn, a.raftNode, a.storageLocal
	nodeState, controlPlane := a.nodeState, a.controlPlane
	walImpl := a.walImpl
	var epochDurable []plane.VersionRef

	// Replays the interrupted transactions the scan at the top of main() found. Three
	// record kinds are handled:
	//   - DeleteMark:      resume the interrupted DeleteKnowledgeBase flow.
	//   - VersionDelete:   resume the interrupted DeleteVersion flow.
	//   - VersionWrite:    replay the version's storage writes from the transaction
	//     input persisted in the WAL's BEGIN record. A record without local
	//     transaction input (applied by a follower, or a legacy WAL) cannot be
	//     replayed here — the node's data integrity for it is restored by Raft log
	//     replay + DataSync instead — so it is surfaced via the replay counter for
	//     operators.
	//
	// A record that still cannot be replayed (after bounded in-process retries) is
	// skipped and counted, never fatal: it stays in the WAL for the next restart and
	// the node keeps starting up. See runCrashRecovery's own comment.
	//
	// WHERE this sits is part of the contract, not an accident of assembly order. Both
	// halves of the replay reach for state wired just above:
	//
	//   - The version-write replay runs through the DATA PLANE
	//     (WriteCoordinator.ReplayVersionStorageWrites → LocalDataPlane.ResumeVersionWrite).
	//     Run it before SetDataPlane and the coordinator falls back to the private plane
	//     NewWriteCoordinatorImpl builds for itself: no tombstone table, no fan-out
	//     targets, no state machine. The replay then re-writes a version this node knows
	//     was deleted (the tombstone check answers "no table, so no"), and reports it
	//     durable without telling any replica.
	//   - A delete replay resolves the deferred droppers (kbDropper / versionDropper).
	//     Resolving them early CACHES the arm built from a nil data plane, so every
	//     later delete would carry a dead broadcast half.
	//
	// Replaying is also storage-layer work: a control node drives no storage coordinator,
	// and running this there dereferences nil. Since it runs during startup the failure
	// mode is a crash loop — in the fault-injection tests that cost the cluster its
	// quorum and timed out every test after it. Its WAL is not idle on a control node
	// (Raft's own records live there); what must not happen is treating those as storage
	// transactions to resume.
	if storageLocal {
		runCrashRecovery(ctx, logger, records, a.writeCoord, a.deleteCoord, a.deleteVersionCoord, walImpl,
			replayRetry{MaxAttempts: cfg.CrashReplayMaxAttempts, BaseMS: cfg.CrashReplayRetryBaseMS})
	}

	// Startup maintenance across the contract: the storage layer trims index
	// files by the retention policy, then reconciles — and the control layer
	// promotes versions whose READY proposal was lost
	// (control-data-separation-design.md §5.3/§7). Retention runs first so the
	// reconcile sees post-retention disk facts.
	// Both halves are about on-disk index facts this node is assumed to have.
	//
	// epochDurable is the durable set the reconcile established here. The §7.9
	// payload is published from it later — after the gRPC server is up, not here;
	// see reportEpochWhenPeersAreUp.
	if storageLocal {
		if err := a.dataPlane.EnforceRetention(ctx, rn); err != nil {
			logger.Warn("index retention: ListKnowledgeBases failed", zap.Error(err))
		}
		epochDurable = reconcileIndexStatus(ctx, logger, a.dataPlane, controlPlane, rn, cfg.IndexRetentionCount)

		// docs/storage-node-state-machine-plan.md §5.1: project the reconciled
		// disk facts into the version layer. Done ONCE, here, before the node
		// serves — which is what lets every later reader treat the machine as
		// complete, with no "no entry, go ask the disk" fallback on the read
		// path.
		projectNodeState(ctx, logger, nodeState, a.dataPlane, rn, epochDurable)

		// A terminal verdict does not come back on a restart — once the log is
		// compacted the entry is only ever restored as snapshot state, so the apply
		// hook that queues the reclaim never fires again. The verdict's METADATA does
		// survive, so sweep the state machine once and rebuild the list: no
		// persistence, and the same set the hook would have queued.
		if n, err := a.dataPlane.ReclaimTerminalVersions(ctx, rn); err != nil {
			logger.Warn("startup: could not rebuild the terminal-version reclaim list", zap.Error(err))
		} else if n > 0 {
			logger.Info("startup: queued terminal versions for local reclaim", zap.Int("versions", n))
		}

		// §B/§C: the deletion reconciliation, once, at boot.
		//
		// It goes through ReconcileDeletionsAtStartup — the same rule the periodic
		// sweep runs — instead of straight to ReconcileDeletedVersions (the live-chain
		// judgement alone), because the start-up pass is the one that can establish this
		// node's watermark into the control layer's deletion log. The old call reclaimed
		// but left the watermark untouched, so the first periodic tick found no
		// watermark, fell back to the full live-chain scan, and repeated exactly the work
		// the start-up pass had just done. It also drains the tombstone entries a crash
		// left PENDING, which is the one piece of deletion debt only this node can name.
		//
		// The read-only half stays read-only: with the sweep switched off the scan still
		// runs, so the leftovers are VISIBLE, and nothing is reclaimed.
		if a.liveness != nil {
			if cfg.ReconcileDeletedVersions {
				res, err := a.dataPlane.ReconcileDeletionsAtStartup(ctx, rn)
				if err != nil {
					logger.Warn("startup: the deletion reconciliation did not finish; the periodic sweep retries it", zap.Error(err))
				}
				if res.Reclaimed > 0 {
					logger.Info("startup: reclaimed local leftovers of deleted versions",
						zap.Int("versions", res.Reclaimed), zap.String("source", res.Source))
				} else {
					logger.Info("startup: deletion reconciliation found nothing to reclaim",
						zap.String("source", res.Source), zap.String("fallback", res.Fallback),
						zap.Int64("watermark", res.LocalSeq))
				}
			} else if leftovers, err := a.dataPlane.DeletedVersionLeftovers(ctx, rn); err != nil {
				logger.Warn("startup: could not scan for local leftovers of deleted versions", zap.Error(err))
			} else if n := totalVersionIDs(leftovers); n > 0 {
				logger.Info("startup: local leftovers of deleted versions found; set reconcile.deleted_versions to reclaim them",
					zap.Int("versions", n))
			}

			// The runtime pass has its own switch (reconcile.deleted_versions_periodic):
			// the sweep above answers "what is lying around at boot", this one "what
			// keeps appearing while running" — and the runtime leftovers are the ones
			// that matter, because a push that outlived the delete meant to remove it
			// needs no failure at all (§B).
			// Started when EITHER path is on, and paced by the incremental interval: one
			// tick is one bounded read of the deletion log plus whatever that asks for (a
			// forced live-chain re-judge on a local landing, a truncated log, or the
			// full-pass age) and the local drain of bytes still owed. The periodic switch
			// gates only the re-judge half, because the two halves cost very differently
			// and wanting the cheap one alone is a reasonable deployment.
			if cfg.ReconcileDeletionsIncremental || cfg.ReconcileDeletedVersionsPeriodic {
				a.dataPlane.StartDeletedVersionReconcile(ctx, rn,
					time.Duration(cfg.ReconcileDeletionsIncrementalIntervalS)*time.Second)
			}
		}
	}

	// This callback is the apply loop's, so only a node with an apply loop has
	// it: a storage node is never told "a version was created", it is handed the
	// version's data directly (ExecuteVersionWrite).
	// Only a node that holds data answers this: it is the hook that pulls a
	// version's records here and builds its index. A control node's versions
	// live in the storage group, and it has no stores to pull them into.
	// Tombstone pruning is deliberately NOT started.
	//
	// It existed to bound the removal records, because every judgement about "was this
	// version removed?" read them — and a record that pruning had dropped took the evidence
	// with it (docs/known-gaps.md §B: "delete a version below the watermark" and the row is
	// gone on the next tick). That judgement no longer reads them (see
	// plane.VersionLivenessLister): it reads the CURRENT state — an allocation counter that
	// only grows and the live version list. So a pruned row now costs an operator a
	// diagnostic and nothing else, while an unpruned row is one more reason a reader that
	// arrives late can still be answered. The records are tiny (one int64 per removed
	// version) and ride the snapshot, so keeping them is the cheap direction — and it is
	// what makes "delete a version below the watermark" uninteresting rather than a window.
	//
	// The mechanism has since been retired outright: the command, the state-machine field,
	// the snapshot field, the internal read and the loop that drove them are gone.

	// Bounds how many on-apply version transfers run at once (M19 of
	// docs/code-review-2026-10-18.md). A bulk import — or a log replay after a
	// restart — produces one cmdCreateVersion per version, and raft/impl.go runs this
	// hook on its own goroutine for each of them. Every hook that decides it needs the
	// data then starts a whole version transfer, so N versions meant N simultaneous
	// transfers, each with its own retry loop and multi-minute deadlines, all competing
	// for the same links. Four is the order LagCatchup uses: enough to make progress on
	// a multi-version gap, few enough that the links are not turned into one queue per
	// version. Waiting costs nothing here — the hook is already off the apply loop.
	fetchSlots := make(chan struct{}, maxConcurrentVersionFetches)

	if storageLocal && raftNode != nil {
		// §10.6's reclaim is a best-effort broadcast: a partitioned or restarting
		// replica never hears it, and the metadata that would have named the leftover
		// is gone by then, so nothing else notices it either. The verdict, unlike the
		// broadcast, travels the Raft log — so let every node learn it from its own
		// apply and join the same bounded, idempotent reclaim queue.
		raftNode.SetOnVersionFailedPermanent(a.dataPlane.NoteTerminalVersion)

		raftNode.SetOnVersionCreated(func(kbID string, versionID int64) {
			// Take a transfer slot before doing any work, and give it back however this
			// returns. A caller that gives up (shutdown) just leaves the version to the
			// next sweep — the data is not lost, only not fetched now.
			select {
			case fetchSlots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-fetchSlots }()

			// §7.5: as of this apply the version EXISTS, whether or not this node
			// ends up holding it. Telling the plane is what keeps its cursor
			// honest — a version announced but not held blocks the cursor from
			// stepping over it, so a lost push can no longer make this node claim
			// an unbroken history it does not have.
			//
			// Announcing for EVERY applier (not only the ones that will fetch) is
			// deliberate: the node that never receives the data is exactly the one
			// that needs the gap recorded.
			a.dataPlane.AnnounceVersion(kbID, versionID)

			// §8.6b: the DATA, always; the index, never on this path.
			//
			// This used to build the index eagerly for whichever version was the
			// control layer's ActiveVersionID. That exception is gone, and its going
			// is the point of §8.6(b) being consistent: "who needs this version" is a
			// question about USE, and applying happens for every version whether
			// anybody reads it or not. The router already answers the question — it
			// ranks versions by recent queries and routes to the owners of the ones
			// that are being read (internal/router/index_priority.go) — so a build
			// happens when a request arrives, and not before.
			//
			// The active pointer was never a good stand-in anyway: only
			// RollbackVersion moves it, so for a knowledge base nobody has rolled
			// back it equals "version 0" and the rule did nothing.
			if err := a.dataPlane.FetchVersionData(context.Background(), kbID, versionID); err != nil {
				logger.Error("sync: version data fetch did not converge",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			}
		})
	}

	// The reclaim retry loop is deliberately NOT inside the block above. It only
	// ever touches this node's own storage, so it belongs wherever there IS local
	// storage — and a storage node is exactly where reclaims get queued: with no
	// Raft it never runs the block above, yet `ReclaimTerminalVersions` queues its
	// startup sweep and being the failure detector queues reclaims in flight.
	// Leaving the loop on the raftNode condition gave that shape a queue with
	// producers and no consumer: a failed local reclaim was never retried until
	// the next restart.
	if storageLocal {
		a.dataPlane.StartTerminalReclaims(ctx)
	}

	// §7.13.2: only a node that leads AT APPLY TIME dispatches (see
	// RaftNodeImpl.SetOnVersionCommittedAsLeader) — one dispatch per write, not
	// one per replica. The changes come from the registration Execute left behind,
	// since the Raft command carries none (§7.7). TAKING it is what keeps the
	// apply hook and Execute's own background dispatch from both acting: whichever
	// arrives first wins, the other becomes a no-op.
	if raftNode != nil {
		raftNode.SetOnVersionCommittedAsLeader(func(kbID string, versionID, parentVersionID int64, clientRequestID string) {
			dispatchCommittedVersion(a.writeCoord, a.writeDispatcher, logger, kbID, versionID, parentVersionID, clientRequestID)
		})

		// The one state a delete can reach and never leave: DeleteVersion marks the
		// version set in the state machine (durable, replicated, and where the API
		// answers the caller) and only THEN hands the cleanup to a goroutine, so a
		// crash in between leaves it marked Deleting with nothing that would finish
		// it — the WAL has no record to resume from, because that mark is written
		// first inside the goroutine, and the reverse reconciliation cannot see it
		// either, because the version is still in the metadata. This pass is the only
		// thing that repairs it without an operator noticing, and it costs a walk of
		// this node's own state machine — ids only, and only for the knowledge bases
		// something has been deleted on, which is what makes a minute affordable.
		//
		// It starts with every knowledge base dirty: the dirty set is memory, so a
		// fresh process has received no marks and could still be carrying stuck ones
		// from a previous life.
		raftNode.MarkAllDeletingDirty()
		go coordinator.NewDeletingVersionSweeper(raftNode, raftNode, a.deleteVersionCoord, cfg.DeletingVersionSweepInterval, logger).Run(ctx)

		// The knowledge-base half of the same repair (M3 of
		// docs/code-review-2026-10-18.md). DeleteKnowledgeBase proposes the
		// Deleting status first and starts the cleanup — which is what writes the
		// WAL's delete mark — only afterwards, so a crash in between used to leave
		// the KB stuck in DELETING with nothing that would finish it: no WAL record
		// to resume from (it was never written), and no other pass looks for a KB
		// that is still in the metadata. This sweep re-runs the same idempotent
		// cleanup, so an interrupted deletion is resumed instead of forgotten.
		go coordinator.NewDeletingKBSweeper(raftNode, a.deleteCoord, cfg.DeletingKBSweepInterval, logger).Run(ctx)
	}
	return epochDurable
}

// nodeBoot is what exists before anything is assembled: the configuration, the
// log, the WAL (scanned, not yet replayed), the metadata channel, and the trust
// mark. Everything after this is built FROM it.
type nodeBoot struct {
	cfg      appConfig
	logger   *zap.Logger
	dataDir  string
	walImpl  *wal.FileWAL
	records  []types.PendingRecord
	rn       raft.RaftNode
	raftNode *raft.RaftNodeImpl
	mark     *authmeta.Signer

	// storageSilence is the -storage-silence-window flag: the §4.7 freshness
	// window, the one budget that stays a flag rather than a YAML key.
	storageSilence time.Duration

	// cleanup closes what this function opened, in reverse order. Registered by
	// main, so the WAL and Raft log live exactly as long as the process.
	cleanup func()
}

// bootstrapNode parses flags, reads the configuration, opens the WAL, scans it
// for uncommitted records and builds the metadata channel. Startup failures that
// cannot be recovered from end the process here (log.Fatal), which is what the
// container runtime reads as a crash.
func bootstrapNode() *nodeBoot {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	// Bound to the VARIABLE, not to the bootstrap logger: logging.level rebuilds
	// `logger` once the config file has been read (below), and that rebuilt one is
	// the one whose buffers need flushing.

	logger.Info("Stratum starting")

	// --- Flags ---
	// Minimal flag surface for local/dev runs (the one-click startup script
	// and ad-hoc manual runs). Defaults match defaultConfig(); a YAML config
	// file (see configs/config1.yaml) can supply the full multi-node
	// deployment settings, with individual flags overriding file values.
	configFlag := flag.String("config", "", "path to YAML config file (optional; flags override file values)")
	dataDirFlag := flag.String("data-dir", "", "data directory (default /var/lib/stratum/node1)")
	grpcAddrFlag := flag.String("grpc-addr", "", "gRPC listen address (default 0.0.0.0:7000)")
	vecstoreAddrFlag := flag.String("vecstore-addr", "", "vecstore gRPC address (default 127.0.0.1:7100)")
	embedAddrFlag := flag.String("embed-addr", "", "embed service address (default http://localhost:8080)")
	// How long a required replica may go quiet before the storage-redundancy
	// verdict stops counting it live (docs/storage-degradation-signal-plan.md
	// §3.2). Zero keeps the plane's own default, which is three report intervals.
	//
	// A flag rather than a YAML key on purpose: it is one of the §10.4 numbers
	// still to be calibrated against a real deployment, and a wrong verdict is
	// retryable by design, so getting it wrong costs a retry rather than an
	// outage.
	storageSilenceWindowFlag := flag.Duration("storage-silence-window", 0,
		"how long a required replica may go without reporting before it stops counting as live (default: 3 report intervals)")
	flag.Parse()

	// --- Configuration ---
	// Load a YAML config file if provided; individual flags override file
	// values. With no config file, use the hardcoded single-node defaults
	// (matching configs/config1.yaml).
	cfg := defaultConfig()
	if *configFlag != "" {
		loaded, err := loadConfig(*configFlag)
		if err != nil {
			logger.Fatal("failed to load config file", zap.String("path", *configFlag), zap.Error(err))
		}
		cfg = loaded
	}
	if *dataDirFlag != "" {
		cfg.DataDir = *dataDirFlag
	}
	if *grpcAddrFlag != "" {
		cfg.GRPCAddr = *grpcAddrFlag
	}
	if *vecstoreAddrFlag != "" {
		cfg.VecstoreGRPCAddr = *vecstoreAddrFlag
	}
	if *embedAddrFlag != "" {
		cfg.EmbedServiceAddr = *embedAddrFlag
	}

	// Now that the config file has been read (and the flags applied), honour
	// logging.level. The bootstrap logger above is info, which is also the
	// default; a deployment that asks for debug gets it from here on — including
	// the query path's per-stage timings.
	logger = loggerAtLevel(logger, cfg.LogLevel)

	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "/var/lib/stratum/node1"
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		logger.Fatal("failed to create data directory", zap.String("path", dataDir), zap.Error(err))
	}

	// --- WAL path ---
	//
	// The data layers' paths live in buildStorageStack now; this one stays here
	// because Raft's log is not part of the storage layer — a control node has
	// one too.
	walPath := dataDir + "/wal"

	// --- Initialize WAL ---
	walImpl, err := wal.NewFileWAL(walPath)
	if err != nil {
		logger.Fatal("failed to open WAL", zap.String("path", walPath), zap.Error(err))
	}

	// --- Crash recovery (records scanned now; replayed once the
	// coordinator layer is ready below) ---
	records, err := walImpl.Recover(nil)
	if err != nil {
		logger.Fatal("WAL recovery scan failed", zap.Error(err))
	}
	if len(records) > 0 {
		logger.Info("WAL recovery: pending records found, will replay after coordinators are ready",
			zap.Int("count", len(records)))
	}

	// --- Metadata channel ---
	//
	// rn is how this process reaches replicated metadata, whoever holds it: its
	// own Raft node when it runs one, the remote proxy when it does not
	// (Stratum_设计文档v13.md §11 阶段 ④「存储集群独立进程」).
	//
	// A storage node keeps no log, votes in no election and never leads, so it
	// holds no RaftNodeImpl: raftNode stays nil and every control-layer callback
	// below is skipped. The metadata it needs — a knowledge base's quantizer
	// configuration, the version chain, who currently leads — is read over gRPC
	// through RemoteRaftNode, and the facts it produces travel back the same way.
	//
	// Every consumer below therefore takes rn, never raftImpl: the two shapes
	// differ in where the metadata lives, not in what it means.
	var rn raft.RaftNode
	var raftNode *raft.RaftNodeImpl
	// The station's trust mark is signed with a key shared with the station
	// (internal/authmeta.Signer): this node verifies what the station stamped, so
	// a "1" forged by whoever reached the port directly no longer satisfies the
	// gate (H4 of docs/code-review-2026-09-24.md). An empty secret disables both
	// halves — it stamps nothing and verifies nothing — which is what a cluster
	// that does not require the mark needs.
	mark := authmeta.NewSigner([]byte(cfg.StationSecret), cfg.StationMarkTTL)
	if cfg.RequireAuthenticated && !mark.Enabled() {
		logger.Warn("require_authenticated is on but no station_secret is configured: " +
			"every client-facing call will be refused (set node.station_secret to the station's secret)")
	}
	if cfg.Role == NodeRoleStorage {
		rn = &raft.RemoteRaftNode{ControlAddrs: controlAddrsFromPeers(cfg.Peers), Mark: mark}
		logger.Info("starting as a storage node: no Raft log, metadata read through the control cluster",
			zap.String("role", string(cfg.Role)), zap.Int("control_nodes", len(cfg.Peers)))
	} else {
		raftNode, err = raft.NewRaftNodeImpl(raft.Config{
			NodeID:             cfg.NodeID,
			DataDir:            dataDir,
			RaftAddr:           cfg.RaftAddr,
			Peers:              cfg.Peers,
			WAL:                walImpl,
			Logger:             logger.Named("raft"),
			MaxLogLength:       cfg.MaxLogLength,
			DeletedLogLimit:    cfg.DeletedLogLimit,
			HeartbeatInterval:  cfg.HeartbeatInterval,
			ElectionTimeoutMin: cfg.ElectionTimeoutMin,
			ElectionTimeoutMax: cfg.ElectionTimeoutMax,
			// raft.*: the log's own bounds. Zero leaves kvraft's / raft's defaults,
			// which is what every deployment ran with before these were config keys.
			RPCTimeout:          cfg.RaftRPCTimeout,
			ProposeApplyTimeout: cfg.RaftProposeApplyTimeout,
		})
		if err != nil {
			logger.Fatal("failed to start RaftNode", zap.Error(err))
		}
		rn = raftNode
	}
	// Reverse order of acquisition: Raft first (it owns the log the WAL's
	// snapshots reference), then the WAL itself.
	cleanup := func() {
		if raftNode != nil {
			raftNode.Stop()
		}
		_ = walImpl.Close()
	}
	return &nodeBoot{
		cfg: cfg, logger: logger, dataDir: dataDir,
		walImpl: walImpl, records: records,
		rn: rn, raftNode: raftNode, mark: mark,
		storageSilence: *storageSilenceWindowFlag,
		cleanup:        cleanup,
	}
}
