// Package plane defines the contract between Stratum's control layer and its
// storage layer.
//
// The control layer owns Raft-replicated metadata only: knowledge bases, the
// version chain, the active version and abstract availability state. The
// storage layer owns everything physical: documents, chunks, vector indexes,
// replication, placement and repair. The contract between them carries
// logical objects only — never a replica count, an erasure-coding scheme, a
// file path or a node identity — which is what lets the two layers be split
// into separate processes (or clusters) without the control layer learning
// anything about placement. See control-data-separation-design.md (v1) §4 and
// Stratum_设计文档v13.md §7.0.
//
// Stage ① of that roadmap ("抽出 DataPlane/ControlPlane 契约，同进程实现")
// draws the boundary first, at zero distributed cost: the in-process
// implementations (see the local*.go files in this package) wrap the existing
// components, so the control layer stops reaching into local disks and the
// storage layer stops reaching into the Raft state machine. Later stages move
// the same contract across a process boundary (v1 §7 阶段 2).
package plane

import (
	"context"

	"stratum/internal/types"
)

// DurabilityPolicy is the declarative persistence target the control layer
// hands to the storage layer for one knowledge base (v1 §4.1
// SetDurabilityPolicy). How that target is met — replication factor, erasure
// coding, placement — is a storage-layer internal matter; the control layer
// only declares and then consumes the reported availability.
type DurabilityPolicy struct {
	// MaxFailures is how many failed attempts the control layer tolerates
	// before declaring a version FAILED_PERMANENT (Stratum_设计文档v13.md
	// §10.1). Zero means "unset": DefaultFailureBudget applies.
	MaxFailures int

	// MaxInFlightWrites caps how many writes may be in flight for this KB at
	// once (Stratum_设计文档v13.md §7.7). Zero means "unset": the default
	// applies. It is the storage layer's backstop against a stalled version
	// letting the rest pile up behind it.
	MaxInFlightWrites int

	// Replicas is the desired number of durable copies. Zero means "unset":
	// the storage layer keeps its current (or default) target.
	Replicas int
}

// Availability is the abstract serviceability state the storage layer reports
// up (v1 §4.3). The control layer stores this state without learning *why* a
// version is degraded — that is precisely the "control layer is blind to
// physical details" property the separation is built on.
type Availability int

const (
	// AvailabilityAvailable: data meets the durability policy and the index
	// can serve reads.
	AvailabilityAvailable Availability = iota
	// AvailabilityDegraded: serviceable, but below the redundancy target or
	// requiring cold-data decoding first.
	AvailabilityDegraded
	// AvailabilityUnavailable: not serviceable.
	AvailabilityUnavailable
)

// String renders an Availability for logs and error messages.
func (a Availability) String() string {
	switch a {
	case AvailabilityAvailable:
		return "AVAILABLE"
	case AvailabilityDegraded:
		return "DEGRADED"
	case AvailabilityUnavailable:
		return "UNAVAILABLE"
	default:
		return "UNKNOWN"
	}
}

// VersionRef names one (knowledge base, version) pair for bulk reports.
type VersionRef struct {
	KBID      string
	VersionID int64
}

// DataPlane is what the control layer calls on the storage layer (v1 §4.1).
// Implementations may be in-process (stage ①, see LocalDataPlane) or remote
// (stage ② onwards).
type DataPlane interface {
	// WriteVersionData durably stores one version's document changes. The
	// storage layer owns replication and placement, and it owns the write
	// transaction itself (BEGIN through COMMIT) — see Stratum_设计文档v13.md
	// §7.12. Completion is reported back through
	// ControlPlane.ReportDataDurable, not through this call.
	//
	// parentVersionID is required because a version's document set is derived
	// from its parent's (the version→document list is inherited rather than
	// restated), and the storage layer does not hold the version chain.
	WriteVersionData(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error

	// EnsureIndex makes versionID's vector index built and serviceable.
	// Idempotent: repeating the call for an already-built version is a
	// no-op. Only "ActiveVersionID or explicitly accessed" versions need to
	// be built eagerly (Stratum_设计文档v13.md §8.6b), which is why this is
	// a declaration rather than part of WriteVersionData.
	EnsureIndex(ctx context.Context, kbID string, versionID int64) error

	// FetchVersionData brings versionID's data to this node but leaves the
	// index unbuilt: the version is not the active one, so its index is worth
	// building only if something actually queries it (§8.6b). Data still has to
	// be here — "all replicas hold the data" is what makes the version durable.
	FetchVersionData(ctx context.Context, kbID string, versionID int64) error

	// ResumeVersionWrite completes a write transaction whose BEGIN record is
	// already durable (crash recovery): it re-runs the version's storage
	// writes and commits, without writing a second BEGIN. Every storage write
	// is idempotent, so resuming an interrupted transaction is safe.
	ResumeVersionWrite(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error

	// DropVersionData removes versionID's physical data: its documents, any
	// chunk that no surviving version references, and its index files.
	DropVersionData(ctx context.Context, kbID string, versionID int64) error

	// Search runs a vector query against versionID's index.
	Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]types.SearchResult, error)

	// SetDurabilityPolicy declares the persistence target for kbID.
	SetDurabilityPolicy(ctx context.Context, kbID string, policy DurabilityPolicy) error
}

// ControlPlane is what the storage layer calls back on the control layer
// (v1 §4.2). It is the only channel through which storage-layer progress
// reaches the replicated metadata, and it has no counterpart for reads — the
// read path never touches the control layer (v1 §5.2, revised in
// Stratum_设计文档v13.md §9 to route through the service station instead).
type ControlPlane interface {
	// ReportDataDurable reports that versionID's data has reached the
	// durability policy. digest is the version's document-set digest, the
	// equivalent of the committed VersionMeta.DocIDSetHash that followers
	// already use to verify a data pull.
	ReportDataDurable(ctx context.Context, kbID string, versionID int64, digest string) error

	// ReportIndexReady reports that versionID's index is built and
	// serviceable.
	ReportIndexReady(ctx context.Context, kbID string, versionID int64) error

	// ReportVersionFailure records a failed attempt at making versionID
	// durable — where "durable" means whichever side the failure belongs to:
	// its data (FailureSideData) or its index (FailureSideIndex). The control
	// layer owns the terminal verdict (v13 §10.1): once that side's failure
	// budget is spent it records the terminal state with the recorded cause —
	// DataStatusFailedPermanent for the data side, IndexStatusFailedPermanent
	// for the index side — and nothing retries it automatically again.
	//
	// The two sides count separately (v13 §10.1b): an index build that keeps
	// failing must not spend the budget the data write needs.
	//
	// Returning terminal=true means the control layer has just recorded the
	// terminal verdict for that side: when it is the DATA side, the caller
	// should reclaim the version's physical data (Stratum_设计文档v13.md §10.6).
	ReportVersionFailure(ctx context.Context, kbID string, versionID int64, side types.FailureSide, class types.FailureClass, detail string) (terminal bool, err error)

	// SetFailureBudget declares how many failed attempts kbID tolerates before
	// its versions are declared FAILED_PERMANENT. It comes from the KB's
	// DurabilityPolicy: the *decision* stays the control layer's (§10.1), but
	// the number is per-KB policy. A non-positive value means "use the default".
	SetFailureBudget(ctx context.Context, kbID string, maxFailures int) error

	// ReportAvailability reports versionID's abstract availability state.
	ReportAvailability(ctx context.Context, kbID string, versionID int64, state Availability) error

	// ReportEpoch reconciles control-layer state with storage-layer facts
	// after a restart (v1 §5.3): the control layer records the new epoch,
	// invalidates reports from older epochs, and demotes every version that
	// is absent from durableVersions. The invariant it enforces is
	// "control-layer state never runs ahead of storage-layer data".
	ReportEpoch(ctx context.Context, epoch uint64, dataVersions map[string]int64, indexReadyVersions map[string][]int64) error

	// ReclaimableChangesThrough reports the highest version V whose recorded
	// changes may be discarded, because every replica that should hold it has
	// reported a cursor reaching V (v13 §7.5). ok=false means "unknown" — not the
	// leader, no aggregate wired, or some replica has not reported — and every one
	// of those answers means the same thing: keep the data.
	//
	// It is on this interface because the storage layer is what PHYSICALLY reclaims
	// (the WAL is its own file), while the judgement is global: only the leader holds
	// the reports, so a node asks its control layer rather than computing it locally.
	ReclaimableChangesThrough(kbID string) (int64, bool)
}
