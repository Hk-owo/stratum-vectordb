// Package types defines data structures shared across Stratum's internal
// modules. These types have no behavior of their own; they are pure data
// carriers exchanged between coordinator, storage, index, and service layers.
//
// See Stratum_接口设计v9.md "数据类型定义" for the authoritative definitions.
package types

// IndexStatus represents the build status of a version's vector index.
type IndexStatus int

const (
	// IndexStatusPending means metadata has been allocated and storage-layer
	// writes are in progress; the version is not queryable yet.
	IndexStatusPending IndexStatus = iota
	// IndexStatusReady means the index build completed; the version is queryable.
	IndexStatusReady
	// IndexStatusFailed means the index build failed. RollbackVersion refuses
	// to switch to a FAILED version; RebuildIndex can re-trigger the build.
	IndexStatusFailed
)

// String returns a human-readable name for the status, primarily for logging.
func (s IndexStatus) String() string {
	switch s {
	case IndexStatusPending:
		return "PENDING"
	case IndexStatusReady:
		return "READY"
	case IndexStatusFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// KBStatus represents the lifecycle status of a knowledge base.
type KBStatus int

const (
	KBStatusActive KBStatus = iota
	KBStatusDeleting
	KBStatusDeleteFailed
)

// String returns a human-readable name for the status, primarily for logging.
func (s KBStatus) String() string {
	switch s {
	case KBStatusActive:
		return "ACTIVE"
	case KBStatusDeleting:
		return "DELETING"
	case KBStatusDeleteFailed:
		return "DELETE_FAILED"
	default:
		return "UNKNOWN"
	}
}

// Chunk is a single fragment produced by splitting a document.
type Chunk struct {
	ChunkID string // SHA-256(chunk text + embed config ID)
	Content string // raw chunk text
}

// SearchResult is a single vector-search hit at chunk granularity.
type SearchResult struct {
	ChunkID string
	Score   float32
}

// AggregationMethod controls how per-chunk scores are combined into a
// per-document score when multiple chunks of the same document are hit.
// Configurable per query; default is Median.
type AggregationMethod int

const (
	AggregationMethodMedian AggregationMethod = iota // default
	AggregationMethodMax
	AggregationMethodMean
)

// EmbedConfig describes the embed service a knowledge base is bound to.
// This is a knowledge-base-level fixed attribute: set at creation time and
// immutable afterward.
type EmbedConfig struct {
	ServiceAddr string // embed service address
	ModelID     string // embed model ID; participates in chunk ID computation:
	// SHA-256(chunk text + ModelID)
}

// KnowledgeBaseMeta is knowledge base metadata stored in the Raft state machine.
type KnowledgeBaseMeta struct {
	KBID             string
	Name             string
	ChunkWindowSize  int
	ChunkOverlapSize int
	IndexType        string // HNSW / IVF / FLAT; immutable after creation.
	// Only HNSW has a real implementation today.
	Similarity string // COSINE / EUCLIDEAN / INNER_PRODUCT; immutable after
	// creation; defaults to COSINE.
	EmbedConfig     EmbedConfig
	ActiveVersionID int64
	Status          KBStatus
}

// VersionMeta is version metadata stored in the Raft state machine.
type VersionMeta struct {
	VersionID       int64
	ParentVersionID int64
	KBID            string
	CreatedAt       int64 // Unix timestamp; leader's local clock at apply time.
	// Not required to be strictly monotonic across nodes.
	IndexStatus IndexStatus

	// DocIDSetHash is the SHA-256 digest of this version's full document-ID
	// set (sorted docIDs, '\n'-separated — see sync.ComputeDocIDSetHash).
	// The leader computes and commits it (via ProposeUpdateVersionSummary)
	// only after its own storage writes have finished, so it doubles as a
	// "storage writes complete" marker. Followers recompute the digest from
	// their locally pulled VersionDocList and retry the DataSync pull until
	// the two match, which closes the "follower pulled before the leader's
	// writes landed" race without moving data into the Raft log. Empty
	// string means no digest has been committed yet (initial/empty version,
	// or a missed propose) — followers then skip verification.
	DocIDSetHash string

	// Deleting marks the version as being removed asynchronously (the
	// DeleteVersion flow). Set by cmdMarkVersionDeleting; while true the
	// version cannot be used as a parent for CreateVersion, cannot be
	// queried, and is skipped by normal reads. The version's metadata is
	// removed entirely by cmdRemoveVersionMeta once the background cleanup
	// finishes. Versions marked Deleting that never reach RemoveVersionMeta
	// (e.g. a crashed cleanup) are surfaced via GetSystemStatus.
	Deleting bool
}

// ChangeOp identifies the kind of mutation a DocChange represents.
type ChangeOp int

const (
	ChangeOpAdd ChangeOp = iota
	ChangeOpDelete
	ChangeOpUpdate
)

// String returns a human-readable name for the op, primarily for logging.
func (o ChangeOp) String() string {
	switch o {
	case ChangeOpAdd:
		return "ADD"
	case ChangeOpDelete:
		return "DELETE"
	case ChangeOpUpdate:
		return "UPDATE"
	default:
		return "UNKNOWN"
	}
}

// DocChange is a single document mutation passed into CreateVersion.
type DocChange struct {
	Op      ChangeOp
	DocID   string
	Content string // required for ADD / UPDATE; vectors are generated
	// internally by Stratum via the embed service, callers never supply them.
}

// PendingRecordType identifies the kind of PendingRecord returned by
// WAL.Recover.
type PendingRecordType int

const (
	// PendingRecordTypeDeleteMark indicates an unfinished knowledge base deletion.
	PendingRecordTypeDeleteMark PendingRecordType = iota
	// PendingRecordTypeVersionWrite indicates a CreateVersion flow that
	// reached WriteVersionID but never reached WriteCommit: Raft apply
	// completed (so the state machine already has this version), but the
	// storage-layer writes (steps 3-6 of the write path) may not have
	// finished. Recovery skips re-proposing to Raft and replays storage
	// writes from scratch using VersionID, which is safe because every
	// storage write is idempotent.
	//
	// This type was added to close a gap between Stratum_接口设计v9.md
	// (whose original PendingRecordType enum only covered DeleteMark) and
	// Stratum_测试顺序.md's T1-6 case table, which expects WAL.Recover to
	// directly surface a pending versionID for this scenario rather than
	// requiring callers to discover it through a separate accessor.
	PendingRecordTypeVersionWrite
	// PendingRecordTypeVersionDelete indicates a DeleteVersion flow that
	// wrote its delete mark but never reached the delete-complete record:
	// the versions in the state machine are marked Deleting but the
	// background cleanup may not have finished. Recovery re-runs the
	// DeleteVersion cleanup for the affected (kbID, versionID), which is
	// idempotent end-to-end.
	PendingRecordTypeVersionDelete
)

// PendingRecord is a WAL record requiring crash-recovery handling.
type PendingRecord struct {
	Type      PendingRecordType
	KBID      string // used by PendingRecordTypeDeleteMark
	VersionID int64  // used by PendingRecordTypeVersionWrite
}

// ClusterStatus is Raft cluster connectivity status; it does not depend on
// any specific knowledge base.
type ClusterStatus struct {
	HasLeader   bool
	MemberCount int
	LeaderID    int64
}

// ReplayCounter tracks WAL replay failures. In-memory only, not persisted;
// resets to zero on process restart.
type ReplayCounter struct {
	Record     PendingRecord
	RetryCount int
}
