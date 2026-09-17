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
	// IndexStatusFailedPermanent is the control layer's verdict that a
	// version has spent its retry budget (or hit an unrecoverable error) and
	// will not be retried automatically again. Unlike FAILED, nothing
	// re-triggers it: an operator must retry or abandon it
	// (Stratum_设计文档v13.md §10.1).
	IndexStatusFailedPermanent
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
	case IndexStatusFailedPermanent:
		return "FAILED_PERMANENT"
	default:
		return "UNKNOWN"
	}
}

// IsFailed reports whether the index reached a failed verdict — transient
// (IndexStatusFailed) or terminal (IndexStatusFailedPermanent). Both mean the
// version has no usable index, so the read path and the operator-facing checks
// must refuse it; the only difference is whether the control layer may still
// retry.
func (s IndexStatus) IsFailed() bool {
	return s == IndexStatusFailed || s == IndexStatusFailedPermanent
}

// FailureClass is the storage layer's classification of a failed attempt to
// make a version durable. It decides how the control layer treats the failure
// (Stratum_设计文档v13.md §10.1): the storage layer names the kind, the control
// layer owns the verdict.
type FailureClass int

const (
	// FailureTransient is the default. A network blip, a restarted peer, a full
	// disk: another attempt — possibly on another replica — may well succeed,
	// so it counts against the version's retry budget.
	FailureTransient FailureClass = iota
	// FailureFatalGlobal means retrying cannot help: the knowledge base is
	// gone, the input was rejected, the data is unavailable everywhere. It
	// short-circuits the retry budget and goes straight to the terminal
	// verdict, because spending retries only delays the inevitable and keeps
	// the version PENDING in the meantime.
	FailureFatalGlobal
)

// String returns a human-readable name, primarily for logging.
func (c FailureClass) String() string {
	switch c {
	case FailureTransient:
		return "TRANSIENT"
	case FailureFatalGlobal:
		return "FATAL_GLOBAL"
	default:
		return "UNKNOWN"
	}
}

// FailureSide names which half of a version's Saga a failure belongs to.
//
// A version carries two independent states — its data (DataStatus) and its
// index (IndexStatus) — so "this version failed" is not a complete statement.
// The retry budget, the failure counter, and the terminal verdict all apply to
// one side at a time: a version whose index build keeps failing must not spend
// the budget its data write needs, and vice versa
// (Stratum_设计文档v13.md §10.1, §10.1b).
type FailureSide int

const (
	// FailureSideData is the default: the version's data never became durable.
	//
	// It is the zero value on purpose. Before the two sides were separated,
	// every failure report came from the data write path, so a command written
	// by an older build — which carries no side at all — replays as the data
	// side, which is exactly what it was.
	FailureSideData FailureSide = iota
	// FailureSideIndex means the version's index never became serviceable.
	FailureSideIndex
)

// String returns a human-readable name, primarily for logging.
func (s FailureSide) String() string {
	switch s {
	case FailureSideData:
		return "data"
	case FailureSideIndex:
		return "index"
	default:
		return "UNKNOWN"
	}
}

// DataStatus is the storage-layer state of a version's DATA, kept independent
// of its index state (Stratum_设计文档v13.md §10.1b).
//
// The two are tracked apart because they fail apart: a version whose data is
// durable may still have no working index, and a failed index build says
// nothing about whether the data landed. Before this existed the two shared
// one field, and the data side's terminal verdict was recorded as an INDEX
// state — the inverse of what that field means.
type DataStatus int

const (
	// DataStatusPending means the version's metadata is allocated and the
	// storage-layer write is still in flight. This is what "PENDING" always
	// described; it was never the index's state.
	DataStatusPending DataStatus = iota
	// DataStatusDurable means a quorum of replicas has confirmed the version's
	// data durable (ControlPlane.ReportDataDurable).
	DataStatusDurable
	// DataStatusFailedPermanent is the data side's terminal verdict: the retry
	// budget is spent, or the failure was globally fatal, so the data will
	// never be written. Like the index side's, nothing re-triggers it — only an
	// operator can retry or abandon the version (Stratum_设计文档v13.md §10.1).
	DataStatusFailedPermanent
)

// EmptyDocIDSetHash is the document-set digest of a version with no documents:
// SHA-256 of empty input, which is what sync.ComputeDocIDSetHash(nil) returns.
//
// It is written out here, beside the metadata that stores it, because both layers
// need it and neither may import the other: the control layer records it when a
// version is created with no document changes, and the storage layer compares
// against it when deciding whether a node holds such a version. A test in
// internal/sync keeps this value and that function in step.
const EmptyDocIDSetHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// String returns a human-readable name for the status, primarily for logging.
func (s DataStatus) String() string {
	switch s {
	case DataStatusPending:
		return "DATA_PENDING"
	case DataStatusDurable:
		return "DATA_DURABLE"
	case DataStatusFailedPermanent:
		return "DATA_FAILED_PERMANENT"
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

// ChunkMode names the splitting algorithm a knowledge base uses to turn a
// document into chunks (docs/content-defined-chunking-plan.md §3).
type ChunkMode string

const (
	// ChunkModeCDC is content-defined chunking (splitter.CDCSplitter): the
	// default, and what the zero value means. Boundaries come from a rolling
	// Rabin fingerprint, so a mid-document edit disturbs only the chunks around
	// it instead of every chunk after it. Measured against this repository's own
	// documents, that moves the share of chunks past the edit point that keep
	// their ChunkID from 0% to about 95%, and the text that has to be embedded
	// again from 64% to 10% of the document
	// (docs/content-defined-chunking-plan.md §1.2b).
	//
	// The zero value means this mode deliberately. Knowledge base metadata is
	// stored as JSON/gob in the Raft state machine and read back through the
	// control plane, so it arrives in three shapes: written by this code (the
	// field is set), decoded from a snapshot that predates the field (""), or
	// mapped field by field out of the control-plane proto, which has no field
	// for the mode at all (""). Making the default here means all three resolve
	// to the same algorithm — which is what removes the need for a proto field,
	// and with it the failure mode of one node cutting a knowledge base
	// differently from another (docs/content-defined-chunking-plan.md §3.6).
	ChunkModeCDC ChunkMode = "cdc"

	// ChunkModeWindow is the fixed-offset sliding window
	// (splitter.SlidingWindowSplitter). Nothing in the API selects it: every
	// knowledge base is chunked content-defined, and the window survives as the
	// implementation the CDC tests are measured against
	// (docs/content-defined-chunking-plan.md §8). Only code sets this.
	ChunkModeWindow ChunkMode = "window"
)

// String returns the mode's name for logging. The empty mode reports "cdc",
// because that is the behaviour it selects.
func (m ChunkMode) String() string {
	if m == "" {
		return string(ChunkModeCDC)
	}
	return string(m)
}

// ChunkParams is one document's splitting input: which algorithm to use and
// the sizes that algorithm reads.
//
// The two modes count in different units, and the field names keep them apart:
// WindowSize/OverlapSize are RUNES (as they always were, so that multi-byte
// text counts correctly), while MinSize/MaxSize are BYTES — the content-defined
// chunker rolls a byte-wise fingerprint and reports byte offsets
// (docs/content-defined-chunking-plan.md §3.2, §3.3). Each mode reads only its
// own fields.
type ChunkParams struct {
	Mode        ChunkMode
	WindowSize  int // window mode: window size, in runes.
	OverlapSize int // window mode: overlap between neighbours, in runes.
	MinSize     int // cdc mode: minimum chunk size, in bytes.
	MaxSize     int // cdc mode: maximum chunk size, in bytes.
	AvgBits     int // cdc mode: target average size, 2^AvgBits bytes.
}

// WindowChunkParams builds sliding-window parameters.
func WindowChunkParams(windowSize, overlapSize int) ChunkParams {
	return ChunkParams{Mode: ChunkModeWindow, WindowSize: windowSize, OverlapSize: overlapSize}
}

// CDCChunkParams builds content-defined chunking parameters.
func CDCChunkParams(minSize, maxSize, avgBits int) ChunkParams {
	return ChunkParams{Mode: ChunkModeCDC, MinSize: minSize, MaxSize: maxSize, AvgBits: avgBits}
}

// Defaults for the chunk sizes each mode falls back to when metadata leaves
// them unset (docs/content-defined-chunking-plan.md §3.2). The window pair
// matches knowledge_base_defaults in configs; the CDC triple is the measured
// pick — it averages 738 bytes per chunk against the window's 766, so the
// retrieval granularity is unchanged while the reuse rate is not.
const (
	DefaultChunkWindowSize  = 512
	DefaultChunkOverlapSize = 64
	DefaultChunkMinSize     = 256
	DefaultChunkMaxSize     = 1536
	DefaultChunkAvgBits     = 9
)

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
	// ChunkMode selects how documents are split into chunks. The zero value is
	// ChunkModeCDC, so a knowledge base is chunked content-defined unless code
	// says otherwise; nothing in the API sets this field, by design
	// (docs/content-defined-chunking-plan.md §3.6). Each mode reads only its own
	// sizes — the two window fields above, or the three CDC fields below.
	// Immutable after creation: every version of a knowledge base has to be
	// split the same way, or the same text yields different chunk IDs and
	// nothing can be reused.
	ChunkMode    ChunkMode
	ChunkMinSize int    // cdc mode: minimum chunk size, in bytes.
	ChunkMaxSize int    // cdc mode: maximum chunk size, in bytes.
	ChunkAvgBits int    // cdc mode: target average size, 2^ChunkAvgBits bytes.
	IndexType    string // HNSW / IVF / FLAT; immutable after creation.
	// Only HNSW has a real implementation today.
	Similarity string // COSINE / EUCLIDEAN / INNER_PRODUCT; immutable after
	// creation; defaults to COSINE.
	// QuantizerType selects the in-memory index payload storage:
	// OFF / SQ8 / SQ_BF16 / SQ_FP16 / PQ (Stratum_设计文档v12.md). Immutable
	// after creation; defaults to OFF (full precision, current behavior).
	QuantizerType    string
	QuantizerPQM     int // PQ sub-vectors; used only when QuantizerType == "PQ".
	QuantizerPQNBits int // PQ bits per sub-quantizer; used only when QuantizerType == "PQ".
	EmbedConfig      EmbedConfig
	ActiveVersionID  int64
	Status           KBStatus
}

// Chunking returns the splitter input this knowledge base's versions must be
// split with, with defaults filled in for anything the stored metadata left
// unset. Everything except an explicit ChunkModeWindow is content-defined.
//
// The defaults are duplicated on purpose. service.KnowledgeBaseService fills the
// window pair in when a knowledge base is created, but this function also has to
// answer for metadata that never went through that path: a knowledge base decoded
// from a snapshot predating these fields carries neither a mode nor CDC sizes, and
// an unset size must not become a zero-size window.
func (m KnowledgeBaseMeta) Chunking() ChunkParams {
	if m.ChunkMode == ChunkModeWindow {
		windowSize, overlapSize := m.ChunkWindowSize, m.ChunkOverlapSize
		if windowSize <= 0 {
			windowSize = DefaultChunkWindowSize
		}
		if overlapSize < 0 {
			overlapSize = DefaultChunkOverlapSize
		}
		return WindowChunkParams(windowSize, overlapSize)
	}

	minSize, maxSize, avgBits := m.ChunkMinSize, m.ChunkMaxSize, m.ChunkAvgBits
	if minSize <= 0 {
		minSize = DefaultChunkMinSize
	}
	if maxSize <= 0 {
		maxSize = DefaultChunkMaxSize
	}
	if avgBits <= 0 {
		avgBits = DefaultChunkAvgBits
	}
	return CDCChunkParams(minSize, maxSize, avgBits)
}

// VersionMeta is version metadata stored in the Raft state machine.
type VersionMeta struct {
	VersionID       int64
	ParentVersionID int64
	KBID            string
	CreatedAt       int64 // Unix timestamp; leader's local clock at apply time.
	// Not required to be strictly monotonic across nodes.
	IndexStatus IndexStatus

	// DataStatus is this version's DATA-side state, kept independent of
	// IndexStatus (Stratum_设计文档v13.md §10.1b). The two are tracked apart
	// because they fail apart: a version whose data is durable may still have
	// no working index, and a failed index build says nothing about whether the
	// data landed. Before this field existed, the data side's terminal verdict
	// was recorded in IndexStatus.
	DataStatus DataStatus

	// IndexReadyNodes records WHICH nodes have reported this version's index
	// built and serviceable (ReportIndexReady), sorted and deduplicated.
	//
	// IndexStatus alone answers "is this version serviceable somewhere"; this
	// field answers the different question §8.6(d)'s rolling cleanup has to ask
	// before it takes one replica out of service to reclaim an artifact: "if I
	// step out for a moment, how many are still serving?" — which needs the
	// identities, not a count, because the asking node has to exclude itself.
	//
	// Replicas report it themselves; the control layer is the single authority
	// that aggregates them (§1.2), so nothing here is a soft observation.
	IndexReadyNodes []int64

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

	// FailureReason records why a version reached FAILED_PERMANENT: the
	// storage layer's classification plus a detail string, so an operator can
	// tell "the data is unavailable everywhere" from "every attempt timed
	// out". Set together with IndexStatusFailedPermanent; empty otherwise.
	FailureReason string

	// FailureCount is how many attempts failed before that verdict, so the
	// cause stays auditable after the fact.
	FailureCount int32

	// FailureSide is WHICH half of the version that verdict settled
	// (Stratum_设计文档v13.md §10.1b). The two sides carry independent states and
	// therefore independent verdicts, so "the version failed" is not a complete
	// statement without it — and the reason chain above belongs to whichever side
	// this names.
	FailureSide FailureSide

	// Deleting marks the version as being removed asynchronously (the
	// DeleteVersion flow). Set by cmdMarkVersionDeleting; while true the
	// version cannot be used as a parent for CreateVersion, cannot be
	// queried, and is skipped by normal reads. The version's metadata is
	// removed entirely by cmdRemoveVersionMeta once the background cleanup
	// finishes. Versions marked Deleting that never reach RemoveVersionMeta
	// (e.g. a crashed cleanup) are surfaced via GetSystemStatus.
	Deleting bool
}

// VersionDeleteMode selects which versions DeleteVersion removes relative
// to the target version. It is the internal counterpart of the
// VersionDeleteMode proto enum; the service layer maps one to the other.
type VersionDeleteMode int

const (
	// VersionDeleteSubtree removes the target version together with every
	// descendant version. This is the original DeleteVersion semantics and
	// the default for callers that do not select a mode.
	VersionDeleteSubtree VersionDeleteMode = iota
	// VersionDeleteSingle removes only the target version itself: its
	// direct children are re-parented onto the target's parent, so the
	// branch structure below it is preserved. This is what lets an
	// arbitrary "middle" version be removed without dragging its
	// descendants along.
	VersionDeleteSingle
	// VersionDeleteAncestors removes every ancestor (前置版本) of the
	// target version, making the target the new base (root) of the
	// knowledge base. Sibling branches hanging off those ancestors are
	// removed together with them.
	VersionDeleteAncestors
)

// String returns a human-readable name for the mode, primarily for logging.
func (m VersionDeleteMode) String() string {
	switch m {
	case VersionDeleteSubtree:
		return "SUBTREE"
	case VersionDeleteSingle:
		return "SINGLE"
	case VersionDeleteAncestors:
		return "ANCESTORS"
	default:
		return "UNKNOWN"
	}
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

	// ParentVersionID and Changes are carried by
	// PendingRecordTypeVersionWrite only: the recovery path replays the
	// version's storage writes (steps 3-6 of the write path) from scratch
	// using these, so the WAL must persist them alongside the transaction.
	// A PendingRecord whose Changes is nil was written by an older WAL
	// format (BEGIN carried no payload) and cannot be replayed
	// automatically — the operator must intervene (see the WAL package
	// doc comment).
	ParentVersionID int64
	Changes         []DocChange
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
