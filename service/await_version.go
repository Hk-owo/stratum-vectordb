package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// AwaitVersion: the caller-facing half of docs/await-version-plan.md.
//
// CreateVersion answers as soon as the version's metadata is committed; the
// data write and the index build happen afterwards, elsewhere. "Has my write
// landed yet" is therefore a question only the replicated state can answer, and
// this file is how a caller asks it: read one version's metadata, derive a
// stage, and — when the wait has gone on long enough to be worth the cost —
// probe whether any reachable replica holds the data at all.
//
// Nothing here decides on the caller's behalf. A version that will never
// complete is reported as PENDING plus data_missing, never declared dead:
// whether to re-send the changes or discard the version depends on whether the
// caller still holds them, and the cluster cannot know that (§2.1).

// The stages a caller switches on. They are DERIVED from the two independent
// status fields rather than being a status themselves: "data durable, index
// still building" and "index ready, data still arriving" are both real, and
// folding them into one field would lose which side the caller is waiting for
// (§3).
const (
	StageDataPending          = "DATA_PENDING"
	StageDataDurable          = "DATA_DURABLE"
	StageIndexReady           = "INDEX_READY"
	StageIndexFailed          = "INDEX_FAILED"
	StageDataFailedPermanent  = "DATA_FAILED_PERMANENT"
	StageIndexFailedPermanent = "INDEX_FAILED_PERMANENT"
	StageDeleting             = "DELETING"
)

const (
	// awaitMaxWait bounds ONE AwaitVersion call. A waiting caller costs a
	// goroutine on the control node, so the server — not the caller's wish —
	// decides when a call must come back (§5 contract 5).
	awaitMaxWait = 5 * time.Second
	// awaitPollInterval is how often the loop re-reads the version. It is the
	// same order of magnitude as takeoverTimeout (200 ms) for the same reason:
	// that is the granularity at which this cluster notices anything.
	awaitPollInterval = 200 * time.Millisecond
	// awaitProbeTTL caches a data_missing verdict, so a caller polling every
	// 200 ms does not make the control node probe N replicas on every poll
	// (§6.4).
	awaitProbeTTL = 10 * time.Second
	// awaitProbeConcurrency caps in-flight probes per knowledge base. Over the
	// cap the answer is "unknown" (false) rather than a queue: await's latency
	// must not be spent waiting on its own probe (§6.4).
	awaitProbeConcurrency = 4
)

// AwaitVersion implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) AwaitVersion(ctx context.Context, req *pb.AwaitVersionRequest) (*pb.AwaitVersionResponse, error) {
	if req.GetKnowledgeBaseId() == "" {
		return nil, stratumerrors.ToGRPCStatus(fmt.Errorf("knowledge_base_id is required: %w", stratumerrors.ErrInvalidArgument))
	}
	target, err := awaitTargetFromProto(req.GetTarget())
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	wait := time.Duration(req.GetWaitTimeoutMs()) * time.Millisecond
	if wait <= 0 || wait > awaitMaxWait {
		wait = awaitMaxWait
	}
	deadline := time.Now().Add(wait)

	// A NotFound from the LOCAL read is ambiguous, and the difference matters:
	// it can mean "no such version" or "this node has not applied the entry
	// yet". On a follower a few hundred milliseconds behind, the two are
	// indistinguishable — and reporting the second as the first tells a caller
	// its perfectly healthy write never happened, with no way to tell them apart.
	// So the wait treats "cannot see it yet" as "not reached yet", and reports
	// NotFound only once the whole window has passed without it ever appearing.
	//
	// Measured on a live 3+3 cluster: a version the leader had committed was
	// invisible on a follower that the station then load-balanced the read to, so
	// a first await right after a write hit exactly this window.
	var everVisible bool
	for {
		// Register BEFORE reading. A change landing between the read and the
		// registration would otherwise be invisible until the next poll, and the
		// point of the watcher is that the wait costs what the change costs
		// (§12 item 1). The poll below stays as the fallback: a missed
		// notification may cost latency, never correctness.
		var changed <-chan struct{}
		var stopWatching func()
		if s.versionWatcher != nil {
			changed, stopWatching = s.versionWatcher.WatchVersion(req.GetVersionId())
		}
		stopIfWatching := func() {
			if stopWatching != nil {
				stopWatching()
			}
		}

		v, err := s.raftNode.GetVersion(ctx, req.GetKnowledgeBaseId(), req.GetVersionId())
		switch {
		case err == nil:
			everVisible = true
			stage := deriveStage(v)
			if awaitReached(stage, target) || awaitTerminal(stage) || !time.Now().Before(deadline) {
				stopIfWatching()
				return s.awaitResponse(ctx, v, stage), nil
			}
		case notVisibleYet(err) && !everVisible && time.Now().Before(deadline):
			// Keep waiting: this node may simply be behind.
		default:
			// No such knowledge base or version (§5 contract 2), or an error
			// that is not about visibility at all.
			stopIfWatching()
			return nil, stratumerrors.ToGRPCStatus(err)
		}

		select {
		case <-changed:
			// The state moved; go read it. A nil channel here (no watcher) simply
			// never fires, which is what leaves the poll as the only wake-up.
		case <-time.After(awaitPollInterval):
			// Fallback: covers a watcher that is absent or missed an event.
		case <-ctx.Done():
			stopIfWatching()
			// The caller hung up or ran out of budget: report its cancellation
			// instead of finishing the wait on its behalf (§5 contract 3).
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		stopIfWatching()
	}
}

// awaitResponse assembles the answer for the version's CURRENT state. Reaching
// the target and running out of the wait produce the same shape on purpose: the
// caller reads stage, and not having arrived is not an error (§5 contract 1).
func (s *KnowledgeBaseServiceImpl) awaitResponse(ctx context.Context, v types.VersionMeta, stage string) *pb.AwaitVersionResponse {
	return &pb.AwaitVersionResponse{
		Version:      versionInfoToProto(v),
		Stage:        stage,
		RetryAfterMs: int64(awaitPollInterval / time.Millisecond),
		DataMissing:  s.probeDataMissing(ctx, v, stage),
	}
}

// deriveStage folds the two status fields into the one value a caller switches
// on. Terminal verdicts come first (a retired version is not "still going"),
// then READY, then the rebuildable failure, and only then the in-progress
// states.
func deriveStage(v types.VersionMeta) string {
	switch {
	case v.Deleting:
		return StageDeleting
	case v.DataStatus == types.DataStatusFailedPermanent:
		return StageDataFailedPermanent
	case v.IndexStatus == types.IndexStatusFailedPermanent:
		return StageIndexFailedPermanent
	case v.IndexStatus == types.IndexStatusReady:
		return StageIndexReady
	case v.IndexStatus == types.IndexStatusFailed && v.DataStatus != types.DataStatusPending:
		// The index failed once the data had settled: that is the rebuildable
		// failure §3 names. While the data is still on its way, the data side is
		// the more informative of the two.
		return StageIndexFailed
	case v.DataStatus == types.DataStatusDurable:
		return StageDataDurable
	default:
		return StageDataPending
	}
}

// awaitReached reports whether stage already satisfies target. WAIT_FOR is
// inclusive in the obvious direction: durable data is past a DATA_DURABLE
// target, and READY implies both.
func awaitReached(stage string, target pb.AwaitTarget) bool {
	switch target {
	case pb.AwaitTarget_AWAIT_TARGET_DATA_DURABLE:
		return stage == StageDataDurable || stage == StageIndexReady
	default:
		return stage == StageIndexReady
	}
}

// awaitTerminal reports whether nothing further will happen to this version, so
// the caller has to stop waiting and decide what to do (§4.2): a rebuildable
// index failure, a permanent verdict on either side, or a delete in progress.
func awaitTerminal(stage string) bool {
	switch stage {
	case StageIndexFailed, StageDataFailedPermanent, StageIndexFailedPermanent, StageDeleting:
		return true
	}
	return false
}

// awaitTargetFromProto validates the caller's target instead of silently
// downgrading an unrecognized value: a caller that asked for the wrong thing
// should be told, not handed the default.
func awaitTargetFromProto(t pb.AwaitTarget) (pb.AwaitTarget, error) {
	switch t {
	case pb.AwaitTarget_AWAIT_TARGET_INDEX_READY, pb.AwaitTarget_AWAIT_TARGET_DATA_DURABLE:
		return t, nil
	default:
		return pb.AwaitTarget_AWAIT_TARGET_INDEX_READY, fmt.Errorf("unknown await target %d: %w", t, stratumerrors.ErrInvalidArgument)
	}
}

// VersionWatcher reports when a version's replicated state moves, so the await
// path can wake on the change instead of polling for it (docs/await-version-plan.md
// §12 item 1). Declared here, by the consumer, like the other narrow interfaces in
// this package.
//
// The returned channel is signal-only and coalesced: the waiter re-reads the
// state machine after every signal, so a dropped duplicate costs nothing. stop
// MUST be called (it is idempotent) or the registration outlives the wait.
type VersionWatcher interface {
	WatchVersion(versionID int64) (<-chan struct{}, func())
}

// SetVersionWatcher wires the event-driven half of the await path. Without it
// await polls, which is correct but pays up to awaitPollInterval of latency.
func (s *KnowledgeBaseServiceImpl) SetVersionWatcher(w VersionWatcher) {
	s.versionWatcher = w
}

// notVisibleYet reports whether err is the "I cannot see it" family: the version
// or its knowledge base is absent from THIS node's state machine. That is not
// the same question as "it does not exist" — locally the two are
// indistinguishable — so callers must not turn it into a NotFound on their own.
func notVisibleYet(err error) bool {
	return errors.Is(err, stratumerrors.ErrVersionNotFound) ||
		errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound)
}

// SetPresenceProbe wires the data_missing probe's inputs: the presence checker
// and the candidate-replica list AdminService's status view already uses
// (§6.4). Both optional — unwired, the probe is skipped and data_missing answers
// false, which reads as "unknown" and is never a claim that the data is there.
func (s *KnowledgeBaseServiceImpl) SetPresenceProbe(presence PresenceChecker, replicas func() []string) {
	s.presence = presence
	s.replicas = replicas
	if s.probeCache == nil {
		s.probeCache = newAwaitProbe()
	}
}

// probeDataMissing answers §6.4's question for ONE version: does any reachable
// candidate replica hold its data? It only asks while the version is still
// PENDING — once the data side has settled the answer is already known, and
// asking then would be misleading rather than merely useless.
func (s *KnowledgeBaseServiceImpl) probeDataMissing(ctx context.Context, v types.VersionMeta, stage string) bool {
	if stage != StageDataPending || s.presence == nil || s.replicas == nil || s.probeCache == nil {
		return false
	}

	// §12 item 2, the cheap half: the control leader's aggregate is what the
	// replicas themselves reported, so a NAMED holder settles the question without
	// probing anyone. An EMPTY answer does not settle the opposite — it means
	// "nobody I have heard from" (§3.1) — so the probe below stays as the
	// fallback, and a node that is not the leader (ok == false) skips straight to
	// it.
	if s.versionHolders != nil {
		if holders, ok := s.versionHolders.DataVersionHolders(v.KBID, v.VersionID); ok && len(holders) > 0 {
			return false
		}
	}
	return s.probeCache.lookupOrProbe(ctx, v, s.replicas(), s.presence)
}

// awaitProbe caches data_missing verdicts and bounds how many probes run at
// once.
//
// Both are about the same cost: a probe asks every candidate replica over gRPC,
// and the await path is a caller-facing one, so the work has to be bounded by
// something other than the caller's polling rate (§6.4). The maps grow with the
// number of knowledge bases (the concurrency slots) and with the number of
// versions probed (the verdicts), both of which are bounded by what the cluster
// holds; entries are small and never outlive the process.
type awaitProbe struct {
	mu      sync.Mutex
	entries map[string]awaitProbeEntry
	slots   map[string]chan struct{}
}

type awaitProbeEntry struct {
	missing bool
	at      time.Time
}

func newAwaitProbe() *awaitProbe {
	return &awaitProbe{
		entries: make(map[string]awaitProbeEntry),
		slots:   make(map[string]chan struct{}),
	}
}

// lookupOrProbe returns the cached verdict while it is fresh, and otherwise
// probes — unless this knowledge base already has awaitProbeConcurrency probes
// running, in which case it answers "unknown" (false) instead of queueing.
func (p *awaitProbe) lookupOrProbe(ctx context.Context, v types.VersionMeta, replicas []string, checker PresenceChecker) bool {
	key := fmt.Sprintf("%s\x00%d", v.KBID, v.VersionID)

	p.mu.Lock()
	if e, ok := p.entries[key]; ok && time.Since(e.at) < awaitProbeTTL {
		p.mu.Unlock()
		return e.missing
	}
	slot, ok := p.slots[v.KBID]
	if !ok {
		slot = make(chan struct{}, awaitProbeConcurrency)
		p.slots[v.KBID] = slot
	}
	p.mu.Unlock()

	select {
	case slot <- struct{}{}:
		defer func() { <-slot }()
	default:
		// Over the cap: "unknown" is the honest answer, and returning it keeps
		// await's latency independent of the probe's.
		return false
	}

	// dataMissingVersions carries the age threshold and the
	// unreachable-means-unknown rule; reusing it is what keeps this answer and
	// GetSystemStatus's from ever disagreeing about the same version.
	missing := len(dataMissingVersions(ctx, []types.VersionMeta{v}, replicas, checker, time.Now().Unix(), dataMissingMinAgeSec)) > 0

	p.mu.Lock()
	p.entries[key] = awaitProbeEntry{missing: missing, at: time.Now()}
	p.mu.Unlock()
	return missing
}

// versionInfoToProto is the single types→proto conversion for a version, shared
// by ListVersions and AwaitVersion so the two cannot drift into describing the
// same version differently.
func versionInfoToProto(v types.VersionMeta) *pb.VersionInfo {
	return &pb.VersionInfo{
		VersionId:       v.VersionID,
		ParentVersionId: v.ParentVersionID,
		CreatedAt:       v.CreatedAt,
		IndexStatus:     pb.IndexStatus(v.IndexStatus),
		Deleting:        v.Deleting,
		// The data side travels alongside the index side (§10.1b): callers that
		// only look at index_status cannot tell "the data never landed" from
		// "everything is fine".
		DataStatus: pb.DataStatus(v.DataStatus),
		// And the digest, which the storage layer needs whole: it is what
		// separates an empty version from one whose digest was never committed
		// (see the field's comment in the proto).
		DocIdSetHash: v.DocIDSetHash,
		// And which replicas have reported this version's index ready. The
		// storage layer reads versions THROUGH this contract — RemoteRaftNode over
		// gRPC — and it is the storage layer that runs §8.6(d)'s rolling cleanup,
		// so without this the serving count every replica computes is 0 and
		// collection can never start (see the field's comment in the proto).
		IndexReadyNodes: v.IndexReadyNodes,
	}
}
