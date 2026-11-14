// Building a version's index (§5.5, §5.6): priority, submission, the build itself
// (including the append-to-parent reuse), the retry that waits for a racing data
// sync, and the build-completion callback.
package index

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	vecstorepb "stratum/api/proto/vecstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"time"
)

// buildPriorityName is what the build-timings line reports for a request's
// priority. Spelled out rather than a boolean, because the reader's question is
// "was this build one somebody was waiting for" and the answer is the queue it
// went into.
func buildPriorityName(p BuildPriority) string {
	if p == BuildPriorityBackfill {
		return "backfill"
	}
	return "interactive"
}

// TriggerBuild implements IndexManager.
//
// Interactive priority: this is what EnsureIndex calls when someone is waiting for
// the index to answer a query or a confirmation. Reconcile used to call it too, which
// is why a post-restart sweep could outrank live work; reconcile now uses
// TriggerBuildBackfill below.
func (im *IndexManagerImpl) TriggerBuild(ctx context.Context, kbID string, versionID int64) error {
	return im.triggerBuild(kbID, versionID, im.coldShapeFor(ctx, kbID, versionID), BuildPriorityInteractive)
}

// TriggerBuildBackfill schedules a build nobody is waiting for yet — a head start,
// typically reconcile restoring what it can after a restart.
//
// It yields to every interactive build, so no backlog of these can starve a live
// write of its build.
func (im *IndexManagerImpl) TriggerBuildBackfill(ctx context.Context, kbID string, versionID int64) error {
	return im.triggerBuild(kbID, versionID, im.coldShapeFor(ctx, kbID, versionID), BuildPriorityBackfill)
}

func (im *IndexManagerImpl) triggerBuild(kbID string, versionID int64, graphFree bool, priority BuildPriority) error {
	key := indexKey{kbID, versionID}

	im.mu.Lock()
	if im.loading[key] {
		im.mu.Unlock()
		return nil // build already in progress
	}
	im.loading[key] = true
	im.mu.Unlock()

	// Reported with the lock released, and BEFORE the build is queued: the fact
	// is "a build is on its way", and the queueing below is what could refuse
	// it. A version already building is not an error — the early return above
	// makes this a no-op on the state machine, which treats a repeat as idempotent.
	im.reportBuildScheduled(kbID, versionID)

	// Queued, not spawned. The pool bounds how many builds run at once, and lets a
	// build somebody is waiting for jump ahead of a reconcile sweep's backfill.
	if !im.submitBuild(buildRequest{
		kbID:       kbID,
		versionID:  versionID,
		graphFree:  graphFree,
		priority:   priority,
		enqueuedAt: time.Now(),
	}) {
		// The pool is shut down and will never run this. Clear the in-progress marker
		// so the state stays truthful — a version that is NOT being built must not
		// look like one that is, or EnsureIndex would wait for a build that will never
		// arrive.
		im.mu.Lock()
		delete(im.loading, key)
		im.mu.Unlock()
		// The state machine needs the same correction, and it is exactly the case a
		// callback-based report would miss: no build ever ran, so no completion
		// callback will fire, and the version would read BUILDING forever.
		im.reportBuildFailed(kbID, versionID, "the build pool is closed")
		return errors.New("index: the build pool is closed")
	}
	return nil
}

// submitBuild hands a request to the pool, starting it on first use.
//
// Lazy on purpose: a manager that never builds anything — most unit tests — should
// start no workers at all, and deferring the startup to the first request puts the
// cost where the work is.
func (im *IndexManagerImpl) submitBuild(req buildRequest) bool {
	im.buildPoolOnce.Do(func() {
		pool := newBuildPool(im.cfg.BuildConcurrency, func(r buildRequest) {
			im.doBuild(r)
		}, im.logger)
		im.buildPoolMu.Lock()
		im.buildPool = pool
		im.buildPoolMu.Unlock()
	})
	return im.buildPool.Submit(req)
}

func (im *IndexManagerImpl) doBuild(req buildRequest) {
	kbID, versionID, graphFree := req.kbID, req.versionID, req.graphFree
	key := indexKey{kbID, versionID}

	status := types.IndexStatusReady
	var sizeBytes int64
	// evicted collects what makeRoomLocked had to drop to fit this index; it is
	// handed to the vecstore after the lock is released (dropEvicted).
	var evicted []indexKey

	// Per-stage timings of one build, at debug level.
	//
	// Why they are here: this is stage 6 of the write path — the version is
	// PENDING until this returns and the control layer records READY — and until
	// now the only thing a caller could observe was the wait itself. Queueing and
	// building are the two things that can make that wait long, and they call for
	// opposite responses (more build workers vs. a look at vecstore and the
	// batch), so the line separates them. build_us measures buildWithRetry, and
	// therefore includes any retries it needed.
	buildStart := time.Now()
	var buildUs time.Duration

	// Deferred cleanup guarantees that loading is ALWAYS cleared and
	// waiters are ALWAYS woken, even if build()/makeRoomLocked/panics
	// below blow up. Without this a wedged build goroutine leaves
	// loading[key] true forever and every acquire() on that version spins
	// in cond.Wait until its context dies — the pressure-test Query hang.
	defer func() {
		if r := recover(); r != nil {
			im.logger.Error("index build panicked; marking version FAILED",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Any("panic", r))
			status = types.IndexStatusFailed
		}

		// Emitted HERE, and not from a defer of its own, for two reasons.
		//
		// Ordering: this line must be in the log before anything downstream of
		// the build can act on its outcome — the waiters woken by the broadcast
		// below and the completion callbacks that carry the status to the
		// control layer. A separate defer registered above this one would run
		// after them (defers are LIFO), so a reader woken by the outcome would
		// race the line's arrival and could not rely on it to explain what it
		// just saw.
		//
		// Completeness: status is only final once the panic recovery above has
		// run, so a panicking build gets its line too, with the FAILED status
		// this build actually reported.
		if !req.enqueuedAt.IsZero() {
			im.buildLogger().Debug("index: build timings",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Bool("graph_free", graphFree), zap.String("priority", buildPriorityName(req.priority)),
				zap.Int64("queue_us", buildStart.Sub(req.enqueuedAt).Microseconds()),
				zap.Int64("build_us", buildUs.Microseconds()),
				zap.Int64("total_us", time.Since(req.enqueuedAt).Microseconds()),
				zap.String("status", status.String()),
				zap.Int64("size_bytes", sizeBytes))
		}

		im.mu.Lock()
		delete(im.loading, key)
		if status == types.IndexStatusReady {
			// Make room before inserting the new index, then release whatever it
			// evicted on the vecstore side once the lock is gone (see
			// dropEvicted).
			evicted = im.makeRoomLocked()
			// The key may ALREADY be resident: a version that is loaded and gets
			// rebuilt (needsReshape → TriggerBuild → here, §8.6a) arrives with its
			// entry in place. Replacing that entry wholesale loses two things that are
			// not this build's to lose (H8 of docs/code-review-2026-10-18.md):
			//
			//   - the refCount pins held by searches already in flight. release() looks
			//     up the CURRENT entry, so it decrements a fresh zero and does nothing,
			//     and the version becomes evictable — and Droppable in vecstore — while
			//     another goroutine is still searching it;
			//   - the old size in the accounting: loadedBytes only ever grew, so each
			//     rebuild added a whole index worth of bytes that nothing would ever
			//     subtract, and MemoryThresholdMB turned into a permanent eviction
			//     trigger.
			idx, ok := im.loaded[key]
			if !ok {
				idx = &loadedIndex{}
				im.loaded[key] = idx
			}
			idx.lastAccess = time.Now()
			if old, ok := im.sizeByKey[key]; ok {
				im.loadedBytes -= old
			}
			im.sizeByKey[key] = sizeBytes
			im.loadedBytes += sizeBytes
			// Remember which shape is now on disk, so a version whose
			// shape already matches its traffic is not rebuilt (§8.6a).
			im.builtGraphFree[key] = graphFree
			// A version nobody has searched yet has no access record;
			// seed one so the cold policy has a baseline to age from
			// instead of having to guess when an unqueried version was
			// last relevant.
			im.seedAccessLocked(key)
		}
		callbacks := append([]BuildCompleteCallback(nil), im.callbacks...)
		im.cond.Broadcast()
		im.mu.Unlock()
		im.dropEvicted(evicted)

		// Persist the size sidecar next to the index file so a later
		// restart (loadFromDisk) can account for this version's memory
		// footprint. Best-effort: a failed sidecar write only degrades
		// the estimate to 0.
		if status == types.IndexStatusReady {
			im.persistSizeSidecar(kbID, versionID, sizeBytes)
			// §8.4(a): put this version's retention shield on disk too, not
			// only in seedAccessLocked's in-memory table. The callback below
			// ships the artifact to the replicas, and the retention pass that
			// could drop it runs on the *next* build — possibly one that fires
			// while this distribution is still queued behind the push semaphore.
			im.recordInterestNow(kbID, versionID)
		}

		// Report the outcome to the node state machine before the completion
		// callbacks run. Same fact, two audiences: the callbacks carry the status
		// to the control layer, the state machine is the local half — and a reader
		// of one should not be able to miss the other.
		if status == types.IndexStatusReady {
			im.reportBuildSucceeded(kbID, versionID)
		} else {
			im.reportBuildFailed(kbID, versionID, "index build reported "+status.String())
		}

		// Invoke callbacks with retry. The on-disk retention policy is
		// NOT enforced here: EnforceDiskRetention needs to know the KB's
		// active version (to avoid dropping a rolled-back active
		// version's index), which requires the Raft layer — the
		// registered BuildCompleteCallback in cmd/stratum/main.go
		// applies the policy instead.
		for _, cb := range callbacks {
			im.invokeCallback(cb, kbID, versionID, status)
		}
	}()

	var err error
	sizeBytes, err = im.buildWithRetry(kbID, versionID, graphFree)
	buildUs = time.Since(buildStart)
	if err != nil {
		im.logger.Error("index build failed",
			zap.String("kb_id", kbID),
			zap.Int64("version_id", versionID),
			zap.Error(err))
		status = types.IndexStatusFailed
		if !isTransientBuildErr(err) {
			// 确定性失败（vecstore 拒绝了这批数据、参数不合法……）不会因为再试一次
			// 而变好：buildWithRetry 已经在 isTransientBuildErr 上认出了这一点并提前
			// 返回。上报时必须把同一判断带上——否则控制层收到的是「暂时失败」，把它
			// 记进重试预算，版本就一直停在 PENDING 等一个永远不会来的成功
			// （§10.1、cmd/stratum/main.go 的 reportIndexStatus）。
			status = types.IndexStatusFailedPermanent
		}
	}
}

// buildWithRetry runs build() inside a bounded window, retrying on
// failure so transient conditions self-heal instead of leaving the
// version FAILED/PENDING. Observed transient failures:
//   - vecstore "Save: no index has been built or loaded": a concurrent
//     build of the same (kb, version) can reset the shared HNSW index
//     between this node's Build and Save RPCs; a retry typically
//     succeeds once the other build finishes.
//   - follower builds racing the data sync: a pull may not yet have
//     landed all chunk-doc/version-doc entries when the build reads them,
//     so the chunk set comes back empty; a retry after the sync
//     converges succeeds.
//
// build() is idempotent (Build/AddChunks/Save re-write the same keys), so
// retrying is safe. The whole retry loop is bounded by buildRetryTimeout
// so a permanently-failing build still surfaces as FAILED instead of
// wedging the version forever.
// The defaults for IndexManagerConfig.BuildRetryTimeout / BuildRetryInterval.
const buildRetryTimeout = 5 * time.Minute

const buildRetryInterval = 2 * time.Second

// isTransientBuildErr reports whether a build failure is worth retrying:
// vecstore RPC failures of the "data not ready / index reset by a
// concurrent build / connection teardown" kind (FailedPrecondition,
// Unavailable, DeadlineExceeded, Internal). Deterministic errors (e.g. a
// caller-level error string) fail immediately so callers see a FAILED
// status promptly instead of a silent retry loop.
func isTransientBuildErr(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.FailedPrecondition, codes.Internal:
		return true
	}
	return false
}

// chunkVectorReadErr 给"读 chunk 向量失败"一个可判别的名字：vecstore 对尚未落盘的
// 向量答 NOT FOUND，而那是**数据侧的一个过渡态**，不是关于这次构建的判据。
//
// 写入路径先让文档与 chunk 映射可见、向量随后才落盘，所以构建恰好在这两者之间起跑
// 时，"要一个还在路上的向量"是合法的。isTransientBuildErr 把裸的 NOT FOUND 读成
// 确定性失败，一次竞态就退掉整个版本 —— 3+3 集群实测：一个副本的数据还在落盘，它用
// 暴力扫描回答了查询（§7.4 有意保留的容忍），同一次查询触发了 lazy build，其中一个
// chunk 的向量读回 NOT FOUND，于是版本被标 FAILED_PERMANENT：包括两个已经建好索引的
// 副本在内，**任何副本上都不再可查**，直到运维重建。
//
// 换成 FailedPrecondition（并保留原文）就把这一例交给 buildWithRetry —— 它在
// buildRetryTimeout 内等数据落齐，构造函数内注释里"抢在同步前面的构建，重试即可收敛"
// 说的正是这件事。其余错误码原样保留，确定性失败照旧立刻上报。
func chunkVectorReadErr(chunkID string, err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("index: read chunk vector %s: not stored yet (the data is still landing): %w",
			chunkID, status.Errorf(codes.FailedPrecondition, "%v", err))
	}
	return fmt.Errorf("index: read chunk vector %s: %w", chunkID, err)
}

func (im *IndexManagerImpl) buildWithRetry(kbID string, versionID int64, graphFree bool) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), im.cfg.BuildRetryTimeout)
	defer cancel()

	var lastErr error
	for {
		// skipReuse false: a normal build, where starting from the parent's
		// artifact is the cheaper path (§8.6(c)).
		sizeBytes, err := im.build(ctx, kbID, versionID, graphFree, false)
		if err == nil {
			return sizeBytes, nil
		}
		lastErr = err
		if !isTransientBuildErr(err) {
			return 0, err
		}
		select {
		case <-ctx.Done():
			return 0, lastErr
		case <-time.After(im.cfg.BuildRetryInterval):
		}
	}
}

// build executes the full build data flow and returns the estimated
// in-memory footprint of the built index (sum of vector payload bytes;
// 0 for an empty version). It reports success only if the index was also
// persisted to disk (see saveToDisk), so a failed save surfaces as a
// build failure.
func (im *IndexManagerImpl) build(ctx context.Context, kbID string, versionID int64, graphFree bool, skipReuse bool) (int64, error) {
	// Forward the KB-level quantizer config with every Build RPC; OFF
	// (default) keeps the historical full-precision index type.
	quantizerType, pqM, pqNBits := im.buildQuantizerFromKB(ctx, kbID, graphFree)
	// Last reported in-memory estimate from the vecstore (0 = unreported).
	reportedMemBytes := int64(0)

	docIDs, err := im.listDocIDs(ctx, kbID, versionID)
	if err != nil {
		return 0, fmt.Errorf("index: ListDocIDs: %w", err)
	}

	chunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, docIDs)
	if err != nil {
		return 0, fmt.Errorf("index: ListChunkIDsByDocs: %w", err)
	}

	// 码本刷新判定（docs/codebook-refresh-plan.md §3）：只在"追加复用本来会发生"
	// 时才需要 —— 判定成立就把它关掉，让下面走全量重建，而全量重建正是重训码本
	// 的唯一途径。放在 appendBase 之前，是为了不先白付一次 LoadForAppend。
	if !skipReuse {
		if needed, reason, ratio := im.codebookRefreshNeeded(ctx, kbID, versionID, quantizerType, len(chunkIDs)); needed {
			skipReuse = true
			im.logger.Info("index: full rebuild to refresh the quantizer codebook (§3)",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.String("reason", reason),
				zap.Float64("cumulative_drift_ratio", ratio),
				zap.Int("total_chunks", len(chunkIDs)),
				zap.String("quantizer", quantizerType.String()))
		}
	}

	// §8.6(c): a pure-append version can start from its parent's artifact and
	// add only the new chunks, instead of rebuilding the whole graph. Purely
	// an optimisation — a failure here falls back to the full build below,
	// and nothing downstream (callback, distribution, retention) can tell
	// which path produced the artifact.
	//
	// skipReuse turns it off, and two callers need that: §8.6(d)'s graphed
	// collection, and §3's codebook refresh just above. The reasoning is the
	// same shape for both — the rebuild exists to discard something the parent's
	// artifact would otherwise carry in: tombstones in the first case, a stale
	// codebook in the second (a quantized version's existing codes cannot be
	// re-encoded under a new codebook in place, so a full rebuild is the only
	// way to refresh it).
	if !skipReuse {
		if parentID, delta, dead, ok := im.appendBase(ctx, kbID, versionID, graphFree, chunkIDs); ok {
			size, appendErr := im.buildFromBase(ctx, kbID, versionID, parentID, delta, dead, len(chunkIDs), graphFree)
			if appendErr == nil {
				im.logger.Info("index: built by appending to the parent version's artifact (§8.6c)",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
					zap.Int64("parent_version_id", parentID),
					zap.Int("total_chunks", len(chunkIDs)), zap.Int("delta_chunks", len(delta)),
					zap.Float64("append_delta_ratio", float64(len(delta))/float64(max(len(chunkIDs), 1))),
					zap.Int("deleted_chunks", len(dead)), zap.Bool("graph_free", graphFree))
				return size, nil
			}
			if errors.Is(appendErr, errAppendTooManyTombstones) {
				// Not a failure: the reuse was legal, but the base carried too much
				// dead weight, so rebuilding (which drops it) is the better trade.
				// The ratio is reported here too: this path still pays for a full
				// rebuild, and without the number beside it there is no way to see
				// which versions keep drifting past the limit
				// (docs/content-defined-chunking-plan.md §5).
				im.logger.Info("index: append reuse not worth it; rebuilding from scratch",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
					zap.Int64("parent_version_id", parentID),
					zap.Int("total_chunks", len(chunkIDs)), zap.Int("delta_chunks", len(delta)),
					zap.Float64("append_delta_ratio", float64(len(delta))/float64(max(len(chunkIDs), 1))),
					zap.Error(appendErr))
			} else {
				im.logger.Warn("index: append reuse failed; rebuilding from scratch",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
					zap.Int64("parent_version_id", parentID), zap.Error(appendErr))
			}
		} else {
			// No reuse AND no line about it: the silent full rebuild. A version this
			// node received from a peer had no in-memory shape to check, so appendBase
			// declined and this path left no trace at all — which is why "only the
			// parent's builder ever reuses" was invisible in production logs. One line
			// per version, the same volume as "built by appending" below, so the two
			// can be counted against each other (§8.6c reuse rate).
			im.logger.Info("index: built from scratch — no reusable parent artifact on this node (§8.6c)",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.Int("total_chunks", len(chunkIDs)), zap.Bool("graph_free", graphFree))
		}
	}

	// 分批读取并发送：单条 Build/AddChunks RPC 的载荷必须小于 gRPC 默认
	// 4 MiB 上限。第一批用 Build 全量建索引，后续批用 AddChunks 增量追加。
	batches, sizeBytes, err := im.collectChunkBatches(ctx, kbID, chunkIDs)
	if err != nil {
		return 0, err
	}

	// 空版本（没有 chunk）：vecstore 侧不建索引。
	//
	// 这里曾经调一次 Build(empty) 再 Save，期望在 vecstore 侧"建立该
	// (kb,version) 的索引条目"。实测不成立：vecstore 的
	// AddChunksLocked 对空 batch 直接返回 OkStatus，不创建 Faiss 索引，
	// 于是紧随其后的 Save 永远报
	// "Save: no index has been built or loaded"（FailedPrecondition）。
	// 该错误被 isTransientBuildErr 判为可重试，构建便进入 5 分钟重试窗
	// 口，loading[key] 一直为真 —— 期间对该版本的任何查询都以
	// "index load timeout" 失败（integration 的 TwoNodeReplication /
	// FaultTolerance 就是这么被拖垮的：KB 初始版本 v1 是空版本）。
	//
	// 空版本没有可检索内容，构建到此即完成：查询由 tryBruteForce 直接
	// 回答空结果（chunk 数 0 远低于 BruteForceMaxChunks），不需要、也
	// 无法在 vecstore 侧留下一个空索引文件。
	if len(batches) == 0 {
		// "没有 chunk" 有两种来源，而它们完全不同，不该共用一句话。
		//
		// 一种是版本本身没有文档：正常，上面的推理到此为止。
		//
		// 另一种是版本有文档、却一个 chunk 都没产出——切分没吐出内容，或者每
		// 一次嵌入都失败了却没有让调用方失败。这一种从前也走这里，记一行 Info
		// 就算完，于是集群少一个依赖（实测：mock-embed 没起来）时，写入是半成
		// 功的：版本提交了、数据没落地、索引永远 PENDING，查询被 FailedPrecondition
		// 一直拒，而日志里只有一行看起来无害的 "nothing to build"。把异常那一
		// 种以 Error 记出来，让静默半成功至少不再静默。
		if im.listDocIDs != nil {
			if ids, err := im.listDocIDs(ctx, kbID, versionID); err == nil && len(ids) > 0 {
				im.logger.Error("index: version has documents but produced no chunks; its data never landed",
					zap.String("kb_id", kbID),
					zap.Int64("version_id", versionID),
					zap.Int("documents", len(ids)))
				// Reporting is not enough (H7 of docs/code-review-2026-10-18.md).
				// Returning (0, nil) marked this build as SUCCEEDED: doBuild kept the
				// status at READY, inserted the version into `loaded` and told the
				// control layer this node serves it. But nothing was ever built in
				// vecstore — this branch never calls Build/Save — so every later query
				// was a `loaded` hit, went to vecstore, got NOT_FOUND, and failed with
				// ErrIndexNotReady. Because the version was in `loaded`, the lazy
				// rebuild path was unreachable too: the version was permanently
				// unqueryable on this node while being routed here as ready.
				//
				// A plain (non-status) error, i.e. a DETERMINISTIC failure: retrying
				// the same missing data cannot succeed, and marking it retryable is the
				// mistake this branch's own history records — a transient verdict puts
				// the build into the 5-minute retry window, keeps loading[key] true the
				// whole time, and turns every query for the version into an index load
				// timeout. The control layer records FAILED_PERMANENT instead, and an
				// operator re-triggers the build once the data has actually landed.
				return 0, fmt.Errorf(
					"index: %s v%d has %d documents but produced no chunks: its data never landed, so there is nothing to build",
					kbID, versionID, len(ids))
			}
		}
		im.logger.Info("index: version has no chunks; nothing to build",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
		return 0, nil
	}

	for i, batch := range batches {
		chunks := make([]*vecstorepb.ChunkVectorProto, 0, len(batch))
		for _, cv := range batch {
			chunks = append(chunks, &vecstorepb.ChunkVectorProto{ChunkId: cv.id, Vector: cv.vec})
		}
		if i == 0 {
			resp, buildErr := im.vectorIndexClient.Build(ctx, &vecstorepb.BuildIndexRequest{
				KbId:      kbID,
				VersionId: versionID,
				Chunks:    chunks,
				Metric:    vecstorepb.MetricTypeProto_COSINE,
				Quantizer: quantizerType,
				PqM:       pqM,
				PqNbits:   pqNBits,
			})
			if buildErr != nil {
				return 0, fmt.Errorf("index: Build RPC: %w", buildErr)
			}
			reportedMemBytes = resp.GetMemBytes()
		} else {
			resp, addErr := im.vectorIndexClient.AddChunks(ctx, &vecstorepb.AddChunksRequest{
				KbId:      kbID,
				VersionId: versionID,
				Chunks:    chunks,
			})
			if addErr != nil {
				return 0, fmt.Errorf("index: AddChunks RPC: %w", addErr)
			}
			reportedMemBytes = resp.GetMemBytes()
		}
	}

	// Memory accounting (Stratum_设计文档v12.md 3.3): for quantized KBs the
	// vecstore reports the index's resident estimate on the coarse-
	// retriever basis (HNSW graph edges + quantized codes); prefer that
	// over the 4×d×n vector-payload estimate (which is only right for
	// full-precision OFF indexes). OFF KBs and unreported quantized
	// builds keep the historical sizeBytes estimate.
	if quantizerType != vecstorepb.QuantizerTypeProto_QUANTIZER_OFF && reportedMemBytes > 0 {
		sizeBytes = reportedMemBytes
	}

	// Persist the finished index to disk — the durable fact that READY
	// status is derived from. A failed save means the build is not
	// durable, so it is reported as a build failure (the caller marks the
	// version FAILED instead of READY).
	return sizeBytes, im.saveToDisk(ctx, kbID, versionID)
}

// maxBuildMessageBytes 是单次 Build/AddChunks RPC 载荷的字节预算上限。
// gRPC 默认最大消息 4 MiB（4194304 字节）；预算取一半，给 chunk_id 与
// protobuf 序列化开销留出余量。预算按 chunk 粒度切分，若单个 chunk 的向量
// 本身已超过预算，该批会如实超限（现实中 embedding 维度远达不到该量级）。
const maxBuildMessageBytes = 2 * 1024 * 1024

// chunkVec 是单个 chunk 的向量载荷（chunk_id + 向量）。
type chunkVec struct {
	id  string
	vec []float32
}

// appendBase reports whether versionID's index can be built by extending its
// parent version's artifact rather than rebuilding from scratch (§8.6c "pure
// append"), and if so returns the parent's id and the chunks that are new.
//
// The reuse needs all of:
//   - a parent version (the version chain is linear, §6);
//   - that parent's artifact still present on this node's disk;
//   - this node knowing the parent's shape, and it being the shape this build
//     wants — extending a graph-free artifact into a graphed index would have
//     to rebuild anyway (and vice versa), and right after a restart nothing is
//     known, so the conservative answer there is "no";
//   - at least one chunk to append (with none, reuse is a copy, not a saving).
//
// Deletions do NOT disqualify the reuse: HNSW cannot remove vectors, so a
// version that deletes documents leaves dead vectors in the base — but they
// are filtered out of results by the read path (the chunk→doc mapping is
// cleared for a deleted document, and the version's doc list confirms it), so
// they cost memory and candidate slots rather than correctness. How much of
// the base is dead is only known after the base is loaded (its `ntotal`),
// which is why buildFromBase checks the ratio and may still fall back.
func (im *IndexManagerImpl) appendBase(
	ctx context.Context, kbID string, versionID int64, graphFree bool, chunkIDs []string,
) (parentID int64, delta []string, dead []string, ok bool) {
	if im.versionParent == nil || im.cfg.IndexDataDir == "" || len(chunkIDs) == 0 {
		return 0, nil, nil, false
	}
	parent, err := im.versionParent(ctx, kbID, versionID)
	if err != nil || parent <= 0 || parent == versionID {
		return 0, nil, nil, false
	}
	// The artifact must be on this node. The files are the fact; the
	// ExistsIndex RPC is not used as the criterion (some vecstore builds
	// answer Unimplemented for it).
	if !fileExists(im.indexPath(kbID, parent)) || !fileExists(im.sidecarPath(kbID, parent)) {
		return 0, nil, nil, false
	}
	// The base's shape must be known here and match what this build wants.
	//
	// Through shapeGraphFree, not the in-memory record alone: a parent this node
	// received by §8.4 distribution has no builtGraphFree entry, and reading that
	// as "unknown shape" is what made every replica but the parent's builder
	// rebuild each child from scratch — the reuse existed on paper (the shape line
	// travels in the sidecar) but never triggered there. A shape that still cannot
	// be read (a sidecar written before §8.6a) stays unknown, and this returns
	// false: the old conservative rebuild.
	parentGraphFree, known := im.shapeGraphFree(kbID, parent)
	if !known || parentGraphFree != graphFree {
		im.buildLogger().Debug("index: §8.6c reuse skipped — the parent's shape is not usable here",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.Int64("parent_version_id", parent),
			zap.Bool("parent_graph_free", parentGraphFree), zap.Bool("parent_shape_known", known),
			zap.Bool("wanted_graph_free", graphFree))
		return 0, nil, nil, false
	}

	parentDocIDs, err := im.listDocIDs(ctx, kbID, parent)
	if err != nil {
		return 0, nil, nil, false
	}
	parentChunkIDs, err := im.listChunkIDsByDocs(ctx, kbID, parentDocIDs)
	if err != nil || len(parentChunkIDs) == 0 {
		return 0, nil, nil, false
	}
	present := make(map[string]bool, len(chunkIDs))
	for _, id := range chunkIDs {
		present[id] = true
	}
	parentSet := make(map[string]bool, len(parentChunkIDs))
	for _, id := range parentChunkIDs {
		parentSet[id] = true
	}
	delta = make([]string, 0, len(chunkIDs))
	for _, id := range chunkIDs {
		if !parentSet[id] {
			delta = append(delta, id)
		}
	}
	if len(delta) == 0 {
		// Nothing new: the parent's artifact already describes this version's
		// chunk set (or is a superset of it), so reusing it would be a copy at
		// best — and if this version deleted anything, the dead vectors would
		// come along for nothing. Rebuild from scratch.
		return 0, nil, nil, false
	}
	// Chunks the parent holds and this version no longer does: the deletions of
	// this step. A graph-free base can drop their vectors outright (§8.6c's
	// RemoveChunks); a graphed one has to carry them as tombstones until the
	// dead-vector ratio forces a rebuild.
	dead = make([]string, 0)
	for _, id := range parentChunkIDs {
		if !present[id] {
			dead = append(dead, id)
		}
	}
	return parent, delta, dead, true
}

// buildFromBase extends the parent's artifact with delta and seals it for
// (kbID, versionID) (§8.6c). The outcome is an ordinary artifact of this
// version: Load restores the retrieval mode from the stored type, so the
// base's shape carries over unchanged — which is exactly why appendBase only
// reuses a base whose shape matches the build's target.
func (im *IndexManagerImpl) buildFromBase(
	ctx context.Context, kbID string, versionID, parentID int64, delta, dead []string,
	totalChunks int, graphFree bool,
) (int64, error) {
	resp, err := im.vectorIndexClient.LoadForAppend(ctx, &vecstorepb.LoadIndexForAppendRequest{
		KbId: kbID, VersionId: versionID, Path: im.indexPath(kbID, parentID),
	})
	if err != nil {
		return 0, fmt.Errorf("index: LoadForAppend RPC: %w", err)
	}
	baseNtotal := resp.GetBaseNtotal()

	// §8.6(c) deletions, cheap path: a graph-free base can drop the vectors
	// this version no longer needs, so they never become tombstones at all
	// (faiss compacts IndexFlatCodes; a graphed index cannot remove). The
	// build is still open here — Save is what seals it — which is exactly the
	// window RemoveChunks requires.
	if graphFree && len(dead) > 0 {
		removed, removeErr := im.removeDeadChunks(ctx, kbID, versionID, dead)
		if removeErr != nil {
			return 0, fmt.Errorf("index: RemoveChunks RPC (append reuse): %w", removeErr)
		}
		baseNtotal -= removed
	}

	// How much of the base this version still no longer needs, now that the
	// deletions we can identify are dropped. The base holds baseNtotal vectors
	// and this version needs len(delta) of them to be new; everything else is
	// either still in use or dead (a document deleted by an ancestor and
	// carried along by an earlier reuse). |base ∩ thisVersion| <=
	// totalChunks-len(delta), so this is an upper bound on the dead weight —
	// erring towards rebuilding, the safe direction: a rebuild is slower, a
	// bloated index is wrong for longer.
	deadWeight := baseNtotal - int64(totalChunks-len(delta))
	if deadWeight < 0 {
		deadWeight = 0
	}
	if limit := im.appendMaxDeadRatio(); limit < 1.0 {
		total := baseNtotal + int64(len(delta))
		if total > 0 && float64(deadWeight)/float64(total) > limit {
			return 0, fmt.Errorf("%w: %d of %d vectors are dead, ratio limit is %.2f",
				errAppendTooManyTombstones, deadWeight, total, limit)
		}
	}

	batches, deltaBytes, err := im.collectChunkBatches(ctx, kbID, delta)
	if err != nil {
		return 0, err
	}
	reportedMemBytes := int64(0)
	for _, batch := range batches {
		chunks := make([]*vecstorepb.ChunkVectorProto, 0, len(batch))
		for _, cv := range batch {
			chunks = append(chunks, &vecstorepb.ChunkVectorProto{ChunkId: cv.id, Vector: cv.vec})
		}
		addResp, addErr := im.vectorIndexClient.AddChunks(ctx, &vecstorepb.AddChunksRequest{
			KbId: kbID, VersionId: versionID, Chunks: chunks,
		})
		if addErr != nil {
			return 0, fmt.Errorf("index: AddChunks RPC (append reuse): %w", addErr)
		}
		reportedMemBytes = addResp.GetMemBytes()
	}

	if err := im.saveToDisk(ctx, kbID, versionID); err != nil {
		return 0, err
	}
	// Memory accounting: prefer what the vecstore reports for the whole
	// resident structure (base + delta); otherwise the parent's recorded
	// footprint plus the delta payload.
	if reportedMemBytes > 0 {
		return reportedMemBytes, nil
	}
	return im.readSizeSidecar(kbID, parentID) + deltaBytes, nil
}

// errAppendTooManyTombstones reports that pure-append reuse is legal but not
// worth it: too much of the base artifact is dead weight for the version being
// built, so a full rebuild (which drops the dead vectors) is the better trade.
var errAppendTooManyTombstones = errors.New("index: append reuse skipped, too many tombstoned vectors")

// collectChunkBatches 逐个读取 chunk 向量，并按估算字节数切分成多个批次，
// 使得每批序列化后都不会超过 maxBuildMessageBytes。同时返回所有 chunk 向量
// 载荷的总字节数（4 × 维度 × chunk 数），作为该版本索引内存占用的估算。
func (im *IndexManagerImpl) collectChunkBatches(ctx context.Context, kbID string, chunkIDs []string) ([][]chunkVec, int64, error) {
	var batches [][]chunkVec
	var cur []chunkVec
	curBytes := 0
	var sizeBytes int64
	for _, chunkID := range chunkIDs {
		v, err := im.readChunkVector(ctx, kbID, chunkID)
		if err != nil {
			return nil, 0, chunkVectorReadErr(chunkID, err)
		}
		// 估算该 chunk 在请求中的字节开销：向量(float32) + chunk_id + 字段头。
		est := 4*len(v) + len(chunkID) + 64
		if len(cur) > 0 && curBytes+est > maxBuildMessageBytes {
			batches = append(batches, cur)
			cur = nil
			curBytes = 0
		}
		cur = append(cur, chunkVec{id: chunkID, vec: v})
		curBytes += est
		sizeBytes += int64(4 * len(v))
	}
	if len(cur) > 0 {
		batches = append(batches, cur)
	}
	return batches, sizeBytes, nil
}

// invokeCallback calls cb with exponential backoff retry.
func (im *IndexManagerImpl) invokeCallback(cb BuildCompleteCallback, kbID string, versionID int64, status types.IndexStatus) {
	base := time.Duration(im.cfg.CallbackRetryBaseMS) * time.Millisecond
	if base <= 0 {
		base = 200 * time.Millisecond
	}
	maxRetries := im.cfg.CallbackMaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	// 记下最后一次的 err 并报出去。原实现把 err 的作用域关在 if 里，于是失败时
	// 只留下"重试耗尽"——而**为什么**耗尽正是唯一的诊断信息。定位这个问题时，
	// 就是因为这一行为空，才只能一路读代码去猜失败落在了哪一环。
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := cb(kbID, versionID, status)
		if err == nil {
			return
		}
		// 版本已经不在了——被 `DiscardVersion` 放弃，或被 `DeleteVersion` 删掉，
		// 而索引构建这时可能才刚跑完。它的上报撞上 "version not found" 是**预期
		// 结果**，不是失败：放弃一个版本的语义本来就是"当它没存在过"，那么之后
		// 为它上报索引就绪自然无处可报。
		//
		// 不识别这一条，每次 discard 都会留下 4 次退避重试（200+400+800ms）和一
		// 条 error 级日志——把一次正常的放弃渲染成看起来像故障的东西，而真正的
		// 故障会淹在这片噪音里。
		if errors.Is(err, stratumerrors.ErrVersionNotFound) {
			im.logger.Debug("build callback: version is gone (discarded or deleted); nothing to report",
				zap.String("kb_id", kbID),
				zap.Int64("version_id", versionID),
				zap.String("status", status.String()),
			)
			return
		}
		lastErr = err
		if attempt < maxRetries {
			backoff := base * time.Duration(int64(math.Pow(2, float64(attempt))))
			time.Sleep(backoff)
		}
	}
	im.logger.Error("build callback retries exhausted",
		zap.String("kb_id", kbID),
		zap.Int64("version_id", versionID),
		zap.String("status", status.String()),
		zap.Int("attempts", maxRetries+1),
		zap.Error(lastErr),
	)
}

// RegisterBuildCallback implements IndexManager.
func (im *IndexManagerImpl) RegisterBuildCallback(cb BuildCompleteCallback) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.callbacks = append(im.callbacks, cb)
}
