import { useEffect, useState } from 'react'

import { appendHistory } from '../history/store'
import {
  DataStatus,
  IndexStatus,
  VersionDeleteMode,
  type VersionInfo,
} from '../api/gen/knowledgebase'
import {
  useDeleteVersion,
  useDiscardVersion,
  useKnowledgeBase,
  useRebuildIndex,
  useRollbackVersion,
  useVersions,
  useWarmupVersion,
} from '../api/queries'
import { chainTail } from '../ingest/chainTail'
import { resendPending } from '../pending/resend'
import { loadPending, type PendingWrite } from '../pending/store'

/**
 * 版本页。
 *
 * 这里要**同时**展示两个状态位（`DataStatus` 与 `IndexStatus`），因为它们互相
 * 独立（§10.1b）：一个版本可以在数据还在写的时候就可查，也可以在数据持久之后
 * 完全没有索引。把它们折成一个"状态"会让人分不清该重试写入还是该重建索引——
 * 这正是判死时 `side` 要一起报出来的同一个理由。
 *
 * `doc_id_set_hash` 也展示：它是判断"两个版本的文档集是否同一份"的唯一凭据
 * （空的文档集有它自己的摘要，与"摘要从未提交"不同）。
 */
export function Versions({ kbId }: { kbId: string | null }) {
  const kb = useKnowledgeBase(kbId)
  const versions = useVersions(kbId)

  const rollback = useRollbackVersion(kbId ?? '')
  const del = useDeleteVersion(kbId ?? '')
  const discard = useDiscardVersion(kbId ?? '')
  const rebuild = useRebuildIndex(kbId ?? '')
  const warmup = useWarmupVersion(kbId ?? '')

  /**
   * 本机的在途记录。版本页看到 `DATA_STATUS_PENDING` 时，**能不能救取决于这里
   * 有没有那份 changes**——服务端从不保存 changes（只有一份 `doc_id_set_hash`
   * 摘要），所以"续传"只可能由当初提交它的那个客户端发起。
   */
  const [local, setLocal] = useState<PendingWrite[]>([])
  const [resending, setResending] = useState<string | null>(null)
  const [resendNote, setResendNote] = useState<Record<string, string>>({})

  useEffect(() => {
    void loadPending()
      .then(setLocal)
      .catch(() => setLocal([]))
  }, [])

  if (kbId === null) return <p className="placeholder">先在上面选一个知识库。</p>

  const activeId = kb.data?.knowledge_base?.active_version_id
  const list = [...(versions.data?.versions ?? [])].sort(
    (a, b) => Number(b.version_id) - Number(a.version_id), // 新的在前；int64 是字符串
  )

  /** 这个版本在本机有没有对应的在途记录——有，才谈得上重发。 */
  function recordFor(versionId: string): PendingWrite | undefined {
    return local.find((w) => w.knowledge_base_id === kbId && w.version_id === versionId)
  }

  async function resend(w: PendingWrite) {
    setResending(w.id)
    setResendNote((p) => ({ ...p, [w.id]: '重发中…' }))
    try {
      const outcome = await resendPending(w, chainTail(versions.data?.versions ?? []), {
        onProgress: (stage) => setResendNote((p) => ({ ...p, [w.id]: `等待中 · ${stage}` })),
      })
      // 只有真的落地才算"重发成功"——DISCARD 意味着数据找不回来，那是另一种结局，
      // 值得在历史里分开记，而不是混进"我重发过了"。
      void appendHistory({
        kind: 'resend',
        knowledge_base_id: w.knowledge_base_id,
        detail:
          outcome.decision === 'DONE'
            ? `v${outcome.versionId} 落地（${w.changes.length} 处变更）`
            : outcome.decision === 'DISCARD'
              ? `放弃：数据已不可找回（${w.changes.length} 处变更）`
              : `未到终局：${outcome.stage}`,
      })
      setResendNote((p) => ({
        ...p,
        [w.id]:
          outcome.decision === 'DONE'
            ? `已落地 v${outcome.versionId}`
            : outcome.decision === 'DISCARD'
              ? '数据已不可找回，记录已清除'
              : `${outcome.stage} —— 可以再试一次`,
      }))
      setLocal(await loadPending())
    } catch (err) {
      setResendNote((p) => ({
        ...p,
        [w.id]: `重发失败：${err instanceof Error ? err.message : String(err)}`,
      }))
    } finally {
      setResending(null)
    }
  }

  const err = versions.error ?? kb.error ?? rollback.error ?? del.error ?? discard.error
  const busy = rollback.isPending || del.isPending || discard.isPending || rebuild.isPending

  return (
    <div className="page">
      {err !== null && err !== undefined && (
        <p className="error-box">{err instanceof Error ? err.message : String(err)}</p>
      )}

      {versions.isPending && <p className="muted">加载版本链…</p>}
      {versions.data !== undefined && list.length === 0 && (
        <p className="placeholder">
          这个库还没有版本。`CreateKnowledgeBase` 不创建版本（initial_version_id 恒为 0）
          ——第一个版本要等真正有内容时才出现。
        </p>
      )}

      {list.length > 0 && (
        <table className="queue-table">
          <thead>
            <tr>
              <th>版本</th>
              <th>数据</th>
              <th>索引</th>
              <th>文档集摘要</th>
              <th>创建时间</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {list.map((v) => (
              <tr key={v.version_id} className={v.version_id === activeId ? 'row-active' : undefined}>
                <td>
                  <code>v{v.version_id}</code>
                  {v.version_id === activeId && <span className="chip ok">激活</span>}
                  {v.parent_version_id !== '0' && (
                    <code className="muted small"> ← v{v.parent_version_id}</code>
                  )}
                  {v.deleting && <span className="chip warn">清理中</span>}
                </td>
                <td>
                  <span className={chipClass(v.data_status === DataStatus.DATA_STATUS_DURABLE)}>
                    {shortStatus(v.data_status)}
                  </span>
                </td>
                <td>
                  <span
                    className={chipClass(
                      v.index_status === IndexStatus.INDEX_STATUS_READY,
                      v.index_status === IndexStatus.INDEX_STATUS_FAILED ||
                        v.index_status === IndexStatus.INDEX_STATUS_FAILED_PERMANENT,
                    )}
                  >
                    {shortStatus(v.index_status)}
                  </span>
                </td>
                <td>
                  <code className="muted small" title={v.doc_id_set_hash}>
                    {v.doc_id_set_hash === '' ? '（未提交）' : v.doc_id_set_hash.slice(0, 12)}
                  </code>
                </td>
                <td className="muted small">{formatUnix(v.created_at)}</td>
                <td className="actions">
                  <button
                    type="button"
                    disabled={busy || v.version_id === activeId}
                    title={v.version_id === activeId ? '已经是激活版本' : '把激活版本切到这一个（发布 / 回滚）'}
                    onClick={() =>
                      rollback.mutate(
                        { knowledge_base_id: kbId, target_version_id: v.version_id },
                        {
                          onSuccess: () =>
                            void appendHistory({
                              kind: 'activate',
                              knowledge_base_id: kbId,
                              detail: `激活 v${v.version_id}`,
                            }),
                        },
                      )
                    }
                  >
                    激活
                  </button>
                  {/* rebuild 只对 FAILED 有意义；对 FAILED_PERMANENT 无效——那只有运维能处置。 */}
                  {v.index_status === IndexStatus.INDEX_STATUS_FAILED && (
                    <button
                      type="button"
                      disabled={busy}
                      onClick={() => rebuild.mutate({ knowledge_base_id: kbId, version_id: v.version_id })}
                    >
                      重建索引
                    </button>
                  )}
                  <button
                    type="button"
                    disabled={busy}
                    title="把索引载入内存（不改激活版本）；产物会因此受保留策略保护"
                    onClick={() => warmup.mutate({ knowledge_base_id: kbId, version_id: v.version_id })}
                  >
                    预热
                  </button>
                  {/* discard 只接受仍 PENDING 的版本：它针对"从未落地"的写。
                      已经落地的版本属于 delete-version。 */}
                  {v.data_status === DataStatus.DATA_STATUS_PENDING && (
                    <>
                      {/* 能不能救，只取决于**本机**还有没有那份 changes：服务端从不保存
                          changes，所以"续传"不可能由界面凭空发起。这两种情况在状态列上
                          长得一模一样，必须在这里分开讲清楚——否则用户只会看到一句读不懂
                          的 PENDING，既不知道为什么，也不知道能不能救。 */}
                      {(() => {
                        const rec = recordFor(v.version_id)
                        if (rec === undefined) {
                          return (
                            <span
                              className="muted small"
                              title="这份 changes 只存在于当初提交它的那个客户端。服务端从不保存 changes（版本里只有 doc_id_set_hash 摘要），所以本机没有记录时无法重发——只能等运维处置，或删掉重来"
                            >
                              本机无记录
                            </span>
                          )
                        }
                        return (
                          <button
                            type="button"
                            disabled={busy || resending !== null}
                            title="用当初的 client_request_id 重发同一批 changes——服务端认它就会复用这个版本，不会另开一个"
                            onClick={() => void resend(rec)}
                          >
                            {resending === rec.id ? '重发中…' : '重发'}
                          </button>
                        )
                      })()}
                      <button
                        type="button"
                        disabled={busy}
                        title="放弃一个从未落地的 PENDING 版本"
                        onClick={() =>
                          discard.mutate(
                            { knowledge_base_id: kbId, version_id: v.version_id },
                            {
                              onSuccess: () =>
                                void appendHistory({
                                  kind: 'discard',
                                  knowledge_base_id: kbId,
                                  detail: `放弃 v${v.version_id}`,
                                }),
                            },
                          )
                        }
                      >
                        放弃
                      </button>
                      {(() => {
                        const rec = recordFor(v.version_id)
                        const note = rec === undefined ? undefined : resendNote[rec.id]
                        return note === undefined ? null : (
                          <div className="muted small">{note}</div>
                        )
                      })()}
                    </>
                  )}
                  <button
                    type="button"
                    className="danger"
                    disabled={busy || v.version_id === activeId}
                    title="删除该版本及其子树（活跃版本不可删）"
                    onClick={() =>
                      del.mutate(
                        {
                          knowledge_base_id: kbId,
                          version_id: v.version_id,
                          mode: VersionDeleteMode.VERSION_DELETE_MODE_SUBTREE,
                        },
                        {
                          onSuccess: () =>
                            void appendHistory({
                              kind: 'delete',
                              knowledge_base_id: kbId,
                              detail: `删除 v${v.version_id} 及其子树`,
                            }),
                        },
                      )
                    }
                  >
                    删除
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

/** 枚举值都很长，表格里取后半段。 */
function shortStatus(v: DataStatus | IndexStatus): string {
  return v.replace(/^(DATA_STATUS_|INDEX_STATUS_)/, '')
}

function chipClass(good: boolean, bad = false): string {
  if (bad) return 'chip bad'
  return good ? 'chip ok' : 'chip warn'
}

/** protojson 的 int64 是字符串，`created_at` 是 Unix 秒。 */
function formatUnix(seconds: string): string {
  const n = Number(seconds)
  if (!Number.isFinite(n) || n <= 0) return '—'
  return new Date(n * 1000).toLocaleString()
}

export type { VersionInfo }
