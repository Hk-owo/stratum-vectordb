import { useEffect, useState } from 'react'

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
import { ConfirmButton } from '../components/ConfirmButton'
import { KbMetaStrip } from '../components/KbMetaStrip'
import { appendHistory } from '../history/store'
import { chainTail } from '../ingest/chainTail'
import { resendPending } from '../pending/resend'
import { loadPending, type PendingWrite } from '../pending/store'

/**
 * 版本页：独立展示 DataStatus 与 IndexStatus、文档集摘要、就绪副本以及版本链路操作。
 */
export function Versions({
  kbId,
  onJumpToSearch,
  onOpenKbManager,
}: {
  kbId: string | null
  onJumpToSearch?: (versionId: string) => void
  onOpenKbManager?: () => void
}) {
  const kb = useKnowledgeBase(kbId)
  const versions = useVersions(kbId)

  const rollback = useRollbackVersion(kbId ?? '')
  const del = useDeleteVersion(kbId ?? '')
  const discard = useDiscardVersion(kbId ?? '')
  const rebuild = useRebuildIndex(kbId ?? '')
  const warmup = useWarmupVersion(kbId ?? '')

  const [local, setLocal] = useState<PendingWrite[]>([])
  const [resending, setResending] = useState<string | null>(null)
  const [resendNote, setResendNote] = useState<Record<string, string>>({})
  const [deleteMode, setDeleteMode] = useState<VersionDeleteMode>(
    VersionDeleteMode.VERSION_DELETE_MODE_SUBTREE,
  )
  const [copiedHash, setCopiedHash] = useState<string | null>(null)
  const [actionMsg, setActionMsg] = useState<string | null>(null)

  useEffect(() => {
    void loadPending()
      .then(setLocal)
      .catch(() => setLocal([]))
  }, [])

  if (kbId === null) {
    return (
      <div className="placeholder">
        <p style={{ margin: '0 0 10px' }}>还没有选中知识库。请先在右上角选择，或新建一个知识库。</p>
        {onOpenKbManager !== undefined && (
          <button type="button" className="btn-primary" onClick={onOpenKbManager}>
            + 新建 / 管理知识库
          </button>
        )}
      </div>
    )
  }

  const activeId = kb.data?.knowledge_base?.active_version_id
  const list = [...(versions.data?.versions ?? [])].sort(
    (a, b) => Number(b.version_id) - Number(a.version_id),
  )

  function recordFor(versionId: string): PendingWrite | undefined {
    return local.find((w) => w.knowledge_base_id === kbId && w.version_id === versionId)
  }

  async function resend(w: PendingWrite) {
    setResending(w.id)
    setResendNote((p) => ({ ...p, [w.id]: '重发中…' }))
    try {
      const tail = chainTail(versions.data?.versions ?? [])
      const outcome = await resendPending(w, tail.kind === 'ok' ? tail.versionId : null, {
        onProgress: (stage) => setResendNote((p) => ({ ...p, [w.id]: `等待中 · ${stage}` })),
      })
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
      void versions.refetch()
    } catch (err) {
      setResendNote((p) => ({
        ...p,
        [w.id]: `重发失败：${err instanceof Error ? err.message : String(err)}`,
      }))
    } finally {
      setResending(null)
    }
  }

  function copyHash(vid: string, hash: string) {
    void navigator.clipboard?.writeText(hash).then(() => {
      setCopiedHash(vid)
      setTimeout(() => setCopiedHash((cur) => (cur === vid ? null : cur)), 1500)
    })
  }

  const err = versions.error ?? kb.error ?? rollback.error ?? del.error ?? discard.error ?? rebuild.error ?? warmup.error
  const busy = rollback.isPending || del.isPending || discard.isPending || rebuild.isPending || warmup.isPending

  return (
    <div className="page">
      {kb.data?.knowledge_base !== undefined && (
        <KbMetaStrip kb={kb.data.knowledge_base} onManageKbs={onOpenKbManager} />
      )}

      <div className="toolbar">
        <div className="toolbar-group">
          <span className="muted small">共 {list.length} 个版本</span>
          <label className="kb-picker small">
            <span className="muted">删除模式</span>
            <select
              value={deleteMode}
              onChange={(e) => setDeleteMode(e.target.value as VersionDeleteMode)}
            >
              <option value={VersionDeleteMode.VERSION_DELETE_MODE_SUBTREE}>
                SUBTREE（含子孙版本）
              </option>
              <option value={VersionDeleteMode.VERSION_DELETE_MODE_SINGLE}>
                SINGLE（仅当前版本）
              </option>
            </select>
          </label>
        </div>
        <button
          type="button"
          disabled={versions.isFetching}
          onClick={() => {
            void versions.refetch()
            void kb.refetch()
          }}
        >
          {versions.isFetching ? '刷新中…' : '刷新列表'}
        </button>
      </div>

      {err !== null && err !== undefined && (
        <p className="error-box">{err instanceof Error ? err.message : String(err)}</p>
      )}
      {actionMsg !== null && <p className="notice ok">{actionMsg}</p>}

      {versions.isPending && <p className="muted">加载版本链…</p>}
      {versions.data !== undefined && list.length === 0 && (
        <p className="placeholder">
          这个库还没有版本（<code>initial_version_id = 0</code>）。去「文档」页上传内容后就会生成第一个版本。
        </p>
      )}

      {list.length > 0 && (
        <div className="table-wrap">
          <table className="queue-table">
            <thead>
              <tr>
                <th>版本</th>
                <th>数据状态</th>
                <th>索引状态</th>
                <th>就绪副本</th>
                <th>文档集摘要</th>
                <th>创建时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {list.map((v) => {
                const isAct = v.version_id === activeId
                const isDurable = v.data_status === DataStatus.DATA_STATUS_DURABLE
                return (
                  <tr key={v.version_id} className={isAct ? 'row-active' : undefined}>
                    <td>
                      <code>v{v.version_id}</code>{' '}
                      {isAct && <span className="chip ok">激活</span>}{' '}
                      {v.parent_version_id !== '0' && (
                        <code className="muted small">← v{v.parent_version_id}</code>
                      )}
                      {v.deleting && <span className="chip warn">清理中</span>}
                    </td>
                    <td>
                      <span
                        className={chipClass(
                          isDurable,
                          v.data_status === DataStatus.DATA_STATUS_FAILED_PERMANENT,
                        )}
                      >
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
                    <td className="muted small">
                      {v.index_ready_nodes.length === 0
                        ? '—'
                        : `${v.index_ready_nodes.length} 节点 (${v.index_ready_nodes.join(',')})`}
                    </td>
                    <td>
                      {v.doc_id_set_hash === '' ? (
                        <span className="muted small">（未提交）</span>
                      ) : (
                        <button
                          type="button"
                          className="btn-sm"
                          title={v.doc_id_set_hash}
                          onClick={() => copyHash(v.version_id, v.doc_id_set_hash)}
                        >
                          <code>
                            {copiedHash === v.version_id
                              ? '已复制'
                              : v.doc_id_set_hash.slice(0, 10)}
                          </code>
                        </button>
                      )}
                    </td>
                    <td className="muted small">{formatUnix(v.created_at)}</td>
                    <td className="actions">
                      <button
                        type="button"
                        className={isAct ? undefined : 'btn-primary'}
                        disabled={busy || isAct || !isDurable}
                        title={
                          isAct
                            ? '已经是激活版本'
                            : !isDurable
                              ? '数据尚未落盘，不能激活'
                              : '把激活版本切到这一个（发布 / 回滚）'
                        }
                        onClick={() =>
                          rollback.mutate(
                            { knowledge_base_id: kbId, target_version_id: v.version_id },
                            {
                              onSuccess: () => {
                                setActionMsg(`已将激活版本切换为 v${v.version_id}`)
                                void appendHistory({
                                  kind: 'activate',
                                  knowledge_base_id: kbId,
                                  detail: `激活 v${v.version_id}`,
                                })
                              },
                            },
                          )
                        }
                      >
                        激活
                      </button>
                      {onJumpToSearch !== undefined && isDurable && !v.deleting && (
                        <button
                          type="button"
                          onClick={() => onJumpToSearch(v.version_id)}
                          title="带着这个版本跳转到检索页查询"
                        >
                          检索
                        </button>
                      )}
                      {v.index_status === IndexStatus.INDEX_STATUS_FAILED && (
                        <button
                          type="button"
                          disabled={busy}
                          onClick={() =>
                            rebuild.mutate(
                              { knowledge_base_id: kbId, version_id: v.version_id },
                              {
                                onSuccess: () =>
                                  setActionMsg(`已触发 v${v.version_id} 重建索引`),
                              },
                            )
                          }
                        >
                          重建索引
                        </button>
                      )}
                      <button
                        type="button"
                        disabled={busy || !isDurable}
                        title="把索引载入内存（不改激活版本）；产物会因此受保留策略保护"
                        onClick={() =>
                          warmup.mutate(
                            { knowledge_base_id: kbId, version_id: v.version_id },
                            {
                              onSuccess: () =>
                                setActionMsg(`已发送 v${v.version_id} 索引预热请求`),
                            },
                          )
                        }
                      >
                        预热
                      </button>
                      {v.data_status === DataStatus.DATA_STATUS_PENDING && (
                        <>
                          {(() => {
                            const rec = recordFor(v.version_id)
                            if (rec === undefined) {
                              return (
                                <span
                                  className="muted small"
                                  title="这份 changes 只存在于当初提交它的客户端。本机无记录时无法重发，可放弃或联系运维处理"
                                >
                                  本机无记录
                                </span>
                              )
                            }
                            return (
                              <button
                                type="button"
                                disabled={busy || resending !== null}
                                onClick={() => void resend(rec)}
                              >
                                {resending === rec.id ? '重发中…' : '重发'}
                              </button>
                            )
                          })()}
                          <ConfirmButton
                            label="放弃"
                            confirmLabel="确认放弃"
                            disabled={busy}
                            title="放弃一个从未落地的 PENDING 版本"
                            onConfirm={() =>
                              discard.mutate(
                                { knowledge_base_id: kbId, version_id: v.version_id },
                                {
                                  onSuccess: () => {
                                    setActionMsg(`已放弃 PENDING 版本 v${v.version_id}`)
                                    void appendHistory({
                                      kind: 'discard',
                                      knowledge_base_id: kbId,
                                      detail: `放弃 v${v.version_id}`,
                                    })
                                  },
                                },
                              )
                            }
                          />
                          {(() => {
                            const rec = recordFor(v.version_id)
                            const note = rec === undefined ? undefined : resendNote[rec.id]
                            return note === undefined ? null : (
                              <div className="muted small">{note}</div>
                            )
                          })()}
                        </>
                      )}
                      <ConfirmButton
                        label="删除"
                        confirmLabel="确认删除"
                        className="danger"
                        disabled={busy || isAct}
                        title="删除该版本（活跃版本不可删）"
                        onConfirm={() =>
                          del.mutate(
                            {
                              knowledge_base_id: kbId,
                              version_id: v.version_id,
                              mode: deleteMode,
                            },
                            {
                              onSuccess: () => {
                                setActionMsg(`已提交删除 v${v.version_id}`)
                                void appendHistory({
                                  kind: 'delete',
                                  knowledge_base_id: kbId,
                                  detail: `删除 v${v.version_id}（${deleteMode === VersionDeleteMode.VERSION_DELETE_MODE_SINGLE ? 'SINGLE' : 'SUBTREE'}）`,
                                })
                              },
                            },
                          )
                        }
                      />
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function shortStatus(v: DataStatus | IndexStatus): string {
  return v.replace(/^(DATA_STATUS_|INDEX_STATUS_)/, '')
}

function chipClass(good: boolean, bad = false): string {
  if (bad) return 'chip bad'
  return good ? 'chip ok' : 'chip warn'
}

function formatUnix(seconds: string): string {
  const n = Number(seconds)
  if (!Number.isFinite(n) || n <= 0) return '—'
  return new Date(n * 1000).toLocaleString()
}

export type { VersionInfo }
