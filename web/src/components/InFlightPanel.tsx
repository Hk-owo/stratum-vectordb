import { useEffect, useState } from 'react'

import { api, kbPath } from '../api/client'
import type { ListVersionsResponse } from '../api/gen/knowledgebase'
import { chainTail } from '../ingest/chainTail'
import { resendPending } from '../pending/resend'
import { loadUnsettled, removePending, type PendingWrite } from '../pending/store'

/**
 * 在途批次面板。
 *
 * 它读的是 IndexedDB 里那些"提交了但还没落地"的记录——**刷新页面之后依然在**。
 * 这不是一个便利功能，而是整套设计里唯一能救回数据的入口：集群不替任何人保存
 * changes（客户端之外的唯一副本要等协调者写下 WAL BEGIN 才出现），所以这份记录
 * 就是"还有东西悬着"的全部证据。
 *
 * 因此这一行**必须带动作**。只显示"有东西悬着"而不给出口，用户唯一能做的就是
 * 丢弃——而那恰好是把数据彻底扔掉。服务端等的正是"调用方按同一个
 * client_request_id 再举一次"（§7.12），这里就是那个动作。
 *
 * 三种可能的结局都在这上面：
 *   · DONE     —— 复用首次分配的版本，数据落地了。
 *   · WAIT     —— 版本还活着但没到 READY（索引在建、或 FAILED 等运维 rebuild）：
 *                 记录留着，可以再点一次。
 *   · DISCARD  —— 没有副本持有数据，而 changes 也已不在别处：数据确实回不来了，
 *                 记录会被清掉，面板上直接说明。
 */
export function InFlightPanel() {
  const [rows, setRows] = useState<PendingWrite[]>([])
  const [error, setError] = useState<string | null>(null)
  const [open, setOpen] = useState(false)
  /** 正在重发的记录 id。 */
  const [busy, setBusy] = useState<string | null>(null)
  /** 每条记录上一次动作的结果。 */
  const [results, setResults] = useState<Record<string, string>>({})

  async function refresh() {
    try {
      setRows(await loadUnsettled())
      setError(null)
    } catch (err) {
      // 读不出来（隐私模式等）要说出来：静默空列表会被误读成"没有在途数据"，
      // 而那正是最不该误判的一件事。
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  useEffect(() => {
    void refresh()
  }, [])

  async function resend(w: PendingWrite) {
    setBusy(w.id)
    setResults((p) => ({ ...p, [w.id]: '重发中…' }))
    try {
      // 链尾现取，不用记录里存的：首次提交若压根没分配出版本，这次就是全新的提交，
      // parent 必须指向真正的链尾；若已分配过版本，服务端按同一个 key 复用，
      // parent 不参与分配。
      const vs = await api.get<ListVersionsResponse>(kbPath(w.knowledge_base_id, '/versions'))
      const outcome = await resendPending(w, chainTail(vs.versions), {
        onProgress: (stage) => setResults((p) => ({ ...p, [w.id]: `等待中 · ${stage}` })),
      })

      setResults((p) => ({
        ...p,
        [w.id]:
          outcome.decision === 'DONE'
            ? `已落地 v${outcome.versionId}`
            : outcome.decision === 'DISCARD'
              ? '数据已不可找回（DISCARD）——记录已清除'
              : `${outcome.stage}${outcome.dataMissing ? ' · 无副本持有数据' : ''} —— 可以再试一次`,
      }))
      await refresh()
    } catch (err) {
      setResults((p) => ({
        ...p,
        [w.id]: `重发失败：${err instanceof Error ? err.message : String(err)}`,
      }))
    } finally {
      setBusy(null)
    }
  }

  if (error !== null) {
    return <p className="badge bad">在途记录不可读：{error}</p>
  }

  if (rows.length === 0) return null

  return (
    <div className="inflight">
      <button type="button" className="inflight-toggle" onClick={() => setOpen((v) => !v)}>
        <span className="badge warn">
          <span className="dot" />
          在途 {rows.length} 批
        </span>
        <span className="muted small">{open ? '收起' : '展开'}</span>
      </button>

      {open && (
        <ul className="status-list">
          {rows.map((w) => (
            <li key={w.id}>
              <code>{w.knowledge_base_id}</code>{' '}
              {w.version_id !== '' && <code>v{w.version_id}</code>}{' '}
              <span className="muted small">
                {w.changes.length} 个文档 · {new Date(w.submitted_at).toLocaleString()}
              </span>
              <br />
              <span className="muted small">
                幂等键 <code>{w.client_request_id}</code>（重发必须带同一个）
              </span>
              <div className="actions" style={{ marginTop: 6 }}>
                <button
                  type="button"
                  disabled={busy !== null}
                  title="用同一个 client_request_id 再举一次：服务端认它就会复用首次分配的版本，不会另开一个"
                  onClick={() => void resend(w)}
                >
                  {busy === w.id ? '重发中…' : '重发'}
                </button>
                <button
                  type="button"
                  className="danger"
                  disabled={busy !== null}
                  title="从本机记录中移除——只在确认这个版本已经不可能落地、并且不再需要重发时使用。移除后数据将无法救回"
                  onClick={() => {
                    void removePending(w.id).then(refresh)
                  }}
                >
                  丢弃记录
                </button>
              </div>
              {results[w.id] !== undefined && (
                <span className="muted small">{results[w.id]}</span>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
