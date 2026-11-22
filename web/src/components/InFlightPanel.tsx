import { useEffect, useState } from 'react'

import { api, kbPath } from '../api/client'
import type { ListVersionsResponse } from '../api/gen/knowledgebase'
import { chainTail } from '../ingest/chainTail'
import { resendPending } from '../pending/resend'
import { refreshUnsettled, removePending, type PendingWrite } from '../pending/store'
import { ConfirmButton } from './ConfirmButton'

/**
 * 在途批次面板（挂在顶栏角标 + 抽屉，避免被埋在长页面最底部）。
 */
export function InFlightPanel() {
  const [rows, setRows] = useState<PendingWrite[]>([])
  const [error, setError] = useState<string | null>(null)
  const [open, setOpen] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [results, setResults] = useState<Record<string, string>>({})

  async function refresh() {
    try {
      setRows(await refreshUnsettled())
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  useEffect(() => {
    void refresh()
    const timer = setInterval(() => {
      void refresh()
    }, 5000)
    return () => clearInterval(timer)
  }, [])

  async function resend(w: PendingWrite) {
    setBusy(w.id)
    setResults((p) => ({ ...p, [w.id]: '重发中…' }))
    try {
      const vs = await api.get<ListVersionsResponse>(kbPath(w.knowledge_base_id, '/versions'))
      const tail = chainTail(vs.versions)
      const outcome = await resendPending(w, tail.kind === 'ok' ? tail.versionId : null, {
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
    return <span className="badge bad">在途记录不可读：{error}</span>
  }

  if (rows.length === 0) return null

  return (
    <>
      <button
        type="button"
        className="badge warn"
        onClick={() => {
          void refresh()
          setOpen(true)
        }}
        title="查看并恢复未完成确认的提交批次"
      >
        <span className="dot" />
        在途 {rows.length} 批
      </button>

      {open && (
        <div className="modal-backdrop" onClick={() => setOpen(false)}>
          <div className="modal modal-wide" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2 className="modal-title">在途提交批次（{rows.length}）</h2>
              <button type="button" className="btn-sm" onClick={() => void refresh()}>
                刷新
              </button>
            </div>
            <p className="muted small">
              以下批次尚未确认到达终态。集群不保存未落地前的 changes，刷新或重连后可在此按原{' '}
              <code>client_request_id</code> 续传或确认丢弃。
            </p>
            <ul className="status-list" style={{ listStyle: 'none', paddingLeft: 0 }}>
              {rows.map((w) => (
                <li
                  key={w.id}
                  className="status-section"
                  style={{ marginBottom: 10 }}
                >
                  <div className="toolbar">
                    <div>
                      <code>{w.knowledge_base_id}</code>{' '}
                      {w.version_id !== '' && <span className="chip accent">v{w.version_id}</span>}{' '}
                      <span className="muted small">
                        {w.changes.length} 个文档 · {new Date(w.submitted_at).toLocaleString()}
                      </span>
                    </div>
                    <div className="actions">
                      <button
                        type="button"
                        className="btn-primary"
                        disabled={busy !== null}
                        title="用同一个 client_request_id 重发：服务端认它就会复用首次分配的版本"
                        onClick={() => void resend(w)}
                      >
                        {busy === w.id ? '重发中…' : '重发 / 继续等待'}
                      </button>
                      <ConfirmButton
                        label="丢弃记录"
                        confirmLabel="确认永久丢弃"
                        className="danger"
                        disabled={busy !== null}
                        title="移除本机记录后，若数据未落盘将无法再救回"
                        onConfirm={() => void removePending(w.id).then(refresh)}
                      />
                    </div>
                  </div>
                  <div className="muted small">
                    幂等键：<code>{w.client_request_id}</code>
                  </div>
                  {results[w.id] !== undefined && (
                    <div className="notice small">{results[w.id]}</div>
                  )}
                </li>
              ))}
            </ul>
            <div className="modal-actions">
              <button type="button" onClick={() => setOpen(false)}>
                关闭
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
