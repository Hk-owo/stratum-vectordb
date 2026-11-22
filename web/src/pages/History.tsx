import { useEffect, useState } from 'react'

import { isDesktop } from '../desktop/tauri'
import { HISTORY_LIMIT, loadHistory, type HistoryEntry } from '../history/store'

const KIND_LABEL: Readonly<Record<string, { label: string; cls: string }>> = {
  submit: { label: '提交', cls: 'chip accent' },
  resend: { label: '重发', cls: 'chip warn' },
  activate: { label: '激活', cls: 'chip ok' },
  discard: { label: '放弃', cls: 'chip bad' },
  delete: { label: '删除', cls: 'chip bad' },
  query: { label: '检索', cls: 'chip' },
}

function when(at: string): string {
  const d = new Date(at)
  return Number.isNaN(d.getTime()) ? at : d.toLocaleString()
}

export function History() {
  const [entries, setEntries] = useState<HistoryEntry[] | null>(null)
  const [kindFilter, setKindFilter] = useState<string>('all')

  async function reload() {
    setEntries(await loadHistory())
  }

  useEffect(() => {
    void reload()
  }, [])

  const visible =
    entries === null
      ? null
      : kindFilter === 'all'
        ? entries
        : entries.filter((e) => e.kind === kindFilter)

  return (
    <div className="page">
      <div className="toolbar">
        <p className="muted small" style={{ margin: 0 }}>
          最近 {HISTORY_LIMIT} 条本机操作记录（存在
          {isDesktop() ? '本机数据目录 ' : '浏览器 IndexedDB '}
          {isDesktop() ? <code>history.jsonl</code> : null}
          ，不同步到其他设备）。
        </p>
        <div className="toolbar-group">
          <select value={kindFilter} onChange={(e) => setKindFilter(e.target.value)}>
            <option value="all">全部操作</option>
            {Object.entries(KIND_LABEL).map(([k, v]) => (
              <option key={k} value={k}>
                {v.label}
              </option>
            ))}
          </select>
          <button type="button" onClick={() => void reload()}>
            刷新
          </button>
        </div>
      </div>

      {visible === null ? (
        <p className="muted">读取中…</p>
      ) : visible.length === 0 ? (
        <p className="placeholder">
          还没有匹配的记录。检索、提交文档、激活或删除版本之后，这里会记录本机操作。
        </p>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th scope="col">时间</th>
                <th scope="col">操作</th>
                <th scope="col">知识库</th>
                <th scope="col">说明</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((e, i) => {
                const meta = KIND_LABEL[e.kind]
                return (
                  <tr key={`${e.at}-${i}`}>
                    <td className="muted small">{when(e.at)}</td>
                    <td>
                      <span className={meta?.cls ?? 'chip'}>{meta?.label ?? e.kind}</span>
                    </td>
                    <td>
                      <code className="muted small">{e.knowledge_base_id}</code>
                    </td>
                    <td>{e.detail}</td>
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
