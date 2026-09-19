/**
 * 历史 —— 本机做过的操作：提交、重发、激活、放弃、删除、检索。
 *
 * 只读，且**是本机的**：桌面落在 `history.jsonl`，浏览器落在 IndexedDB，换设备
 * 或清数据就没了。界面必须说清这一点 —— 否则用户会把它当成服务端的审计日志，
 * 而那不是它的职责（服务端也确实不存这些）。
 *
 * 刻意**不显示检索结果**：历史回答的是「我做过什么」，不是「当时返回了什么」。
 * 结果在知识库里随时可以重查，把它记下来只会让一份记录从几百字节涨到几兆字节。
 */
import { useEffect, useState } from 'react'

import { HISTORY_LIMIT, loadHistory, type HistoryEntry } from '../history/store'
import { isDesktop } from '../desktop/tauri'

const KIND_LABEL: Readonly<Record<string, string>> = {
  submit: '提交',
  resend: '重发',
  activate: '激活',
  discard: '放弃',
  delete: '删除',
  query: '检索',
}

/** 把 ISO 时间转成本地可读形式；解析不了就原样显示，不吞掉。 */
function when(at: string): string {
  const d = new Date(at)
  return Number.isNaN(d.getTime()) ? at : d.toLocaleString()
}

export function History() {
  // null = 还没读完。用它区分"读完了但没有记录"和"还没开始读"——两者的界面
  // 不同，混成一个空数组会让首屏闪一下"没有记录"。
  const [entries, setEntries] = useState<HistoryEntry[] | null>(null)

  useEffect(() => {
    void (async () => {
      setEntries(await loadHistory())
    })()
  }, [])

  return (
    <div className="page">
      <p className="muted small">
        最近 {HISTORY_LIMIT} 条，存在
        {isDesktop() ? '本机数据目录的 ' : '浏览器的 IndexedDB 里'}
        {isDesktop() ? <code>history.jsonl</code> : null}
        ，不会同步到别的设备。桌面侧超过 1 MiB 会自动保留最近 2000 条。
      </p>

      {entries === null ? (
        <p className="muted">读取中…</p>
      ) : entries.length === 0 ? (
        <p className="muted">
          还没有记录。检索、提交文档、激活或删除版本之后，这里会显示本机做过什么。
        </p>
      ) : (
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
            {entries.map((e, i) => (
              // 同一毫秒可能有多条，键里带上序号才不会撞——历史是纯追加的，
              // 下标在两次渲染之间对同一份快照是稳定的。
              <tr key={`${e.at}-${i}`}>
                <td className="muted small">{when(e.at)}</td>
                <td>{KIND_LABEL[e.kind] ?? e.kind}</td>
                <td className="muted small">{e.knowledge_base_id}</td>
                <td>{e.detail}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}
