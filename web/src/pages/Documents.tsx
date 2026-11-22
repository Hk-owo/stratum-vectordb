import { useEffect, useRef, useState, type FormEvent } from 'react'

import { ApiError } from '../api/client'
import { useKnowledgeBase, useRollbackVersion, useVersions } from '../api/queries'
import { KbMetaStrip } from '../components/KbMetaStrip'
import { setQueuedFiles, setSubmitting } from '../exit/activity'
import { appendHistory } from '../history/store'
import { submitBatches } from '../ingest/batch'
import { chainTail, describeNotReady } from '../ingest/chainTail'
import { docIdOf } from '../ingest/docid'
import { sha256Hex } from '../ingest/hash'
import { UnsupportedFileError, acceptedExtensions, parseFile } from '../ingest/parsers'
import { loadDigests, saveDigests, summarize, type QueueItem, type QueueStatus } from '../ingest/queue'
import { loadPrefs, savePrefs } from '../settings/store'

const STATUS_LABEL: Readonly<Record<QueueStatus, string>> = {
  pending: '待解析',
  parsing: '解析中',
  unchanged: '内容未变',
  ready: '待提交',
  submitting: '提交中',
  awaiting: '等待落盘',
  done: '已落盘',
  error: '失败',
}

/**
 * 文档页：文件/文件夹摄入、按 doc_id 删除、增量指纹去重与分批串行提交。
 */
export function Documents({
  kbId,
  onOpenKbManager,
}: {
  kbId: string | null
  onOpenKbManager?: () => void
}) {
  const [items, setItems] = useState<QueueItem[]>([])
  const [busy, setBusy] = useState(false)
  const [notice, setNotice] = useState<{ tone: '' | 'ok' | 'warn'; text: string } | null>(null)
  const [dragging, setDragging] = useState(false)
  const [pendingActivation, setPendingActivation] = useState<string | null>(null)
  const [durableOnly, setDurableOnly] = useState(true)
  const [deleteDocId, setDeleteDocId] = useState('')

  useEffect(() => {
    void (async () => {
      setDurableOnly((await loadPrefs()).durable_only)
    })()
  }, [])

  function toggleDurableOnly(next: boolean) {
    setDurableOnly(next)
    void savePrefs({ durable_only: next })
  }

  // 切换知识库时清空上个库的队列，避免把 KB-A 的待提交文档误发到 KB-B
  const [queueKbId, setQueueKbId] = useState<string | null>(kbId)
  if (queueKbId !== kbId) {
    setQueueKbId(kbId)
    setItems([])
    setNotice(null)
    setPendingActivation(null)
  }

  const fileInput = useRef<HTMLInputElement>(null)
  const dirInput = useRef<HTMLInputElement>(null)

  const versions = useVersions(kbId)
  const kb = useKnowledgeBase(kbId)
  const activate = useRollbackVersion(kbId ?? '')
  const counts = summarize(items)

  function patch(docId: string, p: Partial<QueueItem>) {
    setItems((prev) => prev.map((it) => (it.docId === docId ? { ...it, ...p } : it)))
  }

  async function ingest(files: File[]) {
    if (files.length === 0 || kbId === null) return
    setBusy(true)
    setNotice(null)

    const digests = loadDigests(kbId)

    const bydocId = new Map<string, File>()
    for (const f of files) bydocId.set(docIdOf(f), f)

    const fresh: QueueItem[] = [...bydocId].map(([docId, file]) => ({
      docId,
      file,
      op: 'UPDATE',
      status: 'pending',
      content: '',
      hash: '',
    }))

    setItems((prev) => {
      const rest = prev.filter((p) => !bydocId.has(p.docId))
      return [...rest, ...fresh]
    })

    let unsupported = 0
    let empty = 0
    for (const it of fresh) {
      patch(it.docId, { status: 'parsing' })
      try {
        const result = await parseFile(it.file)
        const hash = await sha256Hex(result.text)

        if (result.text.trim() === '') {
          empty += 1
          patch(it.docId, {
            status: 'error',
            content: result.text,
            hash,
            message: '解析结果为空，不会提交',
            note: result.note,
          })
          continue
        }

        const unchanged = digests[it.docId] === hash
        patch(it.docId, {
          status: unchanged ? 'unchanged' : 'ready',
          content: result.text,
          hash,
          note: result.note,
          message: unchanged ? '内容与当前知识库上次提交一致，跳过' : undefined,
        })
      } catch (err) {
        if (err instanceof UnsupportedFileError) unsupported += 1
        patch(it.docId, {
          status: 'error',
          message: err instanceof Error ? err.message : String(err),
        })
      }
    }

    const parts: string[] = []
    if (unsupported > 0) parts.push(`${unsupported} 个文件的格式不支持`)
    if (empty > 0) parts.push(`${empty} 个文件解析结果为空`)
    setNotice(parts.length > 0 ? { tone: 'warn', text: parts.join('；') } : null)
    setBusy(false)
  }

  function enqueueDelete(e: FormEvent) {
    e.preventDefault()
    const target = deleteDocId.trim()
    if (kbId === null || target === '') return
    const delItem: QueueItem = {
      docId: target,
      file: new File([], target),
      op: 'DELETE',
      status: 'ready',
      content: '',
      hash: '',
      message: '提交后将从版本文档集中移除该 doc_id',
    }
    setItems((prev) => [...prev.filter((it) => it.docId !== target), delItem])
    setDeleteDocId('')
  }

  async function submit() {
    if (kbId === null) return
    const ready = items.filter((it) => it.status === 'ready')
    if (ready.length === 0) {
      setNotice({ tone: 'warn', text: '没有需要提交的内容（内容未变的文件已被跳过）' })
      return
    }

    setBusy(true)
    setSubmitting(true)
    setNotice(null)
    try {
      const tail = chainTail(versions.data?.versions ?? [])
      if (tail.kind === 'not-ready') {
        setNotice({ tone: 'warn', text: describeNotReady(tail) })
        return
      }
      const summary = await submitBatches({
        kbId,
        parentVersionId: tail.kind === 'ok' ? tail.versionId : null,
        items: ready,
        target: durableOnly ? 'DATA_DURABLE' : 'INDEX_READY',
        onItem: patch,
      })

      // 仅按真实落地（DONE）的 doc_id 更新或移除指纹，防止半成功批次污染下次去重。
      const digests = loadDigests(kbId)
      const byId = new Map(ready.map((it) => [it.docId, it]))
      for (const docId of summary.succeededDocIds) {
        const it = byId.get(docId)
        if (it !== undefined && it.hash !== '') {
          digests[docId] = it.hash
        }
      }
      for (const docId of summary.deletedDocIds) {
        delete digests[docId]
      }
      saveDigests(digests, kbId)

      const done = summary.versions.length
      if (done > 0) {
        void appendHistory({
          kind: 'submit',
          knowledge_base_id: kbId,
          detail:
            `${summary.succeededDocIds.length + summary.deletedDocIds.length} 项变更 → ${done} 个版本（${summary.versions.map((v) => `v${v}`).join('、')}）` +
            (durableOnly ? '，按 DATA_DURABLE 收口' : ''),
        })
      }

      const last = summary.lastVersionId
      const activeId = kb.data?.knowledge_base?.active_version_id
      setPendingActivation(last === null || last === activeId ? null : last)
      setNotice(
        done > 0
          ? {
              tone: 'ok',
              text: durableOnly
                ? `已落盘（多数派已确认）：${summary.versions.map((v) => `v${v}`).join('、')}。`
                : `已落盘、索引已就绪：版本 ${summary.versions.map((v) => `v${v}`).join('、')}`,
            }
          : { tone: 'warn', text: '没有任何批次落地，请查看队列中的失败原因' },
      )
    } catch (err) {
      setNotice({
        tone: 'warn',
        text:
          (err instanceof ApiError && err.isUnavailable ? '后端不可达：' : '提交失败：') +
          (err instanceof Error ? err.message : String(err)),
      })
    } finally {
      setBusy(false)
      setSubmitting(false)
    }
  }

  const queued = counts.pending + counts.parsing + counts.ready
  useEffect(() => {
    setQueuedFiles(queued)
    return () => setQueuedFiles(0)
  }, [queued])

  const disabled = kbId === null || busy
  const finishedCount = counts.done + counts.error
  const progressTotal = finishedCount + counts.submitting + counts.awaiting
  const progressPct =
    busy && progressTotal > 0 ? Math.round((finishedCount / progressTotal) * 100) : 0

  if (kbId === null) {
    return (
      <div className="page">
        <div className="placeholder">
          <p style={{ margin: '0 0 10px' }}>
            还没有选中知识库——请先新建或选择一个知识库，然后即可拖拽或选择文件上传。
          </p>
          {onOpenKbManager !== undefined && (
            <button type="button" className="btn-primary" onClick={onOpenKbManager}>
              + 新建 / 管理知识库
            </button>
          )}
        </div>
      </div>
    )
  }

  return (
    <div className="page">
      {kb.data?.knowledge_base !== undefined && (
        <KbMetaStrip kb={kb.data.knowledge_base} onManageKbs={onOpenKbManager} />
      )}

      <div
        className={dragging ? 'dropzone dragging' : 'dropzone'}
        onDragOver={(e) => {
          e.preventDefault()
          setDragging(true)
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={(e) => {
          e.preventDefault()
          setDragging(false)
          if (disabled) return
          void ingest(Array.from(e.dataTransfer.files))
        }}
      >
        <p>
          <strong>拖拽文件或文件夹到这里</strong>
        </p>
        <div className="dropzone-actions">
          <button type="button" disabled={disabled} onClick={() => fileInput.current?.click()}>
            选择文件
          </button>
          <button type="button" disabled={disabled} onClick={() => dirInput.current?.click()}>
            选择文件夹
          </button>
        </div>
        <p className="muted small">
          支持 {acceptedExtensions()} · 同路径文档自动覆盖更新，内容未变时自动跳过
        </p>
      </div>

      <form className="delete-doc-row" onSubmit={enqueueDelete}>
        <span className="muted small">按 doc_id 删除文档：</span>
        <input
          type="text"
          value={deleteDocId}
          disabled={disabled}
          placeholder="输入要删除的 doc_id（例如 docs/old.md）"
          onChange={(e) => setDeleteDocId(e.target.value)}
        />
        <button type="submit" disabled={disabled || deleteDocId.trim() === ''}>
          加入删除队列
        </button>
      </form>

      <div className="status-section">
        <label className="checkbox-row">
          <input
            type="checkbox"
            checked={durableOnly}
            disabled={disabled}
            onChange={(e) => toggleDurableOnly(e.target.checked)}
          />
          <span>
            <strong>只等数据落盘（推荐）</strong>{' '}
            <span className="muted small">
              每批等到多数派落盘即完成（<code>DATA_DURABLE</code>）；取消勾选则同时预热并等待索引就绪。
            </span>
          </span>
        </label>
        <details className="advanced">
          <summary>了解版本提交与激活说明</summary>
          <p className="muted small" style={{ margin: '6px 0 0', lineHeight: 1.65 }}>
            1. 同一知识库上的版本链严格线性，多批文档会按 1 MiB 切批串行提交。
            <br />
            2. <b>提交 ≠ 发布</b>：版本落盘后需要点一次「激活」才会成为检索默认命中的版本。
            <br />
            3. 索引按需构建：激活后首次检索（或在「版本」页手动预热）时构建索引。
          </p>
        </details>
      </div>

      <input
        ref={fileInput}
        type="file"
        multiple
        hidden
        accept={acceptedExtensions()}
        onChange={(e) => {
          void ingest(Array.from(e.target.files ?? []))
          e.target.value = ''
        }}
      />
      <input
        ref={dirInput}
        type="file"
        multiple
        hidden
        // @ts-expect-error webkitdirectory 不在 React 的已知属性里，但浏览器认
        webkitdirectory=""
        onChange={(e) => {
          void ingest(Array.from(e.target.files ?? []))
          e.target.value = ''
        }}
      />

      {notice !== null && (
        <p className={notice.tone === '' ? 'notice' : `notice ${notice.tone}`}>{notice.text}</p>
      )}

      {pendingActivation !== null && (
        <div className="notice warn">
          <div className="toolbar">
            <div>
              版本 <code>v{pendingActivation}</code> 数据已落盘，但<strong>尚未激活</strong>
              （检索页默认查询当前激活版本）。
            </div>
            <button
              type="button"
              className="btn-primary"
              disabled={activate.isPending}
              onClick={() => {
                if (kbId === null) return
                activate.mutate(
                  { knowledge_base_id: kbId, target_version_id: pendingActivation },
                  {
                    onSuccess: () => {
                      void appendHistory({
                        kind: 'activate',
                        knowledge_base_id: kbId,
                        detail: `激活 v${pendingActivation}`,
                      })
                      setPendingActivation(null)
                    },
                  },
                )
              }}
            >
              {activate.isPending ? '激活中…' : `立即激活 v${pendingActivation}`}
            </button>
          </div>
        </div>
      )}

      {items.length > 0 && (
        <>
          <div className="queue-summary">
            <span className="muted small">
              共 {items.length} · 待提交 <strong>{counts.ready}</strong> · 未变跳过{' '}
              {counts.unchanged} · 进行中 {counts.submitting + counts.awaiting} · 已落盘{' '}
              {counts.done} · 失败 {counts.error}
            </span>
            <div className="actions">
              <button
                type="button"
                disabled={busy}
                onClick={() => setItems((prev) => prev.filter((it) => it.status !== 'done' && it.status !== 'unchanged'))}
              >
                清除已完成/未变
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => setItems([])}
              >
                清空队列
              </button>
              <button
                type="button"
                className="btn-primary"
                disabled={disabled || counts.ready === 0}
                onClick={() => void submit()}
              >
                {busy ? '提交处理中…' : `提交 ${counts.ready} 项变更`}
              </button>
            </div>
          </div>

          {busy && progressTotal > 0 && (
            <div className="progress-track" aria-label="提交进度">
              <div className="progress-fill" style={{ width: `${progressPct}%` }} />
            </div>
          )}

          <div className="table-wrap">
            <table className="queue-table">
              <thead>
                <tr>
                  <th>doc_id</th>
                  <th>操作</th>
                  <th>状态</th>
                  <th>字节</th>
                  <th>说明</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {items.map((it) => (
                  <tr key={it.docId}>
                    <td>
                      <code>{it.docId}</code>
                    </td>
                    <td>
                      <span className={it.op === 'DELETE' ? 'chip bad' : 'chip accent'}>
                        {it.op === 'DELETE' ? '删除' : '更新/新增'}
                      </span>
                    </td>
                    <td>
                      <span className={`chip ${statusClass(it.status)}`}>
                        {STATUS_LABEL[it.status]}
                      </span>
                      {it.versionId !== undefined && <code className="muted"> v{it.versionId}</code>}
                    </td>
                    <td className="muted small">
                      {it.content === '' ? '—' : new TextEncoder().encode(it.content).length}
                    </td>
                    <td className="muted small">{it.message ?? it.note ?? ''}</td>
                    <td>
                      <button
                        type="button"
                        className="btn-sm"
                        disabled={busy}
                        onClick={() =>
                          setItems((prev) => prev.filter((x) => x.docId !== it.docId))
                        }
                      >
                        移除
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}

function statusClass(status: QueueStatus): string {
  switch (status) {
    case 'done':
      return 'ok'
    case 'error':
      return 'bad'
    case 'unchanged':
      return 'warn'
    case 'submitting':
    case 'awaiting':
      return 'accent'
    default:
      return ''
  }
}
