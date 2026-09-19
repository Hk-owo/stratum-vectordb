import { useEffect, useRef, useState } from 'react'

import { ApiError } from '../api/client'
import { appendHistory } from '../history/store'
import { useKnowledgeBase, useRollbackVersion, useVersions } from '../api/queries'
import { submitBatches } from '../ingest/batch'
import { chainTail } from '../ingest/chainTail'
import { loadPrefs, savePrefs } from '../settings/store'
import { docIdOf } from '../ingest/docid'
import { sha256Hex } from '../ingest/hash'
import { UnsupportedFileError, acceptedExtensions, parseFile } from '../ingest/parsers'
import { loadDigests, saveDigests, summarize, type QueueItem } from '../ingest/queue'

/**
 * 文档页 —— 把"选文件"这件事做成客户要的样子：系统文件选择器 + 拖拽 + 选整个
 * 文件夹，而不是让人自己把文档内容复制粘贴进来。
 *
 * 这一页承担的是设计里那三条不能省的语义：
 *   · **前端做变更检测**：内容没变的文件不进 changes。空 changes 会被服务端拒
 *     （empty_changes），所以"重传一个没改过的文件"只能在前端被消化掉。
 *   · **doc_id = 相对路径**：重传同一路径就是一条 UPDATE 原地覆盖，不需要任何
 *     本地映射去查"上次这个文件的 doc_id 是什么"。
 *   · **分批串行**：版本链严格线性，多文件不是并发提交。
 */
export function Documents({ kbId }: { kbId: string | null }) {
  const [items, setItems] = useState<QueueItem[]>([])
  const [busy, setBusy] = useState(false)
  const [notice, setNotice] = useState<string | null>(null)
  const [dragging, setDragging] = useState(false)
  /**
   * 刚落地、但还没被激活的版本号。
   *
   * **提交 ≠ 发布**：`CreateVersion` 只是写入，检索页默认查的是**激活**版本
   * （只有 `RollbackVersion` 才切换）。所以传完文档直接去检索会什么都搜不到——
   * 这在服务端是有意的（可以攒几个版本再决定激活哪个），但对"上传完就想搜到"
   * 的用户是个断崖，必须在这里说清楚。
   */
  const [pendingActivation, setPendingActivation] = useState<string | null>(null)
  /**
   * 导入模式：每批只等到 `DATA_DURABLE`（多数派确认落盘），不等索引建完。
   *
   * 批量导入时这是数量级的差别——100 个文件不必逐个等索引构建。代价是那期间
   * 版本不可查、也**不能激活**，得等后台把索引建完。「传到一半先落盘、之后再统一
   * 激活」正是 `AWAIT_TARGET_DATA_DURABLE` 存在的理由。
   */
  const [durableOnly, setDurableOnly] = useState(false)

  // 恢复上次的导入模式。与检索参数同理：只读一次，此后以用户的操作为准。
  useEffect(() => {
    void (async () => {
      setDurableOnly((await loadPrefs()).durable_only)
    })()
  }, [])

  function toggleDurableOnly(next: boolean) {
    setDurableOnly(next)
    void savePrefs({ durable_only: next })
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

  /** 选中/拖入文件后：建项 → 逐个解析 → 与已提交的指纹比对。 */
  async function ingest(files: File[]) {
    if (files.length === 0) return
    setBusy(true)
    setNotice(null)

    const digests = loadDigests()

    // 同一个 doc_id 只留一个：用户可能同时拖了文件夹和单个文件，重复的以最后一个为准。
    const bydocId = new Map<string, File>()
    for (const f of files) bydocId.set(docIdOf(f), f)

    const fresh: QueueItem[] = [...bydocId].map(([docId, file]) => ({
      docId,
      file,
      status: 'pending',
      content: '',
      hash: '',
    }))
    // 保留已经在列上的项（用户可能刚提交过一批），新的覆盖同 doc_id 的旧项。
    setItems((prev) => {
      const rest = prev.filter((p) => !bydocId.has(p.docId))
      return [...rest, ...fresh]
    })

    // 串行解析：并行打开上百个文件句柄没有收益，而且串行才能让每个文件的状态
    // 在界面上按顺序出现。文件小，这不是瓶颈——瓶颈在提交与索引构建。
    let unsupported = 0
    let empty = 0
    for (const it of fresh) {
      patch(it.docId, { status: 'parsing' })
      try {
        const result = await parseFile(it.file)
        const hash = await sha256Hex(result.text)

        // 解析成功但一个字符都没有 —— 别把它当成正常文档提交上去。
        // （去掉 PDF 支持之后这条主要是防空的 docx / 空文件。）
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
          message: unchanged ? '内容与上次提交的一致，跳过' : undefined,
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
    setNotice(parts.length > 0 ? parts.join('；') : null)
    setBusy(false)
  }

  async function submit() {
    if (kbId === null) return
    const ready = items.filter((it) => it.status === 'ready')
    if (ready.length === 0) {
      setNotice('没有需要提交的内容（内容未变的文件已被跳过）')
      return
    }

    setBusy(true)
    setNotice(null)
    try {
      const parent = chainTail(versions.data?.versions ?? [])
      const summary = await submitBatches({
        kbId,
        parentVersionId: parent,
        items: ready,
        target: durableOnly ? 'DATA_DURABLE' : 'INDEX_READY',
        onItem: patch,
      })

      // 只有真正落地的那些才记指纹——否则下次会把"没提交成功的内容"当成已提交，
      // 于是永远不再重试它。
      const digests = loadDigests()
      for (const it of ready) {
        if (it.status === 'done' || summary.versions.length > 0) {
          const cur = items.find((x) => x.docId === it.docId)
          if (cur !== undefined && cur.hash !== '') digests[it.docId] = cur.hash
        }
      }
      saveDigests(digests)

      const done = summary.versions.length
      // 只记真正落地的批次。没落地的那些在「在途」面板里有更详细的状态，混进
      // 历史会让"我提交过什么"这件事变得不可信。
      if (done > 0) {
        void appendHistory({
          kind: 'submit',
          knowledge_base_id: kbId,
          detail:
            `${ready.length} 个文件 → ${done} 个版本（${summary.versions.map((v) => `v${v}`).join('、')}）` +
            (durableOnly ? '，按 DATA_DURABLE 收口' : ''),
        })
      }
      // 提交 ≠ 发布：落地的版本如果不是当前**激活**版本，检索页就还看不到它。
      //
      // 导入模式（等 DATA_DURABLE）下刻意不提激活：那个版本还没 READY，而激活一个
      // 非 READY 版本会被服务端拒——摆一个点了就报错的按钮比不摆更糟。
      const last = summary.lastVersionId
      const activeId = kb.data?.knowledge_base?.active_version_id
      setPendingActivation(durableOnly || last === null || last === activeId ? null : last)
      setNotice(
        done > 0
          ? durableOnly
            ? `已落盘（多数派已确认）：${summary.versions.map((v) => `v${v}`).join('、')}。` +
              '索引仍在后台构建，建好后才能在「版本」页激活并检索。'
            : `已提交并落地：版本 ${summary.versions.map((v) => `v${v}`).join('、')}`
          : '没有任何批次落地，见队列里的状态',
      )
    } catch (err) {
      setNotice(
        (err instanceof ApiError && err.isUnavailable ? '后端不可达：' : '提交失败：') +
          (err instanceof Error ? err.message : String(err)),
      )
    } finally {
      setBusy(false)
    }
  }

  const disabled = kbId === null || busy

  return (
    <div className="page">
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
        <p>把文件或整个文件夹拖进来</p>
        <div className="dropzone-actions">
          <button type="button" disabled={disabled} onClick={() => fileInput.current?.click()}>
            选择文件
          </button>
          <button type="button" disabled={disabled} onClick={() => dirInput.current?.click()}>
            选择文件夹
          </button>
        </div>
        <p className="muted small">支持 {acceptedExtensions()}</p>
        {kbId === null && <p className="badge bad">先在上面选一个知识库</p>}
      </div>

      <label className="checkbox-row">
        <input
          type="checkbox"
          checked={durableOnly}
          disabled={disabled}
          onChange={(e) => toggleDurableOnly(e.target.checked)}
        />
        <span>
          <strong>导入模式</strong>
          <span className="muted small">
            {' '}
            每批只等到多数派确认落盘（<code>AWAIT_TARGET_DATA_DURABLE</code>），不等索引建完——
            批量导入快得多。代价是那期间版本<strong>不可查也不能激活</strong>，要等索引在后台建好，
            再到「版本」页激活。
          </span>
        </span>
      </label>

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
      {/* webkitdirectory 让相对路径带上层级，而 doc_id 正是那个相对路径——
          于是"选一个目录"天然得到一套有层级的文档标识。 */}
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

      {notice !== null && <p className="notice">{notice}</p>}

      {pendingActivation !== null && (
        <div className="notice warn">
          <p style={{ margin: '0 0 8px' }}>
            版本 <code>v{pendingActivation}</code> 已就绪，但<strong>还没有激活</strong>。
            检索页默认查激活版本，所以现在还搜不到这批内容——<strong>提交只是写入，激活才是发布</strong>。
          </p>
          <button
            type="button"
            disabled={activate.isPending}
            onClick={() => {
              if (kbId === null) return
              activate.mutate(
                { knowledge_base_id: kbId, target_version_id: pendingActivation },
                { onSuccess: () => setPendingActivation(null) },
              )
            }}
          >
            {activate.isPending ? '激活中…' : `激活 v${pendingActivation}`}
          </button>
        </div>
      )}

      {items.length > 0 && (
        <>
          <div className="queue-summary">
            <span className="muted">
              共 {items.length} · 待提交 {counts.ready} · 未变跳过 {counts.unchanged} ·
              进行中 {counts.submitting + counts.awaiting} · 完成 {counts.done} · 失败{' '}
              {counts.error}
            </span>
            <button type="button" disabled={disabled || counts.ready === 0} onClick={() => void submit()}>
              {busy ? '处理中…' : `提交 ${counts.ready} 个`}
            </button>
          </div>

          <table className="queue-table">
            <thead>
              <tr>
                <th>doc_id</th>
                <th>状态</th>
                <th>字节</th>
                <th>说明</th>
              </tr>
            </thead>
            <tbody>
              {items.map((it) => (
                <tr key={it.docId}>
                  <td>
                    <code>{it.docId}</code>
                  </td>
                  <td>
                    <span className={`chip ${statusClass(it.status)}`}>{it.status}</span>
                    {it.versionId !== undefined && <code className="muted"> v{it.versionId}</code>}
                  </td>
                  <td>{it.content === '' ? '—' : new TextEncoder().encode(it.content).length}</td>
                  <td className="muted small">{it.message ?? it.note ?? ''}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  )
}

function statusClass(status: QueueItem['status']): string {
  switch (status) {
    case 'done':
      return 'ok'
    case 'error':
      return 'bad'
    case 'unchanged':
      return 'warn'
    default:
      return ''
  }
}
