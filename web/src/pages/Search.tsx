import { useEffect, useState, type FormEvent } from 'react'

import { ApiError } from '../api/client'
import { DataStatus, IndexStatus } from '../api/gen/knowledgebase'
import { AggregationMethod, type QueryResult } from '../api/gen/query'
import { useKnowledgeBase, useQueryText, useVersions } from '../api/queries'
import { KbMetaStrip } from '../components/KbMetaStrip'
import { appendHistory } from '../history/store'
import { DEFAULT_PREFS, loadPrefs, savePrefs } from '../settings/store'

/**
 * 检索页 —— 最终用户的主界面。
 *
 * · 默认查激活版本，版本选择支持从当前知识库版本链下拉挑选（回溯排查）。
 * · 必须显著展示 `storage_degraded` 降级信号。
 */
export function Search({
  kbId,
  presetVersionId,
  onClearPreset,
  onOpenKbManager,
}: {
  kbId: string | null
  presetVersionId?: string | null
  onClearPreset?: () => void
  onOpenKbManager?: () => void
}) {
  const [text, setText] = useState('')
  const [topK, setTopK] = useState('5')
  const [threshold, setThreshold] = useState('')
  const [aggregation, setAggregation] = useState<AggregationMethod>(
    AggregationMethod.AGGREGATION_METHOD_MEDIAN,
  )
  const [versionId, setVersionId] = useState('')
  const [advanced, setAdvanced] = useState(false)
  const [copiedKey, setCopiedKey] = useState<string | null>(null)
  const [previewDoc, setPreviewDoc] = useState<QueryResult | null>(null)

  const kb = useKnowledgeBase(kbId)
  const versions = useVersions(kbId)

  useEffect(() => {
    void (async () => {
      const p = await loadPrefs()
      setTopK(String(p.top_k))
      setAggregation(p.aggregation as AggregationMethod)
    })()
  }, [])

  // 从版本页点“在此版本检索”跳过来时自动填入目标版本
  useEffect(() => {
    if (presetVersionId !== undefined && presetVersionId !== null && presetVersionId !== '') {
      setVersionId(presetVersionId)
      onClearPreset?.()
    }
  }, [presetVersionId, onClearPreset])

  // 切换知识库时重置一次性的目标版本选择
  const [lastKbId, setLastKbId] = useState<string | null>(kbId)
  if (lastKbId !== kbId) {
    setLastKbId(kbId)
    setVersionId('')
  }

  const q = useQueryText()

  function submit(e: FormEvent) {
    e.preventDefault()
    if (kbId === null || text.trim() === '') return
    const k = Number(topK) || DEFAULT_PREFS.top_k
    const asked = text.trim()
    q.mutate(
      {
        knowledge_base_id: kbId,
        text: asked,
        top_k: k,
        threshold: threshold.trim() === '' ? undefined : Number(threshold),
        version_id: versionId.trim() === '' ? undefined : versionId.trim(),
        aggregation,
      },
      {
        onSuccess: (resp) => {
          void appendHistory({
            kind: 'query',
            knowledge_base_id: kbId,
            detail: `${asked} → 命中 ${resp.results.length} 条`,
          })
        },
      },
    )
    void savePrefs({ top_k: k, aggregation })
  }

  function copyText(key: string, value: string) {
    void navigator.clipboard?.writeText(value).then(() => {
      setCopiedKey(key)
      setTimeout(() => setCopiedKey((cur) => (cur === key ? null : cur)), 1500)
    })
  }

  const resp = q.data
  const versionList = [...(versions.data?.versions ?? [])]
    .filter((v) => v.data_status === DataStatus.DATA_STATUS_DURABLE && !v.deleting)
    .sort((a, b) => Number(b.version_id) - Number(a.version_id))

  if (kbId === null) {
    return (
      <div className="page">
        <div className="placeholder">
          <p style={{ margin: '0 0 10px' }}>
            还没有选中知识库——请先新建或选择一个知识库，然后即可在此输入问题发起检索。
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

      <form className="search-form" onSubmit={submit}>
        <input
          type="search"
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder={kbId === null ? '先选择一个知识库…' : '输入检索问题，按 Enter 发起语义检索'}
          disabled={kbId === null}
          aria-label="检索问题"
        />
        <button
          type="submit"
          className="btn-primary"
          disabled={kbId === null || text.trim() === '' || q.isPending}
        >
          {q.isPending ? '检索中…' : '检索'}
        </button>
      </form>

      <div className="search-quick-row">
        <label>
          <span>Top-K</span>
          <input
            type="number"
            min="1"
            max="1024"
            value={topK}
            onChange={(e) => setTopK(e.target.value)}
          />
        </label>
        <label>
          <span>聚合方式</span>
          <select
            value={aggregation}
            onChange={(e) => setAggregation(e.target.value as AggregationMethod)}
          >
            <option value={AggregationMethod.AGGREGATION_METHOD_MEDIAN}>MEDIAN（中位数）</option>
            <option value={AggregationMethod.AGGREGATION_METHOD_MAX}>MAX（最大值）</option>
            <option value={AggregationMethod.AGGREGATION_METHOD_MEAN}>MEAN（均值）</option>
          </select>
        </label>
        <label style={{ minWidth: 210 }}>
          <span>查询版本</span>
          <select
            value={versionId}
            disabled={kbId === null}
            onChange={(e) => setVersionId(e.target.value)}
          >
            <option value="">激活版本（默认）</option>
            {versionList.map((v) => {
              const isAct = v.version_id === kb.data?.knowledge_base?.active_version_id
              const idxTag =
                v.index_status === IndexStatus.INDEX_STATUS_READY ? '索引就绪' : '按需构建'
              return (
                <option key={v.version_id} value={v.version_id}>
                  v{v.version_id}
                  {isAct ? ' · 当前激活' : ''}（{idxTag}）
                </option>
              )
            })}
          </select>
        </label>
        <details
          className="advanced"
          open={advanced}
          onToggle={(e) => setAdvanced((e.target as HTMLDetailsElement).open)}
        >
          <summary>更多参数</summary>
          <div className="advanced-grid" style={{ paddingBottom: 0 }}>
            <label>
              <span>相似度阈值（可空）</span>
              <input
                type="number"
                step="0.01"
                placeholder="不限"
                value={threshold}
                onChange={(e) => setThreshold(e.target.value)}
              />
            </label>
            <label>
              <span>手动指定版本 ID</span>
              <input
                type="text"
                inputMode="numeric"
                value={versionId}
                onChange={(e) => setVersionId(e.target.value)}
                placeholder="留空 = 激活版本"
              />
            </label>
          </div>
        </details>
      </div>

      {q.error !== null && (
        <p className="error-box">
          {q.error instanceof ApiError && q.error.isUnavailable
            ? '后端不可达（或存储层降级）：'
            : '检索失败：'}
          {q.error.message}
        </p>
      )}

      {resp !== undefined && (
        <>
          <div className="result-meta">
            <span className="muted small">
              命中 <strong>{resp.results.length}</strong> 条 · 实际查询版本{' '}
              <code>v{resp.version_id}</code>
            </span>
            {resp.storage_degraded && (
              <span
                className="badge warn"
                title="存储层低于 quorum：读仍在服务，但副本掉得比要求的多"
              >
                <span className="dot" />
                存储已降级
              </span>
            )}
          </div>

          {resp.results.length === 0 ? (
            <div className="placeholder">
              {resp.version_id === '0' ? (
                <>
                  <strong>这个库还没有发布任何版本</strong>（激活版本为 0）。在「文档」页提交文档后需要
                  <strong>激活</strong>版本，检索页才查得到。
                </>
              ) : (
                <>
                  没有命中结果。可能是相似度阈值过高，或库内暂无匹配文档（使用 mock embed
                  时仅按 chunk_id 精确匹配文本）。
                </>
              )}
            </div>
          ) : (
            <ul className="result-list">
              {resp.results.map((r, idx) => (
                <li key={`${r.doc_id}-${idx}`} className="result-card">
                  <div className="result-head">
                    <div className="result-title">
                      <span className="chip accent">#{idx + 1}</span>
                      <code className="doc-id">{r.doc_id}</code>
                    </div>
                    <div className="actions">
                      <span className="chip score">score {r.score.toFixed(4)}</span>
                      <button
                        type="button"
                        className="btn-sm"
                        onClick={() => copyText(`id-${idx}`, r.doc_id)}
                      >
                        {copiedKey === `id-${idx}` ? '已复制 ID' : '复制 ID'}
                      </button>
                      <button
                        type="button"
                        className="btn-sm"
                        onClick={() => copyText(`body-${idx}`, r.content)}
                      >
                        {copiedKey === `body-${idx}` ? '已复制正文' : '复制正文'}
                      </button>
                      <button type="button" className="btn-sm" onClick={() => setPreviewDoc(r)}>
                        展开全文
                      </button>
                    </div>
                  </div>
                  <p className="result-content">{r.content}</p>
                </li>
              ))}
            </ul>
          )}
        </>
      )}

      {previewDoc !== null && (
        <div className="modal-backdrop" onClick={() => setPreviewDoc(null)}>
          <div className="modal modal-wide" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <code className="doc-id">{previewDoc.doc_id}</code>
              <span className="chip score">score {previewDoc.score.toFixed(4)}</span>
            </div>
            <pre className="log-view" style={{ maxHeight: '60vh', marginTop: 0 }}>
              {previewDoc.content}
            </pre>
            <div className="modal-actions">
              <button
                type="button"
                onClick={() => copyText('modal-body', previewDoc.content)}
              >
                {copiedKey === 'modal-body' ? '已复制' : '复制全文'}
              </button>
              <button type="button" onClick={() => setPreviewDoc(null)}>
                关闭
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
