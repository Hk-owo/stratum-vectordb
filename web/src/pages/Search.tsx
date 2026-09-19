import { useEffect, useState, type FormEvent } from 'react'

import { ApiError } from '../api/client'
import { AggregationMethod } from '../api/gen/query'
import { useQueryText } from '../api/queries'
import { appendHistory } from '../history/store'
import { DEFAULT_PREFS, loadPrefs, savePrefs } from '../settings/store'

/**
 * 检索页 —— 最终用户的主界面。
 *
 * 两条产品约定（来自我们的设计决定）：
 *   · **默认查激活版本**，版本选择器收在高级区。最终用户不该看到"版本 12 / 15"
 *     这种概念；能翻历史版本是回溯能力，不是日常路径。
 *   · **`storage_degraded` 必须展示**。它是只读调用方能拿到的唯一降级信号
 *     （QueryResponse 的注释写明了这一点：低于 quorum 时读照样服务，所以不主动
 *     报出来，只读用户永远发现不了）。
 */
export function Search({ kbId }: { kbId: string | null }) {
  const [text, setText] = useState('')
  const [topK, setTopK] = useState('5')
  const [threshold, setThreshold] = useState('')
  const [aggregation, setAggregation] = useState<AggregationMethod>(
    AggregationMethod.AGGREGATION_METHOD_MEDIAN,
  )
  const [versionId, setVersionId] = useState('')
  const [advanced, setAdvanced] = useState(false)

  // 恢复上次用的检索参数。只在挂载时读一次：此后用户改的就是权威值，别让一次
  // 迟到的读把它顶掉。查询文本和版本号**不**恢复——前者是这一问的内容，后者
  // 是"这次要看哪个版本"的一次性选择，留着它们反而会让下一次检索查错地方。
  useEffect(() => {
    void (async () => {
      const p = await loadPrefs()
      setTopK(String(p.top_k))
      setAggregation(p.aggregation as AggregationMethod)
    })()
  }, [])

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
        // 原样传字符串：protojson 的 int64 是字符串，拿响应里的值再传回来才不会
        // 出现 "12" === 12 这种恒假比较。
        version_id: versionId.trim() === '' ? undefined : versionId.trim(),
        aggregation,
      },
      {
        // 记录"问过什么"。用返回的命中数而不是请求参数——历史要能回答"这一问
        // 有没有结果"，而那是响应才知道的事。
        onSuccess: (resp) => {
          void appendHistory({
            kind: 'query',
            knowledge_base_id: kbId,
            detail: `${asked} → 命中 ${resp.results.length} 条`,
          })
        },
      },
    )
    // 记下这次实际用的参数，下次打开就是它。
    void savePrefs({ top_k: k, aggregation })
  }

  const resp = q.data

  return (
    <div className="page">
      <form className="search-form" onSubmit={submit}>
        <input
          type="search"
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder={kbId === null ? '先在上面选一个知识库' : '输入问题，回车检索'}
          disabled={kbId === null}
          aria-label="检索问题"
        />
        <button type="submit" disabled={kbId === null || text.trim() === '' || q.isPending}>
          {q.isPending ? '检索中…' : '检索'}
        </button>
      </form>

      <details
        className="advanced"
        open={advanced}
        onToggle={(e) => setAdvanced((e.target as HTMLDetailsElement).open)}
      >
        <summary>高级</summary>
        <div className="advanced-grid">
          <label>
            <span>top-k</span>
            <input type="number" min="1" max="100" value={topK} onChange={(e) => setTopK(e.target.value)} />
          </label>
          <label>
            <span>阈值（可空）</span>
            <input type="number" step="0.01" value={threshold} onChange={(e) => setThreshold(e.target.value)} />
          </label>
          <label>
            <span>聚合</span>
            <select
              value={aggregation}
              onChange={(e) => setAggregation(e.target.value as AggregationMethod)}
            >
              <option value={AggregationMethod.AGGREGATION_METHOD_MEDIAN}>MEDIAN</option>
              <option value={AggregationMethod.AGGREGATION_METHOD_MAX}>MAX</option>
              <option value={AggregationMethod.AGGREGATION_METHOD_MEAN}>MEAN</option>
            </select>
          </label>
          <label>
            <span>版本（可空 = 激活版本）</span>
            <input
              type="text"
              inputMode="numeric"
              value={versionId}
              onChange={(e) => setVersionId(e.target.value)}
              placeholder="留空"
            />
          </label>
        </div>
      </details>

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
            <span className="muted">
              命中 {resp.results.length} 条 · 实际查询版本 <code>v{resp.version_id}</code>
            </span>
            {resp.storage_degraded && (
              <span className="badge warn" title="存储层低于 quorum：读仍在服务，但副本掉得比要求的多">
                <span className="dot" />
                存储已降级
              </span>
            )}
          </div>

          {resp.results.length === 0 ? (
            <p className="placeholder">
              {resp.version_id === '0' ? (
                <>
                  <strong>这个库还没有发布任何版本</strong>（激活版本为 0）。提交文档之后还需要
                  <strong>激活</strong>那个版本，检索页才查得到——「文档」页提交完会直接提示这一步，
                  也可以在「版本」页手动激活。
                </>
              ) : (
                <>
                  没有结果。可能是阈值设太高、或者库里确实没有匹配的内容——另外，如果这个库用的是
                  mock embed，文本检索会退化成<strong>精确匹配</strong>：mock 服务按 chunk_id 派生
                  向量、不做语义编码，所以只有文本基本一致才会命中。
                </>
              )}
            </p>
          ) : (
            <ul className="result-list">
              {resp.results.map((r) => (
                <li key={`${r.doc_id}-${r.score}`} className="result-card">
                  <div className="result-head">
                    <code className="doc-id">{r.doc_id}</code>
                    <span className="score">{r.score.toFixed(4)}</span>
                  </div>
                  <p className="result-content">{r.content}</p>
                </li>
              ))}
            </ul>
          )}
        </>
      )}
    </div>
  )
}
