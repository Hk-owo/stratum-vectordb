import { useState, type FormEvent } from 'react'

import { ApiError } from '../api/client'
import {
  IndexType,
  KBStatus,
  QuantizerType,
  Similarity,
  type KnowledgeBaseInfo,
} from '../api/gen/knowledgebase'
import {
  useCreateKnowledgeBase,
  useDeleteKnowledgeBase,
  useKnowledgeBases,
} from '../api/queries'
import { ConfirmButton } from './ConfirmButton'

/**
 * 知识库管理弹窗：列表查看、新建、切换、删除。
 *
 * 补齐之前 useCreateKnowledgeBase / useDeleteKnowledgeBase 在 queries.ts 写好
 * 却没有任何界面调用它的闭环问题。
 */
export function KbManagerDialog({
  open,
  selectedKbId,
  onSelectKb,
  onClearKb,
  onClose,
}: {
  open: boolean
  selectedKbId: string | null
  onSelectKb: (kbId: string) => void
  onClearKb: () => void
  onClose: () => void
}) {
  const kbs = useKnowledgeBases()
  const createKb = useCreateKnowledgeBase()
  const deleteKb = useDeleteKnowledgeBase()

  const [tab, setTab] = useState<'list' | 'create'>('list')
  const [name, setName] = useState('')
  const [windowSize, setWindowSize] = useState('512')
  const [overlapSize, setOverlapSize] = useState('64')
  const [indexType, setIndexType] = useState<IndexType>(IndexType.INDEX_TYPE_HNSW)
  const [similarity, setSimilarity] = useState<Similarity>(Similarity.SIMILARITY_COSINE)
  const [quantizer, setQuantizer] = useState<QuantizerType>(QuantizerType.QUANTIZER_OFF)
  const [pqM, setPqM] = useState('96')
  const [pqNbits, setPqNbits] = useState('8')
  const [embedAddr, setEmbedAddr] = useState('http://localhost:8080')
  const [embedModel, setEmbedModel] = useState('mock-embed-v1')
  const [replicaIdsRaw, setReplicaIdsRaw] = useState('')
  const [formError, setFormError] = useState<string | null>(null)

  if (!open) return null

  const list = kbs.data?.knowledge_bases ?? []
  const activeTab = list.length === 0 && tab === 'list' ? 'create' : tab

  async function handleCreate(e: FormEvent) {
    e.preventDefault()
    setFormError(null)
    const trimmedName = name.trim()
    const win = Number(windowSize)
    const overlap = Number(overlapSize)
    if (trimmedName === '') {
      setFormError('请输入知识库名称')
      return
    }
    if (!Number.isInteger(win) || win <= 0) {
      setFormError('切块窗口大小必须是正整数')
      return
    }
    if (!Number.isInteger(overlap) || overlap < 0 || overlap >= win) {
      setFormError('重叠大小必须是非负整数且小于窗口大小')
      return
    }
    const replicas = replicaIdsRaw
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s !== '')
    for (const r of replicas) {
      if (!/^\d+$/.test(r)) {
        setFormError(`存储副本节点 ID 必须是整数：${r}`)
        return
      }
    }

    try {
      const resp = await createKb.mutateAsync({
        name: trimmedName,
        chunk_window_size: win,
        chunk_overlap_size: overlap,
        index_type: indexType,
        similarity,
        quantizer,
        pq_m: quantizer === QuantizerType.QUANTIZER_PQ ? Number(pqM) || 96 : 0,
        pq_nbits: quantizer === QuantizerType.QUANTIZER_PQ ? Number(pqNbits) || 8 : 0,
        embed_config: {
          service_addr: embedAddr.trim(),
          model_id: embedModel.trim(),
        },
        storage_replica_ids: replicas,
      })
      onSelectKb(resp.knowledge_base_id)
      setName('')
      setTab('list')
      onClose()
    } catch (err) {
      setFormError(err instanceof Error ? err.message : String(err))
    }
  }

  async function handleDelete(kb: KnowledgeBaseInfo) {
    setFormError(null)
    try {
      await deleteKb.mutateAsync({ knowledge_base_id: kb.knowledge_base_id })
      if (selectedKbId === kb.knowledge_base_id) {
        onClearKb()
      }
    } catch (err) {
      setFormError(err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal modal-wide" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2 className="modal-title">知识库管理</h2>
          <div className="pill-tabs" role="tablist">
            <button
              type="button"
              role="tab"
              className="pill-tab"
              aria-selected={activeTab === 'list'}
              onClick={() => setTab('list')}
            >
              已有知识库 ({list.length})
            </button>
            <button
              type="button"
              role="tab"
              className="pill-tab"
              aria-selected={activeTab === 'create'}
              onClick={() => setTab('create')}
            >
              + 新建知识库
            </button>
          </div>
        </div>

        {formError !== null && <p className="error-box">{formError}</p>}

        {activeTab === 'list' && (
          <>
            {kbs.isPending && <p className="muted small">读取知识库列表…</p>}
            {kbs.error !== null && (
              <p className="error-box">
                读取失败：
                {kbs.error instanceof ApiError ? kbs.error.message : String(kbs.error)}
              </p>
            )}
            {list.length === 0 ? (
              <p className="placeholder">还没有任何知识库，切到「+ 新建知识库」创建一个。</p>
            ) : (
              <div className="table-wrap">
                <table className="queue-table">
                  <thead>
                    <tr>
                      <th>名称 / ID</th>
                      <th>状态</th>
                      <th>激活版本</th>
                      <th>索引与量化</th>
                      <th>Embed</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {list.map((kb) => {
                      const isCur = kb.knowledge_base_id === selectedKbId
                      return (
                        <tr key={kb.knowledge_base_id} className={isCur ? 'row-active' : undefined}>
                          <td>
                            <div>
                              <strong>{kb.name !== '' ? kb.name : '未命名'}</strong>
                            </div>
                            <code className="muted small">{kb.knowledge_base_id}</code>
                          </td>
                          <td>
                            <span
                              className={
                                kb.status === KBStatus.KB_STATUS_ACTIVE
                                  ? 'chip ok'
                                  : kb.status === KBStatus.KB_STATUS_DELETING
                                    ? 'chip warn'
                                    : 'chip bad'
                              }
                            >
                              {kb.status.replace('KB_STATUS_', '')}
                            </span>
                          </td>
                          <td>
                            <code>
                              {kb.active_version_id === '0' ? '—' : `v${kb.active_version_id}`}
                            </code>
                          </td>
                          <td className="small">
                            <div>
                              {kb.index_type.replace('INDEX_TYPE_', '')} /{' '}
                              {kb.similarity.replace('SIMILARITY_', '')}
                            </div>
                            <div className="muted">
                              {kb.quantizer.replace('QUANTIZER_', '')} · 切块 {kb.chunk_window_size}/
                              {kb.chunk_overlap_size}
                            </div>
                          </td>
                          <td className="small">
                            <div>{kb.embed_config?.model_id || '—'}</div>
                            <div className="muted">{kb.embed_config?.service_addr || ''}</div>
                          </td>
                          <td className="actions">
                            <button
                              type="button"
                              disabled={isCur}
                              onClick={() => {
                                onSelectKb(kb.knowledge_base_id)
                                onClose()
                              }}
                            >
                              {isCur ? '当前' : '切换'}
                            </button>
                            <ConfirmButton
                              label="删除"
                              confirmLabel="确认删除库"
                              className="danger"
                              disabled={
                                deleteKb.isPending || kb.status === KBStatus.KB_STATUS_DELETING
                              }
                              onConfirm={() => void handleDelete(kb)}
                            />
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}

        {activeTab === 'create' && (
          <form className="page" onSubmit={(e) => void handleCreate(e)}>
            <div className="advanced-grid">
              <label>
                <span>名称 *</span>
                <input
                  type="text"
                  value={name}
                  placeholder="例如：产品手册"
                  onChange={(e) => setName(e.target.value)}
                />
              </label>
              <label>
                <span>切块窗口大小（chunk_window_size）</span>
                <input
                  type="number"
                  min={1}
                  value={windowSize}
                  onChange={(e) => setWindowSize(e.target.value)}
                />
              </label>
              <label>
                <span>重叠大小（chunk_overlap_size）</span>
                <input
                  type="number"
                  min={0}
                  value={overlapSize}
                  onChange={(e) => setOverlapSize(e.target.value)}
                />
              </label>
              <label>
                <span>索引类型（创建后不可改）</span>
                <select
                  value={indexType}
                  onChange={(e) => setIndexType(e.target.value as IndexType)}
                >
                  <option value={IndexType.INDEX_TYPE_HNSW}>HNSW</option>
                  <option value={IndexType.INDEX_TYPE_IVF}>IVF</option>
                  <option value={IndexType.INDEX_TYPE_FLAT}>FLAT</option>
                </select>
              </label>
              <label>
                <span>相似度度量</span>
                <select
                  value={similarity}
                  onChange={(e) => setSimilarity(e.target.value as Similarity)}
                >
                  <option value={Similarity.SIMILARITY_COSINE}>COSINE</option>
                  <option value={Similarity.SIMILARITY_EUCLIDEAN}>EUCLIDEAN</option>
                  <option value={Similarity.SIMILARITY_INNER_PRODUCT}>INNER_PRODUCT</option>
                </select>
              </label>
              <label>
                <span>向量量化（创建后不可改）</span>
                <select
                  value={quantizer}
                  onChange={(e) => setQuantizer(e.target.value as QuantizerType)}
                >
                  <option value={QuantizerType.QUANTIZER_OFF}>OFF（全精度）</option>
                  <option value={QuantizerType.QUANTIZER_SQ8}>SQ8</option>
                  <option value={QuantizerType.QUANTIZER_SQ_BF16}>SQ_BF16</option>
                  <option value={QuantizerType.QUANTIZER_SQ_FP16}>SQ_FP16</option>
                  <option value={QuantizerType.QUANTIZER_PQ}>PQ</option>
                </select>
              </label>
              {quantizer === QuantizerType.QUANTIZER_PQ && (
                <>
                  <label>
                    <span>PQ 子空间数（pq_m）</span>
                    <input type="number" min={1} value={pqM} onChange={(e) => setPqM(e.target.value)} />
                  </label>
                  <label>
                    <span>PQ 位数（pq_nbits）</span>
                    <input
                      type="number"
                      min={1}
                      value={pqNbits}
                      onChange={(e) => setPqNbits(e.target.value)}
                    />
                  </label>
                </>
              )}
              <label>
                <span>Embed 服务地址</span>
                <input
                  type="text"
                  value={embedAddr}
                  placeholder="http://localhost:8080"
                  onChange={(e) => setEmbedAddr(e.target.value)}
                />
              </label>
              <label>
                <span>Embed 模型 ID</span>
                <input
                  type="text"
                  value={embedModel}
                  placeholder="mock-embed-v1"
                  onChange={(e) => setEmbedModel(e.target.value)}
                />
              </label>
              <label>
                <span>指定存储节点 ID（可选，逗号分隔）</span>
                <input
                  type="text"
                  value={replicaIdsRaw}
                  placeholder="留空则按集群默认策略"
                  onChange={(e) => setReplicaIdsRaw(e.target.value)}
                />
              </label>
            </div>
            <div className="modal-actions">
              <button type="submit" className="btn-primary" disabled={createKb.isPending}>
                {createKb.isPending ? '创建中…' : '创建知识库'}
              </button>
            </div>
          </form>
        )}

        <div className="modal-actions">
          <button type="button" onClick={onClose}>
            关闭
          </button>
        </div>
      </div>
    </div>
  )
}
