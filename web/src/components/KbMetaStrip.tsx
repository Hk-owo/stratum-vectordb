import { KBStatus, type KnowledgeBaseInfo } from '../api/gen/knowledgebase'

function shortEnum(raw: string, prefix: string): string {
  return raw.startsWith(prefix) ? raw.slice(prefix.length) : raw
}

function kbStatusLabel(st: KBStatus): { label: string; cls: string } {
  switch (st) {
    case KBStatus.KB_STATUS_ACTIVE:
      return { label: '活跃', cls: 'chip ok' }
    case KBStatus.KB_STATUS_DELETING:
      return { label: '删除中', cls: 'chip warn' }
    case KBStatus.KB_STATUS_DELETE_FAILED:
      return { label: '删除失败', cls: 'chip bad' }
    default:
      return { label: st, cls: 'chip' }
  }
}

/** 知识库配置与激活版本摘要条，用于在文档/版本页快速确认当前库的参数。 */
export function KbMetaStrip({
  kb,
  onManageKbs,
}: {
  kb: KnowledgeBaseInfo
  onManageKbs?: () => void
}) {
  const st = kbStatusLabel(kb.status)
  return (
    <div className="kb-meta-bar">
      <div className="kb-meta-chips">
        <strong>{kb.name !== '' ? kb.name : kb.knowledge_base_id}</strong>
        <code className="muted small">{kb.knowledge_base_id}</code>
        <span className={st.cls}>{st.label}</span>
        <span className="chip accent">
          激活版本 {kb.active_version_id === '0' ? '未发布' : `v${kb.active_version_id}`}
        </span>
        <span className="chip">
          {shortEnum(kb.index_type, 'INDEX_TYPE_')} · {shortEnum(kb.similarity, 'SIMILARITY_')} ·{' '}
          {shortEnum(kb.quantizer, 'QUANTIZER_')}
        </span>
        <span className="chip">
          切块 {kb.chunk_window_size}/{kb.chunk_overlap_size}
        </span>
        {kb.embed_config !== undefined && kb.embed_config.model_id !== '' && (
          <span className="chip" title={kb.embed_config.service_addr}>
            Embed: {kb.embed_config.model_id}
          </span>
        )}
      </div>
      {onManageKbs !== undefined && (
        <button type="button" className="btn-sm" onClick={onManageKbs}>
          管理知识库
        </button>
      )}
    </div>
  )
}
