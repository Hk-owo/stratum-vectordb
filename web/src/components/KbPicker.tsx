import { useKnowledgeBases } from '../api/queries'
import { ApiError } from '../api/client'

/**
 * 全局知识库选择器。
 *
 * 放在顶部而不是每页一个：检索、文档、版本三页都作用于"当前这一个库"，让用户
 * 在不同页之间切换时重新选一次库是纯粹的浪费。
 */
export function KbPicker({
  value,
  onChange,
}: {
  value: string | null
  onChange: (kbId: string) => void
}) {
  const kbs = useKnowledgeBases()

  if (kbs.isPending) return <span className="muted">加载知识库…</span>

  if (kbs.error !== null) {
    const err = kbs.error
    const msg = err instanceof ApiError && err.isUnavailable
      ? '网关/后端不可达'
      : err instanceof Error
        ? err.message
        : String(err)
    return <span className="badge bad">知识库列表失败：{msg}</span>
  }

  const list = kbs.data?.knowledge_bases ?? []

  if (list.length === 0) {
    return <span className="muted">还没有知识库（先去「文档」页或运维侧建一个）</span>
  }

  return (
    <label className="kb-picker">
      <span className="muted">知识库</span>
      <select value={value ?? ''} onChange={(e) => onChange(e.target.value)}>
        {/* 未选中时占位，避免浏览器默认选中第一项却与 state 不一致 */}
        <option value="" disabled>
          选择…
        </option>
        {list.map((kb) => (
          <option key={kb.knowledge_base_id} value={kb.knowledge_base_id}>
            {kb.name !== '' ? `${kb.name}（${kb.knowledge_base_id}）` : kb.knowledge_base_id}
          </option>
        ))}
      </select>
    </label>
  )
}
