import { useEffect } from 'react'

import { ApiError } from '../api/client'
import { useKnowledgeBases } from '../api/queries'

/**
 * 全局知识库选择器 + 管理入口。
 */
export function KbPicker({
  value,
  onChange,
  onClear,
  onOpenManager,
}: {
  value: string | null
  onChange: (kbId: string) => void
  onClear?: () => void
  onOpenManager: () => void
}) {
  const kbs = useKnowledgeBases()
  const list = kbs.data?.knowledge_bases ?? []

  // 若当前未选库（或上次选的库已被删除），且列表非空，自动选中第一个活跃库，
  // 避免刚打开控制台或新建/删库后页面处于“全禁用等待选库”的空白状态。
  useEffect(() => {
    if (kbs.data === undefined) return
    if (list.length === 0) {
      if (value !== null) onClear?.()
      return
    }
    const exists = value !== null && list.some((kb) => kb.knowledge_base_id === value)
    if (!exists) {
      const first = list[0]
      if (first !== undefined) onChange(first.knowledge_base_id)
    }
  }, [kbs.data, list, value, onChange, onClear])

  if (kbs.isPending) return <span className="muted small">加载知识库…</span>

  if (kbs.error !== null) {
    const err = kbs.error
    const is401 = err instanceof ApiError && err.status === 401
    const msg = is401
      ? '未授权（请改用 http://127.0.0.1:8081 访问，或带凭据调用）'
      : err instanceof ApiError && err.isUnavailable
        ? '网关/后端不可达'
        : err instanceof Error
          ? err.message
          : String(err)
    return (
      <span className="kb-picker">
        <span className="badge bad" title={err instanceof Error ? err.message : String(err)}>
          知识库列表失败：{msg}
        </span>
        <button type="button" className="btn-sm" onClick={onOpenManager}>
          管理知识库
        </button>
      </span>
    )
  }

  if (list.length === 0) {
    return (
      <span className="kb-picker">
        <span className="muted small">暂无知识库</span>
        <button type="button" className="btn-primary btn-sm" onClick={onOpenManager}>
          + 新建知识库
        </button>
      </span>
    )
  }

  return (
    <span className="kb-picker">
      <label className="kb-picker">
        <span className="muted">知识库</span>
        <select value={value ?? ''} onChange={(e) => onChange(e.target.value)}>
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
      <button type="button" className="btn-sm" onClick={onOpenManager} title="查看、新建或删除知识库">
        管理
      </button>
    </span>
  )
}
