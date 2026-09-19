/**
 * 偏好持久化的浏览器路径。
 *
 * 在 node 环境下 `isDesktop()` 为 false，所以这些用例走的正是浏览器分支
 * （localStorage），不需要 jsdom —— 这里只 stub 掉 `localStorage` 本身。
 * 桌面分支由 Rust 侧的 11 个测试覆盖（`web/src-tauri/src/store.rs`）。
 */
import { beforeEach, describe, expect, it, vi } from 'vitest'

// isDesktop() 读 window.__TAURI__；node 下没有 window，自然是"非桌面"。
// 这里显式断言一次，免得将来有人给 isDesktop 换实现后这些用例静默走错分支。
import { isDesktop } from '../desktop/tauri'
import { DEFAULT_PREFS, loadPrefs, savePrefs } from './store'

/** 最小可用的 localStorage 替身。 */
function installStorage(seed: Record<string, string> = {}): Map<string, string> {
  const m = new Map(Object.entries(seed))
  vi.stubGlobal('localStorage', {
    getItem: (k: string) => m.get(k) ?? null,
    setItem: (k: string, v: string) => void m.set(k, v),
    removeItem: (k: string) => void m.delete(k),
    clear: () => m.clear(),
  })
  return m
}

/** 让 setItem 抛错，模拟配额满或隐私模式。 */
function installFailingStorage(): void {
  vi.stubGlobal('localStorage', {
    getItem: () => null,
    setItem: () => {
      throw new DOMException('QuotaExceededError')
    },
    removeItem: () => undefined,
    clear: () => undefined,
  })
}

describe('偏好持久化（浏览器路径）', () => {
  beforeEach(() => {
    vi.unstubAllGlobals()
  })

  it('前置条件：node 环境下走的是浏览器分支', () => {
    // 这条如果红了，下面所有用例验证的就不是它们声称的东西。
    expect(isDesktop()).toBe(false)
  })

  it('没有记录时回退默认值', async () => {
    installStorage()
    expect(await loadPrefs()).toEqual(DEFAULT_PREFS)
  })

  it('写进去再读出来是同一份', async () => {
    installStorage()
    await savePrefs({ selected_kb_id: 'kb-7', top_k: 12 })
    const p = await loadPrefs()
    expect(p.selected_kb_id).toBe('kb-7')
    expect(p.top_k).toBe(12)
    // 没传的字段保持不变，而不是被抹成 undefined —— 保存检索参数不该顺手
    // 清掉选中的知识库。
    expect(p.aggregation).toBe(DEFAULT_PREFS.aggregation)
    expect(p.durable_only).toBe(DEFAULT_PREFS.durable_only)
  })

  it('多次保存是合并，不是覆盖', async () => {
    installStorage()
    await savePrefs({ selected_kb_id: 'kb-1' })
    await savePrefs({ durable_only: true })
    const p = await loadPrefs()
    expect(p.selected_kb_id).toBe('kb-1')
    expect(p.durable_only).toBe(true)
  })

  it('文件被写坏时回退默认值，而不是抛出去', async () => {
    installStorage({ 'stratum:prefs': '{ 这不是 JSON' })
    // 一份读不出来的偏好不该让界面起不来。
    expect(await loadPrefs()).toEqual(DEFAULT_PREFS)
  })

  it('缺字段的旧记录用默认值补齐', async () => {
    // 兼容"上一个版本只存了部分字段"的情形：缺的补默认，多的忽略。
    installStorage({ 'stratum:prefs': JSON.stringify({ top_k: 9 }) })
    const p = await loadPrefs()
    expect(p.top_k).toBe(9)
    expect(p.selected_kb_id).toBe(DEFAULT_PREFS.selected_kb_id)
    expect(p.aggregation).toBe(DEFAULT_PREFS.aggregation)
  })

  it('写不进去时不抛错（配额满 / 隐私模式）', async () => {
    installFailingStorage()
    // 记不住偏好只影响下次打开时的默认值，不该拦下用户当前的操作。
    await expect(savePrefs({ top_k: 3 })).resolves.toBeUndefined()
  })
})
