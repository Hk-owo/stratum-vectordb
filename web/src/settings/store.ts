/**
 * 界面偏好的持久化：选中的知识库、检索参数、导入模式。
 *
 * 双后端，和 `pending/store.ts` 同一套路：桌面写 `settings.json`（Rust 侧），
 * 浏览器写 localStorage。两边共用同一份 schema —— 字段名、默认值都对齐，所以
 * 同一份文件在两边读出来意思一样。
 *
 * 浏览器没有 `gateway_url` 这一项：页面由网关同源提供，地址由「从哪个地址打开
 * 页面」决定，前端不需要也不该改它（改成别的地址会跨域，而网关有意不设 CORS
 * 头）。桌面的网关地址由 SettingsDialog 单独负责。
 *
 * 与 Rust 侧一致，读失败一律回退默认值：一份读不出来的偏好不该让界面起不来。
 */
import { desktop, isDesktop } from '../desktop/tauri'

export interface Prefs {
  /** 上次选中的知识库。空表示还没选过，界面据此显示占位。 */
  selected_kb_id: string | null
  top_k: number
  aggregation: string
  /** 导入模式：true 表示按 AWAIT_TARGET_DATA_DURABLE 收口。 */
  durable_only: boolean
}

/** 与 Rust 侧 store.rs 的 default_top_k / default_aggregation 保持一致。 */
export const DEFAULT_PREFS: Prefs = {
  selected_kb_id: null,
  top_k: 5,
  aggregation: 'AGGREGATION_METHOD_MEDIAN',
  durable_only: false,
}

const KEY = 'stratum:prefs'

/**
 * 只挑出偏好字段。Rust 的 Settings 还带 schema_version 和 gateway_url，它们
 * 归 SettingsDialog 管，这里不碰 —— 免得保存检索参数时把网关地址一起改了。
 */
function prefsOf(s: { selected_kb_id: string | null; top_k: number; aggregation: string; durable_only: boolean }): Prefs {
  return {
    selected_kb_id: s.selected_kb_id,
    top_k: s.top_k,
    aggregation: s.aggregation,
    durable_only: s.durable_only,
  }
}

export async function loadPrefs(): Promise<Prefs> {
  if (isDesktop()) {
    const s = await desktop.loadSettings().catch(() => null)
    return s === null ? { ...DEFAULT_PREFS } : prefsOf(s)
  }

  try {
    const raw = localStorage.getItem(KEY)
    if (raw === null) return { ...DEFAULT_PREFS }
    // 与 Rust 侧同样的容忍：缺失或多余的字段不该让整份偏好失效。
    return { ...DEFAULT_PREFS, ...(JSON.parse(raw) as Partial<Prefs>) }
  } catch {
    return { ...DEFAULT_PREFS }
  }
}

/**
 * 合并写入。只传变化的字段，调用方不必先读一遍 —— 也避免两个页面各自持有一份
 * 陈旧快照时互相覆盖。
 */
export async function savePrefs(patch: Partial<Prefs>): Promise<void> {
  const next = { ...(await loadPrefs()), ...patch }

  if (isDesktop()) {
    // 读回完整 Settings 再覆盖偏好字段，保住 gateway_url 和 schema_version。
    const s = (await desktop.loadSettings().catch(() => null)) ?? {
      schema_version: 1,
      gateway_url: 'http://127.0.0.1:8081',
      ...DEFAULT_PREFS,
    }
    await desktop.saveSettings({ ...s, ...next }).catch(() => undefined)
    return
  }

  try {
    localStorage.setItem(KEY, JSON.stringify(next))
  } catch {
    // 配额满或隐私模式下写不进去。只影响下次打开时的默认值，不拦当前操作 ——
    // 和 loadDigests 的处理同一个理由。
  }
}
