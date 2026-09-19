/**
 * 桌面壳（Tauri）的接入点。
 *
 * 同一份前端跑在两种环境里，这里负责把它们之间**仅有的两处差异**收拢：
 *
 * | | 浏览器（gateway 的 -static） | 桌面（Tauri） |
 * |---|---|---|
 * | 本机持久化 | IndexedDB（易失） | 本机文件（`app_data_dir`） |
 * | API 地址 | 同源相对路径 `/api/…` | 绝对地址 `http://127.0.0.1:8081` |
 *
 * 第二行不是可选项：Tauri 的 WebView 从 `tauri://localhost` 加载，相对路径会打到
 * 应用自己身上，而不是 gateway。
 *
 * 浏览器下这些函数全是 no-op，所以开发时照旧 `npm run dev` 即可。
 */
import { invoke as tauriInvoke } from '@tauri-apps/api/core'

/** 是否跑在桌面壳里。Tauri 2 会注入 `__TAURI_INTERNALS__`。 */
export function isDesktop(): boolean {
  return typeof window !== 'undefined' && '__TAURI_INTERNALS__' in window
}

/** 调一个 Rust 命令。非桌面环境下抛错——调用方应先问 `isDesktop()`。 */
export async function invoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  if (!isDesktop()) {
    throw new Error(`invoke(${cmd}) 只在桌面壳里可用`)
  }
  return tauriInvoke<T>(cmd, args)
}

// ------------------------------------------------------------------ 类型
// 与 src-tauri/src/store.rs 的 struct 逐字对应。改一边就要改另一边。

export interface DesktopChange {
  op: string
  doc_id: string
  content?: string
}

export interface DesktopPendingWrite {
  id: string
  knowledge_base_id: string
  /** protojson 的 int64 → 字符串；空串表示还没拿到版本号。 */
  version_id: string
  client_request_id: string
  changes: DesktopChange[]
  submitted_at: string
  settled: boolean
}

export interface DesktopSettings {
  schema_version: number
  /** gateway 的绝对地址；桌面壳非同源，必须显式给。 */
  gateway_url: string
  selected_kb_id: string | null
  top_k: number
  aggregation: string
  durable_only: boolean
}

export interface DesktopHistoryEntry {
  /** ISO 8601，由前端生成。 */
  at: string
  /** submit | resend | activate | query | discard … */
  kind: string
  knowledge_base_id: string
  detail: string
}

// ------------------------------------------------------------------ 命令

export const desktop = {
  loadPending: () => invoke<DesktopPendingWrite[]>('load_pending'),
  upsertPending: (record: DesktopPendingWrite) =>
    invoke<void>('upsert_pending', { record }),
  removePending: (id: string) => invoke<void>('remove_pending', { id }),

  loadSettings: () => invoke<DesktopSettings>('load_settings'),
  saveSettings: (settings: DesktopSettings) => invoke<void>('save_settings', { settings }),

  appendHistory: (entry: DesktopHistoryEntry) => invoke<void>('append_history', { entry }),
  loadHistory: (limit: number) => invoke<DesktopHistoryEntry[]>('load_history', { limit }),

  /** 本机记录的存放目录——界面上显示出来，用户才知道去哪儿备份。 */
  storeDir: () => invoke<string>('store_dir'),
}
