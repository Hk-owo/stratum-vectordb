/**
 * 本机操作历史：提交、重发、激活、放弃、删除、检索。
 *
 * 双后端，和 `pending/store.ts` 同一套路：
 *
 * | 环境 | 后端 |
 * |---|---|
 * | 桌面（Tauri） | `history.jsonl`（Rust 侧，1 MiB 轮转 + 保留 2000 条） |
 * | 浏览器 | IndexedDB |
 *
 * **浏览器侧刻意不放 localStorage**：历史的上限是 1 MiB，而 localStorage 的配额
 * 只有约 5 MB 且整个源共享，还要和 `submitted-digests` 挤；更别提它是同步 API，
 * 写一次就阻塞一次主线程。IndexedDB 的配额大得多、天生异步，本来就是干这个的。
 *
 * 记录里**不放检索结果**。历史要回答的是「我做过什么」，不是「当时返回了什么」
 * —— 后者会让一条记录从几百字节涨到几兆字节，而答案在知识库里随时可以重查。
 */
import { desktop, isDesktop } from '../desktop/tauri'
import { HISTORY_STORE, run } from '../idb'

export type HistoryKind = 'submit' | 'resend' | 'activate' | 'discard' | 'delete' | 'query'

export interface HistoryEntry {
  /** ISO 8601。由前端生成，避免在 Rust 侧引入时间库。 */
  at: string
  kind: HistoryKind
  knowledge_base_id: string
  /** 人读的一行说明。**要短**：它是这条记录体积的全部变量。 */
  detail: string
}

/**
 * 界面一次展示多少条。取 200 是因为再多也没人翻——而文件侧的上限（2000 条 /
 * 1 MiB）是留给"以后要查"的，不是留给这一屏的。
 */
export const HISTORY_LIMIT = 200

/** `detail` 的上限。中文按 UTF-8 三字节算，200 字约 600 字节，一条记录约 0.7 KB。 */
export const DETAIL_MAX = 200

/** 导出是为了可测：截断是这里唯一有分支的纯逻辑。 */
export function clipDetail(s: string): string {
  const t = s.trim().replace(/\s+/g, ' ')
  return t.length <= DETAIL_MAX ? t : `${t.slice(0, DETAIL_MAX - 1)}…`
}

export function nowISO(): string {
  return new Date().toISOString()
}

/**
 * 追加一条。**best-effort**：记录失败不该让被记录的操作失败——这一步是在用户
 * 的操作成功之后跑的，抛出去只会把成功说成失败。
 */
export async function appendHistory(
  e: Omit<HistoryEntry, 'at' | 'detail'> & { at?: string; detail: string },
): Promise<void> {
  const entry: HistoryEntry = {
    at: e.at ?? nowISO(),
    kind: e.kind,
    knowledge_base_id: e.knowledge_base_id,
    detail: clipDetail(e.detail),
  }

  if (isDesktop()) {
    await desktop.appendHistory(entry).catch(() => undefined)
    return
  }

  try {
    await run<IDBValidKey>(HISTORY_STORE, 'readwrite', (s) => s.add(entry))
  } catch {
    // 隐私模式 / 配额满。同上：不拦操作。
  }
}

/**
 * 读最近 `limit` 条，**新的在前**。
 *
 * 浏览器侧按自增键全量取出再截断：历史的写入模式只有追加，条目数由文件侧的轮转
 * 和这里的 limit 共同约束，不值得为它建索引。
 */
export async function loadHistory(limit: number = HISTORY_LIMIT): Promise<HistoryEntry[]> {
  if (isDesktop()) {
    const rows = await desktop.loadHistory(limit).catch(() => [])
    // Rust 侧把 kind 当自由字符串存（它不认识前端的联合类型），读回来时收窄。
    // 值仍可能不在这几个里——那说明文件被手工改过，界面按原样显示即可。
    return rows.map((e) => ({ ...e, kind: e.kind as HistoryKind }))
  }

  try {
    const all = await run<HistoryEntry[]>(HISTORY_STORE, 'readonly', (s) =>
      s.getAll() as IDBRequest<HistoryEntry[]>,
    )
    // 自增键保证插入顺序，倒序即"新的在前"。
    return all.slice(-limit).reverse()
  } catch {
    return []
  }
}
