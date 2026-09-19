/**
 * 摄入队列：一次选中的那批文件，从"刚选进来"走到"已就绪"。
 *
 * 它存在的理由是版本链的约束（client-integration-guide §2.1）：**同一 KB 上的
 * 提交必须串行**——一个父版本最多一个子版本，所以必须是「提交一批 → 等它 READY
 * → 再提交下一批」。多文件不是并发提交，是一批批排队走。
 *
 * 状态留在这里，记录留在 IndexedDB（pending/store.ts）。页面刷新后能从后者
 * 接着走——这是这套设计唯一能救回"数据没落地的版本"的途径，所以队列状态本身
 * 不需要持久化，能重建即可。
 */
import type { ParseResult } from './parsers'

export type QueueStatus =
  /** 已选中，等待解析。 */
  | 'pending'
  /** 正在解析。 */
  | 'parsing'
  /** 内容与上次成功提交的一致，不会进 changes。见 ingest/hash.ts。 */
  | 'unchanged'
  /** 解析完成，等待被分批提交。 */
  | 'ready'
  /** 正在提交（CreateVersion）。 */
  | 'submitting'
  /** 已提交，正在等就绪。 */
  | 'awaiting'
  /** 已就绪（INDEX_READY）。 */
  | 'done'
  | 'error'

export interface QueueItem {
  /** 本地唯一键 = doc_id（它本来就是"文档身份"）。 */
  docId: string
  file: File
  status: QueueStatus
  /** 解析出的全文；未解析时为 ''。 */
  content: string
  /** 内容指纹，用于判断"改了吗"。未解析时为 ''。 */
  hash: string
  /** 解析器的说明（docx 的警告等）。 */
  note?: string
  /** 失败原因 / 终局判定（DISCARD 等）。 */
  message?: string
  /** 提交后分配到的版本号（protojson 的 int64 → 字符串）。 */
  versionId?: string
}

/** 已提交内容的指纹：doc_id → hash。用来判断下次是否真的需要提交。 */
export type SubmittedDigests = Record<string, string>

const DIGEST_KEY = 'stratum:submitted-digests'

/**
 * 读物化记录。localStorage 够用：它只存 doc_id → 一个 64 字符的哈希，
 * 几千个文档也就几百 KB，且读是同步的（队列渲染时每项都要查一次）。
 */
export function loadDigests(): SubmittedDigests {
  try {
    const raw = localStorage.getItem(DIGEST_KEY)
    return raw === null ? {} : (JSON.parse(raw) as SubmittedDigests)
  } catch {
    // 读不出来就当作"没有记录"：后果只是可能多提交一次内容没变的批次
    // （服务端会正常受理），而不是把队列卡死。
    return {}
  }
}

export function saveDigests(d: SubmittedDigests): void {
  try {
    localStorage.setItem(DIGEST_KEY, JSON.stringify(d))
  } catch {
    // 写不进去（配额/隐私模式）不影响本次提交，只影响下次的去重。不拦。
  }
}

/** 解析结果合并进队列项。 */
export function applyParseResult(item: QueueItem, result: ParseResult, hash: string): QueueItem {
  return { ...item, content: result.text, hash, note: result.note, status: 'ready' }
}

/** 汇总给界面用的计数。 */
export function summarize(items: readonly QueueItem[]): Record<QueueStatus, number> {
  const out = {
    pending: 0,
    parsing: 0,
    unchanged: 0,
    ready: 0,
    submitting: 0,
    awaiting: 0,
    done: 0,
    error: 0,
  } satisfies Record<QueueStatus, number>
  for (const it of items) out[it.status] += 1
  return out
}
