/**
 * 「现在退出会丢什么」的同步快照。
 *
 * 退出判据必须能在**微任务之外**回答：`beforeunload` 回调不允许 await，Tauri 的
 * `CloseRequested` 也不给第二次机会——先读一遍状态（异步）再决定拦不拦，用户早就
 * 把窗口关掉了。所以这里只存同步可读的标量，由真正做事的页面维护：
 *
 * | 字段 | 谁写 | 退出时意味着什么 |
 * |---|---|---|
 * | `unsettled`（在 `pending/store.ts` 的镜像里） | 每次写/删本机记录 | 已发出的写，退出会放弃 |
 * | `submitting` | 文档页的 `submit()` | 正在发请求，版本号可能还没回来 |
 * | `queuedFiles` | 文档页的队列 | 选了文件、还没提交：**退出即丢，且无处可查** |
 *
 * 第三行是这一层唯一"没有被记录兜住"的部分：解析出来的文本只活在 React state 里
 * （`ingest/queue.ts`：队列状态不持久化，能重建即可）。所以它也必须参与拦截，
 * 否则用户会以为传过了。
 */
import { cachedUnsettled, type PendingWrite } from '../pending/store'

export interface WorkSnapshot {
  /** 未到终局的在途批次（本机记录镜像）。 */
  unsettled: readonly PendingWrite[]
  /** 正在提交（`CreateVersion` 在飞）。 */
  submitting: boolean
  /** 已选中但还没提交的文件数（含解析中/待提交）。 */
  queuedFiles: number
}

let submitting = false
let queuedFiles = 0

/** 文档页在提交期间置真。 */
export function setSubmitting(active: boolean): void {
  submitting = active
}

/** 文档页把"还没提交的文件数"同步过来。 */
export function setQueuedFiles(n: number): void {
  queuedFiles = Number.isFinite(n) && n > 0 ? Math.floor(n) : 0
}

export function workSnapshot(): WorkSnapshot {
  return { unsettled: cachedUnsettled(), submitting, queuedFiles }
}

/** 没有任何东西悬着——退出是安全的，不该弹任何提示。 */
export function hasWork(s: WorkSnapshot): boolean {
  return s.unsettled.length > 0 || s.submitting || s.queuedFiles > 0
}

/**
 * 退出提示的一句话摘要。
 *
 * 分三句写，是因为三件事的后果不同：在途批次会被**放弃**（版本不留），
 * 正在提交的那批可能连版本号都还没拿到，而未提交的文件没有任何记录能找回。
 */
export function describeWork(s: WorkSnapshot): string[] {
  const lines: string[] = []
  if (s.unsettled.length > 0) {
    const ids = s.unsettled
      .map((w) => (w.version_id === '' ? '未分配版本' : `v${w.version_id}`))
      .join('、')
    lines.push(`${s.unsettled.length} 批已提交但还没落地的数据（${ids}）：退出将放弃这些版本。`)
  }
  if (s.submitting) {
    lines.push('有一批正在提交：它的版本号可能还没回来，退出不一定能放弃干净。')
  }
  if (s.queuedFiles > 0) {
    lines.push(`还有 ${s.queuedFiles} 个文件没提交：它们只在本页内存里，退出即丢。`)
  }
  return lines
}
