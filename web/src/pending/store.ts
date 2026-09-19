/**
 * 在途批次的本机记录 —— `client/pending.go` 里 `Store` 的等价物。
 *
 * 为什么必须有它（docs/client-integration-guide.md §7、§8）：**集群不替任何人
 * 保存 changes**。本进程之外的唯一副本要等协调者写下 WAL BEGIN 才出现，所以
 * 在那之前的每一次提交，世界上只有调用方手里有这份数据。这份记录就是"重发
 * 还是放弃"从一句口号变成程序能执行的选择的全部依据。
 *
 * 两个后端，按环境选：
 *
 * | 环境 | 后端 | 可靠性 |
 * |---|---|---|
 * | 桌面（Tauri） | `app_data_dir` 里的 JSON 文件，temp + rename 原子写 | **用户能看见、能备份**，不随浏览器策略消失 |
 * | 浏览器 | IndexedDB | 清缓存 / 隐私模式 / 换设备就没了 |
 *
 * 浏览器后端保留着，是因为开发时 `npm run dev` 仍然要在浏览器里跑。但**关键数据
 * 该落在桌面那一侧**——把"唯一副本"放在会被浏览器静默清掉的地方，本身就是这个
 * 桌面壳存在的理由。
 */
import type { ChangeOp } from '../api/gen/knowledgebase'
import { desktop, isDesktop, type DesktopChange, type DesktopPendingWrite } from '../desktop/tauri'

/** 一条文档变更，与线上 DocChange 同形。 */
export interface Change {
  op: ChangeOp
  doc_id: string
  /** ADD / UPDATE 必填：整篇全文（服务端没有"只传片段"的路径）。 */
  content?: string
}

export interface PendingWrite {
  /** 本地主键，同时用作 `client_request_id`。 */
  id: string
  knowledge_base_id: string
  /** 分配到的版本号。protojson 的 int64 是字符串，这里照存。 */
  version_id: string
  client_request_id: string
  changes: Change[]
  /** ISO 8601。 */
  submitted_at: string
  /** 已经到达终局（DONE/DISCARD）的批次不再需要重发。 */
  settled: boolean
}

// ---------------------------------------------------------------- 桌面后端

function toDesktop(w: PendingWrite): DesktopPendingWrite {
  return {
    ...w,
    // ChangeOp 是字符串枚举，落到 JSON 就是普通字符串；Rust 侧按 String 收。
    changes: w.changes.map((c) => ({ ...c }) as DesktopChange),
  }
}

function fromDesktop(w: DesktopPendingWrite): PendingWrite {
  return { ...w, changes: w.changes as unknown as Change[] }
}

// ---------------------------------------------------------------- 浏览器后端

const DB_NAME = 'stratum-console'
const DB_VERSION = 1
const STORE = 'pending-writes'

function openDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    let req: IDBOpenDBRequest
    try {
      req = indexedDB.open(DB_NAME, DB_VERSION)
    } catch (cause) {
      // 隐私模式下 indexedDB 可能直接抛。这不是"没有记录"，而是"无法记录"，
      // 两者对调用方的意义完全不同，所以原样抛出去。
      reject(cause instanceof Error ? cause : new Error(String(cause)))
      return
    }
    req.onupgradeneeded = () => {
      const db = req.result
      if (!db.objectStoreNames.contains(STORE)) {
        db.createObjectStore(STORE, { keyPath: 'id' })
      }
    }
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error ?? new Error('indexedDB.open failed'))
  })
}

function run<T>(
  mode: IDBTransactionMode,
  fn: (store: IDBObjectStore) => IDBRequest<T>,
): Promise<T> {
  return openDB().then(
    (db) =>
      new Promise<T>((resolve, reject) => {
        const tx = db.transaction(STORE, mode)
        const req = fn(tx.objectStore(STORE))
        req.onsuccess = () => resolve(req.result)
        req.onerror = () => reject(req.error ?? new Error('indexedDB request failed'))
        tx.oncomplete = () => db.close()
      }),
  )
}

// ---------------------------------------------------------------- 公开 API

/** 全部在途批次，按提交时间升序（老的在前，重连时先补它们）。 */
export async function loadPending(): Promise<PendingWrite[]> {
  if (isDesktop()) {
    const all = (await desktop.loadPending()).map(fromDesktop)
    return all.sort((a, b) => a.submitted_at.localeCompare(b.submitted_at))
  }
  const all = await run<PendingWrite[]>('readonly', (s) => s.getAll() as IDBRequest<PendingWrite[]>)
  return all.sort((a, b) => a.submitted_at.localeCompare(b.submitted_at))
}

/** 只取还没到终局的——刷新后要接着等/重发的就是这批。 */
export async function loadUnsettled(): Promise<PendingWrite[]> {
  return (await loadPending()).filter((w) => !w.settled)
}

export async function getPending(id: string): Promise<PendingWrite | undefined> {
  if (isDesktop()) {
    return (await loadPending()).find((w) => w.id === id)
  }
  return run<PendingWrite | undefined>('readonly', (s) =>
    s.get(id) as IDBRequest<PendingWrite | undefined>,
  )
}

/**
 * 写入或覆盖。必须在**发出 CreateVersion 之前**调用——先记再发，顺序反了就等于没记。
 *
 * 桌面后端是"读全部 + 原子重写整份文件"：在途批次本来就只有几条（用户传一批、
 * 等它落地），而整份重写换来的原子性是这里最要紧的东西。
 */
export async function upsertPending(w: PendingWrite): Promise<void> {
  if (isDesktop()) {
    await desktop.upsertPending(toDesktop(w))
    return
  }
  await run<IDBValidKey>('readwrite', (s) => s.put(w))
}

export async function removePending(id: string): Promise<void> {
  if (isDesktop()) {
    await desktop.removePending(id)
    return
  }
  await run<undefined>('readwrite', (s) => s.delete(id) as IDBRequest<undefined>)
}

/**
 * 本机记录存在哪（只有桌面端有）。界面上显示它，用户才知道该备份什么。
 */
export async function storeLocation(): Promise<string | null> {
  if (!isDesktop()) return null
  return desktop.storeDir()
}

/**
 * 幂等键。时间在前，便于人在日志里排序；随机后缀，避免同纳秒的两个标签页撞键。
 *
 * 撞键不是小事：同一个 key 会**复用**首次分配的版本，两个不同的批次撞上就会
 * 互相顶掉对方的版本。Go 版在 crypto/rand 失败时直接 panic 也是同一个理由——
 * 不拿一个更弱的键把事情糊过去。
 */
export function newRequestId(): string {
  const suffix = new Uint8Array(4)
  crypto.getRandomValues(suffix)
  const hex = Array.from(suffix, (b) => b.toString(16).padStart(2, '0')).join('')
  return `web-${Date.now()}-${hex}`
}
