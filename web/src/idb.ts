/**
 * 控制台在浏览器端的 IndexedDB 连接 —— 桌面壳下用不到（那边走文件）。
 *
 * 抽出来是因为两个 store 共用同一个库：`pending-writes`（在途批次）和
 * `history`（操作历史）。各开各的连接会让 onupgradeneeded 互相踩——后开的那次
 * 升级只会带上自己声明的 store，先前那个就此消失。
 *
 * `IDB_VERSION` 每加一个 object store 就要 +1。升级回调里按需判断缺哪个建哪个，
 * 而不是按版本号分支：这样中间版本的升级路径不会漏。
 */

const DB_NAME = 'stratum-console'
const DB_VERSION = 2

export const PENDING_STORE = 'pending-writes'
export const HISTORY_STORE = 'history'

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
      if (!db.objectStoreNames.contains(PENDING_STORE)) {
        db.createObjectStore(PENDING_STORE, { keyPath: 'id' })
      }
      if (!db.objectStoreNames.contains(HISTORY_STORE)) {
        // 历史按自增序号存，读出来再排序 —— 追加是它唯一的写模式，键本身没有
        // 业务含义，用自增键就不必为"同一毫秒两条记录"发明去重规则。
        db.createObjectStore(HISTORY_STORE, { autoIncrement: true })
      }
    }
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error ?? new Error('indexedDB.open failed'))
  })
}

/** 在一个 object store 上跑一次请求。 */
export function run<T>(
  store: string,
  mode: IDBTransactionMode,
  fn: (s: IDBObjectStore) => IDBRequest<T>,
): Promise<T> {
  return openDB().then(
    (db) =>
      new Promise<T>((resolve, reject) => {
        const tx = db.transaction(store, mode)
        const req = fn(tx.objectStore(store))
        req.onsuccess = () => resolve(req.result)
        req.onerror = () => reject(req.error ?? new Error('indexedDB request failed'))
        tx.oncomplete = () => db.close()
        tx.onabort = () => reject(tx.error ?? new Error('indexedDB transaction aborted'))
      }),
  )
}
