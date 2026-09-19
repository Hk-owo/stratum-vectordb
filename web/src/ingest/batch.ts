/**
 * 分批 + 串行提交 + 等就绪。
 *
 * 三条来自接入指南的硬约束决定了这里的形状：
 *
 * 1. **版本链严格线性**（§2.1）：一个父版本最多一个子版本，并发提交同一 KB
 *    时第二个会被拒（`invalid_parent_version`）。所以没有并发提交——一批批排队，
 *    每批的 `parent_version_id` 是上一批返回的版本号。
 * 2. **`UPDATE` 一个不存在的 doc_id 等价于 ADD**（§2.2）。所以前端**只发 UPDATE**，
 *    不必区分"这个文件是新加的还是要改的"——服务端按集合语义处理。
 * 3. **`client_request_id` 是唯一能救回"数据没落地"版本的途径**（§7.12）。
 *    所以它必须先落 IndexedDB 再发请求，顺序反了就等于没记。
 */
import { ChangeOp } from '../api/gen/knowledgebase'
import type { CreateVersionResponse } from '../api/gen/knowledgebase'
import { api, kbPath } from '../api/client'
import type { AwaitVersionResponse } from '../api/gen/knowledgebase'
import { awaitUntilTerminal } from '../pending/awaitUntilTerminal'
import type { AwaitTargetKind, Decision } from '../pending/decide'
import {
  newRequestId,
  removePending,
  upsertPending,
  type Change,
  type PendingWrite,
} from '../pending/store'
import type { QueueItem } from './queue'

/**
 * 一批的字节上限。文档数不足以当阈值——真正的成本是 payload：这批 changes 要
 * 过网关、进 Raft 之外的写路径、在节点上切分与 embed（§3.3 讲的就是批次大小
 * 与节奏）。1 MiB 是个保守起点，可按部署调的。
 */
export const MAX_BATCH_BYTES = 1 << 20

/** RESEND 的上限：服务端说"按同一 key 重发"时我们最多再试几次。 */
export const MAX_RESEND_ATTEMPTS = 3

export interface Batch {
  items: QueueItem[]
  bytes: number
}

/**
 * 按字节分批。**单个文档不拆**——changes 以 doc_id 为单位，且 UPDATE 要整篇全文，
 * 所以一个超限的大文档只能自成一其（超限也照发，服务端才是真正的判据）。
 */
export function planBatches(
  items: readonly QueueItem[],
  maxBytes: number = MAX_BATCH_BYTES,
): Batch[] {
  const out: Batch[] = []
  let cur: QueueItem[] = []
  let curBytes = 0

  for (const it of items) {
    // 用 TextEncoder 而不是 content.length：后者数的是 UTF-16 码元，中文文档会
    // 被低估近一半。
    const bytes = new TextEncoder().encode(it.content).length

    if (cur.length > 0 && curBytes + bytes > maxBytes) {
      out.push({ items: cur, bytes: curBytes })
      cur = []
      curBytes = 0
    }
    cur.push(it)
    curBytes += bytes
  }
  if (cur.length > 0) out.push({ items: cur, bytes: curBytes })
  return out
}

export interface SubmitOptions {
  kbId: string
  /** 当前链尾。省略即 null 表示"这个库还没有版本"，用 0 作为新链的根。 */
  parentVersionId: string | null
  items: readonly QueueItem[]
  /**
   * 每批等到哪一步。
   *
   *   · `INDEX_READY`（默认）—— 可查询，**也是激活版本的前置条件**：传完就能搜到。
   *   · `DATA_DURABLE` —— 只要多数派确认落盘。批量导入快得多，但那期间版本
   *     不可查、也不能激活（索引在后台继续建）。
   */
  target?: AwaitTargetKind
  /** 每个文档的状态变化，用于更新队列 UI。 */
  onItem?: (docId: string, patch: Partial<QueueItem>) => void
  /** 等就绪时的进度回调。 */
  onAwait?: (batch: readonly QueueItem[], resp: AwaitVersionResponse) => void
  signal?: AbortSignal
}

export interface SubmitSummary {
  /** 每个落地批次分配到的版本号，按提交顺序。 */
  versions: string[]
  /** 没能走到 READY 的文档，附原因。 */
  failed: Array<{ docId: string; message: string }>
  /** 有内容落地的最后一个版本号，用作下一批的 parent。 */
  lastVersionId: string | null
}

/** 一批的最终结局。 */
interface BatchOutcome {
  versionId: string
  decision: Decision
}

/**
 * 提交一批 changes，在服务端说"重发"时用同一个 key 再来。
 *
 * 重发不是重试：`client_request_id` 相同会让服务端**复用**首次分配的版本，而不是
 * 另分配一个（§7.12）。这正是"数据没落地的版本"能被救回的原因，也是为什么
 * parent_version_id 在重发时不能变——链尾根本没动过。
 */
async function submitBatchWithRecovery(
  kbId: string,
  parentVersionId: string | null,
  changes: Change[],
  opts: SubmitOptions,
  items: readonly QueueItem[],
): Promise<BatchOutcome> {
  let clientRequestId = newRequestId()

  for (let attempt = 1; ; attempt += 1) {
    // 1. 先记，再发。反过来的话，请求发出去而记录没落盘，重启后就再也不知道
    //    这批数据的存在——而集群里也没有任何副本替我们记着（§8）。
    const record: PendingWrite = {
      id: clientRequestId,
      knowledge_base_id: kbId,
      version_id: '',
      client_request_id: clientRequestId,
      changes,
      submitted_at: new Date().toISOString(),
      settled: false,
    }
    await upsertPending(record)

    // 2. 提交。parent_version_id 是 int64 → protojson 要字符串；0 表示新链的根。
    const resp = await api.post<CreateVersionResponse>(kbPath(kbId, '/versions'), {
      knowledge_base_id: kbId,
      parent_version_id: parentVersionId ?? '0',
      changes,
      client_request_id: clientRequestId,
    })

    // 3. 把服务端回传的版本号与幂等键写回记录。
    //    版本号是"继续等"的入口（WAIT 之后要靠它再问一次），幂等键则是重发的凭证：
    //    若调用方没自带 key，服务端会生成一个并随响应回传（§7.12 Step 4），
    //    不写回的话这条记录就少了一半信息。
    clientRequestId = resp.client_request_id
    await upsertPending({
      ...record,
      id: clientRequestId,
      client_request_id: resp.client_request_id,
      version_id: resp.version_id,
    })

    for (const it of items) {
      opts.onItem?.(it.docId, { status: 'awaiting', versionId: resp.version_id })
    }

    // 4. 等就绪。判定全在 pending/decide.ts，这里只按它的结论行动。
    //    haveChanges 恒为 true：changes 就在我们手里（刚写进 IndexedDB 了）。
    const outcome = await awaitUntilTerminal(kbId, resp.version_id, {
      haveChanges: true,
      target: opts.target,
      signal: opts.signal,
      onProgress: (r) => opts.onAwait?.(items, r),
    })

    if (outcome.decision === 'DONE' || outcome.decision === 'DISCARD') {
      // 终局：这条记录已经没有用途了。DONE 是救回来了，DISCARD 是救不回来了
      // （没有副本持有数据，而 changes 也已不在别处）——两种都不该继续挂在
      // 「在途」面板上，否则它会永久显示一批早已有结论的批次。
      await removePending(clientRequestId)
      return { versionId: resp.version_id, decision: outcome.decision }
    }

    if (outcome.decision === 'WAIT') {
      // 等超时了但没到终局（通常是索引还在建，或 FAILED 在等运维 rebuild）。
      // **记录要留着**：版本还活着，之后可以继续等。这是 InFlightPanel 存在的意义。
      return { versionId: resp.version_id, decision: outcome.decision }
    }

    // RESEND：同一个 key、同一批 changes、同一个 parent。
    if (attempt >= MAX_RESEND_ATTEMPTS) {
      throw new Error(
        `重发 ${MAX_RESEND_ATTEMPTS} 次后仍未落地（stage=${outcome.last.stage}，` +
          `data_missing=${String(outcome.last.data_missing)}）`,
      )
    }
  }
}

export async function submitBatches(opts: SubmitOptions): Promise<SubmitSummary> {
  const batches = planBatches(opts.items)
  const versions: string[] = []
  const failed: Array<{ docId: string; message: string }> = []
  let parent = opts.parentVersionId

  for (const batch of batches) {
    opts.signal?.throwIfAborted()

    for (const it of batch.items) opts.onItem?.(it.docId, { status: 'submitting' })

    // 全部用 UPDATE：服务端对不存在的 doc_id 就是 ADD（§2.2），不必区分。
    const changes: Change[] = batch.items.map((it) => ({
      op: ChangeOp.CHANGE_OP_UPDATE,
      doc_id: it.docId,
      content: it.content,
    }))

    let outcome: BatchOutcome
    try {
      outcome = await submitBatchWithRecovery(opts.kbId, parent, changes, opts, batch.items)
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      for (const it of batch.items) {
        opts.onItem?.(it.docId, { status: 'error', message })
        failed.push({ docId: it.docId, message })
      }
      // 这一批没落地，链尾没动，后面的批次可以继续用同一个 parent 试。
      continue
    }

    switch (outcome.decision) {
      case 'DONE':
        versions.push(outcome.versionId)
        parent = outcome.versionId
        for (const it of batch.items) {
          opts.onItem?.(it.docId, { status: 'done', versionId: outcome.versionId })
        }
        break

      case 'DISCARD':
        // 版本永远不会落地，而 changes 也救不回来了。
        for (const it of batch.items) {
          opts.onItem?.(it.docId, {
            status: 'error',
            versionId: outcome.versionId,
            message: '版本被判死且 changes 无法找回（DISCARD）',
          })
          failed.push({ docId: it.docId, message: 'DISCARD' })
        }
        break

      case 'WAIT':
        // 等超时了但没到终局——通常是索引还在建、或者 FAILED 在等运维 rebuild。
        // 版本存在，链尾**不能**推进到它上面：它还不是 READY。
        for (const it of batch.items) {
          opts.onItem?.(it.docId, {
            status: 'awaiting',
            versionId: outcome.versionId,
            message: '等待超时，稍后可从在途批次面板继续等',
          })
        }
        failed.push({ docId: batch.items[0]?.docId ?? '', message: '等待超时（WAIT）' })
        break
    }
  }

  return { versions, failed, lastVersionId: parent }
}
