/**
 * 退出时放弃在途版本 —— `DiscardVersion` 的退出路径。
 *
 * 语义是用户定的：**关掉客户端就等于放弃那批还没落地的写**。版本链上留一个
 * 永远不 READY 的 PENDING 版本比丢一次提交更糟——它会让这个知识库后面的每一次
 * 提交都撞上 `invalid_parent_version: parent version N is PENDING`
 * （internal/raft/state_machine.go 的 applyCreateVersion）。所以退出不是"什么都
 * 不做"，而是主动收尾。
 *
 * 四条判据，对应四件不同的事：
 *
 * | 服务端/本机的事实 | 处理 | 为什么 |
 * |---|---|---|
 * | 版本号还是空串 | 清记录 | 提交请求还没返回。用户的原话："版本号没存在那不就是没有吗" |
 * | `discarded` true/false | 清记录 | 放弃成功，或版本本来就不在了（幂等） |
 * | `version_not_pending` | 清记录 | 数据已经落盘了，那就**不属于**"没落地"。放弃做不到，但本机记录也没用了：重发只会复用同一个已落盘版本 |
 * | 没答复（超时/断网） | **保留记录** | 唯一一个不能当成功的情况：不知道服务端收没收到。记录是重发凭证，删了就真没了 |
 *
 * "数据已经落盘"不是坏事，只是它需要的是 `WarmupVersion`（把索引建出来），
 * 而不是重发。报告里会把这条说清楚，让用户知道去哪儿收尾。
 */
import { ApiError, api, kbPath, type RequestOptions } from '../api/client'
import type { DiscardVersionResponse } from '../api/gen/knowledgebase'
import { appendHistory } from '../history/store'
import { removePending, type PendingWrite } from './store'

/** 一条记录的结局。 */
export type DiscardKind =
  /** 服务端确认放弃（或版本本来就不在了）。 */
  | 'discarded'
  /** 版本的数据已经落盘，`DiscardVersion` 按契约拒绝——这不是失败，是"它其实成功了"。 */
  | 'already-settled'
  /** 本机还没拿到版本号，没有可放弃的对象。 */
  | 'unknown-version'
  /** 没等到答复：服务端可能收到也可能没收到，记录必须留着。 */
  | 'unreachable'

export interface DiscardOutcome {
  kind: DiscardKind
  /** 被处理的那条本机记录。 */
  record: PendingWrite
  versionId: string
  /** 给界面直接显示的一句话。 */
  note: string
}

export interface DiscardReport {
  outcomes: DiscardOutcome[]
  /** 本机记录已清掉的批次。 */
  cleared: PendingWrite[]
  /** 记录仍留在本机、下次启动还会出现在「在途」面板里的批次。 */
  kept: PendingWrite[]
  /**
   * 整个流程自己出了错（不该发生）。
   *
   * 这个字段存在的理由只有一个：退出路径**不允许把用户困在窗口里**。放弃流程
   * 真抛了的话，界面必须有办法走到"退出"那一步，而这时它必须能解释自己看到
   * 的是一份空报告——"看起来什么都没做"和"做了一半"是两回事。
   */
  error?: string
}

/**
 * 一次退出最多花多久在放弃上。
 *
 * 退出是等不起的操作：用户已经按了"退出"，这里多花的每一秒都是窗口不响应。
 * 所以给**总预算**（默认 5 s）而不是给每条一个预算——十批各等 2.5 s 就是 25 s，
 * 那不是退出。预算耗尽后尚未尝试的记录一律按"没等到答复"处理：保留，交给下次
 * 启动的「在途」面板。
 */
export const DEFAULT_TOTAL_BUDGET_MS = 5000

/** 单条的上限。网关在回环上，2.5 s 已经远超一次正常往返。 */
export const DEFAULT_PER_REQUEST_MS = 2500

export interface DiscardDeps {
  /** 发一次 `DiscardVersion`。 */
  discard: (
    kbId: string,
    versionId: string,
    opts?: RequestOptions,
  ) => Promise<DiscardVersionResponse>
  /** 删掉一条本机记录。 */
  remove: (id: string) => Promise<void>
  /** 记一条历史，让"退出时放弃了什么"事后可查（提示框一闪而过）。 */
  history: (entry: { kind: 'discard'; knowledge_base_id: string; detail: string }) => void | Promise<void>
  /** 测试用的时钟。 */
  now: () => number
}

export const defaultDiscardDeps: DiscardDeps = {
  discard: (kbId, versionId, opts) =>
    api.post<DiscardVersionResponse>(
      kbPath(kbId, '/discard-version'),
      { knowledge_base_id: kbId, version_id: versionId },
      opts,
    ),
  remove: removePending,
  // 历史是审计辅助，不是这一步的目的：不 await，窗口即将销毁，写不进去也不该
  // 拦住退出。
  history: (entry) => {
    void appendHistory(entry)
  },
  now: () => Date.now(),
}

export interface DiscardOptions {
  totalBudgetMs?: number
  perRequestMs?: number
  deps?: Partial<DiscardDeps>
}

/**
 * 逐条放弃 `records`（调用方传 `cachedUnsettled()`）。
 *
 * 串行：批次本来就少，而且并发退出时的请求只会把网关的写入路径挤在一起。
 */
export async function discardForExit(
  records: readonly PendingWrite[],
  opts: DiscardOptions = {},
): Promise<DiscardReport> {
  const deps: DiscardDeps = { ...defaultDiscardDeps, ...opts.deps }
  const totalBudgetMs = opts.totalBudgetMs ?? DEFAULT_TOTAL_BUDGET_MS
  const perRequestMs = opts.perRequestMs ?? DEFAULT_PER_REQUEST_MS
  const started = deps.now()

  const report: DiscardReport = { outcomes: [], cleared: [], kept: [] }

  for (const record of records) {
    // 单条出什么错都不该让整个退出流程抛出去：这个函数跑在"用户已经在关窗口"
    // 的路径上，抛出去的结果是窗口关不掉。所以它自己兜住，逐条降级。
    let outcome: DiscardOutcome
    try {
      outcome = await discardOne(record, {
        deps,
        perRequestMs,
        remainingMs: totalBudgetMs - (deps.now() - started),
      })
    } catch (err) {
      outcome = {
        kind: 'unreachable',
        record,
        versionId: record.version_id,
        note: `处理这一批时出错（${err instanceof Error ? err.message : String(err)}），记录已保留`,
      }
    }
    report.outcomes.push(outcome)
    if (outcome.kind === 'unreachable') report.kept.push(record)
    else report.cleared.push(record)
  }

  return report
}

async function discardOne(
  record: PendingWrite,
  ctx: { deps: DiscardDeps; perRequestMs: number; remainingMs: number },
): Promise<DiscardOutcome> {
  const versionId = record.version_id
  const base = { record, versionId }

  // 版本号都没有：提交请求还没返回。用户裁定按"没有"处理——释放本机记录，
  // 不让它下次启动又冒出来要求人工处置。
  if (versionId === '') {
    await ctx.deps.remove(record.id)
    return {
      ...base,
      kind: 'unknown-version',
      note: '提交请求还没返回版本号，按"未创建"处理，记录已清除',
    }
  }

  // 总预算耗尽：没尝试就是没答复，不能当成功。
  if (ctx.remainingMs <= 0) {
    return {
      ...base,
      kind: 'unreachable',
      note: '退出前没来得及确认放弃，记录已保留（下次启动可在「在途」面板处理）',
    }
  }

  const budget = Math.min(ctx.perRequestMs, ctx.remainingMs)
  try {
    await ctx.deps.discard(record.knowledge_base_id, versionId, {
      signal: AbortSignal.timeout(budget),
    })
    await ctx.deps.remove(record.id)
    ctx.deps.history({
      kind: 'discard',
      knowledge_base_id: record.knowledge_base_id,
      detail: `退出时放弃 v${versionId}`,
    })
    return { ...base, kind: 'discarded', note: `已放弃 v${versionId}` }
  } catch (err) {
    if (err instanceof ApiError && err.isFailedPrecondition) {
      // version_not_pending：数据已经落盘。按用户的判据，这不再属于"没落地"，
      // 所以它不该继续挂在「在途」里——但要说清楚它现在需要什么。
      await ctx.deps.remove(record.id)
      ctx.deps.history({
        kind: 'discard',
        knowledge_base_id: record.knowledge_base_id,
        detail: `退出时未能放弃 v${versionId}（数据已落盘，无索引）`,
      })
      return {
        ...base,
        kind: 'already-settled',
        note: `v${versionId} 的数据已落盘，不能按"未落地"放弃；它还没有索引，需要在「版本」页预热后才能用`,
      }
    }
    if (err instanceof ApiError && err.isNotFound) {
      // version_not_found：版本已经不在了（别人放弃过、或本来就没建成）。幂等。
      await ctx.deps.remove(record.id)
      return { ...base, kind: 'discarded', note: `v${versionId} 已不存在，记录已清除` }
    }
    return {
      ...base,
      kind: 'unreachable',
      note: `放弃 v${versionId} 没等到答复（${err instanceof Error ? err.message : String(err)}），记录已保留`,
    }
  }
}
