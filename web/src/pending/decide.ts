/**
 * 「等就绪」的判定：把一次 AwaitVersion 的回答加上"我手里还有没有 changes"，
 * 翻译成调用方的下一个动作。
 *
 * 这是 client/pending.go 里 `Decide` 的浏览器版——那份 Go 实现的注释把理由
 * 说得比这里更完整，核心是：**服务端只报告事实，选择由调用方做**。
 * 集群能证明"没有任何可达副本持有这份数据"（data_missing），但"这份 changes
 * 是否还存在于世上"只有调用方知道。所以服务端给 fact，调用方给决定
 * （docs/client-integration-guide.md §7、docs/await-version-plan.md §4.3）。
 */
import type { AwaitVersionResponse } from '../api/gen/knowledgebase'

export type Decision =
  /** 按服务端给的建议间隔再问一次。 */
  | 'WAIT'
  /** 写入已落地。 */
  | 'DONE'
  /** 用**同一个**幂等键重发同一批 changes。 */
  | 'RESEND'
  /** 这个版本永远不会落地，而 changes 也找不回来了。 */
  | 'DISCARD'

/**
 * AwaitVersion 会报的 stage。与服务端 service.Stage* 逐字对应——Go 侧那份用
 * 测试钉住了这个对应关系，这里同样不引用服务端代码（前端也引不到），
 * 所以改动时两边都要看。
 */
export const STAGES = {
  dataPending: 'DATA_PENDING',
  dataDurable: 'DATA_DURABLE',
  indexReady: 'INDEX_READY',
  indexFailed: 'INDEX_FAILED',
  dataFailedPermanent: 'DATA_FAILED_PERMANENT',
  indexFailedPermanent: 'INDEX_FAILED_PERMANENT',
  deleting: 'DELETING',
} as const

/**
 * 「等到哪一步算数」。
 *
 * 必须与 `AwaitVersion` 请求里送的 target 保持一致，否则判定会和服务端的判据
 * 打架：服务端在 DATA_DURABLE 就返回了，而这里若仍按"要 READY"去判，就会把一次
 * 已经达成的等待读成"继续等"，一路空转到超时。
 *
 * 两种目标的取舍：
 *   · INDEX_READY —— 可查询，**也是激活版本的前置条件**。传完就能搜到。
 *   · DATA_DURABLE —— 只要多数派确认落盘。批量导入快得多，但那期间版本不可查、
 *     也不能激活（索引还在后台建）。
 */
export type AwaitTargetKind = 'INDEX_READY' | 'DATA_DURABLE'

/** 对应服务端 service/await_version.go 的 isTargetReached —— 逐字一致。 */
function isTargetReached(stage: string, target: AwaitTargetKind): boolean {
  switch (target) {
    case 'DATA_DURABLE':
      return stage === STAGES.dataDurable || stage === STAGES.indexReady
    case 'INDEX_READY':
      return stage === STAGES.indexReady
  }
}

/**
 * haveChanges = 本地是否还留着这批 changes（IndexedDB 里那条记录）。
 * 它决定 RESEND 还是 DISCARD——两者都是终局，但一个能救回数据，一个不能。
 *
 * target 默认 INDEX_READY：它是唯一能保证"传完就能搜到"的目标。
 */
export function decide(
  resp: AwaitVersionResponse,
  haveChanges: boolean,
  target: AwaitTargetKind = 'INDEX_READY',
): Decision {
  // 目标达成检查放最前：DATA_DURABLE 目标下，服务端到达 DURABLE 就算完成，
  // 不该再按"还差一个 READY"继续等下去。
  if (isTargetReached(resp.stage, target)) {
    return 'DONE'
  }

  switch (resp.stage) {
    case STAGES.dataFailedPermanent:
    case STAGES.indexFailedPermanent:
      // 任意一侧的终局判定。版本已被判死，同一个 key 只会把那个死版本号再次
      // 交回来，所以"重来"意味着换一个**新** key——那是调用方的事，不是这里的事。
      return haveChanges ? 'RESEND' : 'DISCARD'

    case STAGES.indexFailed:
      // 可重建，且不是调用方的数据问题：等待就是方案规定的流程
      // （运维 RebuildIndex，然后再问）。
      return 'WAIT'

    case STAGES.deleting:
      return 'DISCARD'

    case STAGES.dataPending:
      // "还在写"与"永远不会写"是同一个 stage，只有存在性探针能把它们分开。
      if (resp.data_missing) {
        return haveChanges ? 'RESEND' : 'DISCARD'
      }
      return 'WAIT'

    case STAGES.dataDurable:
      // 只有 target=INDEX_READY 时才走得到这里（DATA_DURABLE 目标已在上面返回
      // DONE）。数据已持久但还不可查，而调用方要的是可查询，所以继续等。
      return 'WAIT'

    default:
      // 未知 stage = 更新版服务端的回答。等待是唯一安全的读法：它什么都不会毁掉。
      return 'WAIT'
  }
}

/** 判定是否已经到了终局（不必再轮询）。 */
export function isTerminal(d: Decision): boolean {
  return d !== 'WAIT'
}
