import { DataStatus, type VersionInfo } from '../api/gen/knowledgebase'

/**
 * 链尾：可以挂一个新版本的父版本。
 *
 * 两条约束决定它是什么（docs/client-integration-guide.md §2.1）：
 *   · **父版本的数据必须已落盘**——新版本的文档集是"父版本的集合 + 本批 changes"，
 *     所以唯一真实的依赖是父版本的数据在不在（`internal/raft/state_machine.go` 的
 *     applyCreateVersion 读的也是数据侧）。
 *   · 一个父版本最多一个子版本，所以链尾就是"没有子版本"的那个；实际部署里链是
 *     线性的（回滚只改 active_version_id，不产生新版本），取 version_id 最大的那个
 *     即可。
 *
 * **索引状态不参与判断**，这不是省略：索引由查询驱动、按 owner 集动态放置——同一个
 * 版本在 A 节点有索引、在 B 节点没有，谁的答案都不比"数据在"更权威。客户端曾经用
 * `index_status == READY` 挑链尾，于是每批提交后索引都没建（没人查），链尾恒为空，
 * 下一批就挂到了版本 0 上，凭空长出一个新的根。
 */
export type ChainTail =
  /** 这个库还没有版本，新版本以 0 为根。 */
  | { kind: 'none' }
  /** 链尾可作父。 */
  | { kind: 'ok'; versionId: string }
  /**
   * 链尾存在但现在不能作父。调用方应当等待/提示，**不能**退回更早的版本：那个
   * 版本已经有子版本了，服务端会以 `invalid_parent_version` 拒绝。
   */
  | { kind: 'not-ready'; versionId: string; reason: 'data-pending' | 'deleting' }

export function chainTail(versions: readonly VersionInfo[]): ChainTail {
  if (versions.length === 0) return { kind: 'none' }

  const tail = versions.reduce((best, v) =>
    // int64 在 protojson 里是字符串，比较前必须转成数字——"9" > "10" 在字符串
    // 比较下是 true，会让链尾停在错误的版本上。
    Number(v.version_id) > Number(best.version_id) ? v : best,
  )

  if (tail.deleting) {
    return { kind: 'not-ready', versionId: tail.version_id, reason: 'deleting' }
  }
  if (tail.data_status !== DataStatus.DATA_STATUS_DURABLE) {
    return { kind: 'not-ready', versionId: tail.version_id, reason: 'data-pending' }
  }
  return { kind: 'ok', versionId: tail.version_id }
}

/** 「现在不能提交」时给界面的一句话。 */
export function describeNotReady(tail: Extract<ChainTail, { kind: 'not-ready' }>): string {
  return tail.reason === 'deleting'
    ? `链尾 v${tail.versionId} 正在清理，等它结束再提交。`
    : `上一批（v${tail.versionId}）的数据还没落盘，现在提交会因父版本未就绪被拒；稍后重试即可。`
}
