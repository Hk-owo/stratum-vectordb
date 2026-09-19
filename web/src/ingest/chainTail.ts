import { IndexStatus, type VersionInfo } from '../api/gen/knowledgebase'

/**
 * 链尾：可以挂一个新版本的父版本。
 *
 * §2.1 的两条约束决定了它不是"最新那个版本"：
 *   · `parent_version_id` **必须**是已 READY 的版本——挂在一个还在构建、
 *     或者已经 FAILED 的版本上会被拒。
 *   · 一个父版本最多一个子版本，所以链尾就是"没有子版本"的那个。
 *
 * 实际部署里链是线性的（回滚只改 active_version_id，不产生新版本），所以取
 * **version_id 最大的 READY 版本**就是链尾。返回 null 表示这个库还没有可用版本，
 * 新版本要以 0 为根。
 */
export function chainTail(versions: readonly VersionInfo[]): string | null {
  const ready = versions.filter((v) => v.index_status === IndexStatus.INDEX_STATUS_READY)
  if (ready.length === 0) return null

  return ready.reduce((best, v) =>
    // int64 在 protojson 里是字符串，比较前必须转成数字——"9" > "10" 在字符串
    // 比较下是 true，会让链尾停在错误的版本上。
    Number(v.version_id) > Number(best.version_id) ? v : best,
  ).version_id
}
