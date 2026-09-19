import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'

import type { AwaitVersionResponse } from '../api/gen/knowledgebase'
import { decide, STAGES, type Decision } from './decide'

/** 只填判定用得到的两个字段，其余给安全的零值。 */
function resp(stage: string, dataMissing = false): AwaitVersionResponse {
  return {
    version: undefined,
    stage,
    retry_after_ms: '0',
    data_missing: dataMissing,
  }
}

describe('decide', () => {
  // 与 client/pending_test.go 的 TestDecide_EveryStageAndBothOwnershipStates 逐条对应：
  // 每个 stage × 「changes 还在不在手里」的两种状态。Go 侧那份是权威实现，
  // 这里多一条都不能少——漏一条就等于放过一种"数据救不回来"的路径。
  const cases: Array<{
    name: string
    stage: string
    dataMissing: boolean
    haveChanges: boolean
    want: Decision
  }> = [
    { name: 'ready 就是完成', stage: STAGES.indexReady, dataMissing: false, haveChanges: true, want: 'DONE' },
    { name: 'ready 且没有 changes 也是完成', stage: STAGES.indexReady, dataMissing: false, haveChanges: false, want: 'DONE' },
    { name: '还在写：等', stage: STAGES.dataPending, dataMissing: false, haveChanges: true, want: 'WAIT' },
    { name: '数据已持久但没到目标：继续等', stage: STAGES.dataDurable, dataMissing: false, haveChanges: true, want: 'WAIT' },
    { name: '索引失败：可重建，等', stage: STAGES.indexFailed, dataMissing: false, haveChanges: true, want: 'WAIT' },
    { name: '正在删除：放弃', stage: STAGES.deleting, dataMissing: false, haveChanges: true, want: 'DISCARD' },
    { name: '数据缺失但 changes 还在：重发', stage: STAGES.dataPending, dataMissing: true, haveChanges: true, want: 'RESEND' },
    { name: '数据缺失且 changes 已丢：只能放弃', stage: STAGES.dataPending, dataMissing: true, haveChanges: false, want: 'DISCARD' },
    { name: '数据侧判死、changes 还在：从头来', stage: STAGES.dataFailedPermanent, dataMissing: false, haveChanges: true, want: 'RESEND' },
    { name: '数据侧判死、changes 已丢：放弃', stage: STAGES.dataFailedPermanent, dataMissing: false, haveChanges: false, want: 'DISCARD' },
    { name: '索引侧判死、changes 还在：从头来', stage: STAGES.indexFailedPermanent, dataMissing: false, haveChanges: true, want: 'RESEND' },
    { name: '陌生 stage 是更新版服务端：什么都不能毁', stage: 'SOMETHING_NEW', dataMissing: false, haveChanges: true, want: 'WAIT' },
  ]

  it.each(cases)('$name', ({ stage, dataMissing, haveChanges, want }) => {
    expect(decide(resp(stage, dataMissing), haveChanges)).toBe(want)
  })

  it('data_missing 只对 DATA_PENDING 有影响（其余 stage 上它不改变判定）', () => {
    // 服务端的契约：data_missing 只在 PENDING 且超过探测年龄阈值时有意义，
    // 且它**不改变 stage**。所以别处出现 true 也不该改变决定。
    for (const stage of [STAGES.dataDurable, STAGES.indexFailed, STAGES.indexReady]) {
      expect(decide(resp(stage, true), true)).toBe(decide(resp(stage, false), true))
    }
  })
})

describe('decide 的目标维度（await target）', () => {
  // target 必须**同时**作用于「发给服务端的 AwaitTarget」和「decide 的判据」。
  // 只改一边，正是这个 describe 要防的东西：服务端在 DATA_DURABLE 就返回了，
  // 而判据说"还差一个 READY" → 一路空转到超时。
  it('DATA_DURABLE 目标：DURABLE 就算完成', () => {
    expect(decide(resp(STAGES.dataDurable), true, 'DATA_DURABLE')).toBe('DONE')
  })

  it('DATA_DURABLE 目标：READY 当然也算（服务端可能一次跳过头）', () => {
    expect(decide(resp(STAGES.indexReady), true, 'DATA_DURABLE')).toBe('DONE')
  })

  it('DATA_DURABLE 目标：还没到 DURABLE 就继续等', () => {
    expect(decide(resp(STAGES.dataPending), true, 'DATA_DURABLE')).toBe('WAIT')
    expect(decide(resp(STAGES.indexFailed), true, 'DATA_DURABLE')).toBe('WAIT')
  })

  it('INDEX_READY 目标：DURABLE 还不够，继续等', () => {
    expect(decide(resp(STAGES.dataDurable), true, 'INDEX_READY')).toBe('WAIT')
  })

  it('终局判定不被目标检查吞掉', () => {
    // 这些 stage 永远不等于 DURABLE/READY，所以目标检查拦不住它们。
    // 这条钉的是"以后有人把目标检查挪到 switch 之后"不会悄悄改变语义。
    expect(decide(resp(STAGES.dataFailedPermanent), true, 'DATA_DURABLE')).toBe('RESEND')
    expect(decide(resp(STAGES.dataFailedPermanent), false, 'DATA_DURABLE')).toBe('DISCARD')
    expect(decide(resp(STAGES.deleting), true, 'DATA_DURABLE')).toBe('DISCARD')
  })

  it('data_missing 在两种目标下都照样触发重发/放弃', () => {
    for (const t of ['INDEX_READY', 'DATA_DURABLE'] as const) {
      expect(decide(resp(STAGES.dataPending, true), true, t)).toBe('RESEND')
      expect(decide(resp(STAGES.dataPending, true), false, t)).toBe('DISCARD')
    }
  })
})

describe('STAGES 与服务端常量一致', () => {
  // 对应 client/pending_test.go 的 TestStagesMatchTheServiceConstants。
  //
  // Go 侧是把 stage 字面量抄了一份（客户端不该 link 服务端实现），靠测试钉住。
  // 前端连抄都抄不到同一处，所以这里直接读那个 Go 文件——只要服务端改了名字而
  // 前端没跟，这条就会红。stage 是 string 而不是 proto enum，生成器帮不上忙。
  const goSource = readFileSync(
    new URL('../../../service/await_version.go', import.meta.url),
    'utf8',
  )

  const pairs: Array<[string, keyof typeof STAGES]> = [
    ['StageDataPending', 'dataPending'],
    ['StageDataDurable', 'dataDurable'],
    ['StageIndexReady', 'indexReady'],
    ['StageIndexFailed', 'indexFailed'],
    ['StageDataFailedPermanent', 'dataFailedPermanent'],
    ['StageIndexFailedPermanent', 'indexFailedPermanent'],
    ['StageDeleting', 'deleting'],
  ]

  it.each(pairs)('%s 一致', (goConst, ourKey) => {
    const m = new RegExp(`${goConst}\\s*=\\s*"([^"]+)"`).exec(goSource)
    expect(m, `service/await_version.go 里找不到 ${goConst}`).not.toBeNull()
    expect(STAGES[ourKey]).toBe(m?.[1])
  })
})
