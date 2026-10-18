import { describe, expect, it } from 'vitest'

import { ApiError } from '../api/client'
import { discardForExit, type DiscardDeps } from './discard'
import type { PendingWrite } from './store'

function rec(over: Partial<PendingWrite> = {}): PendingWrite {
  return {
    id: over.id ?? 'web-1',
    knowledge_base_id: over.knowledge_base_id ?? 'kb-1',
    version_id: over.version_id ?? '',
    client_request_id: over.client_request_id ?? 'web-1',
    changes: [],
    submitted_at: over.submitted_at ?? '2026-01-01T00:00:00.000Z',
    settled: false,
  }
}

/**
 * 记录每一次调用，让断言直接看"服务端被要求做了什么"。
 *
 * `respond` 只决定服务端怎么答，调用记录由这里统一收集——分开写的话，
 * 自定义应答的用例会把 `discarded` 记漏。
 */
function harness(
  opts: {
    respond?: (versionId: string) => Promise<{ discarded: boolean }>
    now?: () => number
  } = {},
) {
  const discarded: string[] = []
  const removed: string[] = []
  const history: string[] = []

  const deps: DiscardDeps = {
    discard: async (_kbId, versionId) => {
      discarded.push(versionId)
      return opts.respond === undefined ? { discarded: true } : opts.respond(versionId)
    },
    remove: async (id) => {
      removed.push(id)
    },
    history: (e) => {
      history.push(e.detail)
    },
    now: opts.now ?? (() => 0),
  }

  return { deps, discarded, removed, history }
}

describe('discardForExit', () => {
  it('版本号还是空串：按"未创建"处理，清记录且不发请求', async () => {
    const h = harness()

    const report = await discardForExit([rec({ id: 'a', version_id: '' })], { deps: h.deps })

    expect(h.discarded).toEqual([]) // 没有可放弃的对象
    expect(h.removed).toEqual(['a'])
    expect(report.outcomes[0]?.kind).toBe('unknown-version')
    expect(report.cleared).toHaveLength(1)
    expect(report.kept).toHaveLength(0)
  })

  it('放弃成功：清记录，并在历史里留一条', async () => {
    const h = harness()

    const report = await discardForExit([rec({ id: 'a', version_id: '12' })], { deps: h.deps })

    expect(h.discarded).toEqual(['12'])
    expect(h.removed).toEqual(['a'])
    expect(h.history[0]).toContain('v12')
    expect(report.outcomes[0]?.kind).toBe('discarded')
  })

  it('服务端说数据已落盘（version_not_pending）：不再算"未落地"，记录清掉并说明缺的是索引', async () => {
    const h = harness({
      respond: async () => {
        throw new ApiError(412, 'FailedPrecondition', 'version 12 is not pending')
      },
    })

    const report = await discardForExit([rec({ id: 'a', version_id: '12' })], { deps: h.deps })

    expect(h.removed).toEqual(['a'])
    expect(report.outcomes[0]?.kind).toBe('already-settled')
    // 这条提示是用户下一步唯一的线索：数据在，索引要自己建。
    expect(report.outcomes[0]?.note).toContain('预热')
  })

  it('版本已不存在（找不到）：当放弃成功，记录清掉', async () => {
    const h = harness({
      respond: async () => {
        throw new ApiError(404, 'NotFound', 'version 12 not found')
      },
    })

    const report = await discardForExit([rec({ id: 'a', version_id: '12' })], { deps: h.deps })

    expect(h.removed).toEqual(['a'])
    expect(report.outcomes[0]?.kind).toBe('discarded')
  })

  it('没等到答复（网关不可达）：记录必须留下——那是重发凭证', async () => {
    const h = harness({
      respond: async () => {
        throw new ApiError(0, 'Unreachable', 'connect: connection refused')
      },
    })

    const report = await discardForExit([rec({ id: 'a', version_id: '12' })], { deps: h.deps })

    expect(h.removed).toEqual([])
    expect(report.outcomes[0]?.kind).toBe('unreachable')
    expect(report.kept.map((w) => w.id)).toEqual(['a'])
  })

  it('总预算耗尽后不再尝试剩余记录，它们一律按"未确认"保留', async () => {
    let clock = 0
    const h = harness({
      now: () => clock,
      respond: async () => {
        clock += 6000 // 第一条就把 5 s 预算花光
        return { discarded: true }
      },
    })

    const report = await discardForExit(
      [rec({ id: 'a', version_id: '1' }), rec({ id: 'b', version_id: '2' })],
      { deps: h.deps },
    )

    expect(h.discarded).toEqual(['1']) // 第二条没有发
    expect(report.outcomes[1]?.kind).toBe('unreachable')
    expect(report.kept.map((w) => w.id)).toEqual(['b'])
  })

  it('逐条串行处理，且顺序与传入一致', async () => {
    const h = harness()

    await discardForExit(
      [rec({ id: 'a', version_id: '1' }), rec({ id: 'b', version_id: '2' })],
      { deps: h.deps },
    )

    expect(h.discarded).toEqual(['1', '2'])
    expect(h.removed).toEqual(['a', 'b'])
  })

  it('清记录本身失败也只降级这一条，绝不让整个流程抛出去', async () => {
    // 这个函数跑在"用户已经在关窗口"的路径上：抛出去 = 窗口关不掉 = 用户被困住。
    const h = harness({
      now: () => 0,
      respond: async () => ({ discarded: true }),
    })
    const broken: DiscardDeps = {
      ...h.deps,
      remove: async () => {
        throw new Error('app_data_dir 只读')
      },
    }

    const report = await discardForExit(
      [rec({ id: 'a', version_id: '' }), rec({ id: 'b', version_id: '2' })],
      { deps: broken },
    )

    expect(report.outcomes.map((o) => o.kind)).toEqual(['unreachable', 'unreachable'])
    expect(report.kept.map((w) => w.id)).toEqual(['a', 'b'])
    // 服务端那次放弃其实发出去了——记录清不掉不等于版本还在。
    expect(h.discarded).toEqual(['2'])
  })
})
