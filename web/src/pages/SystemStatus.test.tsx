/**
 * 系统状态页的渲染冒烟测试。
 *
 * 为什么是渲染而不是交互：这一页的目的是"把需要动作的状态摆出来"，而两个 action
 * 按钮的**存在**本身就是可运维性的一部分——没有它们，§10.1 判死的版本只能靠手工发
 * RPC。点击 + 二次确认 + mutation 入参要 DOM 与事件，那需要 jsdom 与 testing-library；
 * 这里先用服务端渲染钉住"数据到了就会画出来、按钮在、两侧都画得出来"，与
 * Ops.test.tsx 同一种取舍（首屏白屏这类错误在 SSR 里一样会抛）。
 *
 * 数据全部预置进 query cache（而不是打网关）：断言的是渲染逻辑，不依赖后端，也不受
 * 集群当前状态影响。
 */
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { renderToString } from 'react-dom/server'
import { describe, expect, it } from 'vitest'

import { HealthStatus, type GetSystemStatusResponse } from '../api/gen/admin'
import { FailureSide } from '../api/gen/knowledgebase'
import { qk } from '../api/queries'
import { SystemStatus } from './SystemStatus'

function status(): GetSystemStatusResponse {
  return {
    health: { status: HealthStatus.HEALTH_STATUS_DEGRADED, details: 'two versions need action' },
    stuck_versions: [],
    delete_failed_kbs: [],
    wal_alerts: [],
    resource_usage: undefined,
    deleting_versions: [],
    data_missing_versions: [],
    failed_permanent_versions: [
      // 两侧都放一条：§10.1b 说它们是两个独立裁决、补救办法不同，所以渲染必须能同时
      // 容纳它们，而不是只处理默认的那一侧。
      {
        kb_id: 'kb-index-side',
        version_id: '7',
        reason: 'index build failed after 5 attempts',
        failure_count: 5,
        side: FailureSide.FAILURE_SIDE_INDEX,
      },
      {
        kb_id: 'kb-data-side',
        version_id: '9',
        reason: 'no reachable replica holds the data',
        failure_count: 5,
        side: FailureSide.FAILURE_SIDE_DATA,
      },
    ],
    gc_blocked_versions: [],
  }
}

function render(): string {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  qc.setQueryData(qk.systemStatus, status())
  return renderToString(
    <QueryClientProvider client={qc}>
      <SystemStatus />
    </QueryClientProvider>,
  )
}

describe('SystemStatus', () => {
  it('把判死的版本画出来（两侧各一条）', () => {
    const html = render()
    expect(html).toContain('永久失败')
    expect(html).toContain('kb-index-side')
    expect(html).toContain('kb-data-side')
    expect(html).toContain('no reachable replica holds the data')
    // §10.1b：两个裁决要能分辨，否则运维不知道该重试写入还是重建索引。
    expect(html).toContain('索引侧')
    expect(html).toContain('数据侧')
  })

  it('给出两个运维动作：重试索引与放弃', () => {
    const html = render()
    expect(html).toContain('重试索引')
    expect(html).toContain('放弃')
  })
})
