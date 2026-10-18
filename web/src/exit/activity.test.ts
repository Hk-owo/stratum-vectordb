import { describe, expect, it } from 'vitest'

import { describeWork, hasWork, setQueuedFiles, setSubmitting, workSnapshot } from './activity'
import type { PendingWrite } from '../pending/store'

function pending(versionId: string, id = `w-${versionId || 'new'}`): PendingWrite {
  return {
    id,
    knowledge_base_id: 'kb-1',
    version_id: versionId,
    client_request_id: id,
    changes: [],
    submitted_at: '2026-01-01T00:00:00.000Z',
    settled: false,
  }
}

describe('hasWork', () => {
  it('在途批次、正在提交、待提交文件，任一成立就要拦', () => {
    const none = { unsettled: [], submitting: false, queuedFiles: 0 }
    expect(hasWork(none)).toBe(false)
    expect(hasWork({ ...none, unsettled: [pending('3')] })).toBe(true)
    expect(hasWork({ ...none, submitting: true })).toBe(true)
    expect(hasWork({ ...none, queuedFiles: 2 })).toBe(true)
  })
})

describe('describeWork', () => {
  it('分别说清三件事的后果：放弃版本 / 版本号可能没回来 / 文件退出即丢', () => {
    const lines = describeWork({
      unsettled: [pending('7'), pending('')],
      submitting: true,
      queuedFiles: 3,
    })

    expect(lines).toHaveLength(3)
    expect(lines[0]).toContain('v7')
    expect(lines[0]).toContain('未分配版本') // 空版本号的批次也要看得见
    expect(lines[0]).toContain('放弃')
    expect(lines[1]).toContain('正在提交')
    expect(lines[2]).toContain('3 个文件')
  })

  it('什么都没悬着时不说任何话', () => {
    expect(describeWork({ unsettled: [], submitting: false, queuedFiles: 0 })).toEqual([])
  })
})

describe('workSnapshot', () => {
  it('负数和 NaN 的文件数按 0 处理（判据不能被脏数据骗成"有活"）', () => {
    setSubmitting(false)
    setQueuedFiles(-3)
    expect(workSnapshot().queuedFiles).toBe(0)

    setQueuedFiles(Number.NaN)
    expect(workSnapshot().queuedFiles).toBe(0)

    setQueuedFiles(4)
    setSubmitting(true)
    const snap = workSnapshot()
    expect(snap.queuedFiles).toBe(4)
    expect(snap.submitting).toBe(true)

    setQueuedFiles(0)
    setSubmitting(false)
  })
})
