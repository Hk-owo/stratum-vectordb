import { describe, expect, it } from 'vitest'

import { DataStatus, IndexStatus, type VersionInfo } from '../api/gen/knowledgebase'
import { chainTail, describeNotReady } from './chainTail'

function version(over: Partial<VersionInfo> & { version_id: string }): VersionInfo {
  return {
    version_id: over.version_id,
    knowledge_base_id: 'kb-1',
    parent_version_id: over.parent_version_id ?? '0',
    data_status: over.data_status ?? DataStatus.DATA_STATUS_DURABLE,
    index_status: over.index_status ?? IndexStatus.INDEX_STATUS_PENDING,
    doc_id_set_hash: over.doc_id_set_hash ?? '',
    created_at: over.created_at ?? '0',
    deleting: over.deleting ?? false,
    index_ready_nodes: [],
  } as VersionInfo
}

describe('chainTail', () => {
  it('没有版本：以 0 为根', () => {
    expect(chainTail([])).toEqual({ kind: 'none' })
  })

  it('链尾的数据已落盘就可作父——索引状态完全不参与', () => {
    // 这是这条规则的全部要点：索引由查询驱动、按 owner 集放置，写路径既不知道
    // 也不该知道它建没建。
    const tail = chainTail([version({ version_id: '7', index_status: IndexStatus.INDEX_STATUS_PENDING })])
    expect(tail).toEqual({ kind: 'ok', versionId: '7' })
  })

  it('取 version_id 最大的那个，而不是字符串比较的结果', () => {
    const tail = chainTail([version({ version_id: '9' }), version({ version_id: '10' })])
    expect(tail).toEqual({ kind: 'ok', versionId: '10' })
  })

  it('链尾数据还没落盘：不能作父，也不能退回更早的版本', () => {
    const tail = chainTail([
      version({ version_id: '3' }),
      version({ version_id: '4', data_status: DataStatus.DATA_STATUS_PENDING }),
    ])
    expect(tail).toEqual({ kind: 'not-ready', versionId: '4', reason: 'data-pending' })
  })

  it('链尾正在删除：同样不能作父', () => {
    const tail = chainTail([version({ version_id: '5', deleting: true })])
    expect(tail).toEqual({ kind: 'not-ready', versionId: '5', reason: 'deleting' })
  })

  it('「不能提交」的说明分两种情形，各自说清原因', () => {
    expect(describeNotReady({ kind: 'not-ready', versionId: '4', reason: 'data-pending' })).toContain(
      '还没落盘',
    )
    expect(describeNotReady({ kind: 'not-ready', versionId: '5', reason: 'deleting' })).toContain(
      '正在清理',
    )
  })
})
