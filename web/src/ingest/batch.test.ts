import { describe, expect, it } from 'vitest'

import { MAX_BATCH_BYTES, planBatches } from './batch'
import type { QueueItem } from './queue'

function item(docId: string, content: string): QueueItem {
  return { docId, file: new File([], docId), status: 'ready', content, hash: '' }
}

/** 正好 n 个 ASCII 字节的内容。 */
const bytes = (n: number) => 'a'.repeat(n)

describe('planBatches', () => {
  it('空输入 → 无批次', () => {
    expect(planBatches([])).toEqual([])
  })

  it('总量不超限时合成一批', () => {
    const batches = planBatches([item('a.txt', bytes(10)), item('b.txt', bytes(10))])
    expect(batches).toHaveLength(1)
    expect(batches[0]?.items.map((i) => i.docId)).toEqual(['a.txt', 'b.txt'])
    expect(batches[0]?.bytes).toBe(20)
  })

  it('超限时切分，且不丢项、不重复', () => {
    const items = [item('a', bytes(600)), item('b', bytes(600)), item('c', bytes(600))]
    const batches = planBatches(items, 1000)

    expect(batches.map((b) => b.items.map((i) => i.docId))).toEqual([['a'], ['b'], ['c']])
    expect(batches.flatMap((b) => b.items)).toHaveLength(3)
  })

  it('恰好等于上限时不拆（边界是 > 而不是 >=）', () => {
    const batches = planBatches([item('a', bytes(500)), item('b', bytes(500))], 1000)
    expect(batches).toHaveLength(1)
    expect(batches[0]?.bytes).toBe(1000)
  })

  it('单个超限的文档自成一其——文档不能被拆开', () => {
    const batches = planBatches([item('big', bytes(2500)), item('small', bytes(10))], 1000)
    expect(batches).toHaveLength(2)
    expect(batches[0]?.items.map((i) => i.docId)).toEqual(['big'])
    expect(batches[0]?.bytes).toBe(2500)
    expect(batches[1]?.items.map((i) => i.docId)).toEqual(['small'])
  })

  it('按 UTF-8 字节算，不是按 UTF-16 码元', () => {
    // 一个汉字在 UTF-8 里 3 字节，但在 JS 里 length === 1。若按 length 计，
    // 中文文档会被低估近 3 倍、批次远大于上限——这个用例就是钉这一点。
    const chinese = '中'.repeat(400) // 400 码元，1200 字节
    expect(chinese.length).toBe(400)

    const batches = planBatches([item('zh', chinese)], 1000)
    expect(batches).toHaveLength(1)
    expect(batches[0]?.bytes).toBe(1200)
  })

  it('默认上限是 1 MiB', () => {
    expect(MAX_BATCH_BYTES).toBe(1 << 20)
  })
})
