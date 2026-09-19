import { describe, expect, it } from 'vitest'

import { DETAIL_MAX, HISTORY_LIMIT, clipDetail } from './store'

describe('clipDetail', () => {
  it('把换行和连续空白折成单空格', () => {
    // 历史是一行一条的记录，"说明"里出现换行会把表格撑开，也会让 JSONL 多出
    // 一行——那是真的会把文件解析坏，不只是难看。
    expect(clipDetail('第一行\n第二行')).toBe('第一行 第二行')
    expect(clipDetail('a   b\t\tc')).toBe('a b c')
  })

  it('去掉首尾空白', () => {
    expect(clipDetail('  前后都有  ')).toBe('前后都有')
  })

  it('不超限时原样保留', () => {
    const s = 'x'.repeat(DETAIL_MAX)
    expect(clipDetail(s)).toBe(s)
    expect(clipDetail(s)).not.toContain('…')
  })

  it('超限时截断到上限并加省略号', () => {
    const out = clipDetail('x'.repeat(DETAIL_MAX + 50))
    // 长度仍是上限：省略号占掉最后一个字符位，而不是额外多出一个字符——否则
    // 中文（3 字节/字）会比 DETAIL_MAX 多出 3 字节，"上限"就不成立了。
    expect(out.length).toBe(DETAIL_MAX)
    expect(out.endsWith('…')).toBe(true)
  })

  it('按字符而不是字节截断，中文不被劈开', () => {
    const out = clipDetail('中'.repeat(DETAIL_MAX + 10))
    expect(out.length).toBe(DETAIL_MAX)
    expect(out.endsWith('…')).toBe(true)
    // 劈开多字节字符会得到 U+FFFD，表现为乱码写入历史文件。
    expect(out).not.toContain('\uFFFD')
  })
})

describe('常量', () => {
  it('界面展示的上限小于文件侧保留的条数', () => {
    // 文件侧留 2000 条是为了"以后要查"，界面一屏 200 条是给人看的。若哪天把
    // 界面改成不小于文件侧的上限，说明有人把两个用途搞混了。
    expect(HISTORY_LIMIT).toBeLessThan(2000)
  })
})
