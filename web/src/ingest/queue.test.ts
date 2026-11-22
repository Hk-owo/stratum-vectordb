import { beforeEach, describe, expect, it } from 'vitest'

import { loadDigests, saveDigests } from './queue'

function installLocalStorage(): void {
  const store = new Map<string, string>()
  ;(globalThis as { localStorage?: Storage }).localStorage = {
    get length() {
      return store.size
    },
    clear: () => store.clear(),
    getItem: (k: string) => store.get(k) ?? null,
    key: (i: number) => [...store.keys()][i] ?? null,
    removeItem: (k: string) => {
      store.delete(k)
    },
    setItem: (k: string, v: string) => {
      store.set(k, v)
    },
  }
}

describe('loadDigests / saveDigests 按知识库隔离', () => {
  beforeEach(() => {
    installLocalStorage()
  })

  it('不同 kbId 的指纹互不串扰', () => {
    saveDigests({ 'doc.md': 'hash-a' }, 'kb-1')
    saveDigests({ 'doc.md': 'hash-b' }, 'kb-2')

    expect(loadDigests('kb-1')).toEqual({ 'doc.md': 'hash-a' })
    expect(loadDigests('kb-2')).toEqual({ 'doc.md': 'hash-b' })
    expect(loadDigests('kb-3')).toEqual({})
  })
})
