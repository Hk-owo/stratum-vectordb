import type { ParseResult, Parser } from './index'

/**
 * 纯文本类：`.txt` / `.md` / `.csv` / `.json` / `.log` 等，浏览器直接解码。
 *
 * 换行统一成 `\n` 是必要的：内容哈希（ingest/hash.ts）用来判断"这个文件改了吗"，
 * 而同一个文件在不同平台上可能带 `\r\n`。不统一的话，一次无意义的换行差异就会
 * 让前端误判成"内容变了"，发出一条实际没变的 UPDATE。
 */
const TEXT_EXTENSIONS = [
  '.txt',
  '.md',
  '.markdown',
  '.csv',
  '.tsv',
  '.json',
  '.jsonl',
  '.log',
  '.yaml',
  '.yml',
] as const

export const textParser: Parser = {
  extensions: TEXT_EXTENSIONS,
  label: '纯文本',

  async parse(file: File): Promise<ParseResult> {
    const raw = await file.text()
    return { text: raw.replace(/\r\n?/g, '\n') }
  },
}
