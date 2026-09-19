/**
 * 「文件 → 纯文本」这一步。
 *
 * 这是前端在整条摄入链路上**唯一**的智力工作：stratum 只吃文档全文，切块
 * （CDC 内容定义分块）与 embedding 都在服务端（client-integration-guide §2.4）。
 * 所以前端不做任何切分，也不碰向量——只负责把各种宿主格式还原成文本。
 *
 * 解析器做成注册表而不是 if/switch：加格式时只增一个文件，调用方不动。
 */
import { docxParser } from './docx'
import { textParser } from './text'

export interface ParseResult {
  /** 提取出的纯文本。空字符串是**合法返回值**，由调用方决定它算不算错误。 */
  text: string
  /** 给界面看的一句话，例如"解析器报告了 2 条警告"。 */
  note?: string
}

export interface Parser {
  /** 能处理的扩展名，小写含点。 */
  readonly extensions: readonly string[]
  /** 人可读的名字，用于错误信息。 */
  readonly label: string
  parse(file: File): Promise<ParseResult>
}

/** 认不出扩展名。与"解析出来是空的"是两件事，前端要分开报。 */
export class UnsupportedFileError extends Error {
  constructor(readonly fileName: string) {
    super(`没有能解析 ${fileName} 的解析器`)
    this.name = 'UnsupportedFileError'
  }
}

/** 注册顺序即匹配顺序；扩展名不该重叠。 */
const PARSERS: readonly Parser[] = [textParser, docxParser]

function extensionOf(fileName: string): string {
  const dot = fileName.lastIndexOf('.')
  return dot < 0 ? '' : fileName.slice(dot).toLowerCase()
}

export function parserFor(fileName: string): Parser | undefined {
  const ext = extensionOf(fileName)
  return PARSERS.find((p) => p.extensions.includes(ext))
}

/** `<input type="file" accept="...">` 用得上：把注册表里的扩展名拼出来。 */
export function acceptedExtensions(): string {
  return PARSERS.flatMap((p) => [...p.extensions]).join(',')
}

export async function parseFile(file: File): Promise<ParseResult> {
  const parser = parserFor(file.name)
  if (parser === undefined) {
    throw new UnsupportedFileError(file.name)
  }
  return parser.parse(file)
}
