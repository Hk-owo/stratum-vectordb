import type { ParseResult, Parser } from './index'

/**
 * `.docx` —— mammoth 解 OOXML 取纯文本。
 *
 * 只认 `.docx`，不认 `.doc`：老格式是 OLE 复合文档，mammoth 不支持。用户拖进来
 * 一个 `.doc` 会得到"没有解析器"，这是**对的**——比悄悄解析出乱码强。
 *
 * 为什么不用 PDF：pdf.js 只能取文本层，扫描版 PDF（图片）会解析出 0 字符然后
 * 被当成正常文档提交，污染知识库且事后极难发现。与其在界面上加一堆"可能是扫描版"
 * 的提示，不如不支持——需要 PDF 的用户自己转文本，转的过程本身就会暴露问题。
 *
 * mammoth 走**动态 import**：它连同依赖有几百 KB，而只有真正拖进 .docx 时才需要。
 * 静态 import 会让首屏 bundle 多背 400+ KB，而那对"打开控制台看一眼健康状态"的
 * 用户毫无用处。
 */
export const docxParser: Parser = {
  extensions: ['.docx'],
  label: 'Word 文档 (.docx)',

  async parse(file: File): Promise<ParseResult> {
    const arrayBuffer = await file.arrayBuffer()
    // mammoth 是 CJS：动态 import 拿到的是命名空间，实现挂在 default 上。
    const mammoth = (await import('mammoth')).default
    const result = await mammoth.extractRawText({ arrayBuffer })

    // mammoth 的 messages 是它遇到的样式/结构问题（丢弃的脚注、不认识的元素等）。
    // 不拦截提交——正文通常是完整的——但要让用户看得见，否则"少了半页"没人知道。
    const warnings = result.messages.filter((m) => m.type === 'warning')
    const note =
      warnings.length > 0
        ? `解析器报告 ${warnings.length} 条警告：${warnings[0]?.message ?? ''}`
        : undefined

    return { text: result.value.replace(/\r\n?/g, '\n'), note }
  },
}
