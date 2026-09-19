/**
 * 内容指纹。
 *
 * **不是** doc_id 的组成部分（见 ./docid.ts 的解释），它只回答一个问题：
 * 「这个文件的内容和上次成功提交的一样吗」。
 *
 * 为什么必须有它：`changes` 是**增量**，而空 `changes` 会被服务端拒绝
 * （`empty_changes` / 400）——它的含义是"与父版本相同"，不是"空集"，那不是
 * 服务端能陈述的文档集（client-integration-guide §2.2）。所以"重传一个没改过的
 * 文件"不能在服务端被消化，只能由调用方在提交前滤掉：
 *
 *   前端的变更检测，正是为了让服务端那条原则不被触发。两者是配合，不是矛盾。
 */

/** 文本的 SHA-256 十六进制。 */
export async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text))
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('')
}
