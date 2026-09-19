/**
 * doc_id 的计算：**规范化相对路径**（方案 A）。
 *
 * 为什么不把内容哈希编进 doc_id——三条理由，都有上游依据：
 *
 * 1. **内容更新是最高频的操作，它必须零依赖。** doc_id = 路径时，重传同一路径
 *    就是一条 UPDATE 原地覆盖，不需要任何本地记录去查"上一次这个文件的 doc_id
 *    是什么"。而 doc_id 里带内容哈希，每一次内容变更都得先找到旧 doc_id 再发
 *    DELETE——那份映射一旦丢（换浏览器、清缓存、换个人上传），旧内容就**永久
 *    留在库里继续参与检索**，而且补救不了：`ListVersions` 只回 `doc_id_set_hash`，
 *    没有任何接口能列出 doc_id 集合（client-integration-guide §2.2）。
 *
 * 2. **它换不来额外的去重。** 内容寻址是 chunk 层的事：
 *    `ChunkID = SHA-256(chunk text + model_id)`，**与 doc_id 无关**（§2.4）。
 *    重传同一份内容，无论 doc_id 取什么形状，chunk 都会被复用。"把哈希放进
 *    doc_id"想要的那个幂等，系统本来就有。
 *
 * 3. **doc_id 是"回源定位的键"（§2.3）。** 检索结果里的 `QueryResult.doc_id`
 *    就是当初提交的那个标识——路径才能让人点回原文件，带哈希的串两样都做不到。
 *
 * 至于"留旧版本"：那是**版本链**的职责，而且已经由 MVCC 自动做到了——docstore
 * 的键是 `kbID + docID + versionID`，同一个路径的旧内容一直躺在旧版本里，
 * `Query(version_id=N)` 查得到。把它再编码进 doc_id 只是把同一个维度存了两遍。
 */

/**
 * 规范化一个相对路径，作为 doc_id。
 *
 * 只做保守的清理：统一分隔符、去掉空段与 `.`。`..` **原样保留**——它本来不该
 * 出现在文件选择器给出的相对路径里，真出现就让它在 doc_id 里看得见，而不是
 * 悄悄折叠掉、把一个越界的路径伪装成合法的。
 */
export function normalizeDocId(relPath: string): string {
  return relPath
    .replace(/\\/g, '/')
    .split('/')
    .filter((seg) => seg !== '' && seg !== '.')
    .join('/')
}

/**
 * 一个 File 的 doc_id。
 *
 * `webkitdirectory`（选文件夹）会给 `webkitRelativePath`，那正是我们要的层级；
 * 普通多选时它为空，退回 `name`。
 */
export function docIdOf(file: File): string {
  const rel = file.webkitRelativePath !== '' ? file.webkitRelativePath : file.name
  return normalizeDocId(rel)
}
