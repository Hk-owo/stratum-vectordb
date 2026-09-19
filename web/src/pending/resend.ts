/**
 * 从本机记录重发一批在途数据。
 *
 * 这是「这份记录是重发还是放弃的唯一依据」这句话真正落地的地方：页面刷新之后，
 * IndexedDB 里那条记录还带着**原始的 changes 和原始的 client_request_id**，
 * 于是调用方还能把同一个 key 再举一次——服务端认它，就会复用首次分配的那个版本，
 * 而不是另开一个（docs/client-integration-guide.md §7.12）。
 *
 * 没有这个入口，在途面板就只是个"你看还有东西悬着"的告示牌：用户能做的只有丢弃，
 * 而那恰好是把数据彻底扔掉。
 */
import { api, kbPath } from '../api/client'
import type { CreateVersionResponse } from '../api/gen/knowledgebase'
import { awaitUntilTerminal } from './awaitUntilTerminal'
import type { Decision } from './decide'
import { removePending, upsertPending, type PendingWrite } from './store'

export interface ResendOutcome {
  versionId: string
  decision: Decision
  /** 最终看到的 stage，界面上用来解释结果。 */
  stage: string
  /** 服务端是否报告了"没有副本持有这份数据"。 */
  dataMissing: boolean
}

/**
 * 重发一条在途记录。
 *
 * @param parentVersionId 当前链尾。**必须现取**，不能存进记录里沿用：如果首次
 *   提交压根没分配出版本（网络层就失败了），这次就是一个全新的提交，parent 必须
 *   指向真正的链尾；如果它已经分配过版本，服务端会按同一个 key 复用那个版本，
 *   parent 根本不参与分配。存一个旧的 parent 反而不如现取安全。
 */
export async function resendPending(
  w: PendingWrite,
  parentVersionId: string | null,
  opts: { signal?: AbortSignal; onProgress?: (stage: string) => void } = {},
): Promise<ResendOutcome> {
  const resp = await api.post<CreateVersionResponse>(kbPath(w.knowledge_base_id, '/versions'), {
    knowledge_base_id: w.knowledge_base_id,
    parent_version_id: parentVersionId ?? '0',
    changes: w.changes,
    // **同一个** key —— 这是整件事的关键。换一个 key 就等于新开一条版本链尾。
    client_request_id: w.client_request_id,
  })

  // 服务端可能回传它自己生成的 key（调用方当初没带 key 时），照样写回记录。
  await upsertPending({
    ...w,
    id: resp.client_request_id,
    client_request_id: resp.client_request_id,
    version_id: resp.version_id,
    submitted_at: new Date().toISOString(),
  })

  const outcome = await awaitUntilTerminal(w.knowledge_base_id, resp.version_id, {
    haveChanges: true,
    signal: opts.signal,
    onProgress: (r) => opts.onProgress?.(r.stage),
  })

  if (outcome.decision === 'DONE' || outcome.decision === 'DISCARD') {
    // 有结论了，记录不必再挂着（DISCARD 时数据确实救不回来，留着也只会误导）。
    await removePending(resp.client_request_id)
  }

  return {
    versionId: resp.version_id,
    decision: outcome.decision,
    stage: outcome.last.stage,
    dataMissing: outcome.last.data_missing,
  }
}
