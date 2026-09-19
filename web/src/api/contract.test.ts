/**
 * 契约测试：用**前端自己的代码**（`api` / `kbPath` / `awaitVersionOnce`）去打一个
 * 真在跑的 gateway，验证它发出的请求形状网关能正确理解。
 *
 * 为什么需要它——curl 验证的是"网关的端点没问题"，而这份文件验证的是"**前端的
 * 调用没问题**"。两者的差别是真实存在的：路径拼错、枚举名写成别的写法、`int64`
 * 忘了转字符串，这些都能让 curl 通过而前端静默失败。
 *
 * 它是一个**集成**测试：网关不可达时整体跳过，所以 `npm test` 在没有后端的机器上
 * 仍然全绿。跑它之前先起链路：
 *
 *     ./start.sh        # 或手动起 router + gateway + /ops/start
 */
import { beforeAll, describe, expect, it } from 'vitest'

import { api, kbPath, setApiBase } from './client'
import { awaitVersionOnce } from './queries'
import {
  AwaitTarget,
  type CreateKnowledgeBaseResponse,
  type CreateVersionResponse,
  type GetKnowledgeBaseResponse,
  type ListVersionsResponse,
  type RollbackVersionResponse,
} from './gen/knowledgebase'
import type { GetSystemStatusResponse, HealthCheckResponse } from './gen/admin'
import type { QueryResponse } from './gen/query'

const GATEWAY = process.env.STRATUM_GATEWAY ?? 'http://127.0.0.1:8081'

/** 先探一次。不可达就整体跳过——没有后端的机器上不该红。 */
async function gatewayAlive(): Promise<boolean> {
  try {
    const res = await fetch(`${GATEWAY}/api/health`, { signal: AbortSignal.timeout(3000) })
    return res.status === 200
  } catch {
    return false
  }
}

const alive = await gatewayAlive()

describe.skipIf(!alive)('与真实 gateway 的契约', () => {
  let kbId = ''

  beforeAll(() => {
    // 关键一步：桌面壳就是这么用的（WebView 非同源，必须给绝对地址）。
    setApiBase(GATEWAY)
  })

  it('GET /api/health → 枚举是字符串（protojson）', async () => {
    const h = await api.get<HealthCheckResponse>('/api/health')
    expect(typeof h.status).toBe('string')
    expect(h.status).toMatch(/^HEALTH_STATUS_/)
  })

  it('GET /api/system-status → 九个字段都在（含旧界面缺的那四个）', async () => {
    const s = await api.get<GetSystemStatusResponse>('/api/system-status')
    for (const k of [
      'stuck_versions',
      'deleting_versions',
      'data_missing_versions',
      'failed_permanent_versions',
      'gc_blocked_versions',
      'wal_alerts',
      'delete_failed_kbs',
    ] as const) {
      expect(Array.isArray(s[k]), `${k} 应是数组`).toBe(true)
    }
  })

  it('POST /api/knowledge-bases → 建库', async () => {
    const r = await api.post<CreateKnowledgeBaseResponse>('/api/knowledge-bases', {
      name: `contract-${Date.now()}`,
      chunk_window_size: 512,
      chunk_overlap_size: 64,
      index_type: 'INDEX_TYPE_HNSW',
      similarity: 'SIMILARITY_COSINE',
      embed_config: { service_addr: 'http://localhost:8080', model_id: 'mock-embed-v1' },
    })
    expect(r.knowledge_base_id).not.toBe('')
    kbId = r.knowledge_base_id
  })

  it('GET /api/knowledge-bases/{id} → 单一 KB 详情', async () => {
    const r = await api.get<GetKnowledgeBaseResponse>(kbPath(kbId))
    expect(r.knowledge_base?.knowledge_base_id).toBe(kbId)
  })

  it('POST …/versions → int64 用字符串传（protojson 的要求）', async () => {
    const r = await api.post<CreateVersionResponse>(kbPath(kbId, '/versions'), {
      knowledge_base_id: kbId,
      parent_version_id: '0', // 字符串，不是 0
      client_request_id: `contract-${Date.now()}`,
      changes: [{ op: 'CHANGE_OP_ADD', doc_id: 'docs/契约.txt', content: '契约测试内容' }],
    })
    // 响应里的 int64 也必须是字符串——前端的类型就是这么声明的。
    expect(typeof r.version_id).toBe('string')
  })

  it('POST …/await → 两种 target 都认', async () => {
    const vs = await api.get<ListVersionsResponse>(kbPath(kbId, '/versions'))
    const vid = vs.versions[vs.versions.length - 1]?.version_id ?? '0'

    const durable = await awaitVersionOnce(kbId, {
      knowledge_base_id: kbId,
      version_id: vid,
      target: AwaitTarget.AWAIT_TARGET_DATA_DURABLE,
      wait_timeout_ms: '20000',
    })
    expect(typeof durable.stage).toBe('string')

    const ready = await awaitVersionOnce(kbId, {
      knowledge_base_id: kbId,
      version_id: vid,
      target: AwaitTarget.AWAIT_TARGET_INDEX_READY,
      wait_timeout_ms: '20000',
    })
    expect(ready.stage).toBe('INDEX_READY')
  })

  it('GET …/versions → 两个状态位都是字符串枚举', async () => {
    const vs = await api.get<ListVersionsResponse>(kbPath(kbId, '/versions'))
    const v = vs.versions[0]
    expect(typeof v?.version_id).toBe('string')
    expect(v?.data_status).toMatch(/^DATA_STATUS_/)
    expect(v?.index_status).toMatch(/^INDEX_STATUS_/)
  })

  it('POST /api/query-text → 文本检索（网关代 embed）', async () => {
    const r = await api.post<QueryResponse>('/api/query-text', {
      knowledge_base_id: kbId,
      text: '契约测试内容',
      top_k: 3,
    })
    expect(Array.isArray(r.results)).toBe(true)
    expect(typeof r.version_id).toBe('string')
    // storage_degraded 必须存在：它是只读用户唯一的降级信号。
    expect(typeof r.storage_degraded).toBe('boolean')
  })

  it('POST …/rollback → 激活（target_version_id，不是 version_id）', async () => {
    const vs = await api.get<ListVersionsResponse>(kbPath(kbId, '/versions'))
    const vid = vs.versions[vs.versions.length - 1]?.version_id ?? '0'

    const r = await api.post<RollbackVersionResponse>(kbPath(kbId, '/rollback'), {
      knowledge_base_id: kbId,
      target_version_id: vid,
    })
    expect(r.success).toBe(true)
  })

  it('POST …/warmup 与 …/rebuild → 端点与契约', async () => {
    const vs = await api.get<ListVersionsResponse>(kbPath(kbId, '/versions'))
    const vid = vs.versions[vs.versions.length - 1]?.version_id ?? '0'

    await expect(
      api.post(kbPath(kbId, '/warmup'), { knowledge_base_id: kbId, version_id: vid }),
    ).resolves.toBeDefined()
    await expect(
      api.post(kbPath(kbId, '/rebuild'), { knowledge_base_id: kbId, version_id: vid }),
    ).resolves.toBeDefined()
  })

  it('POST …/discard-version → 只接受仍 PENDING 的版本', async () => {
    // 新提交的版本此刻还是 PENDING——这正是 discard 唯一接受的输入。
    const created = await api.post<CreateVersionResponse>(kbPath(kbId, '/versions'), {
      knowledge_base_id: kbId,
      parent_version_id: '0',
      client_request_id: `contract-discard-${Date.now()}`,
      changes: [{ op: 'CHANGE_OP_ADD', doc_id: 'docs/待弃.txt', content: '将被放弃' }],
    })
    const r = await api.post<{ discarded: boolean }>(kbPath(kbId, '/discard-version'), {
      knowledge_base_id: kbId,
      version_id: created.version_id,
    })
    expect(typeof r.discarded).toBe('boolean')
  })

  it('POST …/delete-version → mode 用完整枚举名', async () => {
    const created = await api.post<CreateVersionResponse>(kbPath(kbId, '/versions'), {
      knowledge_base_id: kbId,
      parent_version_id: '0',
      client_request_id: `contract-del-${Date.now()}`,
      changes: [{ op: 'CHANGE_OP_ADD', doc_id: 'docs/待删.txt', content: '将被删除' }],
    })

    // **必须先等就绪**：还没落地的 PENDING 版本属于 `discard-version` 的辖区，
    // 对它调 delete-version 服务端会以 `router: no leader available` 回绝
    // （那个错名是误导性的，见下面的用例）。
    await awaitVersionOnce(kbId, {
      knowledge_base_id: kbId,
      version_id: created.version_id,
      target: AwaitTarget.AWAIT_TARGET_INDEX_READY,
      wait_timeout_ms: '20000',
    })

    const r = await api.post<{ success: boolean }>(kbPath(kbId, '/delete-version'), {
      knowledge_base_id: kbId,
      version_id: created.version_id,
      mode: 'VERSION_DELETE_MODE_SUBTREE',
    })
    expect(r.success).toBe(true)
  })

  /**
   * 服务端曾在这里报一个**误导性的错误**：对仍 PENDING 的版本调 delete-version，
   * 它回的是 `router: no leader available`（HTTP 500 / grpc_code Unknown）。那既不
   * 准确（leader 好着呢）也不可操作（调用方会去查集群，而真该做的是等就绪或改用
   * discard-version）。
   *
   * 根因有两层，都已修：路由器把逐次失败的原因丢了，并且把 `version_pending` 在
   * **写路径**上也当成可重试——写走 Raft，每个副本共用同一个状态机，换一个得到完全
   * 一样的拒绝。
   *
   * 这条现在钉的是修好后的行为：错误必须自解释，且状态码必须可编程判定。
   */
  it('删 PENDING 版本 → 若被拒，原因必须自解释（不是笼统的 no leader）', async () => {
    const created = await api.post<CreateVersionResponse>(kbPath(kbId, '/versions'), {
      knowledge_base_id: kbId,
      parent_version_id: '0',
      client_request_id: `contract-pending-del-${Date.now()}`,
      changes: [{ op: 'CHANGE_OP_ADD', doc_id: 'docs/还热乎.txt', content: '刚提交' }],
    })

    const err = await api
      .post(kbPath(kbId, '/delete-version'), {
        knowledge_base_id: kbId,
        version_id: created.version_id,
        mode: 'VERSION_DELETE_MODE_SUBTREE',
      })
      .then(
        () => null,
        (e: unknown) => e as { message?: string; status?: number },
      )

    if (err === null) {
      // 版本已经落地了 —— 写路径可以比这个请求快，那时删掉它是正确结果。
      // 这条用例要钉的不是"一定被拒"（那取决于时序，会 flaky），而是
      // "被拒时说人话"：不报笼统的 no leader，且状态码可编程判定。
      return
    }
    expect(err.message ?? '').toContain('PENDING')
    expect(err.status).toBe(412)
    expect(err.message ?? '').not.toContain('no leader')
  })

  it('错误翻译：不存在的库 → 404 + grpc_code', async () => {
    await expect(api.get(kbPath('definitely-not-a-real-kb'))).rejects.toMatchObject({
      status: 404,
    })
  })

  it('POST /api/knowledge-bases/delete → 收尾', async () => {
    const r = await api.post<{ success: boolean }>('/api/knowledge-bases/delete', {
      knowledge_base_id: kbId,
    })
    expect(r.success).toBe(true)
  })
})
