/**
 * TanStack Query 的 API 层：把网关的 REST 面（cmd/stratum-gateway）包成 hooks。
 *
 * 类型全部来自 src/api/gen/，而那份是用 `--ts_proto_opt=snakeToCamel=false`
 * 生成的——字段名与网关的 protojson（UseProtoNames: true）逐字对应。不要手写
 * 这里的请求/响应类型：手写的那份会在 proto 变更时静默漂移。
 *
 * 唯一例外是 QueryTextRequest：`POST /api/query-text` 是本项目新增的薄端点，
 * 它不在任何 .proto 里（见下面的注释）。
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { api, kbPath } from './client'
import type { GetSystemStatusResponse, HealthCheckResponse } from './gen/admin'
import type {
  CreateKnowledgeBaseRequest,
  CreateKnowledgeBaseResponse,
  DeleteKnowledgeBaseRequest,
  DeleteKnowledgeBaseResponse,
  GetKnowledgeBaseResponse,
  ListKnowledgeBasesResponse,
  ListVersionsResponse,
  AwaitVersionRequest,
  AwaitVersionResponse,
  CreateVersionRequest,
  CreateVersionResponse,
  DeleteVersionRequest,
  DeleteVersionResponse,
  DiscardVersionRequest,
  DiscardVersionResponse,
  RollbackVersionRequest,
  RollbackVersionResponse,
} from './gen/knowledgebase'
import type { RebuildIndexRequest, RebuildIndexResponse, WarmupVersionRequest, WarmupVersionResponse } from './gen/admin'
import type { QueryRequest, QueryResponse } from './gen/query'

/** 按实体+参数组织，以便 invalidate 精确命中。 */
export const qk = {
  health: ['health'] as const,
  systemStatus: ['system-status'] as const,
  knowledgeBases: ['knowledge-bases'] as const,
  knowledgeBase: (kbId: string) => ['knowledge-bases', kbId] as const,
  versions: (kbId: string) => ['knowledge-bases', kbId, 'versions'] as const,
}

// ---------------------------------------------------------------- 读

/**
 * 健康检查。refetchIntervalMs 传 0/undefined 即不轮询。
 *
 * retry: false —— 健康检查失败就是失败，重试只会让"网关挂了"这件事晚几秒
 * 才显示出来。
 */
export function useHealth(refetchIntervalMs?: number) {
  return useQuery({
    queryKey: qk.health,
    queryFn: () => api.get<HealthCheckResponse>('/api/health'),
    refetchInterval: refetchIntervalMs,
    retry: false,
  })
}

export function useSystemStatus() {
  return useQuery({
    queryKey: qk.systemStatus,
    queryFn: () => api.get<GetSystemStatusResponse>('/api/system-status'),
  })
}

export function useKnowledgeBases() {
  return useQuery({
    queryKey: qk.knowledgeBases,
    queryFn: () => api.get<ListKnowledgeBasesResponse>('/api/knowledge-bases'),
  })
}

/** kbId 为 null 时不发请求（未选库）。 */
export function useKnowledgeBase(kbId: string | null) {
  return useQuery({
    queryKey: qk.knowledgeBase(kbId ?? ''),
    queryFn: () => api.get<GetKnowledgeBaseResponse>(kbPath(kbId as string)),
    enabled: kbId !== null,
  })
}

export function useVersions(kbId: string | null) {
  return useQuery({
    queryKey: qk.versions(kbId ?? ''),
    queryFn: () => api.get<ListVersionsResponse>(kbPath(kbId as string, '/versions')),
    enabled: kbId !== null,
  })
}

// ---------------------------------------------------------------- 写

export function useCreateKnowledgeBase() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (req: CreateKnowledgeBaseRequest) =>
      api.post<CreateKnowledgeBaseResponse>('/api/knowledge-bases', req),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: qk.knowledgeBases })
    },
  })
}

/** 删库是异步清理：成功只是"受理了"，不是"数据没了"。 */
export function useDeleteKnowledgeBase() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (req: DeleteKnowledgeBaseRequest) =>
      api.post<DeleteKnowledgeBaseResponse>('/api/knowledge-bases/delete', req),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: qk.knowledgeBases })
    },
  })
}

/**
 * CreateVersion：提交一批增量 changes，得到一个 PENDING 版本。
 *
 * 本 hook 只发一次。幂等键的持久化、以及"这一次失败该重发还是放弃"的判定
 * 属于 src/pending/ 那一层——服务端刻意不替调用方做这个决定。
 *
 * 注意 empty_changes 会被拒（400 InvalidArgument）：changes 是**增量**，
 * 空数组的含义是"与父版本相同"，而那不是服务端能陈述的文档集。所以调用方
 * 必须先按内容哈希滤掉没有实际变化的文档（见 ingest/）。
 */
export function useSubmitChanges(kbId: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (req: CreateVersionRequest) =>
      api.post<CreateVersionResponse>(kbPath(kbId, '/versions'), req),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: qk.versions(kbId) })
    },
  })
}

/** 切换激活版本：发布或回滚。两个状态位都就绪的版本才该被激活。 */
export function useRollbackVersion(kbId: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (req: RollbackVersionRequest) =>
      api.post<RollbackVersionResponse>(kbPath(kbId, '/rollback'), req),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: qk.knowledgeBase(kbId) })
    },
  })
}

/** 删版本。mode 省略即 SUBTREE。活跃版本删不掉（服务端拒绝）。 */
export function useDeleteVersion(kbId: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (req: DeleteVersionRequest) =>
      api.post<DeleteVersionResponse>(kbPath(kbId, '/delete-version'), req),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: qk.versions(kbId) })
    },
  })
}

/**
 * 放弃一个**从未落地**的 PENDING 版本——DATA_MISSING 的正式出路之一
 * （另一条是按同一 client_request_id 重发）。
 * 已落地的版本会被拒（FailedPrecondition / version_not_pending），那属于
 * deleteVersion。
 */
export function useDiscardVersion(kbId: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (req: DiscardVersionRequest) =>
      api.post<DiscardVersionResponse>(kbPath(kbId, '/discard-version'), req),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: qk.versions(kbId) })
    },
  })
}

/** 重试 FAILED 版本的索引构建。对 FAILED_PERMANENT 无效——那只有运维能处置。 */
export function useRebuildIndex(kbId: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (req: RebuildIndexRequest) =>
      api.post<RebuildIndexResponse>(kbPath(kbId, '/rebuild'), req),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: qk.versions(kbId) })
    },
  })
}

/** 预热索引进内存（不改激活版本）。产物会因此被保留策略保护。 */
export function useWarmupVersion(kbId: string) {
  return useMutation({
    mutationFn: (req: WarmupVersionRequest) =>
      api.post<WarmupVersionResponse>(kbPath(kbId, '/warmup'), req),
  })
}

// ---------------------------------------------------------------- 检索

/**
 * POST /api/query-text 的请求体。
 *
 * 这是本项目新增的薄端点，**不是 .proto 里的消息**：用户敲的是文本，而
 * /api/query 只收 vector，所以网关负责「文本 → 向量 → 转发 Query」。
 *
 * 网关用知识库自己的 embed_config（service_addr + model_id）去 embed，所以
 * 这个请求体里没有 model 相关字段——把它交给调用方只会让前端有机会传错。
 */
export interface QueryTextRequest {
  knowledge_base_id: string
  text: string
  top_k: number
  /** 省略 = 不设阈值 */
  threshold?: number
  /**
   * 省略 = 查激活版本。类型是 string 而非 number：protojson 把 int64 编成
   * JSON 字符串（见 gen/README.md），而界面上拿到的 version_id 本来就是
   * 响应里的那个字符串——原样传回来才不会出现 "12" === 12 这种恒假比较。
   */
  version_id?: string
  aggregation?: QueryRequest['aggregation']
}

/**
 * 文本检索。用 mutation 而不是 query：它是一次显式动作，参数来自输入框，
 * 把它塞进 query cache 只会让"同一次搜索"与"不同版本的结果"混淆。
 *
 * 响应是 QueryResponse，字段与 /api/query 一致——包括 storage_degraded，
 * 它是只读调用方能拿到的唯一降级信号，界面必须展示。
 */
export function useQueryText() {
  return useMutation({
    mutationFn: (req: QueryTextRequest) => api.post<QueryResponse>('/api/query-text', req),
  })
}

/**
 * 单次 AwaitVersion：问一次"这个版本到哪一步了"。
 *
 * 它不循环、不重试、不做决定。循环的节奏听响应的 retry_after_ms，而
 * "该重发还是该放弃"由 src/pending/decide.ts 判定——服务端刻意不替调用方
 * 选（docs/await-version-plan.md §4.3）。
 */
export function awaitVersionOnce(
  kbId: string,
  req: AwaitVersionRequest,
): Promise<AwaitVersionResponse> {
  return api.post<AwaitVersionResponse>(kbPath(kbId, '/await'), req)
}
