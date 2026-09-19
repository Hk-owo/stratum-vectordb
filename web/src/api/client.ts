/**
 * 网关的 HTTP/JSON 客户端（cmd/stratum-gateway）。
 *
 * 前端由网关同源提供（`-static` 指向 web/dist），所以没有 base URL，也没有 CORS。
 *
 * 两条约定来自 docs/client-integration-guide.md，写错任何一条都会静默错位：
 *
 *   §4.2 编码是 protojson，字段名用 proto 的 snake_case。这是网关侧
 *        `protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}`
 *        （cmd/stratum-gateway/main.go）定的，所以 src/api/gen/ 下的类型必须用
 *        `--ts_proto_opt=snakeToCamel=false` 生成——默认的 camelCase 与线上字段名
 *        对不上，编译期不会报错，运行期全是 undefined。
 *        EmitUnpopulated 意味着响应里字段总是存在（枚举给显式字符串，不用拿
 *        缺字段反推零值）。
 *
 *   §4.4 错误体是 {"error": <文本>, "grpc_code": <code>}。REST 侧拿不到错误的
 *        稳定名字——empty_changes / invalid_parent_version / version_not_pending
 *        都只是文本——所以判定只能落在 grpc_code 上。
 */

/** 网关的错误体形状（cmd/stratum-gateway 的 writeError）。 */
export interface GatewayErrorBody {
  error: string
  grpc_code: string
}

/** §4.4 的映射里前端会分支处理的那几个（其余按 string 收）。 */
export type GrpcCode =
  | 'InvalidArgument' // 400：empty_changes、invalid_parent_version
  | 'NotFound' // 404：version_not_found、knowledge_base_not_found
  | 'FailedPrecondition' // 412：version_pending、index_not_ready、version_is_active
  | 'Unavailable' // 503：kb_storage_degraded、storage_unavailable
  | 'Unimplemented' // 501
  | 'Internal' // 500
  | 'Unreachable' // 网关自己没起来（fetch 抛异常，无 HTTP 状态）
  | 'Unknown'

export class ApiError extends Error {
  readonly status: number
  readonly grpcCode: GrpcCode

  constructor(status: number, grpcCode: GrpcCode, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.grpcCode = grpcCode
  }

  /**
   * 存储层降级/后端不可达。界面上要单独提示，而不是当成普通失败——
   * 这是只读调用方能拿到的唯一降级信号（§4.4 的 Unavailable）。
   */
  get isUnavailable(): boolean {
    return this.grpcCode === 'Unavailable' || this.grpcCode === 'Unreachable'
  }

  /** 调用方发的东西不合法，重试无用。 */
  get isInvalidArgument(): boolean {
    return this.grpcCode === 'InvalidArgument'
  }

  /** 前置条件不满足：通常是时序问题，等一下或换个操作才有意义。 */
  get isFailedPrecondition(): boolean {
    return this.grpcCode === 'FailedPrecondition'
  }

  get isNotFound(): boolean {
    return this.grpcCode === 'NotFound'
  }
}

function toGrpcCode(raw: string | undefined): GrpcCode {
  switch (raw) {
    case 'InvalidArgument':
    case 'NotFound':
    case 'FailedPrecondition':
    case 'Unavailable':
    case 'Unimplemented':
    case 'Internal':
      return raw
    default:
      // 未知 code（含网关未回 JSON 的情况）不静默降级成某个具体语义：
      // 调用方靠 grpcCode 分支，猜错比不知道更糟。
      return 'Unknown'
  }
}

/**
 * API 基地址。
 *
 * 浏览器里前端与 gateway **同源**（gateway 的 -static 提供 dist），所以相对路径
 * 就够了，base 保持空串。
 *
 * 桌面壳里不是同源：WebView 从 `tauri://localhost` 加载，相对路径会打到应用自己
 * 身上而不是 gateway。所以桌面启动时必须显式设成 gateway 的绝对地址——而且它要
 * 可配置，因为客户可能把 gateway 跑在别的端口/机器上。
 */
let baseUrl = ''

/** 设 base（尾部斜杠会被去掉）。传空串即回到同源相对路径。 */
export function setApiBase(url: string): void {
  baseUrl = url.replace(/\/+$/, '')
}

export function apiBase(): string {
  return baseUrl
}

function withBase(path: string): string {
  return baseUrl + path
}

async function request<T>(method: 'GET' | 'POST' | 'PUT', path: string, body?: unknown): Promise<T> {
  let res: Response
  try {
    res = await fetch(withBase(path), {
      method,
      headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    })
  } catch (cause) {
    // fetch 只在网络层失败时抛（网关没起、连接被拒）。这与"网关回了错误"
    // 是两件事：前者多半是本地环境问题，后者是集群的真实答复。
    throw new ApiError(0, 'Unreachable', cause instanceof Error ? cause.message : String(cause))
  }

  if (!res.ok) {
    const errBody = (await res.json().catch(() => null)) as GatewayErrorBody | null
    throw new ApiError(
      res.status,
      toGrpcCode(errBody?.grpc_code),
      errBody?.error ?? `HTTP ${res.status}`,
    )
  }

  return (await res.json()) as T
}

export const api = {
  get: <T>(path: string): Promise<T> => request<T>('GET', path),
  post: <T>(path: string, body?: unknown): Promise<T> => request<T>('POST', path, body),
  // PUT 只在 /ops/* 用得上（保存启动参数与 docker 集群参数），/api/* 的契约
  // 里没有它：那里的写操作都是"提交一件事"，不是"替换一份配置"。
  put: <T>(path: string, body?: unknown): Promise<T> => request<T>('PUT', path, body),
}

/** 路径里的 id 一律过一遍 encodeURIComponent：KB id 是服务端生成的，但不该假设它的字符集。 */
export const kbPath = (kbId: string, suffix = ''): string =>
  `/api/knowledge-bases/${encodeURIComponent(kbId)}${suffix}`
