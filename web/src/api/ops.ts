/**
 * 运维控制面的 API 层：`/ops/*`（cmd/stratum-gateway 的 ops.go / ops_config.go）。
 *
 * 它与 `src/api/` 下那套 REST 面有三点不同，写代码时要记住：
 *
 *   1. **不是 protojson**。`/ops/*` 的响应来自网关手写的 map / Go 结构体
 *      （`writeOpsJSON`），字段名按 Go 的 json tag 来。所以这里的类型是手抄的：
 *      没有 .proto 可生成，也就没有编译期保护——改网关的 tag 时要记得改这里
 *      （`ops_test.go` 与 `docker_ops_test.go` 钉住的是网关那一侧）。
 *   2. **错误体没有 `grpc_code`**。`writeOpsError` 只回 `{"error": <文本>}`，所以
 *      client.ts 的 ApiError 在这里恒为 `'Unknown'`：判定只能落在 HTTP 状态或
 *      message 上。别写 `err.grpcCode === 'Unavailable'`——它对这里永远不成立。
 *   3. **操作对象是节点**，不是知识库。路径有两种形状，语义相同：
 *        本节点直连：`/ops/status`、`/ops/start`、`/ops/logs/{service}`、`/ops/config`
 *        任意节点：`/ops/nodes/{id}/…`（网关转给该节点的 gateway；`id` 是本机时
 *        网关内部分发、不产生网络往返——见 ops.go 的 forwardNode）
 *      `nodeOpsPath` 就是替调用方做这个选择，别在页面里各写一份。
 *
 * docker 那部分是**集群级**的：`/ops/docker/*` 没有按节点区分的版本，从哪个网关
 * 发起都行，本机发起即可。
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { api } from './client'

// ---------------------------------------------------------------- 类型

/** 控制台能管理的三个本地服务（ops_config.go 的 ServiceID）。 */
export type ServiceId = 'vecstore' | 'embed' | 'stratum'

/** 固定顺序，与 Go 侧的 AllServices 一致：依赖在前的先起、后停。 */
export const SERVICES: readonly ServiceId[] = ['vecstore', 'embed', 'stratum']

/** supervisor.go 的 ServiceStatus。 */
export interface ServiceStatus {
  service: ServiceId
  running: boolean
  /** 仅在运行时有值（omitempty）。 */
  pid?: number
  /** RFC3339，仅在运行时有值。 */
  started_at?: string
  log_file: string
  /** 进程曾退出时的原因。 */
  last_error?: string
}

/** GET /ops/status（或 /ops/nodes/{id}/status）。 */
export interface NodeStatus {
  node_id: number
  services: ServiceStatus[]
}

/** ops.go 的 handleNodes 里的一项。 */
export interface ClusterNode {
  id: number
  gateway_addr: string
  /** 本机节点。 */
  local?: boolean
  /** 远端节点由 /ops/health 探测得出；本机恒为 true。 */
  online?: boolean
  /** 只有本机节点带这个字段（远端要另外问它的 /status）。 */
  services?: ServiceStatus[]
}

export interface NodesResponse {
  local_node_id: number
  nodes: ClusterNode[]
}

export interface VecstoreConfig {
  bin: string
  grpc_addr: string
  rocksdb_path: string
  /** 仅信息性字段：vecstore 自己不监听它。 */
  health_addr: string
  extra_args?: string
}

export interface EmbedConfig {
  bin: string
  service_addr: string
}

export interface PeerEntry {
  id: number
  addr: string
  service_addr?: string
}

/** 与 cmd/stratum 的 fileConfig 同构（外加 raft 定时），所以这里能改的就是
 *  "启动时真正生效"的全部参数。 */
export interface StratumConfig {
  bin: string
  node_id: number
  data_dir: string
  grpc_addr: string
  raft_addr: string
  peers: PeerEntry[]
  vecstore_addr: string
  embed_addr: string
  heartbeat_interval_ms?: number
  election_timeout_min_ms?: number
  election_timeout_max_ms?: number
  index_lru_capacity?: number
  index_load_wait_timeout_ms?: number
  index_callback_max_retries?: number
  index_callback_retry_base_interval_ms?: number
  write_max_retries?: number
  write_retry_base_interval_ms?: number
  delete_max_retries?: number
  delete_retry_base_interval_ms?: number
}

export interface ServiceConfigs {
  vecstore: VecstoreConfig
  embed: EmbedConfig
  stratum: StratumConfig
}

export interface DockerClusterConfig {
  enabled: boolean
  /** '' / 'single' = 单层；'two-tier' = 控制层 + 存储层。 */
  topology: string
  script: string
  script_two_tier: string
  nodes: number
  storage_nodes: number
  base_port: number
  storage_base_port: number
  network: string
  image: string
  container_prefix: string
  with_embed: boolean
}

/** GET/PUT /ops/config：run/console.yaml 的完整镜像。 */
export interface OpsConfig {
  node_id: number
  bin_dir: string
  log_dir: string
  config_dir: string
  cluster: ClusterNode[]
  docker: DockerClusterConfig
  services: ServiceConfigs
}

/** GET /ops/logs/{service}：本地日志文件的一段尾巴。 */
export interface ServiceLogs {
  service: ServiceId
  log_file: string
  /** 已经是行数组，不是一整段文本（网关侧 tailFile 切的）。 */
  lines: string[]
  truncated: boolean
}

/** 服务动作的逐个结果：一个失败不该让其余的看起来没发生。 */
export interface ServiceActionResult {
  ok: boolean
  results: Array<{ service: string; error?: string }>
}

export interface LifecycleResult {
  ok: boolean
  /** 编排脚本（docker-cluster*.sh）的输出，原样带回。 */
  output: string
}

export interface SaveConfigResult {
  ok: boolean
  note?: string
  docker?: DockerClusterConfig
}

/** GET /ops/docker/status：docker-cluster*.sh `status --json` 的原样透传。 */
export interface DockerNode {
  id: number
  name: string
  /** 只有两层拓扑有（'control' / 'storage'）。 */
  tier?: string
  role?: string
  status: string
  health: string
  grpc_port: number
  leader: boolean
}

export interface DockerStatus {
  network: string
  /** 只有两层拓扑带这个字段；单层视为 'single'。 */
  topology?: string
  count: number
  control_count?: number
  storage_count?: number
  base_port: number
  control_base_port?: number
  storage_base_port?: number
  image: string
  nodes: DockerNode[]
}

/** GET /ops/docker/logs/{id}：一整段文本，不是行数组。 */
export interface DockerNodeLogs {
  id: number
  lines: number
  log: string
}

// ---------------------------------------------------------------- 路径

/**
 * 把"节点 + 尾巴"编成一条 /ops 路径。
 *
 * 本机走直连（少一跳、错误也少一层包装）；其余节点走 `/ops/nodes/{id}/…`，
 * 由网关转发到那个节点的 gateway。`localNodeId` 为 null（节点列表还没拉到）时
 * 一律按远端拼——拼成 `/ops/nodes/{id}/*` 对任何 id 都是合法请求，而错判成本机
 * 会真的发错地方。
 */
export function nodeOpsPath(nodeId: number, localNodeId: number | null, tail = ''): string {
  const base = localNodeId !== null && nodeId === localNodeId ? '/ops' : `/ops/nodes/${nodeId}`
  return base + tail
}

// ---------------------------------------------------------------- 状态文案

/** '' = 中性。四种色调与 index.css 的 .chip 一致。 */
export type Tone = '' | 'ok' | 'warn' | 'bad'

const SERVICE_ERROR_TONE: Tone = 'bad'

/**
 * 服务状态 → 徽章。
 *
 * `running === false` 时**不能**显示成"已停止"：同一个 false 既可能是"从没起过"，
 * 也可能是"起过但进程没了"（`last_error` 会说明）。把后者显示成"已停止"会让人
 * 以为是自己停的，从而不去看日志。
 */
export function serviceBadge(s: ServiceStatus): { label: string; tone: Tone } {
  if (s.running) return { label: '运行中', tone: 'ok' }
  if (s.last_error !== undefined && s.last_error !== '') {
    return { label: `已退出（${s.last_error}）`, tone: SERVICE_ERROR_TONE }
  }
  return { label: '未运行', tone: '' }
}

const CONTAINER_STATUS: Record<string, { label: string; tone: Tone }> = {
  running: { label: '运行中', tone: 'ok' },
  restarting: { label: '重启中', tone: 'warn' },
  created: { label: '已创建', tone: '' },
  paused: { label: '已暂停', tone: 'warn' },
  exited: { label: '已退出', tone: '' },
  removing: { label: '删除中', tone: 'warn' },
  dead: { label: '已死', tone: 'bad' },
  absent: { label: '不存在', tone: '' },
}

const CONTAINER_HEALTH: Record<string, { label: string; tone: Tone }> = {
  healthy: { label: '健康', tone: 'ok' },
  starting: { label: '启动中', tone: 'warn' },
  unhealthy: { label: '异常', tone: 'bad' },
  // 容器没定义 healthcheck（脚本 inspect 的兜底输出）。
  none: { label: '无检查', tone: '' },
  '-': { label: '—', tone: '' },
}

/** docker inspect 的 State.Status → 徽章。未知取值原样显示，不吞掉。 */
export function containerStatusBadge(status: string): { label: string; tone: Tone } {
  return CONTAINER_STATUS[status] ?? { label: status, tone: '' }
}

/** docker inspect 的 Health.Status → 徽章。 */
export function containerHealthBadge(health: string): { label: string; tone: Tone } {
  return CONTAINER_HEALTH[health] ?? { label: health, tone: '' }
}

/**
 * 集群节点分组。
 *
 * 两层拓扑里控制节点与存储节点**不可互换**（一个不持数据、一个不持元数据），
 * 所以必须分两组展示：排成一张表会让人以为读请求可以发给任意一个。单层拓扑只有
 * 一个组，不加多余的标题。
 */
export function dockerGroups(
  st: DockerStatus,
): Array<{ tier: string | null; title: string | null; nodes: DockerNode[] }> {
  if (st.topology !== 'two-tier') return [{ tier: null, title: null, nodes: st.nodes }]
  return [
    { tier: 'control', title: '控制层（Raft 元数据，不存数据）', nodes: [] },
    { tier: 'storage', title: '存储层（文档 / 向量 / 索引）', nodes: [] },
  ].map((g) => ({ ...g, nodes: st.nodes.filter((n) => n.tier === g.tier) }))
}

// ---------------------------------------------------------------- query keys

/** 按"节点 + 资源"组织，好让一次操作精确命中要刷新的那几项。 */
export const opsQk = {
  nodes: ['ops', 'nodes'] as const,
  services: (nodeId: number) => ['ops', nodeId, 'services'] as const,
  config: (nodeId: number) => ['ops', nodeId, 'config'] as const,
  logs: (nodeId: number, service: ServiceId, lines: number) =>
    ['ops', nodeId, 'logs', service, lines] as const,
  dockerStatus: ['ops', 'docker', 'status'] as const,
  dockerConfig: ['ops', 'docker', 'config'] as const,
  dockerLogs: (id: number, lines: number) => ['ops', 'docker', 'logs', id, lines] as const,
}

// ---------------------------------------------------------------- 读

/**
 * 集群节点列表（本机 + 配置里的远端）。
 *
 * 它会**逐台探测**远端网关（`/ops/health`，3s 超时），所以别把间隔调得太小：
 * 探测是串行的，节点一多就会把请求叠住。
 */
export function useOpsNodes(refetchIntervalMs = 5000) {
  return useQuery({
    queryKey: opsQk.nodes,
    queryFn: () => api.get<NodesResponse>('/ops/nodes'),
    refetchInterval: refetchIntervalMs,
    retry: false,
  })
}

export function useNodeServices(
  nodeId: number | null,
  localNodeId: number | null,
  refetchIntervalMs = 5000,
) {
  return useQuery({
    queryKey: opsQk.services(nodeId ?? -1),
    queryFn: () => api.get<NodeStatus>(nodeOpsPath(nodeId as number, localNodeId, '/status')),
    enabled: nodeId !== null,
    refetchInterval: refetchIntervalMs,
    retry: false,
  })
}

export function useNodeConfig(nodeId: number | null, localNodeId: number | null) {
  return useQuery({
    queryKey: opsQk.config(nodeId ?? -1),
    queryFn: () => api.get<OpsConfig>(nodeOpsPath(nodeId as number, localNodeId, '/config')),
    enabled: nodeId !== null,
    retry: false,
  })
}

export function useNodeLogs(
  nodeId: number | null,
  localNodeId: number | null,
  service: ServiceId | null,
  lines: number,
) {
  return useQuery({
    queryKey: opsQk.logs(nodeId ?? -1, service ?? 'vecstore', lines),
    queryFn: () =>
      api.get<ServiceLogs>(
        nodeOpsPath(nodeId as number, localNodeId, `/logs/${service as ServiceId}`) +
          `?lines=${lines}`,
      ),
    enabled: nodeId !== null && service !== null,
    retry: false,
  })
}

export function useDockerStatus(refetchIntervalMs = 5000) {
  return useQuery({
    queryKey: opsQk.dockerStatus,
    queryFn: () => api.get<DockerStatus>('/ops/docker/status'),
    refetchInterval: refetchIntervalMs,
    retry: false,
  })
}

export function useDockerConfig() {
  return useQuery({
    queryKey: opsQk.dockerConfig,
    queryFn: () => api.get<DockerClusterConfig>('/ops/docker/config'),
    retry: false,
  })
}

/** docker 节点日志用 query 而不是 mutation：它是"看当前状态"，换个节点就是换一份数据。 */
export function useDockerNodeLogs(nodeId: number | null, lines: number) {
  return useQuery({
    queryKey: opsQk.dockerLogs(nodeId ?? -1, lines),
    queryFn: () => api.get<DockerNodeLogs>(`/ops/docker/logs/${nodeId as number}?lines=${lines}`),
    enabled: nodeId !== null,
    retry: false,
  })
}

// ---------------------------------------------------------------- 写

/**
 * 启停一个节点上的服务。`services: []` 在服务端解释为"全部"（opsServicesFromBody）。
 *
 * 成功后只刷新该节点的服务状态：docker 与其它节点与此无关。
 */
export function useServiceAction(nodeId: number | null, localNodeId: number | null) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (req: { action: 'start' | 'stop' | 'restart'; services: ServiceId[] }) =>
      api.post<ServiceActionResult>(
        nodeOpsPath(nodeId as number, localNodeId, `/${req.action}`),
        { services: req.services },
      ),
    onSuccess: () => {
      if (nodeId !== null) void qc.invalidateQueries({ queryKey: opsQk.services(nodeId) })
    },
  })
}

/** 保存启动参数。正在跑的服务不受影响——参数在下次启动/重启时才生效。 */
export function useSaveNodeConfig(nodeId: number | null, localNodeId: number | null) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (cfg: OpsConfig) =>
      api.put<SaveConfigResult>(nodeOpsPath(nodeId as number, localNodeId, '/config'), cfg),
    onSuccess: () => {
      if (nodeId !== null) {
        void qc.invalidateQueries({ queryKey: opsQk.config(nodeId) })
        void qc.invalidateQueries({ queryKey: opsQk.services(nodeId) })
      }
    },
  })
}

/** 集群级生命周期：up（幂等）/ up --force（按当前参数重建）/ down / clean。 */
export function useDockerLifecycle() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (req: { action: 'up' | 'down' | 'clean'; force?: boolean }) =>
      api.post<LifecycleResult>(
        `/ops/docker/${req.action}`,
        req.action === 'up' ? { force: req.force === true } : {},
      ),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: opsQk.dockerStatus })
      void qc.invalidateQueries({ queryKey: opsQk.dockerConfig })
    },
  })
}

export function useDockerNodeAction() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (req: { id: number; action: 'start' | 'stop' | 'restart' }) =>
      api.post<LifecycleResult>(`/ops/docker/nodes/${req.id}/${req.action}`, {}),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: opsQk.dockerStatus })
    },
  })
}

export function useSaveDockerConfig() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (cfg: DockerClusterConfig) =>
      api.put<SaveConfigResult>('/ops/docker/config', cfg),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: opsQk.dockerConfig })
      void qc.invalidateQueries({ queryKey: opsQk.dockerStatus })
    },
  })
}

/**
 * 打开 docker 集群管理。
 *
 * 只有它被关掉时才需要这个专用入口：关掉时 `/ops/docker/*` **连读配置都拒**
 * （ops.go 的 dockerCfg 对整组路由一视同仁），页面上因此拿不到任何可回填的配置，
 * 只剩"把它打开"这一件事可提交。而 `PUT /ops/docker/config` 恰好不做这个检查
 * （它只按 patch 里的字段覆盖），所以一个只带 `enabled` 的 body 就能启用。
 */
export function useEnableDocker() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: () => api.put<SaveConfigResult>('/ops/docker/config', { enabled: true }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: opsQk.dockerConfig })
      void qc.invalidateQueries({ queryKey: opsQk.dockerStatus })
    },
  })
}
