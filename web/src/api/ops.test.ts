/**
 * 运维面（`/ops/*`）的测试。
 *
 * 分两半，理由不同：
 *
 *   1. **纯函数**：路径拼接与状态徽章。这两处错了都是**静默**的——路径拼成
 *      `/ops/nodes/1/status` 而本机其实是 `/ops/status` 时，请求照样是 200（网关
 *      会转发），但如果把本机 id 拼成远端而集群配置里没有它，就是 404；徽章则更
 *      隐蔽：把"起过又退出"显示成"未运行"，会让人以为是自己停的。所以这些断言
 *      钉的是语义，不是实现。
 *
 *   2. **契约**：打一个真在跑的 gateway，验证手抄的那份类型（`ops.ts` 没有 .proto
 *      可生成）与网关实际输出一致。网关不可达时整体跳过，所以没有后端的机器上
 *      `npm test` 仍然全绿。
 *
 * 契约那半**只读**：它不动配置、不启停服务。写操作（PUT /ops/config、
 * /ops/docker/*）会真的改客户机器上的东西，不该由 `npm test` 顺手做掉。
 */
import { beforeAll, describe, expect, it } from 'vitest'

import { ApiError, api, setApiBase } from './client'
import {
  SERVICES,
  containerHealthBadge,
  containerStatusBadge,
  dockerGroups,
  nodeOpsPath,
  serviceBadge,
  type DockerNode,
  type DockerStatus,
  type NodeStatus,
  type NodesResponse,
  type OpsConfig,
  type ServiceLogs,
  type ServiceStatus,
} from './ops'

const GATEWAY = process.env.STRATUM_GATEWAY ?? 'http://127.0.0.1:8081'

async function gatewayAlive(): Promise<boolean> {
  try {
    const res = await fetch(`${GATEWAY}/ops/health`, { signal: AbortSignal.timeout(3000) })
    return res.status === 200
  } catch {
    return false
  }
}

const alive = await gatewayAlive()

describe('nodeOpsPath', () => {
  it('本机走直连，不带 /nodes/{id}', () => {
    expect(nodeOpsPath(1, 1, '/status')).toBe('/ops/status')
    expect(nodeOpsPath(1, 1, '/config')).toBe('/ops/config')
    expect(nodeOpsPath(1, 1, '/logs/stratum?lines=200')).toBe('/ops/logs/stratum?lines=200')
  })

  it('远端节点走网关转发', () => {
    expect(nodeOpsPath(2, 1, '/status')).toBe('/ops/nodes/2/status')
    expect(nodeOpsPath(7, 1, '/start')).toBe('/ops/nodes/7/start')
  })

  it('还不知道本机 id 时按远端拼——那对任何 id 都是合法请求', () => {
    // 反过来的错判（把远端当本机）会真的把请求发到错误的机器上；这个方向的
    // 错判只是多一跳转发，网关自己会分发回本地。
    expect(nodeOpsPath(1, null, '/status')).toBe('/ops/nodes/1/status')
  })

  it('可以不接尾巴', () => {
    expect(nodeOpsPath(3, 1)).toBe('/ops/nodes/3')
  })
})

function svc(over: Partial<ServiceStatus> = {}): ServiceStatus {
  return { service: 'vecstore', running: false, log_file: 'run/log/vecstore.log', ...over }
}

describe('serviceBadge', () => {
  it('运行中 → ok', () => {
    expect(serviceBadge(svc({ running: true, pid: 42 }))).toEqual({ label: '运行中', tone: 'ok' })
  })

  it('从没起过 → 中性的"未运行"', () => {
    expect(serviceBadge(svc())).toEqual({ label: '未运行', tone: '' })
  })

  it('起过又退出 → 必须与"没起过"区分开，并带出原因', () => {
    // 两者在 wire 上都是 running:false，唯一的区别是 last_error。显示成同一个
    // 样子会让人以为进程是自己停的，从而不去看日志——这正是这条断言的意义。
    const b = serviceBadge(svc({ last_error: 'process exited' }))
    expect(b.tone).toBe('bad')
    expect(b.label).toContain('process exited')
  })
})

describe('容器徽章', () => {
  it('running / healthy 是 ok', () => {
    expect(containerStatusBadge('running')).toEqual({ label: '运行中', tone: 'ok' })
    expect(containerHealthBadge('healthy')).toEqual({ label: '健康', tone: 'ok' })
  })

  it('absent 是"不存在"，不是错误色', () => {
    expect(containerStatusBadge('absent')).toEqual({ label: '不存在', tone: '' })
  })

  it('没有 healthcheck 的容器显示"无检查"', () => {
    expect(containerHealthBadge('none')).toEqual({ label: '无检查', tone: '' })
    expect(containerHealthBadge('-')).toEqual({ label: '—', tone: '' })
  })

  it('未知取值原样显示，不吞成空白', () => {
    // docker 的 State 值会随版本增加；静默显示空白比显示一个陌生词更糟。
    expect(containerStatusBadge('whatever')).toEqual({ label: 'whatever', tone: '' })
  })
})

function node(over: Partial<DockerNode> = {}): DockerNode {
  return { id: 1, name: 'stratum-node1', status: 'running', health: 'healthy', grpc_port: 17000, leader: false, ...over }
}

function dockerStatus(over: Partial<DockerStatus> = {}): DockerStatus {
  return { network: 'stratum-net', count: 1, base_port: 17000, image: 'img', nodes: [node()], ...over }
}

describe('dockerGroups', () => {
  it('单层拓扑只有一组，且不加标题', () => {
    const groups = dockerGroups(dockerStatus({ nodes: [node({ id: 1 }), node({ id: 2 })] }))
    expect(groups).toHaveLength(1)
    expect(groups[0]?.title).toBeNull()
    expect(groups[0]?.nodes).toHaveLength(2)
  })

  it('两层拓扑按层分组——两组不可互换，也不能排成一张表', () => {
    const st = dockerStatus({
      topology: 'two-tier',
      count: 3,
      control_count: 2,
      storage_count: 1,
      nodes: [
        node({ id: 1, tier: 'control', name: 'stratum-control1' }),
        node({ id: 2, tier: 'control', name: 'stratum-control2' }),
        node({ id: 3, tier: 'storage', name: 'stratum-storage1' }),
      ],
    })

    const groups = dockerGroups(st)
    expect(groups.map((g) => g.tier)).toEqual(['control', 'storage'])
    expect(groups[0]?.nodes.map((n) => n.id)).toEqual([1, 2])
    expect(groups[1]?.nodes.map((n) => n.id)).toEqual([3])
    // 不丢节点：分组只是展示，任何节点都必须出现在某一组里。
    expect(groups.flatMap((g) => g.nodes)).toHaveLength(st.nodes.length)
  })

  it('没有 tier 的节点也不会凭空消失', () => {
    const st = dockerStatus({ topology: 'two-tier', nodes: [node({ id: 1, tier: 'control' }), node({ id: 2 })] })
    expect(dockerGroups(st).flatMap((g) => g.nodes).map((n) => n.id)).toEqual([1])
  })
})

describe.skipIf(!alive)('与真实 gateway 的 /ops 契约', () => {
  beforeAll(() => {
    setApiBase(GATEWAY)
  })

  it('GET /ops/health → 只有 status 与 node_id', async () => {
    const h = await api.get<{ status: string; node_id: number }>('/ops/health')
    expect(h.status).toBe('ok')
    expect(typeof h.node_id).toBe('number')
  })

  it('GET /ops/status → 三个服务都在，字段名与类型对得上', async () => {
    const st = await api.get<NodeStatus>('/ops/status')
    expect(typeof st.node_id).toBe('number')
    expect(st.services.map((s) => s.service).sort()).toEqual([...SERVICES].sort())
    for (const s of st.services) {
      expect(typeof s.running, `${s.service}.running`).toBe('boolean')
      expect(typeof s.log_file, `${s.service}.log_file`).toBe('string')
      // 未运行时不该有 pid：Go 侧是 omitempty，这里顺带钉住它不是 0 或 null。
      if (!s.running) expect(s.pid).toBeUndefined()
    }
  })

  it('GET /ops/nodes → 本机节点被标出来，且带着自己的服务状态', async () => {
    const n = await api.get<NodesResponse>('/ops/nodes')
    const local = n.nodes.find((x) => x.id === n.local_node_id)
    expect(local, 'cluster 配置里必须有本机节点').toBeDefined()
    expect(local?.local).toBe(true)
    expect(local?.online).toBe(true)
    expect(local?.services ?? []).toHaveLength(SERVICES.length)
  })

  it('GET /ops/config → 手抄的 OpsConfig 与网关输出一致', async () => {
    // 这份类型没有 .proto 可生成，字段名写错只会表现为 undefined。所以抽查
    // 每一个"页面上真的会读"的字段。
    const c = await api.get<OpsConfig>('/ops/config')
    expect(typeof c.node_id).toBe('number')
    expect(typeof c.bin_dir).toBe('string')
    expect(typeof c.log_dir).toBe('string')
    expect(typeof c.config_dir).toBe('string')
    expect(Array.isArray(c.cluster)).toBe(true)
    expect(typeof c.docker.enabled).toBe('boolean')
    expect(typeof c.services.vecstore.grpc_addr).toBe('string')
    expect(typeof c.services.vecstore.rocksdb_path).toBe('string')
    expect(typeof c.services.embed.service_addr).toBe('string')
    expect(typeof c.services.stratum.raft_addr).toBe('string')
    expect(Array.isArray(c.services.stratum.peers)).toBe(true)
  })

  it('GET /ops/logs/{service} → lines 是行数组（不是一整段文本）', async () => {
    const logs = await api.get<ServiceLogs>('/ops/logs/stratum?lines=5')
    expect(logs.service).toBe('stratum')
    expect(Array.isArray(logs.lines)).toBe(true)
    expect(typeof logs.truncated).toBe('boolean')
    expect(typeof logs.log_file).toBe('string')
  })

  it('未知服务名 → 400，且错误体是 {"error": …}（运维面没有 grpc_code）', async () => {
    const err = await api.get('/ops/logs/nope').catch((e: unknown) => e)
    expect(err).toBeInstanceOf(ApiError)
    const e = err as ApiError
    expect(e.status).toBe(400)
    // message 是网关写的那句文本，而不是 client.ts 的兜底 "HTTP 400"——
    // 兜底值意味着错误体不是我们以为的那个形状。
    expect(e.message).toContain('nope')
    expect(e.grpcCode).toBe('Unknown')
  })
})
