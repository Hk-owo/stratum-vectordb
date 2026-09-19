/**
 * 运维页的渲染冒烟测试。
 *
 * 为什么用 `renderToString` 而不是浏览器环境：这个文件的目的是"页面在拿到数据后
 * 能把它渲染出来"，而不是交互——交互（二次确认、日志展开）要 DOM 与事件，那需要
 * 引入 testing-library 与 jsdom，为这一页不值当。而**首屏白屏**这类错误（派生出的
 * 目标节点是 null、某个可选字段被当成必有）在服务端渲染里一样会抛。
 *
 * 数据全部预置进 query cache（而不是打网关）：这样断言的是渲染逻辑，不依赖后端，
 * 也不受集群当前状态影响。
 */
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { renderToString } from 'react-dom/server'
import { describe, expect, it } from 'vitest'

import { opsQk, type DockerClusterConfig, type DockerStatus, type NodeStatus, type NodesResponse, type OpsConfig } from '../api/ops'
import { DockerDisabledCard, Ops } from './Ops'

const LOCAL_ID = 1

function nodes(): NodesResponse {
  return {
    local_node_id: LOCAL_ID,
    nodes: [
      { id: LOCAL_ID, gateway_addr: 'http://127.0.0.1:8081', local: true, online: true },
      { id: 2, gateway_addr: 'http://127.0.0.1:8082', local: false, online: false },
    ],
  }
}

function services(): NodeStatus {
  return {
    node_id: LOCAL_ID,
    services: [
      { service: 'vecstore', running: true, pid: 1234, started_at: '2026-01-01T00:00:00Z', log_file: '/tmp/vecstore.log' },
      { service: 'embed', running: false, log_file: '/tmp/embed.log' },
      { service: 'stratum', running: false, log_file: '/tmp/stratum.log', last_error: 'process exited' },
    ],
  }
}

function opsConfig(): OpsConfig {
  return {
    node_id: LOCAL_ID,
    bin_dir: 'run/bin',
    log_dir: 'run/log',
    config_dir: 'run/configs',
    cluster: [{ id: LOCAL_ID, gateway_addr: 'http://127.0.0.1:8081' }],
    docker: dockerConfig(),
    services: {
      vecstore: { bin: 'vecstore_server', grpc_addr: '127.0.0.1:7100', rocksdb_path: 'run/data/rocks', health_addr: '127.0.0.1:7101' },
      embed: { bin: 'mock-embed', service_addr: 'http://localhost:8080' },
      stratum: {
        bin: 'stratum',
        node_id: LOCAL_ID,
        data_dir: 'run/data/stratum',
        grpc_addr: '127.0.0.1:7000',
        raft_addr: '0.0.0.0:8000',
        peers: [{ id: LOCAL_ID, addr: 'localhost:8000', service_addr: '127.0.0.1:7000' }],
        vecstore_addr: '127.0.0.1:7100',
        embed_addr: 'http://localhost:8080',
        heartbeat_interval_ms: 200,
      },
    },
  }
}

function dockerConfig(): DockerClusterConfig {
  return {
    enabled: true,
    topology: 'two-tier',
    script: 'scripts/cluster.sh',
    script_two_tier: 'scripts/cluster.sh',
    nodes: 2,
    storage_nodes: 1,
    base_port: 17000,
    storage_base_port: 17100,
    network: 'stratum-net',
    image: 'stratum-storage:latest',
    container_prefix: 'stratum-node',
    with_embed: true,
  }
}

function dockerStatus(): DockerStatus {
  return {
    network: 'stratum-net',
    topology: 'two-tier',
    count: 3,
    control_count: 2,
    storage_count: 1,
    base_port: 17000,
    storage_base_port: 17100,
    image: 'stratum-storage:latest',
    nodes: [
      { id: 1, name: 'stratum-control1', tier: 'control', status: 'running', health: 'healthy', grpc_port: 17000, leader: true },
      { id: 2, name: 'stratum-control2', tier: 'control', status: 'exited', health: '-', grpc_port: 17001, leader: false },
      { id: 3, name: 'stratum-storage1', tier: 'storage', status: 'absent', health: '-', grpc_port: 17100, leader: false },
    ],
  }
}

function render(client: QueryClient): string {
  return renderToString(
    <QueryClientProvider client={client}>
      <Ops />
    </QueryClientProvider>,
  )
}

function emptyClient(): QueryClient {
  return new QueryClient({ defaultOptions: { queries: { retry: false } } })
}

describe('Ops 渲染', () => {
  it('数据未到时不抛错（首屏）', () => {
    const html = render(emptyClient())
    expect(html).toContain('集群节点')
  })

  it('拿到数据后渲染服务、集群节点与 docker 分组', () => {
    const qc = emptyClient()
    qc.setQueryData(opsQk.nodes, nodes())
    qc.setQueryData(opsQk.services(LOCAL_ID), services())
    qc.setQueryData(opsQk.config(LOCAL_ID), opsConfig())
    qc.setQueryData(opsQk.dockerStatus, dockerStatus())
    qc.setQueryData(opsQk.dockerConfig, dockerConfig())

    const html = render(qc)

    // 节点表：本机与远端都在，离线要标出来。
    // （`node 1` 这种连文本别断言：React 会在相邻文本节点间插 <!-- --> 注释。）
    expect(html).toContain('stratum-control1')
    expect(html).toContain('http://127.0.0.1:8082')
    expect(html).toContain('离线')

    // 服务：三个服务的状态各自不同，尤其"起过又退出"必须带出原因。
    expect(html).toContain('vecstore')
    expect(html).toContain('运行中')
    expect(html).toContain('未运行')
    expect(html).toContain('process exited')
    // 运行中的服务给的是"停止/重启"，没起过的给"启动"——两者不能都出现成同一种。
    expect(html).toContain('全部启动')

    // 启动参数：表单要有值（不是空框）。
    expect(html).toContain('run/bin')
    expect(html).toContain('0.0.0.0:8000')

    // docker：两层拓扑必须分两组，且节点各自落到自己那层。
    expect(html).toContain('控制层（Raft 元数据，不存数据）')
    expect(html).toContain('存储层（文档 / 向量 / 索引）')
    expect(html).toContain('stratum-storage1')
    expect(html).toContain('两层')
  })

  it('docker 读配置失败时只提供"启用"，不假装有表单', () => {
    // 这条分支没法用预置 query cache 的方式命中（SSR 不发请求，失败态无处安放），
    // 而它恰好是最容易写错的一条：看着像"表单禁用"，实际是"根本没有表单"。
    const html = renderToString(
      <DockerDisabledCard
        error={new Error('docker 集群管理未启用：请在 ops config 中设置 docker.enabled=true')}
        busy={false}
        note={null}
        onEnable={() => {}}
      />,
    )
    expect(html).toContain('未启用')
    expect(html).toContain('启用 docker 集群管理')
    // 网关写的那句原因要原样带出来，否则用户不知道该改哪个字段。
    expect(html).toContain('docker.enabled=true')
    // 没有表单：那一栏的字段不该出现。
    expect(html).not.toContain('container_prefix')
  })
})
