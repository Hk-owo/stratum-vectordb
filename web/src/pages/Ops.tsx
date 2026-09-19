import { useEffect, useState } from 'react'

import { ApiError } from '../api/client'
import {
  SERVICES,
  containerHealthBadge,
  containerStatusBadge,
  dockerGroups,
  serviceBadge,
  useDockerConfig,
  useDockerLifecycle,
  useDockerNodeAction,
  useDockerNodeLogs,
  useDockerStatus,
  useEnableDocker,
  useNodeConfig,
  useNodeLogs,
  useNodeServices,
  useOpsNodes,
  useSaveDockerConfig,
  useSaveNodeConfig,
  useServiceAction,
  type ClusterNode,
  type DockerClusterConfig,
  type DockerStatus,
  type EmbedConfig,
  type OpsConfig,
  type ServiceId,
  type StratumConfig,
  type Tone,
  type VecstoreConfig,
} from '../api/ops'

/**
 * 运维页。
 *
 * 它替掉的是重写前端时被删掉的那个"运维视角"界面，但边界划得比它清楚：
 *
 *   · **一个节点选择器驱动全部节点级操作**。本机直连 `/ops/*`，远端走
 *     `/ops/nodes/{id}/*` 由网关转发（ops.go 的 forwardNode）——两条路在后端
 *     是同一个 handler，所以页面没必要为它们各写一套控件。这也正是"一台控制台
 *     能驱动整个集群"的设计本意。
 *   · **破坏性操作要二次确认**。这里最轻的操作也会踢掉一个正在服务的进程，最重的
 *     （docker clean）连数据卷一起删——而"清理"和"重启"在按钮上只有两个字号的区别。
 *   · **失败要逐个说清楚**。`/ops/start` 会返回每个服务的结果，一个失败不该让
 *     其余的看起来没发生；编排脚本（scripts/cluster.sh）的输出原样显示，
 *     因为真正的报错往往就在里面。
 *   · 参数保存**不等于生效**：正在跑的进程不换参数，服务端返回的 note 也是这么说的。
 *     界面上照抄这句话，免得有人保存完以为改完了。
 *
 * 数据面（检索/文档/版本）仍然在别的页：这一页只碰进程与配置。
 */
export function Ops() {
  const nodes = useOpsNodes()
  const [picked, setPicked] = useState<number | null>(null)

  const list = nodes.data?.nodes ?? []
  const localId = nodes.data?.local_node_id ?? null

  // 目标是**派生**的，不是同步 state：节点列表 5 秒一刷，若选中的节点被移出配置
  // （或列表还没到），这里要立刻退回本机——否则页面会继续对一台已经不存在的节点
  // 发指令，而错误只会在点击之后才出现。
  const target = list.some((n) => n.id === picked) ? picked : localId

  return (
    <div className="page">
      <p className="muted small">
        下面选中的节点决定本页所有节点级操作（服务启停、日志、启动参数）作用在哪台机器上；
        远端节点由网关转发过去。<b>Docker 集群</b>那一段是集群级配置，与所选节点无关。
      </p>

      {nodes.error !== null && (
        <p className="error-box">读取集群节点失败：{reason(nodes.error)}</p>
      )}

      <div className="status-section">
        <div className="status-head">
          <strong>集群节点</strong>
          <span className="chip">{list.length}</span>
        </div>
        {list.length === 0 && <p className="muted small">读取节点列表…</p>}
        {list.length > 0 && (
          <table className="queue-table">
            <thead>
              <tr>
                <th>节点</th>
                <th>gateway 地址</th>
                <th>可达性</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {list.map((n) => (
                <tr key={n.id} className={n.id === target ? 'row-active' : undefined}>
                  <td>
                    node {n.id} {n.local === true && <span className="chip">本机</span>}
                  </td>
                  <td>
                    <code>{n.gateway_addr}</code>
                  </td>
                  <td>
                    <Badge badge={reachabilityBadge(n)} />
                  </td>
                  <td>
                    <button
                      type="button"
                      disabled={n.id === target}
                      onClick={() => setPicked(n.id)}
                    >
                      管理
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {/* 远端节点的可达性是网关逐台探测出来的（/ops/health，3 秒超时）。说清楚，
            是因为"离线"可能只是那个节点没起，也可能是这台网关够不着它。 */}
        <p className="muted small">
          可达性由本机网关探测各节点的 <code>/ops/health</code> 得出；离线不一定是节点
          没起，也可能是这台网关到它的网络不通。
        </p>
      </div>

      <NodeServices nodeId={target} localNodeId={localId} />
      <NodeConfigPanel nodeId={target} localNodeId={localId} />
      <DockerPanel />
    </div>
  )
}

// ---------------------------------------------------------------- 服务

const LOG_LINES = 300

const ACTION_LABEL = { start: '启动', stop: '停止', restart: '重启' } as const

function NodeServices({
  nodeId,
  localNodeId,
}: {
  nodeId: number | null
  localNodeId: number | null
}) {
  const services = useNodeServices(nodeId, localNodeId)
  const act = useServiceAction(nodeId, localNodeId)
  const [logService, setLogService] = useState<ServiceId | null>(null)
  const [result, setResult] = useState<{ tone: Tone; text: string } | null>(null)

  // 换节点要收起日志与提示：那是上一个节点的文件，留着会看错机器。
  // 用"渲染中调整 state"而不是 effect：effect 要等到渲染之后才跑（先闪一帧上一个
  // 节点的日志），而 SSR 里根本不跑，那种写法在这条分支上永远只有初始态。
  const [loadedFor, setLoadedFor] = useState<number | null>(nodeId)
  if (loadedFor !== nodeId) {
    setLoadedFor(nodeId)
    setLogService(null)
    setResult(null)
  }

  async function run(action: 'start' | 'stop' | 'restart', svcs: ServiceId[]) {
    setResult(null)
    try {
      const resp = await act.mutateAsync({ action, services: svcs })
      const failed = resp.results.filter((r) => r.error !== undefined && r.error !== '')
      setResult(
        failed.length === 0
          ? { tone: '', text: `${ACTION_LABEL[action]}完成：${resp.results.map((r) => r.service).join('、')}` }
          : {
              tone: 'bad',
              text:
                `${ACTION_LABEL[action]}未全部成功：` +
                failed.map((r) => `${r.service} —— ${r.error ?? ''}`).join('；'),
            },
      )
    } catch (err) {
      setResult({ tone: 'bad', text: `${ACTION_LABEL[action]}失败：${reason(err)}` })
    }
  }

  if (nodeId === null) return null

  const nodes = services.data?.services ?? []
  const busy = act.isPending
  // 服务端少报一个服务是"它不认识这个进程"，比"未运行"严重得多：那种情况下这里
  // 连启动按钮都给不出来。所以显式说出来，而不是让它悄悄消失。
  const missing = missingServices(nodes.map((s) => s.service))

  return (
    <div className="status-section">
      <div className="status-head">
        <strong>服务</strong>
        <span className="chip">node {nodeId}</span>
        {services.isFetching && <span className="muted small">刷新中…</span>}
      </div>

      {services.error !== null && (
        <p className="error-box">读取服务状态失败：{reason(services.error)}</p>
      )}

      {services.isPending && <p className="muted small">读取服务状态…</p>}

      {missing.length > 0 && (
        <p className="notice warn">
          服务端没有报告这些服务：{missing.join('、')}
          ——它与这个前端的服务列表不一致（多半是版本不同），它们在这里无法启停。
        </p>
      )}

      {nodes.length > 0 && (
        <table className="queue-table">
          <thead>
            <tr>
              <th>服务</th>
              <th>状态</th>
              <th>PID</th>
              <th>启动于</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {nodes.map((s) => (
              <tr key={s.service}>
                <td>
                  <code>{s.service}</code>
                </td>
                <td>
                  <Badge badge={serviceBadge(s)} />
                </td>
                <td>{s.pid ?? '—'}</td>
                <td title={s.log_file}>
                  {s.started_at !== undefined && s.started_at !== '' ? s.started_at : '—'}
                </td>
                <td className="actions">
                  {s.running ? (
                    <>
                      <ConfirmButton
                        label="停止"
                        confirmLabel="确认停止"
                        disabled={busy}
                        onConfirm={() => void run('stop', [s.service])}
                      />
                      <ConfirmButton
                        label="重启"
                        confirmLabel="确认重启"
                        disabled={busy}
                        onConfirm={() => void run('restart', [s.service])}
                      />
                    </>
                  ) : (
                    <button type="button" disabled={busy} onClick={() => void run('start', [s.service])}>
                      启动
                    </button>
                  )}
                  <button
                    type="button"
                    onClick={() => setLogService((cur) => (cur === s.service ? null : s.service))}
                  >
                    {logService === s.service ? '收起日志' : '日志'}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="actions" style={{ marginTop: 10 }}>
        <button type="button" disabled={busy} onClick={() => void run('start', [])}>
          全部启动
        </button>
        <ConfirmButton
          label="全部停止"
          confirmLabel="确认停止全部"
          disabled={busy}
          onConfirm={() => void run('stop', [])}
        />
        <ConfirmButton
          label="全部重启"
          confirmLabel="确认重启全部"
          disabled={busy}
          onConfirm={() => void run('restart', [])}
        />
      </div>

      {result !== null && (
        <p className={result.tone === 'bad' ? 'error-box' : 'notice'}>{result.text}</p>
      )}

      {logService !== null && (
        <ServiceLogView nodeId={nodeId} localNodeId={localNodeId} service={logService} />
      )}
    </div>
  )
}

/** 本地服务日志：网关切好的一段尾巴（`/ops/logs/{service}`）。 */
function ServiceLogView({
  nodeId,
  localNodeId,
  service,
}: {
  nodeId: number
  localNodeId: number | null
  service: ServiceId
}) {
  const logs = useNodeLogs(nodeId, localNodeId, service, LOG_LINES)

  return (
    <div>
      <div className="queue-summary" style={{ marginTop: 10 }}>
        <span className="muted small">
          <code>{service}</code> 最后 {LOG_LINES} 行
          {logs.data !== undefined && <> · <code>{logs.data.log_file}</code></>}
          {logs.data?.truncated === true && ' · 文件更长，只取了尾部'}
        </span>
        <button type="button" disabled={logs.isFetching} onClick={() => void logs.refetch()}>
          {logs.isFetching ? '刷新中…' : '刷新'}
        </button>
      </div>
      {logs.error !== null && <p className="error-box">读取日志失败：{reason(logs.error)}</p>}
      {logs.isPending && <p className="muted small">读取日志…</p>}
      {logs.data !== undefined && <LogView text={logs.data.lines} />}
    </div>
  )
}

// ---------------------------------------------------------------- 启动参数

function NodeConfigPanel({
  nodeId,
  localNodeId,
}: {
  nodeId: number | null
  localNodeId: number | null
}) {
  const cfg = useNodeConfig(nodeId, localNodeId)
  const save = useSaveNodeConfig(nodeId, localNodeId)
  const [edited, setEdited] = useState<OpsConfig | null>(null)
  const [note, setNote] = useState<{ tone: Tone; text: string } | null>(null)

  // 换节点要丢弃草稿与提示，否则会把 A 节点的参数存到 B 节点上。
  // 同样用"渲染中调整 state"：见 NodeServices 里的理由，这里还多一条——草稿如果
  // 只在 effect 里填，首次渲染会是空的（用户看到一瞬空表单）。
  const [loadedFor, setLoadedFor] = useState<number | null>(nodeId)
  if (loadedFor !== nodeId) {
    setLoadedFor(nodeId)
    setEdited(null)
    setNote(null)
  }

  if (nodeId === null) return null

  // 草稿优先于服务端数据：轮询/重新拉取不该把用户正在编辑的内容顶掉。
  const draft = edited ?? cfg.data ?? null

  function up(patch: Partial<OpsConfig>) {
    setEdited((d) => {
      const base = d ?? cfg.data
      return base === undefined ? null : { ...base, ...patch }
    })
  }

  // 三个服务各写一个更新函数，而不是一个泛型的 `upService(key, patch)`：
  // 泛型下标展开会让 TS 退化成交叉类型，字段写错时编译期就不再报错。
  function upVecstore(patch: Partial<VecstoreConfig>) {
    setEdited((d) => {
      const base = d ?? cfg.data
      if (base === undefined) return null
      return {
        ...base,
        services: { ...base.services, vecstore: { ...base.services.vecstore, ...patch } },
      }
    })
  }
  function upEmbed(patch: Partial<EmbedConfig>) {
    setEdited((d) => {
      const base = d ?? cfg.data
      if (base === undefined) return null
      return { ...base, services: { ...base.services, embed: { ...base.services.embed, ...patch } } }
    })
  }
  function upStratum(patch: Partial<StratumConfig>) {
    setEdited((d) => {
      const base = d ?? cfg.data
      if (base === undefined) return null
      return {
        ...base,
        services: { ...base.services, stratum: { ...base.services.stratum, ...patch } },
      }
    })
  }

  async function submit() {
    if (draft === null) return
    setNote(null)
    try {
      const resp = await save.mutateAsync(draft)
      setNote({ tone: '', text: `参数已保存${resp.note !== undefined ? `：${resp.note}` : ''}` })
    } catch (err) {
      setNote({ tone: 'bad', text: `保存参数失败：${reason(err)}` })
    }
  }

  const body = (() => {
    if (cfg.isPending) return <p className="muted small">读取启动参数…</p>
    if (cfg.error !== null) {
      return <p className="error-box">读取启动参数失败：{reason(cfg.error)}</p>
    }
    if (draft === null) return null
    const s = draft.services
    return (
      <>
        <details className="advanced" open>
          <summary>路径</summary>
          <div className="advanced-grid">
            <NumField label="node_id" value={draft.node_id} onChange={(v) => up({ node_id: v })} />
            <TextField label="bin_dir" value={draft.bin_dir} onChange={(v) => up({ bin_dir: v })} />
            <TextField label="log_dir" value={draft.log_dir} onChange={(v) => up({ log_dir: v })} />
            <TextField
              label="config_dir"
              value={draft.config_dir}
              onChange={(v) => up({ config_dir: v })}
              hint="stratum 的 YAML 由网关在这里生成"
            />
          </div>
        </details>

        <details className="advanced" open>
          <summary>vecstore</summary>
          <div className="advanced-grid">
            <TextField label="bin" value={s.vecstore.bin} onChange={(v) => upVecstore({ bin: v })} />
            <TextField
              label="grpc_addr"
              value={s.vecstore.grpc_addr}
              onChange={(v) => upVecstore({ grpc_addr: v })}
            />
            <TextField
              label="rocksdb_path"
              value={s.vecstore.rocksdb_path}
              onChange={(v) => upVecstore({ rocksdb_path: v })}
            />
            <TextField
              label="health_addr"
              value={s.vecstore.health_addr}
              onChange={(v) => upVecstore({ health_addr: v })}
              hint="仅记录用，vecstore 自己不监听它"
            />
            <TextField
              label="extra_args"
              value={s.vecstore.extra_args ?? ''}
              onChange={(v) => upVecstore({ extra_args: v })}
              hint="追加的 CLI 参数，空格分隔"
            />
          </div>
        </details>

        <details className="advanced" open>
          <summary>embed</summary>
          <div className="advanced-grid">
            <TextField label="bin" value={s.embed.bin} onChange={(v) => upEmbed({ bin: v })} />
            <TextField
              label="service_addr"
              value={s.embed.service_addr}
              onChange={(v) => upEmbed({ service_addr: v })}
            />
          </div>
        </details>

        <details className="advanced" open>
          <summary>stratum</summary>
          <div className="advanced-grid">
            <TextField label="bin" value={s.stratum.bin} onChange={(v) => upStratum({ bin: v })} />
            <NumField label="node_id" value={s.stratum.node_id} onChange={(v) => upStratum({ node_id: v })} />
            <TextField label="data_dir" value={s.stratum.data_dir} onChange={(v) => upStratum({ data_dir: v })} />
            <TextField label="grpc_addr" value={s.stratum.grpc_addr} onChange={(v) => upStratum({ grpc_addr: v })} />
            <TextField label="raft_addr" value={s.stratum.raft_addr} onChange={(v) => upStratum({ raft_addr: v })} />
            <TextField
              label="vecstore_addr"
              value={s.stratum.vecstore_addr}
              onChange={(v) => upStratum({ vecstore_addr: v })}
            />
            <TextField
              label="embed_addr"
              value={s.stratum.embed_addr}
              onChange={(v) => upStratum({ embed_addr: v })}
            />
          </div>

          {/* raft peers 决定这个节点能跟谁组成集群。改错会让它起不来（或者起来后
              选不出 leader），所以做成逐行编辑而不是一个自由文本字段。 */}
          <p className="muted small">raft peers</p>
          <table className="queue-table">
            <thead>
              <tr>
                <th>id</th>
                <th>raft 地址</th>
                <th>service 地址</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {s.stratum.peers.map((p, i) => (
                <tr key={i}>
                  <td>
                    <input
                      value={String(p.id)}
                      size={4}
                      onChange={(e) =>
                        upStratum({
                          peers: s.stratum.peers.map((q, j) =>
                            j === i ? { ...q, id: num(e.target.value) } : q,
                          ),
                        })
                      }
                    />
                  </td>
                  <td>
                    <input
                      value={p.addr}
                      onChange={(e) =>
                        upStratum({
                          peers: s.stratum.peers.map((q, j) =>
                            j === i ? { ...q, addr: e.target.value } : q,
                          ),
                        })
                      }
                    />
                  </td>
                  <td>
                    <input
                      value={p.service_addr ?? ''}
                      onChange={(e) =>
                        upStratum({
                          peers: s.stratum.peers.map((q, j) =>
                            j === i ? { ...q, service_addr: e.target.value } : q,
                          ),
                        })
                      }
                    />
                  </td>
                  <td>
                    <button
                      type="button"
                      onClick={() =>
                        upStratum({ peers: s.stratum.peers.filter((_, j) => j !== i) })
                      }
                    >
                      删除
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="actions">
            <button
              type="button"
              onClick={() =>
                upStratum({
                  peers: [
                    ...s.stratum.peers,
                    { id: s.stratum.peers.length + 1, addr: '', service_addr: '' },
                  ],
                })
              }
            >
              ＋ 添加 peer
            </button>
          </div>
        </details>

        <details className="advanced">
          <summary>raft 定时 / 索引 / 协调器（留空 = 不修改）</summary>
          <div className="advanced-grid">
            <NumField
              label="heartbeat_interval_ms"
              value={s.stratum.heartbeat_interval_ms}
              onChange={(v) => upStratum({ heartbeat_interval_ms: v })}
            />
            <NumField
              label="election_timeout_min_ms"
              value={s.stratum.election_timeout_min_ms}
              onChange={(v) => upStratum({ election_timeout_min_ms: v })}
            />
            <NumField
              label="election_timeout_max_ms"
              value={s.stratum.election_timeout_max_ms}
              onChange={(v) => upStratum({ election_timeout_max_ms: v })}
            />
            <NumField
              label="index_lru_capacity"
              value={s.stratum.index_lru_capacity}
              onChange={(v) => upStratum({ index_lru_capacity: v })}
            />
            <NumField
              label="index_load_wait_timeout_ms"
              value={s.stratum.index_load_wait_timeout_ms}
              onChange={(v) => upStratum({ index_load_wait_timeout_ms: v })}
            />
            <NumField
              label="index_callback_max_retries"
              value={s.stratum.index_callback_max_retries}
              onChange={(v) => upStratum({ index_callback_max_retries: v })}
            />
            <NumField
              label="index_callback_retry_base_interval_ms"
              value={s.stratum.index_callback_retry_base_interval_ms}
              onChange={(v) => upStratum({ index_callback_retry_base_interval_ms: v })}
            />
            <NumField
              label="write_max_retries"
              value={s.stratum.write_max_retries}
              onChange={(v) => upStratum({ write_max_retries: v })}
            />
            <NumField
              label="write_retry_base_interval_ms"
              value={s.stratum.write_retry_base_interval_ms}
              onChange={(v) => upStratum({ write_retry_base_interval_ms: v })}
            />
            <NumField
              label="delete_max_retries"
              value={s.stratum.delete_max_retries}
              onChange={(v) => upStratum({ delete_max_retries: v })}
            />
            <NumField
              label="delete_retry_base_interval_ms"
              value={s.stratum.delete_retry_base_interval_ms}
              onChange={(v) => upStratum({ delete_retry_base_interval_ms: v })}
            />
          </div>
        </details>
      </>
    )
  })()

  return (
    <div className="status-section">
      <div className="status-head">
        <strong>启动参数</strong>
        <span className="chip">node {nodeId}</span>
      </div>
      <p className="muted small">
        这些参数写进该节点的 <code>console.yaml</code>，<b>下次启动/重启才生效</b>——
        正在跑的进程不会因为保存而换参数。数字留 0 表示不修改（服务端把 0 当作"未提交"）。
      </p>

      {body}

      {draft !== null && (
        <div className="actions" style={{ marginTop: 10 }}>
          <button type="button" disabled={save.isPending} onClick={() => void submit()}>
            {save.isPending ? '保存中…' : '保存参数'}
          </button>
          <button
            type="button"
            disabled={save.isPending}
            onClick={() => {
              setEdited(null)
              setNote(null)
            }}
          >
            放弃修改
          </button>
        </div>
      )}

      {note !== null && (
        <p className={note.tone === 'bad' ? 'error-box' : 'notice'}>{note.text}</p>
      )}
    </div>
  )
}

// ---------------------------------------------------------------- docker 集群

function DockerPanel() {
  const status = useDockerStatus()
  const cfg = useDockerConfig()
  const enable = useEnableDocker()
  const lifecycle = useDockerLifecycle()
  const nodeAct = useDockerNodeAction()
  const saveCfg = useSaveDockerConfig()

  const [edited, setEdited] = useState<DockerClusterConfig | null>(null)
  const [logNode, setLogNode] = useState<number | null>(null)
  const [output, setOutput] = useState<string | null>(null)
  const [note, setNote] = useState<{ tone: Tone; text: string } | null>(null)

  // 与启动参数那一段同一个模式：编辑过的副本优先，服务端数据只在没有编辑时用。
  // 这里没有 effect——集群参数不轮询，而 effect 会在每次重新拉取时把编辑顶掉。
  const draft = edited ?? cfg.data ?? null
  const update = (patch: Partial<DockerClusterConfig>) =>
    setEdited((d) => {
      const base = d ?? cfg.data
      return base === undefined ? null : { ...base, ...patch }
    })

  async function run(action: 'up' | 'down' | 'clean', force = false) {
    setNote(null)
    setOutput(null)
    try {
      const resp = await lifecycle.mutateAsync({ action, force })
      setOutput(resp.output)
    } catch (err) {
      setNote({ tone: 'bad', text: `${action} 失败：${reason(err)}` })
    }
  }

  async function nodeAction(id: number, action: 'start' | 'stop' | 'restart') {
    setNote(null)
    setOutput(null)
    try {
      const resp = await nodeAct.mutateAsync({ id, action })
      setOutput(resp.output)
    } catch (err) {
      setNote({ tone: 'bad', text: `node ${id} ${action} 失败：${reason(err)}` })
    }
  }

  async function save(rebuild: boolean) {
    if (draft === null) return
    setNote(null)
    try {
      // enabled 固定为 true：PUT 以提交值为准，漏掉它就会把开关关上，之后
      // /ops/docker/* 连读配置都会 400——页面上再也打不开。
      const resp = await saveCfg.mutateAsync({ ...draft, enabled: true })
      setNote({ tone: '', text: `集群参数已保存${resp.note !== undefined ? `：${resp.note}` : ''}` })
      if (rebuild) await run('up', true)
    } catch (err) {
      setNote({ tone: 'bad', text: `保存集群参数失败：${reason(err)}` })
    }
  }

  // 关闭状态：读配置都被拒（ops.go 的 dockerCfg 对整组 /ops/docker/* 一视同仁），
  // 所以这里没有可回填的表单，只有"打开它"这一件事可做。
  if (cfg.error !== null) {
    return (
      <DockerDisabledCard
        error={cfg.error}
        busy={enable.isPending}
        note={note}
        onEnable={() => {
          setNote(null)
          void enable
            .mutateAsync()
            .then(() => setNote({ tone: '', text: '已启用 docker 集群管理' }))
            .catch((err: unknown) => setNote({ tone: 'bad', text: `启用失败：${reason(err)}` }))
        }}
      />
    )
  }

  const st = status.data
  const busy = lifecycle.isPending || nodeAct.isPending || saveCfg.isPending

  return (
    <div className="status-section">
      <div className="status-head">
        <strong>Docker 集群</strong>
        <span className="chip">集群级</span>
        {st?.topology === 'two-tier' && <span className="chip warn">两层拓扑</span>}
      </div>
      <p className="muted small">
        参数是集群级统一配置：改完要<b>重建</b>才生效（单节点差异化修改没有意义）。操作由
        <code>scripts/cluster.sh</code> 执行，输出原样显示在下面。
      </p>

      {status.error !== null && (
        <p className="error-box">读取集群状态失败：{reason(status.error)}</p>
      )}
      {status.isPending && <p className="muted small">读取集群状态…</p>}
      {st !== undefined && <p className="muted">{overviewText(st)}</p>}

      <div className="actions" style={{ marginTop: 10 }}>
        <button type="button" disabled={busy} onClick={() => void run('up')}>
          启动集群
        </button>
        <ConfirmButton
          label="按当前参数重建"
          confirmLabel="确认重建（up --force）"
          disabled={busy}
          onConfirm={() => void run('up', true)}
        />
        <ConfirmButton
          label="停止（down）"
          confirmLabel="确认停止（保留数据卷）"
          disabled={busy}
          onConfirm={() => void run('down')}
        />
        <ConfirmButton
          label="清理（clean）"
          confirmLabel="确认清理：容器 + 数据卷都会删"
          className="danger"
          disabled={busy}
          onConfirm={() => void run('clean')}
        />
      </div>

      {st !== undefined && st.nodes.length > 0 && (
        <div>
          {dockerGroups(st).map((group) => (
            <div key={group.tier ?? 'all'}>
              {group.title !== null && <h3>{group.title}</h3>}
              <table className="queue-table">
                <thead>
                  <tr>
                    <th>节点</th>
                    <th>状态</th>
                    <th>健康</th>
                    <th>gRPC 端口</th>
                    <th>leader</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {group.nodes.map((n) => (
                    <tr key={n.id}>
                      <td>
                        node {n.id} <span className="muted small">{n.name}</span>
                      </td>
                      <td>
                        <Badge badge={containerStatusBadge(n.status)} />
                      </td>
                      <td>
                        <Badge badge={containerHealthBadge(n.health)} />
                      </td>
                      <td>
                        <code>:{n.grpc_port}</code>
                      </td>
                      <td>{n.leader ? '★' : '—'}</td>
                      <td className="actions">
                        {n.status === 'running' ? (
                          <>
                            <ConfirmButton
                              label="停止"
                              confirmLabel="确认停止"
                              disabled={busy}
                              onConfirm={() => void nodeAction(n.id, 'stop')}
                            />
                            <ConfirmButton
                              label="重启"
                              confirmLabel="确认重启"
                              disabled={busy}
                              onConfirm={() => void nodeAction(n.id, 'restart')}
                            />
                          </>
                        ) : (
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() => void nodeAction(n.id, 'start')}
                          >
                            启动
                          </button>
                        )}
                        <button
                          type="button"
                          onClick={() => setLogNode((cur) => (cur === n.id ? null : n.id))}
                        >
                          {logNode === n.id ? '收起日志' : '日志'}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ))}
        </div>
      )}

      {logNode !== null && <DockerLogView nodeId={logNode} />}

      {output !== null && output.trim() !== '' && <LogView text={output} />}
      {note !== null && (
        <p className={note.tone === 'bad' ? 'error-box' : 'notice'}>{note.text}</p>
      )}

      {draft !== null && (
        <>
          <details className="advanced">
            <summary>集群参数（保存后需重建才生效）</summary>
            <div className="advanced-grid">
              <label>
                <span>topology</span>
                <select
                  value={draft.topology === 'two-tier' ? 'two-tier' : 'single'}
                  onChange={(e) => update({ topology: e.target.value })}
                >
                  <option value="single">single（所有节点同构）</option>
                  <option value="two-tier">two-tier（控制层 + 存储层）</option>
                </select>
                <span className="muted small">
                  两者不可互换：两层里一个节点要么是控制节点、要么是存储节点
                  （落盘的 '' / 'single' 都表示单层，保存时会规范成 single）
                </span>
              </label>
              <NumField
                label={draft.topology === 'two-tier' ? 'nodes（控制层节点数）' : 'nodes'}
                value={draft.nodes}
                onChange={(v) => update({ nodes: v })}
              />
              <NumField
                label="base_port"
                value={draft.base_port}
                onChange={(v) => update({ base_port: v })}
              />
              <NumField
                label="storage_nodes"
                value={draft.storage_nodes}
                onChange={(v) => update({ storage_nodes: v })}
                hint="仅两层拓扑使用"
              />
              <NumField
                label="storage_base_port"
                value={draft.storage_base_port}
                onChange={(v) => update({ storage_base_port: v })}
                hint="仅两层拓扑使用"
              />
              <TextField
                label="network"
                value={draft.network}
                onChange={(v) => update({ network: v })}
              />
              <TextField
                label="image"
                value={draft.image}
                onChange={(v) => update({ image: v })}
                hint="两层拓扑的镜像自带 vecstore"
              />
              <TextField
                label="container_prefix"
                value={draft.container_prefix}
                onChange={(v) => update({ container_prefix: v })}
              />
              <TextField
                label="script"
                value={draft.script}
                onChange={(v) => update({ script: v })}
                hint="单层编排脚本"
              />
              <TextField
                label="script_two_tier"
                value={draft.script_two_tier}
                onChange={(v) => update({ script_two_tier: v })}
                hint="两层编排脚本"
              />
            </div>
            <label className="checkbox-row">
              <input
                type="checkbox"
                checked={draft.with_embed}
                onChange={(e) => update({ with_embed: e.target.checked })}
              />
              <span>同时启动 mock-embed 依赖（with_embed）</span>
            </label>

            <div className="actions">
              <button type="button" disabled={busy} onClick={() => void save(false)}>
                保存集群参数
              </button>
              <ConfirmButton
                label="保存并重建集群"
                confirmLabel="确认保存并重建"
                disabled={busy}
                onConfirm={() => void save(true)}
              />
              <button type="button" disabled={busy} onClick={() => setEdited(null)}>
                放弃修改
              </button>
            </div>
          </details>
        </>
      )}
    </div>
  )
}

/**
 * docker 集群管理被关掉时的样子。
 *
 * 单独抽出来（而不是内联在 DockerPanel 里）是因为它是一条**独立的分支**：关掉时
 * 网关对整组 `/ops/docker/*` 一视同仁地拒绝，连读配置都不行，所以这里没有可回填的
 * 表单，只有"打开它"。“为什么不能顺便把表单置灰”这个问题的答案就在这句里。
 */
export function DockerDisabledCard({
  error,
  onEnable,
  busy,
  note,
}: {
  error: unknown
  onEnable: () => void
  busy: boolean
  note: { tone: Tone; text: string } | null
}) {
  return (
    <div className="status-section">
      <div className="status-head">
        <strong>Docker 集群</strong>
        <span className="chip bad">未启用</span>
      </div>
      <p className="muted small">
        读不到集群配置：{reason(error)}
        <br />
        禁用时 <code>/ops/docker/*</code> 一律拒绝，包括读配置——所以页面除了把它打开，
        没有别的可显示。启用后各参数在这里编辑。
      </p>
      <div className="actions">
        <button type="button" disabled={busy} onClick={onEnable}>
          {busy ? '启用中…' : '启用 docker 集群管理'}
        </button>
      </div>
      {note !== null && (
        <p className={note.tone === 'bad' ? 'error-box' : 'notice'}>{note.text}</p>
      )}
    </div>
  )
}

function DockerLogView({ nodeId }: { nodeId: number }) {
  const logs = useDockerNodeLogs(nodeId, LOG_LINES)

  return (
    <div>
      <div className="queue-summary" style={{ marginTop: 10 }}>
        <span className="muted small">
          node {nodeId} 最后 {LOG_LINES} 行（docker logs）
        </span>
        <button type="button" disabled={logs.isFetching} onClick={() => void logs.refetch()}>
          {logs.isFetching ? '刷新中…' : '刷新'}
        </button>
      </div>
      {logs.error !== null && <p className="error-box">读取节点日志失败：{reason(logs.error)}</p>}
      {logs.isPending && <p className="muted small">读取节点日志…</p>}
      {logs.data !== undefined && <LogView text={logs.data.log} />}
    </div>
  )
}

// ---------------------------------------------------------------- 零件

/**
 * 需要二次确认的按钮：第一次点只"装弹"，第二次才真的执行。
 *
 * 装弹状态 5 秒后自行解除——一个忘了点确认的按钮不该长期停在"按下去就删数据"的
 * 状态上。
 */
function ConfirmButton({
  label,
  confirmLabel,
  onConfirm,
  disabled,
  className,
}: {
  label: string
  confirmLabel: string
  onConfirm: () => void
  disabled?: boolean
  className?: string
}) {
  const [armed, setArmed] = useState(false)

  useEffect(() => {
    if (!armed) return
    const t = setTimeout(() => setArmed(false), 5000)
    return () => clearTimeout(t)
  }, [armed])

  if (!armed) {
    return (
      <button
        type="button"
        className={className}
        disabled={disabled}
        onClick={() => setArmed(true)}
      >
        {label}
      </button>
    )
  }

  return (
    <span className="confirm-inline">
      <button
        type="button"
        className={className === undefined ? 'danger' : `${className} danger`}
        disabled={disabled}
        onClick={() => {
          setArmed(false)
          onConfirm()
        }}
      >
        {confirmLabel}
      </button>
      <button type="button" onClick={() => setArmed(false)}>
        取消
      </button>
    </span>
  )
}

/** 日志是一整段文本（docker 节点日志、脚本输出）或行数组（本地服务日志），两种都给这里。 */
function LogView({ text }: { text: string[] | string }) {
  const body = Array.isArray(text) ? text.join('\n') : text
  if (body.trim() === '') return <p className="muted small">（空）</p>
  return <pre className="log-view">{body}</pre>
}

function Badge({ badge }: { badge: { label: string; tone: Tone } }) {
  return <span className={badge.tone === '' ? 'chip' : `chip ${badge.tone}`}>{badge.label}</span>
}

function TextField({
  label,
  value,
  onChange,
  hint,
}: {
  label: string
  value: string
  onChange: (v: string) => void
  hint?: string
}) {
  return (
    <label>
      <span>{label}</span>
      <input value={value} onChange={(e) => onChange(e.target.value)} />
      {hint !== undefined && <span className="muted small">{hint}</span>}
    </label>
  )
}

/**
 * 数字字段。
 *
 * 空串按 0 提交，而 0 在服务端的 merge 逻辑里正是"这项没提交"（ops.go 的
 * mergeServices / handlePutConfig 都判非零），所以清空一个数字字段等于"保持原值"，
 * 不会把它改成 0。这层含义不写出来，页面上"清空"看起来就像在填 0。
 */
function NumField({
  label,
  value,
  onChange,
  hint,
}: {
  label: string
  value: number | undefined
  onChange: (v: number) => void
  hint?: string
}) {
  return (
    <label>
      <span>{label}</span>
      <input
        type="number"
        value={value === undefined || value === 0 ? '' : String(value)}
        onChange={(e) => onChange(num(e.target.value))}
      />
      {hint !== undefined && <span className="muted small">{hint}</span>}
    </label>
  )
}

// ---------------------------------------------------------------- helpers

function num(v: string): number {
  const n = Number(v)
  return Number.isFinite(n) ? n : 0
}

/** ApiError 的 message 就是网关 `{"error": …}` 里的文本（运维面没有 grpc_code）。 */
function reason(err: unknown): string {
  if (err instanceof ApiError) return err.message
  return err instanceof Error ? err.message : String(err)
}

function reachabilityBadge(n: ClusterNode): { label: string; tone: Tone } {
  if (n.local === true) return { label: '本机', tone: 'ok' }
  if (n.online === true) return { label: '在线', tone: 'ok' }
  return { label: '离线', tone: 'bad' }
}

function overviewText(st: DockerStatus): string {
  const running = st.nodes.filter((n) => n.status === 'running').length
  const healthy = st.nodes.filter((n) => n.health === 'healthy').length
  const leader = st.nodes.find((n) => n.leader)
  const topology =
    st.topology === 'two-tier'
      ? `两层（控制 ${st.control_count ?? 0} + 存储 ${st.storage_count ?? 0}）`
      : '单层'
  return (
    `网络 ${st.network} · 编排 ${topology} · 节点 ${st.count}（运行 ${running} / 健康 ${healthy}）` +
    ` · 基础端口 ${st.base_port} · 镜像 ${st.image}` +
    ` · leader ${leader === undefined ? '—' : leader.name}`
  )
}

/** 控制台认为存在的服务里，服务端这一份报告漏掉了哪些（按 SERVICES 的顺序）。 */
function missingServices(reported: ServiceId[]): ServiceId[] {
  return SERVICES.filter((id) => !reported.includes(id))
}
