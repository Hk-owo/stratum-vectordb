import { useState } from 'react'

import {
  containerHealthBadge,
  containerStatusBadge,
  dockerGroups,
  useDockerConfig,
  useDockerLifecycle,
  useDockerNodeAction,
  useDockerNodeLogs,
  useDockerStatus,
  useEnableDocker,
  useSaveDockerConfig,
  type DockerClusterConfig,
  type DockerStatus,
  type Tone,
} from '../../api/ops'
import { ConfirmButton } from '../../components/ConfirmButton'
import { Badge, LOG_LINES, LogView, NumField, TextField, reason } from './shared'

export function DockerPanel() {
  const cfg = useDockerConfig()
  const dockerEnabled = cfg.data?.enabled === true
  const status = useDockerStatus(5000, dockerEnabled)
  const enable = useEnableDocker()
  const lifecycle = useDockerLifecycle()
  const nodeAct = useDockerNodeAction()
  const saveCfg = useSaveDockerConfig()

  const [edited, setEdited] = useState<DockerClusterConfig | null>(null)
  const [logNode, setLogNode] = useState<number | null>(null)
  const [output, setOutput] = useState<string | null>(null)
  const [note, setNote] = useState<{ tone: Tone; text: string } | null>(null)

  const draft = edited ?? cfg.data ?? null
  const dirty = edited !== null
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
      const resp = await saveCfg.mutateAsync({ ...draft, enabled: true })
      setEdited(null)
      setNote({ tone: '', text: `集群参数已保存${resp.note !== undefined ? `：${resp.note}` : ''}` })
      if (rebuild) await run('up', true)
    } catch (err) {
      setNote({ tone: 'bad', text: `保存集群参数失败：${reason(err)}` })
    }
  }

  // 兼容两种情况：旧网关返回 400（cfg.error !== null）或新网关正常返回 { enabled: false }
  if (cfg.error !== null || cfg.data?.enabled === false) {
    const err =
      cfg.error ??
      new Error('docker 集群管理未启用：请在 ops config 中设置 docker.enabled=true')
    return (
      <DockerDisabledCard
        error={err}
        busy={enable.isPending}
        note={note}
        onEnable={() => {
          setNote(null)
          void enable
            .mutateAsync()
            .then(() => setNote({ tone: '', text: '已启用 docker 集群管理' }))
            .catch((e: unknown) => setNote({ tone: 'bad', text: `启用失败：${reason(e)}` }))
        }}
      />
    )
  }

  const st = status.data
  const busy = lifecycle.isPending || nodeAct.isPending || saveCfg.isPending

  return (
    <div className="status-section">
      <div className="toolbar">
        <div className="status-head">
          <strong>Docker 集群</strong>
          <span className="chip">集群级</span>
          {st?.topology === 'two-tier' && <span className="chip warn">两层拓扑</span>}
          {dirty && <span className="chip warn">集群参数有未保存修改</span>}
        </div>
        <div className="actions">
          <button type="button" className="btn-primary" disabled={busy} onClick={() => void run('up')}>
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
      </div>

      <p className="muted small">
        集群级统一编排（通过 <code>scripts/cluster.sh</code> 执行），参数保存后需执行「重建」才会作用于容器。
      </p>

      {status.error !== null && (
        <p className="error-box">读取集群状态失败：{reason(status.error)}</p>
      )}
      {status.isPending && dockerEnabled && <p className="muted small">读取集群状态…</p>}
      {st !== undefined && <p className="muted small">{overviewText(st)}</p>}

      {st !== undefined && st.nodes.length > 0 && (
        <div>
          {dockerGroups(st).map((group) => (
            <div key={group.tier ?? 'all'} style={{ marginTop: 10 }}>
              {group.title !== null && <h3 style={{ marginBottom: 8 }}>{group.title}</h3>}
              <div className="table-wrap">
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
                            className={logNode === n.id ? 'btn-primary' : undefined}
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
              hint="单层编排脚本（不可通过 HTTP 改写路径）"
            />
            <TextField
              label="script_two_tier"
              value={draft.script_two_tier}
              onChange={(v) => update({ script_two_tier: v })}
              hint="两层编排脚本（不可通过 HTTP 改写路径）"
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

          <div className="actions" style={{ marginTop: 10 }}>
            <button
              type="button"
              className={dirty ? 'btn-primary' : undefined}
              disabled={busy || !dirty}
              title={dirty ? '保存集群参数到 console.yaml' : '修改任意集群参数后即可保存'}
              onClick={() => void save(false)}
            >
              {dirty ? '保存集群参数' : '保存集群参数（无修改）'}
            </button>
            <ConfirmButton
              label="保存并重建集群"
              confirmLabel="确认保存并重建"
              disabled={busy}
              onConfirm={() => void save(true)}
            />
            <button type="button" disabled={busy || !dirty} onClick={() => setEdited(null)}>
              放弃修改
            </button>
          </div>
        </details>
      )}
    </div>
  )
}

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
      <p className="muted small">{reason(error)}</p>
      <div className="actions">
        <button type="button" className="btn-primary" disabled={busy} onClick={onEnable}>
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
  const [pollMs, setPollMs] = useState(0)
  const [filter, setFilter] = useState('')
  const logs = useDockerNodeLogs(nodeId, LOG_LINES, pollMs > 0 ? pollMs : undefined)

  return (
    <div>
      <div className="toolbar" style={{ marginTop: 10 }}>
        <span className="muted small">
          node {nodeId} 最后 {LOG_LINES} 行（docker logs）
        </span>
        <div className="toolbar-group">
          <input
            type="search"
            value={filter}
            placeholder="过滤日志关键字…"
            style={{ padding: '4px 8px', fontSize: 12, width: 170 }}
            onChange={(e) => setFilter(e.target.value)}
          />
          <select
            value={pollMs}
            style={{ padding: '4px 8px', fontSize: 12 }}
            onChange={(e) => setPollMs(Number(e.target.value))}
          >
            <option value={0}>手动刷新</option>
            <option value={2000}>每 2 秒</option>
            <option value={5000}>每 5 秒</option>
          </select>
          <button
            type="button"
            className="btn-sm"
            disabled={logs.isFetching}
            onClick={() => void logs.refetch()}
          >
            {logs.isFetching ? '刷新中…' : '刷新'}
          </button>
        </div>
      </div>
      {logs.error !== null && <p className="error-box">读取节点日志失败：{reason(logs.error)}</p>}
      {logs.isPending && <p className="muted small">读取节点日志…</p>}
      {logs.data !== undefined && <LogView text={logs.data.log} filter={filter} />}
    </div>
  )
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
