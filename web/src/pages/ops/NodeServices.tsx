import { useState } from 'react'

import {
  serviceBadge,
  useNodeLogs,
  useNodeServices,
  useServiceAction,
  type ServiceId,
  type Tone,
} from '../../api/ops'
import { ConfirmButton } from '../../components/ConfirmButton'
import { Badge, LOG_LINES, LogView, missingServices, reason } from './shared'

const ACTION_LABEL = { start: '启动', stop: '停止', restart: '重启' } as const

export function NodeServices({
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
  const missing = missingServices(nodes.map((s) => s.service))

  return (
    <div className="status-section">
      <div className="toolbar">
        <div className="status-head">
          <strong>服务</strong>
          <span className="chip">node {nodeId}</span>
          {services.isFetching && <span className="muted small">刷新中…</span>}
        </div>
        <div className="actions">
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
        <div className="table-wrap">
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
                  <td className="mono small">{s.pid ?? '—'}</td>
                  <td className="muted small" title={s.log_file}>
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
                      className={logService === s.service ? 'btn-primary' : undefined}
                      onClick={() => setLogService((cur) => (cur === s.service ? null : s.service))}
                    >
                      {logService === s.service ? '收起日志' : '日志'}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {result !== null && (
        <p className={result.tone === 'bad' ? 'error-box' : 'notice'}>{result.text}</p>
      )}

      {logService !== null && (
        <ServiceLogView nodeId={nodeId} localNodeId={localNodeId} service={logService} />
      )}
    </div>
  )
}

function ServiceLogView({
  nodeId,
  localNodeId,
  service,
}: {
  nodeId: number
  localNodeId: number | null
  service: ServiceId
}) {
  const [pollMs, setPollMs] = useState(0)
  const [filter, setFilter] = useState('')
  const logs = useNodeLogs(nodeId, localNodeId, service, LOG_LINES, pollMs > 0 ? pollMs : undefined)

  return (
    <div>
      <div className="toolbar" style={{ marginTop: 6 }}>
        <span className="muted small">
          <code>{service}</code> 最后 {LOG_LINES} 行
          {logs.data !== undefined && <> · <code>{logs.data.log_file}</code></>}
          {logs.data?.truncated === true && ' · 已截断尾部'}
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
      {logs.error !== null && <p className="error-box">读取日志失败：{reason(logs.error)}</p>}
      {logs.isPending && <p className="muted small">读取日志…</p>}
      {logs.data !== undefined && <LogView text={logs.data.lines} filter={filter} />}
    </div>
  )
}
