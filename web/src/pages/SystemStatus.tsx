import { useState, type FormEvent } from 'react'

import { ApiError } from '../api/client'
import { HealthStatus } from '../api/gen/admin'
import { FailureSide } from '../api/gen/knowledgebase'
import {
  useCancelMigration,
  useDrainStorageNode,
  useForceAbandonVersion,
  useForceRetryVersion,
  useManageStorageNode,
  useMigrateKB,
  useStorageDirectory,
  useSystemStatus,
} from '../api/queries'
import { ConfirmButton } from '../components/ConfirmButton'

/**
 * 系统状态页：告警处置、节点状态机、WAL 回收诊断、资源使用与存储拓扑运维。
 */
export function SystemStatus() {
  const [pollMs, setPollMs] = useState<number>(0)
  const status = useSystemStatus(pollMs > 0 ? pollMs : undefined)
  const storageDir = useStorageDirectory(true)

  const retry = useForceRetryVersion()
  const abandon = useForceAbandonVersion()
  const manageNode = useManageStorageNode()
  const drainNode = useDrainStorageNode()
  const migrateKb = useMigrateKB()
  const cancelMig = useCancelMigration()

  const [migKbId, setMigKbId] = useState('')
  const [migFromId, setMigFromId] = useState('')
  const [migToId, setMigToId] = useState('')
  const [migEpoch, setMigEpoch] = useState('')
  const [storageMsg, setStorageMsg] = useState<{ tone: '' | 'bad'; text: string } | null>(null)

  if (status.isPending) return <p className="muted">读取系统状态…</p>

  if (status.error !== null) {
    return (
      <p className="error-box">
        {status.error instanceof ApiError && status.error.isUnavailable
          ? '网关/后端不可达：'
          : '读取失败：'}
        {status.error.message}
      </p>
    )
  }

  const s = status.data
  if (s === undefined) return null

  const actionNeededCount =
    s.data_missing_versions.length +
    s.failed_permanent_versions.length +
    s.gc_blocked_versions.length +
    s.deleting_versions.length

  const dir = storageDir.data

  async function submitMigration(e: FormEvent) {
    e.preventDefault()
    if (migKbId.trim() === '' || migFromId.trim() === '' || migToId.trim() === '') return
    setStorageMsg(null)
    const opId = `mig-${Date.now()}`
    try {
      await migrateKb.mutateAsync({
        operation_id: opId,
        knowledge_base_id: migKbId.trim(),
        expected_epoch: migEpoch.trim() === '' ? '0' : migEpoch.trim(),
        from_node_id: migFromId.trim(),
        to_node_id: migToId.trim(),
        fault: false,
      })
      setStorageMsg({ tone: '', text: `已发起迁移任务 ${opId}` })
      setMigKbId('')
      setMigFromId('')
      setMigToId('')
      setMigEpoch('')
    } catch (err) {
      setStorageMsg({
        tone: 'bad',
        text: `发起迁移失败：${err instanceof Error ? err.message : String(err)}`,
      })
    }
  }

  return (
    <div className="page">
      <div className="toolbar">
        <div className="toolbar-group">
          <span
            className={`badge ${
              s.health?.status === HealthStatus.HEALTH_STATUS_HEALTHY
                ? 'ok'
                : s.health?.status === HealthStatus.HEALTH_STATUS_DEGRADED
                  ? 'warn'
                  : 'bad'
            }`}
          >
            <span className="dot" />
            {s.health?.status ?? 'UNKNOWN'}
          </span>
          {s.health?.details !== undefined && s.health.details !== '' && (
            <span className="muted small">{s.health.details}</span>
          )}
        </div>
        <div className="toolbar-group">
          <label className="kb-picker small">
            <span className="muted">自动刷新</span>
            <select value={pollMs} onChange={(e) => setPollMs(Number(e.target.value))}>
              <option value={0}>关闭</option>
              <option value={5000}>每 5 秒</option>
              <option value={15000}>每 15 秒</option>
            </select>
          </label>
          <button
            type="button"
            disabled={status.isFetching}
            onClick={() => {
              void status.refetch()
              void storageDir.refetch()
            }}
          >
            {status.isFetching ? '刷新中…' : '立即刷新'}
          </button>
        </div>
      </div>

      <div className="kpi-grid">
        <div className="kpi-card">
          <span className="kpi-label">待动作异常</span>
          <span className="kpi-value" style={{ color: actionNeededCount > 0 ? 'var(--bad)' : 'var(--ok)' }}>
            {actionNeededCount}
          </span>
        </div>
        <div className="kpi-card">
          <span className="kpi-label">已载入索引</span>
          <span className="kpi-value">
            {s.resource_usage !== undefined ? s.resource_usage.loaded_index_count : '—'}
          </span>
        </div>
        <div className="kpi-card">
          <span className="kpi-label">Chunk 存储</span>
          <span className="kpi-value">
            {s.resource_usage !== undefined ? formatBytes(s.resource_usage.chunk_store_bytes) : '—'}
          </span>
        </div>
        <div className="kpi-card">
          <span className="kpi-label">文档存储</span>
          <span className="kpi-value">
            {s.resource_usage !== undefined ? formatBytes(s.resource_usage.doc_store_bytes) : '—'}
          </span>
        </div>
      </div>

      {s.node_state !== undefined && (
        <div className="status-section">
          <div className="status-head">
            <strong>当前应答节点状态（node_state）</strong>
            <span className="chip accent">{s.node_state.state}</span>
            <span
              className={`chip ${
                s.node_state.redundancy === 'HEALTHY'
                  ? 'ok'
                  : s.node_state.redundancy === 'DEGRADED'
                    ? 'warn'
                    : s.node_state.redundancy === 'UNAVAILABLE'
                      ? 'bad'
                      : ''
              }`}
            >
              冗余：{s.node_state.redundancy}
            </span>
            {s.node_state.since !== '' && (
              <span className="muted small">自 {s.node_state.since}</span>
            )}
          </div>
          {s.node_state.redundancy_detail !== '' && (
            <p className="muted small">{s.node_state.redundancy_detail}</p>
          )}
          {s.node_state.actions.length > 0 && (
            <div className="toolbar-group">
              {s.node_state.actions.map((act, i) => (
                <span key={i} className="chip">
                  {act}
                </span>
              ))}
            </div>
          )}
        </div>
      )}

      <h3>需要动作的</h3>

      <Section
        title="数据缺失（data_missing）"
        count={s.data_missing_versions.length}
        tone="bad"
        hint="没有任何可达副本持有这些版本的数据。不会自行恢复：需写入方按原 client_request_id 重发或放弃。"
      >
        {s.data_missing_versions.map((v) => (
          <li key={`${v.kb_id}-${v.version_id}`}>
            <code>{v.kb_id}</code> <code>v{v.version_id}</code>{' '}
            <span className="muted small">索引 {v.index_status}</span>
          </li>
        ))}
      </Section>

      <Section
        title="永久失败（failed_permanent）"
        count={s.failed_permanent_versions.length}
        tone="bad"
        hint="控制层已判死，不会再自动重试。索引侧可重试索引；数据侧没有可重试写入，只能放弃。"
      >
        {s.failed_permanent_versions.map((v) => (
          <li key={`${v.kb_id}-${v.version_id}`}>
            <code>{v.kb_id}</code> <code>v{v.version_id}</code>{' '}
            <span className={`chip ${v.side === FailureSide.FAILURE_SIDE_INDEX ? 'warn' : ''}`}>
              {v.side === FailureSide.FAILURE_SIDE_INDEX ? '索引侧' : '数据侧'}
            </span>{' '}
            <span className="muted small">
              尝试 {v.failure_count} 次 · {v.reason}
            </span>{' '}
            {v.side === FailureSide.FAILURE_SIDE_INDEX && (
              <button
                type="button"
                className="btn-sm"
                disabled={retry.isPending}
                onClick={() => retry.mutate({ kbId: v.kb_id, versionId: v.version_id })}
              >
                重试索引
              </button>
            )}{' '}
            <ConfirmButton
              label="放弃"
              confirmLabel="确认放弃"
              className="danger btn-sm"
              disabled={abandon.isPending}
              onConfirm={() => abandon.mutate({ kbId: v.kb_id, versionId: v.version_id })}
            />
          </li>
        ))}
        {retry.isError && <p className="muted small">重试失败：{String(retry.error)}</p>}
        {abandon.isError && <p className="muted small">放弃失败：{String(abandon.error)}</p>}
      </Section>

      <Section
        title="回收受阻（gc_blocked）"
        count={s.gc_blocked_versions.length}
        tone="warn"
        hint="数据完好、版本可查，但剩余服务副本数不足以轮换清理死重，需调大副本配置。"
      >
        {s.gc_blocked_versions.map((v) => (
          <li key={`${v.kb_id}-${v.version_id}`}>
            <code>{v.kb_id}</code> <code>v{v.version_id}</code>{' '}
            <span className="muted small">
              死重 {(v.dead_share * 100).toFixed(0)}% · 除本节点外服务副本 {v.others_serving} 个 ·
              要求至少 {v.minimum_required} 个
            </span>
          </li>
        ))}
      </Section>

      <Section title="删除中（deleting）" count={s.deleting_versions.length} tone="warn">
        {s.deleting_versions.map((v) => (
          <li key={`${v.kb_id}-${v.version_id}`}>
            <code>{v.kb_id}</code> <code>v{v.version_id}</code>
          </li>
        ))}
      </Section>

      <h3>观察与诊断</h3>

      <Section title="卡住的版本（stuck）" count={s.stuck_versions.length} tone="">
        {s.stuck_versions.map((v) => (
          <li key={`${v.kb_id}-${v.version_id}`}>
            <code>{v.kb_id}</code> <code>v{v.version_id}</code>{' '}
            <span className="muted small">索引 {v.index_status}</span>
          </li>
        ))}
      </Section>

      {(s.version_states?.length ?? 0) > 0 && (
        <Section title="节点非稳态构建（version_states）" count={s.version_states.length} tone="">
          {s.version_states.map((vs) => (
            <li key={`${vs.kb_id}-${vs.version_id}`}>
              <code>{vs.kb_id}</code> <code>v{vs.version_id}</code>{' '}
              <span className={`chip ${vs.state === 'BUILD_FAILED' ? 'bad' : 'warn'}`}>
                {vs.state}
              </span>{' '}
              <span className="muted small">
                {vs.since}
                {vs.detail !== '' ? ` · ${vs.detail}` : ''}
              </span>
            </li>
          ))}
        </Section>
      )}

      {s.reclaim !== undefined && (
        <Section
          title="WAL 回收阻塞诊断（reclaim）"
          count={s.reclaim.blocked.length}
          tone={s.reclaim.blocked.length > 0 ? 'warn' : ''}
          hint={
            s.reclaim.judgement_available
              ? '当必需副本失联或缺少游标时，该知识库的 WAL 回收水位会暂停推进。'
              : '当前节点尚未完成一次回收检查，尚无法判断是否存在阻塞。'
          }
        >
          {s.reclaim.blocked.map((b) => (
            <li key={b.kb_id}>
              <code>{b.kb_id}</code>{' '}
              <span className="muted small">
                阻塞副本：
                {b.blockers.map((bl) => `${bl.address} (${bl.reason})`).join('，')}
              </span>
            </li>
          ))}
        </Section>
      )}

      <Section title="WAL 告警（wal_alerts）" count={s.wal_alerts.length} tone="warn">
        {s.wal_alerts.map((a, i) => (
          <li key={i}>
            {a.description} <span className="muted small">重试 {a.retry_count} 次</span>
          </li>
        ))}
      </Section>

      <Section title="删除失败的库（delete_failed_kbs）" count={s.delete_failed_kbs.length} tone="bad">
        {s.delete_failed_kbs.map((kb) => (
          <li key={kb}>
            <code>{kb}</code>
          </li>
        ))}
      </Section>

      <h3>存储拓扑与迁移（StorageAdminService）</h3>

      <div className="status-section">
        <div className="status-head">
          <strong>存储目录快照</strong>
          {dir !== undefined && <span className="chip">revision {dir.revision}</span>}
        </div>

        {storageDir.isPending && <p className="muted small">读取存储目录…</p>}
        {storageDir.error !== null && (
          <p className="muted small">
            {storageDir.error instanceof ApiError &&
            (storageDir.error.status === 401 || storageDir.error.status === 403)
              ? '当前控制台凭据没有 storage_admin 权限（如需在界面管理存储节点与迁移，请在 router tokens 中开启 storage_admin）。'
              : `读取存储目录失败：${
                  storageDir.error instanceof Error
                    ? storageDir.error.message
                    : String(storageDir.error)
                }`}
          </p>
        )}

        {storageMsg !== null && (
          <p className={storageMsg.tone === 'bad' ? 'error-box' : 'notice'}>
            {storageMsg.text}
          </p>
        )}

        {dir !== undefined && (
          <>
            {dir.nodes.length > 0 && (
              <div className="table-wrap">
                <table className="queue-table">
                  <thead>
                    <tr>
                      <th>节点 ID</th>
                      <th>地址</th>
                      <th>管理状态</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dir.nodes.map((n) => (
                      <tr key={n.node_id}>
                        <td>
                          <code>node {n.node_id}</code>
                        </td>
                        <td>
                          <code>{n.address}</code>
                        </td>
                        <td>
                          <span
                            className={`chip ${
                              n.management_state === 'active'
                                ? 'ok'
                                : n.management_state === 'retired'
                                  ? 'bad'
                                  : 'warn'
                            }`}
                          >
                            {n.management_state}
                          </span>
                        </td>
                        <td className="actions">
                          <ConfirmButton
                            label="排空（drain）"
                            confirmLabel="确认排空节点"
                            disabled={drainNode.isPending || manageNode.isPending}
                            onConfirm={() => {
                              setStorageMsg(null)
                              drainNode.mutate(
                                {
                                  operation_id: `drain-${n.node_id}-${Date.now()}`,
                                  node_id: n.node_id,
                                  expected_revision: dir.revision,
                                  target_node_ids: [],
                                  fault: false,
                                },
                                {
                                  onSuccess: () =>
                                    setStorageMsg({
                                      tone: '',
                                      text: `已提交 node ${n.node_id} 排空操作`,
                                    }),
                                  onError: (e) =>
                                    setStorageMsg({
                                      tone: 'bad',
                                      text: `排空失败：${e instanceof Error ? e.message : String(e)}`,
                                    }),
                                },
                              )
                            }}
                          />
                          <button
                            type="button"
                            disabled={manageNode.isPending}
                            onClick={() => {
                              setStorageMsg(null)
                              manageNode.mutate(
                                {
                                  node_id: n.node_id,
                                  expected_revision: dir.revision,
                                  action: 'resume',
                                  address: n.address,
                                  operation_id: '',
                                  block_revision: '0',
                                },
                                {
                                  onSuccess: () =>
                                    setStorageMsg({
                                      tone: '',
                                      text: `已提交 node ${n.node_id} resume`,
                                    }),
                                  onError: (e) =>
                                    setStorageMsg({
                                      tone: 'bad',
                                      text: `resume 失败：${e instanceof Error ? e.message : String(e)}`,
                                    }),
                                },
                              )
                            }}
                          >
                            恢复
                          </button>
                          <ConfirmButton
                            label="摘除（retire）"
                            confirmLabel="确认摘除"
                            className="danger"
                            disabled={manageNode.isPending}
                            onConfirm={() => {
                              setStorageMsg(null)
                              manageNode.mutate(
                                {
                                  node_id: n.node_id,
                                  expected_revision: dir.revision,
                                  action: 'retire',
                                  address: n.address,
                                  operation_id: '',
                                  block_revision: '0',
                                },
                                {
                                  onSuccess: () =>
                                    setStorageMsg({
                                      tone: '',
                                      text: `已提交 node ${n.node_id} 摘除`,
                                    }),
                                  onError: (e) =>
                                    setStorageMsg({
                                      tone: 'bad',
                                      text: `摘除失败：${e instanceof Error ? e.message : String(e)}`,
                                    }),
                                },
                              )
                            }}
                          />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {dir.placements.length > 0 && (
              <details className="advanced">
                <summary>知识库副本分布（{dir.placements.length}）</summary>
                <div className="table-wrap" style={{ marginTop: 8 }}>
                  <table className="queue-table">
                    <thead>
                      <tr>
                        <th>知识库</th>
                        <th>Placement Epoch</th>
                        <th>副本节点</th>
                        <th>最小要求副本</th>
                      </tr>
                    </thead>
                    <tbody>
                      {dir.placements.map((p) => (
                        <tr key={p.knowledge_base_id}>
                          <td>
                            <code>{p.knowledge_base_id}</code>
                          </td>
                          <td>
                            <code>{p.placement_epoch}</code>
                          </td>
                          <td>
                            <code>{p.replica_ids.join(', ')}</code>
                          </td>
                          <td>{p.required_replicas}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </details>
            )}

            {dir.migrations.length > 0 && (
              <div className="table-wrap">
                <table className="queue-table">
                  <thead>
                    <tr>
                      <th>迁移任务 ID</th>
                      <th>知识库</th>
                      <th>源 → 目标</th>
                      <th>阶段</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dir.migrations.map((m) => (
                      <tr key={m.operation_id}>
                        <td>
                          <code>{m.operation_id}</code>
                        </td>
                        <td>
                          <code>{m.knowledge_base_id}</code>
                        </td>
                        <td>
                          node {m.from_node_id} → node {m.to_node_id}
                        </td>
                        <td>
                          <span className="chip warn">{m.phase}</span>
                        </td>
                        <td>
                          <ConfirmButton
                            label="取消迁移"
                            confirmLabel="确认取消"
                            className="danger btn-sm"
                            disabled={cancelMig.isPending}
                            onConfirm={() =>
                              cancelMig.mutate({ operation_id: m.operation_id })
                            }
                          />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            <details className="advanced">
              <summary>发起知识库副本迁移（MigrateKB）</summary>
              <form className="advanced-grid" onSubmit={(e) => void submitMigration(e)}>
                <label>
                  <span>知识库 ID</span>
                  <input
                    value={migKbId}
                    placeholder="kb-xxxx"
                    onChange={(e) => setMigKbId(e.target.value)}
                  />
                </label>
                <label>
                  <span>源节点 ID</span>
                  <input
                    value={migFromId}
                    placeholder="例如 1"
                    onChange={(e) => setMigFromId(e.target.value)}
                  />
                </label>
                <label>
                  <span>目标节点 ID</span>
                  <input
                    value={migToId}
                    placeholder="例如 4"
                    onChange={(e) => setMigToId(e.target.value)}
                  />
                </label>
                <label>
                  <span>Expected Epoch</span>
                  <input
                    value={migEpoch}
                    placeholder="placement_epoch"
                    onChange={(e) => setMigEpoch(e.target.value)}
                  />
                </label>
                <div className="actions" style={{ alignSelf: 'end' }}>
                  <button type="submit" disabled={migrateKb.isPending}>
                    {migrateKb.isPending ? '提交中…' : '发起迁移'}
                  </button>
                </div>
              </form>
            </details>
          </>
        )}
      </div>
    </div>
  )
}

function Section({
  title,
  count,
  tone,
  hint,
  children,
}: {
  title: string
  count: number
  tone: '' | 'warn' | 'bad'
  hint?: string
  children: React.ReactNode
}) {
  const chip = tone === '' ? 'chip' : `chip ${tone}`
  return (
    <div className="status-section">
      <div className="status-head">
        <strong>{title}</strong>
        <span className={chip}>{count}</span>
      </div>
      {hint !== undefined && <p className="muted small">{hint}</p>}
      {count > 0 && <ul className="status-list">{children}</ul>}
    </div>
  )
}

function formatBytes(raw: string): string {
  const n = Number(raw)
  if (!Number.isFinite(n) || n <= 0) return '0 B'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  let v = n
  let u = 0
  while (v >= 1024 && u < units.length - 1) {
    v /= 1024
    u += 1
  }
  return `${v.toFixed(u === 0 ? 0 : 1)} ${units[u]}`
}
