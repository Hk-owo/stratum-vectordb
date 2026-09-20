import { ApiError } from '../api/client'
import { FailureSide } from '../api/gen/knowledgebase'
import { useForceAbandonVersion, useForceRetryVersion, useSystemStatus } from '../api/queries'

/**
 * 系统状态页。
 *
 * 这里刻意把旧控制台**从未渲染过**的四项放在最上面并加粗，因为它们各自代表
 * 一种"不看就不知道、且需要动作"的状态：
 *
 *   · `data_missing_versions` —— 没有任何可达副本持有这份数据。它**不会**自己变好，
 *     唯一出路是写入方按同一 `client_request_id` 重发（§7.12）。
 *   · `failed_permanent_versions` —— 控制层已判死，只有运维能重试或放弃（§10.1）。
 *     它带着 `side`：数据侧和索引侧的补救办法不同（重试写入 vs 重建索引）。
 *   · `gc_blocked_versions` —— 告警而非故障：数据完好、版本可查，卡住的是"没有多余
 *     副本可以轮换"，只能靠调大副本数（§8.6(d)）。不显示出来就永远没人知道。
 *   · `deleting_versions` —— DeleteVersion 的清理进行中或未完成。
 *
 * 所有这些都不该被折成一句"集群不健康"：每一条的处置办法都不一样。
 */
export function SystemStatus() {
  const status = useSystemStatus()
  // 两个 action 各建一个实例就够：放进 map 里会给每条版本各建一个（违反 hooks
  // 规则），而 pending/error 共享正好符合"一次点一个"的用法。
  const retry = useForceRetryVersion()
  const abandon = useForceAbandonVersion()

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

  return (
    <div className="page">
      <p className="muted">
        健康：<code>{s.health?.status ?? 'UNKNOWN'}</code>
        {s.health?.details !== undefined && s.health.details !== '' && <> · {s.health.details}</>}
      </p>

      <h3>需要动作的</h3>

      <Section
        title="数据缺失（data_missing）"
        count={s.data_missing_versions.length}
        tone="bad"
        hint="没有任何可达副本持有这些版本的数据。它不会自己变好：写入方必须按同一 client_request_id 重发，否则只能放弃。"
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
        hint="控制层已判死，不会再自动重试。索引侧可以重试（撤销裁决、清原因链、重建）；数据侧没有可重试的写入——只能放弃。放弃即版本离链，物理清理异步执行。"
      >
        {s.failed_permanent_versions.map((v) => (
          <li key={`${v.kb_id}-${v.version_id}`}>
            <code>{v.kb_id}</code> <code>v{v.version_id}</code>{' '}
            {/* side 不是装饰：两种判死的补救办法不同，不标出来运维不知道该按哪个。
                注意 FailureSide 的 0 值是 DATA，所以"未设置"落在数据侧，
                这里显式判 INDEX 而不是拿 0 当未知。 */}
            <span className={`chip ${v.side === FailureSide.FAILURE_SIDE_INDEX ? 'warn' : ''}`}>
              {v.side === FailureSide.FAILURE_SIDE_INDEX ? '索引侧' : '数据侧'}
            </span>{' '}
            <span className="muted small">
              尝试 {v.failure_count} 次 · {v.reason}
            </span>{' '}
            {v.side === FailureSide.FAILURE_SIDE_INDEX && (
              <button
                type="button"
                disabled={retry.isPending}
                onClick={() => retry.mutate({ kbId: v.kb_id, versionId: v.version_id })}
              >
                重试索引
              </button>
            )}{' '}
            <button
              type="button"
              disabled={abandon.isPending}
              onClick={() => abandon.mutate({ kbId: v.kb_id, versionId: v.version_id })}
            >
              放弃
            </button>
          </li>
        ))}
        {retry.isError && <p className="muted small">重试失败：{String(retry.error)}</p>}
        {abandon.isError && <p className="muted small">放弃失败：{String(abandon.error)}</p>}
      </Section>

      <Section
        title="回收受阻（gc_blocked）"
        count={s.gc_blocked_versions.length}
        tone="warn"
        hint="告警而非故障：数据完好、版本可查，什么都没被改动。卡住的是副本数不足以轮换——需要改配置，所以必须看得见。"
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

      <h3>观察项</h3>

      <Section title="卡住的版本（stuck）" count={s.stuck_versions.length} tone="">
        {s.stuck_versions.map((v) => (
          <li key={`${v.kb_id}-${v.version_id}`}>
            <code>{v.kb_id}</code> <code>v{v.version_id}</code>{' '}
            <span className="muted small">索引 {v.index_status}</span>
          </li>
        ))}
      </Section>

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

      {s.resource_usage !== undefined && (
        <>
          <h3>资源</h3>
          <p className="muted">
            已载入索引 {s.resource_usage.loaded_index_count} 个 · chunk 存储{' '}
            {formatBytes(s.resource_usage.chunk_store_bytes)} · 文档存储{' '}
            {formatBytes(s.resource_usage.doc_store_bytes)}
          </p>
        </>
      )}
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

/** 字节数是 int64 → 字符串。 */
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
