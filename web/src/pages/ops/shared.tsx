import { ApiError } from '../../api/client'
import { SERVICES, type ClusterNode, type ServiceId, type Tone } from '../../api/ops'

export const LOG_LINES = 300

export function LogView({
  text,
  filter = '',
}: {
  text: string[] | string
  filter?: string
}) {
  const lines = Array.isArray(text) ? text : text.split('\n')
  const q = filter.trim().toLowerCase()
  const visible = q === '' ? lines : lines.filter((l) => l.toLowerCase().includes(q))
  const body = visible.join('\n')
  if (body.trim() === '') {
    return (
      <p className="muted small">
        {q === '' ? '（空）' : `没有包含 "${filter.trim()}" 的日志行`}
      </p>
    )
  }
  return <pre className="log-view">{body}</pre>
}

export function Badge({ badge }: { badge: { label: string; tone: Tone } }) {
  return <span className={badge.tone === '' ? 'chip' : `chip ${badge.tone}`}>{badge.label}</span>
}

export function TextField({
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

export function NumField({
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

export function num(v: string): number {
  const n = Number(v)
  return Number.isFinite(n) ? n : 0
}

export function reason(err: unknown): string {
  if (err instanceof ApiError) return err.message
  return err instanceof Error ? err.message : String(err)
}

export function reachabilityBadge(n: ClusterNode): { label: string; tone: Tone } {
  if (n.local === true) return { label: '本机', tone: 'ok' }
  if (n.online === true) return { label: '在线', tone: 'ok' }
  return { label: '离线', tone: 'bad' }
}

export function missingServices(reported: ServiceId[]): ServiceId[] {
  return SERVICES.filter((id) => !reported.includes(id))
}
