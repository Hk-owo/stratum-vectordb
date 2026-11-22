import { useCallback, useEffect, useState } from 'react'

import { ApiError } from './api/client'
import { HealthStatus } from './api/gen/admin'
import { useHealth } from './api/queries'
import { InFlightPanel } from './components/InFlightPanel'
import { KbManagerDialog } from './components/KbManagerDialog'
import { KbPicker } from './components/KbPicker'
import { SettingsDialog } from './components/SettingsDialog'
import { ExitGuard } from './exit/ExitGuard'
import { Documents } from './pages/Documents'
import { History } from './pages/History'
import { Ops } from './pages/Ops'
import { Search } from './pages/Search'
import { SystemStatus } from './pages/SystemStatus'
import { Versions } from './pages/Versions'
import { loadPrefs, savePrefs } from './settings/store'

type PageId = 'search' | 'documents' | 'versions' | 'history' | 'status' | 'ops'

const WORKSPACE_PAGES: ReadonlyArray<{ id: PageId; label: string }> = [
  { id: 'search', label: '检索' },
  { id: 'documents', label: '文档' },
  { id: 'versions', label: '版本' },
  { id: 'history', label: '历史' },
]

const ADMIN_PAGES: ReadonlyArray<{ id: PageId; label: string }> = [
  { id: 'status', label: '系统状态' },
  { id: 'ops', label: '运维' },
]

const ALL_PAGES = [...WORKSPACE_PAGES, ...ADMIN_PAGES]

function parseHashPage(): PageId {
  const raw = window.location.hash.replace(/^#/, '')
  return ALL_PAGES.some((p) => p.id === raw) ? (raw as PageId) : 'search'
}

function healthLabel(status: HealthStatus | undefined): string {
  switch (status) {
    case HealthStatus.HEALTH_STATUS_HEALTHY:
      return '健康'
    case HealthStatus.HEALTH_STATUS_DEGRADED:
      return '降级'
    case HealthStatus.HEALTH_STATUS_UNHEALTHY:
      return '不健康'
    case undefined:
      return '连接中…'
    default:
      return status
  }
}

function healthClass(status: HealthStatus | undefined): string {
  switch (status) {
    case HealthStatus.HEALTH_STATUS_HEALTHY:
      return 'badge ok'
    case HealthStatus.HEALTH_STATUS_DEGRADED:
      return 'badge warn'
    case HealthStatus.HEALTH_STATUS_UNHEALTHY:
      return 'badge bad'
    default:
      return 'badge'
  }
}

export default function App() {
  const [page, setPage] = useState<PageId>(() => parseHashPage())
  const [kbId, setKbId] = useState<string | null>(null)
  const [settingsOpen, setSettingsOpen] = useState(false)
  const [kbManagerOpen, setKbManagerOpen] = useState(false)
  const [searchVersionPreset, setSearchVersionPreset] = useState<string | null>(null)

  const navigate = useCallback((next: PageId) => {
    setPage(next)
    if (window.location.hash !== `#${next}`) {
      window.location.hash = next
    }
  }, [])

  useEffect(() => {
    const onHash = () => setPage(parseHashPage())
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  useEffect(() => {
    void (async () => {
      const p = await loadPrefs()
      if (p.selected_kb_id !== null) setKbId(p.selected_kb_id)
    })()
  }, [])

  function selectKb(next: string) {
    setKbId(next)
    void savePrefs({ selected_kb_id: next })
  }

  function clearKb() {
    setKbId(null)
    void savePrefs({ selected_kb_id: null })
  }

  const health = useHealth(5000)
  const degraded = health.data?.status === HealthStatus.HEALTH_STATUS_DEGRADED

  return (
    <div className="app">
      <nav className="sidebar" aria-label="主导航">
        <div className="brand-row">
          <div className="brand">
            <span className="brand-mark">S</span>
            <span>Stratum</span>
          </div>
        </div>

        <div className="nav-group">
          <div className="nav-group-label">工作区</div>
          {WORKSPACE_PAGES.map((p) => (
            <button
              key={p.id}
              type="button"
              className="nav-item"
              aria-current={p.id === page ? 'page' : undefined}
              onClick={() => navigate(p.id)}
            >
              <span>{p.label}</span>
            </button>
          ))}
        </div>

        <div className="nav-group">
          <div className="nav-group-label">集群运维</div>
          {ADMIN_PAGES.map((p) => (
            <button
              key={p.id}
              type="button"
              className="nav-item"
              aria-current={p.id === page ? 'page' : undefined}
              onClick={() => navigate(p.id)}
            >
              <span>{p.label}</span>
            </button>
          ))}
        </div>

        <div className="sidebar-footer">
          <button
            type="button"
            className="icon-btn"
            title="连接设置：改 gateway 地址、看本机数据目录"
            onClick={() => setSettingsOpen(true)}
          >
            连接设置
          </button>
        </div>
      </nav>

      <main className="main">
        <div className="topbar">
          <div className="topbar-left">
            <h1 className="title">{ALL_PAGES.find((p) => p.id === page)?.label ?? ''}</h1>
          </div>
          <div className="topbar-right">
            <InFlightPanel />
            <KbPicker
              value={kbId}
              onChange={selectKb}
              onClear={clearKb}
              onOpenManager={() => setKbManagerOpen(true)}
            />
            <span className={healthClass(health.data?.status)} title={health.data?.details ?? ''}>
              <span className="dot" />
              {health.isPending ? '连接中…' : healthLabel(health.data?.status)}
            </span>
          </div>
        </div>

        <div className="page">
          {health.error !== null && (
            <p className="error-box">
              {health.error instanceof ApiError && health.error.status === 401
                ? '网关拒绝了当前访问（401 未授权）：如果当前不是通过 http://127.0.0.1:8081 或 http://localhost:8081 打开，浏览器在普通 http:// 局域网 IP 下不会发送同源校验头，请改用 localhost / 127.0.0.1（或端口转发）访问。'
                : `网关不可达：${health.error.message}`}
            </p>
          )}
          {degraded && (
            <p className="notice warn">
              集群健康状态为「降级」。读仍在服务，但副本低于 quorum 要求，写入可能被拒。
              {health.data?.details !== undefined && health.data.details !== ''
                ? `（${health.data.details}）`
                : ''}
            </p>
          )}

          {page === 'search' && (
            <Search
              kbId={kbId}
              presetVersionId={searchVersionPreset}
              onClearPreset={() => setSearchVersionPreset(null)}
              onOpenKbManager={() => setKbManagerOpen(true)}
            />
          )}

          {/* 文档页保持挂载，切页不丢失待提交文件队列 */}
          <div hidden={page !== 'documents'}>
            <Documents kbId={kbId} onOpenKbManager={() => setKbManagerOpen(true)} />
          </div>

          {page === 'versions' && (
            <Versions
              kbId={kbId}
              onJumpToSearch={(vid) => {
                setSearchVersionPreset(vid)
                navigate('search')
              }}
              onOpenKbManager={() => setKbManagerOpen(true)}
            />
          )}
          {page === 'history' && <History />}
          {page === 'status' && <SystemStatus />}
          {page === 'ops' && <Ops />}
        </div>
      </main>

      <ExitGuard />

      <SettingsDialog open={settingsOpen} onClose={() => setSettingsOpen(false)} />
      <KbManagerDialog
        open={kbManagerOpen}
        selectedKbId={kbId}
        onSelectKb={selectKb}
        onClearKb={clearKb}
        onClose={() => setKbManagerOpen(false)}
      />
    </div>
  )
}
