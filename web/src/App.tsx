import { useEffect, useState } from 'react'

import type { HealthStatus } from './api/gen/admin'
import { useHealth, useSystemStatus } from './api/queries'
import { InFlightPanel } from './components/InFlightPanel'
import { SettingsDialog } from './components/SettingsDialog'
import { KbPicker } from './components/KbPicker'
import { loadPrefs, savePrefs } from './settings/store'
import { Documents } from './pages/Documents'
import { History } from './pages/History'
import { Search } from './pages/Search'
import { SystemStatus } from './pages/SystemStatus'
import { Versions } from './pages/Versions'

/**
 * 应用壳：知识库选择 + 导航 + 页面。
 *
 * 顶栏刻意只放两样东西：当前知识库，以及**健康指示**。后者要一直在，因为
 * "网关还活着吗"是任何一次失败的第一个分支——但它不该抢占页面内容。
 *
 * 另外注意路由：这里用的是组件内状态，没引 router。原因是 gateway 用
 * `http.FileServer` 提供静态资源，对 `/versions` 这种深链会 404（没有 SPA
 * fallback）。改用状态切换就不必为此动网关，代价是 URL 不反映当前页——对一个
 * 控制台可接受，而 `/ops/` 那条由 gateway 自己的 mux 服务的路径也就绝不会被
 * 前端路由吞掉。
 */

type PageId = 'search' | 'documents' | 'versions' | 'history' | 'status'

const PAGES: ReadonlyArray<{ id: PageId; label: string }> = [
  { id: 'search', label: '检索' },
  { id: 'documents', label: '文档' },
  { id: 'versions', label: '版本' },
  { id: 'history', label: '历史' },
  { id: 'status', label: '系统状态' },
]

function healthLabel(status: HealthStatus | undefined): string {
  switch (status) {
    case 'HEALTH_STATUS_HEALTHY':
      return '健康'
    case 'HEALTH_STATUS_DEGRADED':
      return '降级'
    case 'HEALTH_STATUS_UNHEALTHY':
      return '不健康'
    case undefined:
      return '连接中…'
    default:
      return status
  }
}

function healthClass(status: HealthStatus | undefined): string {
  switch (status) {
    case 'HEALTH_STATUS_HEALTHY':
      return 'badge ok'
    case 'HEALTH_STATUS_DEGRADED':
      return 'badge warn'
    case 'HEALTH_STATUS_UNHEALTHY':
      return 'badge bad'
    default:
      return 'badge'
  }
}

export default function App() {
  const [page, setPage] = useState<PageId>('search')
  const [kbId, setKbId] = useState<string | null>(null)
  const [settingsOpen, setSettingsOpen] = useState(false)

  // 恢复上次选中的知识库。读设置失败不阻塞：退回"未选中"，用户重选一次即可
  // ——总比界面起不来强。
  useEffect(() => {
    void (async () => {
      const p = await loadPrefs()
      if (p.selected_kb_id !== null) setKbId(p.selected_kb_id)
    })()
  }, [])

  // 选中即持久化。写在 handler 里而不是 effect 里：effect 会在恢复阶段把刚读到
  // 的值再写回去一次，那一次写没有意义。
  function selectKb(next: string) {
    setKbId(next)
    void savePrefs({ selected_kb_id: next })
  }

  // 5 秒轮询健康：这是控制台唯一需要"自己动"的指标。
  const health = useHealth(5000)
  const status = useSystemStatus()

  // 存储层降级是只读用户唯一的降级信号（QueryResponse.storage_degraded 的注释
  // 写明了理由：低于 quorum 时读照样服务，不主动报出来就永远发现不了）。
  // 这里把它提到全局，因为它在任何一页都成立，而不是检索页独有的事实。
  const degraded = status.data?.health?.status === 'HEALTH_STATUS_DEGRADED'

  return (
    <div className="app">
      <nav className="sidebar" aria-label="主导航">
        <div className="brand">Stratum</div>
        {PAGES.map((p) => (
          <button
            key={p.id}
            type="button"
            className="nav-item"
            aria-current={p.id === page ? 'page' : undefined}
            onClick={() => setPage(p.id)}
          >
            {p.label}
          </button>
        ))}
        <div className="sidebar-foot">
          <a href="/ops/" className="muted small">
            运维控制台 →
          </a>
        </div>
      </nav>

      <main className="main">
        <div className="topbar">
          <h1 className="title">{PAGES.find((p) => p.id === page)?.label ?? ''}</h1>
          <div className="topbar-right">
            <KbPicker value={kbId} onChange={selectKb} />
            <span className={healthClass(health.data?.status)} title={health.data?.details ?? ''}>
              <span className="dot" />
              {health.isPending ? '连接中…' : healthLabel(health.data?.status)}
            </span>
            {/* 连哪个 gateway 是客户的部署决定，不该被编译进客户端。 */}
            <button
              type="button"
              className="icon-btn"
              title="连接设置：改 gateway 地址、看本机数据目录"
              onClick={() => setSettingsOpen(true)}
            >
              连接设置
            </button>
          </div>
        </div>

        {health.error !== null && (
          <p className="error-box">
            网关不可达：{health.error.message}
          </p>
        )}
        {degraded && (
          <p className="notice warn">
            集群健康状态为「降级」。读仍在服务，但副本掉得比要求的多——写入可能被拒。
          </p>
        )}

        {page === 'search' && <Search kbId={kbId} />}
        {page === 'documents' && <Documents kbId={kbId} />}
        {page === 'versions' && <Versions kbId={kbId} />}
        {page === 'history' && <History />}
        {page === 'status' && <SystemStatus />}

        <InFlightPanel />
      </main>

      <SettingsDialog open={settingsOpen} onClose={() => setSettingsOpen(false)} />
    </div>
  )
}
