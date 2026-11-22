import { useState } from 'react'

import { useOpsNodes } from '../api/ops'
import { DockerDisabledCard, DockerPanel } from './ops/DockerPanel'
import { NodeConfigPanel } from './ops/NodeConfigPanel'
import { NodeServices } from './ops/NodeServices'
import { Badge, reachabilityBadge, reason } from './ops/shared'

type OpsSection = 'all' | 'services' | 'config' | 'docker'

/**
 * 运维页：集群节点选择 + 节点服务控制/日志 + 启动参数 + Docker 集群编排。
 * 拆分为多个子模块，支持顶部视角过滤。
 */
export function Ops() {
  const nodes = useOpsNodes()
  const [picked, setPicked] = useState<number | null>(null)
  const [section, setSection] = useState<OpsSection>('all')

  const list = nodes.data?.nodes ?? []
  const localId = nodes.data?.local_node_id ?? null
  const target = list.some((n) => n.id === picked) ? picked : localId

  const showServices = section === 'all' || section === 'services'
  const showConfig = section === 'all' || section === 'config'
  const showDocker = section === 'all' || section === 'docker'

  return (
    <div className="page">
      <div className="toolbar">
        <div className="pill-tabs" role="tablist" aria-label="运维视图切换">
          {(
            [
              ['all', '全部视图'],
              ['services', '节点与服务'],
              ['config', '启动参数'],
              ['docker', 'Docker 集群'],
            ] as const
          ).map(([id, label]) => (
            <button
              key={id}
              type="button"
              role="tab"
              className="pill-tab"
              aria-selected={section === id}
              onClick={() => setSection(id)}
            >
              {label}
            </button>
          ))}
        </div>
        <span className="muted small">
          当前目标节点：{target === null ? '—' : `node ${target}`}
        </span>
      </div>

      {nodes.error !== null && (
        <p className="error-box">读取集群节点失败：{reason(nodes.error)}</p>
      )}

      {(showServices || showConfig) && (
        <div className="status-section">
          <div className="status-head">
            <strong>集群节点</strong>
            <span className="chip">{list.length}</span>
          </div>
          {list.length === 0 && <p className="muted small">读取节点列表…</p>}
          {list.length > 0 && (
            <div className="table-wrap">
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
                        node {n.id} {n.local === true && <span className="chip accent">本机</span>}
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
                          className={n.id === target ? 'btn-primary' : undefined}
                          disabled={n.id === target}
                          onClick={() => setPicked(n.id)}
                        >
                          {n.id === target ? '管理中' : '管理'}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <p className="muted small">
            选中的节点决定「服务」与「启动参数」作用在哪台主机；Docker 集群始终是集群级控制。
          </p>
        </div>
      )}

      {showServices && <NodeServices nodeId={target} localNodeId={localId} />}
      {showConfig && <NodeConfigPanel nodeId={target} localNodeId={localId} />}
      {showDocker && <DockerPanel />}
    </div>
  )
}

export { DockerDisabledCard }
