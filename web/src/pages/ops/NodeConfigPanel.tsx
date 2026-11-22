import { useState } from 'react'

import {
  useNodeConfig,
  useSaveNodeConfig,
  type EmbedConfig,
  type OpsConfig,
  type StratumConfig,
  type Tone,
  type VecstoreConfig,
} from '../../api/ops'
import { NumField, TextField, num, reason } from './shared'

export function NodeConfigPanel({
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

  const [loadedFor, setLoadedFor] = useState<number | null>(nodeId)
  if (loadedFor !== nodeId) {
    setLoadedFor(nodeId)
    setEdited(null)
    setNote(null)
  }

  if (nodeId === null) return null

  const draft = edited ?? cfg.data ?? null
  const dirty = edited !== null

  function up(patch: Partial<OpsConfig>) {
    setEdited((d) => {
      const base = d ?? cfg.data
      return base === undefined ? null : { ...base, ...patch }
    })
  }

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
      setEdited(null)
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
          <summary>基础路径（console.yaml）</summary>
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

          <p className="muted small">raft peers</p>
          <div className="table-wrap">
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
          </div>
          <div className="actions" style={{ marginTop: 8 }}>
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
          <summary>raft 定时 / 索引 / 协调器（留空 = 保持原值）</summary>
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
      <div className="toolbar">
        <div className="status-head">
          <strong>启动参数</strong>
          <span className="chip">node {nodeId}</span>
          {dirty && <span className="chip warn">有未保存修改</span>}
        </div>
        {draft !== null && (
          <div className="actions">
            <button
              type="button"
              className={dirty ? 'btn-primary' : undefined}
              disabled={save.isPending || !dirty}
              title={dirty ? '保存修改到 console.yaml' : '修改任意字段后即可保存'}
              onClick={() => void submit()}
            >
              {save.isPending ? '保存中…' : dirty ? '保存参数' : '保存参数（无修改）'}
            </button>
            <button
              type="button"
              disabled={save.isPending || !dirty}
              onClick={() => {
                setEdited(null)
                setNote(null)
              }}
            >
              放弃修改
            </button>
          </div>
        )}
      </div>

      <p className="muted small">
        写进该节点的 <code>console.yaml</code>，<b>下次启动或重启后生效</b>；<code>bin_dir</code> 与{' '}
        <code>cluster</code> 等执行路径项由部署脚本控制，不允许通过 HTTP 改写。
      </p>

      {body}

      {note !== null && (
        <p className={note.tone === 'bad' ? 'error-box' : 'notice'}>{note.text}</p>
      )}
    </div>
  )
}
