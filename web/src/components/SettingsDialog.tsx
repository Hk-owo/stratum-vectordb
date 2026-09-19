import { useEffect, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'

import { apiBase, setApiBase } from '../api/client'
import { desktop, isDesktop } from '../desktop/tauri'
import { storeLocation } from '../pending/store'

/**
 * 连接设置。
 *
 * 为什么需要它：桌面客户端不该把 gateway 的地址**编译进去**。客户可能把 gateway
 * 跑在另一台机器、另一个端口，或者临时指到一台测试环境上——那是他的部署，不是
 * 我们该替他决定的。
 *
 * 浏览器下这个字段是**只读**的，理由不是"懒得做"：浏览器里的前端就是由 gateway
 * 自己同源提供给它的（`-static`），地址由"你从哪个地址打开这个页面"决定；改成一个
 * 别的地址会跨域，而 gateway 有意不设 CORS 头。所以浏览器下唯一有意义的做法是
 * 换一个地址打开页面，这件事这里做不了。
 */
export function SettingsDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const qc = useQueryClient()
  const [url, setUrl] = useState('')
  const [saved, setSaved] = useState<string | null>(null)
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState<string | null>(null)
  const [dataDir, setDataDir] = useState<string | null>(null)

  useEffect(() => {
    if (!open) return
    setTestResult(null)
    setSaved(null)

    void (async () => {
      if (isDesktop()) {
        const s = await desktop.loadSettings().catch(() => null)
        setUrl(s?.gateway_url ?? (apiBase() || 'http://127.0.0.1:8081'))
        setDataDir(await storeLocation().catch(() => null))
      } else {
        setUrl(apiBase() || window.location.origin)
      }
    })()
  }, [open])

  if (!open) return null

  const editable = isDesktop()

  /** 试一下这个地址通不通——保存之前就该知道，而不是保存完发现连不上。 */
  async function test(target: string) {
    setTesting(true)
    setTestResult(null)
    try {
      const res = await fetch(`${target.replace(/\/+$/, '')}/api/health`)
      const body = (await res.json().catch(() => null)) as { status?: string } | null
      setTestResult(
        res.ok
          ? `通了：${body?.status ?? 'ok'}`
          : `网关回了 HTTP ${res.status}（地址对，但后端可能没起来）`,
      )
    } catch (err) {
      setTestResult(
        `连不上：${err instanceof Error ? err.message : String(err)}` +
          '（可能是地址错、服务没起，或浏览器下的跨域限制）',
      )
    } finally {
      setTesting(false)
    }
  }

  async function save() {
    const next = url.trim().replace(/\/+$/, '')
    if (next === '') return

    if (isDesktop()) {
      const s = (await desktop.loadSettings().catch(() => null)) ?? {
        schema_version: 1,
        gateway_url: next,
        selected_kb_id: null,
        top_k: 5,
        aggregation: 'AGGREGATION_METHOD_MEDIAN',
        durable_only: false,
      }
      await desktop.saveSettings({ ...s, gateway_url: next }).catch(() => undefined)
    }
    setApiBase(next)
    // 缓存里全是**上一个服务器**的数据，不清掉会拿旧库的列表去配新库。
    qc.clear()
    setSaved(`已切到 ${next}，正在重新拉取`)
    setTimeout(onClose, 700)
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2 className="modal-title">连接设置</h2>

        <label className="field">
          <span>gateway 地址</span>
          <input
            type="text"
            value={url}
            readOnly={!editable}
            disabled={!editable}
            placeholder="http://127.0.0.1:8081"
            onChange={(e) => setUrl(e.target.value)}
          />
        </label>

        {!editable && (
          <p className="muted small">
            浏览器里前端与网关**同源**（网关自己提供这个页面），所以地址由"你从哪个地址
            打开它"决定，改这里没有意义——跨域会被网关拒掉。要连别的服务器，换那个地址
            打开页面即可；或者用桌面客户端。
          </p>
        )}

        {editable && (
          <p className="muted small">
            客户端的后端由你决定跑在哪。填 gateway 的地址（默认 <code>http://127.0.0.1:8081</code>），
            保存后立即生效。
          </p>
        )}

        {testResult !== null && <p className="notice">{testResult}</p>}
        {saved !== null && <p className="notice warn">{saved}</p>}

        {dataDir !== null && (
          <p className="muted small">
            本机数据目录：<code>{dataDir}</code>
            <br />
            在途批次与本地设置都存这儿（<code>pending.json</code> / <code>settings.json</code>）——
            它是"重发还是放弃"的唯一依据，值得备份。
          </p>
        )}

        <div className="modal-actions">
          <button
            type="button"
            disabled={testing || url.trim() === ''}
            onClick={() => void test(url)}
          >
            {testing ? '测试中…' : '测试连接'}
          </button>
          <button type="button" disabled={!editable || url.trim() === ''} onClick={() => void save()}>
            保存
          </button>
          <button type="button" onClick={onClose}>
            关闭
          </button>
        </div>
      </div>
    </div>
  )
}
