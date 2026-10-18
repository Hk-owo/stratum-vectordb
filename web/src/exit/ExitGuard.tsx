/**
 * 退出前的拦截与收尾。
 *
 * 两种运行环境能做到的事**不一样**，这里如实分开做：
 *
 * | | 桌面（Tauri） | 浏览器 |
 * |---|---|---|
 * | 拦下关闭 | `CloseRequested` + `preventDefault()` | `beforeunload` + `returnValue` |
 * | 提示文案 | 自定义对话框（能说清放弃了哪个版本） | 浏览器原生文案，改不了 |
 * | 放弃版本 | 确认之后、关窗之前真正执行并等结果 | 只能 `pagehide` 里 `keepalive` 尽力发，结果未知 |
 *
 * 浏览器为什么分两个事件：`beforeunload` 在用户**还没决定**的时候就跑了，谁也不
 * 知道他最后点的是"离开"还是"取消"。在那里发放弃请求，等于把"点了取消、继续干活"
 * 的用户的在途版本也一并放弃掉。`pagehide` 只在真正卸载时触发（`persisted` 为真
 * 表示进了 bfcache，页面还会回来），所以请求放那里。代价是拿不到结果——保留本机
 * 记录，下次启动「在途」面板还能看到。
 *
 * 桌面侧反而简单：Tauri 会 `await` `onCloseRequested` 的回调，所以可以等用户选择、
 * 等放弃请求返回、再 `destroy()`。
 */
import { useEffect, useRef, useState } from 'react'

import { apiBase, kbPath } from '../api/client'
import { isDesktop } from '../desktop/tauri'
import { discardForExit, type DiscardReport } from '../pending/discard'
import { cachedUnsettled, refreshUnsettled } from '../pending/store'
import { describeWork, hasWork, workSnapshot, type WorkSnapshot } from './activity'

type Phase =
  | { kind: 'confirm'; work: WorkSnapshot }
  | { kind: 'discarding' }
  | { kind: 'result'; report: DiscardReport }

export function ExitGuard() {
  const [phase, setPhase] = useState<Phase | null>(null)
  /** 对话框上的选择 → 交还给 `onCloseRequested` 里那个 await。 */
  const decide = useRef<((go: boolean) => void) | null>(null)
  /** 结果屏 → 真正关窗。 */
  const finish = useRef<(() => void) | null>(null)

  // 退出判据读的是同步镜像，所以启动时先把它填上。读失败不报错：镜像为空只会
  // 让这次退出少拦一次，不该因此让界面起不来。
  useEffect(() => {
    void refreshUnsettled().catch(() => {})
  }, [])

  // ---------------------------------------------------------------- 桌面
  useEffect(() => {
    if (!isDesktop()) return
    let unlisten: (() => void) | undefined
    let disposed = false

    void (async () => {
      // 动态 import：浏览器里根本没有这个模块的用处，静态引入会让 dev 构建
      // 也去解析 Tauri 的入口。
      const { getCurrentWindow } = await import('@tauri-apps/api/window')
      const win = getCurrentWindow()

      const off = await win.onCloseRequested(async (event) => {
        // 对话框已经开着：再点一次关闭只是重复请求，别把它变成一个并发流程
        // （两个 await 会抢同一个 resolver）。
        if (decide.current !== null || finish.current !== null) {
          event.preventDefault()
          return
        }

        const work = workSnapshot()
        if (!hasWork(work)) return // 干净：交给 Tauri 自己 destroy()

        event.preventDefault()
        setPhase({ kind: 'confirm', work })

        const go = await new Promise<boolean>((resolve) => {
          decide.current = resolve
        })
        decide.current = null
        if (!go) {
          setPhase(null)
          return
        }

        setPhase({ kind: 'discarding' })
        const pending = cachedUnsettled()
        let report: DiscardReport
        try {
          report = await discardForExit(pending)
        } catch (err) {
          // 退出路径上最不能发生的事：抛出去 → 窗口关不掉。所以整体失败也降级成
          // 一份"什么都没放弃、记录都还在"的报告，让用户仍然能点退出。
          report = {
            outcomes: [],
            cleared: [],
            kept: [...pending],
            error: err instanceof Error ? err.message : String(err),
          }
        }
        setPhase({ kind: 'result', report })

        // 结果给用户看一眼再走：放弃掉了哪些、哪些没放弃成、哪些记录留着
        // 下次还要处理。
        await new Promise<void>((resolve) => {
          finish.current = resolve
        })
        finish.current = null
        setPhase(null)
        await win.destroy()
      })

      if (disposed) off()
      else unlisten = off
    })().catch((err) => {
      // 注册不上就意味着拦不住关闭。说出来，比"以为有保护、其实没有"好。
      console.error('exit guard: 无法注册窗口关闭拦截', err)
    })

    return () => {
      disposed = true
      unlisten?.()
    }
  }, [])

  // ---------------------------------------------------------------- 浏览器
  useEffect(() => {
    if (isDesktop()) return

    function onBeforeUnload(e: BeforeUnloadEvent) {
      if (!hasWork(workSnapshot())) return
      // 只有给 returnValue 才会弹原生确认框；文案由浏览器定，改不了。
      e.preventDefault()
      e.returnValue = ''
    }

    function onPageHide(e: PageTransitionEvent) {
      if (e.persisted) return // 进 bfcache：页面还会回来，什么都不该放弃
      for (const w of cachedUnsettled()) {
        if (w.version_id === '') continue // 没有版本号，没有可放弃的对象
        // keepalive 让请求活过页面卸载。不 await（卸载时没有"之后"），
        // 也不删本机记录——送没送到只有服务端知道。
        void fetch(apiBase() + kbPath(w.knowledge_base_id, '/discard-version'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            knowledge_base_id: w.knowledge_base_id,
            version_id: w.version_id,
          }),
          keepalive: true,
        }).catch(() => {})
      }
    }

    window.addEventListener('beforeunload', onBeforeUnload)
    window.addEventListener('pagehide', onPageHide)
    return () => {
      window.removeEventListener('beforeunload', onBeforeUnload)
      window.removeEventListener('pagehide', onPageHide)
    }
  }, [])

  if (phase === null) return null

  return (
    <div
      className="modal-backdrop"
      // 退出流程里点背景等于"取消退出"：那是这里唯一安全的默认。
      // 放弃进行中与结果屏不响应——那两步没有"取消"可言。
      onClick={() => {
        if (phase.kind === 'confirm') decide.current?.(false)
      }}
    >
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        {phase.kind === 'confirm' && (
          <>
            <h2 className="modal-title">退出前确认</h2>
            {describeWork(phase.work).map((line) => (
              <p key={line}>{line}</p>
            ))}
            <p className="muted small">
              想留下它们就取消退出，等它们落地；继续退出的话，上面这些在途版本会被放弃，
              那批数据也不会进入知识库。
            </p>
            <div className="modal-actions">
              <button type="button" onClick={() => decide.current?.(false)}>
                取消退出
              </button>
              <button type="button" className="danger" onClick={() => decide.current?.(true)}>
                {/* 没有在途版本时不该说"放弃"——那时只有内存里的文件要丢。 */}
                {phase.work.unsettled.length > 0 ? '放弃并退出' : '仍然退出'}
              </button>
            </div>
          </>
        )}

        {phase.kind === 'discarding' && (
          <>
            <h2 className="modal-title">正在放弃在途版本…</h2>
            <p className="muted small">最多等 5 秒；没等到答复的记录会留在本机。</p>
          </>
        )}

        {phase.kind === 'result' && (
          <>
            <h2 className="modal-title">已收尾，可以退出</h2>
            {phase.report.error !== undefined && (
              <p className="notice warn">
                放弃过程出错了（{phase.report.error}）。本机记录一条都没动，下次启动仍会列出。
              </p>
            )}
            <ul className="status-list">
              {phase.report.outcomes.map((o) => (
                <li key={o.record.id}>
                  <span className={o.kind === 'unreachable' || o.kind === 'already-settled' ? 'badge warn' : 'badge ok'}>
                    <span className="dot" />
                    {o.kind === 'discarded'
                      ? '已放弃'
                      : o.kind === 'already-settled'
                        ? '已落盘'
                        : o.kind === 'unknown-version'
                          ? '未创建'
                          : '未确认'}
                  </span>{' '}
                  <span className="muted small">{o.note}</span>
                </li>
              ))}
            </ul>
            {phase.report.kept.length > 0 && (
              <p className="notice warn">
                有 {phase.report.kept.length} 条记录留在本机：下次启动会在「在途」面板里列出，
                可以重发或手动放弃。
              </p>
            )}
            <div className="modal-actions">
              <button type="button" onClick={() => finish.current?.()}>
                退出
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  )
}
