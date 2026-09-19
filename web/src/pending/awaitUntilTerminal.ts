/**
 * 「提交 → 等就绪」里那个「等」的循环。
 *
 * 节奏听服务端：每次 AwaitVersion 都会回一个 retry_after_ms，我们照它等，
 * 不自己拍一个固定间隔——服务端知道版本落在哪个阶段、构建池有多挤。
 *
 * 每一次回答都交给 decide()，它才是"下一步做什么"的唯一判据。这个循环只
 * 负责重复问、以及把人能取消这件事做对。
 */
import { awaitVersionOnce } from '../api/queries'
import { AwaitTarget, type AwaitVersionResponse } from '../api/gen/knowledgebase'
import { decide, type AwaitTargetKind, type Decision } from './decide'

export interface AwaitOutcome {
  decision: Decision
  /** 最后一次回答，界面上用来显示 stage / data_missing。 */
  last: AwaitVersionResponse
  /** 问了多少次。 */
  rounds: number
}

export interface AwaitOptions {
  /**
   * 本地是否还留着这批 changes。决定终局时是 RESEND 还是 DISCARD——
   * 服务端永远不会替调用方回答这个问题。
   */
  haveChanges: boolean
  /**
   * 等到哪一步算数。默认 INDEX_READY。
   *
   * 它同时决定两件事，必须成对：发给服务端的 `AwaitTarget`，以及 `decide` 用哪套
   * 判据。只改一边就会出现"服务端说到了、前端说继续等"的空转。
   */
  target?: AwaitTargetKind
  /** 每问一次回调一次，用于界面上的进度显示。 */
  onProgress?: (resp: AwaitVersionResponse) => void
  /** 取消。切页/用户点取消时用它，别让轮询在后台一直跑。 */
  signal?: AbortSignal
  /**
   * 总的等待上限。到了就返回当前判定（通常是 WAIT），由调用方决定是否
   * 重新发起——循环本身不该无限期占着一个标签页。
   * 默认 10 分钟。
   */
  maxTotalMs?: number
  /** 每次 AwaitVersion 请求内服务端最长等多久。默认 30s（服务端还会再 cap）。 */
  perCallTimeoutMs?: number
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms)
    signal?.addEventListener(
      'abort',
      () => {
        clearTimeout(timer)
        reject(signal.reason ?? new DOMException('aborted', 'AbortError'))
      },
      { once: true },
    )
  })
}

export async function awaitUntilTerminal(
  kbId: string,
  versionId: string,
  opts: AwaitOptions,
): Promise<AwaitOutcome> {
  const maxTotalMs = opts.maxTotalMs ?? 10 * 60 * 1000
  const perCallTimeoutMs = opts.perCallTimeoutMs ?? 30_000
  const target: AwaitTargetKind = opts.target ?? 'INDEX_READY'
  const deadline = Date.now() + maxTotalMs

  let rounds = 0
  let last: AwaitVersionResponse | undefined

  while (Date.now() < deadline) {
    opts.signal?.throwIfAborted()

    last = await awaitVersionOnce(kbId, {
      knowledge_base_id: kbId,
      version_id: versionId,
      // 送出去的 target 与 decide 用的判据是**同一个**变量：一边改一边不改，
      // 就会变成"服务端说到了、前端说继续等"的空转。
      // 用 enum 成员而不是字面量：TS 的字符串 enum 是标称类型，字面量不能赋值。
      target:
        target === 'DATA_DURABLE'
          ? AwaitTarget.AWAIT_TARGET_DATA_DURABLE
          : AwaitTarget.AWAIT_TARGET_INDEX_READY,
      // int64 在 protojson 里是字符串（见 gen/README.md），所以这里要转。
      wait_timeout_ms: String(perCallTimeoutMs),
    })
    rounds += 1
    opts.onProgress?.(last)

    const decision = decide(last, opts.haveChanges, target)
    if (decision !== 'WAIT') {
      return { decision, last, rounds }
    }

    // retry_after_ms 是服务端的建议（int64 → 字符串，转回数字再比较）；
    // 缺失或为 0 时给个下限，避免热轮询。
    const waitMs = Math.max(Number(last.retry_after_ms), 250)
    await sleep(Math.min(waitMs, Math.max(deadline - Date.now(), 0)), opts.signal)
  }

  // 超时：最后一次回答已经有了，判定交回调用方（继续等就得重新发起）。
  opts.signal?.throwIfAborted()
  if (last === undefined) {
    // maxTotalMs <= 0 之类的退化输入：一次都没问过。
    throw new Error('awaitUntilTerminal: no result within the time budget')
  }
  return { decision: decide(last, opts.haveChanges, target), last, rounds }
}
