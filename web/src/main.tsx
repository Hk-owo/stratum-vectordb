import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import App from './App.tsx'
import { setApiBase } from './api/client'
import { desktop, isDesktop } from './desktop/tauri'
import './index.css'

const rootEl = document.getElementById('root')
if (rootEl === null) {
  // Vite's index.html owns this node. A missing one means the built asset and
  // the HTML no longer agree (e.g. dist/ served against a stale index.html),
  // so fail loudly instead of rendering into nothing.
  throw new Error('web/index.html is missing #root')
}

// 上面已判空；但 render() 是个闭包，TS 不在闭包里保留 narrowing，所以在这里
// 固定一次类型，免得每次引用都要再断言一遍。
const container: HTMLElement = rootEl

// 默认不重试：网关回的错误基本是语义性的（empty_changes / version_not_pending /
// index_not_ready），重试只会把同一个错误晚几秒再呈现一次。真正需要重试的是
// pending 层那条"等就绪"的循环，而它有服务端给的明确节奏（retry_after_ms）。
//
// refetchOnWindowFocus 关掉：这是控制台，切个窗口回来就重拉一遍会让人误以为
// 数据变了。
const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: false, refetchOnWindowFocus: false },
  },
})

function render() {
  createRoot(container).render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <App />
      </QueryClientProvider>
    </StrictMode>,
  )
}

/**
 * 启动前的一步：桌面壳里 WebView 不是同源的（`tauri://localhost`），必须先拿到
 * gateway 的绝对地址再渲染——否则第一个请求会打到应用自己身上，表现为一串
 * 莫名其妙的 404。
 *
 * 读设置失败**不阻塞启动**：退回默认地址，界面会显示"网关不可达"。
 */
async function bootstrap() {
  if (isDesktop()) {
    const settings = await desktop.loadSettings().catch(() => null)
    setApiBase(settings?.gateway_url ?? 'http://127.0.0.1:8081')
  }
  render()
}

void bootstrap()
