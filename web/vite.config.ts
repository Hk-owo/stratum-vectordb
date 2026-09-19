import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 构建产物交给 stratum-gateway 的 -static 使用（web/dist）。
// 网关同源提供静态资源与 /api/*，所以生产环境没有 CORS 问题。
//
// dev 模式下把 /api 与 /ops 代理到网关，让开发期与生产期走同一条调用路径
// ——否则前端在 dev 下"能跑"、切到网关就跨域失败。
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:8081',
      '/ops': 'http://127.0.0.1:8081',
    },
  },
})
