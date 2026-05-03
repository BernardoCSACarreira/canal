import react from '@vitejs/plugin-react'
import { defineConfig, mergeConfig } from 'vite'
import { defineConfig as defineVitest } from 'vitest/config'

const apiTarget = process.env.VITE_API_PROXY_TARGET ?? 'http://127.0.0.1:8080'

export default mergeConfig(
  defineConfig({
    esbuild: { jsx: 'automatic' },
    resolve: { dedupe: ['react', 'react-dom'] },
    plugins: [react()],
    server: {
      proxy: {
        '/health': { target: apiTarget, changeOrigin: true },
        '/v1': { target: apiTarget, changeOrigin: true },
      },
    },
  }),
  defineVitest({
    test: {
      environment: 'jsdom',
      setupFiles: ['./test/setup.ts'],
      include: ['test/**/*.test.{ts,tsx}'],
    },
  }),
)
