import react from '@vitejs/plugin-react'
import { defineConfig, mergeConfig } from 'vite'
import { defineConfig as defineVitest } from 'vitest/config'

/** Data plane: `GET /health`, `POST /v1/events`, `GET /v1/stream`, other `/v1/*` not handled by control. */
const dataPlaneTarget =
  process.env.VITE_DATA_PLANE_PROXY_TARGET ?? 'http://127.0.0.1:8080'
/**
 * Python control plane: `GET /v1/control/*`, `/v1/adapter-instances*`.
 * Defaults to :8091 (matches `ingestion-control-plane` Dockerfile).
 */
const controlPlaneTarget =
  process.env.VITE_CONTROL_PLANE_PROXY_TARGET ?? 'http://127.0.0.1:8091'

export default mergeConfig(
  defineConfig({
    esbuild: { jsx: 'automatic' },
    resolve: { dedupe: ['react', 'react-dom'] },
    plugins: [react()],
    server: {
      // Prefix order: first match wins (Vite `proxyMiddleware`); keep `/v1` after control paths.
      proxy: {
        '/health': { target: dataPlaneTarget, changeOrigin: true },
        '/v1/control': { target: controlPlaneTarget, changeOrigin: true },
        '/v1/adapter-instances': { target: controlPlaneTarget, changeOrigin: true },
        '/v1': { target: dataPlaneTarget, changeOrigin: true },
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
