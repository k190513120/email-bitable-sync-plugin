import { defineConfig } from 'vite';

// https://vitejs.dev/config/
export default defineConfig({
  base:'./',
  server: {
    host: '0.0.0.0',
    proxy: {
      '/api/feishu': {
        target: 'https://apaas.feishu.cn',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/feishu/, '/ai/api/v1/skill_runtime/namespaces/spring_0385727c0f__c/trigger/00edccme'),
        configure: (proxy, options) => {
          proxy.on('proxyReq', (proxyReq, req, res) => {
            console.log('代理请求:', req.method, req.url);
          });
          proxy.on('proxyRes', (proxyRes, req, res) => {
            console.log('代理响应:', proxyRes.statusCode, req.url);
          });
        }
      }
    }
  }
})
