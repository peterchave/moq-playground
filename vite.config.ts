import { defineConfig } from 'vite';
import { resolve } from 'path';

export default defineConfig({
  server: {
    allowedHosts: true,
    fs: {
      strict: false,
    },
  },
  resolve: {
    alias: {
      '@moqt/transport': resolve(__dirname, 'src/transport/index.ts'),
      '@moqt/webtransport': resolve(__dirname, 'src/webtransport/index.ts'),
    },
  },
  build: {
    rollupOptions: {
      input: resolve(__dirname, 'index.html'),
    },
  },
});
