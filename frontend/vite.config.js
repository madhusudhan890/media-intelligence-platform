import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 3000,
    strictPort: true
  },
  preview: {
    host: '0.0.0.0',
    port: 3000
  },
  define: {
    'import.meta.env.VITE_SIGNALING_URL': JSON.stringify(
      process.env.VITE_SIGNALING_URL || 'ws://localhost:8080/ws'
    ),
    'import.meta.env.VITE_MEDIA_SERVER_URL': JSON.stringify(
      process.env.VITE_MEDIA_SERVER_URL || 'http://localhost:8080'
    )
  }
});
