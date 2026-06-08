const express = require('express');
const http = require('http');
const { v4: uuidv4 } = require('uuid');
const RoomManager = require('./roomManager');
const { setupWebSocket } = require('./websocket');

const PORT = process.env.PORT || 8080;

function ts() {
  return new Date().toISOString();
}

function log(category, action, data = {}) {
  const dataStr = Object.keys(data).length > 0
    ? ' | ' + Object.entries(data).map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`).join(' ')
    : '';
  console.log(`[${ts()}] [Server] [${category}] ${action}${dataStr}`);
}

const app = express();

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }
  next();
});

app.use(express.json());

app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    log('HTTP', `${req.method} ${req.url}`, { status: res.statusCode, durationMs: duration });
  });
  next();
});

const roomManager = new RoomManager();

app.get('/health', (req, res) => {
  const stats = roomManager.getStats();
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    ...stats
  });
});

app.post('/rooms', (req, res) => {
  const roomId = roomManager.createRoom();
  log('API', 'Room created via HTTP', { roomId });
  res.status(201).json({ roomId });
});

app.get('/rooms/:roomId', (req, res) => {
  const info = roomManager.getRoomInfo(req.params.roomId);
  if (!info) {
    log('API', 'Room not found via HTTP', { roomId: req.params.roomId });
    return res.status(404).json({ error: 'Room not found' });
  }
  log('API', 'Room info requested', { roomId: req.params.roomId, peerCount: info.peerCount });
  res.json(info);
});

const server = http.createServer(app);

const wss = setupWebSocket(server, roomManager);

server.listen(PORT, () => {
  log('STARTUP', '=== Signaling Server Started ===');
  log('STARTUP', `HTTP endpoint: http://localhost:${PORT}`);
  log('STARTUP', `Health check:  http://localhost:${PORT}/health`);
  log('STARTUP', `WebSocket:     ws://localhost:${PORT}`);
  log('STARTUP', `Environment:   ${process.env.NODE_ENV || 'development'}`);
  log('STARTUP', `Node.js:       ${process.version}`);
});

process.on('SIGTERM', () => {
  log('SHUTDOWN', 'SIGTERM received — shutting down gracefully');
  wss.clients.forEach((ws) => ws.close(1000, 'Server shutdown'));
  server.close(() => {
    log('SHUTDOWN', 'Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  log('SHUTDOWN', 'SIGINT received — shutting down gracefully');
  wss.clients.forEach((ws) => ws.close(1000, 'Server shutdown'));
  server.close(() => {
    log('SHUTDOWN', 'Server closed');
    process.exit(0);
  });
});

process.on('uncaughtException', (err) => {
  log('ERROR', 'Uncaught exception', { error: err.message, stack: err.stack });
  process.exit(1);
});

process.on('unhandledRejection', (reason) => {
  log('ERROR', 'Unhandled promise rejection', { reason: String(reason) });
});
