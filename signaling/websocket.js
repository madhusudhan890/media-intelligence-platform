const { WebSocketServer } = require('ws');

const HEARTBEAT_INTERVAL = 30000;

function ts() {
  return new Date().toISOString();
}

function log(category, action, data = {}) {
  const dataStr = Object.keys(data).length > 0
    ? ' | ' + Object.entries(data).map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`).join(' ')
    : '';
  console.log(`[${ts()}] [Signaling] [${category}] ${action}${dataStr}`);
}

function logSDP(direction, type, sdp) {
  if (!sdp || !sdp.sdp) {
    log('SDP', `${direction} ${type} (no SDP body)`);
    return;
  }
  const lines = sdp.sdp.split('\r\n');
  const mediaLines = lines.filter(l => l.startsWith('m='));
  const codecLines = lines.filter(l => l.startsWith('a=rtpmap:'));
  log('SDP', `${direction} ${type}`, {
    byteLength: sdp.sdp.length,
    mediaStreams: mediaLines.join(' | '),
    codecs: codecLines.slice(0, 4).join(' | ')
  });
  console.log(`  SDP first 5 lines: ${lines.slice(0, 5).join(' | ')}`);
}

function logICE(direction, candidate) {
  if (!candidate || !candidate.candidate) {
    log('ICE', `${direction} (null candidate — gathering complete)`);
    return;
  }
  const parts = candidate.candidate.split(' ');
  log('ICE', `${direction}`, {
    sdpMid: candidate.sdpMid,
    sdpMLineIndex: candidate.sdpMLineIndex,
    candidate: candidate.candidate.substring(0, 100)
  });
}

function setupWebSocket(server, roomManager) {
  const wss = new WebSocketServer({ server });

  const heartbeatInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) {
        log('HEARTBEAT', 'Terminating inactive connection', { peerId: ws.peerId });
        return ws.terminate();
      }
      ws.isAlive = false;
      ws.ping();
    });
  }, HEARTBEAT_INTERVAL);

  wss.on('close', () => {
    clearInterval(heartbeatInterval);
    log('SERVER', 'WebSocket server closed');
  });

  wss.on('connection', (ws, req) => {
    ws.peerId = null;
    ws.isAlive = true;
    const clientIP = req.socket.remoteAddress;
    log('CONNECTION', 'New WebSocket connection', { clientIP, totalClients: wss.clients.size });

    ws.on('pong', () => {
      ws.isAlive = true;
    });

    ws.on('message', (data) => {
      let message;
      try {
        message = JSON.parse(data);
      } catch (err) {
        log('CONNECTION', 'ERROR: Invalid JSON received', { error: err.message, clientIP });
        sendError(ws, 'Invalid JSON message');
        return;
      }

      handleMessage(ws, message, roomManager);
    });

    ws.on('close', (code, reason) => {
      log('CONNECTION', 'WebSocket closed', { peerId: ws.peerId, code, reason: reason.toString() || 'none' });
      handleDisconnect(ws, roomManager);
    });

    ws.on('error', (err) => {
      log('CONNECTION', 'WebSocket ERROR', { peerId: ws.peerId, error: err.message });
    });
  });

  log('SERVER', 'WebSocket server initialized', { heartbeatInterval: HEARTBEAT_INTERVAL });
  return wss;
}

function handleMessage(ws, message, roomManager) {
  const { type, roomId, peerId, targetPeerId, payload } = message;

  log('MESSAGE', `Received: ${type}`, { from: peerId, room: roomId, target: targetPeerId || 'N/A' });

  switch (type) {
    case 'join':
      handleJoin(ws, roomId, peerId, roomManager);
      break;
    case 'offer':
      log('RELAY', 'Relaying OFFER');
      logSDP('RELAY', 'offer', payload?.sdp);
      relayMessage(ws, message, roomManager);
      break;
    case 'answer':
      log('RELAY', 'Relaying ANSWER');
      logSDP('RELAY', 'answer', payload?.sdp);
      relayMessage(ws, message, roomManager);
      break;
    case 'ice-candidate':
      logICE('RELAY', payload?.candidate);
      relayMessage(ws, message, roomManager);
      break;
    default:
      log('MESSAGE', 'Unknown message type', { type });
      sendError(ws, `Unknown message type: ${type}`);
  }
}

function handleJoin(ws, roomId, peerId, roomManager) {
  if (!roomId || !peerId) {
    log('JOIN', 'ERROR: Missing roomId or peerId');
    sendError(ws, 'roomId and peerId are required');
    return;
  }

  log('JOIN', 'Processing join request', { peerId, roomId });

  const previousPeerId = ws.peerId;
  if (previousPeerId && previousPeerId !== peerId) {
    log('JOIN', 'Cleaning up previous peer identity', { previousPeerId });
    roomManager.leaveRoom(previousPeerId);
  }

  ws.peerId = peerId;
  ws.isAlive = true;

  const result = roomManager.joinRoom(roomId, peerId, ws);

  if (!result.success) {
    log('JOIN', 'REJECTED', { peerId, roomId, reason: result.error });
    sendMessage(ws, {
      type: 'error',
      payload: { message: result.error }
    });
    return;
  }

  sendMessage(ws, {
    type: 'joined',
    roomId,
    peerId,
    payload: {
      peers: result.peers,
      isReconnect: result.isReconnect
    }
  });
  log('JOIN', 'SUCCESS — sent joined confirmation', { peerId, roomId, peers: result.peers, isReconnect: result.isReconnect });

  const otherPeers = roomManager.getOtherPeers(roomId, peerId);
  otherPeers.forEach((otherPeerId) => {
    const otherWs = roomManager.getPeerSocket(roomId, otherPeerId);
    if (otherWs && otherWs.readyState === otherWs.OPEN) {
      sendMessage(otherWs, {
        type: 'peer-joined',
        roomId,
        peerId: otherPeerId,
        payload: { peerId: peerId }
      });
      log('JOIN', 'Notified existing peer', { notified: otherPeerId, about: peerId });
    }
  });
}

function relayMessage(ws, message, roomManager) {
  const { type, roomId, peerId, targetPeerId, payload } = message;

  if (!roomId || !peerId || !targetPeerId) {
    log('RELAY', 'ERROR: Missing required fields', { roomId, peerId, targetPeerId });
    sendError(ws, 'roomId, peerId, and targetPeerId are required for relay');
    return;
  }

  const targetWs = roomManager.getPeerSocket(roomId, targetPeerId);

  if (!targetWs || targetWs.readyState !== targetWs.OPEN) {
    log('RELAY', 'FAILED — target peer unavailable', { type, targetPeerId, roomId });
    sendMessage(ws, {
      type: 'error',
      payload: { message: `Peer ${targetPeerId} is not available` }
    });
    return;
  }

  sendMessage(targetWs, { type, roomId, peerId, targetPeerId, payload });
  log('RELAY', `SUCCESS — ${type} delivered`, { from: peerId, to: targetPeerId });
}

function handleDisconnect(ws, roomManager) {
  if (!ws.peerId) {
    log('DISCONNECT', 'Anonymous connection closed (no peerId)');
    return;
  }

  const peerId = ws.peerId;
  const { roomId, remainingPeers } = roomManager.leaveRoom(peerId);

  log('DISCONNECT', 'Peer disconnected', { peerId, roomId: roomId || 'none', remainingPeers });

  if (roomId) {
    remainingPeers.forEach((remainingPeerId) => {
      const peerWs = roomManager.getPeerSocket(roomId, remainingPeerId);
      if (peerWs && peerWs.readyState === peerWs.OPEN) {
        sendMessage(peerWs, {
          type: 'peer-left',
          roomId,
          payload: { peerId }
        });
        log('DISCONNECT', 'Sent peer-left notification', { to: remainingPeerId, about: peerId });
      }
    });
  }
}

function sendMessage(ws, message) {
  if (ws.readyState === ws.OPEN) {
    try {
      const json = JSON.stringify(message);
      ws.send(json);
      log('SEND', `Sent: ${message.type}`, { to: ws.peerId || 'unknown', byteLength: json.length });
    } catch (err) {
      log('SEND', 'ERROR: Failed to send message', { error: err.message, type: message.type });
    }
  } else {
    log('SEND', 'SKIPPED — WebSocket not open', { type: message.type, readyState: ws.readyState });
  }
}

function sendError(ws, errorMessage) {
  log('ERROR', 'Sending error to client', { message: errorMessage });
  sendMessage(ws, {
    type: 'error',
    payload: { message: errorMessage }
  });
}

module.exports = { setupWebSocket };
