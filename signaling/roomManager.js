const { v4: uuidv4 } = require('uuid');

function ts() {
  return new Date().toISOString();
}

function log(action, data = {}) {
  const dataStr = Object.keys(data).length > 0
    ? ' | ' + Object.entries(data).map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`).join(' ')
    : '';
  console.log(`[${ts()}] [RoomManager] ${action}${dataStr}`);
}

class RoomManager {
  constructor() {
    this.rooms = new Map();
    this.peerToRoom = new Map();
    log('Initialized');
  }

  createRoom() {
    const roomId = uuidv4();
    this.rooms.set(roomId, {
      id: roomId,
      peers: new Map(),
      createdAt: Date.now()
    });
    log('Room CREATED', { roomId, totalRooms: this.rooms.size });
    return roomId;
  }

  joinRoom(roomId, peerId, ws) {
    let room = this.rooms.get(roomId);

    if (!room) {
      log('Room not found — auto-creating', { roomId });
      room = {
        id: roomId,
        peers: new Map(),
        createdAt: Date.now()
      };
      this.rooms.set(roomId, room);
    }

    if (room.peers.has(peerId) && room.peers.get(peerId) !== ws) {
      log('Peer RECONNECT — replacing WebSocket', { peerId, roomId, peerCount: room.peers.size });
      room.peers.set(peerId, ws);
      this.peerToRoom.set(peerId, roomId);
      return { success: true, isReconnect: true, peers: this.getPeerIds(roomId) };
    }

    if (room.peers.size >= 2 && !room.peers.has(peerId)) {
      log('Room FULL — rejecting peer', { peerId, roomId, peerCount: room.peers.size });
      return { success: false, error: 'Room is full (max 2 peers)' };
    }

    room.peers.set(peerId, ws);
    this.peerToRoom.set(peerId, roomId);
    log('Peer JOINED', { peerId, roomId, peerCount: room.peers.size, totalPeers: this.peerToRoom.size });

    return { success: true, isReconnect: false, peers: this.getPeerIds(roomId) };
  }

  leaveRoom(peerId) {
    const roomId = this.peerToRoom.get(peerId);
    if (!roomId) {
      log('Leave — peer not in any room', { peerId });
      return { roomId: null, remainingPeers: [] };
    }

    const room = this.rooms.get(roomId);
    if (!room) {
      this.peerToRoom.delete(peerId);
      log('Leave — room not found (stale reference)', { peerId, roomId });
      return { roomId, remainingPeers: [] };
    }

    room.peers.delete(peerId);
    this.peerToRoom.delete(peerId);
    log('Peer LEFT', { peerId, roomId, remainingPeers: room.peers.size });

    const remainingPeers = this.getPeerIds(roomId);

    if (room.peers.size === 0) {
      this.rooms.delete(roomId);
      log('Room DESTROYED (empty)', { roomId, totalRooms: this.rooms.size });
    }

    return { roomId, remainingPeers };
  }

  getPeerSocket(roomId, peerId) {
    const room = this.rooms.get(roomId);
    if (!room) return null;
    return room.peers.get(peerId) || null;
  }

  getPeerIds(roomId) {
    const room = this.rooms.get(roomId);
    if (!room) return [];
    return Array.from(room.peers.keys());
  }

  getOtherPeers(roomId, peerId) {
    const peers = this.getPeerIds(roomId);
    return peers.filter(id => id !== peerId);
  }

  getRoomInfo(roomId) {
    const room = this.rooms.get(roomId);
    if (!room) return null;
    return {
      id: room.id,
      peerCount: room.peers.size,
      peers: this.getPeerIds(roomId),
      createdAt: room.createdAt
    };
  }

  getStats() {
    return {
      totalRooms: this.rooms.size,
      totalPeers: this.peerToRoom.size,
      rooms: Array.from(this.rooms.entries()).map(([id, room]) => ({
        id,
        peerCount: room.peers.size,
        peers: Array.from(room.peers.keys())
      }))
    };
  }
}

module.exports = RoomManager;
