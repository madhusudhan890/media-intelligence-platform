package main

import (
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type SignalingPeer struct {
	ID   string
	Conn *websocket.Conn
	mu   sync.Mutex // Protects concurrent writes to WebSocket
}

func (p *SignalingPeer) Send(message interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Conn.WriteJSON(message)
}

type Room struct {
	ID        string
	Peers     map[string]*SignalingPeer
	CreatedAt time.Time
}

type SignalingManager struct {
	rooms map[string]*Room
	mu    sync.RWMutex
}

var sigManager = NewSignalingManager()

func NewSignalingManager() *SignalingManager {
	return &SignalingManager{
		rooms: make(map[string]*Room),
	}
}

func (m *SignalingManager) CreateRoom() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	roomID := uuid.New().String()
	m.rooms[roomID] = &Room{
		ID:        roomID,
		Peers:     make(map[string]*SignalingPeer),
		CreatedAt: time.Now(),
	}
	
	log.Printf("[Signaling] Room created: %s", roomID)
	return roomID
}

func (m *SignalingManager) GetRoomInfo(roomID string) (map[string]interface{}, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	room, exists := m.rooms[roomID]
	if !exists {
		return nil, false
	}
	
	peerIDs := make([]string, 0, len(room.Peers))
	for id := range room.Peers {
		peerIDs = append(peerIDs, id)
	}
	
	return map[string]interface{}{
		"roomId":    room.ID,
		"peerCount": len(room.Peers),
		"peers":     peerIDs,
		"createdAt": room.CreatedAt,
	}, true
}

func (m *SignalingManager) EnsureRoomExists(roomID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if _, exists := m.rooms[roomID]; !exists {
		log.Printf("[Signaling] Auto-creating room %s on join", roomID)
		m.rooms[roomID] = &Room{
			ID:        roomID,
			Peers:     make(map[string]*SignalingPeer),
			CreatedAt: time.Now(),
		}
	}
}

func (m *SignalingManager) JoinRoom(roomID, peerID string, conn *websocket.Conn) ([]string, bool) {
	m.EnsureRoomExists(roomID)
	
	m.mu.Lock()
	defer m.mu.Unlock()
	
	room := m.rooms[roomID]
	isReconnect := false
	
	if existing, ok := room.Peers[peerID]; ok {
		log.Printf("[Signaling] Peer %s reconnecting to room %s", peerID, roomID)
		existing.Conn.Close()
		isReconnect = true
	}
	
	peer := &SignalingPeer{
		ID:   peerID,
		Conn: conn,
	}
	room.Peers[peerID] = peer
	
	// Get current peers
	peerIDs := make([]string, 0, len(room.Peers))
	for id := range room.Peers {
		peerIDs = append(peerIDs, id)
	}
	
	// Notify others in room
	for id, otherPeer := range room.Peers {
		if id != peerID {
			msg := map[string]interface{}{
				"type":   "peer-joined",
				"roomId": roomID,
				"peerId": id,
				"payload": map[string]string{
					"peerId": peerID,
				},
			}
			go otherPeer.Send(msg)
		}
	}
	
	return peerIDs, isReconnect
}

func (m *SignalingManager) LeaveRoom(roomID, peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	room, exists := m.rooms[roomID]
	if !exists {
		return
	}
	
	if peer, ok := room.Peers[peerID]; ok {
		peer.Conn.Close()
		delete(room.Peers, peerID)
		
		log.Printf("[Signaling] Peer %s left room %s", peerID, roomID)
		
		// Notify others
		for id, otherPeer := range room.Peers {
			msg := map[string]interface{}{
				"type":   "peer-left",
				"roomId": roomID,
				"peerId": id,
				"payload": map[string]string{
					"peerId": peerID,
				},
			}
			go otherPeer.Send(msg)
		}
		
		// Clean up empty room
		if len(room.Peers) == 0 {
			log.Printf("[Signaling] Room %s is empty, removing", roomID)
			delete(m.rooms, roomID)
		}
	}
}

func (m *SignalingManager) RelayMessage(roomID, targetPeerID string, message map[string]interface{}) error {
	m.mu.RLock()
	room, exists := m.rooms[roomID]
	if !exists {
		m.mu.RUnlock()
		return nil // Ignore if room doesn't exist
	}
	
	target, ok := room.Peers[targetPeerID]
	m.mu.RUnlock() // Release lock early before sending
	
	if ok {
		return target.Send(message)
	}
	
	return nil
}

func (m *SignalingManager) CleanupStaleRooms() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	now := time.Now()
	for id, room := range m.rooms {
		if len(room.Peers) == 0 && now.Sub(room.CreatedAt) > 24*time.Hour {
			log.Printf("[Signaling] Cleaning up stale room %s", id)
			delete(m.rooms, id)
		}
	}
}
