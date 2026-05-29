package signaling

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for now
	},
}

const (
	pingPeriod = 25 * time.Second
	pongWait   = 30 * time.Second
)

func WSHandler(manager *Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("[Signaling] WebSocket upgrade error: %v", err)
			return
		}

		log.Printf("[Signaling] Client connected: %s", conn.RemoteAddr())

		conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(pongWait))
			return nil
		})

		var currentRoomID string
		var currentPeerID string

		// Ping goroutine
		ticker := time.NewTicker(pingPeriod)
		go func() {
			defer ticker.Stop()
			for range ticker.C {
				if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second)); err != nil {
					return
				}
			}
		}()

		defer func() {
			if currentRoomID != "" && currentPeerID != "" {
				manager.LeaveRoom(currentRoomID, currentPeerID)
			}
			conn.Close()
			log.Printf("[Signaling] Client disconnected: %s", conn.RemoteAddr())
		}()

		for {
			_, messageData, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("[Signaling] WebSocket read error: %v", err)
				}
				break
			}

			var msg map[string]interface{}
			if err := json.Unmarshal(messageData, &msg); err != nil {
				log.Printf("[Signaling] Invalid JSON received: %v", err)
				continue
			}

			msgType, _ := msg["type"].(string)
			roomID, _ := msg["roomId"].(string)
			peerID, _ := msg["peerId"].(string)
			targetPeerID, _ := msg["targetPeerId"].(string)

			if roomID == "" || peerID == "" {
				log.Printf("[Signaling] Invalid message: missing roomId or peerId")
				continue
			}

			// Handle Join
			if msgType == "join" {
				currentRoomID = roomID
				currentPeerID = peerID

				peers, isReconnect := manager.JoinRoom(roomID, peerID, conn)
				
				response := map[string]interface{}{
					"type":   "joined",
					"roomId": roomID,
					"peerId": peerID,
					"payload": map[string]interface{}{
						"peers":       peers,
						"isReconnect": isReconnect,
					},
				}
				
				if err := conn.WriteJSON(response); err != nil {
					log.Printf("[Signaling] Failed to send joined message: %v", err)
					break
				}
				continue
			}

			// Ensure peer has joined before relaying
			if currentRoomID == "" {
				log.Printf("[Signaling] Peer %s attempted to send message without joining", peerID)
				continue
			}

			// Relay SDP and ICE messages
			if msgType == "offer" || msgType == "answer" || msgType == "ice-candidate" {
				if targetPeerID == "" {
					log.Printf("[Signaling] Missing targetPeerId in %s message", msgType)
					continue
				}

				log.Printf("[Signaling] Relaying %s from %s to %s in room %s", msgType, peerID, targetPeerID, roomID)

				// Forward the message to the target peer.
				// peerId = the SENDER's ID so the receiver knows who it came from.
				// targetPeerId = the RECEIVER's ID (kept for context, but the receiver
				// uses peerId to identify the sender).
				relayMsg := map[string]interface{}{
					"type":         msgType,
					"roomId":       roomID,
					"peerId":       peerID,       // Sender's ID — receiver reads this to know who sent it
					"targetPeerId": targetPeerID, // Receiver's ID — for routing context
					"payload":      msg["payload"],
				}

				if err := manager.RelayMessage(roomID, targetPeerID, relayMsg); err != nil {
					log.Printf("[Signaling] Failed to relay %s from %s to %s: %v", msgType, peerID, targetPeerID, err)
				}
			}
		}
	}
}
