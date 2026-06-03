/*
Package api provides HTTP handler functions for the Media Intelligence Platform.
It handles REST endpoints for room management, health checks, and WebRTC SDP offer negotiation.
*/
package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pion/webrtc/v4"
	"media-server/internal/signaling"
	mywebrtc "media-server/internal/webrtc"
	"media-server/internal/kafka"
)

type OfferRequest struct {
	RoomID string `json:"roomId"`
	PeerID string `json:"peerId"`
	Name   string `json:"name"`
	SDP    string `json:"sdp"`
}

type OfferResponse struct {
	SDP string `json:"sdp"`
}

func HealthHandler(sigManager *signaling.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		
		activeRooms, totalPeers := sigManager.GetStats()
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "ok",
			"activeRooms": activeRooms,
			"totalPeers":  totalPeers,
		})
	}
}

func CreateRoomHandler(sigManager *signaling.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		roomID := sigManager.CreateRoom()
		
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"roomId": roomID,
		})
	}
}

func GetRoomHandler(sigManager *signaling.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		roomID := vars["roomId"]
		
		roomInfo, exists := sigManager.GetRoomInfo(roomID)
		if !exists {
			http.Error(w, "Room not found", http.StatusNotFound)
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(roomInfo)
	}
}

func OfferHandler(webrtcAPI *webrtc.API, peerManager *mywebrtc.PeerManager, kafkaProd *kafka.Producer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req OfferRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		log.Printf("Received offer from Peer: %s in Room: %s", req.PeerID, req.RoomID)

		name := req.Name
		if name == "" {
			name = "Anonymous"
		}

		peer, err := mywebrtc.NewPeer(req.RoomID, req.PeerID, name, webrtcAPI, peerManager, kafkaProd)
		if err != nil {
			log.Printf("Failed to create peer: %v", err)
			http.Error(w, "Failed to create peer", http.StatusInternalServerError)
			return
		}

		peerManager.AddPeer(peer)

		offer := webrtc.SessionDescription{
			Type: webrtc.SDPTypeOffer,
			SDP:  req.SDP,
		}

		if err := peer.PC.SetRemoteDescription(offer); err != nil {
			log.Printf("Failed to set remote description: %v", err)
			http.Error(w, "Failed to set remote description", http.StatusInternalServerError)
			return
		}

		answer, err := peer.PC.CreateAnswer(nil)
		if err != nil {
			log.Printf("Failed to create answer: %v", err)
			http.Error(w, "Failed to create answer", http.StatusInternalServerError)
			return
		}
		
		// Create channels that will resolve when the gather is complete
		gatherComplete := webrtc.GatheringCompletePromise(peer.PC)

		if err = peer.PC.SetLocalDescription(answer); err != nil {
			log.Printf("Failed to set local description: %v", err)
			http.Error(w, "Failed to set local description", http.StatusInternalServerError)
			return
		}

		// Block until ICE Gathering is complete
		<-gatherComplete

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(OfferResponse{
			SDP: peer.PC.LocalDescription().SDP,
		})
	}
}

func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
