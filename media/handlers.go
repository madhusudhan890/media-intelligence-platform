package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pion/webrtc/v4"
)

type OfferRequest struct {
	RoomID string `json:"roomId"`
	PeerID string `json:"peerId"`
	SDP    string `json:"sdp"`
}

type OfferResponse struct {
	SDP string `json:"sdp"`
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	// Count total connected signaling peers across all rooms
	totalPeers := 0
	sigManager.mu.RLock()
	activeRooms := len(sigManager.rooms)
	for _, room := range sigManager.rooms {
		totalPeers += len(room.Peers)
	}
	sigManager.mu.RUnlock()
	
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "ok",
		"activeRooms": activeRooms,
		"totalPeers":  totalPeers,
	})
}

func createRoomHandler(w http.ResponseWriter, r *http.Request) {
	roomID := sigManager.CreateRoom()
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"roomId": roomID,
	})
}

func getRoomHandler(w http.ResponseWriter, r *http.Request) {
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

func offerHandler(w http.ResponseWriter, r *http.Request) {
	var req OfferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	log.Printf("Received offer from Peer: %s in Room: %s", req.PeerID, req.RoomID)

	peer, err := NewPeer(req.RoomID, req.PeerID)
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
