package main

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

type Peer struct {
	RoomID         string
	PeerID         string
	PC             *webrtc.PeerConnection
	AudioProcessor *AudioProcessor
	closeOnce      sync.Once
}

type PeerManager struct {
	peers map[string]*Peer
	mu    sync.RWMutex
}

func NewPeerManager() *PeerManager {
	return &PeerManager{
		peers: make(map[string]*Peer),
	}
}

func (m *PeerManager) AddPeer(p *Peer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Close existing peer if it exists
	if existing, ok := m.peers[p.PeerID]; ok {
		log.Printf("Closing existing peer connection for %s", p.PeerID)
		existing.Close()
	}
	
	m.peers[p.PeerID] = p
}

func (m *PeerManager) RemovePeer(peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.peers[peerID]; ok {
		p.Close()
		delete(m.peers, peerID)
	}
}

func (m *PeerManager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, p := range m.peers {
		p.Close()
		delete(m.peers, id)
	}
}

func NewPeer(roomID, peerID string) (*Peer, error) {
	if webrtcAPI == nil {
		return nil, errors.New("webrtc API not initialized")
	}

	pc, err := webrtcAPI.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
	})

	if err != nil {
		return nil, err
	}

	processor := NewAudioProcessor(roomID, peerID)

	peer := &Peer{
		RoomID:         roomID,
		PeerID:         peerID,
		PC:             pc,
		AudioProcessor: processor,
	}

	pc.OnICEConnectionStateChange(func(connectionState webrtc.ICEConnectionState) {
		log.Printf("ICE Connection State has changed to %s for peer %s\n", connectionState.String(), peerID)
		if connectionState == webrtc.ICEConnectionStateDisconnected ||
			connectionState == webrtc.ICEConnectionStateFailed ||
			connectionState == webrtc.ICEConnectionStateClosed {
			// Schedule cleanup on disconnect
			go func() {
				time.Sleep(5 * time.Second) // wait for potential reconnects before full tear down
				if peer.PC.ICEConnectionState() == webrtc.ICEConnectionStateDisconnected ||
					peer.PC.ICEConnectionState() == webrtc.ICEConnectionStateFailed ||
					peer.PC.ICEConnectionState() == webrtc.ICEConnectionStateClosed {
					log.Printf("Peer %s disconnected permanently, cleaning up", peerID)
					peerManager.RemovePeer(peerID)
				}
			}()
		}
	})

	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("Got track: %s (kind: %s) for peer %s\n", track.ID(), track.Kind(), peerID)

		if track.Kind() == webrtc.RTPCodecTypeVideo {
			log.Printf("Ignoring video track from peer %s", peerID)
			return
		}

		// Handle audio track
		peer.AudioProcessor.Start(track)
	})

	return peer, nil
}

func (p *Peer) Close() {
	p.closeOnce.Do(func() {
		log.Printf("Closing peer %s", p.PeerID)
		if p.AudioProcessor != nil {
			p.AudioProcessor.Stop()
		}
		if p.PC != nil {
			p.PC.Close()
		}
	})
}
