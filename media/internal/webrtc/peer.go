/*
Package webrtc manages the Pion WebRTC media server connections.
It handles incoming audio tracks and delegates RTP processing to the AudioProcessor.
*/
package webrtc

import (
	"errors"
	"log"
	"sync"
	"time"

	pionwebrtc "github.com/pion/webrtc/v4"
	"media-server/internal/kafka"
)

type Peer struct {
	RoomID         string
	PeerID         string
	PC             *pionwebrtc.PeerConnection
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

func NewPeer(roomID, peerID string, webrtcAPI *pionwebrtc.API, peerManager *PeerManager, kafkaProd *kafka.Producer) (*Peer, error) {
	if webrtcAPI == nil {
		return nil, errors.New("webrtc API not initialized")
	}

	pc, err := webrtcAPI.NewPeerConnection(pionwebrtc.Configuration{
		ICEServers: []pionwebrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
	})

	if err != nil {
		return nil, err
	}

	processor := NewAudioProcessor(roomID, peerID, kafkaProd)

	peer := &Peer{
		RoomID:         roomID,
		PeerID:         peerID,
		PC:             pc,
		AudioProcessor: processor,
	}

	pc.OnICEConnectionStateChange(func(connectionState pionwebrtc.ICEConnectionState) {
		log.Printf("ICE Connection State has changed to %s for peer %s\n", connectionState.String(), peerID)
		if connectionState == pionwebrtc.ICEConnectionStateDisconnected ||
			connectionState == pionwebrtc.ICEConnectionStateFailed ||
			connectionState == pionwebrtc.ICEConnectionStateClosed {
			// Schedule cleanup on disconnect
			go func() {
				time.Sleep(5 * time.Second) // wait for potential reconnects before full tear down
				if peer.PC.ICEConnectionState() == pionwebrtc.ICEConnectionStateDisconnected ||
					peer.PC.ICEConnectionState() == pionwebrtc.ICEConnectionStateFailed ||
					peer.PC.ICEConnectionState() == pionwebrtc.ICEConnectionStateClosed {
					log.Printf("Peer %s disconnected permanently, cleaning up", peerID)
					peerManager.RemovePeer(peerID)
				}
			}()
		}
	})

	pc.OnTrack(func(track *pionwebrtc.TrackRemote, receiver *pionwebrtc.RTPReceiver) {
		log.Printf("Got track: %s (kind: %s) for peer %s\n", track.ID(), track.Kind(), peerID)

		if track.Kind() == pionwebrtc.RTPCodecTypeVideo {
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
