package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
)

type AudioProcessor struct {
	RoomID     string
	PeerID     string
	buffer     []byte
	mu         sync.Mutex
	stopChan   chan struct{}
	ticker     *time.Ticker
	processing bool
}

func NewAudioProcessor(roomID, peerID string) *AudioProcessor {
	return &AudioProcessor{
		RoomID:   roomID,
		PeerID:   peerID,
		buffer:   make([]byte, 0),
		stopChan: make(chan struct{}),
	}
}

func (ap *AudioProcessor) Start(track *webrtc.TrackRemote) {
	ap.mu.Lock()
	if ap.processing {
		ap.mu.Unlock()
		return
	}
	ap.processing = true
	ap.ticker = time.NewTicker(5 * time.Second)
	ap.mu.Unlock()

	log.Printf("Starting AudioProcessor for peer %s", ap.PeerID)

	go ap.processLoop()
	go ap.readRTP(track)
}

func (ap *AudioProcessor) Stop() {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	if !ap.processing {
		return
	}
	log.Printf("Stopping AudioProcessor for peer %s", ap.PeerID)
	ap.processing = false
	if ap.ticker != nil {
		ap.ticker.Stop()
	}
	close(ap.stopChan)
	// Flush remaining buffer
	ap.flushBuffer()
}

func (ap *AudioProcessor) readRTP(track *webrtc.TrackRemote) {
	for {
		// Read RTP packets
		packet, _, err := track.ReadRTP()
		if err != nil {
			ap.mu.Lock()
			processing := ap.processing
			ap.mu.Unlock()
			if processing {
				log.Printf("Error reading RTP packet for peer %s: %v", ap.PeerID, err)
				ap.Stop()
			}
			return
		}

		ap.mu.Lock()
		// Store the raw RTP payload (Opus data usually)
		// For this phase, we just concatenate payloads. In future, this would be proper decoding.
		ap.buffer = append(ap.buffer, packet.Payload...)
		ap.mu.Unlock()
	}
}

func (ap *AudioProcessor) processLoop() {
	for {
		select {
		case <-ap.ticker.C:
			ap.flushBuffer()
		case <-ap.stopChan:
			return
		}
	}
}

func (ap *AudioProcessor) flushBuffer() {
	ap.mu.Lock()
	if len(ap.buffer) == 0 {
		ap.mu.Unlock()
		return
	}
	
	// Copy buffer to process and reset original buffer
	dataToProcess := make([]byte, len(ap.buffer))
	copy(dataToProcess, ap.buffer)
	ap.buffer = ap.buffer[:0] // Retain capacity, clear length
	ap.mu.Unlock()

	chunkID := uuid.New().String()
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if kafkaProd != nil {
		err := kafkaProd.PublishChunk(ctx, ap.RoomID, ap.PeerID, chunkID, dataToProcess)
		if err != nil {
			log.Printf("Error publishing audio chunk for %s: %v", ap.PeerID, err)
		}
	}
}
