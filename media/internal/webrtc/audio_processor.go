package webrtc

import (
	"bytes"
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media/oggwriter"
	"media-server/internal/kafka"
)

type AudioProcessor struct {
	RoomID     string
	PeerID     string
	PeerName   string
	kafkaProd  *kafka.Producer
	oggBuf     *bytes.Buffer
	oggWriter  *oggwriter.OggWriter
	mu         sync.Mutex
	stopChan   chan struct{}
	ticker     *time.Ticker
	processing bool
}

func NewAudioProcessor(roomID, peerID, peerName string, kafkaProd *kafka.Producer) *AudioProcessor {
	return &AudioProcessor{
		RoomID:    roomID,
		PeerID:    peerID,
		PeerName:  peerName,
		kafkaProd: kafkaProd,
		stopChan:  make(chan struct{}),
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
	ap.flushBufferLocked()
}

func (ap *AudioProcessor) initWriter(sampleRate uint32, channels uint16) error {
	ap.oggBuf = new(bytes.Buffer)
	w, err := oggwriter.NewWith(ap.oggBuf, sampleRate, channels)
	if err != nil {
		return err
	}
	ap.oggWriter = w
	return nil
}

func (ap *AudioProcessor) readRTP(track *webrtc.TrackRemote) {
	for {
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
		if ap.processing {
			if ap.oggWriter == nil {
				channels := track.Codec().Channels
				if channels == 0 {
					channels = 2
				}
				sampleRate := track.Codec().ClockRate
				if sampleRate == 0 {
					sampleRate = 48000
				}
				if err := ap.initWriter(sampleRate, channels); err != nil {
					log.Printf("Failed to initialize OggWriter for peer %s: %v", ap.PeerID, err)
					ap.mu.Unlock()
					continue
				}
			}
			if err := ap.oggWriter.WriteRTP(packet); err != nil {
				log.Printf("Failed to write RTP to OggWriter for peer %s: %v", ap.PeerID, err)
			}
		}
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
	defer ap.mu.Unlock()
	ap.flushBufferLocked()
}

func (ap *AudioProcessor) flushBufferLocked() {
	if ap.oggWriter == nil || ap.oggBuf == nil || ap.oggBuf.Len() == 0 {
		return
	}

	// Close the current OggWriter to write headers and trailers properly
	if err := ap.oggWriter.Close(); err != nil {
		log.Printf("Error closing OggWriter for peer %s: %v", ap.PeerID, err)
	}

	// Copy data out of the buffer and reset OggWriter/Buffer for the next 5 seconds
	dataToProcess := make([]byte, ap.oggBuf.Len())
	copy(dataToProcess, ap.oggBuf.Bytes())

	ap.oggWriter = nil
	ap.oggBuf = nil

	chunkID := uuid.New().String()

	if ap.kafkaProd != nil {
		go func(data []byte, cID string) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := ap.kafkaProd.PublishChunk(ctx, ap.RoomID, ap.PeerID, ap.PeerName, cID, data)
			if err != nil {
				log.Printf("Error publishing audio chunk for %s: %v", ap.PeerID, err)
			}
		}(dataToProcess, chunkID)
	}
}
