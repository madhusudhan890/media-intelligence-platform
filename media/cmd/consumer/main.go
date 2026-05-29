package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	segmentio "github.com/segmentio/kafka-go"
)

type AudioChunkMessage struct {
	RoomID     string `json:"roomId"`
	PeerID     string `json:"peerId"`
	ChunkID    string `json:"chunkId"`
	Timestamp  string `json:"timestamp"`
	DurationMs int    `json:"durationMs"`
	AudioData  string `json:"audioData"`
}

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9094"
	}
	topic := "audio-chunks"

	log.Printf("Starting Kafka consumer connecting to %s for topic %s...", brokers, topic)

	r := segmentio.NewReader(segmentio.ReaderConfig{
		Brokers:   []string{brokers},
		Topic:     topic,
		GroupID:   "temp-consumer-group",
		MinBytes:  10,
		MaxBytes:  10e6, // 10MB
	})
	defer r.Close()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-sigChan
		log.Println("Shutting down consumer...")
		cancel()
	}()

	log.Println("Ready to consume messages. Press Ctrl+C to exit.")

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Error reading message: %v", err)
			continue
		}

		var msg AudioChunkMessage
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			log.Printf("Consumed raw message: key=%s value=%s (failed to parse JSON: %v)", string(m.Key), string(m.Value), err)
			continue
		}

		// Truncate audio data for pretty printing
		audioLen := len(msg.AudioData)
		preview := ""
		if audioLen > 30 {
			preview = msg.AudioData[:30] + "..."
		} else {
			preview = msg.AudioData
		}

		fmt.Printf("\n[Event Received]\n")
		fmt.Printf("  Topic:     %s\n", m.Topic)
		fmt.Printf("  Partition: %d\n", m.Partition)
		fmt.Printf("  Offset:    %d\n", m.Offset)
		fmt.Printf("  Room ID:   %s\n", msg.RoomID)
		fmt.Printf("  Peer ID:   %s\n", msg.PeerID)
		fmt.Printf("  Chunk ID:  %s\n", msg.ChunkID)
		fmt.Printf("  Timestamp: %s\n", msg.Timestamp)
		fmt.Printf("  Duration:  %d ms\n", msg.DurationMs)
		fmt.Printf("  AudioData: %s (Length: %d characters)\n", preview, audioLen)
	}

	log.Println("Consumer stopped.")
}
