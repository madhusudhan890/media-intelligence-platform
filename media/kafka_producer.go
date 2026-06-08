package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type AudioChunkMessage struct {
	RoomID     string `json:"roomId"`
	PeerID     string `json:"peerId"`
	ChunkID    string `json:"chunkId"`
	Timestamp  string `json:"timestamp"`
	DurationMs int    `json:"durationMs"`
	AudioData  string `json:"audioData"`
}

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(brokers string, topic string) *KafkaProducer {
	log.Printf("Initializing Kafka producer connecting to %s for topic %s", brokers, topic)
	
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	return &KafkaProducer{
		writer: w,
	}
}

func (kp *KafkaProducer) PublishChunk(ctx context.Context, roomID, peerID, chunkID string, audioBytes []byte) error {
	msg := AudioChunkMessage{
		RoomID:     roomID,
		PeerID:     peerID,
		ChunkID:    chunkID,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		DurationMs: 5000,
		AudioData:  base64.StdEncoding.EncodeToString(audioBytes),
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	err = kp.writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(peerID),
			Value: msgBytes,
		},
	)

	if err != nil {
		log.Printf("Failed to write to kafka: %v", err)
		return err
	}
	
	log.Printf("Published audio chunk to kafka: Room=%s, Peer=%s, ChunkSize=%d bytes", roomID, peerID, len(audioBytes))
	return nil
}

func (kp *KafkaProducer) Close() {
	if err := kp.writer.Close(); err != nil {
		log.Printf("Failed to close kafka writer: %v", err)
	}
}
