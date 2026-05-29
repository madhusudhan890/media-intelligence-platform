/*
Package kafka provides integration with Apache Kafka for event streaming.
It handles publishing audio chunks to specific Kafka topics for downstream AI processing.
*/
package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"strings"
	"time"

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

type Producer struct {
	writer *segmentio.Writer
}

func NewProducer(brokers string, topic string) *Producer {
	log.Printf("Initializing Kafka producer connecting to %s for topic %s", brokers, topic)

	// Ensure topic exists
	ensureTopicExists(brokers, topic)

	w := &segmentio.Writer{
		Addr:     segmentio.TCP(brokers),
		Topic:    topic,
		Balancer: &segmentio.LeastBytes{},
	}

	return &Producer{
		writer: w,
	}
}

func ensureTopicExists(brokers, topic string) {
	client := &segmentio.Client{
		Addr: segmentio.TCP(strings.Split(brokers, ",")...),
	}
	
	resp, err := client.CreateTopics(context.Background(), &segmentio.CreateTopicsRequest{
		Topics: []segmentio.TopicConfig{
			{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
	})

	if err != nil {
		log.Printf("[Kafka] Failed to send CreateTopic request for %s: %v", topic, err)
		return
	}

	if topicErr, ok := resp.Errors[topic]; ok && topicErr != nil {
		// Ignore the 'Topic with this name already exists' error
		if !strings.Contains(topicErr.Error(), "already exists") {
			log.Printf("[Kafka] Topic creation warning/error for %s: %v", topic, topicErr)
		} else {
			log.Printf("[Kafka] Topic %s already exists.", topic)
		}
	} else {
		log.Printf("[Kafka] Topic %s created successfully.", topic)
	}
}

func (kp *Producer) PublishChunk(ctx context.Context, roomID, peerID, chunkID string, audioBytes []byte) error {
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
		segmentio.Message{
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

func (kp *Producer) Close() {
	if err := kp.writer.Close(); err != nil {
		log.Printf("Failed to close kafka writer: %v", err)
	}
}
