#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:29092 1 60

echo "Kafka is ready. Creating topics..."

# Create all required topics
for TOPIC in audio-chunks transcripts ai-insights; do
  kafka-topics --create \
    --if-not-exists \
    --bootstrap-server kafka:29092 \
    --partitions 1 \
    --replication-factor 1 \
    --topic "$TOPIC"
  echo "Topic '$TOPIC' ensured."
done

echo "All topics created successfully."
