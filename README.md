# Real-Time Media Intelligence Platform

> **Phase 1** — Peer-to-Peer Video/Audio Communication
> **Phase 2** — Real-Time Media Processing Pipeline (Audio Uplink to Go + Kafka)

A production-structured WebRTC communication platform designed as the foundation for a larger real-time media intelligence system. The system maintains reliable 1:1 peer-to-peer video and audio communication while establishing a secondary audio uplink to a backend media server for event streaming via Kafka.

---

## Architecture

```
┌──────────────┐     WebSocket     ┌──────────────────┐     WebSocket     ┌──────────────┐
│   Browser A  │◄────signaling────►│  Node.js Signal  │◄────signaling────►│   Browser B  │
│  (React/Vite)│                   │    Server (ws)    │                   │  (React/Vite)│
└──────┬───────┘                   └──────────────────┘                   └──────┬───────┘
       │                                                                         │
       │                    Direct WebRTC Connection                             │
       │                    (Audio + Video P2P)                                  │
       ├─────────────────────────────────────────────────────────────────────────┤
       │                                                                         │
       │ Audio Uplink (WebRTC)                             Audio Uplink (WebRTC) │
       │                                                                         │
       ▼                                                                         ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   Go Media Server                                      │
│                                (Pion WebRTC + Kafka)                                   │
└────────────────────────────────────────┬───────────────────────────────────────────────┘
                                         │ Audio Chunks (5s intervals)
                                         ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   Apache Kafka                                         │
│                               (Topic: audio-chunks)                                    │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Technology | Port | Purpose |
|-----------|-----------|------|---------|
| **Frontend** | React 18 + Vite | 3000 | UI, WebRTC client (P2P + Uplink) |
| **Signaling Server** | Node.js + Express | 8080 | Room management, P2P SDP/ICE relay |
| **Media Server** | Golang + Pion | 8081 | Audio ingestion, chunking, Kafka publishing |
| **Kafka/Zookeeper** | Confluent | 9092 | Event streaming pipeline |

---

## Why Separate the Media Uplink?

This architecture deliberately decouples P2P communication from media processing:
- **Zero Latency P2P:** Users communicate directly with each other without going through a central server, ensuring the lowest possible latency and best video quality.
- **Dedicated Processing:** The Go media server only receives an audio uplink. It acts purely as an ingestion point, avoiding the complexity and overhead of an SFU (Selective Forwarding Unit) while still preparing the stream for backend AI processing.
- **Event-Driven:** By chunking audio into Kafka, we create a robust, scalable event pipeline where various downstream workers (e.g., transcription, analytics) can consume the media independently.

---

## WebRTC Flows

### Flow 1: P2P Communication
(Same as Phase 1) Browser creates a full audio/video `RTCPeerConnection` and negotiates via the Node.js WebSocket signaling server.

### Flow 2: Audio Uplink
When the user's media is acquired, the browser creates a *second* `RTCPeerConnection` configured as `sendonly` (audio only). It sends an SDP offer directly to the Go Media Server via `POST http://localhost:8081/offer` and receives the SDP answer in the HTTP response. The Go server then receives RTP packets, buffers them, and flushes them to Kafka every 5 seconds.

---

## Setup Instructions

### Prerequisites

- [Docker](https://www.docker.com/get-started) and Docker Compose installed

### Quick Start (Docker)

```bash
# Clone the repository
git clone <repo-url>
cd media-intelligence-platform

# Build and start all services
docker-compose up --build
```

That's it. Open two browser tabs:
- Tab 1: http://localhost:3000 → Click "Create Room"
- Tab 2: http://localhost:3000 → Paste the room ID → Click "Join"

You can inspect the Go media server logs to see the audio chunking:
```bash
docker-compose logs -f media
```

---

## Future Roadmap

The architecture is designed to support these future additions:

### Phase 3 — AI Intelligence & Transcription
- **Real-time transcription** using `faster-whisper` Python workers consuming from Kafka.
- AI-powered media intelligence extraction (sentiment, topics, entities).
- Redis for context windowing.

### Phase 4 — Analytics & Dashboard
- **Real-time media analytics** dashboard.
- Call quality metrics and monitoring.
- SSE (Server-Sent Events) push updates to the frontend for live transcripts.

### Phase 5 — Scale & Production
- SFU (Selective Forwarding Unit) for group calls (migrating P2P to server-routed).
- TURN server deployment for NAT traversal.
- Kubernetes orchestration.

---

## License

MIT
