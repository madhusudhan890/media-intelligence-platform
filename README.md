# Real-Time Media Intelligence Platform

> **Phase 1** — Peer-to-Peer Video/Audio Communication

A production-structured WebRTC communication platform designed as the foundation for a larger real-time media intelligence system. Phase 1 establishes reliable 1:1 peer-to-peer video and audio communication with a clean, modern UI.

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
       └─────────────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Technology | Port | Purpose |
|-----------|-----------|------|---------|
| **Frontend** | React 18 + Vite | 3000 | UI, WebRTC client, media controls |
| **Signaling Server** | Node.js + Express + ws | 8080 | Room management, SDP/ICE relay |

---

## WebRTC Flow

The platform uses native browser `RTCPeerConnection` for direct peer-to-peer media transport:

```
1. User A creates room         → POST /rooms → receives roomId
2. User A joins room           → WebSocket "join" message
3. User A acquires media       → getUserMedia({ audio: true, video: true })
4. User B joins same room      → WebSocket "join" message
5. Server notifies A           → "peer-joined" message to A
6. A creates RTCPeerConnection → adds local tracks, creates offer
7. A sends offer               → WebSocket relay → delivered to B
8. B receives offer            → creates answer, sets remote description
9. B sends answer              → WebSocket relay → delivered to A
10. ICE candidates exchanged   → trickle ICE via WebSocket relay
11. P2P connection established → direct audio/video streaming
```

### ICE Handling

- **STUN server**: `stun:stun.l.google.com:19302`
- **Trickle ICE**: Candidates sent as they're discovered
- **Pending queue**: Early candidates buffered until remote description is set
- **ICE restart**: Automatic on connection failure

---

## Signaling Flow

### WebSocket Message Format

All messages follow this structure:

```json
{
  "type": "offer",
  "roomId": "room-uuid",
  "peerId": "peer-uuid",
  "targetPeerId": "other-peer-uuid",
  "payload": {}
}
```

### Message Types

| Type | Direction | Purpose |
|------|-----------|---------|
| `join` | Client → Server | Join a room |
| `joined` | Server → Client | Confirm join with peer list |
| `peer-joined` | Server → Client | Notify existing peer of new peer |
| `offer` | Client → Server → Client | SDP offer relay |
| `answer` | Client → Server → Client | SDP answer relay |
| `ice-candidate` | Client → Server → Client | ICE candidate relay |
| `peer-left` | Server → Client | Notify peer disconnection |
| `error` | Server → Client | Error notification |

### HTTP Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Health check with stats |
| `POST` | `/rooms` | Create a new room |
| `GET` | `/rooms/:roomId` | Get room info |

---

## Setup Instructions

### Prerequisites

- [Docker](https://www.docker.com/get-started) and Docker Compose installed
- OR Node.js 20+ for local development

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

### Local Development (without Docker)

**Terminal 1 — Signaling Server:**

```bash
cd signaling
npm install
npm run dev
```

**Terminal 2 — Frontend:**

```bash
cd frontend
npm install
npm run dev
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` (signaling) | `8080` | Signaling server port |
| `VITE_SIGNALING_URL` (frontend) | `ws://localhost:8080` | WebSocket URL for signaling |

---

## Project Structure

```
media-intelligence-platform/
├── docker-compose.yml              # Orchestrates all services
├── README.md
│
├── signaling/                      # WebRTC signaling server
│   ├── server.js                   # Express + HTTP server entry point
│   ├── roomManager.js              # In-memory room/peer management
│   ├── websocket.js                # WebSocket handler + message routing
│   ├── package.json
│   └── Dockerfile
│
└── frontend/                       # React SPA
    ├── src/
    │   ├── App.jsx                 # Router setup
    │   ├── main.jsx                # React 18 entry point
    │   ├── pages/
    │   │   ├── Home.jsx            # Create/Join room UI
    │   │   └── Room.jsx            # Video call UI
    │   ├── hooks/
    │   │   └── useWebRTC.js        # WebRTC + signaling logic
    │   ├── components/
    │   │   ├── VideoPlayer.jsx     # Video stream renderer
    │   │   ├── Controls.jsx        # Mute/Video/Leave controls
    │   │   └── ConnectionStatus.jsx # Connection state indicator
    │   └── styles/
    │       └── app.css             # Full design system
    │
    ├── index.html
    ├── package.json
    ├── vite.config.js
    └── Dockerfile
```

---

## Troubleshooting

### Camera/Microphone not working

- Ensure browser has permission to access camera and microphone
- Check that no other application is using the camera
- Try using Chrome or Firefox (Safari has limited WebRTC support)
- If using Docker, the frontend must be accessed via `localhost` (not `0.0.0.0`) for media permissions to work

### Can't connect to peer

- Both users must be in the same room (same room ID)
- Check that the signaling server is running: `curl http://localhost:8080/health`
- Check browser console for WebSocket connection errors
- Ensure port 8080 is not blocked by firewall

### Video not showing

- Verify `stun:stun.l.google.com:19302` is accessible (not blocked by corporate firewall)
- Check browser console for ICE connection state — should reach "connected"
- Try refreshing both browser tabs simultaneously

### Docker issues

- Run `docker-compose down` and then `docker-compose up --build` to rebuild
- Check logs: `docker-compose logs signaling` or `docker-compose logs frontend`
- Ensure ports 3000 and 8080 are not in use: `lsof -i :3000` / `lsof -i :8080`

---

## Future Roadmap

Phase 1 establishes the P2P communication foundation. The architecture is designed to support these future additions:

### Phase 2 — Media Server & Audio Uplink
- **Golang media server** using [Pion WebRTC](https://github.com/pion/webrtc)
- Separate audio uplink from browser to media server
- Server-side audio stream extraction for processing

### Phase 3 — Event Streaming & Processing
- **Apache Kafka** event streaming pipeline
- Audio chunk events published to Kafka topics
- Event-driven architecture for real-time processing

### Phase 4 — AI Intelligence
- **Real-time transcription** using speech-to-text models
- AI-powered media intelligence extraction (sentiment, topics, entities)
- Distributed AI worker pool for parallel processing

### Phase 5 — Analytics & Dashboard
- **Real-time media analytics** dashboard
- Call quality metrics and monitoring
- Transcription viewer with search
- Media intelligence insights and visualization

### Phase 6 — Scale & Production
- SFU (Selective Forwarding Unit) for group calls
- TURN server deployment for NAT traversal
- Redis for distributed state
- PostgreSQL for persistence
- Kubernetes orchestration

---

## License

MIT
