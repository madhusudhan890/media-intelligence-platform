# OMNISIGHT: Real-Time AI-Driven P2P Media Intelligence Platform

> **"I built this because meeting platforms like Teams and Meet give you a wall of raw transcript text after the call. Nobody reads it. This platform extracts structured intelligence — action items with owners, decisions made, open questions — in real time, while the meeting is happening. Built on WebRTC (Pion), Golang, Kafka, Python, and Groq."**

---

## 💡 The Core Problem
Most modern video conferencing platforms (Zoom, Teams, Google Meet) focus heavily on transport latency but treat post-meeting intelligence as an afterthought. After a call, you receive a long, unstructured wall of transcribed text.
* **Transcripts are unread:** Nobody has the time to read through thousands of lines of raw speech.
* **Delayed Actionability:** Decisions, open questions, and tasks are buried, requiring manual parsing post-meeting.
* **Compute Overhead:** Standard in-line AI processing on video channels can choke media server performance, degrading real-time call quality.

## 🚀 The Solution: OMNISIGHT
OMNISIGHT is an **enterprise-grade, event-driven cognitive media platform** designed to solve this. It splits real-time peer-to-peer WebRTC communication from background AI pipelines using a highly scalable, distributed event broker (Kafka).

While participants communicate in a call:
1. **Real-time Acoustic Ingestion:** A Go-based media pipeline (built on Pion WebRTC) captures peer audio tracks and streams them directly to Kafka.
2. **Diarized Speech-to-Text:** Asynchronous Python workers consume the audio streams and run local, diarized `faster-whisper` models to produce text logs.
3. **Cognitive Synthesis (Groq/Gemini):** A sliding context window managed via Redis feeds diarized transcripts to high-speed LLMs (like Llama 3 via Groq) to extract and push structured meeting intelligence (action items with owners, decisions, deadlines, risks) **live, while the meeting is happening** over Server-Sent Events (SSE).

---

# Key Engineering Highlights

- WebRTC Peer-to-Peer Video & Audio Communication
- Golang Signaling & Media Server (Pion WebRTC)
- Kafka Event-Driven Architecture
- Real-Time Speech-to-Text using Faster-Whisper
- AI Meeting Intelligence using LLMs
- Redis Sliding Context Window
- Server-Sent Events (SSE) for Real-Time Updates
- PostgreSQL Persistence Layer
- Dockerized Local Development Environment
- Hexagonal Architecture (Ports & Adapters)
- Provider-Agnostic LLM Strategy Pattern
- Fault-Tolerant Asynchronous Processing

---

# Why This Project Exists

Traditional meeting platforms focus primarily on communication.

This project explores how real-time media streams can be transformed into actionable intelligence using distributed systems, AI pipelines, and low-latency media processing.

The goal is not simply video communication, but building a platform capable of understanding and extracting value from live media streams.

This architecture creates a foundation for future capabilities such as:

- Speaker intelligence
- Sentiment analysis
- Video intelligence
- Face detection
- Meeting engagement analytics
- Multimodal AI systems

---

# System Context

```mermaid
---
config:
  theme: base
  themeVariables:
    primaryColor: '#3b82f6'
    primaryTextColor: '#ffffff'
    primaryBorderColor: '#1d4ed8'
    lineColor: '#60a5fa'
    secondaryColor: '#10b981'
    tertiaryColor: '#1e293b'
---
flowchart LR

UserA[User A]
UserB[User B]

Platform[Real-Time Media Intelligence Platform]

UserA --> Platform
UserB --> Platform

Platform --> Kafka[Apache Kafka]
Platform --> Redis[Redis]
Platform --> Postgres[PostgreSQL]
Platform --> LLM[LLM Provider]
```

---

# High Level Architecture

```mermaid
---
config:
  theme: base
  themeVariables:
    primaryColor: '#3b82f6'
    primaryTextColor: '#ffffff'
    primaryBorderColor: '#1d4ed8'
    lineColor: '#60a5fa'
    secondaryColor: '#10b981'
    tertiaryColor: '#1e293b'
---
flowchart TB

subgraph Client Layer
Frontend[React Frontend]
end

subgraph Communication Layer
Media[Go Media Server<br/>Signaling & Audio Ingestion]
end

subgraph Event Layer
Kafka[(Kafka)]
end

subgraph AI Processing Layer
TW[Transcription Worker]
AW[AI Insight Worker]
SSE[SSE Broadcast Service]
end

subgraph Storage Layer
Redis[(Redis)]
Postgres[(PostgreSQL)]
end

Frontend <--> Media
Frontend <--> SSE

Media --> Kafka

Kafka --> TW
TW --> Kafka

Kafka --> AW

AW --> Redis

TW --> Postgres
AW --> Postgres

Kafka --> SSE
```

---

# End-to-End Architecture

```mermaid
---
config:
  theme: base
  themeVariables:
    primaryColor: '#3b82f6'
    primaryTextColor: '#ffffff'
    primaryBorderColor: '#1d4ed8'
    lineColor: '#60a5fa'
    secondaryColor: '#10b981'
    tertiaryColor: '#1e293b'
---
graph TD

A[Browser A]
B[Browser B]

A <-->|WebRTC Video + Audio| B

A -->|Audio Uplink| Media[Go Media Server]
B -->|Audio Uplink| Media

Media --> Kafka[(Kafka)]

Kafka --> TW[Whisper Worker]

TW --> Kafka

Kafka --> AW[AI Worker]

AW --> Redis[(Redis)]

TW --> Postgres[(Postgres)]
AW --> Postgres

Kafka --> SSE[SSE Service]

SSE --> UI[React Dashboard]
```

---

# WebRTC Connection Flow

```mermaid
---
config:
  theme: base
  themeVariables:
    actorBkg: '#3b82f6'
    actorBorder: '#1d4ed8'
    actorTextColor: '#ffffff'
    actorLineColor: '#60a5fa'
    signalColor: '#3b82f6'
    signalTextColor: '#1e293b'
    labelBoxBkgColor: '#f1f5f9'
    labelBoxBorderColor: '#cbd5e1'
    labelTextColor: '#0f172a'
    noteBkgColor: '#fef08a'
    noteBorderColor: '#ca8a04'
    noteTextColor: '#854d0e'
---
sequenceDiagram

participant A as Browser A
participant S as Go Media Server (Signaling)
participant B as Browser B

A->>S: Join Room

B->>S: Join Room

S->>A: Peer Joined

A->>B: SDP Offer
B->>A: SDP Answer

A->>B: ICE Candidate
B->>A: ICE Candidate

Note over A, B: Peer-to-Peer Video & Audio Streams
```

---

# AI Processing Pipeline

```mermaid
---
config:
  theme: base
  themeVariables:
    primaryColor: '#3b82f6'
    primaryTextColor: '#ffffff'
    primaryBorderColor: '#1d4ed8'
    lineColor: '#60a5fa'
    secondaryColor: '#10b981'
    tertiaryColor: '#1e293b'
---
flowchart LR

Audio
--> Whisper

Whisper
--> Transcript

Transcript
--> Redis

Redis
--> LLM

LLM
--> Insights

Insights
--> SSE

SSE
--> Dashboard
```

---

# Detailed Processing Sequence

```mermaid
---
config:
  theme: base
  themeVariables:
    actorBkg: '#3b82f6'
    actorBorder: '#1d4ed8'
    actorTextColor: '#ffffff'
    actorLineColor: '#60a5fa'
    signalColor: '#3b82f6'
    signalTextColor: '#1e293b'
    labelBoxBkgColor: '#f1f5f9'
    labelBoxBorderColor: '#cbd5e1'
    labelTextColor: '#0f172a'
    noteBkgColor: '#fef08a'
    noteBorderColor: '#ca8a04'
    noteTextColor: '#854d0e'
---
sequenceDiagram

autonumber

participant Browser
participant Media
participant Kafka
participant Whisper
participant Redis
participant LLM
participant Postgres
participant Frontend

Browser->>Media: WebRTC RTP Audio

Media->>Media: Buffer 5 Seconds

Media->>Kafka: audio-chunks

Kafka->>Whisper: Consume Chunk

Whisper->>Whisper: Speech To Text

Whisper->>Postgres: Store Transcript

Whisper->>Kafka: transcripts

Kafka->>Redis: Update Context Window

Redis->>Redis: Keep Latest 10 Chunks

Redis->>LLM: Context

LLM->>Postgres: Store Insight

LLM->>Kafka: ai-insights

Kafka->>Frontend: SSE Update
```

---

# Database Model

```mermaid
---
config:
  theme: base
  themeVariables:
    attributeBackgroundColor: '#f8fafc'
    attributeTextColor: '#0f172a'
    entityBackgroundColor: '#3b82f6'
    entityBorderColor: '#1d4ed8'
    entityTextColor: '#ffffff'
    lineColor: '#60a5fa'
---
erDiagram

MEETINGS ||--o{ TRANSCRIPTS : contains
MEETINGS ||--o{ INSIGHTS : contains

MEETINGS {
    uuid id
    string room_id
    timestamp started_at
    timestamp ended_at
}

TRANSCRIPTS {
    uuid id
    string room_id
    string peer_id
    text transcript
    float confidence
}

INSIGHTS {
    uuid id
    string room_id
    json payload
}
```

---

# Component Responsibilities

| Component | Responsibility |
|------------|------------|
| React Frontend | User Interface |
| Go Media Server | WebRTC Signaling & Audio Ingestion |
| Kafka | Event Streaming |
| Whisper Worker | Speech-to-Text |
| AI Worker | Meeting Intelligence |
| Redis | Sliding Context Window |
| PostgreSQL | Long-Term Storage |
| SSE Service | Real-Time Updates |

---

# Architecture Principles

## 1. Media Isolation

Real-time communication must never depend on AI workloads.

Video and audio communication continue even if:

- Kafka fails
- Redis fails
- Whisper crashes
- LLM APIs become unavailable

---

## 2. Event-Driven Processing

All AI workloads are asynchronous.

Media traffic never waits for:

- transcription
- database writes
- AI inference

---

## 3. Loose Coupling

Each subsystem can evolve independently.

- Signaling Layer
- Media Layer
- Kafka Layer
- AI Layer
- Frontend Layer

---

## 4. Incremental Scalability

Architecture evolution path:

```text
P2P
↓
AI Analytics
↓
Speaker Intelligence
↓
Video Intelligence
↓
SFU
↓
Multi-Region Deployment
```

---

# Technology Stack

| Layer | Technology |
|---------|---------|
| Frontend | React 18 + Vite |
| Signaling & Media | Golang + Pion + WebSockets |
| Queue | Apache Kafka |
| AI Workers | Python 3.11 |
| Transcription | Faster-Whisper |
| AI Engine | Groq/OpenAI/Gemini/Ollama |
| Cache | Redis |
| Database | PostgreSQL |
| Streaming Updates | Server Sent Events |
| Deployment | Docker Compose |

---

# Hexagonal Architecture

The AI Worker subsystem follows Hexagonal Architecture.

```text
workers/app/
├── domain/
├── usecases/
├── adapters/
│   ├── kafka/
│   ├── redis/
│   ├── postgres/
│   ├── whisper/
│   └── llm/
└── main.py
```

Benefits:

- Testability
- Dependency inversion
- Provider independence
- Maintainability

---

# Adaptive LLM Strategy

The platform supports multiple LLM providers through a Strategy Pattern.

| Provider | Model |
|-----------|-----------|
| Groq | llama3-8b-8192 |
| OpenAI | gpt-4o-mini |
| Gemini | gemini-2.5-flash |
| Ollama | llama3 |

Provider selection:

```bash
LLM_PROVIDER=groq
```

Runtime switching requires no code changes.

---

# Kafka Topics

| Topic | Purpose |
|----------|----------|
| audio-chunks | Raw audio payloads |
| transcripts | Whisper outputs |
| ai-insights | AI-generated meeting intelligence |

---

# PostgreSQL Schema

## Meetings

Stores meeting metadata.

## Transcripts

Stores all transcription segments.

## Insights

Stores AI-generated summaries and structured outputs.

---

# Redis Usage

Redis is used for maintaining a bounded transcript context.

Key:

```text
context:{roomId}
```

Behavior:

- Latest 10 transcript segments
- 1 hour TTL
- Fast retrieval for AI summarization

---

# Reliability Features

- Kafka-backed durability
- Redis sliding context windows
- AI retry mechanism
- Graceful degradation
- SSE auto-reconnect
- Independent worker scaling
- Media isolation from AI workloads
- Structured logging
- Retryable LLM requests
- Fault-tolerant architecture

---

# Failure Handling

## Whisper Failure

- Error logged
- Message skipped
- System continues operating

## LLM Failure

- Retry up to 3 times
- Fallback JSON returned
- UI remains functional

## Kafka Failure

- Media communication unaffected
- Analytics temporarily unavailable

## Redis Failure

- AI falls back to current transcript batch

---

# Performance Targets

| Metric | Target |
|----------|----------|
| Call Setup Time | < 3 seconds |
| Transcript Delay | < 10 seconds |
| AI Refresh Time | 20-30 seconds |
| Audio Chunk Size | 5 seconds |
| Redis Context Window | 10 Segments |
| Kafka Processing Latency | < 500ms |

---

# Environment Configuration

Create:

```bash
cp workers/.env.example workers/.env
```

Example:

```env
LLM_PROVIDER=groq

GROQ_API_KEY=your_api_key
GROQ_MODEL=llama3-8b-8192

KAFKA_BROKER=localhost:9094

REDIS_URL=redis://localhost:6379

DATABASE_URL=postgresql://user:password@localhost/db
```

---

# Running The Platform

## Docker

```bash
docker-compose up --build
```

---

## Local Development

```bash
cd workers

source venv/bin/activate

pip install -r requirements.txt

python -m app.main
```

---

# Future Roadmap

## Phase 1

- P2P Video Calls
- Audio Calls
- Room Management

## Phase 2

- Media Ingestion
- Kafka Event Streaming
- Go Media Pipeline

## Phase 3

- Real-Time Transcription
- AI Meeting Intelligence
- SSE Updates

## Phase 4

- Speaker Diarization
- Sentiment Analysis
- Keyword Extraction

## Phase 5

- Video Intelligence
- Face Detection
- Engagement Analytics
- Object Detection

## Phase 6

- SFU Architecture
- Multi-Participant Calls
- Horizontal Scaling

## Phase 7

- Kubernetes Deployment
- Multi-Region Processing
- Observability Stack

---

# Future Media Intelligence Extensions

## Computer Vision

- Face Detection
- Face Recognition
- Emotion Analysis
- Liveness Detection
- Attendance Tracking

## Video Intelligence

- Object Detection
- Scene Understanding
- Behavioral Analytics
- Compliance Monitoring

## Audio Intelligence

- Speaker Diarization
- Sentiment Analysis
- Keyword Extraction
- Compliance Monitoring

## Multimodal AI

- Audio + Video Correlation
- Context-Aware Summaries
- Speaker Activity Detection
- Real-Time Insights

---

# Operational Troubleshooting

## Kafka Connection Issues

Verify:

```bash
docker ps
```

Ensure Kafka is healthy and reachable.

---

## Whisper Model Issues

Pre-download model:

```bash
python -c "from faster_whisper import WhisperModel; WhisperModel('base')"
```

---

## Import Errors

Run:

```bash
python -m py_compile app/*.py app/**/*.py app/**/**/*.py
```

---

## Redis Connection Issues

Verify:

```bash
redis-cli ping
```

Expected:

```text
PONG
```

---

# Project Goal

Build a scalable foundation for transforming live media streams into actionable intelligence.

The current implementation focuses on meeting intelligence, but the architecture is intentionally designed to support future computer vision, multimodal AI, and large-scale media analytics workloads without major redesign.