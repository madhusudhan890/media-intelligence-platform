-- Postgres initialization script for Phase 3 schema

CREATE TABLE IF NOT EXISTS transcripts (
    id UUID PRIMARY KEY,
    room_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    peer_name TEXT DEFAULT 'Anonymous',
    chunk_id TEXT NOT NULL,
    text TEXT NOT NULL,
    confidence FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS insights (
    id UUID PRIMARY KEY,
    room_id TEXT NOT NULL,
    summary TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transcripts_room
ON transcripts(room_id);

CREATE INDEX IF NOT EXISTS idx_insights_room
ON insights(room_id);
