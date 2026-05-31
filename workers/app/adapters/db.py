import uuid
import json
import logging
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from app.config import DATABASE_URL, DB_USER, DB_PASSWORD, DB_DBNAME, DB_HOST, DB_PORT

logger = logging.getLogger("db_adapter")

_pool = None

def get_db_pool():
    global _pool
    if _pool is None:
        try:
            if DATABASE_URL:
                _pool = pool.ThreadedConnectionPool(
                    minconn=2,
                    maxconn=20,
                    dsn=DATABASE_URL
                )
            else:
                _pool = pool.ThreadedConnectionPool(
                    minconn=2,
                    maxconn=20,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    host=DB_HOST,
                    port=DB_PORT,
                    database=DB_DBNAME
                )
            logger.info("Successfully connected to Postgres database pool.")
        except Exception as e:
            logger.error(f"Failed to connect to Postgres: {e}")
            raise e
    return _pool

def execute_write(query, params=None):
    db_pool = get_db_pool()
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database write execution error: {e}")
        raise e
    finally:
        db_pool.putconn(conn)

def execute_read(query, params=None):
    db_pool = get_db_pool()
    conn = db_pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
    except Exception as e:
        logger.error(f"Database read execution error: {e}")
        raise e
    finally:
        db_pool.putconn(conn)

def save_transcript(room_id: str, peer_id: str, chunk_id: str, text: str, confidence: float):
    query = """
    INSERT INTO transcripts (id, room_id, peer_id, chunk_id, text, confidence, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (id) DO NOTHING;
    """
    row_id = str(uuid.uuid4())
    execute_write(query, (row_id, room_id, peer_id, chunk_id, text, confidence))
    logger.info(f"Saved transcript to Postgres: room_id={room_id}, chunk_id={chunk_id}")

def save_insight(room_id: str, summary: str, payload: dict):
    query = """
    INSERT INTO insights (id, room_id, summary, payload, created_at)
    VALUES (%s, %s, %s, %s, NOW());
    """
    row_id = str(uuid.uuid4())
    execute_write(query, (row_id, room_id, summary, json.dumps(payload)))
    logger.info(f"Saved insight to Postgres: room_id={room_id}")

def get_room_summary(room_id: str) -> dict:
    transcripts_query = """
    SELECT peer_id as "peerId", chunk_id as "chunkId", text, confidence, created_at as "timestamp"
    FROM transcripts
    WHERE room_id = %s
    ORDER BY created_at ASC;
    """
    
    insights_query = """
    SELECT summary, payload, created_at as "timestamp"
    FROM insights
    WHERE room_id = %s
    ORDER BY created_at DESC
    LIMIT 1;
    """
    
    transcripts_rows = execute_read(transcripts_query, (room_id,))
    insights_rows = execute_read(insights_query, (room_id,))
    
    for row in transcripts_rows:
        row["timestamp"] = row["timestamp"].isoformat() + "Z"
        
    latest_insight = None
    if insights_rows:
        row = insights_rows[0]
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
            
        latest_insight = {
            "summary": row["summary"],
            "topics": payload.get("topics", []),
            "decisions": payload.get("decisions", []),
            "action_items": payload.get("action_items", []),
            "deadlines": payload.get("deadlines", []),
            "risks": payload.get("risks", []),
            "timestamp": row["timestamp"].isoformat() + "Z"
        }
        
    return {
        "roomId": room_id,
        "transcripts": transcripts_rows,
        "latestInsight": latest_insight
    }

def ensure_db_tables_exist():
    """Auto-creates transcripts and insights tables + indexes if missing."""
    transcripts_sql = """
    CREATE TABLE IF NOT EXISTS transcripts (
        id UUID PRIMARY KEY,
        room_id TEXT NOT NULL,
        peer_id TEXT NOT NULL,
        chunk_id TEXT NOT NULL,
        text TEXT NOT NULL,
        confidence FLOAT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    insights_sql = """
    CREATE TABLE IF NOT EXISTS insights (
        id UUID PRIMARY KEY,
        room_id TEXT NOT NULL,
        summary TEXT NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    transcripts_idx_sql = """
    CREATE INDEX IF NOT EXISTS idx_transcripts_room ON transcripts(room_id);
    """
    insights_idx_sql = """
    CREATE INDEX IF NOT EXISTS idx_insights_room ON insights(room_id);
    """
    try:
        execute_write(transcripts_sql)
        execute_write(insights_sql)
        execute_write(transcripts_idx_sql)
        execute_write(insights_idx_sql)
        logger.info("PostgreSQL database tables and indexes verified/created successfully.")
    except Exception as e:
        logger.error(f"Failed to auto-create database tables: {e}")
        raise e
