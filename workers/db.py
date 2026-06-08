import os
import time
import logging
import uuid
import json
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("db_utility")

# Load environment variables
DB_USER = os.getenv("POSTGRES_USER", "meeting")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "meeting")
DB_DBNAME = os.getenv("POSTGRES_DB", "meetingdb")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

_pool = None

def get_connection_pool():
    global _pool
    if _pool is None:
        # Retry connection to Postgres in case it's starting up
        retries = 10
        while retries > 0:
            try:
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
                break
            except Exception as e:
                logger.error(f"Failed to connect to Postgres. Retrying in 3s... ({retries} left). Error: {e}")
                retries -= 1
                time.sleep(3)
        if _pool is None:
            raise RuntimeError("Could not connect to Postgres database pool.")
    return _pool

def execute_write(query, params=None):
    db_pool = get_connection_pool()
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
    db_pool = get_connection_pool()
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
    
    # Format timestamps as ISO strings
    for row in transcripts_rows:
        row["timestamp"] = row["timestamp"].isoformat() + "Z"
        
    latest_insight = None
    if insights_rows:
        row = insights_rows[0]
        # payload is JSONB, so it might be returned as a dict or needs parsing
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
