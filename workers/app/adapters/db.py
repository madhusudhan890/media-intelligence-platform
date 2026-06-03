import uuid
import json
import logging
from contextlib import contextmanager
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from app.config import DATABASE_URL, DB_USER, DB_PASSWORD, DB_DBNAME, DB_HOST, DB_PORT

logger = logging.getLogger("uvicorn.error.db_adapter")

class DatabaseAdapter:
    """
    Manages a threaded Postgres connection pool and exposes typed
    read/write helpers and domain-specific repository methods.
    """

    _TRANSCRIPTS_DDL = """
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
    """
    _INSIGHTS_DDL = """
    CREATE TABLE IF NOT EXISTS insights (
        id UUID PRIMARY KEY,
        room_id TEXT NOT NULL,
        summary TEXT NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    _TRANSCRIPTS_IDX = "CREATE INDEX IF NOT EXISTS idx_transcripts_room ON transcripts(room_id);"
    _INSIGHTS_IDX = "CREATE INDEX IF NOT EXISTS idx_insights_room ON insights(room_id);"

    def __init__(self, min_conn: int = 2, max_conn: int = 20):
        self._pool: pool.ThreadedConnectionPool | None = None
        self._min_conn = min_conn
        self._max_conn = max_conn

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the connection pool. Call once on application startup."""
        if self._pool is not None:
            return
        try:
            if DATABASE_URL:
                self._pool = pool.ThreadedConnectionPool(
                    self._min_conn, self._max_conn, dsn=DATABASE_URL
                )
            else:
                self._pool = pool.ThreadedConnectionPool(
                    self._min_conn,
                    self._max_conn,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    host=DB_HOST,
                    port=DB_PORT,
                    database=DB_DBNAME,
                )
            logger.info("Successfully connected to Postgres database pool.")
        except Exception as e:
            logger.error(f"Failed to connect to Postgres: {e}")
            raise

    def disconnect(self) -> None:
        """Close the database connection pool. Call on shutdown."""
        if self._pool is not None:
            self._pool.closeall()
            self._pool = None
            logger.info("Database connection pool closed.")

    def ensure_schema(self) -> None:
        """Auto-create tables and indexes. Idempotent — safe to call every boot."""
        for ddl in (
            self._TRANSCRIPTS_DDL,
            self._INSIGHTS_DDL,
            self._TRANSCRIPTS_IDX,
            self._INSIGHTS_IDX,
        ):
            self._execute_write(ddl)
        try:
            self._execute_write("ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS peer_name TEXT DEFAULT 'Anonymous';")
        except Exception as mig_err:
            logger.warning(f"Could not run alter table migration for transcripts: {mig_err}")
        logger.info("PostgreSQL schema verified/created successfully.")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @contextmanager
    def _get_conn(self):
        """Context manager that checks out a connection and always returns it."""
        assert self._pool is not None, "DatabaseAdapter.connect() must be called first."
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def _execute_write(self, query: str, params=None) -> None:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Database write error: {e}")
                raise

    def _execute_read(self, query: str, params=None) -> list:
        with self._get_conn() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    return cur.fetchall()
            except Exception as e:
                logger.error(f"Database read error: {e}")
                raise

    # ------------------------------------------------------------------
    # Repository methods
    # ------------------------------------------------------------------

    def save_transcript(
        self,
        room_id: str,
        peer_id: str,
        peer_name: str,
        chunk_id: str,
        text: str,
        confidence: float,
    ) -> None:
        query = """
        INSERT INTO transcripts (id, room_id, peer_id, peer_name, chunk_id, text, confidence, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO NOTHING;
        """
        self._execute_write(query, (str(uuid.uuid4()), room_id, peer_id, peer_name, chunk_id, text, confidence))
        logger.info(f"Saved transcript: room={room_id} chunk={chunk_id}")

    def save_insight(self, room_id: str, summary: str, payload: dict) -> None:
        query = """
        INSERT INTO insights (id, room_id, summary, payload, created_at)
        VALUES (%s, %s, %s, %s, NOW());
        """
        self._execute_write(query, (str(uuid.uuid4()), room_id, summary, json.dumps(payload)))
        logger.info(f"Saved insight: room={room_id}")

    def get_room_summary(self, room_id: str) -> dict:
        transcripts_rows = self._execute_read(
            """
            SELECT peer_id as "peerId", peer_name as "peerName", chunk_id as "chunkId", text, confidence,
                   created_at as "timestamp"
            FROM transcripts
            WHERE room_id = %s
            ORDER BY created_at ASC;
            """,
            (room_id,),
        )
        insights_rows = self._execute_read(
            """
            SELECT summary, payload, created_at as "timestamp"
            FROM insights
            WHERE room_id = %s
            ORDER BY created_at DESC
            LIMIT 1;
            """,
            (room_id,),
        )

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
                "timestamp": row["timestamp"].isoformat() + "Z",
            }

        return {"roomId": room_id, "transcripts": transcripts_rows, "latestInsight": latest_insight}


db = DatabaseAdapter()
