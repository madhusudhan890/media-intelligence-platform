import json
import logging
import time

from app.domain.models import TranscriptMessage, AIInsightMessage
from app.adapters.db import DatabaseAdapter
from app.adapters.kafka import KafkaProducerAdapter
from app.adapters.redis import RedisAdapter
from app.adapters.llm.factory import LLMProviderFactory
from app.config import AI_INSIGHT_TRIGGER_EVERY

logger = logging.getLogger("uvicorn.error.summarize_usecase")


class SummarizeUseCase:
    """
    Orchestrates the AI insight pipeline for each incoming transcript event:
      1. Push transcript into Redis sliding-window context (last 10 chunks).
      2. Every N events, fetch the window and call the LLM.
      3. Persist the structured insight to Postgres.
      4. Publish the insight event to Kafka.

    Dependencies are injected via the constructor.
    """

    CONTEXT_WINDOW = 10
    TRIGGER_EVERY = AI_INSIGHT_TRIGGER_EVERY
    CONTEXT_TTL_S = 3600

    def __init__(
        self,
        db: DatabaseAdapter,
        producer: KafkaProducerAdapter,
        redis: RedisAdapter,
    ):
        self._db = db
        self._producer = producer
        self._redis = redis
        self._llm = LLMProviderFactory.get_provider()

    async def execute(self, raw_msg_value: bytes) -> None:
        # ── 1. Parse ─────────────────────────────────────────────────────
        try:
            payload = json.loads(raw_msg_value.decode("utf-8"))
            msg = TranscriptMessage(**payload)
        except Exception as e:
            logger.error(f"Invalid transcript message: {e}")
            return

        room_id = msg.roomId
        r = self._redis.client

        logger.info(f"Transcript received: room={room_id} peer={msg.peerId} text='{msg.text}'")

        # ── 2. Update sliding-window context ─────────────────────────────
        context_key = f"context:{room_id}"
        counter_key = f"counter:{room_id}"

        await r.lpush(context_key, json.dumps(msg.model_dump()))  # pyrefly: ignore[not-async]
        await r.ltrim(context_key, 0, self.CONTEXT_WINDOW - 1)  # pyrefly: ignore[not-async]
        await r.expire(context_key, self.CONTEXT_TTL_S)  # pyrefly: ignore[not-async]

        count: int = await r.incr(counter_key)  # pyrefly: ignore[not-async]
        await r.expire(counter_key, self.CONTEXT_TTL_S)  # pyrefly: ignore[not-async]

        logger.info(f"Context counter for room {room_id}: {count}")

        if count % self.TRIGGER_EVERY != 0:
            return

        # ── 3. Build transcript block ─────────────────────────────────────
        logger.info(f"Triggering AI summarisation for room {room_id} (count={count})")
        raw_chunks: list[str] = await r.lrange(context_key, 0, self.CONTEXT_WINDOW - 1)  # pyrefly: ignore[not-async]
        raw_chunks.reverse()

        lines = []
        for raw in raw_chunks:
            try:
                c = json.loads(raw)
                speaker_name = c.get('peerName') or f"Speaker {c.get('peerId', 'Unknown')[:8]}"
                lines.append(f"{speaker_name}: {c.get('text', '')}")
            except Exception as e:
                logger.error(f"Failed to parse context item: {e}")

        transcript_block = "\n".join(lines)

        if not transcript_block.strip():
            logger.info("Transcript window block is empty. Skipping LLM call to save credits.")
            return

        # ── 4. Call LLM ───────────────────────────────────────────────────
        ai_data = await self._llm.fetch_insights(transcript_block)
        summary = ai_data.get("summary", "No summary generated.")

        # ── 5. Persist ────────────────────────────────────────────────────
        try:
            self._db.save_insight(room_id, summary, ai_data)
        except Exception as e:
            logger.error(f"Failed to persist insight: {e}")

        # ── 6. Publish ────────────────────────────────────────────────────
        insight_raw = {
            "roomId": room_id,
            "summary": summary,
            "topics": ai_data.get("topics", []),
            "decisions": ai_data.get("decisions", []),
            "action_items": ai_data.get("action_items", []),
            "deadlines": ai_data.get("deadlines", []),
            "risks": ai_data.get("risks", []),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        # Normalise to Pydantic with fallback coercion
        try:
            valid_insight = AIInsightMessage(**insight_raw)
        except Exception:
            insight_raw["decisions"] = [
                {"text": d.get("text", str(d)) if isinstance(d, dict) else str(d)}
                for d in insight_raw["decisions"]
            ]
            insight_raw["action_items"] = [
                {"owner": a.get("owner", "Unknown"), "task": a.get("task", "")}
                for a in insight_raw["action_items"]
            ]
            insight_raw["deadlines"] = [
                {"text": d.get("text", str(d)) if isinstance(d, dict) else str(d)}
                for d in insight_raw["deadlines"]
            ]
            insight_raw["risks"] = [
                {"text": r_.get("text", str(r_)) if isinstance(r_, dict) else str(r_)}
                for r_ in insight_raw["risks"]
            ]
            valid_insight = AIInsightMessage(**insight_raw)

        await self._producer.send(
            "ai-insights", json.dumps(valid_insight.model_dump()).encode()
        )
        logger.info(f"Published AI insight event: room={room_id}")
