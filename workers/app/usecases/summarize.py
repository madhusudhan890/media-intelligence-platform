import json
import logging
import time

from app.domain.models import TranscriptMessage, AIInsightMessage
from app.adapters.db import save_insight
from app.adapters.kafka import get_kafka_producer
from app.adapters.redis import get_redis_client
from app.adapters.llm import fetch_ai_insights

logger = logging.getLogger("summarize_usecase")

class SummarizeUseCase:
    def __init__(self):
        self.kafka_producer = None

    async def execute(self, raw_msg_value: bytes):
        r_client = await get_redis_client()
        try:
            payload = json.loads(raw_msg_value.decode("utf-8"))
            try:
                msg = TranscriptMessage(**payload)
            except Exception as ve:
                logger.error(f"Invalid transcript format: {ve}")
                return

            room_id = msg.roomId
            logger.info(f"Received transcript event: Room={room_id}, Peer={msg.peerId}, Text='{msg.text}'")

            # 2. Maintain rolling context in Redis (Latest 10 transcript chunks)
            context_key = f"context:{room_id}"
            await r_client.lpush(context_key, json.dumps(msg.model_dump()))
            await r_client.ltrim(context_key, 0, 9)
            await r_client.expire(context_key, 3600)  # 1 hour TTL

            # 3. Increment counter to trigger every 5 transcripts
            counter_key = f"counter:{room_id}"
            val = await r_client.incr(counter_key)
            await r_client.expire(counter_key, 3600)

            logger.info(f"Rolling context size for room {room_id}: current counter={val}")

            if val % 5 == 0:
                logger.info(f"Triggering AI Worker summarization for room {room_id} (count={val})...")
                
                # Fetch latest 10 chunks from Redis
                chunks_json = await r_client.lrange(context_key, 0, 9)
                chunks_json.reverse()
                
                # Format the transcripts chronologically
                formatted_lines = []
                for c_str in chunks_json:
                    try:
                        c_data = json.loads(c_str)
                        speaker = c_data.get("peerId", "Unknown")[:8]
                        text = c_data.get("text", "")
                        formatted_lines.append(f"Speaker {speaker}: {text}")
                    except Exception as e:
                        logger.error(f"Error parsing redis context item: {e}")
                        
                transcript_content = "\n".join(formatted_lines)
                
                # 4. Invoke LLM Adapter
                ai_data = await fetch_ai_insights(transcript_content)
                
                # 5. Save to Postgres
                summary_text = ai_data.get("summary", "No summary generated.")
                try:
                    save_insight(room_id, summary_text, ai_data)
                except Exception as e:
                    logger.error(f"Failed to save insight to Postgres: {e}")

                # 6. Publish to Kafka ai-insights topic
                insight_event = {
                    "roomId": room_id,
                    "summary": summary_text,
                    "topics": ai_data.get("topics", []),
                    "decisions": ai_data.get("decisions", []),
                    "action_items": ai_data.get("action_items", []),
                    "deadlines": ai_data.get("deadlines", []),
                    "risks": ai_data.get("risks", []),
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                }

                try:
                    valid_insight = AIInsightMessage(**insight_event)
                except Exception:
                    # Fallback mapping
                    insight_event["decisions"] = [{"text": str(d.get("text", d)) if isinstance(d, dict) else str(d)} for d in insight_event["decisions"]]
                    insight_event["action_items"] = [{"owner": str(a.get("owner", "Unknown")), "task": str(a.get("task", ""))} for a in insight_event["action_items"]]
                    insight_event["deadlines"] = [{"text": str(d.get("text", d)) if isinstance(d, dict) else str(d)} for d in insight_event["deadlines"]]
                    insight_event["risks"] = [{"text": str(r.get("text", r)) if isinstance(r, dict) else str(r)} for r in insight_event["risks"]]
                    valid_insight = AIInsightMessage(**insight_event)

                if self.kafka_producer is None:
                    self.kafka_producer = await get_kafka_producer()

                event_bytes = json.dumps(valid_insight.model_dump()).encode("utf-8")
                await self.kafka_producer.send_and_wait("ai-insights", value=event_bytes)
                logger.info(f"Published AI insights event to 'ai-insights' topic for room {room_id}")

        except Exception as e:
            logger.error(f"Unexpected error in SummarizeUseCase: {e}")
