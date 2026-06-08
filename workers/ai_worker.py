import os
import json
import asyncio
import logging
import time
import httpx
from pydantic import ValidationError
import redis.asyncio as aioredis

from models import TranscriptMessage, AIInsightMessage
from db import save_insight
from kafka_client import start_kafka_consumer, get_kafka_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ai_worker")

# Environment configs
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

SYSTEM_PROMPT = """
You are a realtime meeting intelligence assistant.

Return ONLY valid JSON.

No markdown.
No explanations.
No extra text.
"""

USER_PROMPT = """
Analyze this realtime conversation transcript:

{transcript}

Extract:

1. Important discussion topics
2. Decisions made
3. Action items
4. Deadlines mentioned
5. Risks/issues discussed

Return EXACTLY this JSON format:

{{
"summary": "short summary",
"topics": ["topic1"],
"decisions": [{{"text": "decision"}}],
"action_items": [
{{
"owner": "name or Unknown",
"task": "task description"
}}
],
"deadlines": [{{"text": "deadline mentioned"}}],
"risks": [{{"text": "risk or issue"}}
]}}
"""

# State clients
redis_client = None
kafka_producer = None

async def get_redis_client():
    global redis_client
    if redis_client is None:
        retries = 10
        while retries > 0:
            try:
                redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
                await redis_client.ping()
                logger.info("Connected to Redis successfully.")
                break
            except Exception as e:
                logger.error(f"Failed to connect to Redis. Retrying in 3s... ({retries} left). Error: {e}")
                retries -= 1
                await asyncio.sleep(3)
        if redis_client is None:
            raise RuntimeError("Could not connect to Redis.")
    return redis_client

async def fetch_ai_insights_from_groq(transcript_text: str) -> dict:
    """Calls Groq API with retries and timeout handling to generate structured insights."""
    if not GROQ_API_KEY:
        logger.error("GROQ_API_KEY environment variable is not set. Cannot run summarization.")
        return get_fallback_insight_structure("Groq API key missing")

    formatted_user_prompt = USER_PROMPT.format(transcript=transcript_text)
    
    payload = {
        "model": "llama3-8b-8192",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": formatted_user_prompt}
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.2
    }
    
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(GROQ_API_URL, json=payload, headers=headers)
                
                if response.status_code != 200:
                    logger.warning(f"Groq API returned status {response.status_code}: {response.text}. Attempt {attempt}/{max_attempts}")
                    await asyncio.sleep(2)
                    continue

                res_json = response.json()
                content = res_json["choices"][0]["message"]["content"]
                
                # Validate JSON structure
                parsed_data = json.loads(content)
                
                # Check for top-level keys and set defaults if missing
                required_keys = ["summary", "topics", "decisions", "action_items", "deadlines", "risks"]
                for key in required_keys:
                    if key not in parsed_data:
                        parsed_data[key] = [] if key != "summary" else ""
                
                return parsed_data

        except httpx.RequestError as re:
            logger.warning(f"Network error calling Groq API: {re}. Attempt {attempt}/{max_attempts}")
            await asyncio.sleep(2)
        except json.JSONDecodeError as jde:
            logger.warning(f"Failed to parse Groq response as JSON: {jde}. Attempt {attempt}/{max_attempts}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.warning(f"Unexpected error during Groq API call: {e}. Attempt {attempt}/{max_attempts}")
            await asyncio.sleep(2)

    logger.error("Max retries exceeded calling Groq. Returning fallback structure.")
    return get_fallback_insight_structure("AI service unavailable after retries")

def get_fallback_insight_structure(error_msg: str) -> dict:
    return {
        "summary": f"Could not generate summary ({error_msg}).",
        "topics": ["Error"],
        "decisions": [],
        "action_items": [],
        "deadlines": [],
        "risks": [{"text": error_msg}]
    }

async def process_transcript(raw_msg_value: bytes):
    global kafka_producer
    r_client = await get_redis_client()
    
    try:
        # 1. Decode and parse transcript message
        payload = json.loads(raw_msg_value.decode("utf-8"))
        try:
            msg = TranscriptMessage(**payload)
        except ValidationError as ve:
            logger.error(f"Invalid transcript format: {ve}")
            return

        room_id = msg.roomId
        logger.info(f"Received transcript event: Room={room_id}, Peer={msg.peerId}, Text='{msg.text}'")

        # 2. Maintain rolling context in Redis (Latest 10 transcript chunks)
        # Store as standard JSON strings so we can pull speaker info (peerId) as well
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
            
            # Redis lpush puts latest at index 0, so we reverse it to chronological order for AI
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
            
            # 4. Invoke Groq API
            ai_data = await fetch_ai_insights_from_groq(transcript_content)
            
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
                # Validate message using Pydantic model
                valid_insight = AIInsightMessage(**insight_event)
            except ValidationError as ve:
                logger.error(f"Failed to validate generated AI insight structure: {ve}")
                # Use fallback model structure
                insight_event["decisions"] = [{"text": str(d.get("text", d)) if isinstance(d, dict) else str(d)} for d in insight_event["decisions"]]
                insight_event["action_items"] = [{"owner": str(a.get("owner", "Unknown")), "task": str(a.get("task", ""))} for a in insight_event["action_items"]]
                insight_event["deadlines"] = [{"text": str(d.get("text", d)) if isinstance(d, dict) else str(d)} for d in insight_event["deadlines"]]
                insight_event["risks"] = [{"text": str(r.get("text", r)) if isinstance(r, dict) else str(r)} for r in insight_event["risks"]]
                valid_insight = AIInsightMessage(**insight_event)

            if kafka_producer is None:
                kafka_producer = await get_kafka_producer()

            event_bytes = json.dumps(valid_insight.model_dump()).encode("utf-8")
            await kafka_producer.send_and_wait("ai-insights", value=event_bytes)
            logger.info(f"Published AI insights event to 'ai-insights' topic for room {room_id}")

    except Exception as e:
        logger.error(f"Unexpected error in process_transcript: {e}")

async def main():
    global kafka_producer
    logger.info("Starting AI Intelligence Worker...")
    
    # Pre-verify Redis and Kafka connections
    await get_redis_client()
    kafka_producer = await get_kafka_producer()
    
    # Start consumer loop
    await start_kafka_consumer(
        topic="transcripts",
        group_id="ai-worker-group",
        callback=process_transcript
    )

if __name__ == "__main__":
    asyncio.run(main())
