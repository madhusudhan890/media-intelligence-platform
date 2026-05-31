import json
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Set

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from app.adapters.db import get_db_pool, get_room_summary, ensure_db_tables_exist
from app.adapters.redis import get_redis_client
from app.adapters.kafka import get_kafka_producer, start_kafka_consumer, ensure_kafka_topics_exist
from app.adapters.whisper import get_whisper_model

# Import Handlers / Use Cases
from app.usecases.transcribe import TranscribeUseCase
from app.usecases.summarize import SummarizeUseCase

logger = logging.getLogger("uvicorn.error")

# --- Push Service / SSE Subscriptions ---
subscriptions: Dict[str, Set[asyncio.Queue]] = {}
sub_lock = asyncio.Lock()

async def broadcast_message(room_id: str, event_type: str, data: dict):
    async with sub_lock:
        queues = subscriptions.get(room_id)
        if not queues:
            return
        payload = json.dumps({"type": event_type, "data": data})
        for q in list(queues):
            try:
                q.put_nowait(payload)
            except Exception as e:
                logger.warning(f"Failed to enqueue message: {e}")

async def handle_raw_transcript(raw_val: bytes):
    try:
        data = json.loads(raw_val.decode("utf-8"))
        room_id = data.get("roomId")
        if room_id:
            await broadcast_message(room_id, "transcript", data)
    except Exception as e:
        logger.error(f"Error handling raw transcript: {e}")

async def handle_raw_insight(raw_val: bytes):
    try:
        data = json.loads(raw_val.decode("utf-8"))
        room_id = data.get("roomId")
        if room_id:
            await broadcast_message(room_id, "insight", data)
    except Exception as e:
        logger.error(f"Error handling raw insight: {e}")

# Initialize Usecase instances
transcribe_usecase = TranscribeUseCase()
summarize_usecase = SummarizeUseCase()

# --- FastAPI Setup with Lifespan ---
consumer_tasks = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing unified workers on lifespan startup...")
    
    # 1. Warm up resources
    get_db_pool()
    ensure_db_tables_exist()
    await get_redis_client()
    get_whisper_model()
    await get_kafka_producer()

    # 2. Auto-create Kafka topics if they do not exist
    await ensure_kafka_topics_exist()

    # 3. Launch background consumers mapped to Usecase execution
    t_task = asyncio.create_task(
        start_kafka_consumer("audio-chunks", "unified-transcripts-group", transcribe_usecase.execute)
    )
    a_task = asyncio.create_task(
        start_kafka_consumer("transcripts", "unified-ai-group", summarize_usecase.execute)
    )
    push_t_task = asyncio.create_task(
        start_kafka_consumer("transcripts", "unified-push-transcripts-group", handle_raw_transcript)
    )
    push_i_task = asyncio.create_task(
        start_kafka_consumer("ai-insights", "unified-push-insights-group", handle_raw_insight)
    )
    
    consumer_tasks.extend([t_task, a_task, push_t_task, push_i_task])
    logger.info("All background threads running.")
    yield
    
    logger.info("Stopping unified background tasks...")
    for task in consumer_tasks:
        task.cancel()
    if consumer_tasks:
        await asyncio.gather(*consumer_tasks, return_exceptions=True)
    logger.info("Shutdown complete.")

app = FastAPI(title="Real-Time Media Intelligence Unified Pipeline", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "unified-pipeline"}

@app.get("/rooms/{roomId}/summary")
async def get_summary(roomId: str):
    try:
        return get_room_summary(roomId)
    except Exception as e:
        logger.error(f"Failed to fetch summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch room summary")

@app.get("/rooms/{roomId}/stream")
async def stream_room_events(roomId: str, request: Request):
    async def sse_generator():
        queue = asyncio.Queue()
        async with sub_lock:
            if roomId not in subscriptions:
                subscriptions[roomId] = set()
            subscriptions[roomId].add(queue)
            
        logger.info(f"SSE client registered for room {roomId}.")
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=2.0)
                    yield f"data: {payload}\n\n"
                    queue.task_done()
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            async with sub_lock:
                if roomId in subscriptions:
                    subscriptions[roomId].discard(queue)
                    if not subscriptions[roomId]:
                        del subscriptions[roomId]
            logger.info(f"SSE client disconnected from room {roomId}.")

    return StreamingResponse(sse_generator(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    uvicorn.run("app.entrypoints.main:app", host="0.0.0.0", port=8082, reload=True)
