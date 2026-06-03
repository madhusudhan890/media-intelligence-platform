import json
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Set

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# Adapter singletons
from app.adapters.db import db
from app.adapters.redis import redis_adapter
from app.adapters.kafka import KafkaProducerAdapter, KafkaConsumerAdapter, KafkaAdminAdapter
from app.adapters.whisper import whisper_adapter
from app.adapters.audio import audio_converter

# Use Cases
from app.usecases.transcribe import TranscribeUseCase
from app.usecases.summarize import SummarizeUseCase

logger = logging.getLogger("uvicorn.error")

# ---------------------------------------------------------------------------
# Kafka singletons
# ---------------------------------------------------------------------------
kafka_producer = KafkaProducerAdapter()
kafka_admin = KafkaAdminAdapter()

# ---------------------------------------------------------------------------
# Use case instances wired with injected adapters
# ---------------------------------------------------------------------------
transcribe_usecase = TranscribeUseCase(
    db=db,
    producer=kafka_producer,
    whisper=whisper_adapter,
    audio=audio_converter,
)
summarize_usecase = SummarizeUseCase(
    db=db,
    producer=kafka_producer,
    redis=redis_adapter,
)

# ---------------------------------------------------------------------------
# SSE subscription registry
# ---------------------------------------------------------------------------
subscriptions: Dict[str, Set[asyncio.Queue]] = {}
sub_lock = asyncio.Lock()


async def _broadcast(room_id: str, event_type: str, data: dict) -> None:
    async with sub_lock:
        queues = subscriptions.get(room_id)
        if not queues:
            return
        payload = json.dumps({"type": event_type, "data": data})
        for q in list(queues):
            try:
                q.put_nowait(payload)
            except Exception as e:
                logger.warning(f"Failed to enqueue SSE message: {e}")


async def _handle_transcript_push(raw: bytes) -> None:
    try:
        data = json.loads(raw.decode())
        if room_id := data.get("roomId"):
            await _broadcast(room_id, "transcript", data)
    except Exception as e:
        logger.error(f"Transcript push error: {e}")


async def _handle_insight_push(raw: bytes) -> None:
    try:
        data = json.loads(raw.decode())
        if room_id := data.get("roomId"):
            await _broadcast(room_id, "insight", data)
    except Exception as e:
        logger.error(f"Insight push error: {e}")


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown
# ---------------------------------------------------------------------------
consumer_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting unified pipeline...")

    # 1. Connect all adapters
    db.connect()
    db.ensure_schema()
    await redis_adapter.connect()
    whisper_adapter.load()
    await kafka_producer.connect()
    await kafka_admin.ensure_topics()

    # 2. Launch background consumer tasks
    consumers = [
        KafkaConsumerAdapter("audio-chunks",  "unified-transcripts-group",      transcribe_usecase.execute),
        KafkaConsumerAdapter("transcripts",   "unified-ai-group",               summarize_usecase.execute),
        KafkaConsumerAdapter("transcripts",   "unified-push-transcripts-group", _handle_transcript_push),
        KafkaConsumerAdapter("ai-insights",   "unified-push-insights-group",    _handle_insight_push),
    ]
    for c in consumers:
        consumer_tasks.append(asyncio.create_task(c.run()))

    logger.info("All background consumers running.")
    yield

    logger.info("Shutting down background consumers...")
    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)

    # Disconnect all active adapters cleanly
    logger.info("Disconnecting adapters...")
    await kafka_producer.disconnect()
    await redis_adapter.disconnect()
    db.disconnect()

    logger.info("Shutdown complete.")


# ASGI middleware to suppress ClientDisconnected exception groups in Python 3.11+
class SuppressDisconnectMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        try:
            await self.app(scope, receive, send)
        except (Exception, BaseException) as e:
            is_disconnect = False
            if e.__class__.__name__ in ("ExceptionGroup", "BaseExceptionGroup"):
                exceptions = getattr(e, "exceptions", [])
                if exceptions and all(
                    ex.__class__.__name__ in ("ClientDisconnected", "ClientDisconnect") or 
                    isinstance(ex, (ConnectionResetError, BrokenPipeError, asyncio.CancelledError))
                    for ex in exceptions
                ):
                    is_disconnect = True
            elif e.__class__.__name__ in ("ClientDisconnected", "ClientDisconnect") or isinstance(e, (ConnectionResetError, BrokenPipeError, asyncio.CancelledError)):
                is_disconnect = True
                
            if is_disconnect:
                logger.info("ASGI connection closed by client disconnect.")
                return
            raise e


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Real-Time Media Intelligence Unified Pipeline",
    lifespan=lifespan,
)

app.add_middleware(SuppressDisconnectMiddleware)
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
        return db.get_room_summary(roomId)
    except Exception as e:
        logger.error(f"Failed to fetch summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch room summary")


@app.get("/rooms/{roomId}/stream")
async def stream_room_events(roomId: str, request: Request):
    async def sse_generator():
        queue: asyncio.Queue = asyncio.Queue()
        async with sub_lock:
            subscriptions.setdefault(roomId, set()).add(queue)

        logger.info(f"SSE client connected: room={roomId}")
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
        except (asyncio.CancelledError, Exception) as e:
            if isinstance(e, asyncio.CancelledError):
                pass
            elif e.__class__.__name__ in ("ClientDisconnected", "ClientDisconnect") or isinstance(e, (ConnectionResetError, BrokenPipeError)):
                pass
            else:
                logger.error(f"SSE stream error: {e}", exc_info=True)
        finally:
            async with sub_lock:
                if roomId in subscriptions:
                    subscriptions[roomId].discard(queue)
                    if not subscriptions[roomId]:
                        del subscriptions[roomId]
            logger.info(f"SSE client disconnected: room={roomId}")

    return StreamingResponse(sse_generator(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    uvicorn.run("app.main:app", host="0.0.0.0", port=8082, reload=True)
