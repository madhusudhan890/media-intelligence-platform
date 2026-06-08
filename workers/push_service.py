import os
import json
import asyncio
import logging
from typing import Dict, Set
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from db import get_room_summary
from kafka_client import start_kafka_consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("push_service")

app = FastAPI(title="Real-Time Media Intelligence Push Service", version="1.0")

# Allow CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust in production to frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory subscriptions dictionary: roomId -> set of asyncio.Queue
subscriptions: Dict[str, Set[asyncio.Queue]] = {}
sub_lock = asyncio.Lock()

async def broadcast_message(room_id: str, event_type: str, data: dict):
    """Sends a message to all active SSE queues for a specific room."""
    async with sub_lock:
        queues = subscriptions.get(room_id)
        if not queues:
            return
        
        logger.info(f"Broadcasting '{event_type}' to {len(queues)} subscribers in room {room_id}")
        payload = json.dumps({"type": event_type, "data": data})
        
        # Write to all queues in parallel
        for q in list(queues):
            try:
                q.put_nowait(payload)
            except Exception as e:
                logger.warning(f"Failed to put message on queue for room {room_id}: {e}")

async def handle_raw_transcript(raw_val: bytes):
    """Callback for transcripts Kafka topic consumer."""
    try:
        data = json.loads(raw_val.decode("utf-8"))
        room_id = data.get("roomId")
        if room_id:
            await broadcast_message(room_id, "transcript", data)
    except Exception as e:
        logger.error(f"Error handling raw transcript in push service: {e}")

async def handle_raw_insight(raw_val: bytes):
    """Callback for ai-insights Kafka topic consumer."""
    try:
        data = json.loads(raw_val.decode("utf-8"))
        room_id = data.get("roomId")
        if room_id:
            await broadcast_message(room_id, "insight", data)
    except Exception as e:
        logger.error(f"Error handling raw insight in push service: {e}")

# Background tasks lifecycle
consumer_tasks = []

@app.on_event("startup")
async def startup_event():
    """Starts Kafka consumers as background tasks."""
    logger.info("Starting background Kafka consumers in Push Service...")
    
    # Start transcripts consumer
    t_task = asyncio.create_task(
        start_kafka_consumer(
            topic="transcripts",
            group_id="push-service-transcripts-group",
            callback=handle_raw_transcript
        )
    )
    
    # Start ai-insights consumer
    i_task = asyncio.create_task(
        start_kafka_consumer(
            topic="ai-insights",
            group_id="push-service-insights-group",
            callback=handle_raw_insight
        )
    )
    
    consumer_tasks.extend([t_task, i_task])
    logger.info("Kafka consumer tasks launched successfully.")

@app.on_event("shutdown")
async def shutdown_event():
    """Cancels background consumers and drains resources."""
    logger.info("Shutting down Push Service background tasks...")
    for task in consumer_tasks:
        task.cancel()
    
    # Wait for all tasks to complete/terminate
    if consumer_tasks:
        await asyncio.gather(*consumer_tasks, return_exceptions=True)
    logger.info("All background tasks shut down.")

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "push-service"}

@app.get("/rooms/{roomId}/summary")
async def get_summary(roomId: str):
    """Queries Postgres to fetch historical transcripts and the latest AI summary/insights."""
    try:
        summary_data = get_room_summary(roomId)
        return summary_data
    except Exception as e:
        logger.error(f"Failed to fetch summary for room {roomId}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch room summary from database")

@app.get("/rooms/{roomId}/stream")
async def stream_room_events(roomId: str, request: Request):
    """Server-Sent Events endpoint to stream transcripts and insights to the frontend."""
    async def sse_generator():
        queue = asyncio.Queue()
        async with sub_lock:
            if roomId not in subscriptions:
                subscriptions[roomId] = set()
            subscriptions[roomId].add(queue)
            
        logger.info(f"New SSE client connected for room {roomId}. Total subscribers: {len(subscriptions[roomId])}")
        
        try:
            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    logger.info(f"SSE client disconnected from room {roomId} (detected via request.is_disconnected)")
                    break
                
                try:
                    # Wait for a message with a timeout to allow periodic disconnect checks
                    payload = await asyncio.wait_for(queue.get(), timeout=2.0)
                    yield f"data: {payload}\n\n"
                    queue.task_done()
                except asyncio.TimeoutError:
                    # Send a keepalive comment
                    yield ": keepalive\n\n"
                    
        except asyncio.CancelledError:
            logger.info(f"SSE stream generator cancelled for room {roomId}")
        finally:
            async with sub_lock:
                if roomId in subscriptions:
                    subscriptions[roomId].discard(queue)
                    if not subscriptions[roomId]:
                        del subscriptions[roomId]
            logger.info(f"SSE client cleanup done for room {roomId}.")

    return StreamingResponse(sse_generator(), media_type="text/event-stream")
