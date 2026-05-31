import logging
import redis.asyncio as aioredis
from app.config import REDIS_URL

logger = logging.getLogger("redis_adapter")

redis_client = None

async def get_redis_client():
    global redis_client
    if redis_client is None:
        try:
            redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
            await redis_client.ping()
            logger.info("Connected to Redis successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise e
    return redis_client
