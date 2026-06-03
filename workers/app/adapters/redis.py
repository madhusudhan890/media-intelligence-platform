import logging
import redis.asyncio as aioredis
from app.config import REDIS_URL

logger = logging.getLogger("uvicorn.error.redis_adapter")


class RedisAdapter:
    """
    Wraps an async Redis connection with a lazy-connect pattern.
    Call `connect()` once on startup; then use `client` freely.
    """

    def __init__(self, url: str = REDIS_URL):
        self._url = url
        self._client: aioredis.Redis | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open and ping the Redis connection. Call once on application startup."""
        if self._client is not None:
            return
        try:
            self._client = aioredis.from_url(self._url, decode_responses=True)
            await self._client.ping()
            logger.info("Connected to Redis successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def disconnect(self) -> None:
        """Close the Redis client connection. Call on shutdown."""
        if self._client is not None:
            await self._client.close()
            self._client = None
            logger.info("Redis connection closed.")

    # ------------------------------------------------------------------
    # Property
    # ------------------------------------------------------------------

    @property
    def client(self) -> aioredis.Redis:
        assert self._client is not None, "RedisAdapter.connect() must be called first."
        return self._client

redis_adapter = RedisAdapter()
