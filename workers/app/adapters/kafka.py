import asyncio
import logging
from typing import Callable, Awaitable

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from app.config import KAFKA_BROKER

logger = logging.getLogger("uvicorn.error.kafka_adapter")

REQUIRED_TOPICS = ["audio-chunks", "transcripts", "ai-insights"]
MessageCallback = Callable[[bytes], Awaitable[None]]


class KafkaProducerAdapter:
    """
    Async-friendly wrapper around confluent-kafka's synchronous Producer.
    Runs a background poll loop so delivery callbacks fire without blocking.
    """

    def __init__(self, broker: str = KAFKA_BROKER):
        self._broker = broker
        self._producer: Producer | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._poll_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Create the producer and start the background poll loop."""
        if self._producer is not None:
            return
        try:
            self._producer = Producer({"bootstrap.servers": self._broker})
            self._loop = asyncio.get_running_loop()
            self._poll_task = self._loop.create_task(self._poll_loop())
            logger.info("Kafka producer connected.")
        except Exception as e:
            logger.error(f"Failed to connect Kafka producer: {e}")
            raise

    async def _poll_loop(self) -> None:
        """Drains the producer delivery queue at a regular cadence."""
        assert self._loop is not None
        try:
            while True:
                await self._loop.run_in_executor(None, self._producer.poll, 0.1)
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass

    async def disconnect(self) -> None:
        """Cancel background poll loop and flush remaining messages."""
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

        if self._producer is not None:
            if self._loop is not None:
                await self._loop.run_in_executor(None, self._producer.flush, 3.0)
            else:
                self._producer.flush(3.0)
            self._producer = None
            logger.info("Kafka producer disconnected.")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def send(self, topic: str, value: bytes) -> None:
        """Publish *value* to *topic* and await broker acknowledgement."""
        assert self._producer is not None, "KafkaProducerAdapter.connect() must be called first."
        assert self._loop is not None

        future: asyncio.Future = self._loop.create_future()

        def _delivery(err, msg):
            if err:
                self._loop.call_soon_threadsafe(
                    future.set_exception, Exception(f"Kafka delivery failed: {err}")
                )
            else:
                self._loop.call_soon_threadsafe(future.set_result, msg)

        self._producer.produce(topic, value=value, callback=_delivery)
        self._producer.poll(0)
        await future


class KafkaConsumerAdapter:
    """
    Runs a single Kafka consumer in an asyncio-friendly executor loop,
    dispatching each message to a caller-supplied async callback.
    """

    def __init__(
        self,
        topic: str,
        group_id: str,
        callback: MessageCallback,
        broker: str = KAFKA_BROKER,
    ):
        self._topic = topic
        self._group_id = group_id
        self._callback = callback
        self._broker = broker

    async def run(self) -> None:
        """Block (async) until the task is cancelled, processing messages."""
        conf = {
            "bootstrap.servers": self._broker,
            "group.id": self._group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
        try:
            consumer = Consumer(conf)
            consumer.subscribe([self._topic])
            logger.info(f"Kafka consumer started: topic='{self._topic}' group='{self._group_id}'")
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer for '{self._topic}': {e}")
            raise

        loop = asyncio.get_running_loop()
        try:
            while True:
                msg = await loop.run_in_executor(None, consumer.poll, 1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error on '{self._topic}': {msg.error()}")
                    continue
                try:
                    await self._callback(msg.value())
                except Exception as e:
                    logger.error(f"Callback error on '{self._topic}': {e}")
        except asyncio.CancelledError:
            logger.info(f"Kafka consumer cancelled: topic='{self._topic}'")
        finally:
            consumer.close()
            logger.info(f"Kafka consumer closed: topic='{self._topic}'")


class KafkaAdminAdapter:
    """
    Ensures required Kafka topics exist at startup, creating any that are missing.
    """

    def __init__(self, topics: list[str] = REQUIRED_TOPICS, broker: str = KAFKA_BROKER):
        self._topics = topics
        self._broker = broker

    async def ensure_topics(self) -> None:
        admin = AdminClient({"bootstrap.servers": self._broker})
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._check_and_create, admin)

    def _check_and_create(self, admin: AdminClient) -> None:
        try:
            existing = admin.list_topics(timeout=10).topics.keys()
            new_topics = [
                NewTopic(t, num_partitions=1, replication_factor=1)
                for t in self._topics
                if t not in existing
            ]
            if not new_topics:
                logger.info("All required Kafka topics already exist.")
                return
            for topic, f in admin.create_topics(new_topics).items():
                f.result()
                logger.info(f"Kafka topic '{topic}' created.")
        except Exception as e:
            logger.error(f"Failed to ensure Kafka topics: {e}")
            raise
