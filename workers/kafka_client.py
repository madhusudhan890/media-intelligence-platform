import os
import asyncio
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("kafka_client")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

async def get_kafka_producer(max_retries=10, retry_delay=3) -> AIOKafkaProducer:
    """Initializes and starts an AIOKafkaProducer with connection retries."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
            await producer.start()
            logger.info("Successfully connected Kafka producer.")
            return producer
        except Exception as e:
            logger.warning(f"Kafka producer connection attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                await asyncio.sleep(retry_delay)
            else:
                logger.error("Could not connect Kafka producer after maximum retries.")
                raise e

async def start_kafka_consumer(topic: str, group_id: str, callback, max_retries=10, retry_delay=3):
    """Starts an AIOKafkaConsumer loop and invokes the callback on each message value."""
    consumer = None
    for attempt in range(1, max_retries + 1):
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                group_id=group_id,
                auto_offset_reset="latest"
            )
            await consumer.start()
            logger.info(f"Successfully started Kafka consumer for topic '{topic}' (group: '{group_id}').")
            break
        except Exception as e:
            logger.warning(f"Kafka consumer connection attempt {attempt}/{max_retries} failed for topic '{topic}': {e}")
            if attempt < max_retries:
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Could not connect Kafka consumer for topic '{topic}' after maximum retries.")
                raise e

    try:
        async for msg in consumer:
            try:
                await callback(msg.value)
            except Exception as e:
                logger.error(f"Error executing callback on message from topic '{topic}': {e}")
    except asyncio.CancelledError:
        logger.info(f"Kafka consumer task for topic '{topic}' has been cancelled.")
    finally:
        if consumer:
            await consumer.stop()
            logger.info(f"Kafka consumer for topic '{topic}' stopped.")
