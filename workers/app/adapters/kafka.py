import logging
import asyncio
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from app.config import KAFKA_BROKER

logger = logging.getLogger("kafka_adapter")

class ConfluentKafkaProducerWrapper:
    def __init__(self, producer: Producer):
        self._producer = producer
        self._loop = asyncio.get_event_loop()
        self._poll_task = self._loop.create_task(self._poll_loop())
        
    async def _poll_loop(self):
        try:
            while True:
                await self._loop.run_in_executor(None, self._producer.poll, 0.1)
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass
            
    async def send_and_wait(self, topic: str, value: bytes):
        future = self._loop.create_future()
        
        def delivery_report(err, msg):
            if err is not None:
                self._loop.call_soon_threadsafe(future.set_exception, Exception(f"Kafka delivery failed: {err}"))
            else:
                self._loop.call_soon_threadsafe(future.set_result, msg)
                
        self._producer.produce(topic, value=value, callback=delivery_report)
        self._producer.poll(0)
        return await future

kafka_producer = None

async def get_kafka_producer() -> ConfluentKafkaProducerWrapper:
    global kafka_producer
    if kafka_producer is None:
        try:
            conf = {'bootstrap.servers': KAFKA_BROKER}
            producer = Producer(conf)
            logger.info("Successfully connected Kafka producer.")
            kafka_producer = ConfluentKafkaProducerWrapper(producer)
        except Exception as e:
            logger.error(f"Could not connect Kafka producer: {e}")
            raise e
    return kafka_producer

async def start_kafka_consumer(topic: str, group_id: str, callback):
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    
    try:
        consumer = Consumer(conf)
        consumer.subscribe([topic])
        logger.info(f"Successfully started Kafka consumer for topic '{topic}' (group: '{group_id}').")
    except Exception as e:
        logger.error(f"Could not connect Kafka consumer for topic '{topic}': {e}")
        raise e

    loop = asyncio.get_running_loop()
    try:
        while True:
            msg = await loop.run_in_executor(None, consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            try:
                await callback(msg.value())
            except Exception as e:
                logger.error(f"Error executing callback on message from topic '{topic}': {e}")
    except asyncio.CancelledError:
        logger.info(f"Kafka consumer task for topic '{topic}' has been cancelled.")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' stopped.")

async def ensure_kafka_topics_exist():
    """Checks for required topics and creates them asynchronously if missing."""
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    loop = asyncio.get_running_loop()
    
    def _check_and_create():
        try:
            metadata = admin_client.list_topics(timeout=10)
            existing_topics = metadata.topics.keys()
            
            required_topics = ["audio-chunks", "transcripts", "ai-insights"]
            new_topics = []
            for topic in required_topics:
                if topic not in existing_topics:
                    logger.info(f"Kafka topic '{topic}' does not exist. Auto-creating...")
                    new_topics.append(NewTopic(topic, num_partitions=1, replication_factor=1))
            
            if new_topics:
                futures = admin_client.create_topics(new_topics)
                for topic, f in futures.items():
                    f.result()
                    logger.info(f"Kafka topic '{topic}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to check or auto-create Kafka topics: {e}")
            
    try:
        await loop.run_in_executor(None, _check_and_create)
    except Exception as e:
        logger.error(f"Failed to check or auto-create Kafka topics: {e}")
