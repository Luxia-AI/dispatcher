import asyncio
import contextlib
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from prometheus_client import start_http_server

from app.consumers.kafka import KafkaPostConsumer
from app.publishers.kafka import KafkaPublisher
from app.utils.config import (
    GROUP_ID,
    KAFKA_BOOTSTRAP,
    LOG_LEVEL,
    POSTS_TOPIC,
    PROM_PORT,
    get_kafka_config,
)
from app.utils.observability import setup_tracing

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("dispatcher")

app = FastAPI(title="Dispatcher Service")
setup_tracing(app)

consumer: AIOKafkaConsumer = None
producer: AIOKafkaProducer = None
dispatch_task = None


@app.on_event("startup")
async def startup():
    global consumer, producer, dispatch_task

    start_http_server(PROM_PORT)
    logger.info("Prometheus running on %s", PROM_PORT)

    kafka_config = get_kafka_config()

    producer = AIOKafkaProducer(**kafka_config)
    consumer = AIOKafkaConsumer(
        POSTS_TOPIC,
        **kafka_config,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
    )

    await producer.start()
    await consumer.start()
    logger.info("Kafka connected (bootstrap=%s)", KAFKA_BOOTSTRAP)

    publisher = KafkaPublisher(producer)
    handler = KafkaPostConsumer(consumer, publisher)

    dispatch_task = asyncio.create_task(handler.start_loop())
    logger.info("Dispatcher started.")


@app.on_event("shutdown")
async def shutdown():
    dispatch_task.cancel()
    with contextlib.suppress(BaseException):
        await dispatch_task

    await consumer.stop()
    await producer.stop()

    logger.info("Dispatcher shut down cleanly.")


@app.get("/health")
async def health():
    return {"status": "ok"}
